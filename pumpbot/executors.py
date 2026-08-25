from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Callable, Awaitable
import time
import os
import asyncio
import csv

from pumpbot.config import Config

@dataclass
class Position:
    """Representa una posición (simulada o real) que gestiona el EXECUTOR.
    Deliberadamente independiente de TokenCandidate: el executor no necesita
    saber nada del proceso de filtrado, solo mint/symbol y estado de precio."""
    mint: str
    symbol: str
    entry_price: float
    entry_time: float = field(default_factory=time.time)
    amount_sol: float = 0.0
    position_tokens: float = 0.0
    v_sol_in_curve: float = 0.0
    v_tokens_in_curve: float = 0.0
    trades_since_open: int = 0

    @property
    def current_price(self) -> Optional[float]:
        if self.v_tokens_in_curve <= 0:
            return None
        return self.v_sol_in_curve / self.v_tokens_in_curve

    def pnl_pct(self) -> Optional[float]:
        price = self.current_price
        if not price or not self.entry_price:
            return None
        return ((price - self.entry_price) / self.entry_price) * 100

    def update_from_trade(self, msg: dict):
        v_sol = msg.get("vSolInBondingCurve")
        v_tok = msg.get("vTokensInBondingCurve")
        if v_sol is not None:
            self.v_sol_in_curve = float(v_sol)
        if v_tok is not None:
            self.v_tokens_in_curve = float(v_tok)


# ======================================================================
# TRADE EXECUTOR: interfaz + implementaciones
# ======================================================================

# Tipo de los callbacks que el scanner inyecta para que el executor pueda
# pedir que se (des)suscriba a los trades de un mint, sin que el executor
# tenga que conocer el objeto WebSocket directamente.
SubscribeFn = Callable[[str], Awaitable[None]]


class TradeExecutor(ABC):
    """
    Interfaz común para "qué hacer" con un candidato que pasó el filtro.
    El PumpFunScanner solo conoce esta interfaz, nunca la implementación
    concreta — así puedes pasar de SimulatedTradeExecutor a RealTradeExecutor
    sin tocar el scanner.
    """

    def __init__(self, config: Config):
        self.cfg = config
        self.open_positions: dict[str, Position] = {}
        self._subscribe_fn: Optional[SubscribeFn] = None
        self._unsubscribe_fn: Optional[SubscribeFn] = None

    def bind_subscription_callbacks(self, subscribe_fn: SubscribeFn, unsubscribe_fn: SubscribeFn):
        """El scanner llama a esto tras crear el executor, para darle forma de
        pedir (des)suscripciones a trades sin acoplarse al WebSocket."""
        self._subscribe_fn = subscribe_fn
        self._unsubscribe_fn = unsubscribe_fn

    def has_open_position(self, mint: str) -> bool:
        return mint in self.open_positions

    @abstractmethod
    async def open_position(self, candidate) -> bool:
        """Intenta abrir posición en el candidato. Devuelve True si se abrió."""
        raise NotImplementedError

    @abstractmethod
    def handle_trade_message(self, msg: dict):
        """Se llama para CADA mensaje de trade que llega por WebSocket (el
        scanner no filtra por ti); el executor decide si le interesa ese mint."""
        raise NotImplementedError

    def print_open_positions_summary(self):
        if not self.open_positions:
            return
        print(f"\n{'='*80}\nPOSICIONES ABIERTAS AL PARAR ({len(self.open_positions)})\n{'='*80}")
        for p in self.open_positions.values():
            pnl_pct = p.pnl_pct()
            pnl_str = f"P&L: {pnl_pct:+.1f}%" if pnl_pct is not None else "precio desconocido"
            print(f"  {p.symbol:<10}  {pnl_str}  https://pump.fun/{p.mint}")
        print(f"{'='*80}\n")


class SimulatedTradeExecutor(TradeExecutor):
    """Papertrading: simula la compra/venta sobre precios reales de la
    bonding curve, sin mover dinero. TP/SL/gracia/timeout configurables."""

    def __init__(self, config: Config):
        super().__init__(config)
        self._ensure_positions_csv_header()

    def _ensure_positions_csv_header(self):
        if not os.path.exists(self.cfg.positions_csv_path):
            with open(self.cfg.positions_csv_path, "w", newline="") as f:
                csv.writer(f).writerow([
                    "entry_time", "exit_time", "hold_seconds", "mint", "symbol",
                    "entry_price_sol", "exit_price_sol", "pnl_pct", "pnl_sol",
                    "exit_reason", "pumpfun_url",
                ])

    def _append_position_csv(self, p: Position, exit_price: float,
                              pnl_pct: float, pnl_sol: float, exit_reason: str):
        with open(self.cfg.positions_csv_path, "a", newline="") as f:
            csv.writer(f).writerow([
                int(p.entry_time), int(time.time()), int(time.time() - p.entry_time),
                p.mint, p.symbol, p.entry_price, exit_price,
                round(pnl_pct, 2), round(pnl_sol, 5), exit_reason,
                f"https://pump.fun/{p.mint}",
            ])

    async def open_position(self, candidate) -> bool:
        price = candidate.current_price
        if not price:
            print(f"[SIM] No se pudo abrir posición en {candidate.symbol}: precio desconocido.")
            return False

        cfg = self.cfg
        position = Position(
            mint=candidate.mint, symbol=candidate.symbol, entry_price=price,
            amount_sol=cfg.buy_amount_sol, position_tokens=cfg.buy_amount_sol / price,
            v_sol_in_curve=candidate.v_sol_in_curve, v_tokens_in_curve=candidate.v_tokens_in_curve,
        )
        self.open_positions[candidate.mint] = position
        print(f"[SIM COMPRA] {position.symbol} ({position.mint[:10]}...) — {cfg.buy_amount_sol} SOL @ "
              f"{price:.10f} SOL/token → TP +{cfg.take_profit_pct}% / SL -{cfg.stop_loss_pct}% "
              f"(gracia {cfg.grace_period_seconds}s)")

        # la posición ya estaba suscrita a trades desde el scanner (para poder
        # evaluarla), así que no hace falta volver a suscribirse aquí
        return True

    def handle_trade_message(self, msg: dict):
        mint = msg.get("mint")
        position = self.open_positions.get(mint)
        if not position:
            return  # este trade no es de ninguna posición abierta, ignorar

        position.update_from_trade(msg)
        reason = self._check_exit_conditions(position)
        if reason:
            asyncio.create_task(self._close_position(mint, reason))
        else:
            position.trades_since_open += 1
            if position.trades_since_open % self.cfg.position_status_every_n_trades == 0:
                pnl_pct = position.pnl_pct()
                if pnl_pct is not None:
                    print(f"[SIM ESTADO] {position.symbol} sigue abierta — "
                          f"P&L actual: {pnl_pct:+.1f}% ({time.time() - position.entry_time:.0f}s)")

    def _check_exit_conditions(self, p: Position) -> Optional[str]:
        cfg = self.cfg
        price = p.current_price
        if not price:
            return None

        elapsed = time.time() - p.entry_time
        pnl_pct = p.pnl_pct()

        if elapsed < cfg.grace_period_seconds:
            if elapsed >= cfg.max_hold_seconds:
                return f"timeout {cfg.max_hold_seconds}s ({pnl_pct:+.1f}%)"
            return None

        if pnl_pct >= cfg.take_profit_pct:
            return f"take-profit +{pnl_pct:.1f}%"
        if pnl_pct <= -cfg.stop_loss_pct:
            return f"stop-loss {pnl_pct:.1f}%"
        if elapsed >= cfg.max_hold_seconds:
            return f"timeout {cfg.max_hold_seconds}s ({pnl_pct:+.1f}%)"
        return None

    async def _close_position(self, mint: str, exit_reason: str):
        p = self.open_positions.pop(mint, None)
        if not p:
            return
        price = p.current_price or p.entry_price
        pnl_pct = ((price - p.entry_price) / p.entry_price) * 100
        pnl_sol = self.cfg.buy_amount_sol * (pnl_pct / 100)

        signo = "GANANCIA" if pnl_sol >= 0 else "PÉRDIDA"
        print(f"[SIM VENTA] {p.symbol} ({p.mint[:10]}...) — {exit_reason} — "
              f"{signo}: {pnl_pct:+.1f}% ({pnl_sol:+.5f} SOL) — mantenido {int(time.time() - p.entry_time)}s")

        self._append_position_csv(p, price, pnl_pct, pnl_sol, exit_reason)
        if self._unsubscribe_fn:
            await self._unsubscribe_fn(mint)


class RealTradeExecutor(TradeExecutor):
    """
    ESQUELETO para ejecución REAL vía la Trading API de PumpPortal
    (POST https://pumpportal.fun/api/trade-local o /api/trade, según el
    modo Lightning/Local que elijas — revisa la doc antes de implementar).

    DELIBERADAMENTE NO ENVÍA TRANSACCIONES TODAVÍA. Antes de completar los
    TODO de abajo, decide y ten resueltas estas cosas:

      - Cómo cargas la clave privada de forma segura (variable de entorno,
        vault, etc.) — NUNCA hardcodeada en el script ni subida a git.
      - Slippage máximo aceptable y priority fee a usar en cada compra/venta.
        En pump.fun el movimiento de precio es muy rápido; sin slippage
        bien calibrado, tus transacciones pueden fallar constantemente
        o ejecutarse mucho peor de lo esperado.
      - Simulación previa de cada transacción (para detectar honeypots
        antes de firmarla de verdad).
      - Manejo de errores de red / transacción fallida sin duplicar compras.
      - Límite de capital total en riesgo simultáneo (no solo por posición).

    Recomendación: corre SimulatedTradeExecutor un tiempo largo primero y
    valida que positions_log.csv da un resultado que te convence antes de
    siquiera empezar a rellenar esta clase.
    """

    def __init__(self, config: Config, wallet_private_key: Optional[str] = None):
        super().__init__(config)
        self.wallet_private_key = wallet_private_key
        if not wallet_private_key:
            print("[REAL] Aviso: no se ha configurado wallet_private_key. "
                  "open_position() fallará hasta que la configures.")

    async def open_position(self, candidate) -> bool:
        if not self.wallet_private_key:
            print(f"[REAL] Compra de {candidate.symbol} abortada: falta wallet_private_key.")
            return False

        # TODO: aquí iría la llamada real, algo en la línea de:
        #
        # response = requests.post("https://pumpportal.fun/api/trade-local", data={
        #     "publicKey": TU_WALLET_PUBLICA,
        #     "action": "buy",
        #     "mint": candidate.mint,
        #     "amount": self.cfg.buy_amount_sol,
        #     "denominatedInSol": "true",
        #     "slippage": 10,           # % — ajusta según tu tolerancia real
        #     "priorityFee": 0.0001,    # SOL — ajusta según congestión de red
        #     "pool": "pump",
        # })
        # -> firmar la transacción devuelta con solders usando wallet_private_key
        # -> enviarla al RPC y esperar confirmación
        # -> solo si confirma, crear el Position real de verdad
        #
        # Hasta que implementes esto, lanzamos un error explícito en vez de
        # fingir que compramos algo.
        raise NotImplementedError(
            "RealTradeExecutor.open_position() no está implementado todavía. "
            "Revisa los TODO en esta clase antes de usarla con dinero real."
        )

    def handle_trade_message(self, msg: dict):
        # El tracking de precio en vivo (para saber cuándo tocar TP/SL) puede
        # reutilizar exactamente la misma lógica que SimulatedTradeExecutor
        # una vez tengas Position rellenándose con datos reales; lo que
        # cambia es SOLO cómo se ejecuta el cierre real (ver _close_position).
        raise NotImplementedError

    async def _close_position(self, mint: str, exit_reason: str):
        # TODO: mismo patrón que open_position pero con action="sell"
        raise NotImplementedError