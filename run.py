"""
pumpfun_filter.py
------------------
Arquitectura en dos capas separadas:

  1. PumpFunScanner   -> WebSocket de PumpPortal, detección de tokens nuevos,
                          filtro de seguridad y momentum. No sabe comprar ni
                          vender: cuando un candidato pasa el filtro, se lo
                          entrega a un TradeExecutor.

  2. TradeExecutor     -> interfaz abstracta para "qué hacer" con un candidato
                          que pasó el filtro: abrir posición, trackear precio,
                          cerrar por TP/SL/timeout.
       - SimulatedTradeExecutor: papertrading real sobre precios reales,
         sin mover dinero. Es el que se usa por defecto.
       - RealTradeExecutor: ESQUELETO preparado para ejecutar en real más
         adelante vía la Trading API de PumpPortal. Deliberadamente NO
         implementa el envío de transacciones reales todavía (ver comentarios
         TODO) — eso requiere que decidas tú cómo gestionas la clave privada,
         el slippage y las confirmaciones antes de arriesgar dinero.

ESTE SCRIPT, TAL CUAL VIENE CONFIGURADO, NO EJECUTA NINGUNA COMPRA/VENTA
REAL. Usa SimulatedTradeExecutor por defecto.

Requisitos:
    pip install websockets requests

Configuración necesaria (ver clase Config):
  1. pumpportal_api_key -> gratis en https://pumpportal.fun/trading-api/setup
  2. solana_rpc_url -> usa un RPC decente (Helius/QuickNode)
"""

import asyncio
import csv
import json
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Callable, Awaitable

import requests
import websockets


# ======================================================================
# CONFIGURACIÓN
# ======================================================================

@dataclass
class Config:
    pumpportal_api_key: str = "5wvpcmbe9xvpyka89nw4wv3pctb6jj3caxw4gc3h8tak4ya9d925anahcth4yykp9nk70jah9dw6cdhh9cv5mcbcaxu4pvtq65h4jtbuahcmrykcdha7cdvt6566ah3d6hak6p2a84yku99j70xkk9x5n4jjg6x2jyckh9r8djmygbn8d0q2dahehmmye279hx4cuat9tkkuf8"
    solana_rpc_url: str = "https://mainnet.helius-rpc.com/?api-key=6db3c142-52bf-418e-a97c-ae7462bc5292"  # cámbialo por Helius/QuickNode

    # --- ventanas de tiempo (filtro / scanner) ---
    observation_window_seconds: int = 60   # límite MÁXIMO antes de descartar (fallback)
    early_trigger_min_seconds: int = 10    # no compra antes de esto (necesita algo de dato)

    # --- umbrales de filtrado (scanner) ---
    max_dev_holding_pct: float = 8.0
    min_unique_buyers: int = 10
    min_buy_sell_ratio: float = 1.5
    min_bonding_curve_progress: float = 1.0
    max_bonding_curve_progress: float = 70.0

    # --- ejecución de trades (executor) ---
    buy_amount_sol: float = 0.01
    take_profit_pct: float = 15.0
    stop_loss_pct: float = 15.0
    grace_period_seconds: int = 1    # tras comprar, ignora TP/SL este tiempo
    max_hold_seconds: int = 3600      # cierre de seguridad si nunca toca TP/SL
    position_status_every_n_trades: int = 5

    # --- constantes del propio pump.fun ---
    total_supply: float = 1_000_000_000
    migration_sol_threshold: float = 85.0  # SOL acumulado al que migra a Raydium/PumpSwap

    # --- logging / debug ---
    verbose: bool = True
    debug_raw: bool = False
    candidates_csv_path: str = "candidates_log.csv"
    positions_csv_path: str = "positions_log.csv"

    @property
    def pumpportal_ws_url(self) -> str:
        return f"wss://pumpportal.fun/api/data?api-key={self.pumpportal_api_key}"


# ======================================================================
# MODELOS DE DATOS
# ======================================================================

@dataclass
class TokenCandidate:
    """Representa un token mientras el SCANNER lo está evaluando."""
    mint: str
    name: str
    symbol: str
    dev_wallet: str
    created_at: float = field(default_factory=time.time)

    bonding_curve_progress: float = 0.0
    dev_initial_buy_pct: float = 0.0
    mint_authority_revoked: Optional[bool] = None
    freeze_authority_revoked: Optional[bool] = None
    buyers: set = field(default_factory=set)
    buy_count: int = 0
    sell_count: int = 0

    score: float = 0.0
    reasons_rejected: list = field(default_factory=list)
    passed: bool = False
    finalize_triggered: bool = False

    v_sol_in_curve: float = 0.0
    v_tokens_in_curve: float = 0.0

    @property
    def current_price(self) -> Optional[float]:
        if self.v_tokens_in_curve <= 0:
            return None
        return self.v_sol_in_curve / self.v_tokens_in_curve

    @property
    def buy_sell_ratio(self) -> float:
        return (self.buy_count / self.sell_count) if self.sell_count > 0 else float("inf")

    def update_from_trade(self, msg: dict):
        if msg.get("txType") == "buy":
            self.buy_count += 1
            buyer = msg.get("traderPublicKey")
            if buyer:
                self.buyers.add(buyer)
        else:
            self.sell_count += 1
        v_sol = msg.get("vSolInBondingCurve")
        v_tok = msg.get("vTokensInBondingCurve")
        if v_sol is not None:
            self.v_sol_in_curve = float(v_sol)
        if v_tok is not None:
            self.v_tokens_in_curve = float(v_tok)


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
    async def open_position(self, candidate: TokenCandidate) -> bool:
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

    async def open_position(self, candidate: TokenCandidate) -> bool:
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

    async def open_position(self, candidate: TokenCandidate) -> bool:
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


# ======================================================================
# SCANNER: WebSocket, detección, filtro (no sabe nada de comprar/vender)
# ======================================================================

class PumpFunScanner:
    """
    Detecta tokens nuevos en pump.fun, los evalúa contra criterios de
    seguridad y momentum, y delega en un TradeExecutor la apertura de
    posición cuando un candidato pasa el filtro.
    """

    def __init__(self, config: Config, executor: TradeExecutor):
        self.cfg = config
        self.executor = executor
        self.executor.bind_subscription_callbacks(self._subscribe_trade, self._unsubscribe_trade)

        self.tracked_candidates: dict[str, TokenCandidate] = {}
        self.candidate_shortlist: list[TokenCandidate] = []
        self._warned_missing_curve_fields = False
        self._ws = None

        self._ensure_candidates_csv_header()

    # ------------------------------------------------------------------
    # RPC de Solana (seguridad: mint/freeze authority)
    # ------------------------------------------------------------------

    def _rpc_call_with_retry(self, method: str, params: list,
                              max_retries: int = 5, delay: float = 2.0) -> dict:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        last_result = {}
        for _ in range(max_retries):
            try:
                resp = requests.post(self.cfg.solana_rpc_url, json=payload, timeout=8)
                data = resp.json()
                if "error" in data or data.get("result", {}).get("value") is None:
                    last_result = data
                    time.sleep(delay)
                    continue
                return data
            except Exception as e:
                last_result = {"error": f"{type(e).__name__}: {e}"}
                time.sleep(delay)
        return last_result

    def _get_mint_authorities(self, mint: str) -> dict:
        data = self._rpc_call_with_retry(
            "getAccountInfo", [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}]
        )
        if "error" in data or not data.get("result", {}).get("value"):
            print(f"[RPC] getAccountInfo({mint[:10]}...) sin resultado: {data.get('error', 'value null')}")
            return {"mint_authority": "unknown", "freeze_authority": "unknown"}
        info = data["result"]["value"]["data"]["parsed"]["info"]
        return {"mint_authority": info.get("mintAuthority"), "freeze_authority": info.get("freezeAuthority")}

    def _run_security_check(self, candidate: TokenCandidate):
        auth = self._get_mint_authorities(candidate.mint)
        candidate.mint_authority_revoked = auth["mint_authority"] is None
        candidate.freeze_authority_revoked = auth["freeze_authority"] is None

    # ------------------------------------------------------------------
    # Evaluación / scoring
    # ------------------------------------------------------------------

    def _evaluate_candidate(self, c: TokenCandidate):
        cfg = self.cfg
        c.reasons_rejected.clear()

        if not c.mint_authority_revoked:
            c.reasons_rejected.append("mint authority no revocada")
        if not c.freeze_authority_revoked:
            c.reasons_rejected.append("freeze authority no revocada")
        if c.dev_initial_buy_pct > cfg.max_dev_holding_pct:
            c.reasons_rejected.append(f"dev compró {c.dev_initial_buy_pct:.1f}% del supply al crear")
        if len(c.buyers) < cfg.min_unique_buyers:
            c.reasons_rejected.append(f"solo {len(c.buyers)} compradores únicos")

        ratio = c.buy_sell_ratio
        if ratio < cfg.min_buy_sell_ratio:
            c.reasons_rejected.append(f"ratio compra/venta {ratio:.2f}")
        if not (cfg.min_bonding_curve_progress <= c.bonding_curve_progress <= cfg.max_bonding_curve_progress):
            c.reasons_rejected.append(f"progreso curva {c.bonding_curve_progress:.1f}%")

        c.passed = len(c.reasons_rejected) == 0
        if not c.passed and any("authority" in r for r in c.reasons_rejected):
            c.score = 0.0
            return

        score = 100.0
        score -= max(0, c.dev_initial_buy_pct - cfg.max_dev_holding_pct) * 3
        score += min(len(c.buyers), 100) * 0.3
        score += min(ratio, 5) * 5
        c.score = max(0.0, round(score, 1))

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def _ensure_candidates_csv_header(self):
        if not os.path.exists(self.cfg.candidates_csv_path):
            with open(self.cfg.candidates_csv_path, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "mint", "symbol", "name", "score", "passed",
                    "dev_initial_buy_pct", "unique_buyers", "buy_count", "sell_count",
                    "bonding_curve_progress", "reasons_rejected", "pumpfun_url",
                ])

    def _append_candidate_csv(self, c: TokenCandidate):
        with open(self.cfg.candidates_csv_path, "a", newline="") as f:
            csv.writer(f).writerow([
                int(time.time()), c.mint, c.symbol, c.name, c.score, c.passed,
                round(c.dev_initial_buy_pct, 1), len(c.buyers), c.buy_count, c.sell_count,
                round(c.bonding_curve_progress, 1), "; ".join(c.reasons_rejected),
                f"https://pump.fun/{c.mint}",
            ])

    def _print_candidate_row(self, c: TokenCandidate):
        if not c.passed and not self.cfg.verbose:
            return
        estado = "CANDIDATO ✔" if c.passed else "descartado"
        print(f"[{c.symbol:<10}] score={c.score:<6} buyers={len(c.buyers):<4} "
              f"buy/sell={c.buy_count}/{c.sell_count:<3} curve={c.bonding_curve_progress:>5.1f}%  "
              f"{estado}  https://pump.fun/{c.mint}")
        if not c.passed:
            print(f"    motivo: {'; '.join(c.reasons_rejected)}")

    def print_shortlist(self, force: bool = False):
        if not self.cfg.verbose and not force:
            return
        passed = [c for c in self.candidate_shortlist if c.passed]
        if not passed:
            print("\n(sin candidatos que pasen el filtro todavía)\n")
            return
        passed.sort(key=lambda c: c.score, reverse=True)
        print(f"\n{'='*80}\nCANDIDATOS ({len(passed)})\n{'='*80}")
        for c in passed:
            print(f"  {c.score:>6.1f}  {c.symbol:<10}  https://pump.fun/{c.mint}")
        print(f"{'='*80}\n")

    # ------------------------------------------------------------------
    # Suscripciones WebSocket (usadas por el executor vía callback)
    # ------------------------------------------------------------------

    async def _subscribe_trade(self, mint: str):
        await self._ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))

    async def _unsubscribe_trade(self, mint: str):
        await self._ws.send(json.dumps({"method": "unsubscribeTokenTrade", "keys": [mint]}))

    # ------------------------------------------------------------------
    # Ciclo de vida de un candidato
    # ------------------------------------------------------------------

    async def _finalize_candidate(self, mint: str, trigger_reason: str = ""):
        candidate = self.tracked_candidates.pop(mint, None)
        if not candidate:
            return

        self._evaluate_candidate(candidate)
        if self.cfg.verbose and trigger_reason:
            print(f"[TRIGGER] {candidate.symbol}: {trigger_reason}")
        self._print_candidate_row(candidate)
        self._append_candidate_csv(candidate)
        self.candidate_shortlist.append(candidate)
        self.print_shortlist()

        if candidate.passed:
            opened = await self.executor.open_position(candidate)
            if not opened:
                await self._unsubscribe_trade(mint)
        else:
            await self._unsubscribe_trade(mint)

    async def _finalize_after_window(self, mint: str):
        await asyncio.sleep(self.cfg.observation_window_seconds)
        await self._finalize_candidate(
            mint, trigger_reason=f"límite de {self.cfg.observation_window_seconds}s alcanzado"
        )

    async def _handle_new_token(self, msg: dict):
        mint = msg.get("mint")
        if not mint:
            return
        candidate = TokenCandidate(
            mint=mint, name=msg.get("name", "?"), symbol=msg.get("symbol", "?"),
            dev_wallet=msg.get("traderPublicKey", ""),
        )

        initial_buy = float(msg.get("initialBuy", 0) or 0)
        candidate.dev_initial_buy_pct = (initial_buy / self.cfg.total_supply) * 100

        v_sol = msg.get("vSolInBondingCurve")
        if v_sol is not None:
            candidate.bonding_curve_progress = min(100.0, (float(v_sol) / self.cfg.migration_sol_threshold) * 100)

        self.tracked_candidates[mint] = candidate
        if self.cfg.verbose:
            print(f"[NUEVO] {candidate.symbol} ({mint[:10]}...) detectado...")

        self._run_security_check(candidate)  # bloqueante (RPC síncrono)

        await self._subscribe_trade(mint)
        asyncio.create_task(self._finalize_after_window(mint))

    def _handle_token_trade(self, msg: dict):
        mint = msg.get("mint")

        if "vSolInBondingCurve" not in msg or "vTokensInBondingCurve" not in msg:
            if not self._warned_missing_curve_fields:
                self._warned_missing_curve_fields = True
                print(f"[DEBUG] Trade sin campos de bonding curve. Claves: {list(msg.keys())}")

        # 1) candidato aún en ventana de observación -> actualiza filtro
        candidate = self.tracked_candidates.get(mint)
        if candidate:
            candidate.update_from_trade(msg)
            v_sol = msg.get("vSolInBondingCurve")
            if v_sol is not None:
                candidate.bonding_curve_progress = min(
                    100.0, (float(v_sol) / self.cfg.migration_sol_threshold) * 100
                )

            elapsed = time.time() - candidate.created_at
            if not candidate.finalize_triggered and elapsed >= self.cfg.early_trigger_min_seconds:
                self._evaluate_candidate(candidate)  # evaluación provisional
                if candidate.passed:
                    candidate.finalize_triggered = True
                    asyncio.create_task(
                        self._finalize_candidate(mint, trigger_reason=f"disparo anticipado a los {elapsed:.0f}s")
                    )

        # 2) reenvía SIEMPRE al executor; él decide si le interesa ese mint
        #    (es el desacoplamiento clave: el scanner no sabe qué posiciones
        #    tiene abiertas el executor, ni falta que le hace)
        self.executor.handle_trade_message(msg)

    # ------------------------------------------------------------------
    # Bucle principal
    # ------------------------------------------------------------------

    async def run(self):
        print("Conectando a PumpPortal...")
        async with websockets.connect(self.cfg.pumpportal_ws_url) as ws:
            self._ws = ws
            if self.cfg.verbose:
                print("[OK] Conexión WebSocket abierta.")

            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            print(f"Suscrito a nuevos tokens. Ventana máxima: {self.cfg.observation_window_seconds}s, "
                  f"disparo anticipado desde {self.cfg.early_trigger_min_seconds}s. "
                  f"Log en {self.cfg.candidates_csv_path}\n")

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                if self.cfg.debug_raw:
                    print(f"[RAW] {json.dumps(msg)[:300]}")

                if "mint" in msg and msg.get("txType") == "create":
                    await self._handle_new_token(msg)
                elif msg.get("txType") in ("buy", "sell"):
                    self._handle_token_trade(msg)
                elif "message" in msg and self.cfg.verbose:
                    print(f"[SERVIDOR] {msg.get('message')}")


# ======================================================================
# PUNTO DE ENTRADA
# ======================================================================

if __name__ == "__main__":
    config = Config(
        pumpportal_api_key="5wvpcmbe9xvpyka89nw4wv3pctb6jj3caxw4gc3h8tak4ya9d925anahcth4yykp9nk70jah9dw6cdhh9cv5mcbcaxu4pvtq65h4jtbuahcmrykcdha7cdvt6566ah3d6hak6p2a84yku99j70xkk9x5n4jjg6x2jyckh9r8djmygbn8d0q2dahehmmye279hx4cuat9tkkuf8",   # <-- sustituye por tu key real
        solana_rpc_url="https://mainnet.helius-rpc.com/?api-key=6db3c142-52bf-418e-a97c-ae7462bc5292",  # <-- sustituye por Helius/QuickNode
        verbose=False,
    )

    print("=" * 70)
    print("SIMULACIÓN (PAPERTRADING) — NO SE EJECUTA NINGUNA COMPRA/VENTA REAL.")
    print(f"Posición simulada: {config.buy_amount_sol} SOL | TP +{config.take_profit_pct}% | "
          f"SL -{config.stop_loss_pct}% | gracia {config.grace_period_seconds}s | "
          f"timeout {config.max_hold_seconds}s")
    print("Riesgo extremo de pérdida total si en el futuro conectas esto a dinero real.")
    print("=" * 70 + "\n")

    # Cambia esta línea por RealTradeExecutor(config, wallet_private_key=...)
    # el día que hayas completado los TODO de esa clase y estés listo para
    # operar con dinero real. Hasta entonces, todo es papertrading.
    executor = SimulatedTradeExecutor(config)
    scanner = PumpFunScanner(config, executor)

    try:
        asyncio.run(scanner.run())
    except KeyboardInterrupt:
        print("\nDetenido por el usuario.")
        scanner.print_shortlist(force=True)
        executor.print_open_positions_summary()