from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Callable, Awaitable
import time
import asyncio
import base64
import csv
import logging

import aiohttp

from pumpbot.config import Config

logger = logging.getLogger(__name__)


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
    buy_tx_signature: str = ""  # solo RealTradeExecutor: firma de la tx de COMPRA,
                                 # para poder auditar entrada y salida por separado en Solscan

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
        # Mints con un cierre de posición YA en curso (tarea de venta enviada,
        # esperando confirmación). Evita que ráfagas de mensajes de trade
        # que llegan mientras esa venta sigue "en vuelo" disparen ventas
        # duplicadas concurrentes para el mismo mint — ver handle_trade_message
        # de cada subclase y su uso de este set.
        self._closing_mints: set[str] = set()

    def bind_subscription_callbacks(self, subscribe_fn: SubscribeFn, unsubscribe_fn: SubscribeFn):
        """El scanner llama a esto tras crear el executor, para darle forma de
        pedir (des)suscripciones a trades sin acoplarse al WebSocket."""
        self._subscribe_fn = subscribe_fn
        self._unsubscribe_fn = unsubscribe_fn

    def has_open_position(self, mint: str) -> bool:
        return mint in self.open_positions

    def _check_exposure_limits(self) -> Optional[str]:
        """Comprueba si abrir UNA posición más de tamaño cfg.buy_amount_sol
        violaría los límites de riesgo configurados. Devuelve el motivo de
        rechazo como str, o None si es seguro abrir.

        Se comprueba en la clase base (no en cada subclase) para que tanto
        SimulatedTradeExecutor como RealTradeExecutor respeten SIEMPRE estos
        límites sin poder olvidarse de llamarlo — real y simulado comparten
        la misma disciplina de riesgo."""
        cfg = self.cfg
        if len(self.open_positions) >= cfg.max_concurrent_positions:
            return (f"límite de posiciones concurrentes alcanzado "
                     f"({len(self.open_positions)}/{cfg.max_concurrent_positions})")

        current_exposure = sum(p.amount_sol for p in self.open_positions.values())
        projected = current_exposure + cfg.buy_amount_sol
        if projected > cfg.max_total_exposure_sol:
            return (f"excedería exposición máxima "
                     f"({current_exposure:.4f} + {cfg.buy_amount_sol} = {projected:.4f} "
                     f"> {cfg.max_total_exposure_sol} SOL)")
        return None

    @abstractmethod
    async def open_position(self, candidate) -> bool:
        """Intenta abrir posición en el candidato. Devuelve True si se abrió."""
        raise NotImplementedError

    @abstractmethod
    def handle_trade_message(self, msg: dict):
        """Se llama para CADA mensaje de trade que llega por WebSocket (el
        scanner no filtra por ti); el executor decide si le interesa ese mint."""
        raise NotImplementedError

    async def force_close_all_positions(self, reason: str):
        """Cierra TODAS las posiciones abiertas ya mismo, al último precio
        conocido de cada una (sin esperar a que toquen TP/SL/timeout).
        Pensada para un apagado forzado (segunda señal Ctrl+C).

        Reutiliza `_close_position` de la subclase concreta, así que el
        registro en el CSV y los logs de venta son exactamente los mismos
        que en un cierre normal — solo cambia el motivo."""
        for mint in list(self.open_positions.keys()):
            try:
                await self._close_position(mint, reason)
            except NotImplementedError:
                logger.error(f"No se puede vender automáticamente la posición en {mint}: "
                             f"la venta real no está implementada todavía. "
                             f"Ciérrala manualmente: https://pump.fun/{mint}")
            except Exception as e:
                logger.error(f"Error al forzar el cierre de la posición {mint}: {e}")

    def print_open_positions_summary(self):
        if not self.open_positions:
            return
        lines = [f"{'='*80}", f"POSICIONES ABIERTAS AL PARAR ({len(self.open_positions)})", f"{'='*80}"]
        for p in self.open_positions.values():
            pnl_pct = p.pnl_pct()
            pnl_str = f"P&L: {pnl_pct:+.1f}%" if pnl_pct is not None else "precio desconocido"
            lines.append(f"  {p.symbol:<10}  {pnl_str}  https://pump.fun/{p.mint}")
        lines.append("=" * 80)
        logger.info("\n" + "\n".join(lines))

    # ------------------------------------------------------------------
    # Lógica compartida entre Simulated y Real: condiciones de salida
    # (TP/SL/timeout) y registro en CSV. Vive aquí para que AMBOS
    # executors respeten exactamente las mismas reglas — solo cambia
    # CÓMO se ejecuta la compra/venta en sí (ver cada subclase).
    # ------------------------------------------------------------------

    def _check_exit_conditions(self, p: Position) -> Optional[str]:
        cfg = self.cfg
        price = p.current_price
        if not price:
            return None

        elapsed = time.time() - p.entry_time
        pnl_pct = p.pnl_pct()

        # TP y SL tienen su propia ventana de gracia, independiente entre sí:
        # puedes, por ejemplo, dejar el SL activo casi desde el minuto uno
        # (para cortar rug pulls rápido) mientras le das más margen al TP
        # (para no vender demasiado pronto una subida que sigue corriendo).
        if elapsed >= cfg.take_profit_grace_period_seconds and pnl_pct >= cfg.take_profit_pct:
            return f"take-profit +{pnl_pct:.1f}%"
        if elapsed >= cfg.stop_loss_grace_period_seconds and pnl_pct <= -cfg.stop_loss_pct:
            return f"stop-loss {pnl_pct:.1f}%"
        if elapsed >= cfg.max_hold_seconds:
            return f"timeout {cfg.max_hold_seconds}s ({pnl_pct:+.1f}%)"
        return None

    def _ensure_positions_csv_header(self):
        with open(self.cfg.positions_csv_path, "w", newline="") as f:
            csv.writer(f).writerow([
                "entry_time", "exit_time", "hold_seconds", "mint", "symbol",
                "entry_price_sol", "exit_price_sol", "pnl_pct", "pnl_sol",
                "exit_reason", "buy_tx_signature", "sell_tx_signature", "pumpfun_url",
            ])

    def _append_position_csv(self, p: Position, exit_price: float, pnl_pct: float,
                              pnl_sol: float, exit_reason: str, tx_signature: str = ""):
        with open(self.cfg.positions_csv_path, "a", newline="") as f:
            csv.writer(f).writerow([
                int(p.entry_time), int(time.time()), int(time.time() - p.entry_time),
                p.mint, p.symbol, p.entry_price, exit_price,
                round(pnl_pct, 2), round(pnl_sol, 5), exit_reason,
                p.buy_tx_signature, tx_signature,
                f"https://pump.fun/{p.mint}",
            ])


class SimulatedTradeExecutor(TradeExecutor):
    """Papertrading: simula la compra/venta sobre precios reales de la
    bonding curve, sin mover dinero. TP/SL/gracia/timeout configurables."""

    def __init__(self, config: Config):
        super().__init__(config)
        self._ensure_positions_csv_header()

    async def open_position(self, candidate) -> bool:
        rejection = self._check_exposure_limits()
        if rejection:
            logger.info(f"[SIM] Compra de {candidate.symbol} descartada: {rejection}.")
            return False

        price = candidate.current_price
        if not price:
            logger.warning(f"[SIM] No se pudo abrir posición en {candidate.symbol}: precio desconocido.")
            return False

        cfg = self.cfg
        position = Position(
            mint=candidate.mint, symbol=candidate.symbol, entry_price=price,
            amount_sol=cfg.buy_amount_sol, position_tokens=cfg.buy_amount_sol / price,
            v_sol_in_curve=candidate.v_sol_in_curve, v_tokens_in_curve=candidate.v_tokens_in_curve,
        )
        self.open_positions[candidate.mint] = position
        logger.info(f"[SIM COMPRA] {position.symbol} ({position.mint[:10]}...) — {cfg.buy_amount_sol} SOL @ "
                    f"{price:.10f} SOL/token → TP +{cfg.take_profit_pct}% (gracia {cfg.take_profit_grace_period_seconds}s) "
                    f"/ SL -{cfg.stop_loss_pct}% (gracia {cfg.stop_loss_grace_period_seconds}s)")

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
                    logger.info(f"[SIM ESTADO] {position.symbol} sigue abierta — "
                                f"P&L actual: {pnl_pct:+.1f}% ({time.time() - position.entry_time:.0f}s)")

    async def _close_position(self, mint: str, exit_reason: str):
        p = self.open_positions.pop(mint, None)
        if not p:
            return
        price = p.current_price or p.entry_price
        pnl_pct = ((price - p.entry_price) / p.entry_price) * 100
        pnl_sol = self.cfg.buy_amount_sol * (pnl_pct / 100)

        signo = "GANANCIA" if pnl_sol >= 0 else "PÉRDIDA"
        logger.info(f"[SIM VENTA] {p.symbol} ({p.mint[:10]}...) — {exit_reason} — "
                    f"{signo}: {pnl_pct:+.1f}% ({pnl_sol:+.5f} SOL) — mantenido {int(time.time() - p.entry_time)}s")

        self._append_position_csv(p, price, pnl_pct, pnl_sol, exit_reason)
        if self._unsubscribe_fn:
            await self._unsubscribe_fn(mint)


class _OnchainRealExecutorBase(TradeExecutor):
    """
    Base común para los DOS modos de ejecución REAL contra PumpPortal:
    RealTradeExecutor (Local Transaction API, firmas tú mismo) y
    LightningTradeExecutor (Lightning API, firma y envía PumpPortal por ti).

    Todo lo que NO depende de cómo se firma/envía la transacción vive aquí:
    sesión HTTP, llamadas RPC a tu propio nodo (para confirmar y leer el
    fill real), el candado anti-ventas-duplicadas, y el flujo completo de
    open_position / handle_trade_message / _close_position. Cada subclase
    solo implementa `_execute_trade()` con su propio mecanismo de firma/envío
    y fija `self.wallet_public_key` (necesario para leer el fill real de
    cada tx vía `_get_trade_fill`, sin importar quién la firmó).
    """

    LOG_PREFIX = "[REAL]"  # cada subclase lo sobreescribe para distinguir logs

    def __init__(self, config: Config):
        super().__init__(config)
        self._ensure_positions_csv_header()
        self._http_session: Optional[aiohttp.ClientSession] = None
        self.wallet_public_key: Optional[str] = None  # cada subclase lo fija en su __init__

    # ------------------------------------------------------------------
    # Infraestructura HTTP / RPC (a TU RPC, cfg.solana_rpc_url — para
    # confirmar y leer el fill real, independientemente de quién firme)
    # ------------------------------------------------------------------

    async def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    async def close(self):
        """Cierra la sesión HTTP. Llamar al parar el bot."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

    async def _rpc_call(self, method: str, params: list, timeout: float = 15.0) -> dict:
        session = await self._get_http_session()
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        try:
            async with session.post(self.cfg.solana_rpc_url, json=payload,
                                     timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                return await resp.json()
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}"}

    async def _get_trade_fill(self, signature: str, mint: str) -> Optional[dict]:
        """Lee del RPC la transacción YA confirmada y calcula cuánto SOL y
        cuántos tokens se movieron REALMENTE en tu wallet, en vez de asumir
        el precio que se veía en el feed de WebSocket en ese instante.

        Esto importa porque el precio del feed y el precio de ejecución
        real pueden divergir bastante: hay varios segundos entre "decido
        operar" y "la tx confirma", y en ese margen el precio de un token
        recién lanzado puede moverse mucho — además del propio slippage
        permitido.

        Devuelve {"sol_delta": float, "token_delta": float} donde sol_delta
        es SOL que SALIÓ de tu wallet (positivo en una compra, negativo en
        una venta) y token_delta es cuánto token entró/salió (positivo en
        compra, negativo en venta). Devuelve None si no se pudo leer o
        parsear (el llamador debe entonces caer de vuelta al precio del
        feed como aproximación, dejándolo claro en el log)."""
        data = await self._rpc_call("getTransaction", [
            signature, {"encoding": "jsonParsed", "commitment": "confirmed",
                        "maxSupportedTransactionVersion": 0},
        ], timeout=15.0)
        result = data.get("result")
        if not result:
            return None
        try:
            meta = result["meta"]
            account_keys = result["transaction"]["message"]["accountKeys"]
            wallet_idx = next(
                i for i, a in enumerate(account_keys)
                if (a.get("pubkey") if isinstance(a, dict) else a) == self.wallet_public_key
            )
            # positivo = SOL que salió de la wallet (compra); negativo = SOL
            # que entró (venta). Incluye fee de red + priority fee + rent de
            # la cuenta de token (si es la primera vez que tocas ese mint) +
            # el propio swap — es decir, el coste/ingreso TOTAL, no solo el
            # del swap en sí.
            sol_delta = (meta["preBalances"][wallet_idx] - meta["postBalances"][wallet_idx]) / 1e9

            def _token_ui_amount(balances):
                for b in balances:
                    if b.get("owner") == self.wallet_public_key and b.get("mint") == mint:
                        ui = b.get("uiTokenAmount", {}).get("uiAmount")
                        return float(ui) if ui is not None else 0.0
                return 0.0

            pre_tokens = _token_ui_amount(meta.get("preTokenBalances") or [])
            post_tokens = _token_ui_amount(meta.get("postTokenBalances") or [])
            return {"sol_delta": sol_delta, "token_delta": post_tokens - pre_tokens}
        except (KeyError, IndexError, TypeError, StopIteration) as e:
            logger.warning(f"{self.LOG_PREFIX} No se pudo leer el detalle real de la tx "
                           f"{signature[:10]}...: {e}")
            return None

    # ------------------------------------------------------------------
    # Método a implementar por cada subclase: CÓMO se firma y envía.
    # ------------------------------------------------------------------

    async def _execute_trade(self, action: str, mint: str, amount,
                              denominated_in_sol: bool) -> Optional[str]:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Interfaz TradeExecutor — común a Local y Lightning
    # ------------------------------------------------------------------

    async def open_position(self, candidate) -> bool:
        rejection = self._check_exposure_limits()
        if rejection:
            logger.info(f"{self.LOG_PREFIX} Compra de {candidate.symbol} descartada: {rejection}.")
            return False

        if not self.wallet_public_key:
            logger.error(f"{self.LOG_PREFIX} Compra de {candidate.symbol} abortada: "
                         f"falta configurar la wallet.")
            return False

        price = candidate.current_price
        if not price:
            logger.warning(f"{self.LOG_PREFIX} No se pudo comprar {candidate.symbol}: precio desconocido.")
            return False

        cfg = self.cfg
        logger.info(f"{self.LOG_PREFIX} COMPRA: Enviando orden de compra: {candidate.symbol} "
                    f"({candidate.mint[:10]}...) — {cfg.buy_amount_sol} SOL...")
        signature = await self._execute_trade(
            "buy", candidate.mint, cfg.buy_amount_sol, denominated_in_sol=True
        )
        if not signature:
            logger.error(f"{self.LOG_PREFIX} Compra de {candidate.symbol} NO confirmada. No se abre posición.")
            return False

        # Intentamos leer de la propia transacción confirmada cuánto SOL
        # salió realmente y cuántos tokens entraron — así el precio de
        # entrada y el tamaño de la posición reflejan la ejecución real
        # (incluyendo slippage, fees y rent de la cuenta de token), no una
        # estimación previa a la compra.
        fill = await self._get_trade_fill(signature, candidate.mint)
        if fill and fill["token_delta"] > 0 and fill["sol_delta"] > 0:
            actual_sol_spent = fill["sol_delta"]
            actual_tokens = fill["token_delta"]
            actual_price = actual_sol_spent / actual_tokens
            fill_note = "medido on-chain"
        else:
            actual_sol_spent = cfg.buy_amount_sol
            actual_tokens = cfg.buy_amount_sol / price
            actual_price = price
            fill_note = "APROX. (no se pudo leer la tx — precio del feed)"

        position = Position(
            mint=candidate.mint, symbol=candidate.symbol, entry_price=actual_price,
            amount_sol=actual_sol_spent, position_tokens=actual_tokens,
            v_sol_in_curve=candidate.v_sol_in_curve, v_tokens_in_curve=candidate.v_tokens_in_curve,
            buy_tx_signature=signature,
        )
        self.open_positions[candidate.mint] = position
        logger.info(f"{self.LOG_PREFIX} COMPRA CONFIRMADA: {position.symbol} — entrada {fill_note}: "
                    f"{actual_price:.10f} SOL/token ({actual_sol_spent:.5f} SOL total) — "
                    f"https://solscan.io/tx/{signature} → TP +{cfg.take_profit_pct}% "
                    f"/ SL -{cfg.stop_loss_pct}%")
        return True

    def handle_trade_message(self, msg: dict):
        # Mismo tracking de precio en vivo que en modo simulado: solo cambia
        # CÓMO se ejecuta el cierre real (ver _close_position / _execute_trade).
        mint = msg.get("mint")
        position = self.open_positions.get(mint)
        if not position:
            return

        position.update_from_trade(msg)

        # Si ya hay una venta en curso para este mint (esperando confirmación
        # on-chain, lo cual puede tardar varios segundos), NO lances otra:
        # cada trade que llega mientras tanto seguiría viendo la posición
        # "abierta" y volvería a detectar la misma condición de salida,
        # inundando la red de ventas duplicadas (y pagando fee por cada
        # una, incluso las que fallan on-chain por "vender saldo cero").
        if mint in self._closing_mints:
            return

        reason = self._check_exit_conditions(position)
        if reason:
            self._closing_mints.add(mint)  # marcado ANTES de crear la tarea:
            # así ningún mensaje de trade posterior, por rápido que llegue,
            # puede colarse entre este punto y el arranque real de la tarea.
            asyncio.create_task(self._close_position(mint, reason))
        else:
            position.trades_since_open += 1
            if position.trades_since_open % self.cfg.position_status_every_n_trades == 0:
                pnl_pct = position.pnl_pct()
                if pnl_pct is not None:
                    logger.info(f"{self.LOG_PREFIX} ESTADO: {position.symbol} sigue abierta — "
                                f"P&L actual: {pnl_pct:+.1f}% ({time.time() - position.entry_time:.0f}s)")

    async def _close_position(self, mint: str, exit_reason: str):
        try:
            p = self.open_positions.get(mint)
            if not p:
                return

            logger.info(f"{self.LOG_PREFIX} VENTA: Enviando orden de venta: {p.symbol} "
                        f"({p.mint[:10]}...) — motivo: {exit_reason}...")
            # Vendemos el 100% del balance de tokens en vez de una cantidad
            # exacta: no trackeamos el balance real on-chain (puede diferir
            # ligeramente de position_tokens por redondeos/decimales del
            # token), así que "100%" es más robusto que arriesgarse a una
            # venta parcial por pedir de más.
            signature = await self._execute_trade("sell", mint, "100%", denominated_in_sol=False)

            if not signature:
                logger.error(f"{self.LOG_PREFIX} Venta de {p.symbol} NO confirmada. La posición SIGUE "
                             f"ABIERTA — se reintentará en el próximo trade que llegue de este mint, "
                             f"o ciérrala manualmente: https://pump.fun/{p.mint}")
                return

            # La posición se retira del tracking solo tras confirmar la venta
            # on-chain, para no perder de vista una posición que en realidad
            # sigue abierta si la venta falla.
            self.open_positions.pop(mint, None)

            # Igual que en la compra: intentamos leer de la tx confirmada
            # cuánto SOL entró realmente (no el precio del feed en el
            # instante en que se disparó el TP/SL, que para cuando la venta
            # confirma ya puede estar desactualizado varios segundos).
            fill = await self._get_trade_fill(signature, mint)
            if fill and fill["token_delta"] < 0 and fill["sol_delta"] < 0:
                sol_received = -fill["sol_delta"]
                pnl_sol = sol_received - p.amount_sol
                pnl_pct = (pnl_sol / p.amount_sol) * 100 if p.amount_sol else 0.0
                price = sol_received / (-fill["token_delta"])
                fill_note = "medido on-chain"
            else:
                price = p.current_price or p.entry_price
                pnl_pct = ((price - p.entry_price) / p.entry_price) * 100
                pnl_sol = p.amount_sol * (pnl_pct / 100)
                fill_note = "APROX. (no se pudo leer la tx — precio del feed)"

            signo = "GANANCIA" if pnl_sol >= 0 else "PÉRDIDA"
            logger.info(f"{self.LOG_PREFIX} VENTA CONFIRMADA: {p.symbol} ({p.mint[:10]}...) — "
                        f"{exit_reason} — salida {fill_note} — {signo}: {pnl_pct:+.1f}% "
                        f"({pnl_sol:+.5f} SOL) — mantenido {int(time.time() - p.entry_time)}s — "
                        f"https://solscan.io/tx/{signature}")

            self._append_position_csv(p, price, pnl_pct, pnl_sol, exit_reason, tx_signature=signature)
            if self._unsubscribe_fn:
                await self._unsubscribe_fn(mint)
        finally:
            # Pase lo que pase (éxito, fallo, excepción) liberamos el candado:
            # si la venta falló y la posición sigue abierta, el PRÓXIMO
            # mensaje de trade debe poder reintentar el cierre.
            self._closing_mints.discard(mint)


class RealTradeExecutor(_OnchainRealExecutorBase):
    """
    Ejecución REAL vía la Local Transaction API de PumpPortal
    (POST https://pumpportal.fun/api/trade-local): PumpPortal devuelve una
    transacción SIN FIRMAR, la firmamos aquí mismo con `solders` usando la
    clave privada del propio usuario (nunca sale del proceso) y la enviamos
    nosotros mismos al RPC configurado (cfg.solana_rpc_url).

    Requiere el paquete `solders` (ver requirements.txt): pip install solders

    Seguridad / comportamiento:
      - La clave privada SOLO vive en memoria (nunca se loguea ni se escribe
        a disco). Cárgala desde config.toml (que está en .gitignore) o mejor
        aún desde una variable de entorno.
      - El primer envío al RPC se hace CON preflight (simulación on-chain):
        si la simulación falla (fondos insuficientes, honeypot, slippage
        excedido, etc.) se aborta sin gastar nada y sin reintentar a ciegas.
      - Si la simulación pasa, la transacción firmada se reenvía varias
        veces (skip-preflight) mientras se consulta su estado — esto es
        seguro: reenviar EXACTAMENTE los mismos bytes firmados no duplica
        el trade, ya que Solana deduplica por firma. Es el patrón estándar
        para maximizar la probabilidad de inclusión en un bloque.
      - Si no se confirma dentro de cfg.tx_confirm_timeout_seconds, NO se
        asume que falló: puede haber quedado pendiente en la red. Se loguea
        la firma y el link de Solscan para que la revises a mano.
    """

    LOG_PREFIX = "[REAL/LOCAL]"
    TRADE_LOCAL_URL = "https://pumpportal.fun/api/trade-local"

    def __init__(self, config: Config, wallet_private_key: Optional[str] = None):
        super().__init__(config)

        self.wallet_private_key = wallet_private_key
        self._keypair = None
        if wallet_private_key:
            try:
                from solders.keypair import Keypair
                self._keypair = Keypair.from_base58_string(wallet_private_key)
            except ImportError:
                raise RuntimeError(
                    "Falta el paquete 'solders' (necesario para RealTradeExecutor). "
                    "Instálalo con: pip install solders"
                )
            except Exception as e:
                raise RuntimeError(f"pumpportal_wallet_private no es una clave base58 válida: {e}")

            pubkey = str(self._keypair.pubkey())
            if config.pumpportal_wallet_public and config.pumpportal_wallet_public != pubkey:
                logger.warning(
                    f"{self.LOG_PREFIX} Aviso: pumpportal_wallet_public en config "
                    f"({config.pumpportal_wallet_public}) no coincide con la clave pública derivada "
                    f"de pumpportal_wallet_private ({pubkey}). Se usará esta última."
                )
            self.wallet_public_key = pubkey
            logger.warning(
                f"{self.LOG_PREFIX} Modo REAL (Local) activo — wallet {pubkey}. Cada operación abierta "
                f"gastará {config.buy_amount_sol} SOL de verdad. Slippage máx. {config.max_slippage_pct}% "
                f"| priority fee {config.priority_fee_sol} SOL | pool '{config.trade_pool}'."
            )
        else:
            logger.warning(f"{self.LOG_PREFIX} Aviso: no se ha configurado wallet_private_key. "
                            "open_position() fallará hasta que la configures.")

    # ------------------------------------------------------------------
    # Construcción, firma y envío de transacciones (compra Y venta)
    # ------------------------------------------------------------------

    async def _fetch_unsigned_tx(self, action: str, mint: str, amount, denominated_in_sol: bool):
        """Pide a PumpPortal la transacción sin firmar. Devuelve los bytes
        crudos de la transacción, o None si PumpPortal devolvió un error
        (p.ej. JSON con 'errors' en vez de la transacción serializada)."""
        session = await self._get_http_session()
        body = {
            "publicKey": self.wallet_public_key,
            "action": action,
            "mint": mint,
            "amount": amount,
            "denominatedInSol": "true" if denominated_in_sol else "false",
            "slippage": self.cfg.max_slippage_pct,
            "priorityFee": self.cfg.priority_fee_sol,
            "pool": self.cfg.trade_pool,
        }
        try:
            async with session.post(self.TRADE_LOCAL_URL, json=body,
                                     timeout=aiohttp.ClientTimeout(total=15)) as resp:
                raw = await resp.read()
                content_type = resp.headers.get("Content-Type", "")
                if resp.status != 200 or "application/json" in content_type:
                    # PumpPortal responde JSON solo cuando hay un error de validación
                    logger.error(f"{self.LOG_PREFIX} PumpPortal rechazó la solicitud de {action} "
                                 f"({resp.status}): {raw[:500]}")
                    return None
                return raw
        except Exception as e:
            logger.error(f"{self.LOG_PREFIX} Error de red pidiendo tx de {action} a PumpPortal: {e}")
            return None

    def _sign_tx(self, raw_tx_bytes: bytes):
        from solders.transaction import VersionedTransaction
        unsigned = VersionedTransaction.from_bytes(raw_tx_bytes)
        return VersionedTransaction(unsigned.message, [self._keypair])

    async def _send_and_confirm(self, signed_tx) -> Optional[str]:
        """Envía la tx firmada y espera confirmación, reenviando (mismos
        bytes, mismo signature) mientras no confirme y no expire el
        blockhash. Devuelve el signature si confirmó, o None si no se pudo
        confirmar dentro del timeout (no necesariamente significa que
        falló: puede seguir viva en la red — se loguea el link a Solscan
        para comprobarlo a mano)."""
        signature = str(signed_tx.signatures[0])
        tx_b64 = base64.b64encode(bytes(signed_tx)).decode("ascii")

        # Primer envío: CON preflight, para pillar errores de simulación
        # (fondos insuficientes, slippage excedido, honeypot, etc.) sin
        # gastar ni reintentar a ciegas.
        first = await self._rpc_call("sendTransaction", [
            tx_b64, {"encoding": "base64", "skipPreflight": False,
                     "preflightCommitment": "confirmed", "maxRetries": 0},
        ])
        if "error" in first:
            logger.error(f"{self.LOG_PREFIX} Simulación/envío rechazado por el RPC: {first['error']}")
            return None

        deadline = time.time() + self.cfg.tx_confirm_timeout_seconds
        while time.time() < deadline:
            status = await self._rpc_call("getSignatureStatuses", [[signature],
                                          {"searchTransactionHistory": True}])
            value = (status.get("result") or {}).get("value") or [None]
            info = value[0]
            if info is not None:
                if info.get("err") is not None:
                    logger.error(f"{self.LOG_PREFIX} Transacción confirmada pero FALLÓ on-chain: "
                                 f"{info['err']} — https://solscan.io/tx/{signature}")
                    return None
                if info.get("confirmationStatus") in ("confirmed", "finalized"):
                    return signature

            await asyncio.sleep(self.cfg.tx_confirm_poll_interval_seconds)
            # Reenvío de los MISMOS bytes firmados: es seguro (Solana
            # deduplica por firma) y aumenta la probabilidad de inclusión
            # sin arriesgarse a duplicar el trade.
            await self._rpc_call("sendTransaction", [
                tx_b64, {"encoding": "base64", "skipPreflight": True, "maxRetries": 0},
            ])

        logger.warning(f"{self.LOG_PREFIX} No se confirmó en {self.cfg.tx_confirm_timeout_seconds:.0f}s "
                        f"(puede seguir pendiente) — revisa https://solscan.io/tx/{signature}")
        return None

    async def _execute_trade(self, action: str, mint: str, amount,
                              denominated_in_sol: bool) -> Optional[str]:
        raw_tx = await self._fetch_unsigned_tx(action, mint, amount, denominated_in_sol)
        if raw_tx is None:
            return None
        try:
            signed = self._sign_tx(raw_tx)
        except Exception as e:
            logger.error(f"{self.LOG_PREFIX} No se pudo firmar la transacción de {action}: {e}")
            return None
        return await self._send_and_confirm(signed)


class LightningTradeExecutor(_OnchainRealExecutorBase):
    """
    Ejecución REAL vía la Lightning Transaction API de PumpPortal
    (POST https://pumpportal.fun/api/trade?api-key=...): PumpPortal
    gestiona una wallet propia (creada en pumpportal.fun/trading-api/setup)
    y firma/envía la transacción por ti usando sus propios nodos dedicados,
    proveedores SWQoS y relays de Jito — tú NUNCA tienes la clave privada
    de esa wallet.

    Diferencia clave frente a RealTradeExecutor (Local Transaction API):
    CUSTODIA. Aquí PumpPortal controla los fondos de esa wallet en todo
    momento — si su servicio tiene un problema, o pierdes/revocan tu API
    key, no hay forma de recuperar los fondos salvo a través de ellos. A
    cambio no necesitas `solders` ni gestionar tu propia clave privada, y
    en teoría es más rápida (rutas Jito/SWQoS ya optimizadas de su lado).

    Requiere en config.toml:
      - pumpportal_api_key: el API key ligado a la wallet Lightning (se
        genera junto con la wallet en pumpportal.fun/trading-api/setup —
        distinto del key que uses solo para el feed de datos/WebSocket).
      - pumpportal_wallet_public: la dirección pública de esa wallet
        Lightning. PumpPortal no te la devuelve en la respuesta del trade,
        así que la necesitamos configurada aparte para poder leer el fill
        real de cada tx vía tu propio RPC (ver _get_trade_fill).
    """

    LOG_PREFIX = "[REAL/LIGHTNING]"
    TRADE_URL = "https://pumpportal.fun/api/trade"

    def __init__(self, config: Config):
        super().__init__(config)

        if not config.pumpportal_api_key:
            logger.error(f"{self.LOG_PREFIX} Falta pumpportal_api_key en config. "
                         "open_position() fallará.")
        if not config.pumpportal_wallet_public:
            logger.warning(f"{self.LOG_PREFIX} Aviso: falta pumpportal_wallet_public en config — "
                           "no podré leer el fill real de cada tx (usaré precios aproximados del feed) "
                           "y open_position() fallará (necesito saber a qué wallet apunto).")
        self.wallet_public_key = config.pumpportal_wallet_public or None

        logger.warning(
            f"{self.LOG_PREFIX} Modo REAL (Lightning) activo — PumpPortal firma y envía por ti "
            f"con la wallet {config.pumpportal_wallet_public or '(sin configurar)'}. Cada operación "
            f"abierta gastará {config.buy_amount_sol} SOL de verdad. Slippage máx. "
            f"{config.max_slippage_pct}% | priority fee {config.priority_fee_sol} SOL "
            f"| pool '{config.trade_pool}'."
        )

    async def _execute_trade(self, action: str, mint: str, amount,
                              denominated_in_sol: bool) -> Optional[str]:
        if not self.cfg.pumpportal_api_key:
            logger.error(f"{self.LOG_PREFIX} No hay pumpportal_api_key configurado.")
            return None

        session = await self._get_http_session()
        body = {
            "action": action,
            "mint": mint,
            "amount": amount,
            "denominatedInSol": "true" if denominated_in_sol else "false",
            "slippage": self.cfg.max_slippage_pct,
            "priorityFee": self.cfg.priority_fee_sol,
            "pool": self.cfg.trade_pool,
            # PumpPortal documenta que, si no se manda, "skipPreflight" vale
            # "true" por DEFECTO en la Lightning API — es decir, si no lo
            # forzamos aquí, PumpPortal NO simularía la tx antes de
            # enviarla. Lo forzamos a "false" para conservar la misma red
            # de seguridad que ya tenemos en modo Local (detectar fondos
            # insuficientes / slippage excedido / honeypot ANTES de gastar).
            "skipPreflight": "true",
        }
        url = f"{self.TRADE_URL}?api-key={self.cfg.pumpportal_api_key}"
        try:
            async with session.post(url, data=body,
                                     timeout=aiohttp.ClientTimeout(total=20)) as resp:
                data = await resp.json(content_type=None)
        except Exception as e:
            logger.error(f"{self.LOG_PREFIX} Error de red enviando {action}: {e}")
            return None

        signature = data.get("signature") if isinstance(data, dict) else None
        if not signature:
            logger.error(f"{self.LOG_PREFIX} PumpPortal rechazó la orden de {action}: {data}")
            return None

        # PumpPortal ya intenta "aterrizar" la tx con sus propias rutas
        # optimizadas (SWQoS/Jito) — aquí solo esperamos confirmación
        # contra TU RPC. A diferencia del modo Local, no podemos reenviar
        # nosotros mismos (no tenemos los bytes firmados: la firma vive del
        # lado de PumpPortal), así que si no confirma a tiempo simplemente
        # se reporta como no confirmada — el siguiente trade que llegue
        # reintentará (ver _close_position/handle_trade_message).
        deadline = time.time() + self.cfg.tx_confirm_timeout_seconds
        while time.time() < deadline:
            status = await self._rpc_call("getSignatureStatuses", [[signature],
                                          {"searchTransactionHistory": True}])
            value = (status.get("result") or {}).get("value") or [None]
            info = value[0]
            if info is not None:
                if info.get("err") is not None:
                    logger.error(f"{self.LOG_PREFIX} Transacción confirmada pero FALLÓ on-chain: "
                                 f"{info['err']} — https://solscan.io/tx/{signature}")
                    return None
                if info.get("confirmationStatus") in ("confirmed", "finalized"):
                    return signature
            await asyncio.sleep(self.cfg.tx_confirm_poll_interval_seconds)

        logger.warning(f"{self.LOG_PREFIX} No se confirmó en {self.cfg.tx_confirm_timeout_seconds:.0f}s "
                        f"(puede seguir pendiente) — revisa https://solscan.io/tx/{signature}")
        return None
