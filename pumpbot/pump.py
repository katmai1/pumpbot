import time
import os
import csv
import json
import asyncio
import logging
import aiohttp
import websockets
from dataclasses import dataclass, field
from typing import Optional

from pumpbot.config import Config

logger = logging.getLogger(__name__)


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
    ath_price: Optional[float] = None  # precio máximo visto durante la observación

    @property
    def current_price(self) -> Optional[float]:
        if self.v_tokens_in_curve <= 0:
            return None
        return self.v_sol_in_curve / self.v_tokens_in_curve

    @property
    def buy_sell_ratio(self) -> float:
        return (self.buy_count / self.sell_count) if self.sell_count > 0 else float("inf")

    @property
    def drawdown_from_ath_pct(self) -> Optional[float]:
        """% de caída del precio actual respecto al máximo (ATH) visto
        durante la ventana de observación. None si aún no hay datos
        suficientes para calcularlo."""
        price = self.current_price
        if price is None or not self.ath_price:
            return None
        return ((self.ath_price - price) / self.ath_price) * 100

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

        price = self.current_price
        if price is not None and (self.ath_price is None or price > self.ath_price):
            self.ath_price = price

# ======================================================================
# SCANNER: WebSocket, detección, filtro (no sabe nada de comprar/vender)
# ======================================================================


class PumpFunScanner:
    """
    Detecta tokens nuevos en pump.fun, los evalúa contra criterios de
    seguridad y momentum, y delega en un TradeExecutor la apertura de
    posición cuando un candidato pasa el filtro.
    """

    def __init__(self, config: Config, executor):
        self.cfg = config
        self.executor = executor
        self.executor.bind_subscription_callbacks(self._subscribe_trade, self._unsubscribe_trade)

        self.tracked_candidates: dict[str, TokenCandidate] = {}
        self.candidate_shortlist: list[TokenCandidate] = []
        self._warned_missing_curve_fields = False
        self._ws = None
        # Sesión aiohttp reutilizada para TODAS las llamadas RPC (evita abrir
        # una conexión TCP nueva por request y permite que las llamadas RPC
        # sean no bloqueantes de verdad, sin congelar el bucle del WebSocket).
        self._http_session: Optional[aiohttp.ClientSession] = None

        # Apagado ordenado: cuando se activa, se deja de buscar/abrir
        # posiciones en monedas nuevas, pero se sigue escuchando el
        # WebSocket para las posiciones ya abiertas hasta que toquen su
        # TP/SL/timeout. Ver request_shutdown().
        self._shutting_down = False

        self._ensure_candidates_csv_header()

    # ------------------------------------------------------------------
    # RPC de Solana (seguridad: mint/freeze authority)
    # ------------------------------------------------------------------

    async def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    async def close(self):
        """Cierra recursos externos (sesión HTTP). Llamar al parar el bot."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

    async def _rpc_call_with_retry(self, method: str, params: list,
                                    max_retries: int = 5, delay: float = 2.0) -> dict:
        """Versión async: usa aiohttp + asyncio.sleep, así que mientras espera
        respuesta de Helius o reintenta, el bucle de eventos sigue libre para
        seguir leyendo mensajes del WebSocket en paralelo (antes, con
        `requests` + `time.sleep`, esto bloqueaba TODO el proceso, incluida
        la recepción de trades, hasta 10s por candidato)."""
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        session = await self._get_http_session()
        last_result = {}
        for _ in range(max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=8)
                async with session.post(self.cfg.solana_rpc_url, json=payload, timeout=timeout) as resp:
                    data = await resp.json()
                if "error" in data or data.get("result", {}).get("value") is None:
                    last_result = data
                    await asyncio.sleep(delay)
                    continue
                return data
            except Exception as e:
                last_result = {"error": f"{type(e).__name__}: {e}"}
                await asyncio.sleep(delay)
        return last_result

    async def _get_mint_authorities(self, mint: str) -> dict:
        data = await self._rpc_call_with_retry(
            "getAccountInfo", [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}]
        )
        if "error" in data or not data.get("result", {}).get("value"):
            logger.warning(f"getAccountInfo({mint[:10]}...) sin resultado: {data.get('error', 'value null')}")
            return {"mint_authority": "unknown", "freeze_authority": "unknown"}
        info = data["result"]["value"]["data"]["parsed"]["info"]
        return {"mint_authority": info.get("mintAuthority"), "freeze_authority": info.get("freezeAuthority")}

    async def _run_security_check(self, candidate: TokenCandidate):
        auth = await self._get_mint_authorities(candidate.mint)
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

        drawdown = c.drawdown_from_ath_pct
        if drawdown is not None and drawdown > cfg.max_drawdown_from_ath_pct:
            c.reasons_rejected.append(
                f"cayó {drawdown:.1f}% desde su máximo (ATH {c.ath_price:.10f} SOL/token)"
            )

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
        """Registra el candidato. Los que pasan el filtro son relevantes
        siempre (INFO); los descartados solo interesan en modo verbose
        (DEBUG) — eso ahora lo decide el nivel del logger, configurado a
        partir de config.verbose, en vez de comprobarlo aquí a mano."""
        estado = "CANDIDATO ✔" if c.passed else "descartado"
        line = (f"[{c.symbol:<10}] score={c.score:<6} buyers={len(c.buyers):<4} "
                f"buy/sell={c.buy_count}/{c.sell_count:<3} curve={c.bonding_curve_progress:>5.1f}%  "
                f"{estado}  https://pump.fun/{c.mint}")
        log_fn = logger.info if c.passed else logger.debug
        log_fn(line)
        if not c.passed and c.reasons_rejected:
            logger.debug(f"    motivo: {'; '.join(c.reasons_rejected)}")

    def print_shortlist(self, force: bool = False):
        passed = [c for c in self.candidate_shortlist if c.passed]
        log_fn = logger.info if force else logger.debug
        if not passed:
            log_fn("(sin candidatos que pasen el filtro todavía)")
            return
        passed.sort(key=lambda c: c.score, reverse=True)
        lines = [f"{'='*80}", f"CANDIDATOS ({len(passed)})", f"{'='*80}"]
        for c in passed:
            lines.append(f"  {c.score:>6.1f}  {c.symbol:<10}  https://pump.fun/{c.mint}")
        lines.append("=" * 80)
        log_fn("\n" + "\n".join(lines))

    # ------------------------------------------------------------------
    # Suscripciones WebSocket (usadas por el executor vía callback)
    # ------------------------------------------------------------------

    async def _subscribe_trade(self, mint: str):
        await self._ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))

    async def _unsubscribe_trade(self, mint: str):
        await self._ws.send(json.dumps({"method": "unsubscribeTokenTrade", "keys": [mint]}))

    async def _resubscribe_active_mints(self):
        """Tras (re)conectar hay que volver a suscribirse a los trades de
        los mints que ya estaban siendo trackeados: candidatos aún en
        ventana de observación y posiciones ya abiertas. Un WebSocket
        nuevo (tras una caída de conexión) no conserva ninguna
        suscripción anterior — sin esto, esos mints se quedarían sin
        recibir más precio para siempre y una posición abierta nunca
        llegaría a cerrar por TP/SL/timeout."""
        mints = set(self.tracked_candidates.keys()) | set(self.executor.open_positions.keys())
        if not mints:
            return
        logger.info(f"Resuscribiendo a {len(mints)} mint(s) tras (re)conexión...")
        for mint in mints:
            await self._subscribe_trade(mint)

    # ------------------------------------------------------------------
    # Apagado ordenado
    # ------------------------------------------------------------------

    def request_shutdown(self):
        """Inicia un apagado ordenado del bot.

        A partir de este momento:
          - se deja de suscribir/escuchar monedas nuevas (no se procesan
            más eventos "create" ni se abren posiciones nuevas);
          - las posiciones ya abiertas se siguen trackeando con normalidad
            hasta que cierren por TP, SL o timeout;
          - solo cuando no queda ninguna posición abierta se cierra la
            conexión y el programa termina.

        Pensado para llamarse desde un signal handler síncrono (Ctrl+C),
        por lo que no es una corrutina. Es idempotente: llamarlo más de
        una vez no hace nada la segunda vez."""
        if self._shutting_down:
            return
        self._shutting_down = True

        n_open = len(self.executor.open_positions)
        if n_open == 0:
            logger.info("Apagado solicitado. No hay posiciones abiertas: cerrando ahora.")
        else:
            logger.info(f"Apagado solicitado: dejando de buscar monedas nuevas. "
                        f"Esperando a que se cierren {n_open} posición(es) abierta(s) "
                        f"por su TP/SL/timeout...")

        if self._ws is not None:
            asyncio.create_task(self._begin_shutdown_sequence())

    async def _begin_shutdown_sequence(self):
        try:
            await self._ws.send(json.dumps({"method": "unsubscribeNewToken"}))
        except Exception as e:
            logger.warning(f"No se pudo desuscribir de nuevos tokens: {e}")

        if not self.executor.open_positions:
            try:
                await self._ws.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Ciclo de vida de un candidato
    # ------------------------------------------------------------------

    async def _finalize_candidate(self, mint: str, trigger_reason: str = ""):
        candidate = self.tracked_candidates.pop(mint, None)
        if not candidate:
            return

        self._evaluate_candidate(candidate)
        if trigger_reason:
            logger.debug(f"[TRIGGER] {candidate.symbol}: {trigger_reason}")
        self._print_candidate_row(candidate)
        self._append_candidate_csv(candidate)
        self.candidate_shortlist.append(candidate)
        self.print_shortlist()

        if candidate.passed and not self._shutting_down:
            opened = await self.executor.open_position(candidate)
            if not opened:
                await self._unsubscribe_trade(mint)
        else:
            if candidate.passed and self._shutting_down:
                logger.info(f"{candidate.symbol} pasó el filtro pero el bot está en apagado; "
                            f"no se abre posición nueva.")
            await self._unsubscribe_trade(mint)

    async def _finalize_after_window(self, mint: str):
        await asyncio.sleep(self.cfg.observation_window_seconds)
        await self._finalize_candidate(
            mint, trigger_reason=f"límite de {self.cfg.observation_window_seconds}s alcanzado"
        )

    def _passes_prefilter(self, c: TokenCandidate) -> bool:
        """Comprueba SOLO los criterios que no cuestan dinero: autoridades del
        mint (RPC gratuito) y % del dev (viene en el propio evento de creación,
        gratuito). Si esto ya falla, ni nos molestamos en pagar por
        subscribeTokenTrade — ese stream es de pago (0.01 SOL / 10.000 eventos,
        cargado a tu wallet vinculada)."""
        c.reasons_rejected.clear()
        if not c.mint_authority_revoked:
            c.reasons_rejected.append("mint authority no revocada")
        if not c.freeze_authority_revoked:
            c.reasons_rejected.append("freeze authority no revocada")
        if c.dev_initial_buy_pct > self.cfg.max_dev_holding_pct:
            c.reasons_rejected.append(f"dev compró {c.dev_initial_buy_pct:.1f}% del supply al crear")
        return len(c.reasons_rejected) == 0

    async def _handle_new_token(self, msg: dict):
        if self._shutting_down:
            # En apagado no procesamos monedas nuevas en absoluto.
            return

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

        logger.debug(f"[NUEVO] {candidate.symbol} ({mint[:10]}...) detectado...")

        await self._run_security_check(candidate)  # async: no bloquea el loop mientras espera al RPC

        if self._shutting_down:
            # Pudo activarse el apagado mientras esperábamos el RPC.
            return

        if not self._passes_prefilter(candidate):
            # descartado SIN suscribirnos a trades: nos ahorramos la cuota de pago
            candidate.passed = False
            candidate.score = 0.0
            self._print_candidate_row(candidate)
            self._append_candidate_csv(candidate)
            self.candidate_shortlist.append(candidate)
            self.print_shortlist()
            return

        # pasó el pre-filtro gratuito: ahora sí merece la pena pagar por sus trades
        self.tracked_candidates[mint] = candidate
        await self._subscribe_trade(mint)
        asyncio.create_task(self._finalize_after_window(mint))

    def _handle_token_trade(self, msg: dict):
        mint = msg.get("mint")

        if "vSolInBondingCurve" not in msg or "vTokensInBondingCurve" not in msg:
            if not self._warned_missing_curve_fields:
                self._warned_missing_curve_fields = True
                logger.debug(f"Trade sin campos de bonding curve. Claves: {list(msg.keys())}")

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
        #    tiene abiertas el executor, ni falta que le hace). Esto ocurre
        #    también durante el apagado: las posiciones abiertas siguen
        #    recibiendo precio hasta que cierran por su cuenta.
        self.executor.handle_trade_message(msg)

    # ------------------------------------------------------------------
    # Bucle principal
    # ------------------------------------------------------------------

    async def run(self):
        """Punto de entrada público: mantiene la conexión viva, reconectando
        automáticamente con backoff si el WebSocket se cae por lo que sea
        (caída de red puntual, el servidor cierra sin avisar, etc.).

        Devuelve el control cuando termina el apagado ordenado (ver
        request_shutdown): sin posiciones abiertas y sin buscar más
        monedas."""
        backoff = 3
        max_backoff = 60
        try:
            await self._run_forever(backoff, max_backoff)
        finally:
            # Se ejecuta también si el apagado se completa o hay una
            # cancelación: no queremos dejar la conexión HTTP a Helius
            # abierta colgando.
            await self.close()

    async def _run_forever(self, backoff: int, max_backoff: int):
        while True:
            try:
                await self._run_once()
                # _run_once solo retorna limpiamente cuando el apagado
                # ordenado ha terminado (sin posiciones abiertas).
                return
            except (websockets.exceptions.ConnectionClosedError,
                    websockets.exceptions.ConnectionClosedOK,
                    OSError) as e:
                if self._shutting_down and not self.executor.open_positions:
                    logger.info("Conexión perdida durante el apagado y no quedan posiciones "
                                "abiertas; no se reconecta.")
                    return
                logger.warning(f"Conexión perdida ({type(e).__name__}: {e}). "
                               f"Reintentando en {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _run_once(self):
        logger.info("Conectando a PumpPortal...")
        async with websockets.connect(self.cfg.pumpportal_ws_url) as ws:
            self._ws = ws
            logger.debug("Conexión WebSocket abierta.")

            if self._shutting_down:
                if not self.executor.open_positions:
                    logger.info("Apagado solicitado antes de conectar y sin posiciones "
                                "abiertas; cerrando.")
                    return
                logger.info("Apagado en curso: no se suscribe a nuevos tokens, solo se "
                            "esperan los stops de las posiciones abiertas.")
            else:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                logger.info(f"Suscrito a nuevos tokens. Ventana máxima: "
                            f"{self.cfg.observation_window_seconds}s, disparo anticipado "
                            f"desde {self.cfg.early_trigger_min_seconds}s. "
                            f"Log en {self.cfg.candidates_csv_path}")

            await self._resubscribe_active_mints()

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                if self.cfg.debug_raw:
                    logger.debug(f"[RAW] {json.dumps(msg)[:300]}")

                if "mint" in msg and msg.get("txType") == "create":
                    await self._handle_new_token(msg)
                elif msg.get("txType") in ("buy", "sell"):
                    self._handle_token_trade(msg)
                elif "message" in msg:
                    logger.debug(f"[SERVIDOR] {msg.get('message')}")
                elif "errors" in msg:
                    # los errores del servidor se muestran SIEMPRE, aunque no
                    # esté activo el modo verbose, porque si algo falla aquí
                    # es la causa más probable de "no encuentra nada"
                    logger.error(f"[ERROR SERVIDOR] {msg.get('errors')}")

                if self._shutting_down and not self.executor.open_positions:
                    logger.info("Todas las posiciones se han cerrado. Apagando el bot.")
                    break
