import time
import requests
import os
import csv
import json
import asyncio
import websockets
from dataclasses import dataclass, field
from typing import Optional

from config import Config


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
 
        if self.cfg.verbose:
            print(f"[NUEVO] {candidate.symbol} ({mint[:10]}...) detectado...")
 
        self._run_security_check(candidate)  # bloqueante (RPC síncrono, gratuito)
 
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
        """Punto de entrada público: mantiene la conexión viva, reconectando
        automáticamente con backoff si el WebSocket se cae por lo que sea
        (caída de red puntual, el servidor cierra sin avisar, etc.)."""
        backoff = 3
        max_backoff = 60
        while True:
            try:
                await self._run_once()
                backoff = 3  # se resetea si tuvimos una sesión estable
            except (websockets.exceptions.ConnectionClosedError,
                    websockets.exceptions.ConnectionClosedOK,
                    OSError) as e:
                print(f"[RECONEXIÓN] Conexión perdida ({type(e).__name__}: {e}). "
                      f"Reintentando en {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
 
    async def _run_once(self):
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
                elif "errors" in msg:
                    # los errores del servidor se muestran SIEMPRE, aunque verbose=False,
                    # porque si algo falla aquí es la causa más probable de "no encuentra nada"
                    print(f"[ERROR SERVIDOR] {msg.get('errors')}")
 