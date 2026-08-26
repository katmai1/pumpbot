from dataclasses import dataclass
import os
import tomllib


# ======================================================================
# CONFIGURACIÓN
# ======================================================================

@dataclass
class Config:
    # Se cargan desde config.toml (ver Config.from_toml) — NUNCA hardcodees
    # aquí tu key ni tu RPC real, y NUNCA los pegues en un chat, log o repo.
    pumpportal_api_key: str = ""
    helius_api_key: str = ""
    pumpportal_wallet_private: str = ""
    pumpportal_wallet_public: str = ""
 
    # --- ventanas de tiempo (filtro / scanner) ---
    observation_window_seconds: int = 60   # límite MÁXIMO antes de descartar (fallback)
    early_trigger_min_seconds: int = 10    # no compra antes de esto (necesita algo de dato)
 
    # --- umbrales de filtrado (scanner) ---
    max_dev_holding_pct: float = 8.0
    min_unique_buyers: int = 10
    min_buy_sell_ratio: float = 1.5
    min_bonding_curve_progress: float = 1.0
    max_bonding_curve_progress: float = 70.0
    max_drawdown_from_ath_pct: float = 20.0  # descarta si el precio ya cayó
        # más de esto desde su máximo visto durante la observación — evita
        # entrar en un token que ya pegó su pico y viene cayendo, aunque el
        # ratio compra/venta ACUMULADO desde su creación siga viéndose sano
 
    # --- ejecución de trades (executor) ---
    buy_amount_sol: float = 0.01
    take_profit_pct: float = 50.0
    stop_loss_pct: float = 25.0
    stop_loss_grace_period_seconds: int = 15   # tras comprar, ignora el SL este tiempo
    take_profit_grace_period_seconds: int = 15 # tras comprar, ignora el TP este tiempo
    max_hold_seconds: int = 3600      # cierre de seguridad si nunca toca TP/SL
    position_status_every_n_trades: int = 5
    max_concurrent_positions: int = 5     # nº máx. de posiciones abiertas a la vez
    max_total_exposure_sol: float = 0.05  # SOL máx. sumando TODAS las posiciones abiertas
 
    # --- constantes del propio pump.fun ---
    total_supply: float = 1_000_000_000
    migration_sol_threshold: float = 85.0  # SOL acumulado al que migra a Raydium/PumpSwap
 
    # --- logging / debug ---
    verbose: bool = False
    debug_raw: bool = False
    candidates_csv_path: str = "candidates_log.csv"
    positions_csv_path: str = "positions_log.csv"
 
    @property
    def pumpportal_ws_url(self) -> str:
        return f"wss://pumpportal.fun/api/data?api-key={self.pumpportal_api_key}"
    
    @property
    def solana_rpc_url(self) -> str:
        return f"https://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"
 
    @classmethod
    def from_toml(cls, path: str) -> "Config":
        """Carga la configuración desde un archivo TOML (ver config.example.toml
        como plantilla). El archivo real (config.toml) NUNCA debe subirse a git
        — está en el .gitignore que te dejé."""
        if not os.path.exists(path):
            raise RuntimeError(
                f"No se encontró {path}. Copia config.example.toml a {path}"
                f"y rellena tus valores reales (API key, RPC, etc.)."
            )
        with open(path, "rb") as f:
            data = tomllib.load(f)
 
        # aplana todas las secciones ([credentials], [scanner], [executor]...)
        # en un único dict de kwargs para el dataclass
        flat = {}
        for value in data.values():
            if isinstance(value, dict):
                flat.update(value)
            # (ignora claves sueltas de nivel superior si las hubiera)
        return cls(**flat)
 
    def validate(self):
        """Comprueba que las credenciales están configuradas antes de arrancar,
        para no descubrirlo a medias de una sesión con errores silenciosos."""
        missing = []
        if not self.pumpportal_api_key:
            missing.append("pumpportal_api_key")
        if not self.helius_api_key:
            missing.append("helius_api_key")
        if missing:
            raise RuntimeError(
                f"Faltan valores en config.toml: {', '.join(missing)}."
            )
 
 
def _mask_key(key: str, visible_chars: int = 4) -> str:
    """Nunca imprimas una API key completa. Usa esto si alguna vez necesitas
    mostrarla en un log o mensaje de error para depurar."""
    if not key or len(key) <= visible_chars:
        return "****"
    return f"{key[:visible_chars]}...{'*' * 8}"