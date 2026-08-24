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
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from config import Config, _mask_key
from pump import PumpFunScanner
from executors import SimulatedTradeExecutor

if __name__ == "__main__":
    config = Config.from_toml("config.toml")
    config.validate()

    
    print("=" * 70)
    print("SIMULACIÓN (PAPERTRADING) — NO SE EJECUTA NINGUNA COMPRA/VENTA REAL.")
    print(f"API key: {_mask_key(config.pumpportal_api_key)}  |  RPC: {config.solana_rpc_url[:30]}...")
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