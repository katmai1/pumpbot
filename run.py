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
    pip install -r requirements.txt   (websockets, aiohttp — ver ese archivo)

Configuración necesaria (ver clase Config):
  1. pumpportal_api_key -> gratis en https://pumpportal.fun/trading-api/setup
  2. solana_rpc_url -> usa un RPC decente (Helius/QuickNode)

Apagado (Ctrl+C / SIGTERM):
  La primera señal inicia un apagado ORDENADO: se deja de buscar/abrir
  posiciones en monedas nuevas, y el bot se queda esperando a que las
  posiciones que ya tenga abiertas cierren por su TP/SL/timeout. Solo
  entonces termina el proceso. Una segunda señal fuerza la salida
  inmediata, cancelando cualquier posición abierta sin esperar su cierre.
"""

import asyncio
import argparse
import logging
import signal

from pumpbot.config import Config, _mask_key
from pumpbot.pump import PumpFunScanner
from pumpbot.executors import SimulatedTradeExecutor

logger = logging.getLogger("pumpbot")


def configure_parser():
    parser = argparse.ArgumentParser(description="Bot para Pump.Fun")
    parser.add_argument(
        "-c", "--config",
        default="config.toml"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Activa el modo verbose"
    )

    return parser.parse_args()


def setup_logging(verbose: bool):
    """Configura el logging de todo el bot. El nivel DEBUG (activado por
    --verbose) es lo que hace que se vean también los candidatos
    descartados, mensajes del servidor, etc. — los distintos módulos ya
    deciden con qué nivel loguean cada cosa, aquí solo se fija el umbral."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def install_shutdown_handlers(scanner: PumpFunScanner):
    """Registra SIGINT/SIGTERM para pedir un apagado ordenado al scanner.
    Una segunda señal cancela todas las tareas en curso para forzar la
    salida inmediata (por si alguna posición se queda colgada esperando
    un stop que no llega).

    `loop.add_signal_handler` es la forma "nativa" de asyncio para esto,
    pero Windows no la soporta en absoluto (ni con SelectorEventLoop ni
    con ProactorEventLoop) y lanza NotImplementedError. Por eso aquí se
    usa como alternativa `signal.signal`, que sí funciona en Windows para
    Ctrl+C (SIGINT) — se intenta primero la vía nativa por señal y, si no
    está disponible, se cae a `signal.signal` para esa señal concreta.

    `signal.signal` invoca su callback de forma síncrona en el hilo
    principal (entre bytecodes), así que aquí no se llama directamente a
    código async: se agenda con `loop.call_soon_threadsafe`, que es la
    forma segura de despertar el event loop desde un signal handler y
    funciona igual en Linux, macOS y Windows.

    Nota sobre Windows: solo Ctrl+C en la consola (SIGINT) dispara el
    apagado ordenado de forma fiable; puede haber hasta ~1s de retraso en
    reconocerlo, es una limitación conocida de asyncio en Windows. Un
    "Terminar tarea" forzado (taskkill /F) o cerrar la consola no ejecuta
    ningún handler de Python, igual que un `kill -9` en Linux — contra
    eso no hay apagado ordenado posible en ningún sistema operativo."""
    loop = asyncio.get_running_loop()
    state = {"signal_count": 0}

    def _force_cancel_all():
        logger.warning("Segunda señal recibida: forzando salida inmediata "
                        "(puede dejar posiciones abiertas sin cerrar).")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    def _on_shutdown_signal(*_args):
        state["signal_count"] += 1
        if state["signal_count"] == 1:
            loop.call_soon_threadsafe(scanner.request_shutdown)
        else:
            loop.call_soon_threadsafe(_force_cancel_all)

    registered_natively = set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_shutdown_signal)
            registered_natively.add(sig)
        except NotImplementedError:
            pass  # Windows: sin soporte para add_signal_handler, se usa el fallback

    for sig in (signal.SIGINT, signal.SIGTERM):
        if sig in registered_natively:
            continue
        try:
            signal.signal(sig, _on_shutdown_signal)
        except (ValueError, OSError, AttributeError):
            pass  # p.ej. SIGTERM no siempre se puede registrar según la plataforma


async def main(config: Config):
    executor = SimulatedTradeExecutor(config)
    scanner = PumpFunScanner(config, executor)
    install_shutdown_handlers(scanner)

    try:
        await scanner.run()
    finally:
        scanner.print_shortlist(force=True)
        executor.print_open_positions_summary()
        logger.info("Bot detenido.")


if __name__ == "__main__":
    args = configure_parser()

    config = Config.from_toml(args.config)
    config.verbose = args.verbose
    config.validate()
    setup_logging(config.verbose)

    logger.info("=" * 70)
    logger.info("SIMULACIÓN (PAPERTRADING) — NO SE EJECUTA NINGUNA COMPRA/VENTA REAL.")
    logger.info(f"API key: {_mask_key(config.pumpportal_api_key)}  |  RPC: {config.solana_rpc_url[:30]}...")
    logger.info(f"Posición simulada: {config.buy_amount_sol} SOL | TP +{config.take_profit_pct}% "
                f"(gracia {config.take_profit_grace_period_seconds}s) | "
                f"SL -{config.stop_loss_pct}% (gracia {config.stop_loss_grace_period_seconds}s) | "
                f"timeout {config.max_hold_seconds}s")
    logger.info(f"Límites de exposición: máx. {config.max_concurrent_positions} posiciones concurrentes | "
                f"máx. {config.max_total_exposure_sol} SOL en riesgo simultáneo")
    logger.info("Riesgo extremo de pérdida total si en el futuro conectas esto a dinero real.")
    logger.info("=" * 70)

    # Cambia esta línea (en main()) por RealTradeExecutor(config, wallet_private_key=...)
    # el día que hayas completado los TODO de esa clase y estés listo para
    # operar con dinero real. Hasta entonces, todo es papertrading.
    try:
        asyncio.run(main(config))
    except KeyboardInterrupt:
        # Red de seguridad por si la señal llega antes de que el loop esté
        # listo para registrar el handler (arranque muy temprano): en ese
        # caso no hay apagado ordenado, se corta de golpe.
        logger.info("Detenido por el usuario.")
