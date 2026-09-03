"""Servidor web de redirect do encurtador próprio (agnóstico de plataforma)."""
from __future__ import annotations
import os

from aiohttp import web

from config import _SHORT_BASE
from database import db_get_short
from logger import log_sys


# [E3.4] Handle de shutdown do servidor. Sem isto o AppRunner fica
# inalcancavel apos _iniciar_servidor_web retornar, e nao ha' caminho
# de cleanup. Quem cria o recurso expoe o encerramento.
_runner = None


async def _handle_redirect(request: web.Request) -> web.Response:
    code = request.match_info.get("code", "")
    if not code:
        return web.Response(status=404, text="Not found")
    url_destino = db_get_short(code)
    if url_destino:
        raise web.HTTPFound(location=url_destino)
    return web.Response(status=404, text="Link não encontrado")


async def _handle_health(request: web.Request) -> web.Response:
    return web.Response(text="OK")


async def _iniciar_servidor_web() -> None:
    global _runner
    app = web.Application()
    app.router.add_get("/",       _handle_health)
    app.router.add_get("/health", _handle_health)
    app.router.add_get("/{code}", _handle_redirect)
    runner = web.AppRunner(app)
    await runner.setup()
    _runner = runner
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log_sys.info(f"🌐 Servidor redirect ativo | porta={port} | base={_SHORT_BASE}")


async def _encerrar_servidor_web() -> None:
    """Encerra o servidor de redirect. Idempotente."""
    global _runner
    if _runner is None:
        return
    try:
        await _runner.cleanup()
        log_sys.info("🌐 Servidor redirect encerrado")
    finally:
        _runner = None
  
