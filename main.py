"""
FOGUETÃO v80.2 — Ponto de entrada.

Responsabilidades exclusivas deste módulo:
  - Inicializar globals, DB e orchestrator
  - Registrar handlers de eventos (NewMessage / MessageEdited)
  - Iniciar health check e servidor web
  - Loop de restart com tratamento de erros fatais

═══════════════════════════════════════════════════════════════════
Mudanças v80.2 (em relação a v80.0):
  • Detecção de cupom em 4 níveis de confiança (crases, formato
    profissional, palavra-chave, fallback)
  • Captura de entidades CODE/PRE do Telegram (cupom em crases)
  • Identidade canônica respeita cupom no produto
    (mesmo produto + cupom diferente = ofertas distintas)
  • Janelas por TIPO (cupom 30min, produto 60min, evento 20min)
  • _MAX_EDITS=1 (1 edição estética entre grupos)
  • Edição do grupo original (correção) NÃO conta no limite
  • Lock atômico por identidade (resolve race condition)
  • Magalu: desencurtador agressivo até a alma + Opção A
═══════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio

from telethon import events
from telethon.errors import AuthKeyUnregisteredError, SessionPasswordNeededError

import config
import globals as g

from client import client

from config import (
    API_ID, API_HASH, SESSION_STRING,
    GRUPOS_ORIGEM, GRUPO_DESTINO,
    _AMZ_TAG, _SHP_APP_ID, _MGL_PROMOTER, _MGL_SLUG,
    _PIL_OK, _EXECUTOR,
)

from database import _init_db, _db, db_limpar
from globals import _init_globals
from logger import log_sys, log_hc

from pipeline.orchestrator import processar, _iniciar_orchestrator

from plataformas.registry import cadastrar
from plataformas import amazon, shopee, magalu

from web.redirect import _iniciar_servidor_web


# ── Health check ──────────────────────────────────────────────────
async def _health_check() -> None:
    while True:
        await asyncio.sleep(300)
        try:
            db_limpar()
            try:
                with _db() as db:
                    n_links = db.execute(
                        "SELECT COUNT(*) FROM links_cache").fetchone()[0]
                    n_dedup = db.execute(
                        "SELECT COUNT(*) FROM dedupe_temp").fetchone()[0]
                    n_sat   = db.execute(
                        "SELECT COUNT(*) FROM saturacao").fetchone()[0]
            except Exception:
                n_links = n_dedup = n_sat = "?"
            log_hc.info(
                f"💚 links={n_links} | dedupe={n_dedup} | sat={n_sat} | "
                f"anti-loop={len(g._IDS_PROC)} | fila={len(g._buf)} "
                f"w={g._w_ativos} | PIL={'OK' if _PIL_OK else 'OFF'}"
            )
        except Exception as e:
            log_hc.error(f"❌ Health: {e}", exc_info=True)


# ── Startup ───────────────────────────────────────────────────────
async def _run() -> bool:
    # 1. Inicializa locks, semáforos e caches no loop correto
    _init_globals()

    # 2. Inicializa banco de dados
    _init_db()

    # 3. Composition root: o registry é povoado explicitamente aqui,
    #    no entrypoint da aplicação. O cadastro é idempotente quanto
    #    à reexecução de _run() no mesmo processo.
    cadastrar(amazon.PLATAFORMA)
    cadastrar(shopee.PLATAFORMA)
    cadastrar(magalu.PLATAFORMA)

    # 4. Conecta ao Telegram
    log_sys.info("🔌 Conectando...")
    await client.connect()
    if not await client.is_user_authorized():
        log_sys.error("❌ Sessão inválida")
        return False

    me = await client.get_me()
    log_sys.info(f"✅ {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 {GRUPOS_ORIGEM} → {GRUPO_DESTINO}")
    log_sys.info("🚀 FOGUETÃO — ONLINE")

    # 5. Registra handlers de eventos
    @client.on(events.NewMessage(chats=GRUPOS_ORIGEM))
    async def on_new(event):
        try:
            await processar(event, is_edit=False)
        except Exception as e:
            log_sys.error(f"❌ on_new: {e}", exc_info=True)

    @client.on(events.MessageEdited(chats=GRUPOS_ORIGEM))
    async def on_edit(event):
        try:
            await processar(event, is_edit=True)
        except Exception as e:
            log_sys.error(f"❌ on_edit: {e}", exc_info=True)

    # 6. Inicia tarefas de background
    asyncio.create_task(_health_check())
    asyncio.create_task(_iniciar_orchestrator())
    asyncio.create_task(_iniciar_servidor_web())

    # 7. Aguarda desconexão
    await client.run_until_disconnected()
    return True


# ── Loop principal com restart automático ────────────────────────
async def main() -> None:
    while True:
        try:
            await _run()
        except (AuthKeyUnregisteredError, SessionPasswordNeededError) as e:
            log_sys.error(f"❌ Auth fatal: {e}")
            break
        except Exception as e:
            log_sys.error(f"💥 Caiu: {e} — restart em 15s", exc_info=True)
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(15)

    # Shutdown limpo
    try:
        _EXECUTOR.shutdown(wait=False)
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
                      
