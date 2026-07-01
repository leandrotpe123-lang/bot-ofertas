"""
FOGUETÃO v80.2 — Ponto de entrada.

Responsabilidades exclusivas deste módulo:
  - Inicializar globals, DB e orchestrator
  - Disparar o boot do catálogo de plataformas (Auto Discovery)
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
    _PIL_OK, _EXECUTOR,
)

from database import _init_db, _db, db_limpar
from globals import _init_globals
from logger import log_sys, log_hc

from pipeline.orchestrator import processar, _iniciar_orchestrator
from pipeline.identidade import precarregar_usernames

import plataformas

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
async def _diag_botofera(client) -> None:
    # 🔬 DIAG TEMPORÁRIO — por que 'botofera' não resolve. REMOVER depois.
    from telethon.tl.functions.contacts import ResolveUsernameRequest
    from logger import log_sys
    log_sys.info("🔬 ===== DIAG botofera =====")
    try:
        r = await client(ResolveUsernameRequest("botofera"))
        chats = [(getattr(c, "id", None), getattr(c, "username", None)) for c in getattr(r, "chats", [])]
        users = [(getattr(u, "id", None), getattr(u, "username", None)) for u in getattr(r, "users", [])]
        log_sys.info(f"🔬 ResolveUsername('botofera') OK -> chats={chats} users={users}")
    except Exception as e:
        log_sys.info(f"🔬 ResolveUsername('botofera') ERRO -> {type(e).__name__}: {e}")
    try:
        achou = False
        async for d in client.iter_dialogs():
            ent = d.entity
            uname = (getattr(ent, "username", None) or "")
            if getattr(ent, "id", None) == 3817694320 or "boto" in uname.lower():
                log_sys.info(f"🔬 DIALOGO -> id={getattr(ent,'id',None)} username={uname!r} title={getattr(ent,'title',None)!r}")
                achou = True
        if not achou:
            log_sys.info("🔬 DIALOGO -> NENHUM diálogo com id=3817694320 nem username com 'boto' (conta não é membro / não vê o grupo)")
    except Exception as e:
        log_sys.info(f"🔬 iter_dialogs ERRO -> {type(e).__name__}: {e}")
    log_sys.info("🔬 ===== FIM DIAG =====")

async def _run() -> bool:
    # 1. Inicializa locks, semáforos e caches no loop correto
    _init_globals()

    # 2. Inicializa banco de dados
    _init_db()

    # NB: o boot do catálogo (plataformas.inicializar()) NÃO mora aqui.
    # _run() está dentro do loop de restart e pode reexecutar; o Auto
    # Discovery deve rodar uma única vez por processo. Ele é disparado
    # em main(), antes do loop. Ver main().

    # 3. Conecta ao Telegram
    log_sys.info("🔌 Conectando...")
    await client.connect()
    if not await client.is_user_authorized():
        log_sys.error("❌ Sessão inválida")
        return False

    me = await client.get_me()
    log_sys.info(f"✅ {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 {GRUPOS_ORIGEM} → {GRUPO_DESTINO}")
    log_sys.info("🚀 FOGUETÃO — ONLINE")
  # Pré-aquece identidade (id→@username) dos grupos monitorados, para
    # regras dependentes de @username valerem já na 1ª mensagem.
    fontes = await precarregar_usernames(client, GRUPOS_ORIGEM)
  await _diag_botofera(client)
    if not fontes:
        log_sys.error("❌ Nenhuma fonte de origem resolvida — encerrando.")
        return False
  

    # 4. Registra handlers de eventos — SOBRE AS FONTES RESOLVIDAS.
    # Registrar com a lista crua de usernames faz o Telethon re-resolver
    # o filtro de chats; uma fonte morta (username inexistente) faz a
    # resolução falhar e repetir a cada update. Com entidades já
    # resolvidas, a fonte morta foi descartada uma vez no boot.
    @client.on(events.NewMessage(chats=fontes))
    async def on_new(event):
        try:
            await processar(event, is_edit=False)
        except Exception as e:
            log_sys.error(f"❌ on_new: {e}", exc_info=True)

    @client.on(events.MessageEdited(chats=fontes))
    async def on_edit(event):
        try:
            await processar(event, is_edit=True)
        except Exception as e:
            log_sys.error(f"❌ on_edit: {e}", exc_info=True)

    # 5. Inicia tarefas de background
    asyncio.create_task(_health_check())
    asyncio.create_task(_iniciar_orchestrator())
    asyncio.create_task(_iniciar_servidor_web())

    # 6. Aguarda desconexão
    await client.run_until_disconnected()
    return True


# ── Loop principal com restart automático ────────────────────────
async def main() -> None:
  
    plataformas.inicializar()  # boot do catálogo (único por processo — garantido na própria função)

    while True:
        try:
            conectado = await _run()
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
            continue

        # _run() retornou sem exceção:
        #   False → sessão inválida: terminal, NÃO reiniciar;
        #   True  → desconexão limpa: reconecta (resiliência do worker).
        if not conectado:
            log_sys.error("❌ Sessão inválida — encerrando sem restart.")
            break

    # Shutdown limpo
    try:
        _EXECUTOR.shutdown(wait=False)
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
          
