"""
FOGUETÃO v80.2 — Ponto de entrada.

Responsabilidades exclusivas deste módulo, em DOIS lifecycles
deliberadamente separados:

  PROCESSO (_preparar_processo — roda UMA vez, fora do laço):
    - boot do catálogo de plataformas (Auto Discovery)
    - globals, DB, handlers de evento e tarefas de fundo
    - resolução das fontes monitoradas

  CONEXÃO (_run — pode reexecutar a cada reconexão):
    - conectar, autorizar e aguardar desconexão, e NADA ALÉM DISSO

A separação é o contrato desta camada: uma reconexão NUNCA pode
acumular worker, health check, servidor, handler, semáforo ou
conexão de banco. Ver o bloco LIFECYCLE abaixo.

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

import globals as g

from client import client

import config
from config import (
    GRUPOS_ORIGEM, GRUPO_DESTINO,
    _PIL_OK,
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
            except Exception:
                n_links = "?"
            log_hc.info(
                f"💚 links={n_links} | claims={len(g._atomic_mem)} | "
                f"anti-loop={len(g._IDS_PROC)} | fila={len(g._buf)} "
                f"w={g._w_ativos} workers={_n_workers()} | "
                f"PIL={'OK' if _PIL_OK else 'OFF'}"
            )
        except Exception as e:
            log_hc.error(f"❌ Health: {e}", exc_info=True)


# ── Observabilidade de LIFECYCLE (E-P5) ───────────────────────────
# Bloco INERTE: não altera comportamento, não persiste nada, não faz
# rede. Emite UMA linha por ciclo de conexão — nunca por mensagem,
# nunca por chamada. Existe para PROVAR em produção as invariantes
# desta frente: `id()` idêntico entre ciclos significa que a
# reconexão não recriou o objeto. Removível isoladamente: apagar
# este bloco e as duas chamadas a _log_lifecycle.
_CICLO = 0
_TASKS_FUNDO: dict = {}


def _n_workers() -> int:
    """Quantos _worker_loop existem vivos. Prova direta da invariante
    'reconexão não cria novo worker', sem alterar orchestrator_fila."""
    try:
        return sum(
            1 for t in asyncio.all_tasks()
            if getattr(t.get_coro(), "__name__", "") == "_worker_loop"
        )
    except Exception:
        return -1


def _log_lifecycle(fase: str) -> None:
    try:
        n_tasks = len(asyncio.all_tasks())
    except Exception:
        n_tasks = -1
    try:
        n_handlers = len(client.list_event_handlers())
    except Exception:
        n_handlers = -1
    try:
        with _db() as _cx:
            id_db = id(_cx)
    except Exception:
        id_db = 0
    vivas = "".join(
        "1" if (t is not None and not t.done()) else "0"
        for t in (_TASKS_FUNDO.get("health"), _TASKS_FUNDO.get("web"))
    )
    log_sys.info(
        f"🔁 LIFECYCLE | fase={fase} ciclo={_CICLO} "
        f"tasks={n_tasks} handlers={n_handlers} workers={_n_workers()} "
        f"fundo={vivas} buf_evt={id(g._buf_evt)} "
        f"sem_http={id(config._SEM_HTTP)} "
        f"sem_envio={id(config._SEM_ENVIO)} db={id_db} "
        f"w={g._w_ativos} fila={len(g._buf)}"
    )


# ── Handlers de evento ────────────────────────────────────────────
def _registrar_handlers(fontes) -> None:
    """
    Registra os handlers SOBRE AS FONTES RESOLVIDAS. Registrar com a
    lista crua de usernames faz o Telethon re-resolver o filtro de
    chats; uma fonte morta faz a resolução falhar e repetir a cada
    update. Com entidades já resolvidas, a fonte morta foi descartada
    uma vez no boot.

    Chamada EXATAMENTE UMA VEZ, por _preparar_processo. Os handlers
    vivem no objeto client e sobrevivem a connect/disconnect — não há
    razão para re-registrar. Re-registrar acumulava callbacks e fazia
    cada EDIÇÃO ser processada N vezes, porque a idempotência do
    pipeline cobre apenas mensagens novas.
    """
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


# ── Lifecycle de PROCESSO — roda UMA vez ──────────────────────────
async def _preparar_processo() -> bool:
    """
    Boot único do processo, FORA do laço de restart.

    Aqui mora tudo cuja IDENTIDADE deve sobreviver a uma reconexão:
    primitivas asyncio, conexão do banco, handlers e tarefas de fundo.
    É a mesma razão já registrada para plataformas.inicializar(),
    estendida aos demais itens de processo.

    Devolve False quando o boot não pode prosseguir (sessão inválida
    ou nenhuma fonte resolvida). Nesse caso o processo termina sem
    entrar no laço de restart.
    """
    # 1. Locks, semáforos e caches — no loop correto, UMA vez.
    _init_globals()

    # 2. Banco de dados — UMA conexão por processo.
    _init_db()

    # 3. Primeira conexão ao Telegram.
    log_sys.info("🔌 Conectando...")
    await client.connect()
    if not await client.is_user_authorized():
        log_sys.error("❌ Sessão inválida")
        return False

    me = await client.get_me()
    log_sys.info(f"✅ {me.first_name} (@{me.username}) | ID={me.id}")
    log_sys.info(f"📡 {GRUPOS_ORIGEM} → {GRUPO_DESTINO}")

    # 4. Fontes resolvidas UMA vez por processo — decisão registrada:
    # alteração da lista de grupos monitorados exige redeploy.
    fontes = await precarregar_usernames(client, GRUPOS_ORIGEM)
    if not fontes:
        log_sys.error("❌ Nenhuma fonte de origem resolvida — encerrando.")
        return False

    # 5. Handlers — UMA vez.
    _registrar_handlers(fontes)

    # 6. Tarefas de fundo — UMA instância de cada por processo.
    # _iniciar_orchestrator é aguardado, não posto em task: ele apenas
    # loga e cria o _worker_loop, retornando de imediato. Envolvê-lo
    # numa task criava uma task supérflua que morria em seguida.
    _TASKS_FUNDO["health"] = asyncio.create_task(_health_check())
    await _iniciar_orchestrator()
    _TASKS_FUNDO["web"] = asyncio.create_task(_iniciar_servidor_web())

    _log_lifecycle("boot")
    return True


# ── Lifecycle de CONEXÃO — pode reexecutar ────────────────────────
async def _run() -> bool:
    """
    Ciclo de conexão. Contém EXCLUSIVAMENTE o que pertence à sessão do
    Telegram.

    NÃO inicializa globals, NÃO abre banco, NÃO registra handler e NÃO
    cria tarefa de fundo. Tudo isso é de processo e vive em
    _preparar_processo. Esta função pode rodar mil vezes sem acumular
    nada — é o contrato da Frente E.
    """
    global _CICLO
    _CICLO += 1

    if not client.is_connected():
        log_sys.info(f"🔌 Reconectando... (ciclo={_CICLO})")
        await client.connect()

    if not await client.is_user_authorized():
        log_sys.error("❌ Sessão inválida")
        return False

    log_sys.info("🚀 FOGUETÃO — ONLINE")
    _log_lifecycle("run")

    await client.run_until_disconnected()
    return True


# ── Loop principal com restart automático ────────────────────────
async def main() -> None:
    plataformas.inicializar()  # boot do catálogo (único por processo)

    # Boot do processo — fora do laço, pela MESMA razão do catálogo.
    if not await _preparar_processo():
        log_sys.error("❌ Boot interrompido — encerrando sem restart.")
        return

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


if __name__ == "__main__":
    asyncio.run(main())
  
