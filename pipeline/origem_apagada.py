"""
Entrada — a FONTE APAGOU a mensagem → o post sai do canal também.

POR QUE EXISTE (produção, 02/10): o post 24539 nasceu do fumotom 35483 e
casou o Promotom 110388; as duas fontes apagaram — e o 24539 ficou no ar.
O 24548 (fumotom 35488, apagada) idem. O bot não escutava exclusão.

REGRA (decisão do Léo, 02/10):
  · o post sai quando TODAS as mensagens de origem ligadas a ele foram
    apagadas — a que publicou e as que casaram depois com o mesmo post
    (vínculo de Origem). Enquanto uma fonte mantiver a oferta, ele fica;
  · no mesmo instante da exclusão: sem varredura e sem limite de idade
    além da retenção do vínculo (30 dias);
  · sai também do canal de cupons.

COMO:
  1. o handler de exclusão (instalar) só AGENDA uma task: o despacho do
     Telethon nunca espera lock nem banco;
  2. por id apagado, sob o lock de ORIGEM — o mesmo da publicação:
       · lembra a origem como apagada: NOVA/EDIÇÃO dessa mensagem ainda
         na fila não publica nada (publicacao.enviar confere sob o mesmo
         lock; a orquestração descarta antes, sem lock, para poupar
         trabalho);
       · lê o vínculo, trava o POST apontado (ordem da casa: ORIGEM →
         POST) e, numa transação, desfaz o vínculo — e, se não sobrou
         origem nenhuma, encerra o post (database.db_desvincular_origem);
  3. fora dos locks:
       · post encerrado → o bot ESQUECE a oferta (pipeline.esquecimento;
         o banco esquece na própria transação) e a remoção física é a da
         fusão (convergencia.agendar_remocao): tentativas, FloodWait,
         status persistido, canal de cupons e retomada no boot;
       · post mantido → se a apagada era a CHEFE, a fonte que sobrou
         assume (pipeline.sucessao).

CORRIDAS:
  · publicação da mesma origem em curso: termina antes (lock de ORIGEM);
    o post nasce com o vínculo e a exclusão o encontra;
  · exclusão antes da publicação: a lembrança impede o post;
  · outra fonte casando o mesmo post: o lock do POST serializa — ou ela
    registra o vínculo antes (o post fica), ou encontra o post encerrado
    e vira post novo (ela é uma fonte viva);
  · fusão e substituição só mudam vínculo sob o lock do post: relido sob
    o lock; se mudou, refaz no post novo.

LIMITES (do Telegram, não deste módulo):
  · o aviso de exclusão não é 100% garantido (documentação do Telethon);
  · exclusão feita com o bot fora do ar (deploy, restart) não chega.
"""
from __future__ import annotations

import asyncio
import time

import globals as g
from database import db_desvincular_origem
from logger import log_out
from pipeline import convergencia, esquecimento, exclusao, origem, sucessao
from pipeline.identidade import username_de

__all__ = ["instalar", "agendar", "apagadas", "apagada"]

# Lembrança das origens apagadas: cobre a mensagem que ainda está na
# fila quando a exclusão chega (segundos; minutos numa rajada). 2 h dá
# folga larga. O teto limita a memória.
_LEMBRAR_S = 2 * 3600
_TETO = 5000
_APAGADAS: dict = {}

# Vínculo que mudou sob o lock (fusão/substituição): refaz no post novo,
# no máximo isto de vezes.
_MAX_REFAZER = 3

# Tasks em voo (referência forte).
_TAREFAS: set = set()


def _chave(chat, msg_id) -> str:
    return f"{chat}|{msg_id}"


def _nome(chat: str) -> str:
    u = username_de(chat)
    return f"@{u}" if u else chat


def apagada(chat: str, msg_id: int) -> bool:
    """A fonte apagou esta mensagem? (lembrança de _LEMBRAR_S)"""
    ts = _APAGADAS.get(_chave(chat, msg_id))
    return ts is not None and time.monotonic() - ts < _LEMBRAR_S


def _lembrar(chat: str, msg_id: int) -> None:
    agora = time.monotonic()
    chave = _chave(chat, msg_id)
    _APAGADAS.pop(chave, None)              # reinserida no fim (mais nova)
    _APAGADAS[chave] = agora
    if len(_APAGADAS) > _TETO:
        for k in [k for k, ts in _APAGADAS.items() if agora - ts >= _LEMBRAR_S]:
            del _APAGADAS[k]
        while len(_APAGADAS) > _TETO:
            del _APAGADAS[next(iter(_APAGADAS))]


def instalar(cliente, fontes) -> None:
    """Liga o handler de exclusão sobre as FONTES RESOLVIDAS. Uma vez
    por processo (os handlers vivem no client e sobrevivem à reconexão).
    Exclusão de canal traz o id do canal: o filtro de chats funciona."""
    from telethon import events

    async def on_delete(event):
        try:
            agendar(event.chat_id, event.deleted_ids)
        except Exception as e:                      # noqa: BLE001
            log_out.error(f"❌ on_delete: {e}", exc_info=True)

    cliente.add_event_handler(on_delete, events.MessageDeleted(chats=fontes))


def agendar(chat_id, ids) -> None:
    """Agenda o tratamento numa task própria e volta na hora."""
    if g._encerrando or chat_id is None or not ids:
        return
    t = asyncio.get_running_loop().create_task(
        apagadas(str(chat_id), list(ids)))
    _TAREFAS.add(t)
    t.add_done_callback(_TAREFAS.discard)


async def apagadas(chat: str, ids) -> list:
    """Trata cada id apagado na fonte `chat`. Fora de todos os locks:
    post encerrado → esquece a oferta e agenda a remoção física na hora;
    post mantido → confere a sucessão da chefe. Devolve os posts
    encerrados."""
    encerrados = []
    for msg_id in ids:
        try:
            situacao, dest = await _uma(chat, int(msg_id))
        except asyncio.CancelledError:
            raise
        except Exception as e:                      # noqa: BLE001
            log_out.error(f"❌ origem apagada {_nome(chat)} id={msg_id}: {e}",
                          exc_info=True)
            continue
        if situacao in ("morto", "mantido"):
            esquecimento.origem_saiu(chat, int(msg_id), dest)
        if situacao == "morto":
            await esquecimento.post_saiu(dest)
            convergencia.agendar_remocao(dest, "ORIGEM_APAGADA")
            encerrados.append(dest)
        elif situacao == "mantido":
            sucessao.agendar(dest)
    return encerrados


async def _uma(chat: str, msg_id: int) -> tuple:
    """Uma origem apagada. Devolve (situação, post) — situação de
    database.db_desvincular_origem."""
    async with await origem.lock_origem(chat, msg_id):
        _lembrar(chat, msg_id)
        dest = origem.consultar(chat, msg_id)
        situacao, n = "sem_vinculo", 0
        for _ in range(_MAX_REFAZER + 1):
            if not dest:
                return "sem_vinculo", None
            async with await exclusao.lock_post(dest):
                situacao, n = db_desvincular_origem(chat, msg_id, dest,
                                                    time.time())
            if situacao != "mudou":
                break
            dest = n
        else:
            log_out.warning(
                f"⚠️ [ORIGEM_APAGADA] {_nome(chat)} id={msg_id} — vínculo "
                f"mudou {_MAX_REFAZER + 1}x sob o lock; nada removido")
            return "mudou", None
    if situacao == "morto":
        log_out.info(
            f"🗑 [ORIGEM_APAGADA] {_nome(chat)} id={msg_id} → post:{dest} "
            f"sem nenhuma origem no ar — removendo do canal")
    elif situacao == "mantido":
        log_out.info(
            f"🗑 [ORIGEM_APAGADA] {_nome(chat)} id={msg_id} → post:{dest} "
            f"MANTIDO — {n} origem(ns) ainda no ar")
    elif situacao in ("sem_post", "ja_removido"):
        log_out.info(
            f"🗑 [ORIGEM_APAGADA] {_nome(chat)} id={msg_id} → post:{dest} "
            f"{situacao}")
    elif situacao == "erro":
        log_out.warning(
            f"⚠️ [ORIGEM_APAGADA] {_nome(chat)} id={msg_id} → post:{dest} "
            f"falha no banco — nada removido")
    return situacao, dest
