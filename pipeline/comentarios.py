"""Repasse SELETIVO de comentários da fonte como RESPOSTA ao nosso post.

O canal-fonte responde (marca) o próprio post com "Precinho", "Esgotado ❌",
"VOLTOU!". Quando a resposta é um SINAL OBJETIVO (pipeline.comentario_sinal)
e o post respondido virou um post nosso, o canal principal responde o nosso
post com o mesmo texto. Nada além disso.

INDEPENDENTE DA OFERTA — o handler daqui só decide e AGENDA (microssegundos,
sem banco, sem rede, sem espera). A mensagem segue também para o pipeline
pelo handler de sempre, que a descarta como antes (sem link). Comentário
nunca vira oferta: este módulo não chama o pipeline e não cria post.

ÂNCORA — só o vínculo de origem (origem_post: chat, msg_id → nosso post),
o mesmo que roteia as edições. Inclui a duplicata: a resposta do Fada ao
post dele que caiu como DUP_SILENCIOSO no nosso post 34670 acha o 34670.
Sem âncora, espera em segundo plano (o post pode estar saindo, ou segurado
pela curadoria — até 300 s) e desiste: comentário sem destino não sai.

UM POR POST — database_comentarios: o primeiro comentário ELEGÍVEL reserva
a vaga (UNIQUE dest); o mesmo comentário nunca reserva duas vezes (evento
repetido, edição, reinício: PRIMARY KEY chat, msg_id). Edição de comentário
não é ouvida (só NewMessage). Destaque que o post já tem não repete e não
ocupa a vaga.

Fontes: as 5 públicas onde o corpus foi medido. Fora: o Nerd (só aval da
curadoria — nunca publica) e o Oferta Convertida (canal de teste/manual).
"""
from __future__ import annotations

import asyncio
import contextvars
import time
from typing import Optional

import config
import globals as g
from database import db_get_post
from database_comentarios import (ENVIADO, FALHOU, OK_RESERVA, db_comentario_desfecho,
                                  db_comentario_reservar)
from logger import log_out
from pipeline import comentario_sinal as sinal
from pipeline import origem, saida
from pipeline.exclusao import lock_post
from pipeline.filtros_linha import sem_convites

FONTES = frozenset(config.GRUPO_ID[n] for n in (
    "promotom", "fumotom", "SamuelF3lipePromo", "fadapromos", "fadadoscupons"))
_NOME = {cid: nome.lower() for nome, cid in config.GRUPO_ID.items()}

IDADE_MAX_S = 600.0          # comentário mais velho que isso (reentrega) não sai
# Espera pela âncora: ~6 min no total (curadoria segura até 300 s). Uma
# consulta local por degrau; o caso comum acha na primeira.
ESPERA_ANCORA_S = (1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 128.0)
PENDENTES_MAX = 50

_TAREFAS: set = set()
_EM_VOO: set = set()


def instalar(client, fontes) -> int:
    """Handler próprio, UMA vez por processo (main._preparar_processo),
    só sobre as fontes autorizadas. Devolve quantas."""
    from telethon import events
    from telethon.utils import get_peer_id
    alvos = [f for f in fontes if get_peer_id(f) in FONTES]
    if not alvos:
        log_out.warning("⚠️ comentários: nenhuma fonte autorizada resolvida")
        return 0

    @client.on(events.NewMessage(chats=alvos))
    async def on_comentario(event):
        try:
            ao_receber(event)
        except Exception as e:
            log_out.error(f"❌ comentário: {type(e).__name__}: {e}")

    log_out.info(f"💬 comentários: repasse seletivo ligado em {len(alvos)} fontes")
    return len(alvos)


def ao_receber(event, agora: Optional[float] = None) -> str:
    """SÍNCRONO e barato: decide e agenda. Devolve o desfecho imediato."""
    m = event.message
    rt = getattr(m, "reply_to", None)
    alvo = getattr(rt, "reply_to_msg_id", None) if rt is not None else None
    if not alvo:
        return "nao_e_resposta"                 # 99% das mensagens saem aqui
    chat = event.chat_id
    if chat not in FONTES:
        return "fonte_fora"
    peer = getattr(rt, "reply_to_peer_id", None)
    if peer is not None:
        from telethon.utils import get_peer_id
        if get_peer_id(peer) != chat:
            return "resposta_de_outro_chat"
    if not getattr(m, "post", False):
        return "nao_e_post_do_canal"
    if g._encerrando:
        return "encerrando"
    agora = time.time() if agora is None else agora
    data = getattr(m, "date", None)
    if data is None or agora - data.timestamp() > IDADE_MAX_S:
        return "velho"
    d = sinal.avaliar(getattr(m, "message", "") or "", _NOME.get(chat, ""), int(alvo))
    if not d.aceito:
        log_out.info(f"💬 [COMENTARIO_RECUSADO] {_NOME.get(chat, chat)} id={m.id} "
                     f"→ id={alvo} motivo={d.motivo}")
        return "recusado:" + d.motivo
    texto = sem_convites(d.texto)               # garantia final (nunca convite)
    if not texto:
        return "recusado:vazio"
    chave = (str(chat), int(m.id))
    if chave in _EM_VOO:
        return "em_voo"
    if len(_TAREFAS) >= PENDENTES_MAX:
        log_out.warning(f"⚠️ [COMENTARIO_CHEIO] {_NOME.get(chat, chat)} id={m.id} — "
                        f"{PENDENTES_MAX} pendentes")
        return "cheio"
    _EM_VOO.add(chave)
    _agendar(_repassar(str(chat), int(m.id), int(alvo), texto, d.familias))
    return "agendado"


def _agendar(coro) -> None:
    """Task própria, contexto VAZIO (o padrão da casa: curadoria,
    origem_apagada): vive mais que o handler que a pediu e não leva execução
    nenhuma. O término só tira a task do conjunto."""
    t = asyncio.get_running_loop().create_task(coro, context=contextvars.Context())
    _TAREFAS.add(t)
    t.add_done_callback(_TAREFAS.discard)


async def _esperar_ancora(chat: str, alvo: int) -> Optional[int]:
    dest = origem.consultar(chat, alvo)
    for espera in ESPERA_ANCORA_S:
        if dest is not None or g._encerrando:
            break
        await asyncio.sleep(espera)
        dest = origem.consultar(chat, alvo)
    return dest


def _vivo(post) -> bool:
    return bool(post) and not post.get("fused_into") and not post.get("delete_status")


async def _repassar(chat: str, msg_id: int, alvo: int, texto: str, familias) -> str:
    nome = _NOME.get(int(chat), chat)
    try:
        return await _repassar_um(chat, msg_id, alvo, texto, familias, nome)
    except Exception as e:
        log_out.error(f"❌ [COMENTARIO_FALHOU] {nome} id={msg_id}: {type(e).__name__}: {e}")
        return "erro"
    finally:
        _EM_VOO.discard((chat, msg_id))


async def _repassar_um(chat: str, msg_id: int, alvo: int, texto: str, familias,
                       nome) -> str:
    dest = await _esperar_ancora(chat, alvo)
    if dest is None:
        log_out.info(f"💬 [COMENTARIO_SEM_DESTINO] {nome} id={msg_id} → id={alvo} — "
                     f"o post respondido não está no canal")
        return "sem_destino"
    lk = await lock_post(dest)
    async with lk:
        post = db_get_post(dest)
        if not _vivo(post):
            log_out.info(f"💬 [COMENTARIO_POST_FORA] {nome} id={msg_id} → post:{dest}")
            return "post_fora"
        if sinal.ja_no_post(familias, post.get("texto") or ""):
            log_out.info(f"💬 [COMENTARIO_JA_NO_POST] {nome} id={msg_id} → post:{dest} "
                         f"{'/'.join(familias)}")
            return "ja_no_post"
        r = db_comentario_reservar(chat, msg_id, dest, texto, time.time())
        if r != OK_RESERVA:
            log_out.info(f"💬 [COMENTARIO_{r.upper()}] {nome} id={msg_id} → post:{dest}")
            return r
        try:
            resp = await saida.responder(dest, texto)
        except Exception as e:
            db_comentario_desfecho(chat, msg_id, FALHOU, None, time.time())
            log_out.error(f"❌ [COMENTARIO_FALHOU] {nome} id={msg_id} → post:{dest} "
                          f"{type(e).__name__}: {e}")
            return "falhou"
        rid = getattr(resp, "id", None)
        if not rid:
            db_comentario_desfecho(chat, msg_id, FALHOU, None, time.time())
            log_out.error(f"❌ [COMENTARIO_FALHOU] {nome} id={msg_id} → post:{dest} "
                          f"sem prova do envio")
            return "falhou"
        db_comentario_desfecho(chat, msg_id, ENVIADO, int(rid), time.time())
        log_out.info(f"💬 [COMENTARIO_OK] {nome} id={msg_id} → post:{dest} "
                     f"resposta={rid} texto={texto!r}")
        return "enviado"
