"""
SUCESSÃO — a fonte CHEFE apagou, outra fonte ainda segura o post: ela
vira a chefe e o post passa a espelhar a mensagem dela.

POR QUE EXISTE (Léo, 02/10): a chefe (LÍDER: a mensagem cujo texto está
no ar) apaga — "tem hora que posta com link errado" — e a versão certa
(a mesma fonte repostando, ou outra fonte) já tinha casado o post e sido
ignorada como duplicata. Sem sucessão, o post ficava com o texto que a
fonte retirou (o link errado) e as edições da fonte que sobrou não
espelhavam (só a líder sincroniza). "A segunda fonte já ser o chefe; se
ela apagar, no meu também apaga" — a remoção é da pipeline.origem_apagada.

RESPONSABILIDADE ÚNICA: escolher a sucessora e entregar o post a ela.
  1. só post VIVO (dentro do ciclo de vida_oferta) sem a líder entre as
     origens — post de ciclo encerrado segue congelado (doutrina);
  2. sucessora = a origem ligada mais recentemente (a repostagem
     corrigida, quando houver). A mensagem dela é buscada na fonte, FORA
     de qualquer lock; se não existe mais (exclusão que o Telegram não
     avisou), é tratada como apagada (ao_sumir) e passa-se à próxima;
  3. sob o lock do POST, numa transação, ela vira a líder
     (database.db_transferir_lideranca) — só se tudo ainda vale;
  4. a mensagem dela entra no ponto de entrada de sempre como EDIÇÃO da
     líder: a pipeline SINCRONIZA o post (texto, cupons, canal de cupons),
     com todas as travas e vereditos de sempre.

Uma rodada por post de cada vez; um pedido durante a rodada pede outra.
Dependências INJETADAS em `instalar` (cliente, fontes, ponto de entrada,
tratamento de origem sumida): este módulo não importa a orquestração nem
a exclusão.

NÃO faz: desligar origem nem remover post (pipeline.origem_apagada),
esquecer memória (pipeline.esquecimento), decidir o texto (decisao).
"""
from __future__ import annotations

import asyncio
import time
from typing import Callable

import globals as g
from database import db_get_post, db_origens_do_post, db_transferir_lideranca
from logger import log_out
from pipeline import exclusao
from pipeline.completude import EventoRecuperado
from pipeline.identidade import username_de
from pipeline.vida_oferta import viva

__all__ = ["instalar", "agendar"]

_estado: dict = {"cliente": None, "entidades": {}, "despachar": None,
                 "ao_sumir": None}
_EM_CURSO: dict = {}      # dest -> task da rodada
_DE_NOVO: set = set()     # dest com nova rodada pedida durante a atual


def instalar(cliente, fontes, despachar: Callable, ao_sumir: Callable) -> None:
    """Uma vez por processo. `despachar(evento, is_edit=)` é o ponto de
    entrada da pipeline; `ao_sumir(chat, ids)` trata origem que não existe
    mais na fonte como apagada."""
    from telethon.utils import get_peer_id
    _estado["cliente"] = cliente
    _estado["entidades"] = {get_peer_id(e): e for e in fontes}
    _estado["despachar"] = despachar
    _estado["ao_sumir"] = ao_sumir


def _nome(chat: str) -> str:
    u = username_de(chat)
    return f"@{u}" if u else chat


def agendar(dest: int) -> None:
    """Uma origem do post `dest` foi apagada e ele continua: confere a
    sucessão numa task própria. Sem instalar (ou encerrando), nada faz."""
    if _estado["cliente"] is None or g._encerrando:
        return
    t = _EM_CURSO.get(dest)
    if t is not None and not t.done():
        _DE_NOVO.add(dest)
        return
    t = asyncio.get_running_loop().create_task(_rodar(dest))
    _EM_CURSO[dest] = t
    t.add_done_callback(lambda _t, d=dest: _EM_CURSO.pop(d, None)
                        if _EM_CURSO.get(d) is _t else None)


async def _rodar(dest: int) -> None:
    try:
        while True:
            _DE_NOVO.discard(dest)
            await _suceder(dest)
            if dest not in _DE_NOVO or g._encerrando:
                return
    except asyncio.CancelledError:
        raise
    except Exception as e:                          # noqa: BLE001
        log_out.error(f"❌ sucessão post:{dest}: {e}", exc_info=True)


def _vivo(estado) -> bool:
    return bool(estado) and not estado.get("fused_into") \
        and not estado.get("delete_status") \
        and viva(estado.get("janela_fim") or 0.0, time.time())


def _lider_presente(estado, origens) -> bool:
    lider, lider_msg = estado.get("lider") or "", estado.get("lider_msg")
    if not lider:
        return True                     # sem líder registrada: nada a suceder
    if lider_msg is None:               # legado: líder por canal
        return any(c == lider for c, _ in origens)
    return (lider, lider_msg) in origens


async def _buscar(chat: str, msg_id: int):
    ent = _estado["entidades"].get(int(chat))
    if ent is None:
        raise LookupError(f"fonte {chat} não monitorada")
    msgs = await _estado["cliente"].get_messages(ent, ids=[msg_id])
    m = (msgs or [None])[0]
    if m is None or getattr(m, "action", None) is not None:
        return None
    return m


async def _suceder(dest: int) -> None:
    estado = db_get_post(dest)
    origens = db_origens_do_post(dest)
    if not estado or not origens or _lider_presente(estado, origens):
        return
    if not _vivo(estado):
        log_out.info(
            f"👑 [SUCESSAO] post:{dest} — a chefe apagou, mas o ciclo do "
            f"post já fechou: segue congelado até a última fonte apagar")
        return
    for chat, msg_id in origens:
        try:
            m = await _buscar(chat, msg_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:                      # noqa: BLE001
            log_out.warning(
                f"⚠️ [SUCESSAO] post:{dest} — busca de {_nome(chat)} "
                f"id={msg_id} falhou ({type(e).__name__}: {e}); nada muda")
            return
        if m is None:
            log_out.info(
                f"👑 [SUCESSAO] post:{dest} — {_nome(chat)} id={msg_id} não "
                f"existe mais na fonte: tratada como apagada")
            await _estado["ao_sumir"](chat, [msg_id])
            continue
        async with await exclusao.lock_post(dest):
            assumiu = db_transferir_lideranca(dest, chat, msg_id, time.time())
        if not assumiu:
            return                      # o post mudou: nada a fazer aqui
        log_out.info(
            f"👑 [SUCESSAO] post:{dest} — a chefe apagou; {_nome(chat)} "
            f"id={msg_id} assume e o post passa a espelhar a mensagem dela")
        await _estado["despachar"](EventoRecuperado(m, via="SUCESSAO"), is_edit=True)
        return
