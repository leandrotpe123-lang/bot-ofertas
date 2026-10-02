"""
ESQUECIMENTO — o post saiu porque TODAS as fontes apagaram: o bot
esquece a oferta, para que a fonte postando de novo seja reconhecida.

POR QUE EXISTE (Léo, 02/10): "tem hora que posta com link errado" — a
fonte apaga e posta de novo. A repostagem tem de entrar como oferta
nova, sem nada do post apagado no caminho.

RESPONSABILIDADE ÚNICA: a memória do PROCESSO amarrada ao post apagado.
  · reserva de REATIVAÇÃO ("VOLTOU"): o anti-flood (pipeline.deduplicacao)
    segura a identidade por até 10 min. Sem soltar, o "VOLTOU" reposta
    pela fonte seria barrado como flood. A reserva é da MENSAGEM que a
    tomou; quando ela é apagada e o post continua (outra fonte segura),
    a reserva passa para o post — e só é solta quando o post sai. Soltar
    antes deixaria outro "VOLTOU" renascer um post que continua no ar;
  · chave da mídia aceita do post (g._MIDIA_ACEITA).

O BANCO esquece na mesma transação que encerra o post
(database.db_desvincular_origem): composição exibida e âncoras
(oferta_index) — o post apagado não é dono de nada. A memória de códigos
de cupom (cupom_idx) NÃO é apagada: ela é da corrente de identidade, não
de um post, pode estar servindo posts vivos e não impede a repostagem
(só dá nome à identidade).

NÃO faz: decidir que o post sai (pipeline.origem_apagada), remover do
canal (pipeline.convergencia), sucessão de chefe (pipeline.sucessao).
"""
from __future__ import annotations

import time

import globals as g
from pipeline.deduplicacao_claim import _liberar

__all__ = ["reserva", "origem_saiu", "post_saiu"]

# Mesma validade máxima de um claim (deduplicacao_claim._ATOMIC_TTL_MAX):
# depois disso a reserva já expirou sozinha. Teto de entradas: memória.
_VALIDADE_S = 4 * 60 * 60
_TETO = 2000

_DA_ORIGEM: dict = {}     # "chat|msg_id" -> (fp, ts)
_DO_POST: dict = {}       # msg_id_dest   -> {fp: ts}


def _chave(chat, msg_id) -> str:
    return f"{chat}|{msg_id}"


def _podar(mapa: dict, ts_de) -> None:
    if len(mapa) <= _TETO:
        return
    agora = time.monotonic()
    for k in [k for k, v in mapa.items() if agora - ts_de(v) > _VALIDADE_S]:
        del mapa[k]
    while len(mapa) > _TETO:
        del mapa[next(iter(mapa))]


def reserva(chat: str, msg_id: int, fp: str) -> None:
    """A mensagem (chat, msg_id) tomou a reserva de reativação `fp`."""
    _DA_ORIGEM[_chave(chat, msg_id)] = (fp, time.monotonic())
    _podar(_DA_ORIGEM, lambda v: v[1])


def origem_saiu(chat: str, msg_id: int, dest: int) -> None:
    """A origem foi apagada e desligada do post `dest`: a reserva que ela
    tomou passa a ser do post."""
    item = _DA_ORIGEM.pop(_chave(chat, msg_id), None)
    if item is not None:
        _DO_POST.setdefault(dest, {})[item[0]] = item[1]
        _podar(_DO_POST, lambda v: max(v.values(), default=0.0))


async def post_saiu(dest: int) -> int:
    """O post saiu (todas as fontes apagaram): solta as reservas dele e a
    chave de mídia. Devolve quantas reservas soltou."""
    g.midia_aceita_drop(dest)
    soltas = 0
    for fp in _DO_POST.pop(dest, {}):
        soltas += bool(await _liberar(fp))
    return soltas
