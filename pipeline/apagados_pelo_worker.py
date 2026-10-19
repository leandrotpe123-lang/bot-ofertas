"""
Frente M — o que o PRÓPRIO worker apagou do canal de destino.

POR QUE EXISTE: o handler de exclusão do destino (pipeline.post_apagado)
recebe também as exclusões que o worker faz. A substituição de post
(pipeline.saida) apaga a mensagem antiga ANTES de o banco trocar de id: sem
esta anotação, o aviso dessa exclusão chegaria com o post ainda vivo no
banco e pareceria exclusão do dono.

REGRA: quem chama `client.delete_messages(GRUPO_DESTINO, id)` anota o id
aqui ANTES da chamada. O handler ignora o id anotado há menos de
_LEMBRAR_S. Só memória do processo: um restart perde a anotação, mas o post
apagado pelo worker já está encerrado ou fundido no banco (o handler também
ignora post que não está vivo).

Folha do grafo de imports (só stdlib): pipeline.saida importa daqui sem
ciclo.
"""
from __future__ import annotations

import time

__all__ = ["marcar", "foi_o_worker"]

_LEMBRAR_S = 15 * 60
_TETO = 2000
_ANOTADOS: dict = {}


def marcar(msg_id_dest) -> None:
    """Anota que o worker vai apagar `msg_id_dest` do destino."""
    try:
        mid = int(msg_id_dest)
    except (TypeError, ValueError):
        return
    agora = time.monotonic()
    _ANOTADOS.pop(mid, None)                 # reinserido no fim (mais novo)
    _ANOTADOS[mid] = agora
    if len(_ANOTADOS) > _TETO:
        for k in [k for k, ts in _ANOTADOS.items() if agora - ts >= _LEMBRAR_S]:
            del _ANOTADOS[k]
        while len(_ANOTADOS) > _TETO:
            del _ANOTADOS[next(iter(_ANOTADOS))]


def foi_o_worker(msg_id_dest) -> bool:
    """O worker apagou (ou vai apagar) este id há menos de _LEMBRAR_S?"""
    try:
        ts = _ANOTADOS.get(int(msg_id_dest))
    except (TypeError, ValueError):
        return False
    return ts is not None and time.monotonic() - ts < _LEMBRAR_S
