"""
INFRAESTRUTURA DA ENTIDADE ORIGEM — Fase 1 do MB Sistema de Vida.

Responsabilidade única: o vínculo persistente
    (chat, msg_id) → post lógico (dest)
hoje materializado pelo msg_id_dest físico corrente (decisão C1 do MB:
a substituição troca o corpo físico, nunca a identidade lógica).

INVARIANTES (contrato arquitetural — mais importante que o código):
 I1 Unicidade ....... uma origem possui no máximo UM post lógico ativo.
 I2 Edição preserva . um EDIT de origem vinculada nunca cria post novo.
 I3 Absorção ........ um NEW nunca publica se a origem já tem vínculo ativo.
 I4 Nascimento ...... toda criação de post grava o vínculo na mesma transação.
 I5 Substituição .... a troca do msg físico atualiza o vínculo na mesma operação.
 I6 Exclusão mútua .. eventos da mesma origem serializados por lock;
                      ordem global de aquisição: ORIGEM → identidade → post.
 I7 Fronteira ....... este módulo conhece SOMENTE a própria tabela e o
                      próprio lock — nunca oferta, campanha, cupom, score,
                      identidade ou decisão. A validação de VIDA do post
                      apontado é responsabilidade do chamador.

 ESCRITA (dono único: a tabela origem_post, via database) — dois gatilhos:
   NASCIMENTO ... db_registrar_post(chat_origem, msg_id_origem) — mesma
                  transação do post (I4/I5). Novo envio e substituição.
   ENCONTRO ..... origem.registrar() no funil pós-decisão — todo evento
                  que casa alvo existente, inclusive descartes (I1).
 Nenhum outro ponto do sistema escreve o vínculo.

Consumidores futuros previstos (MB Fases 3-4): replies e comentários
roteiam por reply_to=(chat, msg_id) → este vínculo → post da campanha.
"""
import asyncio
import time

from database import db_origem_get, db_origem_set

_LOCKS: dict = {}
_LOCKS_TS: dict = {}
_LOCKS_LCK = asyncio.Lock()


async def lock_origem(chat: str, msg_id: int) -> asyncio.Lock:
    """Lock dedicado por origem (I6). Mesmo idioma dos locks da casa:
    criação sob mutex + higiene por antiguidade."""
    chave = f"{chat}|{msg_id}"
    async with _LOCKS_LCK:
        lk = _LOCKS.get(chave)
        if lk is None:
            lk = asyncio.Lock()
            _LOCKS[chave] = lk
        _LOCKS_TS[chave] = time.monotonic()
        if len(_LOCKS) > 400:
            for k, _ in sorted(_LOCKS_TS.items(), key=lambda kv: kv[1])[:100]:
                velho = _LOCKS.get(k)
                if velho is not None and not velho.locked():
                    _LOCKS.pop(k, None)
                    _LOCKS_TS.pop(k, None)
    return lk


def consultar(chat: str, msg_id: int):
    """Devolve o dest cru do vínculo, ou None (I7: vida é do chamador)."""
    return db_origem_get(chat, msg_id)


def registrar(chat: str, msg_id: int, dest: int) -> None:
    """REPLACE idempotente do vínculo (I1)."""
    db_origem_set(chat, msg_id, dest)
