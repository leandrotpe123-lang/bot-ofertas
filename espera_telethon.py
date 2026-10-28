"""Espera automática de FloodWait do Telethon — visível no log, sem mudar nada.

O Telethon 1.45 dorme SOZINHO em todo FloodWait <= flood_sleep_threshold
(60 s; client.py não passa outro valor) e ainda adia toda requisição do
mesmo tipo até o prazo (`_flood_waited_requests`). Ele só registra isso em
INFO no logger `telethon.client.users`, que não tem handler: em produção a
mensagem some (só WARNING+ sai, pelo logging.lastResort). Foi assim que a
corrida dos envios de mídia dos cupons (~10 s, 02–23/10/2026) passou sem
uma linha de log.

Um FILTRO no próprio logger — não um handler: vê o registro da espera,
emite UMA linha nossa e devolve o registro intacto. O lastResort continua
sendo o único destino dos registros do Telethon (WARNING+ saem exatamente
como antes; INFO continua não impresso) e a sonda S6.1, quando ligada,
recebe tudo igual.

    ⏳ [ESPERA_TELETHON] req=EditMessageRequest seg=10 antecipada=nao cid=31658.12 destino=cupons op=upgrade_midia

Só o nome da classe da requisição, os segundos, se foi a espera
antecipada (requisição do mesmo tipo dentro do prazo de outra) e QUEM
esperou: a edição medida em curso na task que fez a requisição
(medicao_edicao — o cid casa com a linha ⏱ [EDICAO_MS]), ou cid=- quando
não há edição medida naquela task. Nunca argumento, texto, URL, nome do
canal ou corpo de resposta.

Reverter: remover a chamada `espera_telethon.instalar()` em main.py.
"""
from __future__ import annotations

import logging

import medicao_edicao
from logger import log_sys

__all__ = ["instalar"]

_LOGGER = "telethon.client.users"
# Modelo exato de telethon.client.users._fmt_flood (1.45.0); a comparação é
# pelo MODELO, nunca pelo texto formatado.
_MODELO = "Sleeping%s for %ds (%s) on %s flood wait"


class _Espera(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if record.msg == _MODELO and len(record.args) >= 4:
                antecipada, seg, _duracao, req = record.args[:4]
                quem = medicao_edicao.atual()
                log_sys.warning(
                    f"⏳ [ESPERA_TELETHON] req={req} seg={int(seg)} "
                    f"antecipada={'sim' if str(antecipada).strip() else 'nao'} "
                    f"{quem.rotulo() if quem is not None else 'cid=-'}")
        except Exception:                                  # noqa: BLE001
            pass                    # diagnóstico nunca derruba o envio
        return True                 # o registro segue intacto


_FILTRO = _Espera()


def instalar() -> None:
    """Uma vez por processo (main._preparar_processo). Idempotente."""
    lg = logging.getLogger(_LOGGER)
    if _FILTRO not in lg.filters:
        lg.addFilter(_FILTRO)
    if lg.getEffectiveLevel() > logging.INFO:
        lg.setLevel(logging.INFO)   # sem isto o INFO morre antes do filtro
