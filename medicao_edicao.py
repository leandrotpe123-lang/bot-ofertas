"""Medição das EDIÇÕES — separada da publicação (⏱ [ENVIO_MS]).

Uma linha por edição medida, nos dois destinos, sem texto, URL ou código:

    ⏱ [EDICAO_MS] cid=31658.12 destino=principal op=sincronizacao troca_midia=sim fila_ms=0 ms=412 resultado=ok
    ⏱ [EDICAO_MS] cid=31658.12 destino=cupons op=sincronizacao troca_midia=sim fila_ms=415 ms=380 resultado=ok recon=editou_ref

  cid          post + sequência do processo. A operação do canal de cupons
               nasce da do principal e leva o MESMO cid.
  op           evolucao | sincronizacao | upgrade_midia (o aplicador que
               pediu); reconciliacao quando ninguém disse.
  fila_ms      do pedido até o I/O começar: a vaga do _SEM_ENVIO e, no canal
               de cupons, também a fila do post (inclusive esperar o
               principal terminar).
  ms           do começo do I/O ao fim — tentativas e o FloodWait que o
               Telethon dorme sozinho entram aqui.
  resultado    ok | falha | falha:<Classe da exceção>.
  recon        só cupons: o que a reconciliação fez (editou, editou_ref,
               igual, criou, recriou, apagou, obsoleta, sem_principal,
               nao_cupom, sem_mensagem).

CORRELAÇÃO COM A ESPERA DO TELETHON (espera_telethon): o Telethon dorme o
FloodWait DENTRO da task que fez a requisição. A operação medida fica
registrada pela task que a executa, então a linha ⏳ ganha o cid, o destino
e a op de QUEM esperou — ou cid=- quando nenhuma edição medida está em curso
naquela task (publicação, GetMessages fora de edição...). Sem ContextVar: as
tarefas do canal de cupons nascem num contexto vazio de propósito
(tests/test_eventos_tarefas.py).
"""
from __future__ import annotations

import asyncio
import itertools
import time
from typing import Optional

from logger import log_sys

__all__ = ["Medida", "novo_cid", "medir", "comecou", "atual"]

_seq = itertools.count(1)
_em_curso: dict = {}      # asyncio.Task -> [Medida] (pilha; uma por vez na prática)


def novo_cid(msg_id) -> str:
    return f"{msg_id}.{next(_seq)}"


class Medida:
    __slots__ = ("cid", "destino", "op", "troca", "t_pedido", "t_inicio",
                 "resultado", "recon")

    def __init__(self, cid, destino: str, op: str, troca, t_pedido=None):
        self.cid, self.destino, self.op = cid or "-", destino, op
        self.troca = bool(troca)
        self.t_pedido = time.monotonic() if t_pedido is None else t_pedido
        self.t_inicio = None
        self.resultado = None     # None = ok, se nenhuma exceção subir
        self.recon = None

    def rotulo(self) -> str:
        return f"cid={self.cid} destino={self.destino} op={self.op}"


class medir:
    """`with medir(m):` em volta da operação, DENTRO da task que faz o I/O.
    Na saída escreve a linha ⏱ [EDICAO_MS]; nunca derruba a operação."""

    def __init__(self, medida: Optional[Medida]):
        self.m, self._task = medida, None

    def __enter__(self):
        if self.m is not None:
            self._task = _task()
            if self._task is not None:
                _em_curso.setdefault(self._task, []).append(self.m)
        return self.m

    def __exit__(self, tipo, _exc, _tb):
        m = self.m
        if m is None:
            return False
        pilha = _em_curso.get(self._task)
        if pilha and m in pilha:
            pilha.remove(m)
            if not pilha:
                _em_curso.pop(self._task, None)
        try:
            fim = time.monotonic()
            inicio = m.t_inicio if m.t_inicio is not None else fim
            resultado = (f"falha:{tipo.__name__}" if tipo is not None
                         else (m.resultado or "ok"))
            log_sys.info(
                f"⏱ [EDICAO_MS] {m.rotulo()} troca_midia={'sim' if m.troca else 'nao'} "
                f"fila_ms={round((inicio - m.t_pedido) * 1000)} "
                f"ms={round((fim - inicio) * 1000)} resultado={resultado}"
                + (f" recon={m.recon}" if m.recon else ""))
        except Exception:                                  # noqa: BLE001
            pass                    # diagnóstico nunca derruba a edição
        return False


def comecou() -> None:
    """O I/O da edição em curso nesta task começa agora (a vaga do
    _SEM_ENVIO saiu). Só a 1ª vez conta: a fila é tudo o que veio antes."""
    m = atual()
    if m is not None and m.t_inicio is None:
        m.t_inicio = time.monotonic()


def atual() -> Optional[Medida]:
    """A edição medida em curso na task corrente, ou None."""
    pilha = _em_curso.get(_task())
    return pilha[-1] if pilha else None


def _task():
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None
