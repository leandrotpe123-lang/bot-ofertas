"""Camada 2 — Orquestração / Admissão e dispatch.

Responsabilidade ÚNICA: a máquina de concorrência — teto de admissão,
lane sequencial por origem, orçamento global de execução e o start do
dispatch. Detém os parâmetros operacionais e é a única a MUTAR g._buf
e g._w_ativos.

DOIS LIMITES, RESPONSABILIDADES DIFERENTES — NÃO CONFUNDIR:
  ADMISSÃO ≤ _FILA_MAX (200)   quantos eventos podem existir pendentes
                               em memória. Protege contra crescimento
                               ilimitado de tasks.
  EXECUÇÃO ≤ _ORCAMENTO (16)   quantos pipelines rodam ao mesmo tempo.
                               Para-choque de execução.

O orçamento NÃO substitui a admissão. Medido: numa rajada de mil
eventos da MESMA origem, a lane serializa em UM pipeline e ocupa UMA
única vaga do orçamento, enquanto as mil tasks existem juntas. Quem
contém isso é o teto de admissão, herdado da fila anterior.

ORDEM: LANE → ORÇAMENTO → pipeline. A lane vem primeiro para que
eventos da mesma origem enfileirem SEM consumir vaga do orçamento; o
inverso deixaria as demais origens em inanição.

A lane usa utils.uma_por_vez com namespace próprio `lane|chat`: a
unidade de exclusão é a ORIGEM, não a mensagem.
NÃO usa origem.lock_origem: publicacao.enviar já o adquire e
asyncio.Lock não é reentrante — reutilizá-lo seria auto-deadlock. A
chave da lane é derivada de event.chat_id, que é exatamente o que
identidade.chat_canonico devolve, então lane e lock de origem falam da
mesma origem por caminhos independentes.

A lane é o PRIMEIRO await da corrotina despachada. Nenhum await a
precede, logo a task corre síncrona até o lock e a ordem de aquisição
é a ordem de criação da task: eventos da mesma origem preservam ordem.

NÃO conhece a sequência das camadas: delega a _pipeline, em
pipeline.orchestrator_pipeline. Não importa pipeline.orchestrator.

CONTRATO INTERNO DA CAMADA — LEIA ANTES DE ALTERAR:
  _enfileirar tem prefixo `_`, mas é chamado por `processar` em
  pipeline.orchestrator. Nome e assinatura PRESERVADOS — renomear
  estaria fora do escopo desta frente. _iniciar_orchestrator é
  reexportado pela fachada e consumido por main.py.
"""
from __future__ import annotations

import asyncio

import globals as g
from logger import log_sys
from pipeline.orchestrator_pipeline import _pipeline
from pipeline.vida_oferta import VIDA_OFERTA_S
from utils.uma_por_vez import uma_por_vez


# ── Parâmetros operacionais ───────────────────────────────────────
_FILA_MAX   = 200   # teto de ADMISSÃO — eventos pendentes em memória
_ORCAMENTO  = 16    # teto de EXECUÇÃO — pipelines simultâneos

# Semáforo de nível de módulo, criado no import. Precedente na casa:
# origem._LOCKS_LCK faz o mesmo e roda em produção. Fora de globals de
# propósito: _init_globals pertence ao lifecycle e não é escopo desta
# frente. O processo tem um único event loop; a reconexão acontece
# dentro dele.
_orcamento = asyncio.Semaphore(_ORCAMENTO)


# ── Execução de um evento ─────────────────────────────────────────
async def _executar(event, is_edit: bool) -> None:
    """
    Já dentro da lane da origem. Toma vaga do orçamento e roda o
    pipeline. g._w_ativos conta pipelines EM EXECUÇÃO — o incremento e
    o decremento são síncronos, atômicos no event loop de thread única.
    """
    async with _orcamento:
        g._w_ativos += 1
        try:
            await _pipeline(event, is_edit)
        finally:
            g._w_ativos -= 1


async def _blindado(event, is_edit: bool) -> None:
    """
    Isolamento por evento: uma falha não contamina as demais tasks.
    Substitui o try/except que vivia no laço do worker. CancelledError
    é BaseException e é repassada — cancelamento não é falha.
    """
    try:
        await _executar(event, is_edit)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log_sys.error(f"❌ Worker: {e}", exc_info=True)


# ── Admissão e dispatch ───────────────────────────────────────────
async def _enfileirar(event, is_edit: bool) -> None:
    """
    Admite o evento e despacha imediatamente. Não há fila, heap,
    prioridade nem espera artificial: eventos da mesma origem são
    serializados pela lane; origens diferentes podem executar em paralelo,
    respeitando o orçamento global.

    O teto de admissão preserva a proteção que existia na fila: sem
    ele, uma rajada criaria uma task por evento sem limite algum.
    len(g._buf) e a inserção são síncronos — não há await entre a
    verificação e o add, logo a checagem é atômica.
    """
    if len(g._buf) >= _FILA_MAX:
        log_sys.warning(f"⚠️ Fila cheia | id={event.message.id}")
        return

    chave = f"lane|{event.chat_id}"
    tarefa = asyncio.create_task(uma_por_vez(chave, _blindado, event, is_edit))

    # Referência forte obrigatória: create_task sem referência permite
    # que o coletor descarte a task no meio da execução. O callback
    # remove a entrada quando a task termina.
    g._buf.add(tarefa)
    tarefa.add_done_callback(g._buf.discard)


async def _iniciar_orchestrator() -> None:
    from config import _MAX_EDITS  # noqa: log only
    log_sys.info(
        f"🎛 Orchestrator | orcamento={_ORCAMENTO} admissao={_FILA_MAX} "
        f"vida_oferta={VIDA_OFERTA_S}s "
        f"max_edits={_MAX_EDITS}"
    )
