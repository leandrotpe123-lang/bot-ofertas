"""Camada 2 — Orquestração / Sequência do pipeline.

Responsabilidade ÚNICA: chamar as camadas na ordem correta e propagar
is_edit até a publicação. Não toma decisão de negócio, não conhece
fila, heap, worker nem prioridade.

Lê g._buf e g._w_ativos SOMENTE para compor telemetria (profundidade
de fila e workers ativos nas linhas de log). Não muta esse estado — a
máquina de concorrência é de pipeline.orchestrator_fila.

CONTRATO INTERNO DA CAMADA — LEIA ANTES DE ALTERAR:
  _pipeline tem prefixo `_`, mas NÃO é privado deste arquivo: é
  chamado por _worker_loop em pipeline.orchestrator_fila. O underscore
  foi PRESERVADO na extração para não renomear nada fora de escopo —
  dívida registrada, não descuido. Não é reexportado por
  pipeline.orchestrator.

Extraído de pipeline.orchestrator sem qualquer alteração de
comportamento.
"""
from __future__ import annotations

import eventos
import globals as g
from logger import log_sys
from pipeline.deduplicacao import deve_enviar_async
from pipeline.enriquecimento import enriquecer_edicao, enriquecer
from pipeline.identidade import checar_e_marcar
from pipeline.ingestao import ingerir
from pipeline.montagem import montar
from pipeline.normalizacao import normalizar
from pipeline import origem_apagada
from pipeline.publicacao import destino_vivo_de_origem, enviar


# ── Pipeline principal ────────────────────────────────────────────
async def _pipeline(event, is_edit: bool = False) -> None:
    """
    Fluxo da pipeline. Chama cada camada na ordem e propaga is_edit
    até a publicação. Não toma decisão de negócio.

    [F1.2-A2-E] Cada descarte daqui sai também como origem.descartada,
    logo depois do log de sempre: ponto PRE (fora de qualquer lock) e a
    exceção só pela classe, em `excecao`. O que acontece dentro de enviar
    é da C1.
    """
    msg_id = event.message.id

    # ── Camada 1: Ingestão (primeiro — fornece o chat canônico) ──
    try:
        bruta = await ingerir(event)
    except Exception as e:
        log_sys.error(f"❌ ingestao: {e}")
        eventos.emitir_de("origem.descartada", lambda: (
            {"chat": str(event.chat_id), "msg": msg_id},
            {"motivo": "ERRO_INGESTAO", "etapa": "PIPELINE", "ponto": "PRE",
             "is_edit": bool(is_edit), "efeitos_parciais": "NENHUM",
             "excecao": type(e).__name__}),
            local="orchestrator_pipeline.pipeline.erro_ingestao")
        return

    # ── Origem APAGADA na fonte enquanto esperava: nem entra (nova ou
    #    edição). Best-effort sem lock; a autoridade é a publicação.
    if origem_apagada.apagada(bruta.chat, msg_id):
        log_sys.info(
            f"🧭 TL | id={msg_id} chat={bruta.chat} | DESCARTE | "
            f"motivo=ORIGEM_APAGADA")
        eventos.emitir_de("origem.descartada", lambda: (
            {"chat": bruta.chat, "msg": msg_id},
            {"motivo": "ORIGEM_APAGADA", "etapa": "PIPELINE", "ponto": "PRE",
             "is_edit": bool(is_edit), "efeitos_parciais": "NENHUM"}),
            local="orchestrator_pipeline.pipeline.origem_apagada")
        return

    # ── Idempotência (somente novas) — chave (chat canônico, msg_id) ──
    if not is_edit and await checar_e_marcar(f"{bruta.chat}:{msg_id}"):
        log_sys.info(
            f"🧭 TL | id={msg_id} chat={bruta.chat} | DESCARTE | "
            f"motivo=JA_PROCESSADO")
        eventos.emitir_de("origem.descartada", lambda: (
            {"chat": bruta.chat, "msg": msg_id},
            {"motivo": "JA_PROCESSADO", "etapa": "PIPELINE", "ponto": "PRE",
             "is_edit": bool(is_edit), "efeitos_parciais": "NENHUM"}),
            local="orchestrator_pipeline.pipeline.ja_processado")
        return

    # ── Fast-path ORIGEM (Fase 1): NEW de origem já publicada nem entra
    #    no pipeline — evita efeito colateral no cupom_idx, claim e SAT.
    #    Best-effort sem lock; a autoridade (com lock) é a publicação.
    if not is_edit:
        _dv = destino_vivo_de_origem(bruta.chat, msg_id)
        if _dv:
            log_sys.info(
                f"🧭 TL | id={msg_id} chat={bruta.chat} | DESCARTE | "
                f"motivo=ORIGEM_JA_PUBLICADA dest={_dv}")
            eventos.emitir_de("origem.descartada", lambda: (
                {"chat": bruta.chat, "msg": msg_id},
                {"motivo": "ORIGEM_JA_PUBLICADA", "etapa": "PIPELINE", "ponto": "PRE",
                 "is_edit": bool(is_edit), "efeitos_parciais": "POSSIVEIS",
                 "dest": _dv}),
                local="orchestrator_pipeline.pipeline.origem_ja_publicada")
            return
    log_sys.info(
        f"{'✏️' if is_edit else '📩'} @{bruta.chat} | "
        f"id={msg_id} | q={len(g._buf)} w={g._w_ativos}"
    )

    # ── Camada 2: Normalização ────────────────────────────────────
    try:
        norm = await normalizar(bruta)
    except Exception as e:
        log_sys.error(f"❌ normalizar: {e}")
        eventos.emitir_de("origem.descartada", lambda: (
            {"chat": bruta.chat, "msg": msg_id},
            {"motivo": "ERRO_NORMALIZAR", "etapa": "PIPELINE", "ponto": "PRE",
             "is_edit": bool(is_edit), "efeitos_parciais": "POSSIVEIS",
             "excecao": type(e).__name__}),
            local="orchestrator_pipeline.pipeline.erro_normalizar")
        return
    if norm is None:
        log_sys.info(
            f"🧭 TL | id={msg_id} chat={bruta.chat} | DESCARTE | "
            f"motivo=NORMALIZACAO_VAZIA")
        eventos.emitir_de("origem.descartada", lambda: (
            {"chat": bruta.chat, "msg": msg_id},
            {"motivo": "NORMALIZACAO_VAZIA", "etapa": "PIPELINE", "ponto": "PRE",
             "is_edit": bool(is_edit), "efeitos_parciais": "POSSIVEIS",
             "sub_motivo": "DESCONHECIDO", "n_links": len(bruta.links)}),
            local="orchestrator_pipeline.pipeline.normalizacao_vazia")
        return

    # ── Camada 3a: Enriquecimento (derivados prontos p/ consumo) ─
    #   Roda em AMBOS os caminhos. A edição usa a porta PURA (derivar),
    #   preservando que o efeito de memória de cupom não ocorre em
    #   edições — a fronteira é a função, não o if.
    # ── Camada 3: Deduplicação + saturação (somente novas) ───────
    # Um só produtor de derivados (P2/P5): derivar() é puro e roda em
    # ambos os caminhos; enriquecer() adiciona o efeito de memória de
    # cupom, exclusivo da publicação nova (P9).
    enr = enriquecer_edicao(norm) if is_edit else enriquecer(norm)
    if not is_edit:
        try:
            if not await deve_enviar_async(enr):
                log_sys.info(
                    f"🧭 TL | id={msg_id} chat={norm.chat} | DESCARTE | "
                    f"motivo=DEDUP")
                eventos.emitir_de("origem.descartada", lambda: (
                    {"chat": bruta.chat, "msg": msg_id},
                    {"motivo": "DEDUP", "etapa": "PIPELINE", "ponto": "PRE",
                     "is_edit": bool(is_edit), "efeitos_parciais": "POSSIVEIS",
                     "sub_motivo": "DESCONHECIDO"}),
                    local="orchestrator_pipeline.pipeline.dedup")
                return
        except Exception as e:
            log_sys.error(f"❌ deve_enviar: {e}")
            eventos.emitir_de("origem.descartada", lambda: (
                {"chat": bruta.chat, "msg": msg_id},
                {"motivo": "ERRO_DEDUP", "etapa": "PIPELINE", "ponto": "PRE",
                 "is_edit": bool(is_edit), "efeitos_parciais": "POSSIVEIS",
                 "excecao": type(e).__name__}),
                local="orchestrator_pipeline.pipeline.erro_dedup")
            return


    # ── Camada 4: Montagem ────────────────────────────────────────
    try:
        montada = await montar(norm)
    except Exception as e:
        log_sys.error(f"❌ montar: {e}")
        eventos.emitir_de("origem.descartada", lambda: (
            {"chat": bruta.chat, "msg": msg_id},
            {"motivo": "ERRO_MONTAR", "etapa": "PIPELINE", "ponto": "PRE",
             "is_edit": bool(is_edit), "efeitos_parciais": "POSSIVEIS",
             "excecao": type(e).__name__}),
            local="orchestrator_pipeline.pipeline.erro_montar")
        return

    # ── Camada 5: Publicação ──────────────────────────────────────
    await enviar(montada, norm=norm, enr=enr, is_edit=is_edit)

