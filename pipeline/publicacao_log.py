"""Camada 6 — Publicação / Observabilidade da decisão.

Responsabilidade ÚNICA: registrar o veredito de decidir() e o
detalhe por motivo. SEM EFEITO DE FLUXO — o controle de fluxo
(evoluir/descartar) permanece no chamador, em pipeline.publicacao.

Não envia, não persiste, não decide.

Extraído de pipeline.publicacao sem qualquer alteração de
comportamento.
"""
from __future__ import annotations

from logger import log_out

def _log_decisao(d, montada, norm, estado: dict, score: int,
                 agora: float, identity: str) -> None:
    """Observabilidade da decisão de evolução. Sem efeito de fluxo:
    apenas registra o veredito de decidir() e o detalhe por motivo.
    O controle de fluxo (evoluir/descartar) permanece no chamador."""
    log_out.debug(
        f"🧭 TL | id={montada.msg_id} chat={norm.chat} | DECISAO | dest={identity} "
        f"motivo={d.motivo} "
        f"na_janela={'sim' if d.na_janela else 'nao'} "
        f"score {d.score_atual}→{score} janela_restante="
        f"{max(0.0, (estado.get('janela_fim', 0) or 0) - agora):.1f}s")

    if d.motivo == "JANELA_ENCERRADA":
        log_out.info(
            f"🔒 [JANELA_ENCERRADA] {identity} "
            f"oferta encerrada (ciclo de vida expirado) "
            f"candidato={norm.chat}")
    elif d.motivo == "EVOLUCAO_LIMITE_ATINGIDO":
        log_out.info(
            f"🔒 [EVOLUCAO_LIMITE_ATINGIDO] {identity} "
            f"já evoluiu {estado.get('edit_count', 0) or 0}x na janela "
            f"candidato={norm.chat}")
    elif d.motivo == "EVOLUI":
        log_out.info(
            f"✳️ [EVOLUI] {identity} "
            f"score {d.score_atual}→{score} "
            f"{'(janela)' if d.na_janela else '(lider)'} "
            f"chat={norm.chat} "
            f"img_nova={'sim' if montada.imagem else 'não'}")
    elif d.motivo == "TROCA_IMG_BOA":
        log_out.info(
            f"🖼 [TROCA_IMG_BOA] {identity} "
            f"de {estado.get('lider','')} (ruim) → {norm.chat} (bom) "
            f"delta={d.delta}s")
    elif d.motivo == "DUP_SILENCIOSO":
        log_out.debug(
            f"🔁 [DUP_SILENCIOSO] {identity} sim={d.sim:.2f}")
    elif d.motivo == "SCORE_NAO_EVOLUI":
        log_out.info(
            f"🔁 [SCORE_NAO_EVOLUI] {identity} "
            f"atual={score} salvo={d.score_atual} chat={norm.chat}")

