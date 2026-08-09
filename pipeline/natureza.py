"""
Camada B — NATUREZA DA OFERTA.

Responsabilidade única: decidir O QUE a mensagem é — cupom, produto
ou evento — compondo os detectores textuais de pipeline.assunto com
os fatos já derivados por pipeline.normalizacao.

Este módulo NÃO deriva identidade, NÃO emite chaves e NÃO conhece
família. Ele responde uma pergunta e só uma; identidade_oferta,
enriquecimento e deduplicação CONSOMEM a resposta sem reclassificar.

MB: P5 (classificação única — uma natureza por post, produzida uma
vez, que viaja imutável) e §4.2 (classificação × identidade são
responsabilidades distintas).
"""
from __future__ import annotations

from pipeline.assunto import (
    beneficio_e_de_loja,
    eh_post_cupom,
    tem_preco_de_item,
)
from logger import log_ded
from pipeline.normalizacao import MensagemNormalizada

__all__ = ["eh_entidade_cupom", "natureza"]


def eh_entidade_cupom(norm: MensagemNormalizada) -> bool:
    """[F-C4 / INV-E5] Decisão ÚNICA de natureza cupom-como-entidade
    (gate R1×R2 + R2+). Alimenta âncoras E canônica — nenhuma camada
    pode enxergar uma natureza diferente das demais."""
    texto = norm.texto_limpo
    return eh_post_cupom(texto) and (
        not norm.ids_globais
        or beneficio_e_de_loja(texto)
        or (len(norm.cupons) >= 2 and not tem_preco_de_item(texto)))


def natureza(norm: MensagemNormalizada) -> str:
    """Natureza ratificada da oferta: "cupom" | "produto" | "evento".

    Projeção da decisão acima — não é um segundo classificador. A
    ordem é a do MB: o assunto decide primeiro (R2), o fato de
    produto decide depois (R1), e "evento" é o resto.
    """
    log_ded.info(f"🔬 P2b id={norm.msg_id} assunto_cupom={eh_post_cupom(norm.texto_limpo)} ben_loja={beneficio_e_de_loja(norm.texto_limpo)} preco_item={tem_preco_de_item(norm.texto_limpo)} n_cupons={len(norm.cupons)} ids_globais={norm.ids_globais} camp={len(norm.chaves_campanha)}")
    if eh_entidade_cupom(norm):
        return "cupom"
    if norm.ids_globais:
        return "produto"
    return "evento"
