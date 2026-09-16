"""Camada 5 — Montagem: texto formatado, imagem e dataclass MensagemMontada."""
#
# Implementação: pipeline.montagem_texto (renderização do texto) e
# pipeline.montagem_imagem (obtenção da imagem). Este arquivo retém o
# CONTRATO de saída e o ORQUESTRADOR, e reexporta montar_texto e
# preparar_imagem_tg para preservar a acessibilidade que já existia.
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from pipeline.normalizacao import MensagemNormalizada
from pipeline.montagem_imagem import _resolver_imagem, preparar_imagem_tg
from pipeline.montagem_texto import montar_texto

# ─────────────────────────────────────────────────────────────────
# Dataclass de saída
# ─────────────────────────────────────────────────────────────────

@dataclass
class MensagemMontada:
    msg_id:        int
    chat:          str
    plat:          str
    sku:           str
    texto:         str
    imagem:        object
    mapa:          Dict[str, str]
    msg_id_origem: int


# ─────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────────

async def montar(
    norm: MensagemNormalizada,
) -> MensagemMontada:
    """[E5.0] Monta o TEXTO. NÃO materializa a imagem.

    `imagem` nasce None e só deixa de ser None no ponto único de
    materialização (`materializar_imagem`), invocado pela publicação
    DEPOIS da decisão. Antes desta frente o download acontecia aqui,
    antes de existir qualquer decisão — e era pago inteiro por todo
    evento que terminava em PRESERVA.

    Continua async: o contrato com o orquestrador não muda."""

    texto = montar_texto(norm)

    return MensagemMontada(
        msg_id=norm.msg_id,
        chat=norm.chat,
        plat=norm.plat,
        sku=norm.sku,
        texto=texto,
        imagem=None,
        mapa=norm.mapa,
        msg_id_origem=norm.msg_id,
    )


# ─────────────────────────────────────────────────────────────────
# [E5.0] PONTO ÚNICO DE MATERIALIZAÇÃO
# ─────────────────────────────────────────────────────────────────

async def materializar_imagem(norm: MensagemNormalizada) -> object:
    """FACHADA ÚNICA de materialização dos bytes da imagem.

    É a única função do sistema autorizada a chamar o resolvedor
    (`montagem_imagem._resolver_imagem`), que permanece inalterado —
    esta frente NÃO cria um segundo resolver.

    Recusa a materialização quando não há mídia candidata, e devolve
    None nesse caso. None também é o resultado de qualquer falha do
    resolvedor (timeout, retorno vazio, buffer inválido): a distinção
    entre `norm.tem_midia` (o Telegram TEM mídia) e `montada.imagem`
    (os bytes EXISTEM) é preservada, e quem chama é obrigado a tratar
    o None — na TROCA, redecidindo com `midia_candidata=False`.

    Tolera `norm is None` porque o caminho de publicação nova é
    alcançável sem normalizada."""
    if norm is None or not norm.tem_midia:
        return None
    return await _resolver_imagem(norm)
    
