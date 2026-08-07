"""
Camada 3 — Normalização / Forma do texto.

Responsabilidade ÚNICA: normalizar a FORMA de uma string e responder
perguntas sobre a forma dela.

NÃO conhece URL, plataforma, identidade, cupom nem pipeline. A única
dependência é `re`.

NÃO decide política de conteúdo — isso é de pipeline.filtros,
consumido em pipeline.normalizacao.normalizar.

Extraído de pipeline.normalizacao sem qualquer alteração de
comportamento. As formas aqui são idênticas às originais.
"""
from __future__ import annotations

import re

__all__ = ["limpar_texto", "_tem_emoji"]

# ─────────────────────────────────────────────────────────────────
# LIMPEZA DE TEXTO
# ─────────────────────────────────────────────────────────────────
_RE_INVISIVEIS = re.compile(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]')
_RE_EMOJI_CHK = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF\u2B50\u2B55]"
)


def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI_CHK.search(s))


def limpar_texto(texto: str) -> str:
    """Normaliza a FORMA do texto: remove caracteres invisíveis e
    unifica quebras de linha. NÃO decide política de conteúdo — isso
    é de pipeline.filtros, consumido em `normalizar`.
    """
    return (
        _RE_INVISIVEIS.sub(" ", texto)
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )
    
