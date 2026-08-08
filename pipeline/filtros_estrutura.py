# pipeline/filtros_estrutura.py — VOCABULÁRIO ESTRUTURAL DO POST
#
# Responsabilidade ÚNICA: nomear FORMAS do texto — o que é uma URL
# numa linha, o que é uma enumeração de bloco, o que é um rótulo.
# NÃO decide nada: não remove linha, não remove bloco, não consulta
# registry nem categorias universais.
#
# ══════════════════════════════════════════════════════════════════
# CONTRATO INTERNO DA CAMADA — LEIA ANTES DE ALTERAR
# ══════════════════════════════════════════════════════════════════
# _RE_URL_BLOCO, _RE_ENUM e _eh_rotulo têm prefixo `_`, mas NÃO são
# privados deste arquivo: são o contrato interno da camada de
# filtros, importados por filtros_linha e filtros_bloco. O underscore
# foi PRESERVADO deliberadamente na extração, para não renomear nada
# fora do escopo daquele front — é dívida registrada, não descuido.
#
# Consequências:
#   - alterar qualquer um dos três afeta OS DOIS passes de filtro;
#   - nenhum dos três é reexportado por pipeline.filtros: eles não
#     fazem parte do contrato público da camada, apenas do interno.
#
# _eh_rotulo mantém uma ÚNICA definição de "linha que anuncia
# conteúdo, mas não é conteúdo" em toda a camada — invariante já
# declarado na docstring de _podar_rotulo_orfao.
#
# Extraído de pipeline.filtros sem qualquer alteração de comportamento.
from __future__ import annotations

import re

_RE_URL_BLOCO = re.compile(r'https?://\S+')

# Enumeração de bloco em todas as grafias observadas:
# 1️⃣ | ① | (1) | 1. | 1) | 1 -
_RE_ENUM = re.compile(
    r'^\s*(?:'
    r'[0-9]\uFE0F?\u20E3'
    r'|[\u2460-\u2473]'
    r'|\(\s*\d{1,2}\s*\)'
    r'|\d{1,2}\s*[.)\-–]'
    r'|\d{1,2}\s*(?=https?://)'
    r')\s*'
)


def _eh_rotulo(linha: str) -> bool:
    """Linha que anuncia a URL seguinte (rótulo), não conteúdo."""
    l = linha.strip()
    return l.endswith(":") or bool(_RE_ENUM.match(l))

