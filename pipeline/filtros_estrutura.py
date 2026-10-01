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
from pipeline.normalizacao_texto import sem_marcacao

_RE_URL_BLOCO = re.compile(r'https?://\S+')

# URLs de um bloco para DECIDIR pertencimento. O texto chega com a
# marcação do Telegram: hiperlink embutido (MessageEntityTextUrl) vem
# como `[rótulo](url)`. `\S+` capturava `url](url)` — URL que não está
# no mapa —, e o bloco de um link convertido era removido como se não
# tivesse convertido (post 24381). Aqui o link Markdown vira a própria
# URL de destino e a extração para nos mesmos delimitadores da ingestão
# (`pipeline.ingestao._RE_URL`), para que as duas pontas comparem a
# MESMA URL.
_RE_LINK_MD = re.compile(r'\[[^\]\n]*\]\((https?://[^)\s]+)\)')
_RE_URL_EXATA = re.compile(
    r'https?://[^\s\)\]>,"\'<\u200b\u200c\u200d\u2060]+')


def urls_do_bloco(texto: str) -> list:
    """URLs do texto, com link Markdown `[rótulo](url)` resolvido para a
    `url`, na ordem em que aparecem."""
    plano = _RE_LINK_MD.sub(lambda m: f" {m.group(1)} ", texto or "")
    return _RE_URL_EXATA.findall(plano)

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
    """Linha que anuncia a URL seguinte (rótulo), não conteúdo.

    A FORMA é reconhecida sobre a projeção sem marcação: "Resgate
    aqui:" e "**Resgate aqui:**" são o mesmo rótulo, e a segunda
    forma terminava em `**`, escapando de `endswith(":")`. A linha
    devolvida ao chamador continua sendo a ORIGINAL — este módulo
    não transforma texto, apenas nomeia formas.
    """
    l = sem_marcacao(linha).strip()
    return l.endswith(":") or bool(_RE_ENUM.match(l))
