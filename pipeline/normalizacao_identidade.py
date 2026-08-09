"""
Camada 3 — Normalização / Identidade semântica.

Responsabilidade ÚNICA: derivar os FATOS SEMÂNTICOS da oferta —
produto, campanha e sinal de cashback — a partir das URLs afiliadas
LONGAS e da linha-título do texto limpo.

FRONTEIRA: lê texto para EXTRAIR SINAL, nunca para TRANSFORMAR texto.
Toda transformação de forma é exclusiva de pipeline.normalizacao_texto.

NÃO resolve, NÃO afilia, NÃO encurta, NÃO faz rede, NÃO toca cache
nem banco. Não conhece pipeline.normalizacao nem normalizacao_links.

Extraído de pipeline.normalizacao sem qualquer alteração de
comportamento.
"""
from __future__ import annotations

import re
from typing import List, NamedTuple, Optional, Tuple

from plataformas import registry
from plataformas.contrato import AUSENTE
from utils.urls import _cache_key, _netloc, host_canonico_campanha, chaves_canonicas_campanha

__all__ = [
    "IdentidadeProduto",
    "IdentidadeCampanha",
    "derivar_produto",
    "remover_cupons_da_entidade",
    "derivar_campanha",
    "derivar_ancora_url",
]

# ─────────────────────────────────────────────────────────────────
# DERIVAÇÃO DE IDENTIDADE
#
# INVARIANTE FORMAL DO PIPELINE:
#   A derivação de identidade opera EXCLUSIVAMENTE sobre a URL
#   afiliada LONGA (conv), nunca sobre a URL original do texto
#   (orig) nem sobre URL encurtada. A URL afiliada longa é a única
#   fonte canônica de identidade.
# ─────────────────────────────────────────────────────────────────
def _identidade_de(url: str) -> Optional[Tuple[str, str, str]]:
    """
    Devolve (plataforma, id_produto, tipo_link) de uma URL afiliada
    longa — a estrutura que a plataforma entrega via contrato — ou
    None quando a URL não pertence a uma plataforma ou não
    corresponde a um produto individual.
    """
    plataforma = registry.resolver(url)
    if plataforma is None:
        return None
    ident = plataforma.extrai_identidade(url)
    if ident.id_produto is AUSENTE:
        return None
    return (plataforma.identificador, str(ident.id_produto),
            ident.tipo_link.value)


def _eh_host_de_campanha(url: str) -> bool:
    """
    Verdadeiro se o host de uma única URL pertence à união de hosts
    de campanha composta a partir das plataformas registradas.
    Predicado unitário, sobre a URL afiliada LONGA, usando _netloc.
    A semântica de casamento (igualdade ou sufixo) é idêntica à do
    conjunto hardcoded anterior, preservando o comportamento.
    """
    host = _netloc(url)
    hosts = registry.compor_capacidade("hosts_campanha").keys()
    for h in hosts:
        if host == h or host.endswith("." + h):
            return True
    return False

def _tem_sinal_cashback(texto: str) -> bool:
    """
    Verdadeiro se a PRIMEIRA linha não-vazia do texto casa algum
    padrão de cashback composto a partir das plataformas registradas
    (união de sinais_cashback). Opera sobre a MESMA linha-título que a
    deduplicação usa em _eh_post_cashback, preservando o escopo e,
    portanto, o comportamento. Cada padrão é regex, casado com
    re.IGNORECASE (equivalente ao re.I do regex anterior).
    """
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]
    for padrao in registry.compor_capacidade("sinais_cashback").keys():
        if re.search(padrao, titulo, re.IGNORECASE):
            return True
    return False


# ─────────────────────────────────────────────────────────────────
# CONTRATOS DE SAÍDA DESTE MÓDULO
#
# NamedTuple e não dataclass: contrato tipado explícito que também é
# desempacotável, para que o orquestrador continue usando os mesmos
# nomes locais de hoje e a montagem de MensagemNormalizada permaneça
# intocada.
# ─────────────────────────────────────────────────────────────────
class IdentidadeProduto(NamedTuple):
    ids_globais: List[str]
    idents: List[Tuple[str, str, str]]
    sku: str


class IdentidadeCampanha(NamedTuple):
    tem_host_campanha: bool
    chave_campanha: str
    chaves_campanha: List[str]
    tem_sinal_cashback: bool


def derivar_produto(urls_longas: List[str]) -> IdentidadeProduto:
    """
    Identidade de PRODUTO a partir das URLs afiliadas LONGAS.
    Preserva a ordem de aparição e a deduplicação por id_produto.
    """
    ids_globais: List[str] = []
    idents: List[Tuple[str, str, str]] = []
    for conv in urls_longas:
        trio = _identidade_de(conv)
        if trio and trio[1] not in ids_globais:
            ids_globais.append(trio[1])
            idents.append(trio)

    sku = ids_globais[0] if ids_globais else ""

    return IdentidadeProduto(ids_globais, idents, sku)


def remover_cupons_da_entidade(
    cupons: List[str], ids_globais: List[str]
) -> List[str]:
    """
    [F-C2 / R3-adendo] CONSOLIDAÇÃO: um identificador da própria
    entidade observada nunca é outra entidade. Código que coincide
    com um id de produto derivado DESTE post é o mesmo produto visto
    por outro ângulo — não entra na identidade nem na memória de
    cupons. Comparação genérica contra ids_globais: esta é a única
    camada onde os dois fatos coexistem, e nenhuma plataforma,
    prefixo ou formato é assumido.

    Sem ids_globais, devolve a MESMA lista recebida (sem cópia),
    idêntico ao comportamento anterior.
    """
    if ids_globais:
        _ids_da_entidade = {i.upper() for i in ids_globais}
        cupons = [c for c in cupons if c.upper() not in _ids_da_entidade]
    return cupons


def derivar_campanha(
    urls_longas: List[str], texto_limpo: str
) -> IdentidadeCampanha:
    """
    Identidade de CAMPANHA. chave_campanha e tem_host_campanha
    DEVEM derivar da mesma população de URLs — as URLs de campanha
    — para que sejam coerentes entre si. A chave_campanha jamais
    pode ser derivada de uma URL de produto ou de landing.

    tem_sinal_cashback deriva da linha-título do texto limpo, não de
    URL: é sinal semântico, e por isso mora aqui e não em
    normalizacao_texto.
    """
    urls_campanha     = [u for u in urls_longas if _eh_host_de_campanha(u)]
    tem_host_campanha = bool(urls_campanha)
    chave_campanha    = host_canonico_campanha(urls_campanha)
    chaves_campanha   = chaves_canonicas_campanha(urls_campanha)
    tem_sinal_cashback = _tem_sinal_cashback(texto_limpo)

    return IdentidadeCampanha(
        tem_host_campanha, chave_campanha, chaves_campanha, tem_sinal_cashback
    )

def derivar_ancora_url(urls_longas: List[str]) -> str:
    """
    Identidade de FALLBACK: a chave de URL que ancora o post quando
    nenhuma oferta estruturada (produto, campanha, cupom, cashback)
    foi reconhecida.

    Deriva da PRIMEIRA URL afiliada LONGA — a mesma população que
    alimenta produto e campanha, e a única que carrega a rota
    semântica. A URL de publicação (possivelmente encurtada) JAMAIS
    participa: era o que fazia dois posts da mesma página nascerem
    com identidades distintas.

    Preserva a seleção posicional histórica (o primeiro link
    convertido) e o formato de chave (_cache_key).

    Cadeia vazia quando não há URL: o consumidor cai no terminal de
    texto, exatamente como antes.
    """
    primeira = next(iter(urls_longas), "")
    return _cache_key(primeira) if primeira else ""


