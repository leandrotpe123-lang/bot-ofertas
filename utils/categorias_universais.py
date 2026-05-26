"""
Categorias Universais — Categorias de URL Independentes de Plataforma.

Módulo soberano das categorias semânticas que pertencem ao núcleo
universal da casa, não a nenhuma plataforma específica. Existe como
contraparte arquitetural do registry: enquanto o registry é a fonte
única de verdade para reconhecimento de plataforma, este módulo é a
fonte única de verdade para categorias que transcendem plataforma.

═══════════════════════════════════════════════════════════════════
CATEGORIAS UNIVERSAIS
═══════════════════════════════════════════════════════════════════
A casa reconhece quatro categorias universais legítimas, cada uma
expressando uma propriedade que existe independente de qualquer
plataforma concreta:

  - mundial   : domínios de distribuição mundial gratuita (jogos
                grátis em Epic, Steam, GoG e congêneres) — publicar
                como link direto, sem afiliação.

  - bloqueado : sites comparadores e agregadores de promoção
                concorrentes — a presença descarta o post.

  - preservar : URLs de comunicação direta (api.whatsapp.com,
                wa.me) — manter inalterados no texto publicado.

  - expandir  : encurtadores genéricos (bit.ly, tinyurl e
                congêneres) que não pertencem a nenhuma plataforma
                — o pipeline precisa expandi-los para descobrir o
                destino real antes de qualquer decisão semântica.

A função classificar_universal devolve uma dessas quatro strings ou
cadeia vazia quando a URL não se enquadra em categoria universal —
caso em que a soberania semântica passa ao registry.

═══════════════════════════════════════════════════════════════════
SOBERANIA ARQUITETURAL
═══════════════════════════════════════════════════════════════════
Este módulo NÃO conhece plataforma. Não importa amazon, shopee,
magalu, mercado_livre. Não declara hosts de marketplace. Não declara
encurtadores de plataforma. Conhece exclusivamente categorias que o
registry NÃO reivindica.

Cada plataforma declara seus próprios domínios e encurtadores
LOCALMENTE, no módulo da plataforma, e o registry compõe esse
conhecimento. Este módulo é o complemento dessa soberania: as
categorias que sobrariam órfãs se não tivessem aqui um lar.

═══════════════════════════════════════════════════════════════════
API PÚBLICA
═══════════════════════════════════════════════════════════════════
  - classificar_universal(url) -> str
      Devolve a categoria universal da URL ou cadeia vazia.

  - eh_encurtador_generico(url) -> bool
      Predicado de conveniência para a categoria 'expandir',
      semanticamente equivalente a
      `classificar_universal(url) == "expandir"`.

Funções puras, determinísticas, sem efeito colateral, sem I/O.
"""
from __future__ import annotations

from typing import FrozenSet

from utils.urls import _netloc


# ── Categoria: domínios mundiais ──────────────────────────────────
# Distribuidoras de conteúdo gratuito (jogos, software) que não
# operam por afiliação. URLs destas plataformas são publicadas
# diretamente, sem transformação.
_MUNDIAIS: FrozenSet[str] = frozenset({
    "store.epicgames.com", "epicgames.com",
    "store.steampowered.com", "steampowered.com",
    "gaming.amazon.com", "twitch.tv",
    "gog.com", "humblebundle.com", "itch.io",
})


# ── Categoria: domínios bloqueados ────────────────────────────────
# Sites comparadores e agregadores que disputam tráfego com o
# canal. A presença de URL aqui é sinal de post indesejado.
_BLOQUEADOS: FrozenSet[str] = frozenset({
    "pelando.com.br", "promobit.com.br", "cuponomia.com.br",
    "zoom.com.br", "buscape.com.br", "bondfaro.com.br",
    "ofertasbrasil.com.br",
})


# ── Categoria: domínios preservados ───────────────────────────────
# Endpoints de comunicação direta cujo link deve sobreviver
# intacto no texto publicado, sem afiliação nem transformação.
_PRESERVE: FrozenSet[str] = frozenset({
    "wa.me", "api.whatsapp.com",
})


# ── Categoria: encurtadores genéricos ─────────────────────────────
# Serviços de encurtamento que não pertencem a nenhuma plataforma
# do ecossistema. URLs aqui exigem expansão antes de qualquer
# decisão semântica subsequente. Distintos dos encurtadores de
# plataforma (amzn.to, s.shopee.com.br, maga.lu, meli.la), que são
# declarados pelos próprios plugins e ficam sob a soberania do
# registry.
_ENCURTADORES_GENERICOS: FrozenSet[str] = frozenset({
    "bit.ly", "cutt.ly", "tinyurl.com", "t.co", "ow.ly", "goo.gl",
    "rb.gy", "is.gd", "tiny.cc", "buff.ly", "short.io", "bl.ink",
    "rebrand.ly", "shorturl.at", "tidd.ly",
})


def _bate_dominio(netloc: str, dominios: FrozenSet[str]) -> bool:
    """
    Verdadeiro se o netloc pertence ao conjunto de domínios, por
    correspondência exata ou por sufixo (subdomínio). Função
    auxiliar interna, replicando a semântica consagrada na casa
    para checagem de domínios universais.
    """
    for d in dominios:
        if netloc == d or netloc.endswith("." + d):
            return True
    return False


def classificar_universal(url: str) -> str:
    """
    Devolve a categoria universal de uma URL, ou cadeia vazia
    quando a URL não pertence a nenhuma categoria universal. Pura,
    sem I/O, determinística.

    Valores possíveis:
      - "mundial"   : URL de distribuidora mundial gratuita
      - "bloqueado" : URL de comparador ou agregador concorrente
      - "preservar" : URL de comunicação direta (WhatsApp api)
      - "expandir"  : URL de encurtador genérico universal
      - ""          : nenhum dos casos acima

    Quando devolve cadeia vazia, a decisão sobre a URL passa à
    soberania do registry, que dirá se alguma plataforma a
    reivindica.
    """
    netloc = _netloc(url)
    if not netloc:
        return ""
    if _bate_dominio(netloc, _MUNDIAIS):
        return "mundial"
    if _bate_dominio(netloc, _BLOQUEADOS):
        return "bloqueado"
    if _bate_dominio(netloc, _PRESERVE):
        return "preservar"
    if netloc in _ENCURTADORES_GENERICOS:
        return "expandir"
    return ""


def eh_encurtador_generico(url: str) -> bool:
    """
    Verdadeiro se a URL é um encurtador genérico (universal, não
    declarado por nenhuma plataforma do registry). Predicado de
    conveniência para o consumidor que só precisa da resposta
    booleana.

    Semanticamente equivalente a:
        classificar_universal(url) == "expandir"
    """
    return _netloc(url) in _ENCURTADORES_GENERICOS
