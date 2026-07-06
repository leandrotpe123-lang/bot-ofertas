"""
Camada — Identidade de Oferta (Famílias por oferta).

Responsabilidade ÚNICA: derivar a IDENTIDADE de oferta de um post — o
conjunto de identidades por oferta (produto, campanha, cupom, cashback)
e a chave canônica de deduplicação. É a autoridade de DERIVAÇÃO de
identidade; não é autoridade de DECISÃO de duplicidade (essa é da
deduplicação, que consome o resultado pronto daqui).

Superfície pública (estável):
  - identidades(norm)         -> list[str]  : conjunto per-oferta
  - identidade_canonica(norm) -> str        : chave canônica de dedupe
  - tipo_de_oferta(norm)      -> str        : "produto" | "cupom" | "evento"

CONSUMO DE IDENTIDADE DERIVADA:
  A identidade de produto e de campanha é derivada pela normalização —
  autoridade única dessa derivação — sobre as URLs afiliadas LONGAS e
  antes do encurtamento terminal. Esta camada CONSOME os campos já
  derivados (ids_globais, sku, chave_campanha, chaves_campanha,
  tem_host_campanha, tem_sinal_cashback) e NÃO os reextrai do mapa. A
  única leitura legítima do mapa é o nível de fallback operacional NÃO
  semântico de _id_url, explicitamente reconhecido.

EFEITOS COLATERAIS RECONHECIDOS (herdados — comportamento preservado):
  Esta camada NÃO é pura nesta fase. _id_cupom_indexado (a) muta
  norm._cupom_novos e (b) escreve no índice cupom_idx no banco. Esse
  comportamento é vigente e viaja junto com as funções de identidade;
  purificar é passo posterior, fora do escopo desta extração.

NÃO faz:
  - decisão de duplicidade / claim / janela  (deduplicação)
  - cálculo de score                         (deduplicação → score)
  - publicação / edição / substituição       (publicação)
  - normalização / afiliação de links        (normalização)
"""
from __future__ import annotations

import re

import config
from database import (
    db_cupom_idx_buscar,
    db_cupom_idx_registrar,
)
from pipeline.estado_evento import _KW_EVENTO
from pipeline.normalizacao import MensagemNormalizada
from utils.cupom import _KW_CUPOM
from utils.hashes import _fp4
from utils.textos import _alma
from utils.urls import _cache_key


# ── API pública ──────────────────────────────────────────────────
__all__ = [
    "identidades",
    "identidade_canonica",
    "tipo_de_oferta",
]


# ── KILL-SWITCH do domínio cupom ─────────────────────────────────
# True  → identidade de cupom por CÓDIGO COMPARTILHADO (índice).
# False → comportamento antigo (um código / fingerprint do conjunto).
# Vire False para reverter NA HORA se algum cupom legítimo sumir.
_CUPOM_IDX_ON = True

# ─────────────────────────────────────────────────────────────────
# Detecção do TIPO do post (define qual identidade usar)
# ─────────────────────────────────────────────────────────────────
_RE_TITULO_CUPOM = re.compile(
    r'\b(?:cupom|cupons|c[oó]digo|coupon|novo\s+cupom|cupom\s+novo)\b',
    re.I,
)
_RE_TITULO_CASHBACK = re.compile(
    r'\b(?:cashback|cash\s*back)\b',
    re.I,
)
_RE_PCT = re.compile(r'(\d{1,2})\s*%')

# Resíduo NOMEADO de calendário comercial — fronteira explícita.
# NÃO pertence à família interativa canônica (_KW_EVENTO, dono:
# pipeline.estado_evento) e NÃO deve subir para lá: no canônico,
# este vocabulário furaria a saturação e mudaria títulos no pico
# comercial. Serve apenas ao gate/chave de identidade de campanha.
_RE_CALENDARIO_COMERCIAL = re.compile(
    r'\b(?:black\s*friday|esquenta)\b',
    re.I,
)

# Padrão "lista de cupons" — formato típico:
#   "🔥 R$ 100 OFF em R$ 900: INFLU100"
_RE_LINHA_CUPOM_LISTA = re.compile(
    r'(?:r\$\s*\d+|\d+\s*%)\s+off(?:\s+em\s+r\$\s*\d+)?\s*:\s*[A-Z0-9][A-Z0-9_-]{3,19}',
    re.I,
)

# Padrão "OFF: CODIGO" no título
_RE_TITULO_OFF_COD = re.compile(
    r'(?:r\$\s*\d+|\d+\s*%)\s+off(?:\s+em\s+r\$\s*\d+)?\s*:\s*[A-Z0-9][A-Z0-9_-]{3,19}',
    re.I,
)

# Linha de cashback (sem precisar de "OFF" literal). Usado pra
# detectar posts Shopee de cashback como post-cupom.
_RE_CASHBACK_LINHA = re.compile(
    r'\d+\s*%\s+cash\s*back',
    re.I,
)


def _eh_lista_cupons(texto: str) -> bool:
    """
    Detecta se o post é uma LISTA DE CUPONS (não um cupom único).
    Critério: 2+ linhas no formato "R$ X OFF em R$ Y: CODIGO".
    """
    linhas = texto.strip().split("\n")
    linhas_lista = sum(
        1 for l in linhas
        if _RE_LINHA_CUPOM_LISTA.search(l)
    )
    return linhas_lista >= 2


def _eh_post_cupom(texto: str) -> bool:
    """
    Detecta se o post é 'tipo cupom' — onde o cupom é o ASSUNTO PRINCIPAL.

    Casos cobertos:
      a) palavra "cupom"/"cupons"/"código" no título
      b) título já é "R$ X OFF: CODIGO" / "X% OFF: CODIGO"
      c) 2+ linhas formato lista de cupons (delegada a _eh_lista_cupons)
      d) cashback nas primeiras linhas + código presente
      e) linha "X% Cashback ... : CODIGO" (mesmo sem palavra cupom)

    Casos (d) e (e) cobrem cards Shopee com título genérico ("Leo Indica
    / Ofertas Insanas") + linha "🎟️ 50% Cashback ... : BRUIANHEZ10".
    """
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]

    # Caso (a): palavra "cupom" no título
    if _RE_TITULO_CUPOM.search(titulo):
        return True

    # Caso (b): título já é "R$ X OFF: CODIGO" / "X% OFF: CODIGO"
    if _RE_TITULO_OFF_COD.search(titulo):
        return True

    # Caso (c): 2+ linhas formato lista (reusa _eh_lista_cupons)
    if _eh_lista_cupons(texto):
        return True

    # Caso (d): cashback presente nas primeiras 5 linhas + cupom
    # mencionado no texto inteiro
    primeiras = "\n".join(linhas[:5])
    if (_RE_CASHBACK_LINHA.search(primeiras)
            and _KW_CUPOM.search(texto)):
        return True

    # Caso (e): linha "X% Cashback ... : CODIGO" no formato KV
    for linha in linhas[:6]:
        if (_RE_CASHBACK_LINHA.search(linha)
                and re.search(r':\s*[A-Z0-9][A-Z0-9_-]{3,19}\b', linha)):
            return True

    return False


def _eh_post_cashback(texto: str, tem_sinal_cashback: bool) -> bool:
    """
    Detecta se o post é especificamente sobre cashback (sem cupom code).
    Combina o vocabulário genérico de cashback no título (universal)
    com o sinal derivado tem_sinal_cashback — vocabulário específico
    de plataforma, declarado pela própria plataforma e composto pelo
    registry. A dedup não conhece mais termos de marca diretamente.
    """
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]
    if _RE_TITULO_CASHBACK.search(titulo):
        return True
    return tem_sinal_cashback


def _eh_post_evento(texto: str, tem_host_campanha: bool) -> bool:
    """
    Detecta evento/campanha/roleta (sem ASIN, sem cupom claro).

    CONSUMO DE IDENTIDADE DERIVADA:
      Esta função NÃO reinterpreta o mapa de URLs. A identidade de
      campanha é derivada pela normalização — autoridade única dessa
      derivação — sobre as URLs afiliadas LONGAS, antes do
      encurtamento terminal. A deduplicação apenas consome o campo
      derivado tem_host_campanha.

    A detecção combina dois sinais independentes:
      - vocabulário de evento no texto, em duas famílias nomeadas:
        a interativa canônica (_KW_EVENTO, dono: estado_evento) e o
        resíduo local de calendário comercial — classificação de
        conteúdo, legítima nesta camada por operar sobre o texto;
      - presença de host de campanha: consumida do campo derivado.
    """
    if _KW_EVENTO.search(texto[:200]):
        return True
    if _RE_CALENDARIO_COMERCIAL.search(texto[:200]):
        return True
    return tem_host_campanha


def _extrair_pct_cashback(texto: str) -> str:
    """Extrai o percentual de cashback (ex: '30' de 'cashback 30%')."""
    primeiras = " ".join(texto.split("\n")[:5])
    m = _RE_PCT.search(primeiras)
    return m.group(1) if m else ""


# ─────────────────────────────────────────────────────────────────
# TIPO DA OFERTA
# ─────────────────────────────────────────────────────────────────
def tipo_de_oferta(norm: MensagemNormalizada) -> str:
    """
    Detecta o TIPO da oferta (independente de plataforma).
      - "cupom"   : tem cupom code claro E é post centrado em cupom
      - "produto" : tem ID de produto (ASIN, SKU, ItemID)
      - "evento"  : campanha/roleta/sem ID claro
    """
    texto = norm.texto_limpo

    # P1: tem ID de produto → produto. PRIORIDADE PRODUTO sobre cupom:
    # um produto pode trazer cupom embutido (ex.: Shopee); a presença
    # de id de produto define o tipo, e o cupom passa a ser ATRIBUTO
    # do produto, não um evento de cupom separado.
    if norm.ids_globais:
        return "produto"

    # P2: é post-cupom (cupom domina o título) → cupom
    if norm.cupom and _eh_post_cupom(texto):
        return "cupom"

    # P3: tem cupom mas sem ID — cupom standalone
    if norm.cupom and not norm.ids_globais:
        return "cupom"

    # (P4 removido: a prioridade de produto em P1 já cobre todo caso
    #  com ids_globais; este ramo havia se tornado inalcançável.)

    # P5: cashback sem cupom code
    if _eh_post_cashback(texto, norm.tem_sinal_cashback):
        return "evento"

    # P6: campanha/evento — consome o campo derivado tem_host_campanha
    if _eh_post_evento(texto, norm.tem_host_campanha):
        return "evento"

    # Fallback
    if norm.cupom:
        return "cupom"
    return "evento"


# ─────────────────────────────────────────────────────────────────
# IDENTIDADE CANÔNICA — coração do sistema anti-duplicação
# ─────────────────────────────────────────────────────────────────
def _id_cupom_indexado(norm, plat: str, texto: str, fallback: str) -> str:
    """Resolve a identidade de cupom pelo ÍNDICE POR CÓDIGO.

    Se QUALQUER código do post já foi visto (plat + código) dentro da
    janela de cupom, reusa a identidade daquele post. Senão usa
    `fallback` (identidade nova). Em ambos os casos registra TODOS os
    códigos sob a identidade resolvida — preserva todos, não só um. O
    link nunca participa: a chave é código + plataforma."""
    codes = sorted(set(
        c.upper() for c in norm.cupons if c
    ))
    if not codes:
        return fallback
    janela = float(config._JANELA_CUPOM_S)
    existente = db_cupom_idx_buscar(plat, codes, janela)
    identity = existente or fallback
    # max() preserva a contagem REAL de códigos novos: identidade_canonica
    # roda 2x por post (uma no dedup, outra no enviar). Na 2ª vez os
    # códigos já estão no índice e registrar devolve 0 — sem o max,
    # _cupom_novos zeraria e o cupom enriquecido nunca editaria o post.
    norm._cupom_novos = max(
        getattr(norm, "_cupom_novos", 0),
        db_cupom_idx_registrar(plat, codes, identity))
    return identity


def _id_post_cupom(norm, plat, texto):
    # POST DE CUPOM vence produto: quando o cupom é o ASSUNTO do post
    # (_eh_post_cupom), a oferta É o cupom — não o produto-veículo. Dois
    # posts do mesmo código (ex.: CURTEAI) com produtos diferentes são a
    # MESMA oferta. Produto só vence quando NÃO é post de cupom.
    if not norm.cupom or not _eh_post_cupom(texto):
        return None
    fallback = f"{plat}|cup|{norm.cupom.upper()}"
    if _CUPOM_IDX_ON:
        return _id_cupom_indexado(norm, plat, texto, fallback)
    return fallback


def _id_produto(norm, plat, texto):
    # Produto vence cupom APENAS quando NÃO é post de cupom (esse caso já
    # foi resolvido por _id_post_cupom acima). Aqui: produto comum, cujo
    # cupom eventual é melhoria avaliada por SCORE em enviar(), não dup.
    if not norm.ids_globais:
        return None
    return f"{plat}|{min(norm.ids_globais)}"


def _id_cupom_sem_produto(norm, plat, texto):
    # AUTORIDADE DO CUPOM — fato objetivo, não heurística de formato.
    # Condição exclusiva: cupom válido extraído E ausência de produto.
    # `not norm.ids_globais` é redundante com a precedência (produto já
    # foi avaliado antes), mas é DECLARADO aqui para que a autoridade do
    # nível não dependa da posição na sequência.
    if not norm.cupom or norm.ids_globais:
        return None
    fallback = f"{plat}|cup|{norm.cupom.upper()}"
    if _CUPOM_IDX_ON:
        return _id_cupom_indexado(norm, plat, texto, fallback)
    return fallback


def _id_cashback(norm, plat, texto):
    if not _eh_post_cashback(texto, norm.tem_sinal_cashback) or norm.cupom:
        return None
    pct = _extrair_pct_cashback(texto)
    if not pct:
        return None
    return f"{plat}|cash|{pct}"


def _id_campanha(norm, plat, texto):
    if not _eh_post_evento(texto, norm.tem_host_campanha):
        return None
    if norm.chave_campanha:
        return f"{plat}|camp|{norm.chave_campanha}"
    candidatos = [
        m for m in (
            _KW_EVENTO.search(texto[:200]),
            _RE_CALENDARIO_COMERCIAL.search(texto[:200]),
        ) if m
    ]
    if candidatos:
        primeiro = min(candidatos, key=lambda m: m.start())
        return f"{plat}|camp|{primeiro.group(0).lower()}"
    return None


def _id_url(norm, plat, texto):
    # Fallback operacional NÃO semântico — única leitura do mapa aqui.
    if not norm.mapa:
        return None
    primeira_url = next(iter(norm.mapa.values()), None)
    if not primeira_url:
        return None
    return f"{plat}|url|{_cache_key(primeira_url)}"


def _id_texto(norm, plat, texto):
    # Fallback terminal: nunca devolve None.
    return f"{plat}|txt|{_fp4(_alma(texto))}"


# A ordem desta tupla É a precedência. Não há precedência implícita.
_HIERARQUIA_IDENTIDADE = (
    _id_post_cupom, 
    _id_produto,
    _id_cupom_sem_produto,
    _id_cashback,
    _id_campanha,
    _id_url,
    _id_texto,
)


def identidade_canonica(norm: "MensagemNormalizada") -> str:
    """
    Chave estável de dedupe, eleita por PRECEDÊNCIA DE ESPÉCIE (MB §11.10).

    Eleição: a âncora de PRODUTO vence qualquer outra classe quando
    presente — espelha _id_produto (min dos ids_globais). Sem produto,
    mantém-se o representante lex-menor de identidades(), como desempate
    APENAS entre classes não-produto. A eleição não pode depender do
    alfabeto de ID da plataforma (P7): o lex-menor puro elegia camp/cash/
    cup para SKUs minúsculos (ex.: Magalu) por acidente.

    A grudação transitiva por código (_id_cupom_indexado) segue aplicada
    por cima em ambos os ramos: se qualquer código do post já foi visto
    na janela, a identidade da corrente sobrepõe a base.
    """
    if norm.ids_globais:
        base = f"{norm.plat}|{min(norm.ids_globais)}"
    else:
        base = sorted(identidades(norm))[0]
    return _id_cupom_indexado(norm, norm.plat, norm.texto_limpo, base)


def identidades(norm: "MensagemNormalizada") -> list[str]:
    """
    Conjunto de identidades de OFERTA do post (modelo container/oferta).
    Cada produto, campanha, cupom e cashback é uma oferta — emitidos em
    UNIÃO, sem colapso por min(). Por D4, post de cupom também emite os
    produtos associados. Sem oferta estruturada, percorre a mesma
    hierarquia de resolvers da canônica (sem chamá-la — quebra a
    circularidade que o Estágio 3 exige).

    ADITIVO: ainda NÃO consumido. identidade_canonica segue como chave
    única até o consumo per-oferta ser ligado (Estágio 3).
    """
    plat = norm.plat
    texto = norm.texto_limpo
    ofertas: list[str] = []

    def _add(chave: str) -> None:
        if chave not in ofertas:
            ofertas.append(chave)

    for pid in norm.ids_globais:
        _add(f"{plat}|{pid}")

    for k in norm.chaves_campanha:
        _add(f"{plat}|camp|{k}")

    for cod in norm.cupons:
        _add(f"{plat}|cup|{cod.upper()}")

    if _eh_post_cashback(texto, norm.tem_sinal_cashback):
        pct = _extrair_pct_cashback(texto)
        if pct:
            _add(f"{plat}|cash|{pct}")

    if ofertas:
        return ofertas
    # Fallback AUTOSSUFICIENTE: percorre a MESMA hierarquia da canônica, sem
    # chamar identidade_canonica (quebra a circularidade do Estágio 3). Byte-
    # idêntico por construção (incl. efeito colateral do índice de cupom).
    for resolver in _HIERARQUIA_IDENTIDADE:
        ident = resolver(norm, plat, texto)
        if ident is not None:
            return [ident]
    return [f"{plat}|txt|{_fp4(_alma(texto))}"] 

