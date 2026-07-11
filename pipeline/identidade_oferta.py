"""
Camada — Identidade de Oferta (Famílias por oferta).

Responsabilidade ÚNICA: derivar a IDENTIDADE de oferta de um post — o
conjunto de identidades por oferta (produto, campanha, cupom, cashback)
e a chave canônica de deduplicação. É a autoridade de DERIVAÇÃO de
identidade; não é autoridade de DECISÃO de duplicidade (essa é da
deduplicação, que consome o resultado pronto daqui).

Superfície pública (estável):
  - identidades(norm)         -> list[str]  : conjunto per-oferta
  - ancoras(norm)             -> list[Ancora]: âncoras TIPADAS (P6) — autoritativa
  - identidades(norm)         -> list[str]  : projeção plana de ancoras()
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
from dataclasses import dataclass

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
# Classificação do ASSUNTO do post → pipeline.assunto (C1)
# Os detectores e suas regex foram extraídos para o módulo próprio.
# Consumimos APENAS a API pública — nenhuma regex atravessa a fronteira.
# O classificador NÃO tem autoridade sobre a família nesta fase:
# ancoras() decide exatamente como antes.
# ─────────────────────────────────────────────────────────────────
from pipeline.assunto import (          # noqa: E402
    buscar_calendario_comercial,
    eh_post_cashback,
    eh_post_cupom,
    eh_post_evento,
    extrair_pct_cashback,
)

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
    # max() é guarda DEFENSIVA. Desde o D1 o efeito roda 1x por mensagem
    # nova (identidade_canonica, via enriquecimento). Se algum caminho
    # reexecutar, registrar devolve 0 para códigos já indexados — o max
    # preserva a contagem real. (Fase 2: o valor é congelado em
    # MensagemEnriquecida.cupons_novos APÓS o efeito.)
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


@dataclass(frozen=True)
class Ancora:
    """Âncora tipada de família (MB v1.1, P6). `chave` é byte-idêntica à
    string historicamente emitida por identidades() — que agora é VISTA."""
    especie: str   # "produto" | "cupom" | "campanha" | "cashback" | "fallback"
    chave: str


_ESPECIE_POR_TAG = {"cup": "cupom", "camp": "campanha",
                    "cash": "cashback", "url": "fallback", "txt": "fallback"}


def _especie_da_chave(chave: str) -> str:
    # Só para chaves nascidas no FALLBACK da hierarquia (camp/url/txt na
    # prática; cupom lá é inalcançável — união não-vazia quando há código).
    partes = chave.split("|", 2)
    return _ESPECIE_POR_TAG.get(partes[1], "produto") if len(partes) >= 2 else "produto"


def ancoras(norm: "MensagemNormalizada") -> list[Ancora]:
    """
    Derivação AUTORITATIVA das âncoras de família (MB v1.1, P3/P6):
    com PRODUTO presente, cupom e cashback são ATRIBUTOS e não ancoram —
    só ancoram quando o post não tem produto. camp| segue emitida até a
    fase per-link (§11.11; risco declarado em §11.5). Sem oferta
    estruturada, percorre a hierarquia de resolvers (mesma da canônica,
    sem chamá-la — quebra a circularidade do Estágio 3).
    """
    plat = norm.plat
    texto = norm.texto_limpo
    saida: list[Ancora] = []
    vistos: set[str] = set()

    def _add(especie: str, chave: str) -> None:
        if chave not in vistos:
            vistos.add(chave)
            saida.append(Ancora(especie, chave))

    for pid in norm.ids_globais:
        _add("produto", f"{plat}|{pid}")

    for k in norm.chaves_campanha:
        _add("campanha", f"{plat}|camp|{k}")

    if not norm.ids_globais:
        for cod in norm.cupons:
            _add("cupom", f"{plat}|cup|{cod.upper()}")

        if _eh_post_cashback(texto, norm.tem_sinal_cashback):
            pct = _extrair_pct_cashback(texto)
            if pct:
                _add("cashback", f"{plat}|cash|{pct}")

    if saida:
        return saida
    # Fallback AUTOSSUFICIENTE — mesma hierarquia, mesmos efeitos (inalcançáveis
    # para cupom aqui), byte-idêntico por construção.
    for resolver in _HIERARQUIA_IDENTIDADE:
        ident = resolver(norm, plat, texto)
        if ident is not None:
            return [Ancora(_especie_da_chave(ident), ident)]
    chave = f"{plat}|txt|{_fp4(_alma(texto))}"
    return [Ancora("fallback", chave)]


def identidades(norm: "MensagemNormalizada") -> list[str]:
    """VISTA plana de ancoras() — compatibilidade com os consumidores
    atuais (fallback de edição na publicação; ramo sem-produto da
    canônica). A doutrina de emissão vive em ancoras()."""
    return [a.chave for a in ancoras(norm)]

