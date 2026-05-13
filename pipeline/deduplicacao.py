"""Camada 4 — Deduplicação inteligente, score evolutivo e identidade canônica."""
from __future__ import annotations
import asyncio
import re
import time
from typing import Optional, Tuple
from urllib.parse import urlparse

import config
from database import (
    db_get_dedupe, db_set_dedupe, db_buscar_janela_rapida, db_get_estado,
)
import globals as g
from logger import log_ded
from pipeline.normalizacao import (
    EstadoEvento,
    MensagemNormalizada,
    extrair_todos_cupons,
    _KW_CUPOM,
)
from utils.hashes import _fp4, _fp_benef
from utils.textos import (
    _alma, _cupons_set, _benef_set, _janela, _normalizar_valor, _sim, _SIM_FORTE,
)
from utils.urls import _cache_key, _netloc


# ── Constantes ───────────────────────────────────────────────────
# Janela curta usada APENAS pra reativação ("voltou", "reativado").
# Permite uma reativação legítima passar mas bloqueia flood quando
# múltiplos grupos mandam "voltou" do mesmo evento em sequência.
_JANELA_REATIVACAO_S = 30.0


# ─────────────────────────────────────────────────────────────────
# Detecção do TIPO do post (define qual identidade usar)
# ─────────────────────────────────────────────────────────────────
_RE_TITULO_CUPOM = re.compile(
    r'\b(?:cupom|cupons|c[oó]digo|coupon|novo\s+cupom|cupom\s+novo)\b',
    re.I,
)
_RE_TITULO_CASHBACK = re.compile(
    r'\b(?:cashback|cash\s*back|moedas?\s+shopee)\b',
    re.I,
)
_RE_PCT = re.compile(r'(\d{1,2})\s*%')
_RE_TITULO_VOLTOU = re.compile(
    r'\b(?:voltou|voltando|reativad[oa]|ativ[oa]\s+novamente|'
    r'de\s+volta|cupom\s+voltou|oferta\s+voltou|disponível\s+novamente|'
    r'normalizou|relan[çc]amento|reativa[çc][aã]o|back|return)\b',
    re.I,
)
_RE_EVENTO_CAMPANHA = re.compile(
    r'\b(?:roleta|gire|girar|miss[aã]o|arena|quiz|desafio|sorteio|'
    r'flapremios|prime\s*day|black\s*friday|esquenta)\b',
    re.I,
)
_HOSTS_CAMPANHA = frozenset({
    "flapremios.com.br",
    "premios.shopee.com.br",
    "primevideo.com",
})

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


def _eh_post_cashback(texto: str) -> bool:
    """Detecta se o post é especificamente sobre cashback (sem cupom code)."""
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]
    return bool(_RE_TITULO_CASHBACK.search(titulo))


def _eh_post_evento(texto: str, mapa: dict) -> bool:
    """Detecta evento/campanha/roleta (sem ASIN, sem cupom claro)."""
    if _RE_EVENTO_CAMPANHA.search(texto[:200]):
        return True
    for url in mapa.values():
        host = _netloc(url)
        for h in _HOSTS_CAMPANHA:
            if host == h or host.endswith("." + h):
                return True
    return False


def _eh_reativacao(texto: str) -> bool:
    """Detecta linguagem de reativação ('voltou', 'reativado' etc.)."""
    return bool(_RE_TITULO_VOLTOU.search(texto[:300]))


def _extrair_pct_cashback(texto: str) -> str:
    """Extrai o percentual de cashback (ex: '30' de 'cashback 30%')."""
    primeiras = " ".join(texto.split("\n")[:5])
    m = _RE_PCT.search(primeiras)
    return m.group(1) if m else ""


def _host_canonico_campanha(mapa: dict) -> str:
    """
    Para campanhas, extrai uma chave canônica = host + path (sem
    query string). Garante que campanhas diferentes no mesmo
    domínio não colidam.

    Ex:
      shopee.com.br/m/roleta-shopee         → "shopee.com.br/m/roleta-shopee"
      shopee.com.br/m/promocao-relampago    → "shopee.com.br/m/promocao-relampago"
      shopee.com.br/m/missao-pix            → "shopee.com.br/m/missao-pix"
    """
    for url in mapa.values():
        try:
            p = urlparse(url)
            host = (p.netloc or "").lower()
            if not host:
                continue
            # Remove "www." e porta
            if host.startswith("www."):
                host = host[4:]
            host = host.split(":")[0]
            # Path sem trailing slash duplicado
            path = (p.path or "").rstrip("/")
            return f"{host}{path}" if path else host
        except Exception:
            continue
    return ""


# ─────────────────────────────────────────────────────────────────
# Atomic locks (in-memory, evita race entre tasks)
# ─────────────────────────────────────────────────────────────────
async def _get_atomic_lck():
    return g._atomic_lck_obj


_ATOMIC_TTL_MAX = 4 * 60 * 60      # 4h
_ATOMIC_CLEANUP_THRESHOLD = 500


def _cleanup_atomic_mem_locked() -> int:
    """
    Remove entradas antigas de g._atomic_mem.
    DEVE ser chamado com g._atomic_lck_obj já adquirido.
    """
    if len(g._atomic_mem) <= _ATOMIC_CLEANUP_THRESHOLD:
        return 0
    agora = time.monotonic()
    antigos = [
        k for k, ts in g._atomic_mem.items()
        if agora - ts > _ATOMIC_TTL_MAX
    ]
    for k in antigos:
        g._atomic_mem.pop(k, None)
    return len(antigos)


async def _atomic_check_and_claim(fp: str, janela: float) -> Tuple[bool, Optional[float]]:
    """
    Atômico: verifica se fp existe DENTRO da janela e, se não, faz claim.
    Retorna (na_janela, ts_existente).
      - na_janela=True  → identidade já está sendo processada/foi recente
      - na_janela=False → claim feito agora, primeira vez nessa janela

    Tudo em UM lock pra evitar race entre check e claim. Ao reentrar
    dentro da janela, atualiza o timestamp pra estender.
    """
    async with (await _get_atomic_lck()):
        agora = time.monotonic()
        # Cleanup oportunista
        removidos = _cleanup_atomic_mem_locked()
        if removidos:
            log_ded.debug(
                f"🧹 _atomic_mem cleanup: removidos {removidos} | "
                f"restam {len(g._atomic_mem)}"
            )
        ts = g._atomic_mem.get(fp)
        if ts is not None and (agora - ts) < janela:
            # Atualiza pra estender
            g._atomic_mem[fp] = agora
            return True, ts
        # Claim
        g._atomic_mem[fp] = agora
        return False, ts


async def _atomic_release(fp: str):
    """Libera lock manualmente (raramente usado — TTL faz cleanup auto)."""
    async with (await _get_atomic_lck()):
        g._atomic_mem.pop(fp, None)


# ─────────────────────────────────────────────────────────────────
# Score evolutivo
# ─────────────────────────────────────────────────────────────────
def calcular_score(norm: MensagemNormalizada) -> int:
    """
    Calcula score de qualidade do post.
    Mídia tem peso configurável: grupos em config._GRUPOS_IMG_RUIM
    recebem peso reduzido.
    """
    texto = norm.texto_limpo
    score = 0

    if norm.mapa:
        score += 3
    if re.search(r'r\$\s*[\d.,]+', texto, re.I):
        score += 2
    if norm.cupom:
        score += 2
    if re.search(r'\d+\s*%\s*off', texto, re.I):
        score += 2
    if re.search(r'r\$\s*[\d.,]+\s*off', texto, re.I):
        score += 2
    if re.search(r'(acima|mínimo|min)\s+de\s+r\$', texto, re.I):
        score += 1
    if re.search(r'frete\s+gr[aá]t', texto, re.I):
        score += 1
    if norm.sku:
        score += 1

    # Mídia: peso varia conforme grupo de origem
    if norm.tem_midia:
        chat_lower = (norm.chat or "").lower()
        if chat_lower in config._GRUPOS_IMG_RUIM:
            score += config._SCORE_MIDIA_RUIM
        else:
            score += config._SCORE_MIDIA_NORMAL

    return score


# ─────────────────────────────────────────────────────────────────
# IDENTIDADE CANÔNICA — coração do sistema anti-duplicação
# ─────────────────────────────────────────────────────────────────
def _detectar_tipo_oferta(norm: MensagemNormalizada) -> str:
    """
    Detecta o TIPO da oferta (independente de plataforma).
      - "cupom"   : tem cupom code claro E é post centrado em cupom
      - "produto" : tem ID de produto (ASIN, SKU, ItemID)
      - "evento"  : campanha/roleta/sem ID claro
    """
    texto = norm.texto_limpo

    # P1: tem ID de produto E NÃO é post-cupom → produto
    if norm.ids_globais and not _eh_post_cupom(texto):
        return "produto"

    # P2: é post-cupom (cupom domina o título) → cupom
    if norm.cupom and _eh_post_cupom(texto):
        return "cupom"

    # P3: tem cupom mas sem ID — cupom standalone
    if norm.cupom and not norm.ids_globais:
        return "cupom"

    # P4: tem ID de produto (caso restante)
    if norm.ids_globais:
        return "produto"

    # P5: cashback sem cupom code
    if _eh_post_cashback(texto):
        return "evento"

    # P6: campanha/evento
    if _eh_post_evento(texto, norm.mapa):
        return "evento"

    # Fallback
    if norm.cupom:
        return "cupom"
    return "evento"


def _janela_por_tipo(tipo: str) -> float:
    """Retorna a janela de dedupe em segundos pelo tipo da oferta."""
    if tipo == "cupom":   return float(config._JANELA_CUPOM_S)
    if tipo == "produto": return float(config._JANELA_PRODUTO_S)
    return float(config._JANELA_EVENTO_S)


def identidade_canonica(norm: MensagemNormalizada) -> str:
    """
    Chave estável da oferta. Hierarquia em 7 níveis:

      0. Lista de cupons → plat|cuplist|hash(sorted(cupons))
      1. Post-cupom      → plat|cup|CODIGO
      2. Post-cashback   → plat|cash|VALOR%
      3. Produto/ASIN    → plat|min(ids_globais)        ← determinístico
      4. Campanha        → plat|camp|host+path          ← path evita colisão
      5. Cupom genérico  → plat|cup|CODIGO
      6. URL canônica    → plat|url|cache_key
      7. Texto           → plat|txt|hash

    NÍVEL 3 — Produto/ASIN: usa o MENOR id_global (alfabético) em vez
    do primeiro. Garante que a mesma oferta vinda de grupos diferentes
    (que podem montar mapa em ordens diferentes) produza a MESMA
    identidade. Cupom diferente no mesmo produto = melhoria avaliada
    por SCORE em enviar(), não duplicação.

    NÍVEL 4 — Campanha: usa host+path canônico (não só host). Sem path,
    campanhas diferentes no mesmo domínio (ex: 3 campanhas Shopee em
    shopee.com.br/m/*) colidiriam como duplicata.
    """
    texto = norm.texto_limpo
    plat  = norm.plat

    # NÍVEL 0: LISTA DE CUPONS — identidade pelo conjunto ordenado
    if _eh_lista_cupons(texto):
        cupons_todos = extrair_todos_cupons(
            texto, getattr(norm, "code_entities", None)
        )
        if cupons_todos:
            cupons_set = sorted(set(c.upper() for c in cupons_todos))
            cupons_hash = _fp4("|".join(cupons_set))
            return f"{plat}|cuplist|{cupons_hash}"

    # NÍVEL 1: Post-cupom — cupom no título manda no produto
    if _eh_post_cupom(texto) and norm.cupom:
        return f"{plat}|cup|{norm.cupom.upper()}"

    # NÍVEL 2: Post-cashback sem código
    if _eh_post_cashback(texto) and not norm.cupom:
        pct = _extrair_pct_cashback(texto)
        if pct:
            return f"{plat}|cash|{pct}"

    # NÍVEL 3: Produto com ASIN/SKU — id menor (determinístico)
    if norm.ids_globais:
        id_menor = min(norm.ids_globais)
        return f"{plat}|{id_menor}"

    # NÍVEL 4: Campanha/evento — host+path pra não colidir
    if _eh_post_evento(texto, norm.mapa):
        host_path = _host_canonico_campanha(norm.mapa)
        if host_path:
            return f"{plat}|camp|{host_path}"
        m = _RE_EVENTO_CAMPANHA.search(texto[:200])
        if m:
            return f"{plat}|camp|{m.group(0).lower()}"

    # NÍVEL 5: Cupom genérico
    if norm.cupom:
        return f"{plat}|cup|{norm.cupom.upper()}"

    # NÍVEL 6: URL canônica do primeiro link
    if norm.mapa:
        primeira_url = next(iter(norm.mapa.values()), None)
        if primeira_url:
            return f"{plat}|url|{_cache_key(primeira_url)}"

    # NÍVEL 7: Hash do texto normalizado
    alma_v = _alma(texto)
    return f"{plat}|txt|{_fp4(alma_v)}"


# ─────────────────────────────────────────────────────────────────
# REATIVAÇÃO
# ─────────────────────────────────────────────────────────────────
async def _checar_reativacao(norm: MensagemNormalizada) -> bool:
    """Verifica se o post é uma reativação válida."""
    if not _eh_reativacao(norm.texto_limpo):
        return False
    return True


# ─────────────────────────────────────────────────────────────────
# Wrapping de persistência (schema legado)
#
# A pipeline opera com `ids_globais` (lista, modelo unificado).
# O schema do DB tem colunas separadas (`asin`, `id_prod`) por razões
# históricas. Esta função traduz entre os dois, isolando o mapeamento
# do fluxo principal da pipeline.
#
# É a ÚNICA referência a nome de plataforma no módulo. Quando o DB
# unificar para uma coluna `id_global`, remover esta função e chamar
# `db_set_dedupe` diretamente da `deve_enviar_async`.
# ─────────────────────────────────────────────────────────────────
def _persistir_dedupe(fp, plat, cupons, alma, tipo, ids_globais, benef):
    if not ids_globais:
        asin, id_prod = "", ""
    elif plat == "amazon":
        asin, id_prod = ids_globais[0], ""
    else:
        asin, id_prod = "", ids_globais[0]
    db_set_dedupe(fp, plat, cupons, alma, tipo, asin, id_prod, benef)


# ─────────────────────────────────────────────────────────────────
# DEVE_ENVIAR — entrypoint da camada 4
# ─────────────────────────────────────────────────────────────────
async def deve_enviar_async(norm: MensagemNormalizada) -> bool:
    """
    Decide se a mensagem prossegue pra montagem/publicação.

    Retorna True quando há identidade — a camada 6 (enviar) decide
    via score se publica, edita ou ignora.

    Retorna False APENAS pra bloquear flood de reativação (múltiplas
    mensagens "voltou" do mesmo evento em sequência).
    """
    try:
        texto       = norm.texto_limpo
        plat        = norm.plat
        ids_globais = norm.ids_globais
        cupons      = _cupons_set(texto)
        alma_v      = _alma(texto)
        benef       = _benef_set(texto)
        chat        = (norm.chat or "").lower()

        # ── TIPO + IDENTIDADE + JANELA ────────────────────────────
        tipo     = _detectar_tipo_oferta(norm)
        identity = identidade_canonica(norm)
        janela   = _janela_por_tipo(tipo)

        # ── REATIVAÇÃO ────────────────────────────────────────────
        # Permite a reativação real passar (uma vez por evento), mas
        # bloqueia flood quando múltiplos grupos mandam "voltou" do
        # mesmo evento dentro de _JANELA_REATIVACAO_S.
        if await _checar_reativacao(norm):
            fp_reativ = _fp4(f"reativ|{identity}")
            na_janela, ts_ant = await _atomic_check_and_claim(
                fp_reativ, _JANELA_REATIVACAO_S,
            )
            if na_janela:
                delta = int(time.monotonic() - ts_ant) if ts_ant else 0
                log_ded.info(
                    f"♻️ [REATIVACAO_FLOOD] {identity} delta={delta}s "
                    f"chat={chat} → bloqueada (já reativou recente)"
                )
                return False
            log_ded.info(
                f"♻️ [REATIVACAO_OK] {identity} tipo={tipo} "
                f"chat={chat} → enviar() decide"
            )
            return True

        # ── CHECK + CLAIM ATÔMICO ────────────────────────────────
        fp_identity = _fp4(f"identity|{identity}")
        na_janela, ts_anterior = await _atomic_check_and_claim(
            fp_identity, janela,
        )
        if na_janela:
            log_ded.info(
                f"🔄 [IDENTITY_NA_JANELA] {identity} tipo={tipo} "
                f"delta={int(time.monotonic() - ts_anterior)}s "
                f"chat={chat} → enviar() decide"
            )
            return True

        # ── PERSISTÊNCIA ─────────────────────────────────────────
        _persistir_dedupe(
            fp_identity, plat, list(cupons), alma_v, tipo,
            ids_globais, list(benef),
        )

        log_ded.info(
            f"✅ [PASSOU] {identity} tipo={tipo} chat={chat} "
            f"→ enviar() decide"
        )
        return True

    except Exception as e:
        # Em caso de erro inesperado, deixa passar pra não bloquear
        # ofertas legítimas.
        log_ded.error(f"❌ ERRO DEDUPE: {e}", exc_info=True)
        return True
