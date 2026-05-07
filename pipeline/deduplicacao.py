"""
Camada 4 — Deduplicação inteligente, score evolutivo e identidade canônica.

═══════════════════════════════════════════════════════════════════
v80.4 — Auditoria sênior aplicada
═══════════════════════════════════════════════════════════════════
Cirurgias incluídas:
  • Cirurgia 1 (Bug #1)  — `return True` (era 'true' minúsculo)
  • Cirurgia 2 (Bug #2)  — NÍVEL 3 sem sufixo cupom (mesmo produto = mesma identidade)
  • Cirurgia 5 (Bug #4)  — _eh_post_cupom detecta cashback Shopee
  • Cirurgia 12 (Bug #7) — _atomic_check_and_claim atômico (era racy)
  • Cirurgia 16 (Bug #13)— separar asin/id_prod por plataforma no DB
═══════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import asyncio
import re
import time
from typing import Optional, Tuple

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
    _KW_CUPOM,        # ← Cirurgia 5: pra caso (d)
)
from utils.hashes import _fp4, _fp_benef
from utils.textos import (
    _alma, _cupons_set, _benef_set, _janela, _normalizar_valor, _sim, _SIM_FORTE,
)
from utils.urls import _cache_key, _netloc


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

# CIRURGIA 5 (Bug #4): linha de cashback (sem precisar de "OFF" literal)
# Usado pra detectar posts Shopee de cashback como post-cupom.
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

    REGRA (v80.4 — casos a-e):
      a) palavra "cupom"/"cupons"/"código" no título
      b) título já é "R$ X OFF: CODIGO" / "X% OFF: CODIGO"
      c) 2+ linhas formato lista de cupons (delegada a _eh_lista_cupons,
         resolve Bug #11 ao mesmo tempo)
      d) NOVO: cashback nas primeiras linhas + código presente
      e) NOVO: linha "X% Cashback ... : CODIGO" (mesmo sem palavra cupom)

    Casos novos resolvem o bug das imagens 1 e 2: cards Shopee com título
    genérico ("Leo Indica / Ofertas Insanas") + linha "🎟️ 50% Cashback
    ... : BRUIANHEZ10". Antes não era detectado como post-cupom.
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

    # Caso (d) NOVO: cashback presente nas primeiras 5 linhas + cupom
    # mencionado no texto inteiro
    primeiras = "\n".join(linhas[:5])
    if (_RE_CASHBACK_LINHA.search(primeiras)
            and _KW_CUPOM.search(texto)):
        return True

    # Caso (e) NOVO: linha "X% Cashback ... : CODIGO" no formato KV
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
    """Para campanhas, extrai o host base ignorando subpaths."""
    for url in mapa.values():
        host = _netloc(url)
        if host:
            return host
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


# ═══════════════════════════════════════════════════════════════════
# CIRURGIA 12 (Bug #7): _atomic_check_and_claim atômico
# ═══════════════════════════════════════════════════════════════════
# Antes: _atomic_check + _atomic_claim em locks SEPARADOS — race entre
# eles permitia 2 workers ambos passarem como "primeira vez".
# Agora: tudo em UM lock + atualiza timestamp ao reentrar (Bug #8).
# ═══════════════════════════════════════════════════════════════════

async def _atomic_check_and_claim(fp: str, janela: float) -> Tuple[bool, Optional[float]]:
    """
    Atômico: verifica se fp existe DENTRO da janela e, se não, faz claim.
    Retorna (na_janela, ts_existente).
      - na_janela=True  → identidade já está sendo processada/foi recente
      - na_janela=False → claim feito agora, primeira vez nessa janela
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
            # Atualiza pra estender (cumpre o que comentário antigo prometia)
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
      3. Produto/ASIN    → plat|asin                          ← v80.4: SEM cupom
      4. Campanha        → plat|camp|host
      5. Cupom genérico  → plat|cup|CODIGO
      6. URL canônica    → plat|url|cache_key
      7. Texto           → plat|txt|hash

    ═══════════════════════════════════════════════════════════════
    CIRURGIA 2 (Bug #2): NÍVEL 3 SEM SUFIXO CUPOM
    ═══════════════════════════════════════════════════════════════
    Antes: amazon|B0X|cup|MASTER15 ≠ amazon|B0X (mesmo produto, cupons
    diferentes geravam IDENTIDADES diferentes — duplicação).
    Agora: amazon|B0X em ambos. Cupom vira melhoria avaliada por SCORE
    em enviar(). Mesmo produto + cupom novo = EDIT, não duplicação.
    ═══════════════════════════════════════════════════════════════
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

    # NÍVEL 3: Produto com ASIN/SKU — IDENTIDADE APENAS pelo product_id
    # Cupom diferente = melhoria, decidida por SCORE em enviar().
    if norm.ids_globais:
        return f"{plat}|{norm.ids_globais[0]}"

    # NÍVEL 4: Campanha/evento
    if _eh_post_evento(texto, norm.mapa):
        host = _host_canonico_campanha(norm.mapa)
        if host:
            return f"{plat}|camp|{host}"
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
# DEVE_ENVIAR — entrypoint da camada 4
# ─────────────────────────────────────────────────────────────────
async def deve_enviar_async(norm: MensagemNormalizada) -> bool:
    """
    Decide se a mensagem prossegue pra montagem/publicação.

    Sempre retorna True quando há identidade — a camada 6 (enviar)
    decide via score se publica nova, edita ou ignora.
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

        # ── REATIVAÇÃO: passa direto e reseta janela ─────────────
        if await _checar_reativacao(norm):
            log_ded.info(
                f"♻️ [REATIVACAO_OK] {identity} tipo={tipo} "
                f"chat={chat} → enviar() decide"
            )
            return True

        # ── CHECK + CLAIM ATÔMICO (Cirurgia 12) ──────────────────
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

        # ═════════════════════════════════════════════════════════
        # CIRURGIA 16 (Bug #13): separar asin de id_prod por plataforma
        # Antes: asin=id_principal E id_prod=id_principal (perdia tipo)
        # Agora: asin só para Amazon, id_prod só pra Shopee/Magalu.
        # ═════════════════════════════════════════════════════════
        if ids_globais:
            id_principal = ids_globais[0]
            asin_real = id_principal if plat == "amazon" else ""
            id_prod_real = id_principal if plat != "amazon" else ""
            db_set_dedupe(
                fp_identity, plat, list(cupons), alma_v, tipo,
                asin_real, id_prod_real, list(benef),
            )
        else:
            db_set_dedupe(
                fp_identity, plat, list(cupons), alma_v, tipo,
                "", "", list(benef),
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
        # CIRURGIA 1 (Bug #1): era 'true' minúsculo (NameError silenciado).
        # Agora True com T maiúsculo, Python correto.
        return True
