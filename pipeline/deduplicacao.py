"""
Camada 4 — Deduplicação inteligente, score evolutivo e identidade
canônica.

Responsabilidade única: decidir duplicidade. A deduplicação é a
autoridade de DECISÃO sobre duplicidade; não é autoridade de
derivação de identidade.

CONSUMO DE IDENTIDADE DERIVADA:
  A identidade — de produto e de campanha — é derivada pela
  normalização, autoridade única dessa derivação, sobre as URLs
  afiliadas LONGAS e antes do encurtamento terminal. Esta camada
  CONSOME os campos já derivados (ids_globais, sku, chave_campanha,
  tem_host_campanha) e NÃO os reextrai do mapa.

  O campo mapa, quando a deduplicação executa, já contém URLs na
  forma de publicação — possivelmente encurtadas. Reinterpretá-lo
  para derivar identidade violaria a invariante de que a URL curta
  jamais participa de derivação de identidade. A única leitura
  legítima do mapa nesta camada é o nível 6 da identidade canônica,
  fallback operacional NÃO semântico e explicitamente reconhecido.
"""
from __future__ import annotations
import re
import time
from typing import Optional, Tuple

import config
from database import db_set_dedupe
import globals as g
from logger import log_ded
from pipeline.estado_evento import _KW_EVENTO
from pipeline.normalizacao import MensagemNormalizada
from utils.cupom import _KW_CUPOM, extrair_todos_cupons
from utils.hashes import _fp4
from utils.textos import (
    _alma,
    _cupons_set,
    _benef_set,
)
from utils.urls import _cache_key


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
    r'\b(?:cashback|cash\s*back)\b',
    re.I,
)
_RE_PCT = re.compile(r'(\d{1,2})\s*%')
_RE_TITULO_VOLTOU = re.compile(
    r'\b(?:voltou|voltando|reativad[oa]|ativ[oa]\s+novamente|'
    r'de\s+volta|cupom\s+voltou|oferta\s+voltou|disponível\s+novamente|'
    r'normalizou|relan[çc]amento|reativa[çc][aã]o|back|return)\b',
    re.I,
)
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


def _eh_reativacao(texto: str) -> bool:
    """Detecta linguagem de reativação ('voltou', 'reativado' etc.)."""
    return bool(_RE_TITULO_VOLTOU.search(texto[:300]))


def _extrair_pct_cashback(texto: str) -> str:
    """Extrai o percentual de cashback (ex: '30' de 'cashback 30%')."""
    primeiras = " ".join(texto.split("\n")[:5])
    m = _RE_PCT.search(primeiras)
    return m.group(1) if m else ""


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


def _janela_por_tipo(tipo: str) -> float:
    """Retorna a janela de dedupe em segundos pelo tipo da oferta."""
    if tipo == "cupom":   return float(config._JANELA_CUPOM_S)
    if tipo == "produto": return float(config._JANELA_PRODUTO_S)
    return float(config._JANELA_EVENTO_S)


def _id_lista_cupons(norm, plat, texto):
    if not _eh_lista_cupons(texto):
        return None
    cupons_todos = extrair_todos_cupons(
        texto, getattr(norm, "code_entities", None)
    )
    if not cupons_todos:
        return None
    cupons_set = sorted(set(c.upper() for c in cupons_todos))
    return f"{plat}|cuplist|{_fp4('|'.join(cupons_set))}"


def _id_produto(norm, plat, texto):
    # Produto vence cupom: cupom diferente no mesmo produto é melhoria
    # avaliada por SCORE em enviar(), não duplicação.
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
    return f"{plat}|cup|{norm.cupom.upper()}"


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
    _id_lista_cupons,
    _id_produto,
    _id_cupom_sem_produto,
    _id_cashback,
    _id_campanha,
    _id_url,
    _id_texto,
)


def identidade_canonica(norm: "MensagemNormalizada") -> str:
    """
    Chave estável da oferta, determinada pela invariante de precedência
    declarada em _HIERARQUIA_IDENTIDADE.

    A precedência NÃO depende da ordem de instruções `if`: o dispatcher
    percorre _HIERARQUIA_IDENTIDADE em ordem e devolve a primeira
    identidade não-nula. _eh_post_cupom NÃO participa desta decisão.
    """
    texto = norm.texto_limpo
    plat = norm.plat
    for resolver in _HIERARQUIA_IDENTIDADE:
        ident = resolver(norm, plat, texto)
        if ident is not None:
            return ident
    # Inalcançável: _id_texto é total. Defesa explícita do invariante.
    return f"{plat}|txt|{_fp4(_alma(texto))}"


# ─────────────────────────────────────────────────────────────────
# REATIVAÇÃO
# ─────────────────────────────────────────────────────────────────
async def _checar_reativacao(norm: MensagemNormalizada) -> bool:
    """Verifica se o post é uma reativação válida."""
    if not _eh_reativacao(norm.texto_limpo):
        return False
    return True

def _persistir_dedupe(fp, plat, cupons, alma, tipo, ids_globais, benef):
    # id_prod é a coluna única de identidade de produto, para toda
    # plataforma. A coluna asin permanece vazia (legado a remover em
    # frente de limpeza própria). Sem ramo por nome de plataforma.
    id_prod = ids_globais[0] if ids_globais else ""
    db_set_dedupe(fp, plat, cupons, alma, tipo, "", id_prod, benef)


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
