"""
Camada 4 — Deduplicação inteligente, score evolutivo e identidade canônica.

Estratégia anti-duplicação:
  - Identidade hierárquica em 7 níveis (cupom-first → cashback → ASIN →
    campanha → cupom genérico → URL → hash de texto)
  - Score evolutivo com penalidade pra grupos com mídia ruim
  - Reativação inteligente: posts com "voltou", "reativou" etc. republicam
    APÓS cooldown de 30min (definido em config._COOLDOWN_REATIVACAO_S)

Ordem do fluxo:
  1. deve_enviar_async() → bloqueia/permite chegada na camada 6
  2. enviar() em publicacao.py → decide via score se publica/edita/ignora
"""
from __future__ import annotations
import asyncio
import re
import time
from typing import Optional

import config
from database import (
    db_get_dedupe, db_set_dedupe, db_buscar_janela_rapida, db_get_estado,
)
import globals as g
# g._atomic_mem acessado via g.
from logger import log_ded
from pipeline.normalizacao import EstadoEvento, MensagemNormalizada
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
# Hosts que são tipicamente campanhas/eventos (não produtos diretos)
_HOSTS_CAMPANHA = frozenset({
    "flapremios.com.br",
    "premios.shopee.com.br",
    "primevideo.com",
})


def _eh_post_cupom(texto: str) -> bool:
    """
    Detecta se o post é 'tipo cupom' — onde o cupom é o ASSUNTO PRINCIPAL,
    não um produto específico.

    REGRA (v80.2): SOMENTE a 1ª linha não-vazia (o TÍTULO) é considerada.
    Se a palavra 'cupom'/'cupons'/'código' está no título → post-cupom.

    Antes considerávamos as 3 primeiras linhas, mas isso causava falso
    positivo: posts de PRODUTO frequentemente mencionam 'Cupom: XXX' na
    linha 3 (após título e preço), o que não os torna posts-cupom.

    Posts-cupom reais SEMPRE têm a palavra no título:
      ✅ "🔥 Cupom Amazon APP"        → True  (post-cupom)
      ✅ "🔥 Cupons Magalu SOMENTE"   → True  (post-cupom)
      ✅ "Cupom: BOMDIA10"            → True  (post-cupom)
      ❌ "🔥 Smart TV Samsung 55"
         "R$ 1899"
         "Cupom: BOMDIA10"            → False (post-PRODUTO com cupom)
    """
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]
    return bool(_RE_TITULO_CUPOM.search(titulo))


def _eh_post_cashback(texto: str) -> bool:
    """
    Detecta se o post é especificamente sobre cashback (sem cupom code).
    Mesma lógica do _eh_post_cupom: só olha o TÍTULO (1ª linha não-vazia).
    """
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
    Para campanhas, extrai o host base ignorando subpaths e parâmetros.
    Resolve casos onde o mesmo evento vem com URLs diferentes (raiz vs subpath).
    """
    for url in mapa.values():
        host = _netloc(url)
        if host:
            return host
    return ""


# ─────────────────────────────────────────────────────────────────
# Atomic locks (in-memory, evita race entre workers)
# ─────────────────────────────────────────────────────────────────
async def _get_atomic_lck():
    return g._atomic_lck_obj


# Limpeza periódica de _atomic_mem (v80.3): entradas mais velhas que
# o maior timeout possível (janela cupom/produto + folga) são removidas
# automaticamente pra evitar memory leak.
_ATOMIC_TTL_MAX = 4 * 60 * 60      # 4h (maior que qualquer janela)
_ATOMIC_CLEANUP_THRESHOLD = 500    # rodar cleanup quando dict > 500 entradas


def _cleanup_atomic_mem_locked() -> int:
    """
    Remove entradas antigas de g._atomic_mem.
    DEVE ser chamado com g._atomic_lck_obj já adquirido.
    Retorna número de entradas removidas.
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


async def _atomic_check(fp: str) -> Optional[float]:
    async with (await _get_atomic_lck()):
        return g._atomic_mem.get(fp)


async def _atomic_claim(fp: str) -> bool:
    async with (await _get_atomic_lck()):
        # Cleanup oportunista (não bloqueia o caminho normal)
        removidos = _cleanup_atomic_mem_locked()
        if removidos:
            log_ded.debug(
                f"🧹 _atomic_mem cleanup: removidos {removidos} | "
                f"restam {len(g._atomic_mem)}"
            )
        if fp in g._atomic_mem:
            return False
        g._atomic_mem[fp] = time.monotonic()
        return True


async def _atomic_release(fp: str):
    async with (await _get_atomic_lck()):
        g._atomic_mem.pop(fp, None)


# ─────────────────────────────────────────────────────────────────
# Score evolutivo (mídia tem peso configurável + penalidade)
# ─────────────────────────────────────────────────────────────────
def calcular_score(norm: MensagemNormalizada) -> int:
    """
    Calcula score de qualidade do post.

    Mídia tem peso configurável: grupos em config._GRUPOS_IMG_RUIM
    recebem peso reduzido (imagem feia vale menos).
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
            score += config._SCORE_MIDIA_RUIM   # peso reduzido (1)
        else:
            score += config._SCORE_MIDIA_NORMAL  # peso normal (3)

    return score


# ─────────────────────────────────────────────────────────────────
# IDENTIDADE CANÔNICA — coração do sistema anti-duplicação
# ─────────────────────────────────────────────────────────────────
def _detectar_tipo_oferta(norm: MensagemNormalizada) -> str:
    """
    Detecta o TIPO da oferta (independente de plataforma):
      - "cupom"   : tem cupom code claro E é post centrado em cupom
      - "produto" : tem ID de produto (ASIN, SKU, ItemID)
      - "evento"  : campanha/roleta/sem ID claro

    Esse tipo determina:
      - A janela de deduplicação (30min/60min/20min)
      - A identidade canônica (cupom/produto/host)

    PRIORIDADE:
      1. Tem ID de produto SEM cupom-no-título → produto
      2. É post-cupom (cupom dominando o título) → cupom
      3. Tem cupom mas sem ID de produto → cupom standalone
      4. Cashback sem cupom → evento
      5. Campanha (host de evento) → evento
      6. Fallback: tem cupom = cupom, senão evento
    """
    texto = norm.texto_limpo

    # PRIORIDADE 1: tem ID de produto, NÃO é post-cupom → produto
    # (o cupom mencionado é secundário; o foco é o produto)
    if norm.ids_globais and not _eh_post_cupom(texto):
        return "produto"

    # PRIORIDADE 2: é post-cupom (cupom domina o título)
    if norm.cupom and _eh_post_cupom(texto):
        return "cupom"

    # PRIORIDADE 3: tem cupom mas sem ID — cupom standalone
    if norm.cupom and not norm.ids_globais:
        return "cupom"

    # PRIORIDADE 4: tem ID de produto (caso restante — produto puro)
    if norm.ids_globais:
        return "produto"

    # PRIORIDADE 5: cashback sem cupom code
    if _eh_post_cashback(texto):
        return "evento"

    # PRIORIDADE 6: campanha/evento
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

      1. Post-cupom    → plat|cup|CODIGO              (cupom domina título)
      2. Post-cashback → plat|cash|VALOR%             (cashback Shopee 30%)
      3. Produto/ASIN  → plat|asin[|cup|CODIGO]       (produto, opcional cupom)
      4. Campanha      → plat|camp|host               (flapremios)
      5. Cupom genérico→ plat|cup|CODIGO              (cupom dentro de oferta)
      6. URL canônica  → plat|url|cache_key           (mesma URL)
      7. Texto         → plat|txt|hash                (último recurso)

    REGRA NÍVEL 3 (atualizada v80.2):
    Mesmo produto com cupom DIFERENTE = ofertas DIFERENTES (não deduplica).
    Mesmo produto com mesmo cupom OU sem cupom em ambos = mesma oferta.

    Exemplos NÍVEL 3:
      - Smart TV B0XYZ sem cupom         → "amazon|B0XYZ"
      - Smart TV B0XYZ cupom BOMDIA10    → "amazon|B0XYZ|cup|BOMDIA10"
      - Smart TV B0XYZ cupom SUPER50     → "amazon|B0XYZ|cup|SUPER50"  (diferente!)
      - Mesmo TV+BOMDIA10 outro grupo    → "amazon|B0XYZ|cup|BOMDIA10" (mesma)

    Exemplos NÍVEL 1 (não muda):
      - "Cupom BOMDIA10" + qualquer ASIN → "amazon|cup|BOMDIA10"
        (post-cupom: o cupom é o foco, ASIN é só ilustrativo)
    """
    texto = norm.texto_limpo
    plat  = norm.plat

    # NÍVEL 1: Post-cupom — cupom no título manda no produto
    # (cupom é o foco, ASIN é apenas ilustrativo)
    if _eh_post_cupom(texto) and norm.cupom:
        return f"{plat}|cup|{norm.cupom.upper()}"

    # NÍVEL 2: Post-cashback sem código (Shopee tipicamente)
    if _eh_post_cashback(texto) and not norm.cupom:
        pct = _extrair_pct_cashback(texto)
        if pct:
            return f"{plat}|cash|{pct}"

    # NÍVEL 3: Produto com ASIN/SKU (com cupom opcional)
    # Mesmo produto + cupom diferente = oferta diferente (passa).
    # Mesmo produto + mesmo cupom (ou ambos sem) = mesma oferta (deduplica).
    if norm.ids_globais:
        sufixo_cupom = (
            f"|cup|{norm.cupom.upper()}" if norm.cupom else ""
        )
        return f"{plat}|{norm.ids_globais[0]}{sufixo_cupom}"

    # NÍVEL 4: Campanha/evento (sem ASIN, host de campanha)
    if _eh_post_evento(texto, norm.mapa):
        host = _host_canonico_campanha(norm.mapa)
        if host:
            return f"{plat}|camp|{host}"
        # Sem host claro, usa primeira palavra-chave da campanha
        m = _RE_EVENTO_CAMPANHA.search(texto[:200])
        if m:
            return f"{plat}|camp|{m.group(0).lower()}"

    # NÍVEL 5: Cupom genérico (oferta com cupom mencionado)
    if norm.cupom:
        return f"{plat}|cup|{norm.cupom.upper()}"

    # NÍVEL 6: URL canônica do primeiro link convertido
    if norm.mapa:
        primeira_url = next(iter(norm.mapa.values()), None)
        if primeira_url:
            return f"{plat}|url|{_cache_key(primeira_url)}"

    # NÍVEL 7: Hash do texto normalizado (último recurso)
    alma_v = _alma(texto)
    return f"{plat}|txt|{_fp4(alma_v)}"


# ─────────────────────────────────────────────────────────────────
# REATIVAÇÃO — detecta "voltou", "reativou" e libera republicação
# ─────────────────────────────────────────────────────────────────
async def _checar_reativacao(norm: MensagemNormalizada) -> bool:
    """
    Verifica se o post é uma reativação válida.

    Regra (atualizada): qualquer cupom/oferta que chegar com palavras
    de retorno ("voltou", "reativado") é tratado como reativação válida
    e reseta a janela de dedupe — pode publicar de novo.

    Retorna True = é reativação, deve republicar.
    """
    if not _eh_reativacao(norm.texto_limpo):
        return False
    return True


# ─────────────────────────────────────────────────────────────────
# DEVE_ENVIAR — entrypoint da camada 4
# ─────────────────────────────────────────────────────────────────
async def deve_enviar_async(norm: MensagemNormalizada) -> bool:
    """
    Decide se a mensagem prossegue pra montagem/publicação.

    REGRA UNIFICADA (v80.2):

    1. Detecta TIPO da oferta (cupom/produto/evento)
    2. Calcula identidade canônica (mesma oferta = mesma chave,
       não importa de qual grupo veio)
    3. Lock atômico por IDENTIDADE — só 1 worker processa cada
       identidade por vez (resolve race condition entre workers)
    4. Se identidade já foi vista dentro da janela do tipo:
       → DEIXA PASSAR pra enviar() decidir (publicar/editar/ignorar
         pelo score)
    5. Se reativação detectada ("voltou"): reseta janela e PASSA
    6. Se identidade NUNCA foi vista: PASSA pra enviar() publicar

    A camada 6 (enviar em publicacao.py) é quem decide via score:
      - Publicar como nova (1ª aparição da identidade)
      - Editar versão anterior se score maior (1 vez só, _MAX_EDITS=1)
      - Ignorar se score igual/menor

    Janelas por tipo:
      - Cupom    : 30 min (_JANELA_CUPOM_S)
      - Produto  : 60 min (_JANELA_PRODUTO_S)
      - Evento   : 20 min (_JANELA_EVENTO_S)
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
        # Qualquer cupom/oferta que chegar com palavras "voltou",
        # "reativado" etc. é tratado como nova chance — reseta o
        # tempo da janela e libera republicação.
        if await _checar_reativacao(norm):
            log_ded.info(
                f"♻️ [REATIVACAO_OK] {identity} tipo={tipo} "
                f"chat={chat} → enviar() decide"
            )
            return True

        # ── LOCK ATÔMICO POR IDENTIDADE ──────────────────────────
        # Resolve race condition: sem isso, 2 workers podem ler
        # db_get_estado=None ao mesmo tempo e ambos publicarem como novo.
        # Com lock, só 1 worker processa a identidade por vez.
        fp_identity = _fp4(f"identity|{identity}")
        ts_lock = await _atomic_check(fp_identity)
        if ts_lock is not None and (time.monotonic() - ts_lock) < janela:
            # Identidade já está sendo processada/foi processada
            # recentemente. DEIXA PASSAR — a camada 6 (enviar) vai
            # decidir via score se edita ou ignora.
            log_ded.info(
                f"🔄 [IDENTITY_NA_JANELA] {identity} tipo={tipo} "
                f"delta={int(time.monotonic() - ts_lock)}s "
                f"chat={chat} → enviar() decide"
            )
            # Atualiza timestamp pra estender a janela (mantém
            # identidade ativa enquanto receber posts dela)
            return True

        # Primeira vez vendo essa identidade na janela — claim
        ok = await _atomic_claim(fp_identity)
        if not ok:
            # Outro worker pegou exatamente nesse momento —
            # passa também (vai cair no caso acima na próxima)
            log_ded.info(
                f"🔄 [IDENTITY_RACE] {identity} chat={chat} → enviar() decide"
            )
            return True

        # ── DEDUPE PRINCIPAL — passou todas verificações ──────────
        # Registra no DB pra histórico/análise
        if ids_globais:
            id_principal = ids_globais[0]
            db_set_dedupe(
                fp_identity, plat, list(cupons), alma_v, tipo,
                id_principal, id_principal, list(benef),
            )
        elif norm.cupom:
            db_set_dedupe(
                fp_identity, plat, list(cupons), alma_v, tipo,
                "", "", list(benef),
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
        # ofertas legítimas. Erros aqui são raros — vale o risco de
        # duplicar 1x do que perder oferta boa.
        log_ded.error(f"❌ ERRO DEDUPE: {e}", exc_info=True)
        return True
