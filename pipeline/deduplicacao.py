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
from globals import _atomic_lck_obj, _atomic_mem
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
    Detecta se o post é 'tipo cupom' — onde o cupom é o assunto principal,
    não um produto específico. Nesse caso o ASIN/produto é só exemplo.
    Heurística: olha as primeiras 3 linhas (geralmente título + descrição).
    """
    primeiras_linhas = texto.strip().split("\n")[:3]
    bloco_inicial = " ".join(primeiras_linhas)
    return bool(_RE_TITULO_CUPOM.search(bloco_inicial))


def _eh_post_cashback(texto: str) -> bool:
    """Detecta se o post é especificamente sobre cashback (sem cupom code)."""
    primeiras_linhas = texto.strip().split("\n")[:3]
    bloco_inicial = " ".join(primeiras_linhas)
    return bool(_RE_TITULO_CASHBACK.search(bloco_inicial))


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
    return _atomic_lck_obj


async def _atomic_check(fp: str) -> Optional[float]:
    async with (await _get_atomic_lck()):
        return _atomic_mem.get(fp)


async def _atomic_claim(fp: str) -> bool:
    async with (await _get_atomic_lck()):
        if fp in _atomic_mem:
            return False
        _atomic_mem[fp] = time.monotonic()
        return True


async def _atomic_release(fp: str):
    async with (await _get_atomic_lck()):
        _atomic_mem.pop(fp, None)


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
def identidade_canonica(norm: MensagemNormalizada) -> str:
    """
    Chave estável da oferta. Hierarquia em 7 níveis:

      1. Post-cupom    → plat|cup|CODIGO          (BOMDIA10 vence ASIN)
      2. Post-cashback → plat|cash|VALOR%         (cashback Shopee 30%)
      3. Produto/ASIN  → plat|asin                (mesmo produto entre grupos)
      4. Campanha      → plat|camp|host           (flapremios)
      5. Cupom genérico→ plat|cup|CODIGO          (cupom dentro de oferta)
      6. URL canônica  → plat|url|cache_key       (mesma URL)
      7. Texto         → plat|txt|hash            (último recurso)

    Exemplos:
      - "Cupom BOMDIA10" + ASIN1 → amazon|cup|BOMDIA10
      - "Cupom BOMDIA10" + ASIN2 → amazon|cup|BOMDIA10  (MESMA chave!)
      - "Cashback 30% moedas"    → shopee|cash|30
      - "Roleta Flamengo" raiz   → shopee|camp|flapremios.com.br
      - "Roleta Flamengo" /sub   → shopee|camp|flapremios.com.br (MESMA!)
    """
    texto = norm.texto_limpo
    plat  = norm.plat

    # NÍVEL 1: Post-cupom — cupom no título manda no produto
    if _eh_post_cupom(texto) and norm.cupom:
        return f"{plat}|cup|{norm.cupom.upper()}"

    # NÍVEL 2: Post-cashback sem código (Shopee tipicamente)
    if _eh_post_cashback(texto) and not norm.cupom:
        pct = _extrair_pct_cashback(texto)
        if pct:
            return f"{plat}|cash|{pct}"

    # NÍVEL 3: Produto com ASIN/SKU
    if norm.ids_globais:
        return f"{plat}|{norm.ids_globais[0]}"

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

    Condições para reativação:
      1. Texto tem palavra-chave de retorno ("voltou", "reativado" etc.)
      2. Existe estado prévio dessa identidade no banco
      3. Última publicação foi há MAIS de _COOLDOWN_REATIVACAO_S (30min)

    Retorna True = é reativação legítima, deve republicar.
    Retorna False = não é reativação OU está dentro do cooldown.
    """
    if not _eh_reativacao(norm.texto_limpo):
        return False

    identity = identidade_canonica(norm)
    estado   = db_get_estado(identity)
    if not estado:
        # Sem estado prévio: trata como oferta nova normal
        return False

    ts_anterior = estado.get("ts", 0) or 0
    delta       = time.time() - ts_anterior

    if delta < config._COOLDOWN_REATIVACAO_S:
        # Cooldown ainda ativo — provavelmente é spam de "voltou"
        # logo após o post original. Bloqueia.
        log_ded.info(
            f"⏳ [REATIVACAO_COOLDOWN] {identity} "
            f"delta={int(delta)}s < {int(config._COOLDOWN_REATIVACAO_S)}s"
        )
        return False

    # Reativação legítima: o sistema vai tratar como nova oferta
    log_ded.info(
        f"♻️ [REATIVACAO_OK] {identity} "
        f"delta={int(delta/60)}min — liberando republicação"
    )
    return True


# ─────────────────────────────────────────────────────────────────
# DEVE_ENVIAR — entrypoint da camada 4
# ─────────────────────────────────────────────────────────────────
async def deve_enviar_async(norm: MensagemNormalizada) -> bool:
    """
    Decide se a mensagem prossegue pra montagem/publicação.

    Lógica:
      • Reativação detectada (voltou/reativou) E cooldown OK → PASSA
      • Mesma oferta + mesmo grupo em janela curta → BLOQUEIA
      • Mesma oferta + grupo diferente → PASSA (enviar() decide via score)
      • Oferta nova → registra e PASSA

    A camada 6 (enviar) é quem decide via score:
      - Publicar como nova
      - Editar versão anterior se score maior
      - Ignorar se score igual/menor
    """
    try:
        texto       = norm.texto_limpo
        plat        = norm.plat
        estado      = norm.estado_evento
        ids_globais = norm.ids_globais
        cupons      = _cupons_set(texto)
        alma_v      = _alma(texto)
        benef       = _benef_set(texto)
        valores     = _normalizar_valor(texto)
        janela      = _janela(plat)
        chat        = (norm.chat or "").lower()

        # ── REATIVAÇÃO: passa direto sem checar dedupe ────────────
        if await _checar_reativacao(norm):
            log_ded.info(
                f"♻️ [PASSOU_REATIVACAO] chat={chat} → enviar() decide"
            )
            return True

        # ── RESTOCK clássico (do detectar_estado_evento) ──────────
        if estado == EstadoEvento.RESTOCKED:
            if ids_globais:
                fp_rst = _fp4(
                    f"{plat}|{ids_globais[0]}|restock|{int(time.time()//60)}"
                )
                ok = await _atomic_claim(fp_rst)
                if not ok:
                    log_ded.info(f"🔁 [BLOQ_RESTOCK] {ids_globais[0]}")
                    return False
                db_set_dedupe(
                    _fp4(f"{plat}|{ids_globais[0]}"), plat, list(cupons),
                    alma_v, "restock", ids_globais[0], ids_globais[0],
                    list(benef),
                )
                log_ded.info(f"♻️ [PASSOU_RESTOCK] {ids_globais[0]}")
            return True

        # ── SEEN: já vista recentemente, bloqueia ──────────────────
        if estado == EstadoEvento.SEEN:
            log_ded.info(f"🔁 [BLOQ_SEEN] ids={ids_globais}")
            return False

        # ── EXPIRED: viu há tempo, libera se benefício novo ───────
        if estado == EstadoEvento.EXPIRED:
            if ids_globais:
                fp_base = _fp4(f"{plat}|{ids_globais[0]}")
                entrada = db_get_dedupe(fp_base)
                if entrada:
                    benef_ant = frozenset(entrada.get("benef", []))
                    if benef and benef != benef_ant:
                        fp_ben = _fp_benef(ids_globais[0], plat, benef)
                        ok = await _atomic_claim(fp_ben)
                        if not ok:
                            return False
                        db_set_dedupe(
                            fp_base, plat, list(cupons), alma_v, "benef",
                            ids_globais[0], ids_globais[0], list(benef),
                        )
                        log_ded.info(
                            f"✳️ [PASSOU_BENEF_NOVO] {ids_globais[0]}"
                        )
                        return True
            return False

        # ──────────────────────────────────────────────────────────
        # DEDUPE PRINCIPAL — por (id_global + chat)
        #
        # Princípio:
        #   - Mesma oferta + mesmo grupo em janela = BLOQUEIA (spam)
        #   - Mesma oferta + grupo diferente = PASSA pra enviar()
        #     decidir via score (publica/edita/ignora)
        # ──────────────────────────────────────────────────────────
        for id_global in ids_globais:
            fp_chat = _fp4(f"{plat}|{id_global}|{chat}")
            ts_mem  = await _atomic_check(fp_chat)
            if ts_mem is not None and (time.monotonic() - ts_mem) < janela:
                log_ded.info(
                    f"🔁 [BLOQ_MESMO_GRUPO] {id_global} chat={chat}"
                )
                return False
            ok = await _atomic_claim(fp_chat)
            if not ok:
                log_ded.info(f"🔁 [BLOQ_RACE] {id_global} chat={chat}")
                return False
            entrada_db = db_get_dedupe(fp_chat)
            if entrada_db:
                delta = time.time() - entrada_db.get("ts", 0)
                if delta < janela:
                    await _atomic_release(fp_chat)
                    log_ded.info(
                        f"🔁 [BLOQ_DB_MESMO_GRUPO] {id_global} chat={chat}"
                    )
                    return False
            db_set_dedupe(
                fp_chat, plat, list(cupons), alma_v, "id",
                id_global, id_global, list(benef),
            )
            log_ded.info(
                f"✅ [PASSOU] {id_global} chat={chat} → enviar() decide"
            )
            return True

        # ── Cupom standalone (sem id_global) ──────────────────────
        if norm.cupom:
            fp_cup = _fp4(f"{plat}|cup|{norm.cupom}|{chat}")
            ts_mem = await _atomic_check(fp_cup)
            if ts_mem is not None and (time.monotonic() - ts_mem) < janela:
                log_ded.info(
                    f"🔁 [BLOQ_CUP_MESMO_GRUPO] {norm.cupom} chat={chat}"
                )
                return False
            ok = await _atomic_claim(fp_cup)
            if not ok:
                log_ded.info(f"🔁 [BLOQ_CUP_RACE] {norm.cupom}")
                return False
            entrada_db = db_get_dedupe(fp_cup)
            if entrada_db:
                delta = time.time() - entrada_db.get("ts", 0)
                if delta < janela:
                    await _atomic_release(fp_cup)
                    log_ded.info(f"🔁 [BLOQ_CUP_DB_MESMO_GRUPO]")
                    return False
            db_set_dedupe(
                fp_cup, plat, list(cupons), alma_v, "cup",
                "", "", list(benef),
            )
            log_ded.info(
                f"✅ [PASSOU_CUP] {norm.cupom} chat={chat} → enviar() decide"
            )
            return True

        # ── Texto-base (último recurso): similaridade semântica ───
        # Calcula fingerprint do texto + valores para detectar
        # "mesmo post de outro grupo com texto levemente diferente"
        fp_txt = _fp4(
            f"{plat}|{alma_v}|{chat}|{'|'.join(sorted(benef))}|{valores}"
        )
        ok = await _atomic_claim(fp_txt)
        if not ok:
            log_ded.info(f"🔁 [BLOQ_TEXTO_RACE]")
            return False

        # Verifica similaridade com posts recentes do MESMO grupo
        # (evita falso positivo entre grupos diferentes — esses já são
        # tratados pela identidade_canonica em enviar())
        for e in db_buscar_janela_rapida(plat, janela=max(janela, 900)):
            alma_ant = e.get("alma", "")
            if not alma_ant:
                continue
            if _sim(alma_v, alma_ant) > _SIM_FORTE:
                fp_ant = e.get("fp", "")
                if chat and chat in (fp_ant or ""):
                    await _atomic_release(fp_txt)
                    log_ded.info(f"🔁 [BLOQ_SIM_MESMO_GRUPO]")
                    return False

        db_set_dedupe(
            fp_txt, plat, list(cupons), alma_v, "gen",
            "", "", list(benef),
        )
        log_ded.info(f"✅ [PASSOU_TEXTO] chat={chat} → enviar() decide")
        return True

    except Exception as e:
        # Em caso de erro inesperado, deixa passar pra não bloquear ofertas
        # legítimas. Erros aqui são raros — vale o risco de duplicar 1x do
        # que perder oferta boa.
        log_ded.error(f"❌ ERRO DEDUPE: {e}", exc_info=True)
        return True

