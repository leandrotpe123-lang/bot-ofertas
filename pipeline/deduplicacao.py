"""
Camada 4 — Deduplicação e score.

NOTA (Front 1 — extração): a identidade de oferta foi extraída para
pipeline.identidade_oferta. Esta camada CONSOME identidades()/
identidade_canonica()/tipo_de_oferta() de lá e as REEXPORTA por
compatibilidade (consumidores atuais não mudam nesta fase). O score
(calcular_score) permanece aqui; sua extração é o Front 2.

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
from pipeline.estado_evento import _RE_RETORNO
# ── Camada fina de compatibilidade (Front 1, passo de extração) ──
# A identidade de oferta foi extraída para pipeline.identidade_oferta.
# Estas três funções são importadas aqui e REEXPORTADAS para que os
# consumidores atuais (publicacao, testes) sigam importando de
# pipeline.deduplicacao sem mudança nesta fase. identidade_canonica e
# tipo_de_oferta também são usadas internamente por deve_enviar_async.
from pipeline.identidade_oferta import (
    identidade_canonica,
    identidades,
    tipo_de_oferta,
)
from pipeline.normalizacao import MensagemNormalizada
from pipeline.identidade import username_de
from utils.cupom import extrair_todos_cupons
from utils.hashes import _fp4
from utils.textos import (
    _alma,
    _cupons_set,
    _benef_set,
)


# ── Constantes ───────────────────────────────────────────────────
# Janela curta usada APENAS pra reativação ("voltou", "reativado").
# Permite uma reativação legítima passar mas bloqueia flood quando
# múltiplos grupos mandam "voltou" do mesmo evento em sequência.
_JANELA_REATIVACAO_S = 30.0
# Reativação de CUPOM: janela longa (10 min) — regra do domínio cupom.
# Produto e campanha continuam em _JANELA_REATIVACAO_S (30s). Não mistura.
_JANELA_REATIVACAO_CUPOM_S = 600.0

def _eh_reativacao(texto: str) -> bool:
    """
    Detecta linguagem de reativação ('voltou', 'reativado' etc.).
    Consome o vocabulário canônico _RE_RETORNO (dono: estado_evento);
    a DECISÃO — gate anti-flood de 30s — permanece desta camada, e o
    escopo de busca ([:300]) é do chamador, não do vocabulário.
    """
    return bool(_RE_RETORNO.search(texto[:300]))


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
# ── Bônus de QUANTIDADE (riqueza) no score — D3 ──────────────────
# Aditivos sobre a presença; recompensam ofertas ALÉM da primeira,
# com teto para a quantidade não dominar a qualidade. Tunáveis.
_SCORE_POR_LINK_EXTRA  = 2
_SCORE_POR_CUPOM_EXTRA = 2
_MAX_EXTRAS_CONTADOS   = 3


def calcular_score(norm: MensagemNormalizada) -> int:
    """
    Calcula score de qualidade do post.
    Mídia tem peso configurável: grupos em config._GRUPOS_IMG_RUIM
    recebem peso reduzido. A PRESENÇA de links/cupom mantém o peso
    histórico; a QUANTIDADE (ofertas além da primeira) soma por cima,
    com teto — é o que faz o post mais rico vencer pelo score (D3).
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

    # ── Quantidade (riqueza): ofertas ALÉM da primeira somam, com teto.
    #    A presença acima fica intacta → posts de 1 link/1 cupom NÃO mudam.
    n_links_extra = max(0, len(norm.mapa) - 1)
    if n_links_extra:
        score += min(n_links_extra, _MAX_EXTRAS_CONTADOS) * _SCORE_POR_LINK_EXTRA
    n_cupons = len(extrair_todos_cupons(texto, getattr(norm, "code_entities", None)))
    n_cupons_extra = max(0, n_cupons - 1)
    if n_cupons_extra:
        score += min(n_cupons_extra, _MAX_EXTRAS_CONTADOS) * _SCORE_POR_CUPOM_EXTRA

    # Mídia: peso varia conforme grupo de origem. A identidade é numérica;
    # resolvemos o @username via identidade p/ consultar a lista legível.
    if norm.tem_midia:
        if username_de(norm.chat) in config._GRUPOS_IMG_RUIM:
            score += config._SCORE_MIDIA_RUIM
        else:
            score += config._SCORE_MIDIA_NORMAL

    return score

def _janela_por_tipo(tipo: str) -> float:
    """Retorna a janela de dedupe em segundos pelo tipo da oferta."""
    if tipo == "cupom":   return float(config._JANELA_CUPOM_S)
    if tipo == "produto": return float(config._JANELA_PRODUTO_S)
    return float(config._JANELA_EVENTO_S)

# ─────────────────────────────────────────────────────────────────
# REATIVAÇÃO
# ─────────────────────────────────────────────────────────────────
async def _checar_reativacao(norm: MensagemNormalizada) -> bool:
    """Verifica se o post é uma reativação válida."""
    if not _eh_reativacao(norm.texto_limpo):
        return False
    return True

def _persistir_dedupe(fp, plat, cupons, alma, tipo, ids_globais, benef, cupom_id):
    # id_prod e cupom_id são as colunas canônicas de identidade —
    # produto e cupom — para toda plataforma. cupom_id é o cupom
    # representativo já em caixa alta, derivado no chamador a partir
    # da mesma fonte da identity (norm.cupom). Sem ramo por plataforma.
    id_prod = ids_globais[0] if ids_globais else ""
    db_set_dedupe(fp, plat, cupons, alma, tipo, id_prod, benef, cupom_id)


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
        tipo     = tipo_de_oferta(norm)
        identity = identidade_canonica(norm)
        janela   = _janela_por_tipo(tipo)

        # ── REATIVAÇÃO ────────────────────────────────────────────
        # Permite a reativação real passar (uma vez por evento), mas
        # bloqueia flood quando múltiplos grupos mandam "voltou" do
        # mesmo evento dentro de _JANELA_REATIVACAO_S.
        if await _checar_reativacao(norm):
            fp_reativ = _fp4(f"reativ|{identity}")
            janela_reativ = (
                _JANELA_REATIVACAO_CUPOM_S if tipo == "cupom"
                else _JANELA_REATIVACAO_S
            )
            na_janela, ts_ant = await _atomic_check_and_claim(
                fp_reativ, janela_reativ,
            )
            if na_janela:
                novos = getattr(norm, "_cupom_novos", 0)
                if tipo == "cupom" and novos > 0:
                    log_ded.info(
                        f"♻️ [REATIVACAO_MAIS_CODIGOS] {identity} "
                        f"novos={novos} chat={chat} → enviar() decide"
                    )
                    return True
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
            ids_globais, list(benef), (norm.cupom or "").upper(),
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
