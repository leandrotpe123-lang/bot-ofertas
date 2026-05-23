"""Afiliação Mercado Livre — módulo isolado.

═══════════════════════════════════════════════════════════════════
RESPONSABILIDADE ÚNICA
═══════════════════════════════════════════════════════════════════
Recebe um link bruto do Mercado Livre (qualquer cenário) e devolve
um meli.la curto afiliado À MINHA conta. Retorna None se a afiliação
não puder ser concluída com segurança.

NÃO conhece pipeline. NÃO conhece outras plataformas. NÃO toma
decisão sobre qual link de uma mensagem processar — essa decisão
é da camada chamadora. Para auxiliar essa decisão, é exposta a
função pura `escolher_link_prioritario(texto, urls)`.

═══════════════════════════════════════════════════════════════════
CENÁRIOS COBERTOS
═══════════════════════════════════════════════════════════════════
1. PRODUTO ............ meli.la / mercadolivre.com.br/.../MLB...
   → expandir → limpar TODOS os params (incluindo ref) → API
   → meli.la meu. Produto não carrega contexto de campanha; a
     URL fica nua e a identidade fica só no curto.

2. LISTA SEC .......... meli.la /sec/XXX
   → expandir até /social/<slug>/lists/<uuid>
   → trocar <slug> e <uuid> pelos meus
   → API → meli.la meu. Identidade da lista vive no path (UUID);
     query é descartada por consistência com o exemplo da spec.

3. CAMPANHA SOCIAL .... meli.la → /social/<slug>?matt_word=...
   → expandir
   → substituir IDENTIDADE: slug, matt_word, matt_tool pelos meus
   → remover TELEMETRIA externa: matt_event_ts, matt_d2id,
     matt_tracing_id, origin, sid, tracking_id, source
   → PRESERVAR contexto: ref (carrega qual cupom/oferta o link
     representa — é informação semântica da campanha, não
     identidade do afiliado) e demais params informativos
   → garantir forceInApp=true (UX no app oficial)
   → API → meli.la meu

═══════════════════════════════════════════════════════════════════
DISTINÇÃO CRÍTICA: IDENTIDADE vs CONTEXTO
═══════════════════════════════════════════════════════════════════
Identidade do afiliado (slug, matt_word, matt_tool):
  → SEMPRE substituir pela minha. Quem fatura sou eu.

Telemetria do afiliado externo (matt_event_ts, matt_d2id,
matt_tracing_id, origin, sid, tracking_id, source):
  → SEMPRE remover. Não faz sentido pra mim e pode confundir
    o tracking do ML.

Contexto da campanha (ref, e demais params informativos):
  → PRESERVAR em campanha (cenário 3).
  → REMOVER em produto (cenário 1), porque produto não tem
    granularidade de campanha — a URL é o produto em si.

═══════════════════════════════════════════════════════════════════
REGRAS DE OURO (validadas internamente)
═══════════════════════════════════════════════════════════════════
✗ NUNCA publicar afiliado externo
✗ NUNCA publicar matt_word / matt_tool de outro afiliado
✗ NUNCA publicar telemetria de device externo (matt_event_ts,
  matt_d2id, matt_tracing_id)
✓ Validação anti-vazamento ANTES de gerar o curto e DEPOIS de
  receber a resposta da API. Em qualquer dúvida → descarta.

═══════════════════════════════════════════════════════════════════
DEGRADAÇÃO
═══════════════════════════════════════════════════════════════════
Falha em qualquer ponto (rede, API, OAuth, resposta inválida) →
retorna None. A pipeline interpreta como "não publicar".
JAMAIS retorna URL externa ou identidade alheia.
"""
from __future__ import annotations
import asyncio
import os
import re
import time
from typing import List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp

import config
from database import db_get_link, db_set_link
from globals import _get_final, _set_final
from logger import log_nrm
from utils.url_resolver import desencurtar
from utils.urls import _netloc, _sanitizar_url


# ─────────────────────────────────────────────────────────────────
# Configuração — env vars (Railway) + defaults
# ─────────────────────────────────────────────────────────────────
# Credenciais OAuth (já configuradas no Railway)
_ML_CLIENT_ID     = os.environ.get("ML_CLIENT_ID", "")
_ML_CLIENT_SECRET = os.environ.get("ML_CLIENT_SECRET", "")
_ML_REFRESH_TOKEN = os.environ.get("ML_REFRESH_TOKEN", "")

# Minha identidade afiliada. Valores extraídos da especificação;
# expostos como ENV para permitir rotação sem deploy.
_ML_SLUG      = os.environ.get("ML_SLUG",      "silvaleo20230518163534")
_ML_MATT_WORD = os.environ.get("ML_MATT_WORD", "silvaleo1")
_ML_MATT_TOOL = os.environ.get("ML_MATT_TOOL", "32796315")
_ML_LIST_UUID = os.environ.get("ML_LIST_UUID", "5d3b5d7e-4747-483d-8957-286ccb3ef0ff")

# Endpoints. O de OAuth é estável e documentado. O de shortening
# afiliado não tem doc pública uniforme — exposto como ENV para
# permitir ajuste em produção sem mexer no código.
_ML_OAUTH_URL    = "https://api.mercadolibre.com/oauth/token"
_ML_SHORTEN_URL  = os.environ.get(
    "ML_API_SHORTEN_URL",
    "https://api.mercadolibre.com/affiliate-program/links",
)

# Host canônico final (saída sempre normalizada)
_ML_HOST_PADRAO = "www.mercadolivre.com.br"


# ─────────────────────────────────────────────────────────────────
# Sets de detecção
# ─────────────────────────────────────────────────────────────────
_ML_ENCURTADORES = frozenset({"meli.la"})

_ML_HOSTS_PRODUTO_DIRETO = frozenset({
    "produto.mercadolivre.com.br",
    "click1.mercadolivre.com.br",   # variação de clique direto
})

_ML_HOSTS_CONHECIDOS = frozenset({
    "mercadolivre.com.br",
    "www.mercadolivre.com.br",
    "produto.mercadolivre.com.br",
    "click1.mercadolivre.com.br",
    "mercadolivre.com",
    "www.mercadolivre.com",
    "meli.la",
})

# Parâmetros a remover em qualquer cenário. Tudo que começa com
# `matt_` é removido programaticamente; este set cobre o resto.
_ML_PARAMS_REMOVER = frozenset({
    "matt_word", "matt_tool", "matt_event_ts", "matt_d2id", "matt_tracing_id",
    "ref", "forceinapp", "origin", "sid", "action", "tracking_id",
    "source", "share_from", "deep_link", "deep_link_value",
})


# ─────────────────────────────────────────────────────────────────
# Regex
# ─────────────────────────────────────────────────────────────────
# Captura o slug logo após /social/. group(1)=prefixo, group(2)=slug.
_RE_SLUG_SOCIAL = re.compile(r'(/social/)([^/?#]+)', re.I)

# UUID após /lists/ — ML usa UUID v4 canônico, mas aceito hexa+hifens
# em geral pra ser tolerante a variações.
_RE_LIST_UUID = re.compile(r'(/lists/)[a-f0-9-]+', re.I)

# Padrões de produto: /MLB... no path, ou /p/MLB...
_RE_PATH_MLB    = re.compile(r'/MLB[-]?\d{5,}', re.I)
_RE_PATH_P_MLB  = re.compile(r'/p/MLB[-]?\d{5,}', re.I)

# Detecção de "mostrar mais" para escolher_link_prioritario
_RE_MOSTRAR_MAIS = re.compile(
    r'(?:clique\s+em\s+)?mostrar\s+mais|ver\s+mais|mais\s+detalhes',
    re.I,
)


# ─────────────────────────────────────────────────────────────────
# Estado do OAuth — token cache em memória + lock
# ─────────────────────────────────────────────────────────────────
# Refresh tokens ROTACIONAM no Mercado Livre: cada refresh emite
# um novo refresh_token e invalida o anterior. Mantemos o mais
# recente em memória para evitar precisar redeploy a cada rotação.
# Em restart do processo (Railway), o ENV volta a ser usado.
_TOKEN_STATE = {
    "access_token":  "",
    "refresh_token": _ML_REFRESH_TOKEN,
    "expires_at":    0.0,   # epoch seconds
}
_TOKEN_LOCK: Optional[asyncio.Lock] = None


def _get_token_lock() -> asyncio.Lock:
    """Lock lazy — instanciado na primeira chamada async."""
    global _TOKEN_LOCK
    if _TOKEN_LOCK is None:
        _TOKEN_LOCK = asyncio.Lock()
    return _TOKEN_LOCK


async def _refresh_access_token(sessao: aiohttp.ClientSession) -> Optional[str]:
    """Renova o access_token via refresh_token. Persiste o novo
    refresh em memória (rotação). Retorna None em qualquer falha."""
    if not (_ML_CLIENT_ID and _ML_CLIENT_SECRET and _TOKEN_STATE["refresh_token"]):
        log_nrm.error("❌ ML OAuth: credenciais ausentes (CLIENT_ID/SECRET/REFRESH)")
        return None
    data = {
        "grant_type":    "refresh_token",
        "client_id":     _ML_CLIENT_ID,
        "client_secret": _ML_CLIENT_SECRET,
        "refresh_token": _TOKEN_STATE["refresh_token"],
    }
    try:
        async with sessao.post(
            _ML_OAUTH_URL,
            data=data,
            headers={
                "Accept":       "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            payload = await r.json(content_type=None)
            if r.status != 200:
                log_nrm.error(
                    f"❌ ML OAuth status={r.status}: "
                    f"{(payload.get('message') if isinstance(payload, dict) else payload)}"
                )
                return None
            access  = payload.get("access_token")
            refresh = payload.get("refresh_token")
            ttl     = int(payload.get("expires_in", 21600))
            if not access:
                log_nrm.error(f"❌ ML OAuth sem access_token: {str(payload)[:200]}")
                return None
            _TOKEN_STATE["access_token"] = access
            if refresh:
                _TOKEN_STATE["refresh_token"] = refresh
            # Buffer de 5 min antes do vencimento real
            _TOKEN_STATE["expires_at"] = time.time() + max(ttl - 300, 60)
            log_nrm.info(f"🔑 ML access_token renovado (ttl={ttl}s)")
            return access
    except Exception as e:
        log_nrm.error(f"❌ ML OAuth exceção: {e}")
        return None


async def _get_access_token(sessao: aiohttp.ClientSession) -> Optional[str]:
    """Retorna access_token válido. Refresh sob lock (anti-concorrência)."""
    if _TOKEN_STATE["access_token"] and time.time() < _TOKEN_STATE["expires_at"]:
        return _TOKEN_STATE["access_token"]
    async with _get_token_lock():
        # Re-check: outra task pode ter renovado enquanto esperávamos
        if _TOKEN_STATE["access_token"] and time.time() < _TOKEN_STATE["expires_at"]:
            return _TOKEN_STATE["access_token"]
        return await _refresh_access_token(sessao)


# ─────────────────────────────────────────────────────────────────
# Detecção de cenários (puramente sintática, sobre URL expandida)
# ─────────────────────────────────────────────────────────────────
def _eh_encurtador_ml(url: str) -> bool:
    """meli.la/* ou /sec/* (encurtador interno do site)."""
    if _netloc(url) in _ML_ENCURTADORES:
        return True
    return "/sec/" in urlparse(url).path


def _eh_social_lists(url: str) -> bool:
    p = urlparse(url)
    return "/social/" in p.path and "/lists/" in p.path


def _eh_social_campanha(url: str) -> bool:
    """social/<slug> SEM /lists/ — entrada de campanha."""
    p = urlparse(url)
    return "/social/" in p.path and "/lists/" not in p.path


def _eh_produto_ml(url: str) -> bool:
    p  = urlparse(url)
    nl = _netloc(url)
    if nl in _ML_HOSTS_PRODUTO_DIRETO:
        return True
    if _RE_PATH_MLB.search(p.path) or _RE_PATH_P_MLB.search(p.path):
        return True
    return False


def _eh_link_ml(url: str) -> bool:
    """Filtro de segurança: a URL pertence ao ecossistema ML?"""
    nl = _netloc(url)
    if nl in _ML_HOSTS_CONHECIDOS:
        return True
    return any(nl == d or nl.endswith("." + d)
               for d in ("mercadolivre.com.br", "mercadolivre.com",
                         "mercadolibre.com"))


# ─────────────────────────────────────────────────────────────────
# Limpeza e troca de identidade
# ─────────────────────────────────────────────────────────────────
def _filtrar_params(params: dict) -> dict:
    """Remove TODOS os params de rastreio externos.
    `matt_*` é removido por prefixo; o restante por set."""
    return {
        k: v for k, v in params.items()
        if k.lower() not in _ML_PARAMS_REMOVER
        and not k.lower().startswith("matt_")
    }


def _limpar_url_produto(url: str) -> str:
    """Cenário 1: produto. Remove TUDO de rastreio.
    URL fica nua (sem query)."""
    p = urlparse(url)
    params = {k: v[0] for k, v in parse_qs(p.query, keep_blank_values=True).items()}
    limpos = _filtrar_params(params)
    nova_query = urlencode(limpos) if limpos else ""
    # Normaliza esquema e host pra evitar variações irrelevantes
    host = p.netloc or _ML_HOST_PADRAO
    return urlunparse(("https", host, p.path, "", nova_query, ""))


def _trocar_identidade_lista(url: str) -> str:
    """Cenário 2: substitui slug externo + UUID externo pelos meus.
    Query string original NÃO é repassada (lista usa identidade
    própria via path; manter params seria vazamento)."""
    p = urlparse(url)
    path = _RE_SLUG_SOCIAL.sub(f"/social/{_ML_SLUG}", p.path, count=1)
    path = _RE_LIST_UUID.sub(f"/lists/{_ML_LIST_UUID}", path, count=1)
    return urlunparse(("https", _ML_HOST_PADRAO, path, "", "", ""))


# Params que IDENTIFICAM o afiliado externo ou seu device — sempre
# removidos do cenário campanha. O resto da query é preservado.
#   - matt_word / matt_tool   : identidade do afiliado externo (substituo)
#   - matt_event_ts / matt_d2id / matt_tracing_id : telemetria de
#         device/sessão do afiliado externo, sem valor pra mim
#   - origin / sid / tracking_id / source : tracking de canal externo
# IMPORTANTE: `ref` NÃO está aqui. Ref carrega o contexto da campanha
# (qual cupom/oferta esse link representa) — preservar mantém a
# granularidade analítica e o roteamento correto no destino.
_ML_PARAMS_REMOVER_CAMPANHA = frozenset({
    "matt_event_ts", "matt_d2id", "matt_tracing_id",
    "origin", "sid", "tracking_id", "source",
})


def _trocar_identidade_campanha(url: str) -> str:
    """Cenário 3: substitui IDENTIDADE pela minha, PRESERVANDO o
    contexto da campanha.

    O que SUBSTITUI (identidade do afiliado):
      • slug em /social/<slug>  →  meu slug
      • matt_word               →  meu matt_word
      • matt_tool               →  meu matt_tool

    O que REMOVE (telemetria/tracking externo, sem valor pra mim):
      • matt_event_ts, matt_d2id, matt_tracing_id
      • origin, sid, tracking_id, source

    O que PRESERVA (contexto da oferta/cupom):
      • ref  ← carrega qual campanha/cupom o link representa
      • demais params informativos que vierem na URL original

    O que GARANTE:
      • forceInApp=true (UX no app oficial)
    """
    p = urlparse(url)
    path = _RE_SLUG_SOCIAL.sub(f"/social/{_ML_SLUG}", p.path, count=1)

    # Parse da query original, preservando ordem onde possível.
    params_orig = {
        k: v[0]
        for k, v in parse_qs(p.query, keep_blank_values=True).items()
    }

    # Filtra: remove telemetria do device externo e tracking de canal,
    # mas mantém ref e qualquer outro param que carregue contexto.
    # Os matt_word/matt_tool externos serão sobrescritos abaixo —
    # então também são filtrados aqui pra evitar duplicata na ordem.
    params_finais = {
        k: v for k, v in params_orig.items()
        if k.lower() not in _ML_PARAMS_REMOVER_CAMPANHA
        and k.lower() not in ("matt_word", "matt_tool")
    }

    # Sobrescreve identidade com a minha. Aqui usamos atribuição
    # explícita (não update()) pra deixar claro no código que estas
    # três chaves são MANDATÓRIAS na URL de saída.
    params_finais["matt_word"]  = _ML_MATT_WORD
    params_finais["matt_tool"]  = _ML_MATT_TOOL
    params_finais["forceInApp"] = "true"

    return urlunparse((
        "https", _ML_HOST_PADRAO, path, "",
        urlencode(params_finais), "",
    ))


# ─────────────────────────────────────────────────────────────────
# Validação anti-vazamento — última barreira antes da API
# ─────────────────────────────────────────────────────────────────
def _validar_url_minha(url: str) -> bool:
    """True se a URL é segura para gerar afiliado.
    Bloqueia identidade de afiliado externo.

    O que VALIDA (identidade do afiliado):
      • slug em /social/<slug>  → tem que ser o meu
      • matt_word               → se houver, tem que ser o meu
      • matt_tool               → se houver, tem que ser o meu

    O que NÃO VALIDA (deliberadamente):
      • ref — carrega contexto da campanha (qual cupom/oferta),
        não é identidade externa. Preservar o ref original é
        regra do cenário 3 da spec.
      • outros params informativos da query (utm_*, etc).
    """
    try:
        p = urlparse(url)
    except Exception:
        return False

    # 1) Slug — se houver /social/<slug>, slug DEVE ser o meu.
    #    Essa é a barreira principal: qualquer slug que não seja
    #    o meu significa que a troca de identidade falhou.
    m = _RE_SLUG_SOCIAL.search(p.path)
    if m and m.group(2) != _ML_SLUG:
        return False

    params = parse_qs(p.query)

    # 2) matt_word — se presente, DEVE ser o meu.
    mw = params.get("matt_word", [""])[0]
    if mw and mw != _ML_MATT_WORD:
        return False

    # 3) matt_tool — se presente, DEVE ser o meu.
    mt = params.get("matt_tool", [""])[0]
    if mt and mt != _ML_MATT_TOOL:
        return False

    # 4) Telemetria de device do afiliado externo NÃO pode passar.
    #    Esses params identificam sessão/evento na infra DELE — se
    #    chegaram até aqui, é bug na troca de identidade.
    for k in ("matt_event_ts", "matt_d2id", "matt_tracing_id"):
        if k in params:
            return False

    return True


# ─────────────────────────────────────────────────────────────────
# Cliente da API afiliada (shortening)
# ─────────────────────────────────────────────────────────────────
def _extrair_short_da_resposta(data) -> str:
    """Parser defensivo. A resposta da API afiliada do ML varia
    entre versões — tenta os formatos conhecidos sem assumir um."""
    if isinstance(data, dict):
        # formato 1: {"data": [{"url_short": "..."}]}
        # formato 2: {"urls": [{"short_url": "..."}]}
        # formato 3: {"results": [{"short": "..."}]}
        container = (data.get("data") or data.get("urls")
                     or data.get("results") or data.get("links"))
        if isinstance(container, list) and container:
            item = container[0]
            if isinstance(item, dict):
                for k in ("url_short", "short_url", "short",
                          "url", "shortLink", "shortUrl"):
                    v = item.get(k)
                    if v and isinstance(v, str) and "meli.la" in v:
                        return v
        # formato 4: {"url_short": "..."} no topo
        for k in ("url_short", "short_url", "short", "shortLink", "shortUrl"):
            v = data.get(k)
            if v and isinstance(v, str) and "meli.la" in v:
                return v
    if isinstance(data, list) and data:
        item = data[0]
        if isinstance(item, dict):
            for k in ("url_short", "short_url", "short", "shortLink", "shortUrl"):
                v = item.get(k)
                if v and isinstance(v, str) and "meli.la" in v:
                    return v
    return ""


async def _chamar_api_shorten(url_pronta: str,
                              sessao: aiohttp.ClientSession) -> Optional[str]:
    """Chama a API afiliada com retry. Renova token uma vez em 401."""
    for tentativa in range(1, 4):
        token = await _get_access_token(sessao)
        if not token:
            return None
        try:
            hdrs = {
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/json",
                "Accept":        "application/json",
            }
            payload = {"urls": [url_pronta]}
            async with config._SEM_HTTP:
                async with sessao.post(
                    _ML_SHORTEN_URL,
                    json=payload, headers=hdrs,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    if r.status == 401:
                        # token vencido / revogado → força refresh
                        log_nrm.warning(f"  ⚠️ ML 401 t={tentativa} → refresh")
                        _TOKEN_STATE["expires_at"] = 0.0
                        if tentativa < 3:
                            await asyncio.sleep(0.4)
                            continue
                        return None
                    if r.status >= 500:
                        log_nrm.warning(f"  ⚠️ ML {r.status} t={tentativa} (server)")
                        if tentativa < 3:
                            await asyncio.sleep(tentativa * 1.5)
                            continue
                        return None
                    if r.status >= 400:
                        body = (await r.text())[:200]
                        log_nrm.error(f"  ❌ ML {r.status} t={tentativa}: {body}")
                        return None
                    data  = await r.json(content_type=None)
                    short = _extrair_short_da_resposta(data)
                    if short:
                        log_nrm.info(f"  ✅ ML t={tentativa}: {short}")
                        return short
                    log_nrm.warning(
                        f"  ⚠️ ML resposta sem short t={tentativa}: "
                        f"{str(data)[:200]}"
                    )
        except asyncio.TimeoutError:
            log_nrm.warning(f"  ⏱ ML timeout t={tentativa}")
        except Exception as e:
            log_nrm.warning(f"  ⚠️ ML t={tentativa}: {e}")
        if tentativa < 3:
            await asyncio.sleep(tentativa * 1.5)
    return None


# ─────────────────────────────────────────────────────────────────
# Expansão até destino útil
# ─────────────────────────────────────────────────────────────────
async def _expandir_se_preciso(url: str, sessao: aiohttp.ClientSession) -> str:
    """Expande encurtadores ML (meli.la, /sec/) até cair em
    produto/lista/campanha. Para encurtadores PROFUNDOS (sec
    → lista), o `desencurtar` segue Location até o destino real.
    Limita ao primeiro destino útil — não resolve produtos
    internos de uma lista."""
    if not _eh_encurtador_ml(url):
        return url
    try:
        async with config._SEM_HTTP:
            expandida = await desencurtar(url, sessao)
        log_nrm.debug(f"  ML expandida: {expandida[:90]}")
        return expandida
    except Exception as e:
        log_nrm.warning(f"  ⚠️ ML expandir falhou: {e}")
        return url


# ─────────────────────────────────────────────────────────────────
# Entry point — chamado pelo affiliate_router
# ─────────────────────────────────────────────────────────────────
async def _afiliar_mercadolivre(url: str,
                                 sessao: aiohttp.ClientSession,
                                 msg_id: int = 0) -> Optional[str]:
    """Pipeline interno isolado do ML:

      1. sanitiza
      2. cache (memória + DB)
      3. expande se for encurtador
      4. detecta cenário (lista | campanha | produto)
      5. limpa OU troca identidade
      6. valida anti-vazamento
      7. chama API afiliada (com retry + refresh OAuth)
      8. valida resposta
      9. persiste cache

    Retorna meli.la afiliado meu, ou None em qualquer falha.
    `msg_id` é aceito para compat com o router; não usado aqui.
    """
    url = _sanitizar_url(url)
    log_nrm.debug(f"▶ ML: {url[:90]}")

    # Filtro de segurança: só atende links ML
    if not _eh_link_ml(url):
        log_nrm.debug(f"  ML: não é link ML → ignora ({_netloc(url)})")
        return None

    # Cache (memória + DB)
    cached = _get_final(url) or db_get_link(url)
    if cached:
        return cached

    # Expansão
    url_exp = await _expandir_se_preciso(url, sessao)
    if not _eh_link_ml(url_exp):
        log_nrm.warning(
            f"  ⚠️ ML expansão saiu do domínio: {_netloc(url_exp)} → descarta"
        )
        return None

    # Detecção de cenário + preparação da URL
    # Ordem importa: lists > campanha > produto (lists tem /social/
    # e /MLB pode aparecer dentro de uma lista também).
    if _eh_social_lists(url_exp):
        url_pronta = _trocar_identidade_lista(url_exp)
        cenario = "lista"
    elif _eh_social_campanha(url_exp):
        url_pronta = _trocar_identidade_campanha(url_exp)
        cenario = "campanha"
    elif _eh_produto_ml(url_exp):
        url_pronta = _limpar_url_produto(url_exp)
        cenario = "produto"
    else:
        log_nrm.warning(
            f"  ❓ ML tipo desconhecido pós-expansão: {url_exp[:90]} → descarta"
        )
        return None

    log_nrm.info(f"  🎯 ML {cenario}: {url_pronta[:90]}")

    # Validação anti-vazamento — última barreira
    if not _validar_url_minha(url_pronta):
        log_nrm.error(
            f"  🚫 ML identidade externa detectada — abortado: {url_pronta[:90]}"
        )
        return None

    # API afiliada
    short = await _chamar_api_shorten(url_pronta, sessao)
    if not short:
        log_nrm.warning(f"  ❌ ML API falhou → descarta: {url[:60]}")
        return None

    # Validação da resposta — deve ser meli.la (meu)
    nl_short = _netloc(short)
    if nl_short != "meli.la":
        log_nrm.error(f"  ⚠️ ML resposta com host inesperado: {short}")
        return None

    # Persiste cache
    _set_final(url, short)
    db_set_link(url, short, "mercadolivre")
    return short


# ─────────────────────────────────────────────────────────────────
# Helpers públicos — duas dimensões do CTA de campanha
# ─────────────────────────────────────────────────────────────────
# As duas funções abaixo expõem heurísticas puras sobre o texto.
# São consumidas por camadas diferentes da pipeline, refletindo
# que o CTA "Mostrar mais" opera em DOIS NÍVEIS distintos:
#
#   DIMENSÃO 1 — intra-mensagem (qual link processar):
#     A mensagem tem dois links (produto + Mostrar mais). A camada
#     de normalização deve escolher o link de campanha antes de
#     enviar pro afiliador. Use `escolher_link_prioritario`.
#
#   DIMENSÃO 2 — inter-mensagem (qual versão publicar):
#     Duas mensagens equivalentes chegam de grupos diferentes; uma
#     traz o CTA de campanha (vitrine de produtos), outra não. A
#     camada de publicação deve preferir a com CTA porque ela tem
#     maior potencial de conversão (variedade > produto único).
#     Use `tem_cta_campanha_ml` para alimentar o score de disputa.
#
# Ambas são puras (sem I/O, sem estado), sem dependência de pipeline,
# sem efeitos colaterais. O módulo continua isolado — apenas expõe
# o conhecimento sobre a convenção textual do ecossistema ML para
# que outras camadas decidam o que fazer com essa informação.
# ─────────────────────────────────────────────────────────────────
def escolher_link_prioritario(texto: str, urls: List[str]) -> List[str]:
    """DIMENSÃO 1 — escolha intra-mensagem.

    Recebe o texto da mensagem original e a lista de URLs ML
    extraídas dela. Devolve a lista filtrada por prioridade:

      - 0 ou 1 link              → devolve igual (nada a decidir)
      - sem CTA "mostrar mais"   → devolve igual (todos os links)
      - com CTA detectado        → devolve apenas o link associado

    A heurística de associação procura o link na MESMA linha do
    CTA ou nas três linhas seguintes. Defensiva: se não conseguir
    associar, devolve a lista original em vez de retornar vazio
    (preserva conteúdo válido em vez de perder a mensagem inteira).
    """
    if not urls or len(urls) < 2:
        return urls
    if not _RE_MOSTRAR_MAIS.search(texto or ""):
        return urls

    linhas = (texto or "").splitlines()
    urls_set = set(urls)

    # Janela: link na MESMA linha do CTA ou nas próximas 3.
    for i, linha in enumerate(linhas):
        if not _RE_MOSTRAR_MAIS.search(linha):
            continue
        for j in range(i, min(i + 4, len(linhas))):
            for u in urls:
                if u in linhas[j] and u in urls_set:
                    return [u]
    return urls


def tem_cta_campanha_ml(texto: str) -> bool:
    """DIMENSÃO 2 — sinal binário para score de disputa inter-mensagem.

    True se a mensagem contém o CTA típico de campanha do Mercado
    Livre ("Mostrar mais", "Ver mais", "Mais detalhes"). Esse CTA
    indica que a mensagem leva para uma VITRINE de produtos, não
    para um produto único — convertendo melhor por variedade.

    Uso esperado na publicação: quando duas mensagens equivalentes
    disputam o mesmo espaço (mesma identidade canônica, dentro da
    janela de disputa), a versão com CTA recebe bônus de score e
    vence. Em outras palavras: prefira a versão que oferece mais
    opções de compra ao usuário final, mesmo que chegue um pouco
    depois da versão "produto único".

    Função pura, sem I/O. Custo: uma busca regex.
    """
    return bool(_RE_MOSTRAR_MAIS.search(texto or ""))
