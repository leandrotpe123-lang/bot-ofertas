"""
Plataforma — Mercado Livre.

Módulo autocontido que descreve integralmente a plataforma Mercado
Livre e cumpre o contrato de plataforma. Consolida a lógica antes
acoplada ao roteador de afiliação legado e ao circuito paralelo
que importava de pipeline.normalizacao.

Expõe a instância PLATAFORMA, descoberta automaticamente pelo
registry via Auto Discovery durante a inicialização do sistema.

═══════════════════════════════════════════════════════════════════
TRÊS CENÁRIOS DE AFILIAÇÃO
═══════════════════════════════════════════════════════════════════
1. PRODUTO ─ mercadolivre.com.br/.../MLB... ou meli.la
   Expande → limpa TODOS os parâmetros (inclusive ref) → API →
   meli.la próprio. Produto não carrega contexto de campanha; a
   URL fica nua e a identidade vive apenas no curto.

2. LISTA ─ meli.la/sec/XXX ou /social/<slug>/lists/<uuid>
   Expande até /social/<slug>/lists/<uuid> → substitui <slug> e
   <uuid> pelos próprios → API → meli.la próprio. A identidade
   da lista é capturada no path (UUID); a query é descartada por
   consistência semântica.

3. CAMPANHA SOCIAL ─ /social/<slug>?matt_word=...
   Expande → substitui IDENTIDADE (slug, matt_word, matt_tool)
   pelos próprios → remove TELEMETRIA externa (matt_event_ts,
   matt_d2id, matt_tracing_id, origin, sid, tracking_id, source)
   → PRESERVA contexto da oferta (ref e demais informativos) →
   garante forceInApp=true → API → meli.la próprio.

═══════════════════════════════════════════════════════════════════
DISTINÇÃO CRÍTICA — IDENTIDADE vs CONTEXTO
═══════════════════════════════════════════════════════════════════
Identidade do afiliado (slug, matt_word, matt_tool):
  SEMPRE substituir pela própria. Quem recebe a comissão é quem
  publica.

Telemetria do afiliado externo (matt_event_ts, matt_d2id,
matt_tracing_id, origin, sid, tracking_id, source):
  SEMPRE remover. Não faz sentido para o afiliador atual e pode
  confundir o tracking do Mercado Livre.

Contexto da campanha (ref e demais params informativos):
  PRESERVAR em campanha (cenário 3); REMOVER em produto (cenário
  1), pois produto não tem granularidade de campanha.

═══════════════════════════════════════════════════════════════════
REGRAS ANTI-VAZAMENTO
═══════════════════════════════════════════════════════════════════
NUNCA publicar URL afiliada de outro divulgador.
NUNCA publicar matt_word ou matt_tool de outro afiliado.
NUNCA publicar telemetria de device externo.
Validação anti-vazamento ANTES da chamada à API e DEPOIS de receber
a resposta. Em qualquer dúvida, retorna AUSENTE.

═══════════════════════════════════════════════════════════════════
DEGRADAÇÃO
═══════════════════════════════════════════════════════════════════
Falha em qualquer ponto (rede, API, OAuth, resposta inválida)
resulta no sentinela AUSENTE. A pipeline interpreta como "não
publicar". JAMAIS retorna URL com identidade alheia.

═══════════════════════════════════════════════════════════════════
DEPENDÊNCIAS
═══════════════════════════════════════════════════════════════════
Depende exclusivamente do contrato, dos utilitários do core
(cache_links, url_resolver, urls) e dos recursos externos.
NÃO depende de pipeline, do registry, da orquestração nem do
banco de dados diretamente. As credenciais de OAuth e os
identificadores do afiliado são lidos de variáveis de ambiente,
com valores default razoáveis para desenvolvimento.

Baseline arquitetural: Documento 1 — Especificação do Contrato.
"""
from __future__ import annotations

import asyncio
import os
import re
import time
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp

import config
from logger import log_nrm
from plataformas.contrato import (
    AUSENTE,
    CONTRACT_VERSION,
    IdentidadeProduto,
    ParametrosTemporais,
    Plataforma,
    TipoLink,
)
from utils.cache_links import consultar_link, registrar_link
from utils.url_resolver import desencurtar
from utils.urls import _netloc, _sanitizar_url


# ── Identidade da plataforma ──────────────────────────────────────
_IDENTIFICADOR = "mercadolivre"


# ── Credenciais OAuth ─────────────────────────────────────────────
# Lidas do ambiente. Em desenvolvimento, defaults vazios resultam
# em falha controlada no refresh do token, devolvendo AUSENTE pelo
# caminho normal de degradação. Em produção, devem ser definidas
# como variáveis de ambiente do processo.
_ML_CLIENT_ID = os.environ.get("ML_CLIENT_ID", "")
_ML_CLIENT_SECRET = os.environ.get("ML_CLIENT_SECRET", "")
_ML_REFRESH_TOKEN_INICIAL = os.environ.get("ML_REFRESH_TOKEN", "")


# ── Identidade do afiliado ────────────────────────────────────────
# Valores que substituem identidade externa nas URLs afiliadas.
# Defaults razoáveis para desenvolvimento; em produção devem ser
# definidos via ambiente para permitir rotação sem deploy.
_ML_SLUG = os.environ.get(
    "ML_SLUG", "silvaleo20230518163534",
)
_ML_MATT_WORD = os.environ.get("ML_MATT_WORD", "silvaleo1")
_ML_MATT_TOOL = os.environ.get("ML_MATT_TOOL", "32796315")
_ML_LIST_UUID = os.environ.get(
    "ML_LIST_UUID", "5d3b5d7e-4747-483d-8957-286ccb3ef0ff",
)


# ── Endpoints ─────────────────────────────────────────────────────
# OAuth é estável e documentado. O de shortening afiliado não tem
# documentação pública uniforme — exposto como variável de ambiente
# para permitir ajuste em produção sem mexer no código.
_ENDPOINT_OAUTH = "https://api.mercadolibre.com/oauth/token"
_ENDPOINT_CREATE_LINK = os.environ.get(
    "ML_API_CREATE_LINK_URL",
    "https://www.mercadolivre.com.br/affiliate-program/api/v2/affiliates/createLink",
)

# Mapeamento de cenário interno → valor canônico do campo 'type'
# esperado pela API. Encapsula a única tradução semântica entre
# o vocabulário do plugin e o vocabulário da API.
_TYPE_POR_CENARIO = {
    "produto":  "product",
    "lista":    "list",
    "campanha": "social_profile",
}
# Host canônico de saída para a URL longa pré-API.
_HOST_PADRAO = "www.mercadolivre.com.br"


# ── Domínios e encurtadores ───────────────────────────────────────
# A plataforma reivindica estas formas de URL. Subdomínios são
# tratados pela checagem com endswith em _bate_dominio.
_DOMINIOS = frozenset({
    "mercadolivre.com.br", "mercadolivre.com",
    "produto.mercadolivre.com.br", "click1.mercadolivre.com.br",
    "meli.la",
})

# Subdomínios que representam página de produto direta. Quando a
# URL cai aqui sem MLB visível, o tratamento é diferente do path
# canônico /MLB.../ — não há identidade extraível pela URL.
_HOSTS_PRODUTO_DIRETO = frozenset({
    "produto.mercadolivre.com.br",
    "click1.mercadolivre.com.br",
})

_ENCURTADORES = frozenset({"meli.la"})


# ── Quirk HTTP: hosts que exigem GET na resolução ─────────────────
# `meli.la` responde corretamente a HEAD; o conjunto é declarado
# EXPLICITAMENTE VAZIO para registrar que a consideração foi feita
# e a resposta é nula — semântica distinta de não declarar a
# capacidade, que ficaria como None no contrato.
_ENCURTADORES_FORCA_GET = frozenset()


# ── Padrões regex de extração ─────────────────────────────────────
# Captura o slug logo após /social/. Tolerante a finais variáveis.
_RE_SLUG_SOCIAL = re.compile(r'(/social/)([^/?#]+)', re.I)

# UUID após /lists/. ML usa UUID v4 canônico; tolera variações
# hexadecimais com hífens.
_RE_LIST_UUID = re.compile(r'(/lists/)[a-f0-9-]+', re.I)

# Padrões de produto: /MLB... no path, ou /p/MLB... O grupo
# capturador devolve apenas os dígitos; o prefixo "MLB" é
# prepostado em _extrair_mlb.
_RE_PATH_MLB = re.compile(r'/MLB[-]?(\d{5,})', re.I)
_RE_PATH_P_MLB = re.compile(r'/p/MLB[-]?(\d{5,})', re.I)

# Path de encurtador interno do site (/sec/XXX).
_RE_PATH_SEC = re.compile(r'/sec/', re.I)


# ── Parâmetros temporais de deduplicação ──────────────────────────
_PARAMETROS_TEMPORAIS = ParametrosTemporais(
    janela_s=300.0,
    ttl_restock_s=7200.0,
)


# ── Parâmetros de limpeza ─────────────────────────────────────────
# Telemetria de device e tracking de canal externo — sempre
# removidos em qualquer cenário.
_PARAMS_TELEMETRIA = frozenset({
    "matt_event_ts", "matt_d2id", "matt_tracing_id",
    "origin", "sid", "tracking_id", "source",
})

# Identidade externa do afiliado — substituída pela própria.
_PARAMS_IDENTIDADE_EXTERNA = frozenset({"matt_word", "matt_tool"})

# No cenário PRODUTO, removemos tudo: telemetria, identidade
# externa, contexto de campanha, deep links. URL fica nua.
_PARAMS_REMOVER_PRODUTO = (
    _PARAMS_TELEMETRIA
    | _PARAMS_IDENTIDADE_EXTERNA
    | frozenset({
        "ref", "forceinapp", "action", "share_from",
        "deep_link", "deep_link_value",
    })
)


# ── Configuração de retry da API afiliada ─────────────────────────
_TENTATIVAS_API = 3
_TIMEOUT_API = 15
_TIMEOUT_OAUTH = 15


# ── Estado do OAuth ───────────────────────────────────────────────
# Refresh tokens ROTACIONAM no Mercado Livre: cada refresh emite
# um novo refresh_token e invalida o anterior. Mantemos o mais
# recente em memória para evitar redeploy a cada rotação. Em
# restart do processo, a variável de ambiente volta a ser usada.
_TOKEN_STATE = {
    "access_token": "",
    "refresh_token": _ML_REFRESH_TOKEN_INICIAL,
    "expires_at": 0.0,  # epoch seconds
}
_TOKEN_LOCK: Optional[asyncio.Lock] = None


def _obter_lock_token() -> asyncio.Lock:
    """Lock lazy — instanciado na primeira chamada async."""
    global _TOKEN_LOCK
    if _TOKEN_LOCK is None:
        _TOKEN_LOCK = asyncio.Lock()
    return _TOKEN_LOCK


async def _renovar_access_token(
    sessao: aiohttp.ClientSession,
) -> Optional[str]:
    """
    Renova o access_token via refresh_token. Persiste em memória
    o novo refresh emitido (rotação). Devolve o novo access_token,
    ou None em qualquer falha.
    """
    if not (
        _ML_CLIENT_ID
        and _ML_CLIENT_SECRET
        and _TOKEN_STATE["refresh_token"]
    ):
        log_nrm.error(
            "❌ ML OAuth: credenciais ausentes "
            "(CLIENT_ID / CLIENT_SECRET / REFRESH_TOKEN)"
        )
        return None

    data = {
        "grant_type": "refresh_token",
        "client_id": _ML_CLIENT_ID,
        "client_secret": _ML_CLIENT_SECRET,
        "refresh_token": _TOKEN_STATE["refresh_token"],
    }
    try:
        async with sessao.post(
            _ENDPOINT_OAUTH,
            data=data,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT_OAUTH),
        ) as r:
            payload = await r.json(content_type=None)
            if r.status != 200:
                mensagem = (
                    payload.get("message")
                    if isinstance(payload, dict)
                    else payload
                )
                log_nrm.error(
                    f"❌ ML OAuth status={r.status}: {mensagem}"
                )
                return None
            access = payload.get("access_token")
            refresh = payload.get("refresh_token")
            ttl = int(payload.get("expires_in", 21600))
            if not access:
                log_nrm.error(
                    f"❌ ML OAuth sem access_token: {str(payload)[:200]}"
                )
                return None
            _TOKEN_STATE["access_token"] = access
            if refresh:
                _TOKEN_STATE["refresh_token"] = refresh
            # Buffer de 5min antes do vencimento real.
            _TOKEN_STATE["expires_at"] = time.time() + max(ttl - 300, 60)
            log_nrm.info(f"🔑 ML access_token renovado (ttl={ttl}s)")
            return access
    except Exception as e:
        log_nrm.error(f"❌ ML OAuth exceção: {e}")
        return None


async def _obter_access_token(
    sessao: aiohttp.ClientSession,
) -> Optional[str]:
    """
    Retorna access_token válido. Realiza refresh sob lock para
    evitar tempestade de renovações concorrentes.
    """
    if (
        _TOKEN_STATE["access_token"]
        and time.time() < _TOKEN_STATE["expires_at"]
    ):
        return _TOKEN_STATE["access_token"]
    async with _obter_lock_token():
        # Re-check: outra task pode ter renovado enquanto esperávamos.
        if (
            _TOKEN_STATE["access_token"]
            and time.time() < _TOKEN_STATE["expires_at"]
        ):
            return _TOKEN_STATE["access_token"]
        return await _renovar_access_token(sessao)


# ── Funções de apoio ──────────────────────────────────────────────
def _bate_dominio(netloc: str, dominios: frozenset) -> bool:
    """Verdadeiro se o netloc pertence ao conjunto de domínios."""
    for d in dominios:
        if netloc == d or netloc.endswith("." + d):
            return True
    return False


def _extrair_mlb(path: str) -> str:
    """
    Extrai o identificador MLB do path da URL no formato 'MLB' +
    dígitos. Devolve string vazia quando ausente. A ordem das
    regex importa: /p/MLB primeiro (caso mais específico).
    """
    m = _RE_PATH_P_MLB.search(path) or _RE_PATH_MLB.search(path)
    return f"MLB{m.group(1)}" if m else ""


def _eh_social_lists_path(path: str) -> bool:
    """Verdadeiro se o path contém /social/ E /lists/."""
    return "/social/" in path and "/lists/" in path


def _eh_social_path(path: str) -> bool:
    """Verdadeiro se o path contém /social/."""
    return "/social/" in path


# ── Detecção de cenário ───────────────────────────────────────────
def _cenario_de(url: str) -> str:
    """
    Identifica o cenário de afiliação após expansão. Devolve um
    dos quatro valores:
      - 'lista'    : /social/<slug>/lists/<uuid>
      - 'campanha' : /social/<slug> (sem /lists/)
      - 'produto'  : MLB no path, ou subdomínio de produto direto
      - ''         : nenhum dos três; URL não afiliável.

    A ordem importa: lista vence campanha porque toda URL de lista
    também contém /social/. Pure, sem I/O.
    """
    parsed = urlparse(url)
    path = parsed.path or "/"
    netloc = _netloc(url)

    if _eh_social_lists_path(path):
        return "lista"
    if _eh_social_path(path):
        return "campanha"
    if _RE_PATH_MLB.search(path) or _RE_PATH_P_MLB.search(path):
        return "produto"
    if netloc in _HOSTS_PRODUTO_DIRETO:
        return "produto"
    return ""


# ── Transformações de URL — funções puras ─────────────────────────
def _filtrar_params(params: dict, remover: frozenset) -> dict:
    """
    Remove parâmetros do dicionário pela união de:
      - conjunto explícito `remover` (case-insensitive);
      - qualquer chave que comece por 'matt_' (telemetria e
        identidade externa do afiliado por padrão).
    """
    return {
        k: v for k, v in params.items()
        if k.lower() not in remover
        and not k.lower().startswith("matt_")
    }


def _limpar_url_produto(url: str) -> str:
    """
    Cenário PRODUTO. Remove todo rastreio e contexto, deixando a
    URL nua sobre o host canônico. A identidade do produto (MLB)
    permanece no path original.
    """
    p = urlparse(url)
    params = {
        k: v[0]
        for k, v in parse_qs(p.query, keep_blank_values=True).items()
    }
    limpos = _filtrar_params(params, _PARAMS_REMOVER_PRODUTO)
    nova_query = urlencode(limpos) if limpos else ""
    host = p.netloc or _HOST_PADRAO
    return urlunparse(("https", host, p.path, "", nova_query, ""))


def _trocar_identidade_lista(url: str) -> str:
    """
    Cenário LISTA. Substitui slug externo e UUID externo pelos
    próprios no path. A query original NÃO é repassada — a lista
    usa identidade própria via path; manter params seria vazamento.
    """
    p = urlparse(url)
    path = _RE_SLUG_SOCIAL.sub(f"/social/{_ML_SLUG}", p.path, count=1)
    path = _RE_LIST_UUID.sub(f"/lists/{_ML_LIST_UUID}", path, count=1)
    return urlunparse(("https", _HOST_PADRAO, path, "", "", ""))


def _trocar_identidade_campanha(url: str) -> str:
    """
    Cenário CAMPANHA. Substitui identidade do afiliado (slug,
    matt_word, matt_tool) pela própria. Remove telemetria de
    device e tracking de canal externo. PRESERVA o contexto da
    oferta (ref e demais informativos). Garante forceInApp=true.

    O que SUBSTITUI:
      • slug em /social/<slug>  → próprio slug
      • matt_word               → próprio matt_word
      • matt_tool               → próprio matt_tool

    O que REMOVE (telemetria/tracking externo):
      • matt_event_ts, matt_d2id, matt_tracing_id
      • origin, sid, tracking_id, source

    O que PRESERVA (contexto da oferta):
      • ref e demais params informativos da query original
    """
    p = urlparse(url)
    path = _RE_SLUG_SOCIAL.sub(f"/social/{_ML_SLUG}", p.path, count=1)

    params_orig = {
        k: v[0]
        for k, v in parse_qs(p.query, keep_blank_values=True).items()
    }

    # Remove telemetria E identidade externa. A identidade externa
    # é removida porque será sobrescrita logo abaixo — evita
    # duplicata na ordem dos parâmetros.
    params_finais = {
        k: v for k, v in params_orig.items()
        if k.lower() not in _PARAMS_TELEMETRIA
        and k.lower() not in _PARAMS_IDENTIDADE_EXTERNA
    }

    # Atribuição explícita: estas chaves são MANDATÓRIAS no output.
    # Atribuição direta torna o código autodocumentável.
    params_finais["matt_word"] = _ML_MATT_WORD
    params_finais["matt_tool"] = _ML_MATT_TOOL
    params_finais["forceInApp"] = "true"

    return urlunparse((
        "https", _HOST_PADRAO, path, "",
        urlencode(params_finais), "",
    ))


# ── Validação anti-vazamento ──────────────────────────────────────
def _validar_url_propria(url: str) -> bool:
    """
    True se a URL é segura para gerar afiliado próprio.

    Verifica:
      - slug em /social/<slug> é o próprio (se presente);
      - matt_word, se presente, é o próprio;
      - matt_tool, se presente, é o próprio;
      - nenhum parâmetro de telemetria de device externo presente.

    NÃO valida `ref` nem demais parâmetros informativos da query:
    eles carregam contexto da oferta, não identidade externa.

    Última barreira antes de gerar o link curto. Em qualquer dúvida
    retorna False — a pipeline interpretará como AUSENTE.
    """
    try:
        p = urlparse(url)
    except Exception:
        return False

    # Slug — quando presente em /social/<slug>, deve ser o próprio.
    m = _RE_SLUG_SOCIAL.search(p.path)
    if m and m.group(2) != _ML_SLUG:
        return False

    params = parse_qs(p.query)

    # matt_word — quando presente, deve ser o próprio.
    mw = params.get("matt_word", [""])[0]
    if mw and mw != _ML_MATT_WORD:
        return False

    # matt_tool — quando presente, deve ser o próprio.
    mt = params.get("matt_tool", [""])[0]
    if mt and mt != _ML_MATT_TOOL:
        return False

    # Telemetria de device do afiliado externo NUNCA pode sobreviver
    # até este ponto. Se aparece, é defeito na troca de identidade.
    for k in ("matt_event_ts", "matt_d2id", "matt_tracing_id"):
        if k in params:
            return False

    return True


# ── Cliente da API afiliada (CreateLink v2) ───────────────────────
async def _chamar_api_create_link(
    url_ml: str,
    cenario: str,
    sessao: aiohttp.ClientSession,
) -> Optional[str]:
    """
    Gera o link curto via API CreateLink moderna do Mercado Livre.

    Diferenças arquiteturais frente ao endpoint legado:

      • o servidor recebe identidade SEMÂNTICA estruturada
        (itemId, tag, type, itemAddToList) em vez de URL pré-
        transformada. A construção da URL afiliada longa migrou
        do cliente para o servidor;

      • a validação anti-vazamento é feita pela igualdade entre
        response['tag'] e _ML_MATT_WORD. Se o servidor responder
        com outra tag, abortamos sem retornar afiliado;

      • o parser de resposta é canônico: short_url no topo do
        dicionário, com formato estável.
    """
    parsed = urlparse(url_ml)

    payload = {
        "tag":             _ML_MATT_WORD,
        "type":            _TYPE_POR_CENARIO.get(cenario, "social_profile"),
        "extraCommission": "false",
        "urls":            [url_ml],
    }

    # itemId — obrigatório quando há MLB extraível no path.
    mlb = _extrair_mlb(parsed.path)
    if mlb:
        payload["itemId"] = mlb

    # itemAddToList — wishlist própria, anexada em product e list.
    if cenario in ("produto", "lista"):
        payload["itemAddToList"] = _ML_LIST_UUID

    for tentativa in range(1, _TENTATIVAS_API + 1):
        token = await _obter_access_token(sessao)
        if not token:
            return None
        try:
            hdrs = {
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/json",
                "Accept":        "application/json",
            }
            async with config._SEM_HTTP:
                async with sessao.post(
                    _ENDPOINT_CREATE_LINK,
                    json=payload,
                    headers=hdrs,
                    timeout=aiohttp.ClientTimeout(total=_TIMEOUT_API),
                ) as r:
                    if r.status == 401:
                        log_nrm.warning(
                            f"⚠️ ML 401 t={tentativa} → forçando refresh"
                        )
                        _TOKEN_STATE["expires_at"] = 0.0
                        if tentativa < _TENTATIVAS_API:
                            await asyncio.sleep(0.4)
                            continue
                        return None
                    if r.status >= 500:
                        log_nrm.warning(
                            f"⚠️ ML {r.status} t={tentativa} (servidor)"
                        )
                        if tentativa < _TENTATIVAS_API:
                            await asyncio.sleep(tentativa * 1.5)
                            continue
                        return None
                    if r.status >= 400:
                        corpo = (await r.text())[:200]
                        log_nrm.error(
                            f"❌ ML {r.status} t={tentativa}: {corpo}"
                        )
                        return None

                    data = await r.json(content_type=None)

                    # Anti-vazamento: tag da resposta deve ser a nossa.
                    tag_resposta = data.get("tag", "")
                    if tag_resposta != _ML_MATT_WORD:
                        log_nrm.error(
                            f"🚫 ML tag externa na resposta: "
                            f"'{tag_resposta}' != '{_ML_MATT_WORD}' → abortado"
                        )
                        return None

                    short = data.get("short_url", "")
                    if short and "meli.la" in short:
                        log_nrm.info(f"✅ ML t={tentativa}: {short}")
                        return short

                    log_nrm.warning(
                        f"⚠️ ML resposta sem short_url t={tentativa}: "
                        f"{str(data)[:200]}"
                    )
        except asyncio.TimeoutError:
            log_nrm.warning(f"⏱ ML timeout t={tentativa}")
        except Exception as e:
            log_nrm.warning(f"⚠️ ML t={tentativa}: {e}")

        if tentativa < _TENTATIVAS_API:
            await asyncio.sleep(tentativa * 1.5)

    return None


# ── Capacidade obrigatória: reconhecimento ────────────────────────
def reconhece(url: str) -> bool:
    """
    Verdadeiro se a URL pertence ao Mercado Livre (domínio próprio
    ou encurtador próprio). Pura, determinística, não falha: uma
    URL malformada simplesmente não é reconhecida.
    """
    if not url:
        return False
    netloc = _netloc(url)
    if not netloc:
        return False
    return _bate_dominio(netloc, _DOMINIOS)


# ── Capacidade obrigatória: extração de identidade ────────────────
def extrai_identidade(url: str) -> IdentidadeProduto:
    """
    Extrai a identidade estruturada de uma URL do Mercado Livre.

    Pura e determinística. Para qualquer URL reconhecida, produz
    sempre uma IdentidadeProduto válida.

    Classificação do tipo de link:
      - encurtador externo (meli.la)     → ENCURTADO
      - path /sec/ (encurtador interno)  → ENCURTADO
      - MLB no path                       → PRODUTO
      - /social/.../lists/                → CAMPANHA
      - /social/                          → CAMPANHA
      - subdomínio de produto sem MLB    → CAMPANHA
      - demais paths reconhecidos         → INVALIDO

    Para PRODUTO, id_global é fornecido como
    'mercadolivre:MLB<digitos>'.
    """
    netloc = _netloc(url)

    # Encurtador externo: natureza final desconhecida até expansão.
    if netloc in _ENCURTADORES:
        return IdentidadeProduto(
            tipo_link=TipoLink.ENCURTADO, id_produto=AUSENTE,
        )

    parsed = urlparse(url)
    path = parsed.path or "/"

    # Encurtador interno (/sec/) — também precisa expansão.
    if _RE_PATH_SEC.search(path):
        return IdentidadeProduto(
            tipo_link=TipoLink.ENCURTADO, id_produto=AUSENTE,
        )

    # Produto identificado por MLB.
    mlb = _extrair_mlb(path)
    if mlb:
        return IdentidadeProduto(
            tipo_link=TipoLink.PRODUTO,
            id_produto=mlb,
            id_global=f"{_IDENTIFICADOR}:{mlb}",
        )

    # Lista social.
    if _eh_social_lists_path(path):
        return IdentidadeProduto(
            tipo_link=TipoLink.CAMPANHA, id_produto=AUSENTE,
        )

    # Campanha social genérica.
    if _eh_social_path(path):
        return IdentidadeProduto(
            tipo_link=TipoLink.CAMPANHA, id_produto=AUSENTE,
        )

    # Subdomínio de produto direto sem MLB visível na URL.
    if netloc in _HOSTS_PRODUTO_DIRETO:
        return IdentidadeProduto(
            tipo_link=TipoLink.CAMPANHA, id_produto=AUSENTE,
        )

    # Paths no domínio ML mas fora das três trilhas afiliáveis.
    return IdentidadeProduto(
        tipo_link=TipoLink.INVALIDO, id_produto=AUSENTE,
    )


# ── Capacidade opcional: limpeza de URL ───────────────────────────
def limpa_url(url: str) -> str:
    """
    Remove parâmetros de telemetria e identidade externa, mantendo
    o contexto da oferta e a identidade do produto. Pura, sem I/O.

    Não realiza substituição de identidade — isso é responsabilidade
    de afilia, sob fluxo controlado. Não detecta cenário — opera
    uniformemente em qualquer URL ML, removendo apenas o ruído que
    sempre é indesejável.
    """
    try:
        p = urlparse(url)
        params_orig = {
            k: v[0]
            for k, v in parse_qs(p.query, keep_blank_values=True).items()
        }
        limpos = _filtrar_params(
            params_orig,
            _PARAMS_TELEMETRIA | _PARAMS_IDENTIDADE_EXTERNA,
        )
        return urlunparse(p._replace(
            query=urlencode(limpos) if limpos else "",
            fragment="",
        ))
    except Exception:
        return url


# ── Capacidade obrigatória: afiliação ─────────────────────────────
async def afilia(url: str, sessao: aiohttp.ClientSession) -> object:
    """
    Converte uma URL do Mercado Livre em link afiliado próprio
    (meli.la), através da API CreateLink moderna.

    Após a migração da v2, o fluxo simplifica: o cliente envia a
    URL original + identidade semântica estruturada, e o servidor
    devolve short_url + long_url já transformados com a nossa
    identidade aplicada. As transformações locais de URL
    (_trocar_identidade_lista, _trocar_identidade_campanha) deixam
    de ser invocadas; a anti-vazamento migra para a verificação
    da tag na resposta do servidor.

    Fluxo:
      1. sanitiza a URL recebida
      2. consulta o cache mediado
      3. expande encurtadores externos (meli.la) ou internos (/sec/)
      4. valida que a URL pós-expansão continua sendo ML
      5. detecta cenário entre lista, campanha e produto
      6. chama CreateLink com payload estruturado
      7. valida que a resposta é uma meli.la legítima
      8. registra o par (original, curto) no cache mediado
    """
    url = _sanitizar_url(url)

    # 2. Cache mediado.
    cache = consultar_link(url)
    if cache:
        return cache

    # 3. Expansão de encurtador externo ou interno.
    url_expandida = url
    parsed = urlparse(url)
    precisa_expandir = (
        _netloc(url) in _ENCURTADORES
        or _RE_PATH_SEC.search(parsed.path or "")
    )
    if precisa_expandir:
        try:
            async with config._SEM_HTTP:
                url_expandida = await desencurtar(url, sessao)
        except Exception as e:
            log_nrm.warning(f"⚠️ ML expansão falhou: {e}")
            return AUSENTE

    # 4. Pós-expansão: ainda é ML?
    if not reconhece(url_expandida):
        log_nrm.warning(
            f"⚠️ ML expansão saiu do domínio: "
            f"{_netloc(url_expandida)} → descarta"
        )
        return AUSENTE

    # 5. Detecção de cenário (necessária para o campo 'type' do payload).
    cenario = _cenario_de(url_expandida)
    if not cenario:
        log_nrm.warning(
            f"❓ ML tipo desconhecido pós-expansão: "
            f"{url_expandida[:80]} → descarta"
        )
        return AUSENTE

    log_nrm.info(f"🎯 ML {cenario}: {url_expandida[:90]}")

    # 6. Chamada à API afiliada moderna (CreateLink v2).
    short = await _chamar_api_create_link(url_expandida, cenario, sessao)
    if not short:
        log_nrm.warning(f"❌ ML API falhou para {url[:60]}")
        return AUSENTE

    # 7. Validação da resposta — host deve ser meli.la.
    if _netloc(short) != "meli.la":
        log_nrm.error(f"⚠️ ML resposta com host inesperado: {short}")
        return AUSENTE

    # 8. Registro no cache mediado.
    registrar_link(url, short, _IDENTIFICADOR)
    return short


# ── Definição da plataforma ───────────────────────────────────────
PLATAFORMA = Plataforma(
    identificador=_IDENTIFICADOR,
    versao_contrato=CONTRACT_VERSION,
    reconhece=reconhece,
    extrai_identidade=extrai_identidade,
    afilia=afilia,
    parametros_temporais=_PARAMETROS_TEMPORAIS,
    encurtadores_forca_get=_ENCURTADORES_FORCA_GET,
   )
