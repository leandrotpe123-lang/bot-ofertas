"""
Plataforma — Amazon.

Adapter puro do contrato de plataforma. Conhece URL da Amazon,
identidade de produto e a sua própria política de afiliação. Não
conhece pipeline, publicação, dedupe nem banco de dados.

REGRA DE CONVERSÃO (o formato da entrada define o formato da saída):

  LONGA  → troca somente o valor da tag → SiteStripe → short oficial
  CURTA  → expande → troca a tag       → SiteStripe → short oficial

  Em qualquer falha do SiteStripe a oferta NUNCA se perde: publica-se
  a URL longa já afiliada.

CACHE-FIRST: o short oficial é guardado no cache mediado sob a
IDENTIDADE da oferta (o ASIN), e não apenas sob a URL recebida. Duas
URLs diferentes do mesmo produto — Promotom e Fumotom mandam formas
distintas do mesmo ASIN — reaproveitam o mesmo short sem uma segunda
chamada externa.

SESSÃO: o SiteStripe exige sessão autenticada de Associados. A
sessão é própria deste módulo, com cookie jar que aceita e devolve
os Set-Cookie da Amazon. Valor de cookie NUNCA é registrado em log.
"""
from __future__ import annotations

import asyncio
import base64
import os
import re
import time
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp
from yarl import URL

import config
from logger import log_nrm
from plataformas.contrato import (
    AUSENTE,
    CONTRACT_VERSION,
    Afiliacao,
    IdentidadeProduto,
    Plataforma,
    TipoLink,
)
from utils.cache_links import consultar_link, registrar_link
from utils.uma_por_vez import uma_por_vez
from utils.url_resolver import desencurtar
from utils.urls import _cache_key, _netloc, _sanitizar_url


# ── Identidade da plataforma ──────────────────────────────────────
_IDENTIFICADOR = "amazon"

_AMZ_TAG = os.environ.get("AMAZON_TAG", "")
_AMZ_STORE_ID = os.environ.get("AMAZON_STORE_ID", "")


# ── Domínios e encurtadores ───────────────────────────────────────
_DOMINIOS = frozenset({
    "amazon.com.br", "amazon.com",
})
_ENCURTADORES = frozenset({
    "amzn.to", "link.amazon", "a.co", "amzn.com", "amzlink.to",
})

_ENCURTADORES_FORCA_GET = frozenset({
    "amzn.to", "a.co", "amzn.com", "amzlink.to",
})

# Domínios que a própria Amazon devolve como link curto oficial.
# Verificado em produção: getShortUrl devolve link.amazon, não
# apenas amzn.to. Exigir só amzn.to reprovaria sucesso legítimo.
_SHORTS_OFICIAIS = frozenset({
    "amzn.to", "link.amazon", "a.co",
})

# ── Hosts de campanha ─────────────────────────────────────────────
_HOSTS_CAMPANHA = frozenset({
    "amazon.com.br",
    "primevideo.com",
})

# ── Padrões de extração ───────────────────────────────────────────
_P_ASIN = [
    re.compile(r'/dp/([A-Z0-9]{10})', re.I),
    re.compile(r'/gp/product/([A-Z0-9]{10})', re.I),
    re.compile(r'[?&]asin=([A-Z0-9]{10})', re.I),
]
_P_PROMO = re.compile(r'/promotion/psp/([A-Z0-9]{8,16})', re.I)
_PATHS_SEM_AFILIACAO = re.compile(
    r'^/(?:gaming(?:/|$)|claims(?:/|$)|gp/yourstore(?:/|$)|'
    r'gp/css(?:/|$)|gp/help(?:/|$)|gp/cart(?:/|$)|wishlist(?:/|$)|'
    r'hz/|ap/|gp/registry(?:/|$))',
    re.I,
)

# ── Parâmetros de limpeza de URL ──────────────────────────────────
_PARAMS_MANTER = frozenset({
    "keywords", "node", "k", "i", "rh", "n", "field-keywords",
})


# ── SiteStripe ────────────────────────────────────────────────────
_ENDPOINT_SHORT = (
    "https://www.amazon.com.br/associates/sitestripe/getShortUrl"
)
_MARKETPLACE_ID = "526970"
_TIMEOUT_SHORT = 15

# Disjuntor: sessão vencida ou desafio de WAF falha em série. Sem
# isto, cookie morto vira centenas de tentativas por hora contra a
# Amazon com sessão inválida — que é como uma conta de associado
# entra na mira. Aberto o circuito, publica-se a longa direto.
_LIMITE_FALHAS = 3
_PAUSA_DISJUNTOR = 300.0

_falhas_seguidas = 0
_disjuntor_ate = 0.0

_sessao_amz: Optional[aiohttp.ClientSession] = None
_lock_sessao = asyncio.Lock()

_CABECALHOS_SHORT = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "pt-BR",
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "referer": "https://www.amazon.com.br/",
    "x-requested-with": "XMLHttpRequest",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}


# ── Funções de apoio ──────────────────────────────────────────────
def _bate_dominio(netloc: str, dominios: frozenset) -> bool:
    """Verdadeiro se o netloc pertence ao conjunto de domínios."""
    for d in dominios:
        if netloc == d or netloc.endswith("." + d):
            return True
    return False


def _extrair_asin(parsed) -> str:
    """Extrai o ASIN de uma URL Amazon, ou string vazia se ausente."""
    texto = parsed.path + "?" + parsed.query
    for padrao in _P_ASIN:
        m = padrao.search(texto)
        if m:
            return m.group(1).upper()
    return ""


# ── Capacidade obrigatória: reconhecimento ────────────────────────
def reconhece(url: str) -> bool:
    if not url:
        return False
    netloc = _netloc(url)
    if not netloc:
        return False
    return _bate_dominio(netloc, _DOMINIOS) or netloc in _ENCURTADORES


# ── Capacidade obrigatória: extração de identidade ────────────────
def extrai_identidade(url: str) -> IdentidadeProduto:
    netloc = _netloc(url)

    if netloc in _ENCURTADORES:
        return IdentidadeProduto(
            tipo_link=TipoLink.ENCURTADO, id_produto=AUSENTE,
        )

    parsed = urlparse(url)

    if _PATHS_SEM_AFILIACAO.match(parsed.path):
        return IdentidadeProduto(
            tipo_link=TipoLink.INVALIDO, id_produto=AUSENTE,
        )

    asin = _extrair_asin(parsed)
    if asin:
        return IdentidadeProduto(
            tipo_link=TipoLink.PRODUTO,
            id_produto=asin,
            id_global=f"{_IDENTIFICADOR}:{asin}",
        )

    promo = _P_PROMO.search(parsed.path)
    if promo:
        promo_id = promo.group(1).upper()
        return IdentidadeProduto(
            tipo_link=TipoLink.CAMPANHA,
            id_produto=AUSENTE,
            id_global=f"{_IDENTIFICADOR}:promo_{promo_id}",
        )

    if re.search(r'/s[/?]|/deals|/b[/?]', parsed.path):
        tipo = TipoLink.BUSCA
    elif re.search(r'/events/|/stores/', parsed.path):
        tipo = TipoLink.EVENTO
    else:
        tipo = TipoLink.CAMPANHA

    return IdentidadeProduto(tipo_link=tipo, id_produto=AUSENTE)


# ── Capacidade opcional: limpeza de URL ───────────────────────────
def limpa_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        params = {
            k: v[0] for k, v in parse_qs(parsed.query).items()
            if k.lower() in _PARAMS_MANTER and len(v[0]) < 60
        }
        return urlunparse(parsed._replace(
            query=urlencode(params), fragment="",
        ))
    except Exception:
        return url


# ── Troca cirúrgica da identidade de afiliado ─────────────────────
# A URL longa que chega dos grupos JÁ é a URL final da Amazon. O
# único campo que carrega identidade de afiliado é `tag`. Trocar o
# valor de `tag` e preservar todo o resto byte a byte é a operação
# correta: não reconstrói rota, não inventa campo, não descarta
# parâmetro desconhecido — `th` e `psc`, que selecionam a variação
# do produto, sobrevivem. Mesma doutrina já aplicada na Magalu.
def _partir_url(url: str) -> tuple:
    """Separa (base, query) por corte de cadeia, sem reserializar.

    O FRAGMENTO é descartado, como todo ramo deste módulo já faz:
    não é enviado ao servidor, não participa de atribuição, e
    preservá-lo deixaria a tag de outro afiliado visível no texto
    publicado quando a URL vem na forma anômala `#algo?tag=...`.
    """
    base, _, query = url.split("#", 1)[0].partition("?")
    return base, query


def _aplicar_tag(query: str) -> str:
    """Query com EXATAMENTE uma `tag`, a nossa, na posição original
    quando já existia, ao final quando não existia. Todo parâmetro
    que não é `tag` atravessa intacto: sem decodificar, sem
    recodificar, sem reordenar."""
    if not query:
        return f"tag={_AMZ_TAG}"
    saida, achou = [], False
    for parte in query.split("&"):
        if parte.split("=", 1)[0].lower() == "tag":
            if not achou:
                saida.append(f"tag={_AMZ_TAG}")
                achou = True
            continue
        saida.append(parte)
    if not achou:
        saida.append(f"tag={_AMZ_TAG}")
    return "&".join(p for p in saida if p)


def _trocar_tag(url: str) -> str:
    """Fast path: URL Amazon longa entra, sai idêntica exceto a tag."""
    base, query = _partir_url(url)
    return f"{base}?{_aplicar_tag(query)}"


def _tag_unica_nossa(url: str) -> bool:
    """Guard: uma única `tag` na saída, e o valor é o nosso. Nenhuma
    identidade de afiliado estrangeira pode sobreviver."""
    _, query = _partir_url(url)
    valores = [
        (p.split("=", 1) + [""])[1]
        for p in query.split("&")
        if p.split("=", 1)[0].lower() == "tag"
    ]
    return valores == [_AMZ_TAG]


_PREFIXO_PROMO = f"{_IDENTIFICADOR}:promo_"


def _chave_identidade(identidade: IdentidadeProduto) -> str:
    """Chave de cache derivada da IDENTIDADE, não da URL recebida.

    Forma canônica mínima do alvo. Qualquer URL da mesma identidade —
    decorada, com caminho descritivo, vinda de qualquer grupo —
    produz esta mesma chave, e por isso reaproveita o short já
    obtido em vez de chamar o SiteStripe de novo.

    Devolve cadeia vazia quando não existe identidade estável para
    ancorar o short. Busca e evento caem aí de propósito: a mesma
    página de busca hoje e amanhã não é a mesma oferta, e encurtar
    o que não tem identidade só gastaria chamada externa.
    """
    if (identidade.tipo_link == TipoLink.PRODUTO
            and isinstance(identidade.id_produto, str)):
        return (
            f"https://www.amazon.com.br/dp/{identidade.id_produto}"
            f"?tag={_AMZ_TAG}"
        )
    global_ = identidade.id_global or ""
    if global_.startswith(_PREFIXO_PROMO):
        promo = global_[len(_PREFIXO_PROMO):]
        return (
            f"https://www.amazon.com.br/promotion/psp/{promo}"
            f"?tag={_AMZ_TAG}"
        )
    return ""


def _cache_aproveitavel(guardado, url: str) -> bool:
    """Decide se um registro do cache ainda serve ao fluxo atual.

    Registro legado — gravado antes do SiteStripe, com a URL longa
    como forma publicada — não pode bloquear um alvo que hoje tem
    identidade estável. Se bloqueasse, produto e promoção nunca
    chegariam ao encurtamento oficial; e como a leitura renova o ts,
    o registro nunca expiraria sozinho. Ignorá-lo faz o fluxo seguir
    e, no sucesso, a gravação sobrescreve a entrada velha pelo short.

    Continua valendo como acerto de cache:
      - short em domínio oficial CUJA canônica carrega a nossa tag;
      - alvo sem identidade estável (busca, evento, campanha sem id,
        caminho sem afiliação), onde o comportamento é o de sempre.

    O short oficial sozinho não basta: ele é opaco e não revela a
    quem credita. Quem credita é a canônica. Um short gravado sob
    outra tag — o caso natural depois de uma troca de AMAZON_TAG —
    seria reaproveitado para sempre, publicando atribuição que não
    é nossa. Exigir a nossa tag na canônica invalida esses registros
    sozinho, sem migração de banco: na troca de tag, o cache inteiro
    se regenera no primeiro acerto de cada alvo.
    """
    publicada = getattr(guardado, "publicada", "") or ""
    if _netloc(publicada) in _SHORTS_OFICIAIS:
        return _tag_unica_nossa(getattr(guardado, "canonica", "") or "")
    if _netloc(url) in _ENCURTADORES:
        # Sem expandir não há como saber a identidade, e expandir
        # aqui seria ir à rede antes do cache. Trata como legado.
        return False
    return not _chave_identidade(extrai_identidade(url))


# ── Sessão autenticada do SiteStripe ──────────────────────────────
def _cookies_iniciais() -> dict:
    """Semente do cookie jar, a partir de AMAZON_COOKIE.

    Aceita a cadeia de cookies crua ou em Base64 — a extensão de
    captura entrega Base64. Nenhum valor é registrado em log, aqui
    ou em qualquer outro ponto do módulo.
    """
    bruto = (os.environ.get("AMAZON_COOKIE") or "").strip()
    if not bruto:
        return {}
    if not (";" in bruto and "=" in bruto):
        try:
            bruto = base64.b64decode(bruto, validate=True).decode("utf-8")
        except Exception:
            pass
    pares = {}
    for parte in bruto.split(";"):
        nome, sep, valor = parte.strip().partition("=")
        if sep and nome:
            pares[nome] = valor
    return pares


async def _obter_sessao() -> aiohttp.ClientSession:
    """Sessão persistente e privada deste módulo.

    Privada de propósito: os cookies de Associados não podem entrar
    na sessão compartilhada do sistema. O cookie jar aceita os
    Set-Cookie da Amazon e os devolve nas chamadas seguintes —
    verificado em produção: a Amazon devolve session-id, sst-acbbr e
    companhia a cada chamada, e a segunda chamada na mesma sessão
    responde em fração do tempo da primeira.
    """
    global _sessao_amz
    if _sessao_amz is not None and not _sessao_amz.closed:
        return _sessao_amz
    async with _lock_sessao:
        if _sessao_amz is None or _sessao_amz.closed:
            jar = aiohttp.CookieJar()
            cookies = _cookies_iniciais()
            if cookies:
                jar.update_cookies(
                    cookies, response_url=URL("https://www.amazon.com.br"),
                )
            _sessao_amz = aiohttp.ClientSession(
                cookie_jar=jar, headers=_CABECALHOS_SHORT,
            )
            log_nrm.info(
                f"🔐 AMZ sessão SiteStripe iniciada "
                f"({len(cookies)} cookies)"
            )
    return _sessao_amz


async def _http_getshorturl(params: dict) -> tuple:
    """Costura de I/O: devolve (status, content_type, dados|None).

    Isolada de propósito — é o único ponto deste módulo que fala com
    a rede, o que mantém a política de validação, o disjuntor e o
    cache testáveis sem acesso externo.
    """
    sessao = await _obter_sessao()
    async with config._SEM_HTTP:
        async with sessao.get(
            _ENDPOINT_SHORT, params=params,
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT_SHORT),
            allow_redirects=False,
        ) as resposta:
            tipo = (resposta.headers.get("content-type") or "").lower()
            if resposta.status != 200 or "json" not in tipo:
                return resposta.status, tipo, None
            return resposta.status, tipo, await resposta.json(
                content_type=None,
            )


def _registrar_falha(motivo: str) -> None:
    """Contabiliza a falha e abre o disjuntor no limite."""
    global _falhas_seguidas, _disjuntor_ate
    _falhas_seguidas += 1
    log_nrm.warning(
        f"⚠️ AMZ SiteStripe falhou ({_falhas_seguidas}/"
        f"{_LIMITE_FALHAS}): {motivo}"
    )
    if _falhas_seguidas >= _LIMITE_FALHAS:
        _disjuntor_ate = time.monotonic() + _PAUSA_DISJUNTOR
        log_nrm.error(
            f"🔌 AMZ SiteStripe suspenso por "
            f"{int(_PAUSA_DISJUNTOR)}s — publicando URL longa"
        )


async def _sitestripe(canonica: str) -> str:
    """Devolve o short oficial da Amazon, ou cadeia vazia.

    Só o sucesso é sucesso: HTTP 200, JSON válido, `ok` e `isOk`
    verdadeiros, `shortUrl` num domínio oficial e a `longUrl` de
    resposta carregando a nossa tag. Qualquer outra coisa — desafio
    de WAF, página de login, captcha, 4xx, 5xx, timeout — é falha, e
    falha nunca inventa short.
    """
    global _falhas_seguidas
    if time.monotonic() < _disjuntor_ate:
        return ""
    if not _AMZ_TAG:
        return ""

    params = {
        "longUrl": canonica,
        "marketplaceId": _MARKETPLACE_ID,
        "storeId": _AMZ_STORE_ID,
    }
    try:
        status, tipo, dados = await _http_getshorturl(params)
    except Exception as e:
        _registrar_falha(type(e).__name__)
        return ""

    if dados is None:
        _registrar_falha(f"HTTP {status} tipo={tipo!r}")
        return ""
    if dados.get("ok") is not True or dados.get("isOk") is not True:
        _registrar_falha(
            f"ok={dados.get('ok')} isOk={dados.get('isOk')}"
        )
        return ""

    curta = dados.get("shortUrl") or ""
    if not curta or _netloc(curta) not in _SHORTS_OFICIAIS:
        _registrar_falha("shortUrl fora dos domínios oficiais")
        return ""
    if f"tag={_AMZ_TAG}" not in (dados.get("longUrl") or ""):
        _registrar_falha("longUrl de resposta sem a nossa tag")
        return ""

    _falhas_seguidas = 0
    return curta


# ── Capacidade obrigatória: afiliação ─────────────────────────────
def _construir_url_afiliada(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)

        if _PATHS_SEM_AFILIACAO.match(parsed.path):
            return urlunparse(parsed._replace(query="", fragment=""))

        asin = _extrair_asin(parsed)
        if asin:
            # FAST PATH — a URL longa já é a URL final da Amazon:
            # preserva tudo, troca só o valor da tag.
            rapida = _trocar_tag(url)
            if _tag_unica_nossa(rapida):
                return rapida
            # Guard reprovou (forma anômala de query): cai para a
            # construção canônica, nunca para a entrada crua.
            return urlunparse(parsed._replace(
                path=f"/dp/{asin}",
                query=f"tag={_AMZ_TAG}",
                fragment="",
            ))

        if "/promotion/" in parsed.path:
            return urlunparse(parsed._replace(
                query=f"tag={_AMZ_TAG}", fragment="",
            ))

        limpa = limpa_url(url)
        p_limpa = urlparse(limpa)
        query = parse_qs(p_limpa.query)
        query["tag"] = [_AMZ_TAG]
        pares = [(k, v) for k, vs in query.items() for v in vs]
        return urlunparse(p_limpa._replace(
            query=urlencode(pares), fragment="",
        ))
    except Exception:
        return None


async def _encurtar_identidade(
    url_original: str, afiliada: str, chave: str,
) -> object:
    """Obtém o short oficial para uma identidade estável — produto ou
    promoção — sob exclusão por essa identidade.

    Reconsulta o cache ao entrar: quem esperou pela exclusão encontra
    o short que o primeiro acabou de gravar, em vez de repetir a
    chamada externa.
    """
    guardado = consultar_link(chave)
    if guardado and _cache_aproveitavel(guardado, chave):
        registrar_link(url_original, guardado, _IDENTIFICADOR)
        return guardado

    curta = await _sitestripe(afiliada)

    if curta:
        resultado = Afiliacao(publicada=curta, canonica=afiliada)
        # Sob a IDENTIDADE: qualquer outra URL do mesmo ASIN
        # reaproveita este short sem nova chamada externa.
        registrar_link(chave, resultado, _IDENTIFICADOR)
        registrar_link(url_original, resultado, _IDENTIFICADOR)
        log_nrm.info(f"✅ AMZ short oficial: {curta}")
        return resultado

    # Falha: publica a longa afiliada, sem inventar short. Gravada
    # SÓ sob a URL recebida — nunca sob a identidade, para que a
    # próxima oferta do mesmo alvo tente o SiteStripe de novo em vez
    # de herdar a falha por dias.
    #
    # Quando a URL recebida JÁ É a forma canônica, as duas chaves
    # coincidem — caso comum quando a nossa própria publicação longa
    # volta a entrar no bot. Gravar aí envenenaria a identidade: o
    # alvo ficaria presa na forma longa e, como a leitura renova o
    # ts, o TTL nunca a expiraria. Nesse caso não se grava nada.
    if _cache_key(url_original) != _cache_key(chave):
        registrar_link(url_original, afiliada, _IDENTIFICADOR)
    log_nrm.info(f"↩️ AMZ sem short, publica longa: {afiliada[:70]}")
    return afiliada


async def afilia(url: str, sessao: aiohttp.ClientSession) -> object:
    """
    Converte uma URL da Amazon na sua forma afiliada.

    Capacidade com efeito colateral controlado: acessa a rede para
    expandir encurtadores e para obter o short oficial, e consulta o
    cache de links mediado. Não propaga exceções ao core: qualquer
    falha legítima resulta no sentinela AUSENTE — ou, no caso do
    SiteStripe, na URL longa já afiliada.
    """
    url = _sanitizar_url(url)

    cache = consultar_link(url)
    if cache and _cache_aproveitavel(cache, url):
        return cache

    entrou_curta = _netloc(url) in _ENCURTADORES

    url_expandida = url
    if entrou_curta:
        try:
            async with config._SEM_HTTP:
                url_expandida = await desencurtar(url, sessao)
        except Exception as e:
            log_nrm.warning(f"⚠️ AMZ expansão falhou: {e}")
            return AUSENTE
        # A expansão precisa ter chegado a um domínio Amazon real.
        # Sem isto, um short não resolvido — desencurtar devolve a
        # própria entrada em falha de rede — seguiria adiante e
        # produziria publicação fabricada.
        if not _bate_dominio(_netloc(url_expandida), _DOMINIOS):
            log_nrm.warning(
                "⚠️ AMZ expansão não resultou em URL Amazon — descarta"
            )
            return AUSENTE

    identidade = extrai_identidade(url_expandida)
    if identidade.tipo_link == TipoLink.INVALIDO:
        afiliada = _construir_url_afiliada(url_expandida)
        if afiliada:
            registrar_link(url, afiliada, _IDENTIFICADOR)
            return afiliada
        return AUSENTE

    afiliada = _construir_url_afiliada(url_expandida)
    if not afiliada or not _bate_dominio(_netloc(afiliada), _DOMINIOS):
        log_nrm.warning(f"⚠️ AMZ afiliação inválida: {afiliada}")
        return AUSENTE

    # Identidade estável (produto ou promoção): cache-first, depois
    # SiteStripe. O critério não é o tipo do link e sim a existência
    # de uma âncora estável — a Amazon encurta promoção igual encurta
    # produto, verificado contra o endpoint real.
    chave = _chave_identidade(identidade)
    if chave:
        guardado = consultar_link(chave)
        if guardado and _cache_aproveitavel(guardado, chave):
            registrar_link(url, guardado, _IDENTIFICADOR)
            return guardado
        # Exclusão pela identidade: duas ofertas simultâneas do mesmo
        # alvo não disparam duas chamadas externas. Chave diferente
        # da usada pelo core, portanto sem reentrância.
        return await uma_por_vez(
            f"{_IDENTIFICADOR}:short:{identidade.id_global}",
            _encurtar_identidade, url, afiliada, chave,
        )

    # Busca, evento e campanha sem identidade não têm âncora estável
    # para o short: seguem publicando a longa afiliada.
    registrar_link(url, afiliada, _IDENTIFICADOR)
    log_nrm.info(f"✅ AMZ afiliada: {afiliada[:70]}")
    return afiliada


PLATAFORMA = Plataforma(
    identificador=_IDENTIFICADOR,
    versao_contrato=CONTRACT_VERSION,
    reconhece=reconhece,
    extrai_identidade=extrai_identidade,
    afilia=afilia,
    encurtadores_forca_get=_ENCURTADORES_FORCA_GET,
    encurtadores=_ENCURTADORES,
    hosts_campanha=_HOSTS_CAMPANHA,
)
