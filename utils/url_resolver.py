"""
Utilitário — Resolução de URL.

Responsabilidade única: resolver uma URL até sua forma final,
seguindo redirecionamentos HTTP, meta-refresh e redirecionamentos
por JavaScript.

Recebe uma URL (potencialmente encurtada ou ofuscada) e devolve a
URL de destino real. Usa cache de resolução para evitar repetir
requisições de rede já realizadas.

NÃO faz:
  - classificação de plataforma (responsabilidade da classificação)
  - afiliação de links (responsabilidade das plataformas)
  - normalização de mensagem (responsabilidade da normalização)
  - validação de conteúdo (responsabilidade dos filtros)
"""
from __future__ import annotations

import asyncio
import os
import random
import re
import time

import aiohttp
from bs4 import BeautifulSoup

from config import USER_AGENTS
from globals import _get_raw, _set_raw
from logger import log_nrm
from utils.urls import _netloc, _sanitizar_url


# ── Parâmetros ────────────────────────────────────────────────────
_PROFUNDIDADE_MAX = 15
_TIMEOUT_HEAD     = 10
_TIMEOUT_GET      = 20
_MAX_REDIRECTS    = 20
_LIMITE_HTML      = 500_000
# ── Instrumentação TEMPORÁRIA da expansão (SHP_PERF=1) ────────────
# Decompõe o custo de `desencurtar` em subetapas. Vive aqui, e não
# na Shopee, porque a expansão é do core — não há como decompô-la de
# fora. Desligada, cada gancho é um `if` sobre constante de módulo.
# Remover junto com a instrumentação da Shopee.
_RES_PERF = os.environ.get("SHP_PERF", "") == "1"
_RES_RESUMO = 25
_res_c: dict = {}
_res_t: dict = {}


def _res_agora() -> float:
    return time.monotonic() if _RES_PERF else 0.0


def _res_marca(via: str, t0: float = 0.0) -> None:
    if not _RES_PERF:
        return
    _res_c[via] = _res_c.get(via, 0) + 1
    if t0:
        _res_t[via] = _res_t.get(via, 0.0) + (time.monotonic() - t0)


def _res_entrada(depth: int) -> None:
    """Conta chamadas e emite o resumo UMA vez por bloco."""
    if not _RES_PERF:
        return
    _res_c["chamadas"] = _res_c.get("chamadas", 0) + 1
    if depth == 0:
        _res_c["raiz"] = _res_c.get("raiz", 0) + 1
    else:
        _res_c["recursao"] = _res_c.get("recursao", 0) + 1
    n = _res_c.get("raiz", 0)
    if not n or n % _RES_RESUMO or n == _res_c.get("_ult"):
        return
    _res_c["_ult"] = n
    contas = " ".join(
        f"{k}={v}" for k, v in sorted(_res_c.items())
        if not k.startswith("_")
    )
    medias = " ".join(
        f"{k}={_res_t[k] / max(_res_c.get(k, 1), 1) * 1000:.0f}ms"
        for k in sorted(_res_t)
    )
    log_nrm.debug(f"📊 RES perf | {contas} | media {medias}")


# ── Composição de hosts que exigem GET ────────────────────────────
# Conjunto local de encurtadores genéricos do core, não atribuídos
# a nenhuma plataforma específica do projeto. Hosts cujos servidores
# não respondem corretamente a HEAD e exigem GET direto na resolução
# de redirecionamento.
_FORCA_GET_GENERICOS = frozenset({
    "cutt.ly", "ofertou.ai", "tidd.ly",
})

def _compor_forca_get() -> frozenset[str]:
    """
    Compõe o conjunto efetivo de hosts que exigem GET: une os
    encurtadores genéricos do core (universais, sem dono) com a
    visão plana da composição de encurtadores_forca_get das
    plataformas, obtida do registry (dono da camada coletiva). A
    origem fica retida no registry; aqui só a visão plana importa.
    No pior caso (registry indisponível) resta o conjunto local de
    genéricos. Não faz cache — o cache vive no registry.
    """
    composto: set[str] = set(_FORCA_GET_GENERICOS)

    try:
        from plataformas import registry  # lazy: evita ciclo de import
        # Composição com proveniência vive no registry (dono da camada
        # coletiva). Aqui usamos a visão plana — as chaves do mapa
        # elemento -> donos — para o teste de pertinência. A origem
        # fica retida e consultável em registry.compor_capacidade.
        composto |= set(
            registry.compor_capacidade("encurtadores_forca_get").keys()
        )
    except Exception as e:
        log_nrm.warning(
            f"⚠ _compor_forca_get: falha compondo via registry: {e}"
        )

    return frozenset(composto)

def _compor_encurtadores() -> "frozenset[str] | None":
    """Hosts declarados como ENCURTADOR pelas plataformas.

    Devolve None quando a composição FALHA — estado distinto de
    frozenset() vazio, que significa "compôs com sucesso e ninguém
    declarou". A distinção existe porque aqui a degradação NÃO é
    segura: um conjunto vazio faria _eh_intermediario parar cedo e
    devolver um encurtador NÃO RESOLVIDO como se fosse o destino.
    Em _compor_forca_get a degradação é inofensiva — o fallback GET
    resolve mesmo assim; aqui não há fallback.

    Fonte e proveniência vivem no registry, como em
    _compor_forca_get. A falha nunca é silenciosa.
    """
    try:
        from plataformas import registry  # lazy: evita ciclo de import
        return frozenset(
            registry.compor_capacidade("encurtadores").keys()
        )
    except Exception as e:
        log_nrm.warning(
            f"⚠ _compor_encurtadores: falha compondo via registry: {e}"
        )
        return None


def _eh_intermediario(url: str) -> bool:
    """O destino ainda é etapa intermediária, ou já é o alvo final?

    Fontes, deliberadamente separadas de encurtadores_forca_get:

      _FORCA_GET_GENERICOS     encurtadores universais do core
      capacidade encurtadores  o que cada plataforma declara

    NÃO usa a composição de encurtadores_forca_get. Aquele conjunto
    é quirk de TRANSPORTE ("este host não responde a HEAD") e o
    contrato permite que contenha host que não seja encurtador — um
    host de loja que recuse HEAD entraria lá legitimamente, e
    tratá-lo como intermediário faria a resolução recursar até o
    teto de profundidade. Verificado host a host nas cinco
    plataformas: hoje os conjuntos coincidem, mas a relação é
    factual, não definicional.

    Composição indisponível (None) devolve True: continua resolvendo,
    que é exatamente o comportamento anterior a esta frente. Falha de
    infraestrutura degrada para o código de ontem, nunca para uma
    resposta pior.

    Sem host hardcoded, sem heurística.
    """
    nl = _netloc(url)
    if not nl:
        return False
    declarados = _compor_encurtadores()
    if declarados is None:
        return True
    conhecidos = set(_FORCA_GET_GENERICOS) | set(declarados)
    return any(nl == d or nl.endswith("." + d) for d in conhecidos)

async def desencurtar(
    url: str,
    sessao: aiohttp.ClientSession,
    depth: int = 0,
) -> str:
    """
    Resolve uma URL até sua forma final.

    Segue, em ordem: redirecionamentos HTTP (HEAD e GET), meta-refresh
    em HTML, redirecionamento por JavaScript, e tags og:url / canonical.
    A recursão é limitada por _PROFUNDIDADE_MAX.

    Resultados são cacheados via `_set_raw` / `_get_raw`. Em caso de
    timeout ou erro de rede, devolve a URL recebida sem falhar.
    """
    if depth > _PROFUNDIDADE_MAX:
        return url

    url = _sanitizar_url(url)
    if not url.startswith(("http://", "https://")):
        return url

    nl = _netloc(url)
    if depth > 0 and nl == "cutt.ly":
        return url

    _res_entrada(depth)

    cached = _get_raw(url)
    if cached:
        _res_marca("cache_hit")
        return cached

    hdrs = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }

    try:
        hosts = _compor_forca_get()
        usar_head = (
            nl not in hosts
            and not any(nl.endswith("." + d) for d in hosts)
        )
        if usar_head:
            try:
                _t = _res_agora()
                async with sessao.head(
                    url, headers=hdrs, allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=_TIMEOUT_HEAD),
                    max_redirects=_MAX_REDIRECTS,
                ) as r:
                    _res_marca("head", _t)
                    final = str(r.url)
                    if final != url:
                        _set_raw(url, final)
                        # allow_redirects=True JÁ seguiu a cadeia HTTP
                        # inteira: `final` é o destino por definição do
                        # protocolo. Recursar aqui custava um segundo
                        # HEAD mais um GET com download e parse —
                        # medido em produção: 285ms por link, 60% do
                        # custo total da expansão — só para reconfirmar
                        # o que o HTTP já havia entregue.
                        #
                        # A resolução continua quando o destino ainda é
                        # etapa intermediária declarada por alguma
                        # plataforma (encurtador que resolve por
                        # meta-refresh ou JS, e portanto não aparece na
                        # cadeia de redirecionamento HTTP).
                        #
                        # RESSALVA CONHECIDA E ACEITA (caso C10 do
                        # corpus): se o destino HTTP declarar um
                        # og:url/canonical DIFERENTE, ele deixa de ser
                        # seguido. Não ocorreu em nenhum dos 25 links
                        # reais medidos, e não altera a identidade
                        # Shopee, que deriva do par (loja, item) no
                        # caminho e ignora query string.
                        if _eh_intermediario(final):
                            return await desencurtar(
                                final, sessao, depth + 1)
                        return final
            except Exception:
                pass

        _t = _res_agora()
        async with sessao.get(
            url, headers=hdrs, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT_GET),
            max_redirects=_MAX_REDIRECTS,
        ) as r:
            _res_marca("get", _t)
            pos = str(r.url)
            if pos != url:
                _set_raw(url, pos)
                if _eh_intermediario(pos):
                    return await desencurtar(pos, sessao, depth + 1)
                return pos

            _t = _res_agora()
            html = await r.text(errors="ignore")
            _res_marca("download", _t)
            if len(html) > _LIMITE_HTML:
                _set_raw(url, pos)
                return pos

            _t = _res_agora()
            soup = BeautifulSoup(html, "html.parser")
            _res_marca("parse", _t)

            ref = soup.find(
                "meta", attrs={"http-equiv": re.compile("refresh", re.I)}
            )
            if ref and ref.get("content"):
                m = re.search(
                    r"url[=\s]*([^\s;\"']+)", ref["content"], re.I
                )
                if m:
                    novo = m.group(1).strip().strip("'\"")
                    if novo.startswith("http"):
                        return await desencurtar(novo, sessao, depth + 1)

            for pat in [
                r'window\.location(?:\.href)?\s*=\s*["\']([^"\']{15,})["\']',
                r'location\.replace\s*\(\s*["\']([^"\']{15,})["\']\s*\)',
                r'location\.href\s*=\s*["\']([^"\']{15,})["\']',
            ]:
                mj = re.search(pat, html)
                if mj and mj.group(1).startswith("http"):
                    return await desencurtar(mj.group(1), sessao, depth + 1)

            og = soup.find("meta", attrs={"property": "og:url"})
            if (og and og.get("content", "").startswith("http")
                    and og["content"] != url):
                return await desencurtar(og["content"], sessao, depth + 1)

            canon = soup.find("link", rel="canonical")
            if (canon and canon.get("href", "").startswith("http")
                    and canon["href"] != url):
                return await desencurtar(canon["href"], sessao, depth + 1)

            _set_raw(url, pos)
            return pos

    except asyncio.TimeoutError:
        log_nrm.warning(f"⏱ Timeout desencurtar d={depth}: {url[:60]}")
        return url
    except Exception as e:
        log_nrm.error(f"❌ desencurtar d={depth}: {e}")
        return url
