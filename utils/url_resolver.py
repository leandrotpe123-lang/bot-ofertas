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

ESTADO TRANSICIONAL:
  O conjunto efetivo de hosts que exigem GET na resolução é composto
  lazy a partir de três fontes: encurtadores genéricos declarados
  localmente (_FORCA_GET_GENERICOS), contribuições das plataformas
  via registry (encurtadores_forca_get de cada Plataforma), e o
  legado importado de `pipeline.classificacao` mantido como rede de
  segurança durante a transição.

  A remoção do import legado é entrega futura, condicionada a
  validação empírica de que a composição via registry está
  funcionando corretamente em runtime. O cache da composição é
  invalidável via _resetar_forca_get, ponto único de gerenciamento
  preparado para evolução arquitetural futura sem refatoração
  estrutural deste módulo.
"""
from __future__ import annotations

import asyncio
import random
import re

import aiohttp
from bs4 import BeautifulSoup

from config import USER_AGENTS
from globals import _get_raw, _set_raw
from logger import log_nrm
from pipeline.classificacao import _FORCA_GET as _FORCA_GET_LEGADO
from utils.urls import _netloc, _sanitizar_url


# ── Parâmetros ────────────────────────────────────────────────────
_PROFUNDIDADE_MAX = 15
_TIMEOUT_HEAD     = 10
_TIMEOUT_GET      = 20
_MAX_REDIRECTS    = 20
_LIMITE_HTML      = 500_000


# ── Composição de hosts que exigem GET ────────────────────────────
# Conjunto local de encurtadores genéricos do core, não atribuídos
# a nenhuma plataforma específica do projeto. Hosts cujos servidores
# não respondem corretamente a HEAD e exigem GET direto na resolução
# de redirecionamento.
_FORCA_GET_GENERICOS = frozenset({
    "bit.ly", "cutt.ly", "tinyurl.com", "rb.gy",
    "is.gd", "ow.ly", "buff.ly", "tidd.ly",
})

# Cache do conjunto efetivo, populado na primeira chamada de
# _hosts_forca_get e mantido pela vida do processo. Invalidável
# manualmente via _resetar_forca_get.
_FORCA_GET_COMPOSTO: frozenset[str] | None = None


def _compor_forca_get() -> frozenset[str]:
    """
    Compõe o conjunto efetivo de hosts que exigem GET, unindo três
    fontes ordenadas por confiabilidade: encurtadores genéricos do
    core, contribuições das plataformas via registry, e o legado
    transitório de pipeline.classificacao.

    Cada fonte é processada defensivamente — falhas em uma fonte
    não bloqueiam a composição com as demais. No pior caso resta
    o conjunto local de genéricos. NÃO realiza cache; o cache é
    feito por _hosts_forca_get.
    """
    composto: set[str] = set(_FORCA_GET_GENERICOS)

    try:
        from plataformas import registry  # lazy: evita ciclo de import
        identificadores = registry.plataformas_registradas()
        if not identificadores:
            log_nrm.warning(
                "⚠ _compor_forca_get: registry vazio no momento da "
                "composição — contribuição de plataformas ausente"
            )
        else:
            for ident in identificadores:
                try:
                    plataforma = registry.acessar(ident)
                    if plataforma is None:
                        continue
                    contrib = plataforma.encurtadores_forca_get
                    if contrib is not None:
                        composto |= contrib
                except Exception as e:
                    log_nrm.warning(
                        f"⚠ _compor_forca_get: falha lendo plataforma "
                        f"{ident!r}: {e}"
                    )
    except Exception as e:
        log_nrm.warning(
            f"⚠ _compor_forca_get: falha iterando registry: {e}"
        )

    try:
        composto |= _FORCA_GET_LEGADO
    except Exception as e:
        log_nrm.warning(
            f"⚠ _compor_forca_get: falha lendo legado: {e}"
        )

    return frozenset(composto)

def _logar_decomposicao_inicial() -> None:
    """
    Emite logs observacionais decompondo a origem do conjunto
    efetivo de hosts que exigem GET, por fonte. Invocada uma única
    vez por processo, imediatamente após a primeira composição.

    Função estritamente observacional: não altera composição, não
    altera cache, não propaga exceção. Toda falha individual de
    leitura é registrada como warning e isolada, preservando a
    natureza não-intrusiva da instrumentação.

    Existência temporária: a linha de log da fonte legada perde
    propósito na fase 6 e é removida junto com o próprio legado.
    As demais linhas podem permanecer como observabilidade
    arquitetural legítima, a decidir no momento da fase 6.
    """
    log_nrm.info(
        f"⚙ _hosts_forca_get: fonte=genericos_core "
        f"hosts={sorted(_FORCA_GET_GENERICOS)}"
    )

    try:
        from plataformas import registry  # lazy: simetria com _compor_forca_get
        identificadores = registry.plataformas_registradas()
        for ident in identificadores:
            try:
                plataforma = registry.acessar(ident)
                if plataforma is None:
                    log_nrm.info(
                        f"⚙ _hosts_forca_get: fonte=plataforma "
                        f"id={ident!r} contribuicao=indisponivel"
                    )
                    continue
                contrib = plataforma.encurtadores_forca_get
                if contrib is None:
                    log_nrm.info(
                        f"⚙ _hosts_forca_get: fonte=plataforma "
                        f"id={ident!r} contribuicao=nao_declarada"
                    )
                else:
                    log_nrm.info(
                        f"⚙ _hosts_forca_get: fonte=plataforma "
                        f"id={ident!r} hosts={sorted(contrib)}"
                    )
            except Exception as e:
                log_nrm.warning(
                    f"⚠ _hosts_forca_get: falha lendo plataforma "
                    f"{ident!r} para decomposição: {e}"
                )
    except Exception as e:
        log_nrm.warning(
            f"⚠ _hosts_forca_get: falha iterando registry "
            f"para decomposição: {e}"
        )

    try:
        log_nrm.info(
            f"⚙ _hosts_forca_get: fonte=legado_transitorio "
            f"hosts={sorted(_FORCA_GET_LEGADO)}"
        )
    except Exception as e:
        log_nrm.warning(
            f"⚠ _hosts_forca_get: falha lendo legado "
            f"para decomposição: {e}"
      )


def _hosts_forca_get() -> frozenset[str]:
    """
    Devolve o conjunto efetivo de hosts que exigem GET, compondo-o
    lazy na primeira chamada e cacheando o resultado em variável de
    módulo. Chamadas subsequentes devolvem o cache.

    Na primeira composição, emite logs observacionais decompondo a
    origem do conjunto efetivo por fonte (genéricos do core,
    contribuições por plataforma, legado transitório). Esses logs
    são estritamente observacionais — não alteram a composição nem
    o cache — e existem para autorizar empiricamente a remoção
    futura do legado. A linha referente ao legado desaparece na
    fase 6, junto com o próprio legado.
    """
    global _FORCA_GET_COMPOSTO
    if _FORCA_GET_COMPOSTO is None:
        _FORCA_GET_COMPOSTO = _compor_forca_get()
        log_nrm.info(
            f"⚙ _hosts_forca_get: composição inicial — "
            f"{len(_FORCA_GET_COMPOSTO)} hosts"
        )
        _logar_decomposicao_inicial()
    return _FORCA_GET_COMPOSTO


def _resetar_forca_get() -> None:
    """
    Invalida o cache da composição. Próxima chamada de
    _hosts_forca_get refaz a composição.

    PROPÓSITO ARQUITETURAL: ponto único, identificado e declarado
    de invalidação do cache. NÃO é invocada pelo código atual e
    não tem caso de uso operacional ativo. Existe para que a
    evolução arquitetural futura — registro tardio de plataformas,
    recarga de configuração, ciclos de teste isolados — possa ser
    feita sem refatoração estrutural deste módulo. A sua presença
    é afirmação de que o cache é recurso gerenciável, não efeito
    colateral opaco.
    """
    global _FORCA_GET_COMPOSTO
    _FORCA_GET_COMPOSTO = None


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

    cached = _get_raw(url)
    if cached:
        return cached

    hdrs = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }

    try:
        hosts = _hosts_forca_get()
        usar_head = (
            nl not in hosts
            and not any(nl.endswith("." + d) for d in hosts)
        )
        if usar_head:
            try:
                async with sessao.head(
                    url, headers=hdrs, allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=_TIMEOUT_HEAD),
                    max_redirects=_MAX_REDIRECTS,
                ) as r:
                    final = str(r.url)
                    if final != url:
                        _set_raw(url, final)
                        return await desencurtar(final, sessao, depth + 1)
            except Exception:
                pass

        async with sessao.get(
            url, headers=hdrs, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT_GET),
            max_redirects=_MAX_REDIRECTS,
        ) as r:
            pos = str(r.url)
            if pos != url:
                _set_raw(url, pos)
                return await desencurtar(pos, sessao, depth + 1)

            html = await r.text(errors="ignore")
            if len(html) > _LIMITE_HTML:
                _set_raw(url, pos)
                return pos

            soup = BeautifulSoup(html, "html.parser")

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
