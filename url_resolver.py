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

PENDÊNCIA REGISTRADA:
  O conjunto `_FORCA_GET` é importado de `pipeline.classificacao`,
  criando uma dependência utils → pipeline. O destino arquitetural
  correto desse conjunto é este módulo. A migração está registrada
  para a fase de revisão de `pipeline/classificacao.py`.
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
from pipeline.classificacao import _FORCA_GET
from utils.urls import _netloc, _sanitizar_url


# ── Parâmetros ────────────────────────────────────────────────────
_PROFUNDIDADE_MAX = 15
_TIMEOUT_HEAD     = 10
_TIMEOUT_GET      = 20
_MAX_REDIRECTS    = 20
_LIMITE_HTML      = 500_000


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
        usar_head = (
            nl not in _FORCA_GET
            and not any(nl.endswith("." + d) for d in _FORCA_GET)
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
