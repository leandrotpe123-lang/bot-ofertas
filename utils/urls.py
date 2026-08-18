"""Utilitários de URL: normalização, cache key, sanitização, host canônico."""
from __future__ import annotations
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


def _cache_key(url: str) -> str:
    """Chave canônica de URL — remove UTMs e parâmetros de rastreamento."""
    try:
        p = urlparse(url.strip())
        host = (p.hostname or "").lower().strip(".")
        params = parse_qs(p.query)
        remover = {
            "ascsubtag", "smid", "utm_source", "utm_medium", "utm_campaign",
            "utm_term", "utm_content", "aff_id", "affiliate_id",
            "fbclid", "gclid", "camp", "creative", "linkcode", "linkid",
        }
        params_limpos = {k: v for k, v in params.items() if k.lower() not in remover}
        pares = [(k, val) for k, vals in params_limpos.items() for val in vals]
        query = urlencode(sorted(pares))
        return urlunparse((p.scheme.lower(), host, p.path.rstrip("/"), "", query, ""))
    except Exception:
        return url.strip().lower()


def _sanitizar_url(url: str) -> str:
    """Remove caracteres de lixo no final da URL."""
    return url.strip().rstrip('.,;)>!?\n\r ')


def _netloc(url: str) -> str:
    """
    Extrai o netloc limpo (sem www., sem porta) de uma URL.

    É a função canônica de extração de host do projeto. Toda
    extração de host deve passar por aqui, para garantir tratamento
    uniforme de prefixo www. e de capitalização.
    """
    try:
        p = urlparse(url)
        nl = (p.hostname or "").lower()
        if nl.startswith("www."):
            nl = nl[4:]
        return nl.strip(".")
    except Exception:
        return ""


def host_canonico_campanha(urls) -> str:
    """
    Deriva a chave canônica de campanha a partir de uma coleção de
    URLs: host (via _netloc) mais caminho, sem query string.

    A coleção recebida deve conter APENAS URLs de campanha. O
    filtro de quais URLs são de campanha é responsabilidade do
    chamador — esta função não o aplica.

    Função neutra de derivação de URL. Não conhece o pipeline nem a
    deduplicação. Deve operar sobre URLs afiliadas LONGAS, pois é a
    forma que carrega o host e o caminho semânticos da campanha.

    CRITÉRIO DE DESEMPATE:
      Quando há mais de uma URL de campanha, a função devolve a
      chave MENOR em ordem alfabética. Trata-se de um critério
      determinístico ARBITRÁRIO de desempate, cuja única finalidade
      é garantir que a mesma campanha, vinda de grupos que ordenam
      os links de formas distintas, produza sempre a mesma chave.
      Não é a eleição de um representante semântico da campanha.

    OBSERVAÇÃO DE EVOLUÇÃO:
      A identidade de campanha baseia-se em host + caminho. Caso uma
      plataforma futura distinga campanhas por parâmetro de query
      sobre o mesmo caminho, este critério deverá ser revisto.

    Devolve a chave canônica, ou cadeia vazia quando a coleção está
    vazia ou nenhuma URL produz uma chave.
    """
    chaves = []
    for url in urls:
        try:
            host = _netloc(url)
            if not host:
                continue
            p = urlparse(url)
            path = (p.path or "").rstrip("/")
            chaves.append(f"{host}{path}" if path else host)
        except Exception:
            continue
    return min(chaves) if chaves else ""

def chaves_canonicas_campanha(urls) -> list[str]:
    """
    Plural de host_canonico_campanha: devolve TODAS as chaves
    canônicas de campanha (host+caminho, sem query), ordenadas e sem
    repetição. NÃO colapsa por min() — cada campanha distinta no post
    é uma chave distinta. O filtro de quais URLs são de campanha é
    responsabilidade do chamador.
    """
    chaves: list[str] = []
    for url in urls:
        try:
            host = _netloc(url)
            if not host:
                continue
            p = urlparse(url)
            path = (p.path or "").rstrip("/")
            chave = f"{host}{path}" if path else host
            if chave not in chaves:
                chaves.append(chave)
        except Exception:
            continue
    return sorted(chaves)
