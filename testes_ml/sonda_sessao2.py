"""
SONDA 2 — por que o servidor recusa a credencial.

A rodada 1 e 2 já fecharam a leitura: 28 pares, 24 nomes legais, 4
fragmentos ilegais, e o POST sai e leva 401 `{"message":
"Unauthorized"}` mesmo com jar vazio e header completo.

Falta separar três coisas que produzem o MESMO 401:

  (a) a sessão está morta / não é sessão de afiliado;
  (b) o `x-csrf-token` não casa com o cookie `_csrf`;
  (c) valores de cookie chegaram DESCODIFICADOS, e qualquer valor
      que continha `%3B` virou `;` — partindo o cookie ao meio para
      qualquer parser, inclusive o do Mercado Livre.

Reusa os utilitários da sonda 1 em vez de duplicá-los.

SEGREDO: continua sem imprimir valor nenhum. A comparação entre o
CSRF e o cookie `_csrf` sai como BOOLEANO — igualdade não revela o
conteúdo de nenhum dos dois.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys

AQUI = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(AQUI)
sys.path.insert(0, RAIZ)
sys.path.insert(0, AQUI)

import aiohttp

from plataformas.mercadolivre import afiliado, cliente, sessao
from sonda_sessao import (
    _TOKEN_LEGAL, _URL_TESTE, _sanitizar, linha, secao,
)

# Cookies que os fragmentos quebrados seguem — apurado na rodada 2.
_SUSPEITOS = ("g_state", "_cq_duid", "ssid")


def _pares(header: str) -> list:
    saida = []
    for parte in header.split(";"):
        nome, sep, valor = parte.strip().partition("=")
        if sep and nome:
            saida.append((nome, valor))
    return saida


# ══════════════════════════════════════════════════════════════════
# A — CODIFICAÇÃO: os valores vieram crus ou descodificados?
# ══════════════════════════════════════════════════════════════════
def etapa_codificacao(efetivo: str) -> None:
    secao("A) CODIFICAÇÃO DOS VALORES")
    percentuais = efetivo.count("%")
    sequencias = len(re.findall(r"%[0-9A-Fa-f]{2}", efetivo))
    linha("tamanho do header", len(efetivo))
    linha("ocorrências de '%'", percentuais)
    linha("sequências %XX válidas", sequencias)
    linha("espaços no header", efetivo.count(" "))
    linha("barras '/' no header", efetivo.count("/"))

    # Num header `Cookie` REAL do navegador, valor com caractere
    # especial vem percent-encoded. Zero %XX num header de 28
    # cookies, com espaço e barra soltos, é sinal de que o valor foi
    # descodificado em algum ponto da cópia — e aí todo `%3B` virou
    # `;` e partiu o cookie.
    if sequencias == 0:
        linha("LEITURA", "valores DESCODIFICADOS — %3B virou ';'")
    else:
        linha("LEITURA", f"há {sequencias} valores percent-encoded")


# ══════════════════════════════════════════════════════════════════
# B — O CSRF CASA COM O COOKIE `_csrf`?
# ══════════════════════════════════════════════════════════════════
def etapa_csrf(efetivo: str, csrf: str) -> None:
    secao("B) x-csrf-token x cookie _csrf")
    mapa = dict(_pares(efetivo))
    do_cookie = mapa.get("_csrf")
    linha("cookie _csrf presente", "sim" if do_cookie is not None else "NÃO")
    if do_cookie is None:
        return
    linha("tamanho do cookie _csrf", len(do_cookie))
    linha("tamanho do x-csrf-token", len(csrf))
    # Igualdade é booleano: não revela nenhum dos dois valores.
    linha("SÃO IGUAIS", "SIM" if do_cookie == csrf else "NÃO")
    if do_cookie != csrf:
        linha("LEITURA",
              "token e cookie de sessões/momentos diferentes — "
              "o servidor recusa mesmo com sessão viva")


# ══════════════════════════════════════════════════════════════════
# C — TAMANHO DOS VALORES SUSPEITOS
# ══════════════════════════════════════════════════════════════════
def etapa_suspeitos(efetivo: str) -> None:
    secao("C) VALORES DOS COOKIES QUE ANTECEDEM O LIXO")
    mapa = dict(_pares(efetivo))
    for nome in _SUSPEITOS:
        if nome in mapa:
            linha(f"{nome}: tamanho do valor", len(mapa[nome]))
        else:
            linha(f"{nome}", "(ausente)")
    ttcsid = [n for n in mapa if n.startswith("ttcsid")]
    for n in ttcsid:
        linha(f"{n}: tamanho do valor", len(mapa[n]))


# ══════════════════════════════════════════════════════════════════
# D — A SESSÃO ESTÁ VIVA? (GET, sem CSRF)
# ══════════════════════════════════════════════════════════════════
async def etapa_sessao_viva(sessao_http, efetivo: str) -> None:
    secao("D) A SESSÃO ESTÁ VIVA? — GET do painel de afiliados")
    # GET não exige CSRF. Se a sessão estiver viva, o painel
    # responde 200; se estiver morta, redireciona para login. Isso
    # separa "sessão morta" de "CSRF não casa" — os dois dão 401 no
    # POST e são indistinguíveis por lá.
    alvo = "https://www.mercadolivre.com.br/affiliate-program/hub"
    cabecalhos = {
        "User-Agent": cliente._USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Cookie": efetivo,
    }
    try:
        async with sessao_http.get(
            alvo, headers=cabecalhos, allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=25),
        ) as r:
            linha("status", r.status)
            destino = r.headers.get("location", "")
            # Só o HOST do destino, nunca a query — ela costuma
            # carregar identificador de sessão.
            if destino:
                host = re.sub(r"^https?://([^/]+).*$", r"\1", destino)
                linha("redireciona para (host)", host)
                linha("parece tela de login",
                      "SIM" if re.search(r"login|signin|auth", destino,
                                         re.I) else "não")
            corpo = await r.text(errors="ignore")
            linha("tamanho do corpo", len(corpo))
            for marca in ("affiliate", "Sair", "logout", "login",
                          "Entrar", "captcha"):
                if marca.lower() in corpo[:20000].lower():
                    linha(f"contém '{marca}'", "sim")
            if r.status == 200:
                linha("LEITURA", "sessão RESPONDE — não está morta")
            elif r.status in (301, 302, 303, 307, 308):
                linha("LEITURA", "sessão redirecionada — provável morta")
    except Exception as exc:
        linha("exceção", f"{type(exc).__name__}: {_sanitizar(exc)}")


# ══════════════════════════════════════════════════════════════════
# E — POST só com os 24 cookies legais
# ══════════════════════════════════════════════════════════════════
async def etapa_post_limpo(sessao_http, efetivo: str, csrf: str) -> None:
    secao("E) POST com APENAS os cookies de nome legal")
    legais = [(n, v) for n, v in _pares(efetivo) if _TOKEN_LEGAL.match(n)]
    header = "; ".join(f"{n}={v}" for n, v in legais)
    linha("cookies no header", len(legais))
    linha("descartados", len(_pares(efetivo)) - len(legais))

    corpo = json.dumps({"urls": [_URL_TESTE], "tag": afiliado.TAG})
    cabecalhos = {
        "User-Agent": cliente._USER_AGENT,
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": cliente._ORIGIN,
        "Referer": cliente._REFERER,
        "Cookie": header,
        "x-csrf-token": csrf,
    }
    try:
        async with sessao_http.post(
            cliente.ENDPOINT, data=corpo.encode("utf-8"),
            headers=cabecalhos, allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=25),
        ) as r:
            linha("status", r.status)
            texto = await r.text(errors="ignore")
            try:
                dados = json.loads(texto)
                linha("message", dados.get("message", "(ausente)"))
                linha("total_success", dados.get("total_success"))
            except Exception:
                linha("corpo", f"não-JSON, {len(texto)} chars")
            if r.status == 200:
                linha("LEITURA",
                      "descartar o lixo RESOLVE — a correção é no parser")
            else:
                linha("LEITURA",
                      "descartar o lixo NÃO resolve — a credencial "
                      "em si é recusada")
    except Exception as exc:
        linha("exceção", f"{type(exc).__name__}: {_sanitizar(exc)}")


async def principal() -> int:
    print("\n" + "#" * 66, flush=True)
    print("#  SONDA 2 — POR QUE O SERVIDOR RECUSA", flush=True)
    print("#" * 66, flush=True)

    bruto = os.environ.get("ML_SESSION_COOKIE") or ""
    csrf = sessao._limpar(os.environ.get("ML_CSRF_TOKEN") or "")
    efetivo = sessao._limpar(sessao._decodificar(bruto))

    etapa_codificacao(efetivo)
    etapa_csrf(efetivo, csrf)
    etapa_suspeitos(efetivo)

    sessao_http = aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar())
    try:
        await etapa_sessao_viva(sessao_http, efetivo)
        await etapa_post_limpo(sessao_http, efetivo, csrf)
    finally:
        await sessao_http.close()

    print("\n=== FIM SONDA 2 ===", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(principal()))
