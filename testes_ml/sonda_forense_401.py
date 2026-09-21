"""
SONDA FORENSE DO 401 — laudo completo.

Uma bateria controlada, sequencial, com pausa entre requisições.
Sete cenários, uma requisição cada. Não inunda o Mercado Livre.

Não corrige nada. Não altera produção. Só mede.

═══════════════════════════════════════════════════════════════════
POR QUE O CENÁRIO 7 EXISTE
═══════════════════════════════════════════════════════════════════
A regra é não acrescentar header por chute. Não é chute: o teste
histórico `teste_ml_createlink.py`, o único que comprovadamente
obteve HTTP 200 e gerou `meli.la`, lia a credencial de
`ML_TEST_CURL` — "cURL da requisição createLink, copiado do
DevTools" — e a sua `extrair_do_curl` devolve
`(header_cookie, headers_extra)`. Ou seja: aquele 200 saiu com os
`-H` do navegador, não com os oito cabeçalhos que a produção monta
à mão.

Isso é uma DIFERENÇA OBSERVADA entre a requisição que funcionou e a
que falha. O cenário 7 é o teste de causalidade dela, com uma
variável só.

═══════════════════════════════════════════════════════════════════
SEGREDO
═══════════════════════════════════════════════════════════════════
Nenhum valor de cookie, token ou header sai. Nome de cookie sai
apenas quando é token legítimo; fora disso é pedaço de VALOR e sai
redigido, com tamanho e caracteres proibidos.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
import zlib

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import aiohttp
from yarl import URL

from plataformas.mercadolivre import afiliado, cliente, sessao

_TOKEN_LEGAL = re.compile(r"^[A-Za-z0-9_!#$%&'*+.^`|~-]+$")
_PAUSA_S = 2.0

_URL_TESTE = os.environ.get(
    "ML_TEST_URL",
    "https://www.mercadolivre.com.br/monitor-aoc-238-100hz-1ms"
    "-gaming-hdmi-24b30hm2/p/MLB46469571",
)

_SESSAO_ML = ("ssid", "orguserid", "orgnick", "_csrf", "nsa_rotok",
              "x-meli-session-id", "x-bf-session-v6",
              "_mldataSessionId", "_d2id", "ftid")

_TERCEIROS = ("__rtbh", "_hj", "_pin_", "ttcsid", "ttclid", "QSI_",
              "_gads", "_gcl", "g_state", "_cq_", "_fbp", "_ga",
              "_tt_", "_uetsid", "_uetvid", "IDE", "NID")

MATRIZ = []


def secao(t):
    print(f"\n{'=' * 68}\n{t}\n{'=' * 68}", flush=True)


def linha(r, v):
    print(f"  {r:<36} {v}", flush=True)


def _nome_seguro(n):
    if _TOKEN_LEGAL.match(n):
        return n
    proib = sorted({c for c in n if not _TOKEN_LEGAL.match(c)})
    return (f"<fragmento | len={len(n)} | "
            f"proibidos={''.join(proib)[:10]!r}>")


def _pares(h):
    saida = []
    for parte in (h or "").split(";"):
        nome, sep, valor = parte.strip().partition("=")
        if sep and nome:
            saida.append((nome, valor))
    return saida


def _classe(nome):
    if nome in _SESSAO_ML:
        return "ML-SESSÃO"
    if any(nome.lower() == s.lower() for s in _SESSAO_ML):
        return "ML-SESSÃO"
    if nome.startswith("c_"):
        return "ML-experimento"
    if nome.startswith(("ml_", "nav_")):
        return "ML-interface"
    if any(nome.startswith(p) for p in _TERCEIROS):
        return "rastreador-3º"
    return "desconhecido"


def _corpo_urllib(r):
    bruto = r.read()
    cod = (r.headers.get("Content-Encoding") or "").lower()
    try:
        if "gzip" in cod:
            bruto = gzip.decompress(bruto)
        elif "deflate" in cod:
            bruto = zlib.decompress(bruto, -zlib.MAX_WBITS)
    except Exception:
        pass
    return bruto.decode("utf-8", errors="ignore")


def _resumo_corpo(texto):
    try:
        d = json.loads(texto)
    except Exception:
        return f"(não-JSON, {len(texto)} chars)"
    partes = []
    for k in ("message", "error", "total_success", "total_error"):
        if k in d:
            partes.append(f"{k}={d[k]}")
    urls = d.get("urls")
    if isinstance(urls, list) and urls and isinstance(urls[0], dict):
        partes.append(f"created={urls[0].get('created')}")
        partes.append(
            f"short_url={'SIM' if urls[0].get('short_url') else 'não'}")
    return " ".join(partes) or "(sem campos de controle)"


# ══════════════════════════════════════════════════════════════════
# BLOCO 1 — INVENTÁRIO DA CREDENCIAL
# ══════════════════════════════════════════════════════════════════
def bloco1(efetivo, bruto):
    secao("BLOCO 1 — INVENTÁRIO DA CREDENCIAL")
    pares = _pares(efetivo)
    legais = [(n, v) for n, v in pares if _TOKEN_LEGAL.match(n)]
    frags = [(n, v) for n, v in pares if not _TOKEN_LEGAL.match(n)]

    linha("tamanho bruto da variável", len(bruto))
    linha("tamanho após normalização", len(efetivo))
    linha("sequências %XX (valores codificados)",
          len(re.findall(r"%[0-9A-Fa-f]{2}", efetivo)))
    linha("ENTRAM no parser (split por ;)", len(pares))
    linha("SOBREVIVEM (nome legal)", len(legais))
    linha("DESCARTADOS (fragmento de valor)", len(frags))

    print("\n  ── cookies por função ──", flush=True)
    porclasse = {}
    for n, _ in legais:
        porclasse.setdefault(_classe(n), []).append(n)
    for classe in ("ML-SESSÃO", "ML-experimento", "ML-interface",
                   "rastreador-3º", "desconhecido"):
        nomes = sorted(porclasse.get(classe, []))
        linha(f"{classe}", f"{len(nomes)}  {nomes if nomes else ''}")

    print("\n  ── os 8 nomes da auditoria ──", flush=True)
    existentes = {n.lower() for n, _ in legais}
    for alvo in ("ssid", "orguserid", "_csrf", "x-meli-session-id",
                 "x-bf-session-v6", "_d2id", "_mldataSessionId",
                 "nsa_rotok"):
        linha(alvo, "PRESENTE" if alvo.lower() in existentes
              else "AUSENTE")

    if frags:
        print("\n  ── fragmentos: de qual cookie são a cauda ──",
              flush=True)
        for i, (n, _) in enumerate(pares):
            if not _TOKEN_LEGAL.match(n):
                ant = _nome_seguro(pares[i - 1][0]) if i else "(nenhum)"
                print(f"     {_nome_seguro(n)}\n"
                      f"        └─ cauda do valor de: {ant}", flush=True)
    return legais


# ══════════════════════════════════════════════════════════════════
# BLOCO 2 — CSRF
# ══════════════════════════════════════════════════════════════════
def bloco2(efetivo, csrf):
    secao("BLOCO 2 — CSRF: header x cookie")
    mapa = dict(_pares(efetivo))
    do_cookie = mapa.get("_csrf")

    def forma(v):
        if not v:
            return "(vazio)"
        if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}"
                        r"-[0-9a-f]{4}-[0-9a-f]{12}", v):
            return "uuid"
        if re.fullmatch(r"[0-9a-f]+", v):
            return "hex"
        if re.fullmatch(r"[A-Za-z0-9+/=_-]+", v):
            return "alfanumérico/base64"
        return "outro"

    linha("x-csrf-token: tamanho", len(csrf))
    linha("x-csrf-token: forma", forma(csrf))
    linha("cookie _csrf presente",
          "sim" if do_cookie is not None else "NÃO")
    if do_cookie is not None:
        linha("cookie _csrf: tamanho", len(do_cookie))
        linha("cookie _csrf: forma", forma(do_cookie))
        linha("MESMO VALOR", "SIM" if do_cookie == csrf else "NÃO")
        linha("MESMA FORMA",
              "sim" if forma(do_cookie) == forma(csrf) else "NÃO")


# ══════════════════════════════════════════════════════════════════
# BLOCO 3 — MATRIZ DE CENÁRIOS
# ══════════════════════════════════════════════════════════════════
_BASE = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

_NAVEGADOR = {
    "sec-ch-ua": ('"Chromium";v="148", "Google Chrome";v="148", '
                  '"Not?A_Brand";v="24"'),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
    "x-requested-with": "XMLHttpRequest",
    "priority": "u=1, i",
}


def _cabecalhos(cookie, csrf, extra=None):
    h = dict(_BASE)
    h["User-Agent"] = cliente._USER_AGENT
    h["Origin"] = cliente._ORIGIN
    h["Referer"] = cliente._REFERER
    if cookie is not None:
        h["Cookie"] = cookie
    if csrf is not None:
        h["x-csrf-token"] = csrf
    if extra:
        h.update(extra)
    return h


async def _post_aiohttp(nome, cab, corpo, jar_semeado=None):
    jar = aiohttp.CookieJar()
    if jar_semeado:
        for n, v in jar_semeado:
            try:
                jar.update_cookies({n: v},
                                   response_url=URL(cliente._ORIGIN))
            except Exception:
                pass
    s = aiohttp.ClientSession(cookie_jar=jar)
    t0 = time.monotonic()
    try:
        async with s.post(cliente.ENDPOINT, data=corpo, headers=cab,
                          allow_redirects=False,
                          timeout=aiohttp.ClientTimeout(total=25)) as r:
            texto = await r.text(errors="ignore")
            devolvidos = [c.split("=", 1)[0].strip()
                          for c in r.headers.getall("Set-Cookie", [])]
            MATRIZ.append((nome, r.status, _resumo_corpo(texto),
                           round(time.monotonic() - t0, 2)))
            return devolvidos
    except Exception as e:
        MATRIZ.append((nome, "EXC", type(e).__name__,
                       round(time.monotonic() - t0, 2)))
        return []
    finally:
        await s.close()


def _post_urllib(nome, cab, corpo):
    t0 = time.monotonic()
    req = urllib.request.Request(cliente.ENDPOINT, data=corpo,
                                 headers=dict(cab), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            MATRIZ.append((nome, r.status, _resumo_corpo(_corpo_urllib(r)),
                           round(time.monotonic() - t0, 2)))
    except urllib.error.HTTPError as e:
        MATRIZ.append((nome, e.code, _resumo_corpo(_corpo_urllib(e)),
                       round(time.monotonic() - t0, 2)))
    except Exception as e:
        MATRIZ.append((nome, "EXC", type(e).__name__,
                       round(time.monotonic() - t0, 2)))


async def bloco3(efetivo, csrf, legais):
    secao("BLOCO 3 — MATRIZ DE CENÁRIOS (1 requisição cada)")
    corpo = json.dumps(
        {"urls": [_URL_TESTE], "tag": afiliado.TAG}).encode("utf-8")

    so_ml = "; ".join(f"{n}={v}" for n, v in legais
                      if _classe(n) in ("ML-SESSÃO", "ML-experimento",
                                        "ML-interface"))

    devolvidos = await _post_aiohttp(
        "1 producao (aiohttp + jar)", _cabecalhos(efetivo, csrf),
        corpo, jar_semeado=legais)
    await asyncio.sleep(_PAUSA_S)

    await _post_aiohttp("2 aiohttp sem jar",
                        _cabecalhos(efetivo, csrf), corpo)
    await asyncio.sleep(_PAUSA_S)

    _post_urllib("3 urllib", _cabecalhos(efetivo, csrf), corpo)
    await asyncio.sleep(_PAUSA_S)

    await _post_aiohttp("4 sem Cookie (so CSRF)",
                        _cabecalhos(None, csrf), corpo)
    await asyncio.sleep(_PAUSA_S)

    await _post_aiohttp("5 sem CSRF (so Cookie)",
                        _cabecalhos(efetivo, None), corpo)
    await asyncio.sleep(_PAUSA_S)

    await _post_aiohttp("6 so cookies do ML",
                        _cabecalhos(so_ml, csrf), corpo)
    await asyncio.sleep(_PAUSA_S)

    await _post_aiohttp("7 + headers de navegador",
                        _cabecalhos(efetivo, csrf, _NAVEGADOR), corpo)

    print(f"  {'CENÁRIO':<30}{'HTTP':<8}{'s':<7}RESULTADO", flush=True)
    print("  " + "-" * 64, flush=True)
    for nome, status, res, dur in MATRIZ:
        print(f"  {nome:<30}{str(status):<8}{dur:<7}{res}", flush=True)

    secao("BLOCO 4 — Set-Cookie DEVOLVIDO PELO MERCADO LIVRE")
    linha("quantidade", len(devolvidos))
    for n in devolvidos:
        linha("  nome", _nome_seguro(n))
    if not devolvidos:
        linha("LEITURA", "o servidor não tenta nos dar sessão nova")


# ══════════════════════════════════════════════════════════════════
async def principal():
    print("\n" + "#" * 68, flush=True)
    print("#  SONDA FORENSE DO 401 — laudo completo", flush=True)
    print("#" * 68, flush=True)

    bruto = os.environ.get("ML_SESSION_COOKIE") or ""
    csrf = sessao._limpar(os.environ.get("ML_CSRF_TOKEN") or "")
    efetivo = sessao._limpar(sessao._decodificar(bruto))

    legais = bloco1(efetivo, bruto)
    bloco2(efetivo, csrf)
    await bloco3(efetivo, csrf, legais)

    secao("LAUDO")
    status = {n: s for n, s, _, _ in MATRIZ}
    todos401 = all(s == 401 for s in status.values())
    algum200 = [n for n, s in status.items() if s == 200]

    print("CAUSA PROVADA:", flush=True)
    if algum200:
        for n in algum200:
            print(f"  - cenário '{n}' devolveu 200 → a diferença "
                  f"desse cenário É a causa", flush=True)
    elif todos401:
        print("  - nenhuma ainda. Os 7 cenários deram 401, inclusive "
              "sem Cookie e sem CSRF.", flush=True)
        print("    Quando tirar a credencial NÃO muda a resposta, a "
              "credencial não é o que está sendo avaliado:", flush=True)
        print("    a recusa acontece antes, por contexto.", flush=True)
    else:
        print("  - resultados divergentes; ver matriz.", flush=True)

    print("\nHIPÓTESE REFUTADA:", flush=True)
    if status.get("4 sem Cookie (so CSRF)") == 401 and todos401:
        print("  - 'é o conteúdo do cookie' — sem cookie nenhum o "
              "status é o mesmo", flush=True)
    if status.get("2 aiohttp sem jar") == status.get("3 urllib"):
        print("  - 'é o transporte' — urllib e aiohttp empatam",
              flush=True)
    if status.get("1 producao (aiohttp + jar)") == status.get(
            "2 aiohttp sem jar"):
        print("  - 'é o CookieJar' — com e sem jar dá o mesmo",
              flush=True)

    print("\nDADO AUSENTE:", flush=True)
    print("  - os headers da requisição do navegador que funciona",
          flush=True)
    print("  - qual credencial produziu o HTTP 200 histórico "
          "(vinha de ML_TEST_CURL, hoje inexistente)", flush=True)

    print("\n=== FIM ===", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(principal()))
