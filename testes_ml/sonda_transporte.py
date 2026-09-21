"""
SONDA DE TRANSPORTE — urllib x aiohttp, tudo o mais igual.

O teste histórico `teste_ml_createlink.py`, que já obteve HTTP 200 e
gerou `meli.la`, usa `urllib.request`. A produção usa `aiohttp`.
Isso não prova nada sozinho — e é exatamente por isso que existe
este experimento: separar HIPÓTESE DE TRANSPORTE de HIPÓTESE DE
CREDENCIAL, que hoje são indistinguíveis porque as duas dão 401.

Os dois transportes recebem o MESMO dicionário de cabeçalhos, o
MESMO corpo, a MESMA URL, a MESMA tag e a MESMA credencial — montados
UMA vez e compartilhados, para que nenhuma diferença possa entrar por
descuido de digitação.

═══════════════════════════════════════════════════════════════════
LEITURA DO RESULTADO — decidida ANTES de medir
═══════════════════════════════════════════════════════════════════
  urllib 200 / aiohttp 401 → é transporte
  urllib 401 / aiohttp 401 → transporte não explica; é credencial
  ambos 200                → não reproduz; investigar o fluxo
  falhas diferentes        → registrar a diferença, não concluir

═══════════════════════════════════════════════════════════════════
SEGREDO
═══════════════════════════════════════════════════════════════════
Nenhum valor de cookie, token ou header é impresso. Os NOMES dos
cookies saem na Fase E porque é o que a auditoria pede, e só quando
o nome é um token legítimo — nome fora do padrão é pedaço de VALOR
e sai redigido.
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

from plataformas.mercadolivre import afiliado, cliente, sessao

_TOKEN_LEGAL = re.compile(r"^[A-Za-z0-9_!#$%&'*+.^`|~-]+$")

_URL_TESTE = os.environ.get(
    "ML_TEST_URL",
    "https://www.mercadolivre.com.br/monitor-aoc-238-100hz-1ms"
    "-gaming-hdmi-24b30hm2/p/MLB46469571",
)

# Nomes que a auditoria mandou conferir. Presença/ausência apenas —
# aparecer numa captura histórica não torna um cookie obrigatório.
_CONFERIR = (
    "ssid", "orguserid", "_csrf", "x-meli-session-id",
    "x-bf-session-v6", "_d2id", "_mldataSessionId", "nsa_rotok",
)


def secao(t):
    print(f"\n{'=' * 66}\n{t}\n{'=' * 66}", flush=True)


def linha(r, v):
    print(f"  {r:<34} {v}", flush=True)


def _nome_seguro(nome: str) -> str:
    if _TOKEN_LEGAL.match(nome):
        return nome
    return f"<fragmento redigido | len={len(nome)}>"


def _pares(header: str) -> list:
    saida = []
    for parte in header.split(";"):
        nome, sep, valor = parte.strip().partition("=")
        if sep and nome:
            saida.append((nome, valor))
    return saida


def _relatar(dados_texto: str, status: int, transporte: str,
             duracao: float, erro=None) -> dict:
    """Só os campos que a auditoria autorizou."""
    r = {"transporte": transporte, "status": status,
         "duracao": round(duracao, 2), "message": None,
         "total_success": None, "total_error": None,
         "created": None, "short_url": None, "erro": erro}
    if erro:
        return r
    try:
        d = json.loads(dados_texto)
    except Exception:
        r["message"] = f"(não-JSON, {len(dados_texto)} chars)"
        return r
    r["message"] = d.get("message")
    r["total_success"] = d.get("total_success")
    r["total_error"] = d.get("total_error")
    urls = d.get("urls")
    if isinstance(urls, list) and urls and isinstance(urls[0], dict):
        r["created"] = urls[0].get("created")
        r["short_url"] = "presente" if urls[0].get("short_url") else "ausente"
    return r


def _corpo_decodificado(resposta) -> str:
    """Lê o corpo tratando gzip/deflate — igual ao teste histórico."""
    bruto = resposta.read()
    cod = (resposta.headers.get("Content-Encoding") or "").lower()
    try:
        if "gzip" in cod:
            bruto = gzip.decompress(bruto)
        elif "deflate" in cod:
            bruto = zlib.decompress(bruto, -zlib.MAX_WBITS)
    except Exception:
        pass
    return bruto.decode("utf-8", errors="ignore")


# ══════════════════════════════════════════════════════════════════
# TESTE 1 — urllib, o transporte do teste histórico
# ══════════════════════════════════════════════════════════════════
def teste_urllib(cabecalhos: dict, corpo: bytes) -> dict:
    inicio = time.monotonic()
    req = urllib.request.Request(
        cliente.ENDPOINT, data=corpo, headers=dict(cabecalhos),
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return _relatar(_corpo_decodificado(r), r.status, "urllib",
                            time.monotonic() - inicio)
    except urllib.error.HTTPError as e:
        return _relatar(_corpo_decodificado(e), e.code, "urllib",
                        time.monotonic() - inicio)
    except Exception as e:
        return _relatar("", "NA", "urllib", time.monotonic() - inicio,
                        erro=type(e).__name__)


# ══════════════════════════════════════════════════════════════════
# TESTE 2 — aiohttp, equivalente ao cliente.py de produção
# ══════════════════════════════════════════════════════════════════
async def teste_aiohttp(cabecalhos: dict, corpo: bytes) -> dict:
    inicio = time.monotonic()
    sessao_http = aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar())
    try:
        async with sessao_http.post(
            cliente.ENDPOINT, data=corpo, headers=dict(cabecalhos),
            timeout=aiohttp.ClientTimeout(total=25),
            allow_redirects=False,
        ) as r:
            texto = await r.text(errors="ignore")
            return _relatar(texto, r.status, "aiohttp",
                            time.monotonic() - inicio)
    except Exception as e:
        return _relatar("", "NA", "aiohttp", time.monotonic() - inicio,
                        erro=type(e).__name__)
    finally:
        await sessao_http.close()


# ══════════════════════════════════════════════════════════════════
# FASE E — inventário de nomes de cookie
# ══════════════════════════════════════════════════════════════════
def fase_e(efetivo: str) -> None:
    secao("FASE E — COOKIES IMPORTANTES (só presença)")
    mapa = {n: v for n, v in _pares(efetivo)}
    existentes = set(mapa)
    for nome in _CONFERIR:
        presente = nome in existentes
        # Alguns cookies aparecem com caixa diferente conforme a
        # captura; conferir sem caixa evita falso negativo.
        if not presente:
            presente = any(k.lower() == nome.lower() for k in existentes)
        linha(f"{nome}", "PRESENTE" if presente else "ausente")

    secao("FASE E — TODOS OS NOMES ENCONTRADOS")
    legais = [n for n in mapa if _TOKEN_LEGAL.match(n)]
    fragmentos = [n for n in mapa if not _TOKEN_LEGAL.match(n)]
    linha("total de entradas", len(mapa))
    linha("nomes legais", len(legais))
    linha("fragmentos (pedaço de valor)", len(fragmentos))
    print(flush=True)
    for i, nome in enumerate(sorted(legais)):
        print(f"    {i:>3}  {nome}", flush=True)
    for nome in fragmentos:
        print(f"         {_nome_seguro(nome)}", flush=True)


async def principal() -> int:
    print("\n" + "#" * 66, flush=True)
    print("#  SONDA DE TRANSPORTE — urllib x aiohttp", flush=True)
    print("#" * 66, flush=True)

    efetivo = sessao._limpar(
        sessao._decodificar(os.environ.get("ML_SESSION_COOKIE") or ""))
    csrf = sessao._limpar(os.environ.get("ML_CSRF_TOKEN") or "")

    # UM dicionário, compartilhado pelos dois transportes. Se cada
    # teste montasse o seu, uma diferença podia entrar por descuido
    # e eu atribuiria ao transporte o que era erro meu.
    cabecalhos = {
        "User-Agent": cliente._USER_AGENT,
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Origin": cliente._ORIGIN,
        "Referer": cliente._REFERER,
        "Cookie": efetivo,
        "x-csrf-token": csrf,
    }
    corpo = json.dumps(
        {"urls": [_URL_TESTE], "tag": afiliado.TAG}).encode("utf-8")

    secao("ENTRADA COMUM AOS DOIS TESTES")
    linha("URL alvo", _URL_TESTE[:70])
    linha("endpoint", cliente.ENDPOINT)
    linha("pares no Cookie", len(_pares(efetivo)))
    linha("x-csrf-token presente", "sim" if csrf else "NÃO")
    linha("tamanho do CSRF", len(csrf))
    linha("ML_TAG presente", "sim" if afiliado.TAG else "NÃO")
    linha("bytes do corpo", len(corpo))
    linha("cabeçalhos explícitos", len(cabecalhos))

    secao("TESTE 1 — urllib.request")
    r1 = teste_urllib(cabecalhos, corpo)
    for k, v in r1.items():
        linha(k, v)

    secao("TESTE 2 — aiohttp")
    r2 = await teste_aiohttp(cabecalhos, corpo)
    for k, v in r2.items():
        linha(k, v)

    secao("COMPARAÇÃO")
    print(f"  {'TRANSPORTE':<12} {'STATUS':<8} RESULTADO", flush=True)
    print("  " + "-" * 58, flush=True)
    for r in (r1, r2):
        resumo = (r["erro"] or r["message"]
                  or f"short_url={r['short_url']}")
        print(f"  {r['transporte']:<12} {str(r['status']):<8} {resumo}",
              flush=True)

    s1, s2 = r1["status"], r2["status"]
    if s1 == 200 and s2 != 200:
        veredito = "É TRANSPORTE — urllib passa, aiohttp não"
    elif s1 != 200 and s2 != 200 and s1 == s2:
        veredito = "NÃO é transporte — os dois recusados igual"
    elif s1 == 200 and s2 == 200:
        veredito = "não reproduzido — investigar o fluxo de produção"
    else:
        veredito = "falhas diferentes — registrar, não concluir"
    print(f"\n  VEREDITO: {veredito}", flush=True)

    fase_e(efetivo)
    print("\n=== FIM SONDA TRANSPORTE ===", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(principal()))
