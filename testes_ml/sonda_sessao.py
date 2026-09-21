"""
SONDA MESTRE — sessão / credencial do Mercado Livre.

Diagnóstico, não correção. Não altera nenhum comportamento do
sistema: lê as mesmas variáveis, chama as MESMAS funções do pacote
(`sessao._decodificar`, `sessao._limpar`) e reproduz a semeadura do
jar exatamente como `cliente._obter_sessao` faz — só que um cookie
por vez, para descobrir qual o aiohttp recusa.

═══════════════════════════════════════════════════════════════════
REGRA DE SEGREDO
═══════════════════════════════════════════════════════════════════
Nenhum VALOR de cookie, token ou header é impresso, em nenhuma
etapa, nem dentro de mensagem de exceção.

NOME de cookie é impresso apenas quando ele é um token legítimo
(`^[A-Za-z0-9_!#$%&'*+.^`|~-]+$`). Isso não é preciosismo: se o
header vier mal partido, o "nome" de um pedaço pode ser um trecho
de VALOR de outro cookie — imprimi-lo vazaria segredo justamente no
caso que a sonda existe para investigar. Nome fora do padrão sai
redigido, com tamanho e os caracteres proibidos, que é o que
interessa ao diagnóstico.

A mensagem da CookieError carrega a chave ilegal entre aspas. Por
isso toda mensagem de exceção passa por `_sanitizar` antes de sair.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
import traceback

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import aiohttp
from yarl import URL

from plataformas.mercadolivre import afiliado, cliente, sessao


# Caracteres que o `http.cookies` do Python aceita em NOME de cookie.
_TOKEN_LEGAL = re.compile(r"^[A-Za-z0-9_!#$%&'*+.^`|~-]+$")

# Produto comprovado nas rodadas anteriores. Serve só de alvo do
# POST real; não é segredo.
_URL_TESTE = os.environ.get(
    "ML_TEST_URL",
    "https://www.mercadolivre.com.br/monitor-aoc-238-100hz-1ms"
    "-gaming-hdmi-24b30hm2/p/MLB46469571",
)

RESUMO = {
    "credencial_lida": "FALHA",
    "parsing": "FALHA",
    "cookies_encontrados": 0,
    "cookies_aceitos": 0,
    "cookies_rejeitados": 0,
    "client_session": "FALHA",
    "post_saiu": "NÃO",
    "http_status": "NA",
    "csrf_chegou": "NÃO",
    "ponto_da_falha": "(não determinado)",
}


def secao(titulo: str) -> None:
    print(f"\n{'=' * 66}\n{titulo}\n{'=' * 66}", flush=True)


def linha(rotulo: str, valor) -> None:
    print(f"  {rotulo:<38} {valor}", flush=True)


def _sanitizar(texto: str) -> str:
    """
    Remove qualquer trecho entre aspas de uma mensagem de exceção.

    `CookieError: Illegal key 'xyz'` carrega a chave — que pode ser
    pedaço de um valor. Some com o miolo e preserva a forma.
    """
    texto = str(texto)
    texto = re.sub(r"'[^']*'", "'<redigido>'", texto)
    texto = re.sub(r'"[^"]*"', '"<redigido>"', texto)
    return texto[:200]


def _nome_seguro(nome: str) -> str:
    """Nome de cookie, ou uma descrição redigida quando é suspeito."""
    if _TOKEN_LEGAL.match(nome):
        return nome
    proibidos = sorted({c for c in nome if not _TOKEN_LEGAL.match(c)})
    amostra = "".join(proibidos)[:12]
    return (f"<nome ilegal redigido | len={len(nome)} | "
            f"caracteres proibidos: {amostra!r}>")


# ══════════════════════════════════════════════════════════════════
# 1 — VARIÁVEIS
# ══════════════════════════════════════════════════════════════════
def etapa_variaveis() -> tuple:
    secao("1) VARIÁVEIS")
    bruto = os.environ.get("ML_SESSION_COOKIE") or ""
    csrf = os.environ.get("ML_CSRF_TOKEN") or ""
    tag = os.environ.get("ML_TAG") or ""

    linha("ML_SESSION_COOKIE presente", "sim" if bruto.strip() else "NÃO")
    linha("tamanho bruto do cookie", len(bruto))
    linha("ML_CSRF_TOKEN presente", "sim" if csrf.strip() else "NÃO")
    linha("tamanho do CSRF", len(csrf))
    linha("ML_TAG presente", "sim" if tag.strip() else "NÃO")
    return bruto, csrf


# ══════════════════════════════════════════════════════════════════
# 2 — NORMALIZAÇÃO
# ══════════════════════════════════════════════════════════════════
def etapa_normalizacao(bruto: str) -> str:
    secao("2) NORMALIZAÇÃO")
    sem_espaco = re.sub(r"\s+", "", bruto)
    parece_header = (";" in bruto and "=" in bruto)

    linha("tamanho antes da limpeza", len(bruto))
    linha("tem whitespace/quebra de linha",
          "sim" if len(sem_espaco) != len(bruto) else "não")
    linha("whitespace removido (chars)", len(bruto) - len(sem_espaco))
    linha("aparenta Cookie Header cru", "sim" if parece_header else "não")

    if parece_header:
        linha("tentativa de Base64", "não — atalho de header cru")
        linha("resultado da decodificação", "n/a")
    else:
        try:
            alvo = sem_espaco + "=" * (-len(sem_espaco) % 4)
            import base64
            texto = base64.b64decode(alvo, validate=True).decode("utf-8")
            linha("tentativa de Base64", "sim")
            linha("resultado da decodificação", "SUCESSO")
            linha("decodificado tem forma de header",
                  "sim" if (";" in texto and "=" in texto) else "NÃO")
        except Exception as exc:
            linha("tentativa de Base64", "sim")
            linha("resultado da decodificação",
                  f"FALHA — {type(exc).__name__}: {_sanitizar(exc)}")

    # O caminho REAL do pacote, não uma reimplementação.
    efetivo = sessao._limpar(sessao._decodificar(bruto))
    linha("tamanho final da credencial", len(efetivo))
    RESUMO["credencial_lida"] = "OK" if efetivo else "FALHA"
    return efetivo


# ══════════════════════════════════════════════════════════════════
# 3 — PARSING
# ══════════════════════════════════════════════════════════════════
def etapa_parsing(efetivo: str) -> list:
    secao("3) PARSING")
    pares = []
    for parte in efetivo.split(";"):
        nome, sep, valor = parte.strip().partition("=")
        if sep and nome:
            pares.append((nome, valor))

    linha("total de pares nome=valor", len(pares))
    RESUMO["cookies_encontrados"] = len(pares)
    RESUMO["parsing"] = "OK" if pares else "FALHA"

    print("\n  índice  nome", flush=True)
    print("  " + "-" * 60, flush=True)
    for i, (nome, _v) in enumerate(pares):
        print(f"  {i:<7} {_nome_seguro(nome)}", flush=True)
    return pares


# ══════════════════════════════════════════════════════════════════
# 4 — COOKIEJAR, UM POR UM
# ══════════════════════════════════════════════════════════════════
def etapa_cookiejar(pares: list) -> list:
    secao("4) COOKIEJAR — inserção individual")
    origem = URL("https://www.mercadolivre.com.br")
    aceitos, rejeitados = [], []

    print("  índice  resultado    nome / motivo", flush=True)
    print("  " + "-" * 62, flush=True)
    for i, (nome, valor) in enumerate(pares):
        jar = aiohttp.CookieJar()
        try:
            jar.update_cookies({nome: valor}, response_url=origem)
            aceitos.append(nome)
            print(f"  {i:<7} ACEITO       {_nome_seguro(nome)}", flush=True)
        except Exception as exc:
            # NÃO interrompe: a sonda precisa saber quantos e quais.
            rejeitados.append((i, nome, type(exc).__name__, exc))
            print(
                f"  {i:<7} REJEITADO    {_nome_seguro(nome)}\n"
                f"          └─ {type(exc).__name__}: {_sanitizar(exc)}",
                flush=True,
            )

    RESUMO["cookies_aceitos"] = len(aceitos)
    RESUMO["cookies_rejeitados"] = len(rejeitados)

    print(f"\n  aceitos: {len(aceitos)}   rejeitados: {len(rejeitados)}",
          flush=True)

    if rejeitados:
        RESUMO["ponto_da_falha"] = (
            f"CookieJar.update_cookies — {len(rejeitados)} nome(s) "
            f"recusado(s) pelo http.cookies"
        )

    # Reproduz o que o cliente faz HOJE: tudo de uma vez.
    secao("4b) COOKIEJAR — semeadura em bloco (como o cliente faz)")
    jar = aiohttp.CookieJar()
    try:
        jar.update_cookies(dict(pares), response_url=origem)
        linha("semeadura em bloco", "OK")
        linha("cookies no jar depois", len(jar))
    except Exception as exc:
        linha("semeadura em bloco", f"FALHOU — {type(exc).__name__}")
        linha("mensagem", _sanitizar(exc))
        linha("consequência", "um nome ilegal derruba os 28 de uma vez")
        print("\n  traceback sanitizado:", flush=True)
        for ln in traceback.format_exc().splitlines():
            print("    " + _sanitizar(ln), flush=True)
    return aceitos


# ══════════════════════════════════════════════════════════════════
# 5 — CLIENT SESSION
# ══════════════════════════════════════════════════════════════════
def etapa_session() -> aiohttp.ClientSession:
    secao("5) CLIENT SESSION")
    # Jar VAZIO de propósito: o `cliente` manda o header `Cookie`
    # explicitamente (cliente.py, montagem dos cabeçalhos), então o
    # jar não é o que autentica. Com jar vazio a sonda mede se a
    # credencial vale, independente do defeito de semeadura.
    jar = aiohttp.CookieJar()
    linha("CookieJar criado", "sim")
    sessao_http = aiohttp.ClientSession(cookie_jar=jar)
    linha("ClientSession criada", "sim")
    linha("cookies efetivamente no jar", len(jar))
    linha("estratégia", "jar vazio + header Cookie explícito")
    RESUMO["client_session"] = "OK"
    return sessao_http


# ══════════════════════════════════════════════════════════════════
# 6 — CREATE LINK
# ══════════════════════════════════════════════════════════════════
async def etapa_createlink(sessao_http, efetivo: str, csrf: str) -> None:
    secao("6) CREATE LINK — POST real")
    corpo = json.dumps({"urls": [_URL_TESTE], "tag": afiliado.TAG})
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

    linha("endpoint", cliente.ENDPOINT)
    linha("método", "POST")
    linha("Cookie header presente", "sim" if cabecalhos["Cookie"] else "NÃO")
    linha("pares no Cookie header",
          len([p for p in efetivo.split(";")
               if p.strip() and "=" in p]))
    linha("x-csrf-token presente", "sim" if csrf else "NÃO")
    linha("campos do corpo", "urls[1], tag")
    linha("tag presente no corpo", "sim" if afiliado.TAG else "NÃO")
    RESUMO["csrf_chegou"] = "SIM" if csrf else "NÃO"

    inicio = time.monotonic()
    try:
        async with sessao_http.post(
            cliente.ENDPOINT,
            data=corpo.encode("utf-8"),
            headers=cabecalhos,
            timeout=aiohttp.ClientTimeout(total=25),
            allow_redirects=False,
        ) as resposta:
            RESUMO["post_saiu"] = "SIM"
            linha("request saiu da aplicação", "SIM")
            linha("status HTTP", resposta.status)
            RESUMO["http_status"] = resposta.status
            linha("content-type",
                  resposta.headers.get("content-type", "(ausente)"))
            linha("location",
                  resposta.headers.get("location", "(ausente)"))
            texto = await resposta.text(errors="ignore")
            linha("duração (s)", f"{time.monotonic() - inicio:.2f}")
            _relatar_corpo(texto, resposta.status)
    except Exception as exc:
        RESUMO["post_saiu"] = "SIM (exceção)"
        RESUMO["ponto_da_falha"] = f"POST createLink — {type(exc).__name__}"
        linha("request saiu da aplicação", "SIM, mas levantou exceção")
        linha("exceção", f"{type(exc).__name__}: {_sanitizar(exc)}")
        print("\n  traceback sanitizado:", flush=True)
        for ln in traceback.format_exc().splitlines():
            print("    " + _sanitizar(ln), flush=True)


def _relatar_corpo(texto: str, status: int) -> None:
    """Só campos de controle. Nunca o corpo inteiro."""
    try:
        dados = json.loads(texto)
    except Exception:
        linha("corpo", f"não-JSON, {len(texto)} chars")
        marcas = [m for m in ("login", "signin", "captcha", "denied",
                              "Unauthorized", "<html") if m in texto[:4000]]
        linha("marcas no corpo", marcas or "(nenhuma)")
        return

    linha("corpo", "JSON válido")
    for chave in ("status", "total_success", "total_error", "message",
                  "error"):
        if chave in dados:
            linha(f"  {chave}", dados[chave])
    urls = dados.get("urls")
    if isinstance(urls, list) and urls:
        primeiro = urls[0] if isinstance(urls[0], dict) else {}
        linha("  urls[0].created", primeiro.get("created"))
        linha("  urls[0].error_code", primeiro.get("error_code"))
        curta = primeiro.get("short_url") or ""
        linha("  urls[0].short_url", curta or "(ausente)")
        linha("  tag confirmada", primeiro.get("tag") or "(ausente)")
    if status == 200 and isinstance(urls, list) and urls:
        if (urls[0] or {}).get("short_url"):
            RESUMO["ponto_da_falha"] = (
                "NENHUM no HTTP — credencial válida e createLink "
                "respondeu com link. A falha está SÓ na semeadura do jar."
            )


# ══════════════════════════════════════════════════════════════════
async def principal() -> int:
    print("\n" + "#" * 66, flush=True)
    print("#  SONDA MESTRE — SESSÃO / CREDENCIAL MERCADO LIVRE", flush=True)
    print("#  diagnóstico apenas — nenhum comportamento alterado",
          flush=True)
    print("#" * 66, flush=True)

    bruto, csrf = etapa_variaveis()
    efetivo = etapa_normalizacao(bruto)
    pares = etapa_parsing(efetivo)
    etapa_cookiejar(pares)

    sessao_http = etapa_session()
    try:
        await etapa_createlink(sessao_http, efetivo, csrf)
    finally:
        await sessao_http.close()

    secao("=== DIAGNÓSTICO ML ===")
    print(f"credencial lida: {RESUMO['credencial_lida']}", flush=True)
    print(f"parsing: {RESUMO['parsing']}", flush=True)
    print(f"cookies encontrados: {RESUMO['cookies_encontrados']}",
          flush=True)
    print(f"cookies aceitos pelo CookieJar: {RESUMO['cookies_aceitos']}",
          flush=True)
    print(f"cookies rejeitados: {RESUMO['cookies_rejeitados']}", flush=True)
    print(f"ClientSession: {RESUMO['client_session']}", flush=True)
    print(f"POST createLink saiu: {RESUMO['post_saiu']}", flush=True)
    print(f"HTTP status: {RESUMO['http_status']}", flush=True)
    print(f"CSRF chegou à etapa HTTP: {RESUMO['csrf_chegou']}", flush=True)
    print(f"ponto exato da falha: {RESUMO['ponto_da_falha']}", flush=True)
    print("=== FIM ===", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(principal()))
    except Exception as exc:
        print(f"\nSONDA ABORTOU: {type(exc).__name__}: {_sanitizar(exc)}",
              flush=True)
        for ln in traceback.format_exc().splitlines():
            print("  " + _sanitizar(ln), flush=True)
        sys.exit(1)
