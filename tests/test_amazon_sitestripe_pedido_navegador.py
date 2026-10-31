"""
AMAZON — o pedido do bot ao SiteStripe sai igual ao do navegador.

CAPTURA DO NAVEGADOR (DevTools do dono, 31/10 05:22 UTC, SiteStripe
"Obter link → Texto", resposta 200 com shortUrl link.amazon):
  - GET https://www.amazon.com.br/associates/sitestripe/getShortUrl
  - query: longUrl=<encodeURIComponent da longUrl inteira>
           &marketplaceId=526970&storeId=<loja>
    ex.: `longUrl=https%3A%2F%2Fwww.amazon.com.br%2F<produto>…%25C3%25AD…`
  - 14 cookies, cada um enviado como está guardado (sem aspas
    acrescentadas): at-acbbr, cmc, csm-hit, i18n-prefs, lc-acbbr, rxc,
    sess-at-acbbr, session-id, session-id-time, session-token,
    sso-state-acbbr, sst-acbbr, ubid-acbbr, x-acbbr.

DEFEITOS REPRODUZIDOS (código anterior, aiohttp 3.14.5 de produção):
  D1  o jar padrão do aiohttp (quote_cookie=True) põe ASPAS em todo
      valor com `+ / = ? @` e DUPLICA as aspas de valor já entre aspas
      (`"x"` → `"\\"x\\""`): os cookies de autenticação saem diferentes
      do que o navegador manda;
  D2  o mesmo vale para o cookie que a Amazon RENOVA por Set-Cookie: a
      partir da segunda chamada ele volta com aspas;
  D3  a longUrl ia com `:` `/` `?` crus na query; o navegador codifica
      tudo (valores decodificados iguais, linha diferente).

  01  D1: os cookies de AMAZON_COOKIE saem byte a byte
  02  D2: cookie renovado pela Amazon volta byte a byte
  03  D3: a linha do pedido é IDÊNTICA à que o navegador monta (regras
      de codificação observadas na captura), sobre uma longUrl com a
      estrutura da captura
  04  os valores decodificados continuam os mesmos; Accept e
      Accept-Language iguais aos do navegador (captura de 31/10)
  05  SEM DUPLA CODIFICAÇÃO: o servidor decodifica UMA vez e obtém a
      longUrl original; nenhum `%2525`; cada `%` da longUrl vira um
      único `%25`
  06  quote_cookie=False SÓ na sessão da Amazon: nenhum outro CookieJar
      de produção muda (Mercado Livre e sessão compartilhada intactos)

TODOS os valores aqui são SINTÉTICOS — cookies, URL, tag, linkId e loja;
só os NOMES dos cookies e o FORMATO são os da captura.

    python tests/test_amazon_sitestripe_pedido_navegador.py
"""
import asyncio
import os
import socket
import sys
from urllib.parse import parse_qsl, quote, urlsplit

import aiohttp                                   # real: o pedido é o do bot
from aiohttp import web

os.environ.setdefault("AMAZON_TAG", "fullpromotion-20")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _harness_e5 as harness                                   # noqa: E402

harness.preparar()

import config                                                   # noqa: E402
from plataformas import amazon                                  # noqa: E402

assert amazon.aiohttp is aiohttp and hasattr(aiohttp, "CookieJar"), \
    "aiohttp FALSO — abortado"

_HTTP_REAL = amazon._http_getshorturl
TAG = amazon._AMZ_TAG
LOJA = "LOJA-TESTE-20"

# Os 14 nomes da captura BR; valores SINTÉTICOS no formato típico.
COOKIES = {
    "at-acbbr": "Atza|IwEBIFAKE-abc_DEF.ghi+jkl/mno=",
    "cmc": "FAKEcmc0123456789",
    "csm-hit": "tb:s-FAKE0123|1700000000000&t:1700000000000&adb:adblk_no",
    "i18n-prefs": "BRL",
    "lc-acbbr": "pt_BR",
    "rxc": "FAKErxc+0/9=",
    "sess-at-acbbr": '"FAKEsessAt+abc/def="',
    "session-id": "132-1234567-7654321",
    "session-id-time": "2082787201l",
    "session-token": '"FAKEtok+en/abc==XYZ"',
    "sso-state-acbbr": "Xdsso|FAKE-sso_state",
    "sst-acbbr": "Sst1|FAKE+sst/value=",
    "ubid-acbbr": "133-7654321-1234567",
    "x-acbbr": '"FAKEx?acb@br"',
}
RENOVADO = "Sst1|FAKErenovado+ab/cd="

# longUrl com a ESTRUTURA da captura de 31/10 (nome do produto já
# percent-codificado, parâmetros de rastreio, sufixo do SiteStripe) e
# todos os VALORES sintéticos.
LONGA = ("https://www.amazon.com.br/Produto-Teste-Pept%C3%ADdeos-Col%C3%A1geno"
         "/dp/B0TESTE123?_encoding=UTF8&pd_rd_w=AbCdE"
         "&content-id=amzn1.sym.00000000-0000-4000-8000-000000000001"
         "&pf_rd_p=00000000-0000-4000-8000-000000000001"
         "&pf_rd_r=AAAAAAAAAAAAAAAAAAAA&pd_rd_wg=abcde"
         "&pd_rd_r=00000000-0000-4000-8000-000000000002&linkCode=sl2"
         f"&tag={TAG}&linkId=0123456789abcdef0123456789abcdef"
         "&ref_=as_li_ss_tl")
# A linha que o navegador monta para essa longUrl, escrita à mão com as
# regras observadas na "URL da solicitação" da captura: `%`→%25,
# `:`→%3A, `/`→%2F, `?`→%3F, `&`→%26, `=`→%3D; `.` `-` `_` sem codificar.
LINHA_NAVEGADOR = (
    "/associates/sitestripe/getShortUrl?longUrl=https%3A%2F%2Fwww.amazon"
    ".com.br%2FProduto-Teste-Pept%25C3%25ADdeos-Col%25C3%25A1geno%2Fdp%2F"
    "B0TESTE123%3F_encoding%3DUTF8%26pd_rd_w%3DAbCdE%26content-id%3Damzn1"
    ".sym.00000000-0000-4000-8000-000000000001%26pf_rd_p%3D00000000-0000-"
    "4000-8000-000000000001%26pf_rd_r%3DAAAAAAAAAAAAAAAAAAAA%26pd_rd_wg%3D"
    "abcde%26pd_rd_r%3D00000000-0000-4000-8000-000000000002%26linkCode%3D"
    f"sl2%26tag%3D{TAG}%26linkId%3D0123456789abcdef0123456789abcdef%26ref_"
    "%3Das_li_ss_tl&marketplaceId=526970"
    f"&storeId={LOJA}")


class _ResolverLocal(aiohttp.abc.AbstractResolver):
    """www.amazon.com.br → servidor local: o jar manda os cookies do
    domínio exatamente como mandaria à Amazon. Nada sai da máquina."""

    def __init__(self, porta):
        self.porta = porta

    async def resolve(self, host, port=0, family=socket.AF_INET):
        return [{"hostname": host, "host": "127.0.0.1", "port": self.porta,
                 "family": socket.AF_INET, "proto": 0, "flags": 0}]

    async def close(self):
        pass


async def _rodar(chamadas: int, renovar: bool = False) -> list:
    """Faz `chamadas` pedidos pelo _http_getshorturl REAL, com o jar do
    bot, a um servidor local que responde como a Amazon."""
    recebidos = []

    async def receber(req):
        recebidos.append({
            "linha": req.raw_path,
            "query": parse_qsl(urlsplit(req.raw_path).query,
                               keep_blank_values=True),
            "cookie": req.headers.get("Cookie", ""),
            "cabecalhos": sorted((k.lower(), v) for k, v in req.headers.items()
                                 if k.lower() not in ("cookie", "host")),
        })
        resp = web.json_response({"ok": False})
        if renovar:
            resp.headers.add("Set-Cookie", f"sst-acbbr={RENOVADO}; Path=/")
        return resp

    app = web.Application()
    app.router.add_get("/associates/sitestripe/getShortUrl", receber)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    porta = site._server.sockets[0].getsockname()[1]

    os.environ["AMAZON_COOKIE"] = "; ".join(f"{k}={v}"
                                            for k, v in COOKIES.items())
    endpoint = amazon._ENDPOINT_SHORT
    amazon._ENDPOINT_SHORT = (f"http://www.amazon.com.br:{porta}"
                              "/associates/sitestripe/getShortUrl")
    sessao_original = aiohttp.ClientSession

    def sessao_local(*a, **k):
        k["connector"] = aiohttp.TCPConnector(resolver=_ResolverLocal(porta))
        return sessao_original(*a, **k)

    amazon.aiohttp.ClientSession = sessao_local
    config._SEM_HTTP = asyncio.Semaphore(1)
    amazon._sessao_amz = None
    try:
        for _ in range(chamadas):
            await _HTTP_REAL({"longUrl": LONGA,
                              "marketplaceId": amazon._MARKETPLACE_ID,
                              "storeId": LOJA})
    finally:
        amazon.aiohttp.ClientSession = sessao_original
        if amazon._sessao_amz is not None:
            await amazon._sessao_amz.close()
        amazon._sessao_amz = None
        amazon._ENDPOINT_SHORT = endpoint
        config._SEM_HTTP = None
        os.environ.pop("AMAZON_COOKIE", None)
        await runner.cleanup()
    return recebidos


def _pares(cabecalho: str) -> dict:
    return dict(p.split("=", 1) for p in cabecalho.split("; ") if "=" in p)


# ── 01 ────────────────────────────────────────────────────────────
def test_01_cookies_saem_byte_a_byte(r):
    recebido = _pares(asyncio.run(_rodar(1))[0]["cookie"])
    diferentes = sorted(k for k, v in COOKIES.items()
                        if recebido.get(k) != v)
    r.check(len(recebido) == 14, "os 14 cookies chegam", len(recebido))
    r.check(not diferentes, "nenhum valor alterado (sem aspas a mais)",
            diferentes)


# ── 02 ────────────────────────────────────────────────────────────
def test_02_cookie_renovado_volta_byte_a_byte(r):
    pedidos = asyncio.run(_rodar(2, renovar=True))
    segundo = _pares(pedidos[1]["cookie"])
    r.check(segundo.get("sst-acbbr") == RENOVADO,
            "o valor renovado pela Amazon volta como ela mandou",
            segundo.get("sst-acbbr"))


# ── 03 ────────────────────────────────────────────────────────────
def test_03_linha_identica_a_do_navegador(r):
    linha = asyncio.run(_rodar(1))[0]["linha"]
    r.check(linha == LINHA_NAVEGADOR, "linha do pedido = navegador",
            f"\nbot: {linha}\nnav: {LINHA_NAVEGADOR}")
    # A transcrição do navegador é exatamente encodeURIComponent da
    # longUrl (conjunto não codificado: A-Z a-z 0-9 - _ . ! ~ * ' ( )).
    r.check(LINHA_NAVEGADOR == ("/associates/sitestripe/getShortUrl?longUrl="
                                + quote(LONGA, safe="!*'()")
                                + f"&marketplaceId=526970&storeId={LOJA}"),
            "transcrição = encodeURIComponent da longUrl")


# ── 04 ────────────────────────────────────────────────────────────
def test_04_valores_e_resto_do_pedido_inalterados(r):
    pedido = asyncio.run(_rodar(1))[0]
    r.check(pedido["query"] == [("longUrl", LONGA),
                                ("marketplaceId", "526970"),
                                ("storeId", LOJA)],
            "valores decodificados iguais aos enviados", pedido["query"])
    h = dict(pedido["cabecalhos"])
    r.check(h.get("accept") == amazon._CABECALHOS_SHORT["accept"]
            and h.get("user-agent") == amazon._CABECALHOS_SHORT["user-agent"]
            and h.get("referer") == amazon._CABECALHOS_SHORT["referer"],
            "cabeçalhos do bot inalterados", h)
    # Captura de 31/10 (Cabeçalhos de solicitação do getShortUrl).
    r.check(h.get("accept") == "application/json, text/javascript, */*; q=0.01"
            and h.get("accept-language") == "pt-BR,pt;q=0.9",
            "Accept e Accept-Language iguais aos do navegador", h)


# ── 05 ────────────────────────────────────────────────────────────
def test_05_sem_dupla_codificacao(r):
    pedido = asyncio.run(_rodar(1))[0]
    linha = pedido["linha"]
    longa_na_linha = linha.split("?longUrl=", 1)[1].split("&marketplaceId=")[0]
    r.check("%2525" not in linha, "nenhum % codificado duas vezes", linha)
    r.check(longa_na_linha.count("%25") == LONGA.count("%"),
            "cada % da longUrl vira um único %25",
            (longa_na_linha.count("%25"), LONGA.count("%")))
    r.check(dict(pedido["query"]).get("longUrl") == LONGA,
            "uma decodificação devolve a longUrl original",
            dict(pedido["query"]).get("longUrl"))


# ── 06 ────────────────────────────────────────────────────────────
def test_06_quote_cookie_so_na_sessao_amazon(r):
    import ast
    raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    fora = ("tests", "testes_ml", "testes_amz", ".git", "__pycache__")
    jars = []
    for base, pastas, arquivos in os.walk(raiz):
        pastas[:] = [p for p in pastas if p not in fora]
        for nome in arquivos:
            if not nome.endswith(".py"):
                continue
            caminho = os.path.join(base, nome)
            rel = os.path.relpath(caminho, raiz)
            with open(caminho, encoding="utf-8") as f:
                arvore = ast.parse(f.read())
            for no in ast.walk(arvore):
                if isinstance(no, ast.Call) and (
                        getattr(no.func, "attr", None) == "CookieJar"
                        or getattr(no.func, "id", None) == "CookieJar"):
                    sem_aspas = any(
                        k.arg == "quote_cookie"
                        and isinstance(k.value, ast.Constant)
                        and k.value.value is False
                        for k in no.keywords)
                    jars.append((rel, sem_aspas))
    r.check(("plataformas/amazon.py", True) in jars,
            "a sessão da Amazon usa quote_cookie=False", jars)
    r.check([j for j in jars if j[1]] == [("plataformas/amazon.py", True)],
            "nenhum outro CookieJar de produção muda", jars)


if __name__ == "__main__":
    sys.exit(harness.rodar(globals(),
                           "AMAZON — pedido ao SiteStripe igual ao navegador"))
