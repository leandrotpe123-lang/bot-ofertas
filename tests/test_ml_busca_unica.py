"""
P2-A — MERCADO LIVRE: busca única (meli.la → vitrine → destino numa requisição).

CASO REAL (produção c74f62e, 06–07/11, [CONV_MS] de 185 links novos do ML):
  expandir   p50 647 ms — GET no meli.la seguindo até /social/…; o corpo é
             DESCARTADO (o aiohttp fecha a conexão com corpo pendente)
  descoberta p50 595 ms — GET da MESMA /social/… de novo, em conexão nova
Duas idas à mesma página. A busca única lê o documento da resposta que já
chegou; em todo outro desfecho o caminho anterior decide, sem nenhuma
requisição a mais do que ele fazia (exceto os dois casos raros do teste 10,
documentados).

  01  aiohttp REAL + servidor local: URL final e documento saem da MESMA
      resposta; caminho anterior = 2 GETs na vitrine em 2 conexões TCP;
      busca única = 1 GET e 1 conexão, com os cabeçalhos da descoberta
  02  aiohttp REAL: timeout ANTES da resposta e LENDO o documento — onde o
      aiohttp levanta, e o desfecho de cada um
  03  produto: destino, 1 GET, cabeçalhos/timeout/saltos da descoberta,
      cache de resolução gravado como o expansor gravaria
  04  lista (seeMoreLink)
  05  estrutura desconhecida → caminho anterior; o motivo não repete no log
  06  vitrine com HTTP ≠ 200 → caminho anterior
  07  documento acima do teto → caminho anterior
  08  erro de rede ANTES do documento → desfecho do expansor (a própria URL),
      sem nova tentativa; LENDO o documento → o da descoberta (None)
  09  redirecionamento para não-vitrine (lista de afiliado, produto direto,
      fora do domínio): cache gravado, caminho anterior sem rede
  10  sem redirecionamento / outro encurtador no fim: caminho anterior
      inteiro, cache intocado (+1 requisição, casos raros e documentados)
  11  cache de resolução já preenchido → não busca
  12  defeito (não é rede) → caminho anterior intocado, sem etapa medida
  13  links do ML fora do meli.la e Amazon/Shopee/Magalu → nunca busca
  14  EQUIVALÊNCIA: 14 mundos × (caminho anterior forçado | busca única):
      mesmo alvo, mesmo cache de resolução, mesmo motivo; requisições
      comparadas uma a uma
  15  afilia de ponta a ponta: mesma Afiliacao, createLink com o MESMO alvo
  16  identifica (curadoria): mesma identidade, sem afilia e sem cache de links
  17  medição P0: etapa busca_unica na ficha e no [CONV_MS] pelo core

    python tests/test_ml_busca_unica.py
"""
import asyncio
import json
import logging
import os
import re
import socket
import sys
from collections import Counter

# aiohttp REAL, antes do harness (que só instala o falso se não houver o
# real): os testes 01 e 02 provam a semântica do próprio aiohttp contra um
# servidor em 127.0.0.1 — nenhuma rede externa, nenhum DNS de fora.
import aiohttp                                                  # noqa: E402
from aiohttp import web                                         # noqa: E402
from aiohttp.abc import AbstractResolver                        # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _harness_e5 as harness                                   # noqa: E402

harness.preparar()

import globals as g                                             # noqa: E402
import medicao_desempenho as md                                 # noqa: E402
import plataformas                                              # noqa: E402
from database import _init_db                                   # noqa: E402
from pipeline import normalizacao_links as nl                   # noqa: E402
from plataformas import registry                                # noqa: E402
from plataformas.contrato import AUSENTE, Afiliacao, TipoLink   # noqa: E402
from plataformas.mercadolivre import afiliacao as ml, afiliado as ml_afiliado  # noqa: E402
from plataformas.mercadolivre import cliente as ml_cliente, descoberta as ml_desc  # noqa: E402
from plataformas.mercadolivre import sessao as ml_sessao        # noqa: E402
from utils import url_resolver                                  # noqa: E402

plataformas.inicializar()
_init_db()

_SEQ = iter(range(10 ** 6))
_PRODUTO = "https://www.mercadolivre.com.br/p/MLB16016316"
_LISTAGEM = ("https://lista.mercadolivre.com.br/_Container_promotions-77-full"
             "?coupon_campaign_id=13657213")


def _doc(info, enchimento=0):
    estado = {"appProps": {"pageProps": {"data": {"components": [
        {}, {"recommendation_data": {"recommendation_info": info}}]}}}}
    pad = f"<!-- {'x' * enchimento} -->" if enchimento else ""
    return ("<html><head><script>window.__PRELOADED_STATE__ = "
            f"{json.dumps(estado)};</script></head><body>{pad}</body></html>")


_DOC_PRODUTO = _doc({"seeMoreLink": "",
                     "polycards": [{"url": "www.mercadolivre.com.br/p/MLB16016316"}]})
_DOC_LISTA = _doc({"seeMoreLink": _LISTAGEM, "polycards": [{}, {}]})
_DOC_ESTRANHO = "<html><body>sem estado</body></html>"
# Produto VÁLIDO acima do teto (5000 nos testes): só o teto o recusa.
_DOC_PRODUTO_GRANDE = _doc({"seeMoreLink": "",
                            "polycards": [{"url": "www.mercadolivre.com.br/p/MLB16016316"}]},
                           enchimento=6000)


def _n():
    return next(_SEQ)


def _vitrine(rotulo):
    return f"https://www.mercadolivre.com.br/social/terceiro?ref={rotulo}"


class _Troca:
    """Troca atributos e devolve no fim (monkeypatch da casa, sem pytest)."""
    def __init__(self, *trocas):
        self.trocas, self.antes = trocas, []

    def __enter__(self):
        for alvo, nome, valor in self.trocas:
            self.antes.append((alvo, nome, getattr(alvo, nome)))
            setattr(alvo, nome, valor)
        return self

    def __exit__(self, *exc):
        for alvo, nome, valor in reversed(self.antes):
            setattr(alvo, nome, valor)
        return False


class _Captura(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas = []

    def emit(self, record):
        self.linhas.append(record.getMessage())

    def __enter__(self):
        for nome in ("NORMAL", "SISTEMA"):
            logging.getLogger(nome).addHandler(self)
        return self

    def __exit__(self, *exc):
        for nome in ("NORMAL", "SISTEMA"):
            logging.getLogger(nome).removeHandler(self)
        return False


def _rodar(fabrica):
    async def principal():
        g._init_globals()
        return await fabrica()
    return asyncio.run(principal())


def _anterior():
    """O caminho de ANTES, forçado: a busca única nunca conclui."""
    async def nunca(url, sessao_http):
        return ml._SEGUIR
    return _Troca((ml, "_busca_unica", nunca))


# ══════════════════════════════════════════════════════════════════
# A REDE DO ML COMO O AIOHTTP A ENTREGA (sessão falsa)
# ══════════════════════════════════════════════════════════════════
class _Resposta:
    def __init__(self, mundo, pedida, final, status, corpo, saltos):
        self.mundo, self.pedida = mundo, pedida
        self.url, self.status, self.corpo = final, status, corpo
        self.history = tuple(object() for _ in range(saltos))

    async def __aenter__(self):
        if self.mundo.falha and self.mundo.falha[0] == "antes":
            raise self.mundo.falha[1]
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self, errors=None):
        self.mundo.leituras.append(self.url)
        if self.mundo.falha and self.mundo.falha[0] == "corpo":
            raise self.mundo.falha[1]
        return self.corpo


class _Mundo:
    """Segue o redirecionamento HTTP inteiro (allow_redirects=True) e entrega
    status e corpo da resposta FINAL. Conta cada GET e cada leitura de corpo."""
    def __init__(self, saltos, paginas, falha=None):
        self.saltos, self.paginas, self.falha = saltos, paginas, falha
        self.gets, self.leituras, self.kwargs = [], [], []

    def _pedir(self, metodo, url, k):
        self.gets.append((metodo, url))
        self.kwargs.append(k)
        final, n = url, 0
        while final in self.saltos and n < 20:
            final, n = self.saltos[final], n + 1
        status, corpo = self.paginas.get(final, (200, ""))
        return _Resposta(self, url, final, status, corpo, n)

    def get(self, url, **k):
        return self._pedir("get", url, k)

    def head(self, url, **k):
        return self._pedir("head", url, k)


def _mundo(nome):
    """(url do meli.la, _Mundo) do cenário — URLs novas a cada chamada, para o
    cache de resolução de uma rodada nunca servir a outra."""
    n = _n()
    t = f"T{n}T"                     # marcador da rodada (normalizado na comparação)
    curta, social = f"https://meli.la/{nome}{t}", _vitrine(f"{nome}{t}")
    vitrine = {curta: social}
    mundos = {
        "produto": lambda: _Mundo(vitrine, {social: (200, _DOC_PRODUTO)}),
        "lista": lambda: _Mundo(vitrine, {social: (200, _DOC_LISTA)}),
        "estrutura": lambda: _Mundo(vitrine, {social: (200, _DOC_ESTRANHO)}),
        "http403": lambda: _Mundo(vitrine, {social: (403, "proibido")}),
        "http500": lambda: _Mundo(vitrine, {social: (500, "")}),
        "teto": lambda: _Mundo(vitrine, {social: (200, _DOC_PRODUTO_GRANDE)}),
        "timeout_antes": lambda: _Mundo(vitrine, {}, ("antes", asyncio.TimeoutError())),
        "conexao_antes": lambda: _Mundo(vitrine, {}, (
            "antes", aiohttp.ClientConnectionError("conexão recusada"))),
        "timeout_corpo": lambda: _Mundo(vitrine, {social: (200, _DOC_PRODUTO)},
                                        ("corpo", asyncio.TimeoutError())),
        "lista_afiliado": lambda: _Mundo(
            {curta: f"https://www.mercadolivre.com.br/social/promotom/lists/{t}"}, {}),
        "produto_direto": lambda: _Mundo(
            {curta: f"https://produto.mercadolivre.com.br/MLB-3000000001-x{t}-_JM"}, {}),
        "fora": lambda: _Mundo({curta: f"https://outro-site.exemplo/x{t}"}, {}),
        "sem_salto": lambda: _Mundo({}, {curta: (200, "<html>sem meta</html>")}),
        "encadeado": lambda: _Mundo({curta: f"https://meli.la/B{t}"},
                                    {f"https://meli.la/B{t}": (200, "<html>fim</html>")}),
    }
    return curta, social, mundos[nome]()


# Diferença de GETs (busca única − caminho anterior), por mundo. Negativa
# só onde a busca única concluiu; positiva só nos dois casos raros do 10.
_DELTA_GETS = {"produto": -1, "lista": -1, "timeout_corpo": -1,
               "estrutura": 0, "http403": 0, "http500": 0, "teto": 0,
               "timeout_antes": 0, "conexao_antes": 0, "lista_afiliado": 0,
               "produto_direto": 0, "fora": 0, "sem_salto": +1, "encadeado": +1}


def _teto_pequeno():
    return _Troca((ml_desc, "_TETO_BYTES", 5000))


async def _resolver(url, mundo):
    with md.link() as ficha:
        alvo = await ml._resolver_alvo(url, mundo)
    return alvo, ficha


def _rodada(nome, anterior):
    curta, social, mundo = _mundo(nome)
    with _teto_pequeno():
        if anterior:
            with _anterior():
                alvo, ficha = _rodar(lambda: _resolver(curta, mundo))
        else:
            alvo, ficha = _rodar(lambda: _resolver(curta, mundo))
    cache = url_resolver._get_raw(curta)
    troca = (lambda v: re.sub(r"T\d+T", "T#T", v.replace(curta, "<CURTA>")
                              .replace(social, "<SOCIAL>")) if isinstance(v, str) else v)
    return dict(alvo=troca(alvo), cache=troca(cache), motivo=ficha.motivo,
                gets=len(mundo.gets), leituras=len(mundo.leituras), mundo=mundo,
                etapas=[(n, ok) for n, _, ok in ficha.etapas])


# ══════════════════════════════════════════════════════════════════
# 01–02  aiohttp REAL contra servidor local
# ══════════════════════════════════════════════════════════════════
class _ResolverLocal(AbstractResolver):
    """meli.la e www.mercadolivre.com.br → 127.0.0.1. Nada sai da máquina."""
    async def resolve(self, host, port=0, family=socket.AF_INET):
        return [{"hostname": host, "host": "127.0.0.1", "port": port,
                 "family": socket.AF_INET, "proto": 0, "flags": socket.AI_NUMERICHOST}]

    async def close(self):
        pass


class _Servidor:
    """meli.la/<código> → 302 → www.mercadolivre.com.br/social/terceiro?ref=…;
    a vitrine serve o documento. Conta pedidos e CONEXÕES TCP por host."""
    def __init__(self, documento, atraso_meli=0.0, trava_corpo=False):
        self.documento, self.atraso_meli, self.trava_corpo = documento, atraso_meli, trava_corpo
        self.pedidos, self.conexoes, self.uas = Counter(), {}, []

    async def _tratar(self, req):
        host = req.host.split(":")[0]
        self.pedidos[host] += 1
        self.conexoes.setdefault(host, set()).add(req.transport.get_extra_info("peername")[1])
        self.uas.append((host, req.headers.get("User-Agent", "")))
        if host == "meli.la":
            await asyncio.sleep(self.atraso_meli)
            ref = req.path.strip("/")
            return web.Response(status=302, headers={
                "Location": f"http://www.mercadolivre.com.br:{self.porta}/social/terceiro?ref={ref}"})
        if self.trava_corpo:
            resp = web.StreamResponse(status=200, headers={"Content-Type": "text/html"})
            resp.content_length = len(self.documento)
            await resp.prepare(req)
            await resp.write(self.documento[:1000].encode())
            await asyncio.sleep(5)
            return resp
        return web.Response(text=self.documento, content_type="text/html")

    async def __aenter__(self):
        app = web.Application()
        app.router.add_route("GET", "/{resto:.*}", self._tratar)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self.porta = self._runner.addresses[0][1]
        return self

    async def __aexit__(self, *exc):
        await self._runner.cleanup()
        return False

    def curta(self, codigo):
        return f"http://meli.la:{self.porta}/{codigo}"


def _sessao_local():
    return aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=_ResolverLocal()))


def test_01_aiohttp_real_uma_resposta_uma_conexao(r):
    doc = _doc({"seeMoreLink": "",
                "polycards": [{"url": "www.mercadolivre.com.br/p/MLB16016316"}]},
               enchimento=300_000)                      # ~300 KB, como o real

    async def corpo():
        saida = {}
        for modo in ("anterior", "busca_unica"):
            async with _Servidor(doc) as srv, _sessao_local() as sessao:
                curta = srv.curta(f"{modo}{_n()}")
                if modo == "anterior":
                    with _anterior():
                        alvo = await asyncio.wait_for(ml._resolver_alvo(curta, sessao), 30)
                else:
                    alvo = await asyncio.wait_for(ml._resolver_alvo(curta, sessao), 30)
                saida[modo] = dict(
                    alvo=alvo, pedidos=dict(srv.pedidos),
                    conexoes={h: len(c) for h, c in srv.conexoes.items()},
                    uas=list(srv.uas), cache=url_resolver._get_raw(curta))
        return saida

    s = _rodar(corpo)
    a, b = s["anterior"], s["busca_unica"]
    r.check(a["alvo"] == b["alvo"] == _PRODUTO, "01.mesmo_destino", (a["alvo"], b["alvo"]))
    r.check(a["pedidos"].get("www.mercadolivre.com.br") == 2
            and a["conexoes"].get("www.mercadolivre.com.br") == 2,
            "01.antes_2_gets_2_conexoes_na_vitrine", a)
    r.check(b["pedidos"] == {"meli.la": 1, "www.mercadolivre.com.br": 1}
            and b["conexoes"].get("www.mercadolivre.com.br") == 1,
            "01.busca_unica_1_get_1_conexao", b)
    r.check(all(ua == ml_desc._UA for _h, ua in b["uas"]), "01.cabecalhos_da_descoberta_ate_no_meli",
            b["uas"])
    r.check(b["cache"] is not None and "/social/terceiro?ref=busca_unica" in b["cache"],
            "01.cache_de_resolucao_gravado", b["cache"])


def test_02_aiohttp_real_timeout_antes_e_lendo(r):
    async def caso(atraso_meli, trava_corpo):
        async with _Servidor(_DOC_PRODUTO + " " * 5000, atraso_meli, trava_corpo) as srv, \
                _sessao_local() as sessao:
            curta = srv.curta(f"t{_n()}")
            with _Troca((ml_desc, "_TIMEOUT_S", 0.5)):
                busca = await asyncio.wait_for(ml_desc.buscar(curta, sessao), 10)
                with md.link() as f:
                    alvo = await asyncio.wait_for(ml._resolver_alvo(curta, sessao), 10)
            return busca, alvo, f.motivo, dict(srv.pedidos), url_resolver._get_raw(curta)

    busca, alvo, motivo, pedidos, cache = _rodar(lambda: caso(2.0, False))
    r.check(isinstance(busca.erro, TimeoutError) and busca.final == "" and busca.destino is None,
            "02.antes.timeout_sem_final", busca)
    r.check(alvo is not None and alvo.startswith("http://meli.la:") and motivo == "timeout_expansao"
            and cache is None, "02.antes.desfecho_do_expansor", (alvo, motivo, cache))
    r.check(pedidos == {"meli.la": 2}, "02.antes.sem_nova_tentativa", pedidos)

    busca, alvo, motivo, pedidos, cache = _rodar(lambda: caso(0.0, True))
    r.check(isinstance(busca.erro, TimeoutError) and "/social/terceiro" in busca.final
            and busca.destino is None, "02.lendo.timeout_com_final", busca)
    r.check(alvo is None and motivo == "timeout_descoberta" and cache is not None,
            "02.lendo.desfecho_da_descoberta", (alvo, motivo, cache))
    r.check(pedidos == {"meli.la": 2, "www.mercadolivre.com.br": 2},
            "02.lendo.sem_nova_tentativa", pedidos)


# ══════════════════════════════════════════════════════════════════
# 03–13  casos
# ══════════════════════════════════════════════════════════════════
def test_03_produto(r):
    with _Captura() as cap:
        x = _rodada("produto", anterior=False)
    k = x["mundo"].kwargs[0]
    r.check([l for l in cap.linhas if "ML descoberta" in l]
            == ["🛒 ML descoberta | tipo=produto | origem=social"], "03.marcador_de_sempre",
            cap.linhas)
    r.check(x["alvo"] == _PRODUTO, "03.destino", x["alvo"])
    r.check(x["gets"] == 1 and x["leituras"] == 1, "03.uma_requisicao_um_corpo",
            (x["gets"], x["leituras"]))
    r.check(k.get("headers") == ml_desc._CABECALHOS and k.get("allow_redirects") is True
            and k.get("max_redirects") == 20 and k["timeout"].total == ml_desc._TIMEOUT_S,
            "03.cabecalhos_timeout_saltos_da_descoberta", k)
    r.check("Cookie" not in k.get("headers", {}) and "cookie" not in k.get("headers", {}),
            "03.anonima")
    r.check(x["cache"] == "<SOCIAL>", "03.cache_como_o_expansor", x["cache"])
    r.check(x["etapas"] == [("busca_unica", True)] and x["motivo"] is None, "03.medicao",
            (x["etapas"], x["motivo"]))


def test_04_lista(r):
    with _Captura() as cap:
        x = _rodada("lista", anterior=False)
    r.check([l for l in cap.linhas if "ML descoberta" in l]
            == ["🛒 ML descoberta | tipo=lista | origem=social"], "04.marcador_de_sempre",
            cap.linhas)
    r.check(x["alvo"] == _LISTAGEM and x["gets"] == 1, "04.lista_numa_requisicao",
            (x["alvo"], x["gets"]))


def test_05_estrutura_desconhecida(r):
    with _Captura() as cap:
        x = _rodada("estrutura", anterior=False)
    r.check(x["alvo"] is None and x["motivo"] == "falha_descoberta", "05.desfecho_de_antes",
            (x["alvo"], x["motivo"]))
    r.check(x["gets"] == 2 and [m for m, _ in x["mundo"].gets] == ["get", "get"]
            and x["mundo"].gets[1][1] == x["mundo"].saltos[x["mundo"].gets[0][1]],
            "05.caminho_anterior_pede_a_vitrine", x["mundo"].gets)
    r.check(sum("ML descoberta | estrutura não reconhecida" in l for l in cap.linhas) == 1
            and any("busca única sem destino (estrutura não reconhecida)" in l for l in cap.linhas),
            "05.log_uma_vez_e_o_porque", cap.linhas)
    r.check(x["etapas"] == [("busca_unica", True), ("expandir", True), ("descoberta", False)],
            "05.etapas", x["etapas"])


def test_06_vitrine_http_nao_200(r):
    for nome, motivo in (("http403", "descoberta_http"), ("http500", "descoberta_http")):
        with _Captura() as cap:
            x = _rodada(nome, anterior=False)
        r.check(x["alvo"] is None and x["motivo"] == motivo and x["gets"] == 2
                and x["leituras"] == 0, f"06.{nome}.caminho_anterior", x)
        r.check(any(f"busca única sem destino (HTTP {nome[-3:]})" in l for l in cap.linhas),
                f"06.{nome}.porque", cap.linhas)


def test_07_documento_acima_do_teto(r):
    x = _rodada("teto", anterior=False)
    r.check(x["alvo"] is None and x["motivo"] == "falha_descoberta" and x["gets"] == 2,
            "07.caminho_anterior", x)
    with _Troca((ml_desc, "_TETO_BYTES", 10 ** 7)):
        curta, social, mundo = _mundo("teto")
        alvo, _f = _rodar(lambda: _resolver(curta, mundo))
    r.check(alvo == _PRODUTO and len(mundo.gets) == 1, "07.abaixo_do_teto_le_numa_requisicao",
            (alvo, mundo.gets))


def test_08_erro_de_rede(r):
    for nome, motivo in (("timeout_antes", "timeout_expansao"),
                         ("conexao_antes", "falha_expansao")):
        x = _rodada(nome, anterior=False)
        r.check(x["alvo"] == "<CURTA>" and x["motivo"] == motivo and x["cache"] is None
                and x["gets"] == 1, f"08.{nome}.desfecho_do_expansor_sem_nova_tentativa", x)
        r.check(x["etapas"] == [("busca_unica", False)], f"08.{nome}.etapa_falha", x["etapas"])
    x = _rodada("timeout_corpo", anterior=False)
    r.check(x["alvo"] is None and x["motivo"] == "timeout_descoberta" and x["cache"] == "<SOCIAL>"
            and x["gets"] == 1, "08.lendo.desfecho_da_descoberta_sem_nova_tentativa", x)


def test_09_redirecionamento_para_nao_vitrine(r):
    for nome in ("lista_afiliado", "produto_direto", "fora"):
        x = _rodada(nome, anterior=False)
        r.check(x["gets"] == 1 and x["leituras"] == 0 and x["cache"] is not None,
                f"09.{nome}.uma_requisicao_cache_gravado", x)
    x = _rodada("lista_afiliado", anterior=False)
    r.check("/lists/" in (x["alvo"] or "") and x["motivo"] is None, "09.lista_afiliado.alvo", x)
    x = _rodada("fora", anterior=False)
    r.check(x["alvo"] is None and x["motivo"] == "falha_expansao", "09.fora.descarta", x)


def test_10_sem_salto_ou_outro_encurtador(r):
    for nome in ("sem_salto", "encadeado"):
        novo, antigo = _rodada(nome, anterior=False), _rodada(nome, anterior=True)
        r.check(novo["alvo"] == antigo["alvo"] and novo["cache"] == antigo["cache"]
                and novo["gets"] == antigo["gets"] + 1, f"10.{nome}.caminho_anterior_inteiro",
                (novo, antigo))


def test_11_cache_de_resolucao_preenchido(r):
    curta, social, mundo = _mundo("produto")
    url_resolver._set_raw(curta, social)
    chamadas = []

    async def espiao(url, sessao):
        chamadas.append(url)
        return ml_desc.Busca()
    with _Troca((ml_desc, "buscar", espiao)):
        alvo, f = _rodar(lambda: _resolver(curta, mundo))
    r.check(chamadas == [] and alvo == _PRODUTO and len(mundo.gets) == 1
            and mundo.gets[0] == ("get", social), "11.nao_busca_descobre_como_antes",
            (chamadas, alvo, mundo.gets))


def test_12_defeito_nao_e_rede(r):
    curta, social, mundo = _mundo("produto")

    class _Quebrada:
        def get(self, *a, **k):
            raise ValueError("defeito")
    sessoes = []

    async def expansao(url, sessao):
        sessoes.append(sessao)
        return social
    with _Troca((ml, "desencurtar", expansao)), _Captura() as cap:
        quebrada = _Quebrada()
        busca = _rodar(lambda: ml_desc.buscar(curta, quebrada))
        with _Troca((ml_desc, "descobrir",
                     lambda u, s: _desc_ok(u))):
            alvo, f = _rodar(lambda: _resolver(curta, quebrada))
    r.check(busca == ml_desc.Busca(), "12.busca_vazia", busca)
    r.check(alvo == _PRODUTO and sessoes == [quebrada], "12.caminho_anterior_intocado",
            (alvo, sessoes))
    r.check([(n, ok) for n, _, ok in f.etapas] == [("expandir", True), ("descoberta", True)]
            and f.motivo is None, "12.sem_etapa_de_busca", f.etapas)
    r.check(any("defeito ValueError → caminho anterior" in l for l in cap.linhas),
            "12.aviso", cap.linhas)


async def _desc_ok(url):
    return ml_desc.Destino(tipo="produto", url=_PRODUTO)


def test_13_fora_do_meli_la_nunca_busca(r):
    chamadas = []

    async def espiao(url, sessao):
        chamadas.append(url)
        return ml_desc.Busca()
    urls_ml = [_vitrine(f"direta{_n()}"), _PRODUTO + f"?x={_n()}", _LISTAGEM,
               "https://www.mercadolivre.com/sec/1abc", "https://www.mercadolivre.com.br/social/x/lists/u"]
    with _Troca((ml_desc, "buscar", espiao),
                (ml_desc, "descobrir", lambda u, s: _desc_ok(u))):
        for u in urls_ml:
            _rodar(lambda u=u: ml._resolver_alvo(u, _Mundo({}, {})))
    r.check(chamadas == [], "13.ml_sem_encurtador", chamadas)
    outras = {"https://amzn.to/3abc": "amazon", "https://www.amazon.com.br/dp/B0ABC12345": "amazon",
              "https://s.shopee.com.br/AbC": "shopee", "https://shopee.com.br/x-i.1.2": "shopee",
              "https://maga.lu/abc": "magalu", "https://www.magazineluiza.com.br/p/123/": "magalu"}
    r.check({u: getattr(registry.resolver(u), "identificador", None) for u in outras} == outras,
            "13.amazon_shopee_magalu_nao_passam_pelo_ml")


# ══════════════════════════════════════════════════════════════════
# 14–17  equivalência, ponta a ponta, curadoria, medição
# ══════════════════════════════════════════════════════════════════
def test_14_equivalencia_matriz(r):
    for nome, delta in _DELTA_GETS.items():
        novo, antigo = _rodada(nome, anterior=False), _rodada(nome, anterior=True)
        for campo in ("alvo", "cache", "motivo"):
            r.check(novo[campo] == antigo[campo], f"14.{nome}.{campo}",
                    (novo[campo], antigo[campo]))
        r.check(novo["gets"] - antigo["gets"] == delta, f"14.{nome}.gets_{delta:+d}",
                (novo["gets"], antigo["gets"]))
        r.check(novo["leituras"] <= antigo["leituras"] + (1 if nome in ("estrutura", "teto") else 0),
                f"14.{nome}.leituras", (novo["leituras"], antigo["leituras"]))


def _trocas_createlink(enviados):
    async def criar_link(alvo, credencial, geracao=0, sessao_http=None):
        enviados.append(alvo)
        return ml_cliente.LinkAfiliado(publicada="https://meli.la/NOSSO9",
                                       canonica="https://www.mercadolivre.com.br/social/nosso",
                                       tag="nosso")
    return _Troca((ml_cliente, "criar_link", criar_link),
                  (ml_afiliado, "configurado", lambda: True),
                  (ml_sessao, "disponivel", lambda: True), (ml_sessao, "expirada", lambda: False),
                  (ml_sessao, "carregar", lambda: ml_sessao.Credencial(cookie="c", csrf="x")),
                  (ml_sessao, "geracao", lambda: 0))


def test_15_afilia_ponta_a_ponta(r):
    for nome in ("produto", "lista", "estrutura", "lista_afiliado", "timeout_antes"):
        saidas = []
        for anterior in (True, False):
            enviados = []
            curta, _s, mundo = _mundo(nome)
            with _trocas_createlink(enviados), _teto_pequeno():
                if anterior:
                    with _anterior():
                        res = _rodar(lambda: ml.afilia(curta, mundo))
                else:
                    res = _rodar(lambda: ml.afilia(curta, mundo))
            resumo = ("AUSENTE" if res is AUSENTE
                      else (res.publicada, res.canonica) if isinstance(res, Afiliacao) else res)
            saidas.append((resumo, enviados))
        r.check(saidas[0] == saidas[1], f"15.{nome}.mesma_afiliacao_mesmo_alvo", saidas)
    r.check(saidas[0][0] == "AUSENTE", "15.timeout_antes_ausente")


def test_16_identifica_da_curadoria(r):
    proibidos = []

    def proibido(nome):
        async def _a(*a, **k):
            proibidos.append(nome)
            raise AssertionError(nome)
        return _a
    for nome in ("produto", "lista", "timeout_antes", "timeout_corpo"):
        ids = []
        for anterior in (True, False):
            curta, _s, mundo = _mundo(nome)
            with _Troca((ml_cliente, "criar_link", proibido("criar_link")),
                        (ml, "consultar_link", proibido("consultar_link")),
                        (ml, "registrar_link", proibido("registrar_link"))):
                if anterior:
                    with _anterior():
                        ident = _rodar(lambda: ml.identifica(curta, mundo))
                else:
                    ident = _rodar(lambda: ml.identifica(curta, mundo))
            ids.append((ident.tipo_link, ident.id_produto))
        r.check(ids[0] == ids[1], f"16.{nome}.mesma_identidade", ids)
    r.check(proibidos == [], "16.sem_afilia_nem_cache_de_links", proibidos)
    curta, _s, mundo = _mundo("produto")
    ident = _rodar(lambda: ml.identifica(curta, mundo))
    r.check(ident.tipo_link == TipoLink.PRODUTO and ident.id_produto == "MLB16016316"
            and len(mundo.gets) == 1, "16.produto_numa_requisicao", (ident, mundo.gets))


def test_17_medicao_p0(r):
    enviados = []
    curta, _s, mundo = _mundo("produto")
    with _trocas_createlink(enviados), _Captura() as cap:
        res = _rodar(lambda: nl._normalizar_medido(curta, mundo, 1, Counter()))
    linhas = [l for l in cap.linhas if "[CONV_MS]" in l]
    r.check(res[2] == "mercadolivre" and res[1] is not None, "17.converteu", res)
    r.check(len(linhas) == 1 and "busca_unica=" in linhas[0] and "createlink=" in linhas[0]
            and "expandir=" not in linhas[0] and "descoberta=" not in linhas[0]
            and " resultado=ok" in linhas[0], "17.linha_conv_ms", linhas)
    r.check(all("meli.la" not in l and "mercadolivre.com" not in l for l in linhas),
            "17.sem_url", linhas)
    curta, _s, mundo = _mundo("produto")
    with _Captura() as cap:
        _rodar(lambda: ml.identifica(curta, mundo))
    etapa = [l for l in cap.linhas if "[CONV_ETAPA]" in l]
    r.check(len(etapa) == 1 and "etapa=busca_unica" in etapa[0] and "resultado=ok" in etapa[0],
            "17.curadoria_conv_etapa", etapa)


if __name__ == "__main__":
    sys.exit(harness.rodar(globals(), "P2-A — ML: busca única"))
