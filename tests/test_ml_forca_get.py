"""
P1 — MERCADO LIVRE: o expansor não paga mais o HEAD que o meli.la recusa.

CASO REAL (produção 456afa4, 04–05/11): o rastro passivo do expansor deu
  [RES_FINAL_ERRO] via=head status=405 plataforma=mercadolivre saltos=0
  erros=150/150
— TODO HEAD num host do ML respondeu 405, sem redirecionamento. O único host
do ML que chega ao HEAD é o encurtador meli.la (afiliacao._expandir só roda
em CENARIO_ENCURTADO; a recursão só desce em encurtador declarado). Com 405
e sem salto, o expansor já seguia para o GET: o HEAD era uma ida e volta
inútil por link novo do ML.

A mudança é DECLARATIVA, no lugar que o contrato reserva para isso
(plataforma.encurtadores_forca_get: "hosts que não respondem a HEAD"): o
meli.la passa a ir direto ao GET. A lógica do expansor não muda.

  01  composição: meli.la entra, declarado só pelo ML; Amazon, Shopee e
      Magalu exatamente como antes; nada mais do ML entra
  02  meli.la NÃO executa HEAD: só GET, e devolve o destino do redirecionamento
  03  outro host do ML (mercadolivre.com.br) segue com HEAD primeiro
  04  domínio de fora segue como antes: HEAD; com 405 sem salto, HEAD → GET
  05  Amazon, Shopee e Magalu: a escolha HEAD/GET de cada encurtador igual
  06  timeout no GET do meli.la: devolve a própria URL, o aviso de sempre
  07  resposta inválida no GET do meli.la: mesma classificação
      [RES_FINAL_ERRO] via=get … plataforma=mercadolivre; URL de sempre
  08  ML de ponta a ponta (afilia real, expansor real): sem HEAD, mesmo
      resultado do createLink

    python tests/test_ml_forca_get.py
"""
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _harness_e5 as harness                                   # noqa: E402

harness.preparar()

import globals as g                                             # noqa: E402
import plataformas                                              # noqa: E402
from database import _init_db                                   # noqa: E402
from plataformas import registry                                # noqa: E402
from plataformas.contrato import Afiliacao                      # noqa: E402
from plataformas.mercadolivre import afiliacao as ml, afiliado as ml_afiliado  # noqa: E402
from plataformas.mercadolivre import cliente as ml_cliente, descoberta as ml_desc  # noqa: E402
from plataformas.mercadolivre import sessao as ml_sessao        # noqa: E402
from utils import url_resolver                                  # noqa: E402

plataformas.inicializar()
_init_db()

_SOCIAL = "https://www.mercadolivre.com.br/social/terceiro?ref=abc"
_PRODUTO = "https://www.mercadolivre.com.br/p/MLB16016316"
_SEQ = iter(range(10 ** 6))


class _Resposta:
    def __init__(self, status, url, saltos=0, erro=None):
        self.status, self.url, self.erro = status, url, erro
        self.history = tuple(object() for _ in range(saltos))

    async def __aenter__(self):
        if self.erro is not None:
            raise self.erro
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self, errors=None):
        return ""


class _Sessao:
    """HEAD e GET roteirizados por método; registra a ordem das chamadas."""
    def __init__(self, get, head=None):
        self._get, self._head = get, head
        self.metodos = []

    def get(self, url, **k):
        self.metodos.append("get")
        return self._get(url)

    def head(self, url, **k):
        self.metodos.append("head")
        return (self._head or self._get)(url)


class _Captura(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas = []

    def emit(self, record):
        self.linhas.append(record.getMessage())

    def __enter__(self):
        logging.getLogger("NORMAL").addHandler(self)
        return self

    def __exit__(self, *exc):
        logging.getLogger("NORMAL").removeHandler(self)
        return False


def _expandir(url, sessao):
    async def corpo():
        g._init_globals()
        return await url_resolver.desencurtar(url, sessao)
    return asyncio.run(corpo())


def _meli():
    return f"https://meli.la/P{next(_SEQ)}z"


def test_01_composicao(r):
    mapa = registry.compor_capacidade("encurtadores_forca_get")
    r.check(mapa.get("meli.la") == frozenset({"mercadolivre"}), "01.meli_la_so_do_ml", mapa)
    declarado = {ident: frozenset(getattr(p, "encurtadores_forca_get", None) or ())
                 for ident, p in registry._catalogo.items()}
    r.check(declarado.get("mercadolivre") == frozenset({"meli.la"}), "01.ml_so_o_encurtador",
            declarado.get("mercadolivre"))
    # Os demais, exatamente como em origin/main (456afa4):
    r.check(declarado.get("amazon") == frozenset({"amzn.to", "a.co", "amzn.com", "amzlink.to"})
            and declarado.get("magalu") == frozenset({"maga.lu"})
            and declarado.get("shopee") == frozenset(), "01.outras_plataformas_intocadas",
            declarado)
    hosts = url_resolver._compor_forca_get()
    r.check("meli.la" in hosts and "mercadolivre.com.br" not in hosts
            and "lista.mercadolivre.com.br" not in hosts, "01.so_o_host_comprovado", sorted(hosts))
    r.check(url_resolver._eh_intermediario("https://meli.la/x"), "01.segue_encurtador")


def test_02_meli_la_vai_direto_ao_get(r):
    url = _meli()
    s = _Sessao(get=lambda u: _Resposta(200, _SOCIAL, saltos=1),
                head=lambda u: _Resposta(405, u))
    saida = _expandir(url, s)
    r.check(s.metodos == ["get"], "02.sem_head", s.metodos)
    r.check(saida == _SOCIAL, "02.destino_do_redirecionamento", saida)
    r.check(url_resolver._get_raw(url) == _SOCIAL, "02.cache_de_sempre")


def test_03_outro_host_do_ml_segue_com_head(r):
    url = f"https://www.mercadolivre.com.br/sec/X{next(_SEQ)}"
    s = _Sessao(get=lambda u: _Resposta(200, _PRODUTO, saltos=1))
    _expandir(url, s)
    r.check(s.metodos[:1] == ["head"], "03.head_primeiro", s.metodos)


def test_04_dominio_de_fora_como_antes(r):
    url = f"https://encurta.exemplo.com/x{next(_SEQ)}"
    s = _Sessao(get=lambda u: _Resposta(200, "https://loja.exemplo.com/p/1", saltos=1))
    saida = _expandir(url, s)
    r.check(s.metodos == ["head"] and saida == "https://loja.exemplo.com/p/1",
            "04.head_resolve_sozinho", (s.metodos, saida))
    url = f"https://encurta.exemplo.com/y{next(_SEQ)}"
    s = _Sessao(get=lambda u: _Resposta(200, "https://loja.exemplo.com/p/2", saltos=1),
                head=lambda u: _Resposta(405, u))
    saida = _expandir(url, s)
    r.check(s.metodos == ["head", "get"] and saida == "https://loja.exemplo.com/p/2",
            "04.head_405_cai_no_get", (s.metodos, saida))


def test_05_amazon_shopee_magalu_sem_regressao(r):
    casos = [("https://amzn.to/", "get"), ("https://a.co/", "get"),
             ("https://link.amazon/", "head"), ("https://s.shopee.com.br/", "head"),
             ("https://shope.ee/", "head"), ("https://maga.lu/", "get"),
             ("https://divulgador.magalu.com/", "head")]
    for base, primeiro in casos:
        s = _Sessao(get=lambda u: _Resposta(200, "https://destino.exemplo/p", saltos=1))
        _expandir(f"{base}c{next(_SEQ)}", s)
        r.check(s.metodos[:1] == [primeiro], f"05.{base}", s.metodos)


def test_06_timeout_no_get_tratamento_de_sempre(r):
    url = _meli()
    s = _Sessao(get=lambda u: _Resposta(200, u, erro=asyncio.TimeoutError()))
    with _Captura() as cap:
        saida = _expandir(url, s)
    r.check(saida == url and s.metodos == ["get"], "06.devolve_a_propria_url", (saida, s.metodos))
    r.check(any(l.startswith("⏱ Timeout desencurtar") for l in cap.linhas), "06.aviso_de_sempre",
            cap.linhas)


def test_07_resposta_invalida_mesma_classificacao(r):
    url_resolver._finais.clear()
    url = _meli()
    s = _Sessao(get=lambda u: _Resposta(405, u))
    with _Captura() as cap:
        saida = _expandir(url, s)
    rastro = [l for l in cap.linhas if "[RES_FINAL_ERRO]" in l]
    r.check(saida == url and s.metodos == ["get"], "07.sem_redirecionamento_url_de_sempre",
            (saida, s.metodos))
    r.check(len(rastro) == 1 and "via=get status=405 plataforma=mercadolivre saltos=0" in rastro[0],
            "07.classificacao", rastro)
    url_resolver._finais.clear()           # o rastro é o 1º erro e depois 1 a cada 25
    url = _meli()
    s = _Sessao(get=lambda u: _Resposta(403, _SOCIAL, saltos=1))
    with _Captura() as cap:
        saida = _expandir(url, s)
    rastro = [l for l in cap.linhas if "[RES_FINAL_ERRO]" in l]
    r.check(saida == _SOCIAL and rastro and "via=get status=403 plataforma=mercadolivre saltos=1"
            in rastro[0], "07.destino_com_erro", (saida, rastro))


def test_08_ml_de_ponta_a_ponta(r):
    chamadas = []

    async def descobrir(url, sessao):
        chamadas.append(("descobrir", url))
        return ml_desc.Destino(tipo="produto", url=_PRODUTO)

    async def criar_link(alvo, credencial, geracao=0, sessao_http=None):
        chamadas.append(("criar_link", alvo))
        return ml_cliente.LinkAfiliado(publicada="https://meli.la/NOSSO9",
                                       canonica="https://www.mercadolivre.com.br/social/nosso",
                                       tag="nosso")
    trocas = [(ml_desc, "descobrir", descobrir), (ml_cliente, "criar_link", criar_link),
              (ml_afiliado, "configurado", lambda: True),
              (ml_sessao, "disponivel", lambda: True), (ml_sessao, "expirada", lambda: False),
              (ml_sessao, "carregar", lambda: ml_sessao.Credencial(cookie="c", csrf="x")),
              (ml_sessao, "geracao", lambda: 0)]
    antes = [(m, n, getattr(m, n)) for m, n, _ in trocas]
    for m, n, v in trocas:
        setattr(m, n, v)
    try:
        s = _Sessao(get=lambda u: _Resposta(200, _SOCIAL, saltos=1),
                    head=lambda u: _Resposta(405, u))

        async def corpo():
            g._init_globals()
            return await ml.afilia(_meli(), s)
        res = asyncio.run(corpo())
    finally:
        for m, n, v in antes:
            setattr(m, n, v)
    r.check(s.metodos == ["get"], "08.sem_head_no_caminho_real", s.metodos)
    r.check([c[0] for c in chamadas] == ["descobrir", "criar_link"]
            and chamadas[0][1] == _SOCIAL and chamadas[1][1].startswith(_PRODUTO),
            "08.mesmo_caminho_descoberta_createlink", chamadas)
    r.check(isinstance(res, Afiliacao) and res.publicada == "https://meli.la/NOSSO9"
            and res.canonica.startswith(_PRODUTO), "08.mesmo_resultado", res)


if __name__ == "__main__":
    sys.exit(harness.rodar(globals(), "P1 — ML: meli.la direto ao GET"))
