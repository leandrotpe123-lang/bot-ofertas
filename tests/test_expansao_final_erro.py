"""
EXPANSÃO — destino que responde erro deixa rastro (sonda passiva).

CASO REAL (29/10): o SiteStripe da Amazon passou a responder 503
intermitente, e todo `amzn.to` é expandido por GET com
allow_redirects=True até a página de produto da Amazon — um acesso
anônimo por oferta, do mesmo IP das chamadas autenticadas. A resolução
devolvia a URL mesmo quando o destino respondia 503, sem rastro: não
havia como saber se a expansão também estava sendo barrada.

A mudança só OBSERVA; o comportamento da resolução não muda.

  01  GET que termina em 503 → URL final devolvida como antes, e um
      [RES_FINAL_ERRO] com via, status, host, caminho, saltos e
      erros/total — sem query
  02  destino 2xx não registra nada, mas entra no total do host
  03  erro repetido não inunda o log: o 1º e um a cada 25
  04  HEAD (link.amazon não exige GET) também deixa rastro

    python tests/test_expansao_final_erro.py
"""
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _harness_e5 as harness                                   # noqa: E402

harness.preparar()

import plataformas                                              # noqa: E402
from utils import url_resolver                                  # noqa: E402

# Como no boot do worker: sem o catálogo, amzn.to não estaria entre os
# hosts que exigem GET, e o teste mediria outro caminho.
plataformas.inicializar()

FINAL = ("https://www.amazon.com.br/Produto-X/dp/B0CK2MD3F7"
         "?tag=SEGREDO-TAG&linkId=SEGREDO-LINK")


class _Captura(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas = []
        self.lg = logging.getLogger("NORMAL")

    def emit(self, record):
        self.linhas.append(record.getMessage())

    def __enter__(self):
        self.lg.addHandler(self)
        return self

    def __exit__(self, *exc):
        self.lg.removeHandler(self)
        return False

    def com(self, trecho):
        return [m for m in self.linhas if trecho in m]


class _Resposta:
    def __init__(self, status, url, saltos=1):
        self.status, self.url = status, url
        self.history = tuple(object() for _ in range(saltos))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Sessao:
    """GET e HEAD falsos: devolvem o destino roteirizado e contam."""
    def __init__(self, status, final=FINAL):
        self.status, self.final = status, final
        self.metodos = []

    def get(self, url, **k):
        self.metodos.append("get")
        return _Resposta(self.status, self.final)

    def head(self, url, **k):
        self.metodos.append("head")
        return _Resposta(self.status, self.final)


def _expandir(curta, sessao):
    return asyncio.run(url_resolver.desencurtar(curta, sessao))


def test_01_get_em_503_deixa_rastro_sem_mudar_resultado(r):
    url_resolver._finais.clear()
    sessao = _Sessao(503)
    with _Captura() as cap:
        saida = _expandir("https://amzn.to/caso01", sessao)
    r.check(saida == FINAL, "resolução inalterada", saida)
    r.check(sessao.metodos == ["get"], "amzn.to segue por GET",
            sessao.metodos)
    linhas = cap.com("[RES_FINAL_ERRO]")
    r.check(len(linhas) == 1, "um rastro", linhas)
    if linhas:
        m = linhas[0]
        r.check("via=get status=503 host=www.amazon.com.br" in m
                and "caminho=/Produto-X/dp/B0CK2MD3F7" in m
                and "saltos=1" in m and "erros=1/1" in m, "campos", m)
        r.check("SEGREDO" not in m, "sem query", m)


def test_02_destino_ok_so_conta(r):
    url_resolver._finais.clear()
    with _Captura() as cap:
        _expandir("https://amzn.to/caso02a", _Sessao(200))
        _expandir("https://amzn.to/caso02b", _Sessao(503))
    linhas = cap.com("[RES_FINAL_ERRO]")
    r.check(len(linhas) == 1 and "erros=1/2" in linhas[0],
            "2xx silencioso, mas entra no total", linhas)


def test_03_erro_repetido_nao_inunda(r):
    url_resolver._finais.clear()
    with _Captura() as cap:
        for i in range(60):
            _expandir(f"https://amzn.to/caso03-{i}", _Sessao(503))
    linhas = cap.com("[RES_FINAL_ERRO]")
    r.check([m.split("erros=")[1] for m in linhas]
            == ["1/1", "25/25", "50/50"], "1º e um a cada 25", linhas)


def test_04_head_tambem_deixa_rastro(r):
    url_resolver._finais.clear()
    sessao = _Sessao(503)
    with _Captura() as cap:
        saida = _expandir("https://link.amazon/caso04", sessao)
    r.check(saida == FINAL and sessao.metodos == ["head"],
            "link.amazon resolve por HEAD", (saida, sessao.metodos))
    linhas = cap.com("[RES_FINAL_ERRO]")
    r.check(len(linhas) == 1 and "via=head status=503" in linhas[0],
            "rastro do HEAD", linhas)


if __name__ == "__main__":
    sys.exit(harness.rodar(globals(), "EXPANSÃO — destino com erro"))
