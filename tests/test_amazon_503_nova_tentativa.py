"""
AMAZON — nova tentativa do 503 "Algo deu errado" e disjuntor por oferta.

Caso real (sonda do #39 em produção, 01/11 17:49 → 02/11 04:17 UTC):
49 chamadas ao SiteStripe, 17 com 200 e 32 com 503 — todos
`servidor=server marca=algo_errado`, em ~80 ms contra ~240 ms do 200.

  - P(503 | chamada anterior 503) = 65% = P(503 | anterior 200): cada
    chamada é um sorteio independente da anterior;
  - 200 e 503 se alternam no mesmo segundo com os MESMOS cookies (o 503
    não renova nada): não é sessão, cookie nem URL;
  - o disjuntor contava CHAMADAS: 3 falhas seguidas (rotina com 65% de
    503) abriam 5 min de pausa, e 69 das 101 URLs longas publicadas
    saíram sem nenhuma tentativa. Só 14% das ofertas saíram curtas,
    contra 35% de sucesso por chamada.

  01  caso real: 503, 503, 200 → short na 3ª tentativa, sem falha no
      disjuntor, pausas de 1 s e 1 s
  02  as 10 tentativas falham → UMA falha no disjuntor, pausas
      1-1-2-2-3-3-4-4-4 s
  03  disjuntor conta OFERTAS: só 3 ofertas seguidas sem short (30
      chamadas) abrem a pausa de 5 min; a 4ª oferta não chama
  04  só o 503 "Algo deu errado" é repetido: captcha, robô, automação,
      login, outra página, 503 da borda, 429, 500, 200 em HTML, JSON
      fora do contrato e exceção — UMA chamada cada, como antes
  05  disjuntor aberto por outra oferta durante a espera → para na hora
  06  a espera acontece FORA do semáforo de HTTP (costura real)
  07  com a Amazon respondendo de primeira: nenhuma linha nova
  08  contrato de formato das linhas novas (só números e rótulos)
  09  orçamento: no máximo 10 chamadas e 24 s de espera por oferta

    python tests/test_amazon_503_nova_tentativa.py
"""
import asyncio
import logging
import os
import re
import sys
import types

os.environ.setdefault("AMAZON_TAG", "fullpromotion-20")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _harness_e5 as harness                                   # noqa: E402

harness.preparar()

from database import _init_db                                   # noqa: E402
from plataformas import amazon                                  # noqa: E402

_init_db()

_HTTP_REAL = amazon._http_getshorturl
_DORMIR_REAL = getattr(amazon, "_dormir", None)

TAG = amazon._AMZ_TAG
CANONICA = f"https://www.amazon.com.br/dp/B0CK2MD3F7?tag={TAG}"
JSON_OK = {
    "ok": True, "isOk": True,
    "shortUrl": "https://link.amazon/B0abWqUsA",
    "longUrl": CANONICA,
}
SUCESSO = (200, "application/json", JSON_OK, "")
ALGO_ERRADO = (503, "text/html;charset=UTF-8", None, "algo_errado")

# CONTRATO DE FORMATO das linhas novas: só números e constantes.
_CONTRATO = [
    re.compile(r"🔁 \[AMZ_NOVA_TENTATIVA\] (?:[2-9]|10)/10 em \d+s — "
               r"HTTP 503 → algo_errado"),
    re.compile(r"✅ \[AMZ_SHORT_NA_TENTATIVA\] (?:[2-9]|10)/10"),
]


# ── infraestrutura mínima ─────────────────────────────────────────
class _Captura(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas = []
        self.lg = logging.getLogger("NORMAL")

    def emit(self, record):
        self.linhas.append((record.levelname, record.getMessage()))

    def __enter__(self):
        self.lg.addHandler(self)
        return self

    def __exit__(self, *exc):
        self.lg.removeHandler(self)
        return False

    def com(self, trecho, nivel=None):
        return [m for n, m in self.linhas
                if trecho in m and (nivel is None or n == nivel)]


class _Relogio:
    def __init__(self):
        self.t = 1000.0

    def monotonic(self):
        return self.t


class _Rede:
    """Fronteira de I/O falsa: devolve o roteiro, na ordem."""
    def __init__(self, *respostas):
        self.respostas = list(respostas)
        self.chamadas = 0

    async def __call__(self, params):
        self.chamadas += 1
        r = self.respostas.pop(0) if len(self.respostas) > 1 \
            else self.respostas[0]
        if isinstance(r, BaseException):
            raise r
        return r


class _Sono:
    """Registra as pausas pedidas e avança o relógio falso."""
    def __init__(self, relogio, ao_dormir=None):
        self.relogio, self.pausas, self.ao_dormir = relogio, [], ao_dormir

    async def __call__(self, segundos):
        self.pausas.append(segundos)
        self.relogio.t += segundos
        if self.ao_dormir:
            self.ao_dormir()


def _cenario(rede, ao_dormir=None):
    amazon._falhas_seguidas = 0
    amazon._disjuntor_ate = 0.0
    amazon._sessao_recusada = False
    amazon._ja_respondeu = True
    relogio = _Relogio()
    amazon.time = types.SimpleNamespace(monotonic=relogio.monotonic)
    amazon._http_getshorturl = rede
    sono = _Sono(relogio, ao_dormir)
    amazon._dormir = sono
    return relogio, sono


def _rodar(coro):
    return asyncio.run(coro)


# ── 01 ────────────────────────────────────────────────────────────
def test_01_caso_real_short_na_terceira(r):
    rede = _Rede(ALGO_ERRADO, ALGO_ERRADO, SUCESSO)
    _, sono = _cenario(rede)
    with _Captura() as cap:
        curta = _rodar(amazon._sitestripe(CANONICA))
    r.check(curta == JSON_OK["shortUrl"], "short oficial na 3ª", curta)
    r.check(rede.chamadas == 3, "3 chamadas", rede.chamadas)
    r.check(sono.pausas == [1.0, 1.0], "pausas de 1 s e 1 s", sono.pausas)
    r.check(len(cap.com("[AMZ_NOVA_TENTATIVA]", "INFO")) == 2,
            "uma linha por nova tentativa", cap.linhas)
    r.check(cap.com("[AMZ_SHORT_NA_TENTATIVA] 3/10", "INFO"),
            "o sucesso diz em qual tentativa veio", cap.linhas)
    r.check(not cap.com("AMZ SiteStripe falhou"),
            "503 repetido com sucesso não conta falha", cap.linhas)
    r.check(amazon._falhas_seguidas == 0 and amazon._disjuntor_ate == 0.0,
            "disjuntor fechado e zerado")


# ── 02 ────────────────────────────────────────────────────────────
def test_02_todas_falham_conta_uma(r):
    rede = _Rede(ALGO_ERRADO)
    _, sono = _cenario(rede)
    with _Captura() as cap:
        curta = _rodar(amazon._sitestripe(CANONICA))
    r.check(curta == "", "falha nunca inventa short", curta)
    r.check(rede.chamadas == 10, "10 chamadas", rede.chamadas)
    r.check(sono.pausas == [1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 4.0],
            "pausas 1-1-2-2-3-3-4-4-4 s", sono.pausas)
    falhas = cap.com("AMZ SiteStripe falhou", "WARNING")
    r.check(len(falhas) == 1 and "(1/3): HTTP 503 tipo=html → algo_errado"
            in falhas[0], "UMA falha, com o motivo de sempre", falhas)
    r.check(amazon._falhas_seguidas == 1 and amazon._disjuntor_ate == 0.0,
            "conta 1 e não pausa", (amazon._falhas_seguidas,
                                    amazon._disjuntor_ate))


# ── 03 ────────────────────────────────────────────────────────────
def test_03_disjuntor_conta_ofertas(r):
    rede = _Rede(ALGO_ERRADO)
    relogio, _ = _cenario(rede)
    with _Captura() as cap:
        for i in range(2):
            _rodar(amazon._sitestripe(CANONICA))
            r.check(amazon._disjuntor_ate == 0.0,
                    f"oferta {i + 1} sem short ainda não pausa")
        _rodar(amazon._sitestripe(CANONICA))
        r.check(rede.chamadas == 30, "3 ofertas × 10 chamadas",
                rede.chamadas)
        r.check(round(amazon._disjuntor_ate - relogio.t) == 300,
                "3ª oferta sem short abre a pausa de 5 min",
                amazon._disjuntor_ate - relogio.t)
        r.check(len(cap.com("suspenso por 300s", "ERROR")) == 1,
                "um aviso de pausa")
        _rodar(amazon._sitestripe(CANONICA))
        r.check(rede.chamadas == 30, "com a pausa aberta, nenhuma chamada",
                rede.chamadas)

    # Um sucesso entre as falhas zera a contagem, como antes.
    rede = _Rede(*([ALGO_ERRADO] * 10), SUCESSO, ALGO_ERRADO)
    _cenario(rede)
    with _Captura():
        _rodar(amazon._sitestripe(CANONICA))
        r.check(amazon._falhas_seguidas == 1, "1ª oferta falhou")
        r.check(_rodar(amazon._sitestripe(CANONICA)) == JSON_OK["shortUrl"],
                "2ª oferta sai curta de primeira")
        r.check(amazon._falhas_seguidas == 0, "sucesso zera a contagem")


# ── 04 ────────────────────────────────────────────────────────────
def test_04_so_o_algo_errado_repete(r):
    casos = [
        ("503 captcha", (503, "text/html", None, "captcha")),
        ("503 automação", (503, "text/html", None, "automacao")),
        ("503 robô", (503, "text/html", None, "robo")),
        ("503 outra página", (503, "text/html", None, "outra")),
        ("503 vazia", (503, "text/html", None, "vazia")),
        ("503 da borda", (503, "text/html", None, "cloudfront")),
        ("503 indisponível", (503, "text/html", None, "indisponivel")),
        ("500 algo errado", (500, "text/html", None, "algo_errado")),
        ("200 HTML algo errado", (200, "text/html", None, "algo_errado")),
        ("429", (429, "text/html", None, "outra")),
        ("302 login", (302, "", None, "login")),
        ("302 outro", (302, "", None, "outro")),
        ("JSON ok=false", (200, "application/json",
                           {"ok": False, "isOk": False}, "")),
        ("exceção", TimeoutError()),
    ]
    for nome, resposta in casos:
        rede = _Rede(resposta)
        _, sono = _cenario(rede)
        with _Captura() as cap:
            curta = _rodar(amazon._sitestripe(CANONICA))
        r.check(curta == "" and rede.chamadas == 1 and not sono.pausas
                and not cap.com("[AMZ_NOVA_TENTATIVA]"),
                f"{nome}: UMA chamada, sem espera", (rede.chamadas,
                                                     sono.pausas))


# ── 05 ────────────────────────────────────────────────────────────
def test_05_disjuntor_aberto_durante_a_espera(r):
    rede = _Rede(ALGO_ERRADO, SUCESSO)
    relogio = None

    def outra_oferta_abre():
        amazon._disjuntor_ate = relogio.t + amazon._PAUSA_SESSAO_RECUSADA

    relogio, sono = _cenario(rede, ao_dormir=outra_oferta_abre)
    with _Captura() as cap:
        curta = _rodar(amazon._sitestripe(CANONICA))
    r.check(curta == "" and rede.chamadas == 1,
            "não chama de novo com o disjuntor aberto", rede.chamadas)
    r.check(len(sono.pausas) == 1, "uma espera só", sono.pausas)
    r.check(not cap.com("AMZ SiteStripe falhou"),
            "quem abriu o disjuntor já contou a falha", cap.linhas)


# ── 06 ────────────────────────────────────────────────────────────
class _Corpo:
    def __init__(self, dados: bytes):
        self.dados = dados

    async def read(self, n=-1):
        bloco, self.dados = self.dados[:n], self.dados[n:]
        return bloco


class _Resposta:
    def __init__(self, status, headers, corpo=b"", dados=None):
        self.status, self.headers = status, headers
        self.content = _Corpo(corpo)
        self._dados = dados

    async def json(self, content_type=None):
        return self._dados

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def test_06_espera_fora_do_semaforo(r):
    pagina = b"<html>Desculpe! Algo deu errado!</html>"
    respostas = [
        _Resposta(503, {"content-type": "text/html"}, corpo=pagina),
        _Resposta(503, {"content-type": "text/html"}, corpo=pagina),
        _Resposta(200, {"content-type": "application/json"}, dados=JSON_OK),
    ]
    sessao = types.SimpleNamespace(get=lambda *a, **k: respostas.pop(0))

    async def obter():
        return sessao

    livre = []

    async def corpo():
        amazon.config._SEM_HTTP = asyncio.Semaphore(1)
        try:
            return await amazon._sitestripe(CANONICA)
        finally:
            amazon.config._SEM_HTTP = None

    relogio, _ = _cenario(_HTTP_REAL)

    async def dormir(segundos):
        livre.append(not amazon.config._SEM_HTTP.locked())
        relogio.t += segundos

    amazon._dormir = dormir
    amazon._ultima_chamada = 0.0
    original = amazon._obter_sessao
    amazon._obter_sessao = obter
    try:
        with _Captura() as cap:
            curta = _rodar(corpo())
    finally:
        amazon._obter_sessao = original
    r.check(curta == JSON_OK["shortUrl"], "costura real: short na 3ª", curta)
    r.check(livre == [True, True], "semáforo livre em cada espera", livre)
    sondas = cap.com("[AMZ_SONDA]", "INFO")
    r.check(len(sondas) == 3 and "marca=algo_errado" in sondas[0]
            and "status=200" in sondas[2], "uma sonda por chamada", sondas)


# ── 07 ────────────────────────────────────────────────────────────
def test_07_de_primeira_sem_ruido(r):
    rede = _Rede(SUCESSO)
    _, sono = _cenario(rede)
    with _Captura() as cap:
        curta = _rodar(amazon._sitestripe(CANONICA))
    r.check(curta == JSON_OK["shortUrl"] and rede.chamadas == 1
            and not sono.pausas, "uma chamada, sem espera")
    r.check(not cap.com("[AMZ_NOVA_TENTATIVA]")
            and not cap.com("[AMZ_SHORT_NA_TENTATIVA]"),
            "nenhuma linha nova", cap.linhas)


# ── 08 ────────────────────────────────────────────────────────────
def test_08_contrato_de_formato(r):
    rede = _Rede(ALGO_ERRADO, ALGO_ERRADO, ALGO_ERRADO, SUCESSO)
    _cenario(rede)
    with _Captura() as cap:
        _rodar(amazon._sitestripe(CANONICA))
    novas = [m for _, m in cap.linhas
             if "AMZ_NOVA_TENTATIVA" in m or "AMZ_SHORT_NA_TENTATIVA" in m]
    r.check(len(novas) == 4, "3 novas tentativas + o sucesso", novas)
    fora = [m for m in novas if not any(c.fullmatch(m) for c in _CONTRATO)]
    r.check(not fora, "só números e rótulos", fora)
    r.check(not any("link.amazon" in m or TAG in m for m in novas),
            "nem short nem tag no log", novas)


# ── 09 ────────────────────────────────────────────────────────────
def test_09_orcamento_por_oferta(r):
    r.check(amazon._TENTATIVAS_503 == 10, "no máximo 10 chamadas",
            amazon._TENTATIVAS_503)
    espera = sum(amazon._PAUSAS_503[min(k, len(amazon._PAUSAS_503)) - 1]
                 for k in range(1, amazon._TENTATIVAS_503))
    r.check(espera == 24.0, "no máximo 24 s de espera", espera)
    r.check(amazon._PISTA_REPETIVEL == "algo_errado",
            "só a página medida em produção")
    r.check(_DORMIR_REAL is asyncio.sleep, "em produção, espera real")


if __name__ == "__main__":
    sys.exit(harness.rodar(globals(), "AMAZON — nova tentativa do 503"))
