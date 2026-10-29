"""
AMAZON — sessão do SiteStripe recusada não é falha transitória.

CASO REAL (logs da Railway, worker): de 22/10 a 29/10 o SiteStripe
nunca respondeu. Cada processo recusado desde a PRIMEIRA chamada
(23/10 13:30:21, 0,2 s depois de "🔐 AMZ sessão SiteStripe iniciada
(3 cookies)"); o contador de falhas subiu de 1 a 84 sem um único
sucesso em 4,5 dias, sempre "HTTP 302 tipo=''". Em 29/10 11:29 a
Amazon passou a responder também "HTTP 503 tipo='text/html'". Com o
cookie renovado (redeploy de 29/10 14:15, mesmo commit) os 302 sumiram
e o contador voltou a zerar — sucessos — entremeado de 503.

DEFEITO: o 302 do endpoint XHR é a sessão recusada — nenhuma nova
tentativa a ressuscita —, mas era tratado como falha transitória com
pausa fixa de 5 min. Com ofertas Amazon chegando a cada 5 min ou
mais, TODA oferta batia no endpoint com o cookie morto (72 chamadas
em 6 h na reprodução contra o código anterior), sem alerta, e o log
não separava login de captcha.

  01  caso 23/10: 302 para o login → UM alerta [AMZ_SESSAO_RECUSADA]
      com o destino; 72 ofertas em 6 h → UMA chamada; sondagem a cada
      6 h, sem repetir o alerta
  02  sondagem que volta a responder → short oficial, [AMZ_SESSAO_OK]
      e fim da pausa
  03  caso 29/10: 503 de captcha é transitória — nunca vira sessão
      recusada; pausa 5 min, 10, 20… teto de 2 h
  04  contador sem teto não estoura a pausa
  05  classificação: 3xx e 401 recusam a sessão; 3xx para o captcha,
      403, 429 e 5xx são transitórias
  06  a primeira resposta boa aparece no log uma vez, não a cada short
  07  o boot registra os NOMES dos cookies e nunca os valores
  08  costura de I/O: pista sem query, sem cookie e sem corpo
  09  ponta a ponta: sessão recusada nunca perde a oferta — publica a
      longa com a nossa tag, e só a nossa
  10  Base64 que não decodifica não põe o segredo no log no lugar do
      nome do cookie
  11  SONDA PASSIVA: uma linha [AMZ_SONDA] por chamada — sucesso, 503,
      302 e exceção —, com intervalo, forma do alvo, marca e título, e
      nada de query, cookie ou corpo
  12  JSON fora do contrato (lista) não derruba quem chama

    python tests/test_amazon_sitestripe_sessao.py
"""
import asyncio
import logging
import os
import sys
import types

os.environ.setdefault("AMAZON_TAG", "fullpromotion-20")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _harness_e5 as harness                                   # noqa: E402

harness.preparar()

from database import _init_db                                   # noqa: E402
from plataformas import amazon                                  # noqa: E402

_init_db()

# A costura REAL, guardada antes que os cenários a troquem pela falsa.
_HTTP_REAL = amazon._http_getshorturl

TAG = amazon._AMZ_TAG
CANONICA = f"https://www.amazon.com.br/dp/B0CK2MD3F7?tag={TAG}"
JSON_OK = {
    "ok": True, "isOk": True,
    "shortUrl": "https://link.amazon/B0abWqUsA",
    "longUrl": CANONICA,
}
LOGIN = (302, "", None, "www.amazon.com.br/ap/signin")
CAPTCHA_503 = (503, "text/html", None, "captcha")
SUCESSO = (200, "application/json", JSON_OK, "")


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
    """Fronteira de I/O falsa: devolve o roteiro e conta as chamadas."""
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


def _cenario(rede):
    """Estado limpo de processo recém-iniciado, relógio e rede falsos."""
    amazon._falhas_seguidas = 0
    amazon._disjuntor_ate = 0.0
    amazon._sessao_recusada = False
    amazon._ja_respondeu = False
    relogio = _Relogio()
    amazon.time = types.SimpleNamespace(monotonic=relogio.monotonic)
    amazon._http_getshorturl = rede
    return relogio


def _rodar(coro):
    return asyncio.run(coro)


# ── 01 ────────────────────────────────────────────────────────────
def test_01_caso_real_sessao_recusada_nao_martela(r):
    rede = _Rede(LOGIN)
    relogio = _cenario(rede)

    async def corpo():
        saidas = []
        for _ in range(72):                     # 6 h, oferta a cada 5 min
            saidas.append(await amazon._sitestripe(CANONICA))
            relogio.t += 300.0
        return saidas

    with _Captura() as cap:
        saidas = _rodar(corpo())
        r.check(all(s == "" for s in saidas), "falha nunca inventa short")
        r.check(rede.chamadas == 1, "72 ofertas em 6 h → UMA chamada",
                f"chamadas={rede.chamadas}")
        alertas = cap.com("[AMZ_SESSAO_RECUSADA]", "ERROR")
        r.check(len(alertas) == 1, "um alerta, não um por oferta",
                f"{len(alertas)}")
        r.check(alertas and "www.amazon.com.br/ap/signin" in alertas[0],
                "o alerta diz para onde a Amazon mandou", alertas)
        r.check(alertas and "AMAZON_COOKIE" in alertas[0],
                "o alerta diz o que fazer", alertas)

        # 6 h depois da recusa: uma sondagem, sem repetir o alerta.
        relogio.t = 1000.0 + amazon._PAUSA_SESSAO_RECUSADA
        _rodar(amazon._sitestripe(CANONICA))
        _rodar(amazon._sitestripe(CANONICA))
        r.check(rede.chamadas == 2, "uma sondagem a cada 6 h",
                f"chamadas={rede.chamadas}")
        r.check(len(cap.com("[AMZ_SESSAO_RECUSADA]")) == 1,
                "sondagem recusada não repete o alerta")
        r.check(len(cap.com("segue recusada", "WARNING")) == 1,
                "sondagem recusada deixa rastro discreto")
        r.check(not cap.com("suspenso por 300s"),
                "recusa não usa a pausa de 5 min da transitória")


# ── 02 ────────────────────────────────────────────────────────────
def test_02_sondagem_que_volta_a_responder(r):
    rede = _Rede(LOGIN, SUCESSO)
    relogio = _cenario(rede)
    with _Captura() as cap:
        r.check(_rodar(amazon._sitestripe(CANONICA)) == "", "recusada")
        relogio.t += amazon._PAUSA_SESSAO_RECUSADA
        curta = _rodar(amazon._sitestripe(CANONICA))
        r.check(curta == JSON_OK["shortUrl"], "short oficial volta", curta)
        r.check(len(cap.com("[AMZ_SESSAO_OK]", "INFO")) == 1,
                "recuperação visível")
        r.check(not amazon._sessao_recusada and amazon._falhas_seguidas == 0,
                "estado zerado")
        relogio.t += 1.0
        _rodar(amazon._sitestripe(CANONICA))
        r.check(rede.chamadas == 3, "sem pausa depois do sucesso",
                f"chamadas={rede.chamadas}")


# ── 03 ────────────────────────────────────────────────────────────
def test_03_caso_real_503_captcha_e_transitoria(r):
    rede = _Rede(CAPTCHA_503)
    relogio = _cenario(rede)
    pausas = []
    with _Captura() as cap:
        for _ in range(9):
            relogio.t = max(relogio.t, amazon._disjuntor_ate)
            _rodar(amazon._sitestripe(CANONICA))
            pausas.append(max(0, round(amazon._disjuntor_ate - relogio.t)))
        r.check(rede.chamadas == 9, "cada chamada só depois da pausa")
        r.check(pausas == [0, 0, 300, 600, 1200, 2400, 4800, 7200, 7200],
                "abre no limite; pausa exponencial com teto de 2 h", pausas)
        r.check(not cap.com("[AMZ_SESSAO_RECUSADA]"),
                "captcha nunca vira sessão recusada")
        r.check(not amazon._sessao_recusada, "estado não marca recusa")
        falhas = cap.com("AMZ SiteStripe falhou", "WARNING")
        r.check(falhas and all("captcha" in f for f in falhas),
                "o log separa captcha de login", falhas[:1])


# ── 04 ────────────────────────────────────────────────────────────
def test_04_contador_sem_teto_nao_estoura(r):
    rede = _Rede(CAPTCHA_503)
    relogio = _cenario(rede)
    amazon._falhas_seguidas = 10_000
    with _Captura():
        try:
            _rodar(amazon._sitestripe(CANONICA))
            erro = None
        except Exception as e:                  # noqa: BLE001
            erro = e
    r.check(erro is None, "sem OverflowError", repr(erro))
    r.check(round(amazon._disjuntor_ate - relogio.t) == 7200,
            "pausa no teto", amazon._disjuntor_ate - relogio.t)


# ── 05 ────────────────────────────────────────────────────────────
def test_05_classificacao(r):
    recusa, trans = amazon._SESSAO_RECUSADA, amazon._TRANSITORIA
    casos = [
        (301, "www.amazon.com.br/ap/signin", recusa),
        (302, "www.amazon.com.br/ap/signin", recusa),
        (302, "", recusa),
        (303, "/gp/sign-in.html", recusa),
        (307, "www.amazon.com.br/ap/signin", recusa),
        (308, "www.amazon.com.br/ap/signin", recusa),
        (401, "", recusa),
        (302, "www.amazon.com.br/errors/validateCaptcha", trans),
        (403, "", trans),
        (429, "", trans),
        (500, "", trans),
        (503, "captcha", trans),
        (503, "", trans),
        (200, "", trans),
    ]
    for status, pista, esperado in casos:
        obtido = amazon._classe_falha(status, pista)
        r.check(obtido == esperado, f"{status} {pista!r}", obtido)

    rede = _Rede(TimeoutError())
    _cenario(rede)
    with _Captura() as cap:
        _rodar(amazon._sitestripe(CANONICA))
    r.check(not cap.com("[AMZ_SESSAO_RECUSADA]") and
            cap.com("TimeoutError", "WARNING"),
            "erro de rede é transitório")


# ── 06 ────────────────────────────────────────────────────────────
def test_06_primeira_resposta_boa_visivel_uma_vez(r):
    rede = _Rede(SUCESSO)
    _cenario(rede)
    with _Captura() as cap:
        for _ in range(3):
            _rodar(amazon._sitestripe(CANONICA))
    r.check(rede.chamadas == 3, "três shorts")
    r.check(len(cap.com("[AMZ_SESSAO_OK]")) == 1,
            "prova de sessão válida no boot, sem ruído depois")


# ── 07 ────────────────────────────────────────────────────────────
def test_07_boot_registra_nomes_nunca_valores(r):
    segredos = ("VALOR-SECRETO-AT", "VALOR-SECRETO-SID", "VALOR-SECRETO-UB")
    os.environ["AMAZON_COOKIE"] = (
        f"session-id={segredos[1]}; at-acbbr={segredos[0]}; "
        f"ubid-acbbr={segredos[2]}"
    )

    class _Jar:
        def update_cookies(self, *a, **k):
            pass

    class _Sessao:
        closed = False

        def __init__(self, *a, **k):
            pass

    original = amazon.aiohttp
    amazon.aiohttp = types.SimpleNamespace(
        CookieJar=_Jar, ClientSession=_Sessao,
    )
    amazon._sessao_amz = None
    try:
        with _Captura() as cap:
            _rodar(amazon._obter_sessao())
    finally:
        amazon.aiohttp = original
        amazon._sessao_amz = None
        os.environ.pop("AMAZON_COOKIE", None)

    linhas = cap.com("AMZ sessão SiteStripe iniciada")
    r.check(len(linhas) == 1, "uma linha no boot", linhas)
    r.check(linhas and "3 cookies: at-acbbr, session-id, ubid-acbbr"
            in linhas[0], "nomes ordenados", linhas)
    vazou = [m for _, m in cap.linhas if any(s in m for s in segredos)]
    r.check(not vazou, "nenhum valor de cookie no log", vazou)


# ── 10 ────────────────────────────────────────────────────────────
def test_10_base64_que_nao_decodifica_nao_vaza(r):
    """Base64 com quebra de linha (painel de variáveis) não decodifica
    e chega inteiro na posição do NOME — que não pode ir para o log."""
    import base64
    bloco = base64.b64encode(
        b"at-acbbr=VALOR-SECRETO-AT; session-id=VALOR-SECRETO-SID",
    ).decode()
    os.environ["AMAZON_COOKIE"] = bloco[:30] + "\n" + bloco[30:]
    try:
        cookies = amazon._cookies_iniciais()
    finally:
        os.environ.pop("AMAZON_COOKIE", None)
    linha = amazon._nomes_para_log(cookies)
    r.check(len(cookies) == 1, "o defeito de captura continua visível",
            len(cookies))
    r.check(linha == "<1 sem forma de nome>", "nada do blob no log", linha)
    r.check(bloco[:20] not in linha and bloco[30:50] not in linha,
            "nenhum trecho do segredo", linha)


# ── 08 ────────────────────────────────────────────────────────────
class _Corpo:
    def __init__(self, dados: bytes):
        self.dados = dados

    async def read(self, n=-1):
        bloco, self.dados = self.dados[:max(1, min(n, 7))], \
            self.dados[max(1, min(n, 7)):]
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


def _costura(resposta):
    sessao = types.SimpleNamespace(get=lambda *a, **k: resposta)

    async def obter():
        return sessao

    async def corpo():
        amazon.config._SEM_HTTP = asyncio.Semaphore(1)
        try:
            return await _HTTP_REAL({"longUrl": CANONICA})
        finally:
            amazon.config._SEM_HTTP = None

    original = amazon._obter_sessao
    amazon._obter_sessao = obter
    try:
        return _rodar(corpo())
    finally:
        amazon._obter_sessao = original


def test_08_costura_pista_sem_segredo(r):
    login = _costura(_Resposta(302, {
        "location": "https://www.amazon.com.br/ap/signin?"
                    "openid.return_to=SEGREDO&session=SEGREDO",
    }))
    r.check(login == (302, "", None, "www.amazon.com.br/ap/signin"),
            "302: host+caminho, sem query", login)

    pagina = (b"<html><title>Amazon.com.br</title>" + b"x" * 500 +
              b'<form action="/errors/validateCaptcha">SEGREDO</form>')
    captcha = _costura(_Resposta(
        503, {"content-type": "text/html"}, corpo=pagina))
    r.check(captcha == (503, "text/html", None, "captcha"),
            "503: reconhece o captcha mesmo lido em blocos", captcha)

    outra = _costura(_Resposta(
        503, {"content-type": "text/html"}, corpo=b"<html>erro</html>"))
    r.check(outra == (503, "text/html", None, ""), "503 sem captcha", outra)

    ok = _costura(_Resposta(
        200, {"content-type": "application/json"}, dados=JSON_OK))
    r.check(ok == (200, "application/json", JSON_OK, ""), "200 JSON", ok)


# ── 09 ────────────────────────────────────────────────────────────
def test_09_ponta_a_ponta_oferta_nunca_se_perde(r):
    rede = _Rede(LOGIN)
    _cenario(rede)
    entrada = ("https://www.amazon.com.br/Acer-Aspire-Go-15/dp/B0GQMVCQQ5"
               "?_encoding=UTF8&th=1&psc=1&tag=outroafiliado-20")
    with _Captura():
        saida = _rodar(amazon.afilia(entrada, None))
    publicada = str(getattr(saida, "publicada", saida))
    r.check(rede.chamadas == 1, "tentou o SiteStripe uma vez")
    r.check(publicada.startswith("https://www.amazon.com.br/") and
            amazon._tag_unica_nossa(publicada),
            "publica a longa com a nossa tag, e só a nossa", publicada)
    r.check("th=1" in publicada and "psc=1" in publicada,
            "variação do produto preservada", publicada)


# ── 11 ────────────────────────────────────────────────────────────
class _Relogio11:
    def __init__(self):
        self.t = 5000.0

    def monotonic(self):
        return self.t


def test_11_sonda_uma_linha_por_chamada(r):
    relogio = _Relogio11()
    amazon.time = types.SimpleNamespace(monotonic=relogio.monotonic)
    amazon._ultima_chamada = 0.0
    longa = ("https://www.amazon.com.br/Produto/dp/B0CK2MD3F7?th=1&psc=1"
             f"&linkId=SEGREDO-LINK&tag={TAG}")
    pagina = (b"<html><head><title>\n  Amazon.com.br  </title></head>"
              b"<body>SEGREDO-CORPO<form action='/errors/validateCaptcha'>"
              b"</form></body></html>")
    respostas = [
        _Resposta(200, {"content-type": "application/json",
                        "x-amz-rid": "RID200"}, dados=JSON_OK),
        _Resposta(503, {"content-type": "text/html", "x-amz-rid": "RID503"},
                  corpo=pagina),
        _Resposta(302, {"location": "https://www.amazon.com.br/ap/signin?"
                                    "openid.return_to=SEGREDO-QUERY"}),
    ]
    saidas = []

    class _Sessao:
        def get(self, *a, **k):
            if not respostas:
                raise TimeoutError()
            return respostas.pop(0)

    async def obter():
        return _Sessao()

    async def corpo():
        amazon.config._SEM_HTTP = asyncio.Semaphore(1)
        try:
            for _ in range(4):
                try:
                    saidas.append(await _HTTP_REAL({"longUrl": longa}))
                except TimeoutError:
                    saidas.append("timeout")
                relogio.t += 37.0
        finally:
            amazon.config._SEM_HTTP = None

    original = amazon._obter_sessao
    amazon._obter_sessao = obter
    try:
        with _Captura() as cap:
            _rodar(corpo())
    finally:
        amazon._obter_sessao = original

    linhas = cap.com("[AMZ_SONDA]", "INFO")
    r.check(len(linhas) == 4, "uma linha por chamada, inclusive exceção",
            linhas)
    if len(linhas) != 4:
        return
    ok, bloqueio, login, excecao = linhas
    r.check("status=200" in ok and "intervalo=primeira" in ok
            and "ok=True/True" in ok and "rid=RID200" in ok, "sucesso", ok)
    r.check("status=503" in bloqueio and "intervalo=37s" in bloqueio
            and "marca=captcha" in bloqueio
            and 'titulo="Amazon.com.br"' in bloqueio
            and "rid=RID503" in bloqueio, "503 com marca e título",
            bloqueio)
    r.check("status=302" in login
            and "destino=www.amazon.com.br/ap/signin" in login,
            "302 com destino", login)
    r.check("status=EXC:TimeoutError" in excecao, "exceção registrada",
            excecao)
    r.check(all("alvo=produto params=4" in m and f"tam={len(longa)}" in m
                for m in linhas), "forma do alvo", linhas[0])
    vazou = [m for m in linhas if "SEGREDO" in m]
    r.check(not vazou, "nada de query, cookie ou corpo", vazou)
    r.check(saidas[1] == (503, "text/html", None, "captcha")
            and saidas[3] == "timeout", "contrato da costura intacto",
            saidas)


# ── 12 ────────────────────────────────────────────────────────────
def test_12_json_fora_do_contrato(r):
    lista = _costura(_Resposta(
        200, {"content-type": "application/json"}, dados=["inesperado"]))
    r.check(lista == (200, "application/json", None, ""),
            "lista vira dados=None", lista)
    rede = _Rede(lista)
    _cenario(rede)
    with _Captura():
        try:
            saida = _rodar(amazon._sitestripe(CANONICA))
            erro = None
        except Exception as e:                  # noqa: BLE001
            saida, erro = None, e
    r.check(erro is None and saida == "", "falha, sem exceção", repr(erro))


if __name__ == "__main__":
    sys.exit(harness.rodar(globals(), "AMAZON — sessão do SiteStripe"))
