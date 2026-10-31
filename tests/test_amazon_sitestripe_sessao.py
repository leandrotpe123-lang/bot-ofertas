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
      com a classe do destino; 72 ofertas em 6 h → UMA chamada; sondagem a cada
      6 h, sem repetir o alerta
  02  sondagem que volta a responder → short oficial, [AMZ_SESSAO_OK]
      e fim da pausa
  03  caso 29/10: 503 de captcha é transitória — nunca vira sessão
      recusada, e a política da main fica intacta: no limite, pausa
      fixa de 5 min (mudá-la muda quantas ofertas saem curtas; só com
      os dados da sonda)
  04  falha transitória em série longa continua com a pausa da main
  05  classificação: só 401 e 3xx para o LOGIN recusam a sessão; 3xx
      para destino desconhecido, sem destino ou para o captcha, 403,
      429 e 5xx são transitórias (recusa exige prova)
  06  a primeira resposta boa aparece no log uma vez, não a cada short
  07  o boot registra os NOMES dos cookies e nunca os valores
  08  costura de I/O: a pista é sempre rótulo fechado — nada da
      resposta (caminho, query, corpo) passa por ela
  09  ponta a ponta: sessão recusada nunca perde a oferta — publica a
      longa com a nossa tag, e só a nossa
  10  cookie malformado (Base64 que não decodifica, valor com `;`) não
      põe pedaço do segredo no log no lugar do nome: só nomes da lista
      fechada são impressos, o resto é contado
  11  SONDA PASSIVA: uma linha [AMZ_SONDA] por chamada — sucesso, 503,
      302 e exceção —, com intervalo, forma do alvo e classes fechadas
      de tipo, página, destino e ok
  12  JSON fora do contrato (lista) não derruba quem chama
  14  DE ONDE VEM O 503: página da borda (Server: CloudFront) × resposta
      do servidor da Amazon (Server: Server), com X-Cache, borda (POP)
      e tamanho da página — tudo em rótulos
  15  cookies que a Amazon renova: só os NOMES (lista fechada) e quantos
      vêm entre aspas; nenhum valor no log
  16  ADVERSARIAL nos campos novos: Server, X-Cache, POP e Set-Cookie
      hostis — só rótulos chegam ao log
  13  ADVERSARIAL: título com dado pessoal e escape ANSI, caminho com
      token, header e valores de JSON arbitrários, credencial no host —
      nada disso chega ao log; toda linha obedece ao CONTRATO DE FORMATO
      (cada campo é número ou rótulo de vocabulário fechado)

    python tests/test_amazon_sitestripe_sessao.py
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

# A costura REAL, guardada antes que os cenários a troquem pela falsa.
_HTTP_REAL = amazon._http_getshorturl

TAG = amazon._AMZ_TAG
CANONICA = f"https://www.amazon.com.br/dp/B0CK2MD3F7?tag={TAG}"
JSON_OK = {
    "ok": True, "isOk": True,
    "shortUrl": "https://link.amazon/B0abWqUsA",
    "longUrl": CANONICA,
}
LOGIN = (302, "", None, "login")
CAPTCHA_503 = (503, "text/html", None, "captcha")
SUCESSO = (200, "application/json", JSON_OK, "")


# CONTRATO DE FORMATO: cada campo das linhas novas é número ou rótulo
# de vocabulário fechado. Campo dinâmico novo quebra estes padrões.
_CONTRATO_SONDA = re.compile(
    r"🔬 \[AMZ_SONDA\] sitestripe "
    r"status=(\d{3}|EXC:[A-Za-z_][A-Za-z0-9_]*) ms=\d+ "
    r"intervalo=(primeira|\d+s) alvo=(produto|promo|pagina) "
    r"params=\d+ tam=\d+ tipo=(json|html|vazio|outro|-) "
    r"servidor=(server|cloudfront|outro|ausente|-) "
    r"borda=(hit|miss|refresh|erro|outra|ausente|-) "
    r"pop=([A-Z]{3}|-) corpo=(0|<1k|1-10k|10-64k|64k\+|-) "
    r"renova=(-|[a-z0-9-]+(,[a-z0-9-]+)*(\+\d+)?|\+\d+) aspas=\d+ "
    r"marca=(captcha|automacao|robo|login|cloudfront|algo_errado|"
    r"indisponivel|vazia|outra|-) "
    r"destino=(login|captcha|outro|ausente|-) "
    r"ok=((sim|nao|outro)/(sim|nao|outro)|-)"
)
_MOTIVO = (r"(HTTP \d{3} tipo=(json|html|vazio|outro)"
           r"( → (login|captcha|outro|ausente|automacao|robo|cloudfront|"
           r"algo_errado|indisponivel|vazia|outra))?"
           r"|ok=(sim|nao|outro) isOk=(sim|nao|outro)"
           r"|[A-Za-z_][A-Za-z0-9_]*"
           r"|shortUrl fora dos domínios oficiais"
           r"|longUrl de resposta sem a nossa tag)")
_CONTRATO_FALHAS = [
    re.compile(r"⚠️ AMZ SiteStripe falhou \(\d+/\d+\): " + _MOTIVO),
    re.compile(r"🔐 \[AMZ_SESSAO_RECUSADA\] SiteStripe recusou a sessão "
               r"\(" + _MOTIVO + r"\) — renovar AMAZON_COOKIE; publicando "
               r"URL longa, nova sondagem em \d+h"),
    re.compile(r"⚠️ AMZ sessão segue recusada \(" + _MOTIVO +
               r"\) — nova sondagem em \d+h"),
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
        r.check(alertas and "HTTP 302 tipo=vazio → login" in alertas[0],
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
        r.check(pausas == [0, 0, 300, 300, 300, 300, 300, 300, 300],
                "abre no limite; pausa fixa de 5 min, como na main", pausas)
        r.check(not cap.com("[AMZ_SESSAO_RECUSADA]"),
                "captcha nunca vira sessão recusada")
        r.check(not amazon._sessao_recusada, "estado não marca recusa")
        falhas = cap.com("AMZ SiteStripe falhou", "WARNING")
        r.check(falhas and all("captcha" in f for f in falhas),
                "o log separa captcha de login", falhas[:1])


# ── 04 ────────────────────────────────────────────────────────────
def test_04_transitoria_em_serie_longa_mantem_pausa_da_main(r):
    rede = _Rede(CAPTCHA_503)
    relogio = _cenario(rede)
    amazon._falhas_seguidas = 10_000
    with _Captura():
        try:
            _rodar(amazon._sitestripe(CANONICA))
            erro = None
        except Exception as e:                  # noqa: BLE001
            erro = e
    r.check(erro is None, "sem exceção", repr(erro))
    r.check(round(amazon._disjuntor_ate - relogio.t) == 300,
            "5 min, como na main", amazon._disjuntor_ate - relogio.t)


# ── 05 ────────────────────────────────────────────────────────────
def test_05_classificacao(r):
    recusa, trans = amazon._SESSAO_RECUSADA, amazon._TRANSITORIA
    casos = [
        (301, "login", recusa),
        (302, "login", recusa),
        (307, "login", recusa),
        (308, "login", recusa),
        (401, "", recusa),
        (302, "ausente", trans),
        (302, "outro", trans),
        (303, "outro", trans),
        (302, "", trans),
        (302, "captcha", trans),
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

    destinos = [
        ("https://www.amazon.com.br/ap/signin?openid=x", "login"),
        ("/ap/mfa/new-otp", "login"),
        ("https://www.amazon.com.br/gp/sign-in.html", "login"),
        ("https://www.amazon.com.br/errors/validateCaptcha?a=1", "captcha"),
        ("https://outro.exemplo/qualquer/coisa", "outro"),
        ("", "ausente"),
    ]
    for location, esperado in destinos:
        obtido = amazon._classe_destino(location)
        r.check(obtido == esperado, f"destino {location!r}", obtido)

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
    r.check(linha == "+1 fora da lista", "nada do blob no log", linha)
    r.check(bloco[:20] not in linha and bloco[30:50] not in linha,
            "nenhum trecho do segredo", linha)

    # Valor com `;` (fora da RFC, mas é o que um painel ou uma captura
    # defeituosa entrega): o pedaço depois do `;` tem FORMA de nome.
    os.environ["AMAZON_COOKIE"] = (
        'session-token="AAAA;FRAGMENTOSECRETO==; at-acbbr=VALOR'
    )
    try:
        cookies = amazon._cookies_iniciais()
    finally:
        os.environ.pop("AMAZON_COOKIE", None)
    linha = amazon._nomes_para_log(cookies)
    r.check("FRAGMENTOSECRETO" in cookies, "o fragmento chega como nome",
            sorted(cookies))
    r.check(linha == "at-acbbr, session-token, +1 fora da lista"
            and "FRAGMENTO" not in linha, "só nomes da lista fechada", linha)


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
    r.check(login == (302, "", None, "login"),
            "302: só a classe do destino", login)

    pagina = (b"<html><title>Amazon.com.br</title>" + b"x" * 500 +
              b'<form action="/errors/validateCaptcha">SEGREDO</form>')
    captcha = _costura(_Resposta(
        503, {"content-type": "text/html"}, corpo=pagina))
    r.check(captcha == (503, "text/html", None, "captcha"),
            "503: reconhece o captcha mesmo lido em blocos", captcha)

    outra = _costura(_Resposta(
        503, {"content-type": "text/html"}, corpo=b"<html>erro</html>"))
    r.check(outra == (503, "text/html", None, "outra"), "503 sem marca",
            outra)
    vazia = _costura(_Resposta(503, {"content-type": "text/html"}))
    r.check(vazia == (503, "text/html", None, "vazia"), "503 sem corpo",
            vazia)

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
            and "tipo=json" in ok and "ok=sim/sim" in ok, "sucesso", ok)
    r.check("status=503" in bloqueio and "intervalo=37s" in bloqueio
            and "tipo=html" in bloqueio and "marca=captcha" in bloqueio,
            "503 com classe da página", bloqueio)
    r.check("status=302" in login and "tipo=vazio" in login
            and "destino=login" in login, "302 com classe do destino",
            login)
    r.check(all(_CONTRATO_SONDA.fullmatch(m) for m in linhas),
            "contrato de formato", [m for m in linhas
                                    if not _CONTRATO_SONDA.fullmatch(m)])
    r.check("status=EXC:TimeoutError" in excecao, "exceção registrada",
            excecao)
    r.check(all("alvo=produto params=4" in m and f"tam={len(longa)}" in m
                for m in linhas), "forma do alvo", linhas[0])
    vazou = [m for m in linhas if any(x in m for x in
                                       ("SEGREDO", "RID", "Amazon.com.br"))]
    r.check(not vazou, "nada de query, header, título ou corpo", vazou)
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


# ── 13 ────────────────────────────────────────────────────────────
def test_13_adversarial_nada_da_resposta_chega_ao_log(r):
    """Conteúdo de terceiro hostil em cada campo que a resposta tem."""
    relogio = _Relogio11()
    amazon.time = types.SimpleNamespace(monotonic=relogio.monotonic)
    amazon._ultima_chamada = 0.0
    hostil = ("Olá, Leandro Fulano — leandro@exemplo.com \x1b[31mVERMELHO"
              "\x1b[0m \u202eTXET")
    pagina = (f"<html><title>{hostil}</title><body>CORPO-SECRETO"
              f"</body></html>").encode()
    respostas = [
        # título com dado pessoal, escape ANSI e bidi; header arbitrário
        _Resposta(503, {"content-type": "text/html; X-HOSTIL=1"},
                  corpo=pagina),
        # 200 HTML com o mesmo título (página logada no lugar do JSON)
        _Resposta(200, {"content-type": "text/html"}, corpo=pagina),
        # redirecionamento com token no CAMINHO e credencial no host
        _Resposta(302, {"location": "https://usuario:SENHA@www.amazon.com.br"
                                    "/ap/signin/TOKEN-NO-CAMINHO?q=QUERY"}),
        _Resposta(302, {"location": "https://www.amazon.com.br/x/"
                                    "TOKEN-NO-CAMINHO"}),
        # valores de JSON arbitrários, e content-type estranho
        _Resposta(200, {"content-type": "application/json; x=HEADER"},
                  dados={"ok": "VALOR-JSON-ARBITRARIO", "isOk": 1,
                         "shortUrl": "https://link.amazon/x"}),
    ]

    class _Sessao:
        def get(self, *a, **k):
            return respostas.pop(0)

    async def obter():
        return _Sessao()

    async def corpo():
        amazon.config._SEM_HTTP = asyncio.Semaphore(1)
        try:
            for _ in range(5):
                await amazon._sitestripe(CANONICA)
                relogio.t = max(relogio.t + 1.0, amazon._disjuntor_ate)
        finally:
            amazon.config._SEM_HTTP = None

    rede = _HTTP_REAL
    _cenario(rede)
    amazon.time = types.SimpleNamespace(monotonic=relogio.monotonic)
    original = amazon._obter_sessao
    amazon._obter_sessao = obter
    try:
        with _Captura() as cap:
            _rodar(corpo())
    finally:
        amazon._obter_sessao = original

    todas = [m for _, m in cap.linhas]
    proibidos = ("Leandro", "leandro@", "\x1b", "\u202e", "VERMELHO",
                 "CORPO-SECRETO", "SENHA", "usuario", "TOKEN", "QUERY",
                 "X-HOSTIL", "HEADER", "VALOR-JSON", "text/html",
                 "application/json")
    vazou = [m for m in todas if any(x in m for x in proibidos)]
    r.check(not vazou, "nenhum conteúdo da resposta no log", vazou)

    sondas = [m for m in todas if "[AMZ_SONDA]" in m]
    r.check(len(sondas) == 5, "uma sonda por chamada", len(sondas))
    fora = [m for m in sondas if not _CONTRATO_SONDA.fullmatch(m)]
    r.check(not fora, "sonda obedece ao contrato de formato", fora)

    falhas = [m for m in todas if "AMZ SiteStripe falhou" in m
              or "SESSAO_RECUSADA" in m or "segue recusada" in m]
    fora = [m for m in falhas
            if not any(c.fullmatch(m) for c in _CONTRATO_FALHAS)]
    r.check(falhas and not fora, "falhas obedecem ao contrato", fora)
    r.check(any("ok=outro isOk=outro" in m for m in falhas),
            "valor de JSON vira classe", falhas)
    r.check(any("→ login" in m for m in falhas)
            and any("→ outro" in m for m in falhas),
            "destino vira classe", falhas)


# ── 14 / 15 / 16 ──────────────────────────────────────────────────
class _Cabecalhos(dict):
    """Cabeçalhos com vários Set-Cookie, como o aiohttp entrega."""

    def __init__(self, simples, set_cookies=()):
        super().__init__({k.lower(): v for k, v in simples.items()})
        self.set_cookies = list(set_cookies)

    def get(self, chave, padrao=None):
        return super().get(chave.lower(), padrao)

    def getall(self, chave, padrao=None):
        if chave.lower() == "set-cookie":
            return list(self.set_cookies)
        return padrao if padrao is not None else []


def _uma_sonda(resposta):
    amazon._ultima_chamada = 0.0
    with _Captura() as cap:
        _costura(resposta)
    return [m for _, m in cap.linhas if "[AMZ_SONDA]" in m]


def test_14_503_da_borda_x_503_do_servidor(r):
    pagina_borda = (b"<html><h1>503 ERROR</h1><h2>The request could not be "
                    b"satisfied.</h2>" + b"x" * 1500 + b"Generated by "
                    b"cloudfront (CloudFront)</html>")
    borda = _uma_sonda(_Resposta(503, _Cabecalhos({
        "content-type": "text/html", "server": "CloudFront",
        "x-cache": "Error from cloudfront", "x-amz-cf-pop": "SFO53-P1",
    }), corpo=pagina_borda))
    r.check(len(borda) == 1 and "servidor=cloudfront borda=erro pop=SFO "
            "corpo=1-10k" in borda[0] and "marca=cloudfront" in borda[0],
            "503 gerado pela borda", borda)

    servidor = _uma_sonda(_Resposta(503, _Cabecalhos({
        "content-type": "text/html;charset=UTF-8", "server": "Server",
        "x-cache": "Error from cloudfront", "x-amz-cf-pop": "GRU1-P7",
    }), corpo=b"<html>Desculpe! Algo deu errado!</html>"))
    r.check(len(servidor) == 1 and "servidor=server borda=erro pop=GRU "
            "corpo=<1k" in servidor[0] and "marca=algo_errado" in servidor[0],
            "503 vindo do servidor da Amazon", servidor)

    ok = _uma_sonda(_Resposta(200, _Cabecalhos({
        "content-type": "application/json;charset=UTF-8", "server": "Server",
        "x-cache": "Miss from cloudfront", "x-amz-cf-pop": "GRU1-P7",
    }), dados=JSON_OK))
    r.check(len(ok) == 1 and "servidor=server borda=miss pop=GRU corpo=-"
            in ok[0] and "ok=sim/sim" in ok[0], "200 como na captura", ok)
    r.check(all(_CONTRATO_SONDA.fullmatch(m) for m in borda + servidor + ok),
            "contrato de formato", borda + servidor + ok)


def test_15_renovados_so_nomes_e_aspas(r):
    linhas = _uma_sonda(_Resposta(200, _Cabecalhos(
        {"content-type": "application/json", "server": "Server",
         "x-cache": "Miss from cloudfront"},
        ["session-id=000-0000000-SEGREDO1; Path=/; Domain=.amazon.com.br",
         'session-token="SEGREDO2+abc/def=="; Path=/; Secure; HttpOnly',
         "nome-desconhecido=SEGREDO3; Path=/"]), dados=JSON_OK))
    r.check(len(linhas) == 1 and "renova=session-id,session-token+1 aspas=1"
            in linhas[0], "nomes conhecidos, desconhecidos contados, aspas",
            linhas)
    r.check(not any("SEGREDO" in m or "desconhecido" in m for m in linhas),
            "nenhum valor nem nome desconhecido no log", linhas)
    r.check(all(_CONTRATO_SONDA.fullmatch(m) for m in linhas),
            "contrato de formato", linhas)


def test_16_adversarial_campos_novos(r):
    hostis = _uma_sonda(_Resposta(503, _Cabecalhos({
        "content-type": "text/html",
        "server": "CloudFront\x1b[31m TOKEN",
        "x-cache": "Weird from somewhere TOKEN",
        "x-amz-cf-pop": "sfo53-TOKEN",
    }, ["x\x1b[0m=TOKEN", '"aspas"=TOKEN', "=semnome"]),
        corpo=b"TOKEN" * 50))
    r.check(len(hostis) == 1 and "servidor=outro borda=outra pop=- "
            "corpo=<1k renova=+3 aspas=0" in hostis[0],
            "tudo vira rótulo", hostis)
    r.check(not any(x in m for m in hostis
                    for x in ("TOKEN", "\x1b", "sfo", "semnome")),
            "nada dos cabeçalhos hostis no log", hostis)
    r.check(all(_CONTRATO_SONDA.fullmatch(m) for m in hostis),
            "contrato de formato", hostis)


if __name__ == "__main__":
    sys.exit(harness.rodar(globals(), "AMAZON — sessão do SiteStripe"))
