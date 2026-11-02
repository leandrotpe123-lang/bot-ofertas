"""
AMAZON — o cache guarda e reutiliza SOMENTE short oficial validado.

Regra de negócio (02/11): quando a Amazon não gera o short, a URL longa
afiliada é publicada na hora, mas NÃO é gravada como resultado de
conversão. Caso real (01/11 → 02/11, 98 publicações Amazon): 11 saíram
longas SEM chamar o SiteStripe — a falha de dias antes tinha gravado a
longa sob a URL de origem, e a etapa de cache da normalização a servia
por 30 dias, renovados a cada leitura (db_get_link renova o ts).
B0FG1PV5C3: os links do @fumotom e do @promotom saíram do cache sem
chamada; o do @samuelf3lipepromo, nunca visto, chamou.

Todo cenário passa pelo caminho REAL — etapa de cache do core
(normalizacao_links._normalizar_um) e depois amazon.afilia — com SQLite
de verdade; só a fronteira de rede do SiteStripe é falsa.

  a  SiteStripe 503: a oferta recebe a longa afiliada (tag trocada,
     parâmetros preservados) e NADA é gravado
  b  a mesma URL volta: tenta o SiteStripe de novo (reprodução do caso
     real — na main, a 2ª passagem devolvia a longa sem chamar)
  c  registro legado longo no SQLite (sob a origem e sob a identidade)
     é invalidado e REMOVIDO ao ser encontrado, mesmo com ts antigo
  d  SiteStripe 200: short gravado na identidade e na URL de origem
  e  outra fonte, mesmo ASIN: short reaproveitado sem nova chamada
  f  cache antigo com short oficial e a nossa tag continua valendo
  g  short com tag estrangeira não é reutilizado (e é removido)
  h  concorrência no mesmo ASIN: nunca duas chamadas simultâneas
  i  nenhum fallback longo vira url_conv — produto, busca, promoção,
     amzn.to expandido e caminho sem afiliação
  j  url_canon longa legítima preservada quando pertence a short válido
  k  afiliacao_vigente: tabela de casos e declaração no PLATAFORMA
  l  o caminho de falha não espera: nenhuma pausa nas funções do fluxo

    python tests/test_amazon_cache_so_short.py
"""
import ast
import asyncio
import inspect
import logging
import os
import sys
import time

os.environ.setdefault("AMAZON_TAG", "fullpromotion-20")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _harness_e5 as harness                                   # noqa: E402

harness.preparar()

from database import _init_db                                   # noqa: E402

_init_db()

import config                                                   # noqa: E402
import globals as g                                             # noqa: E402
import plataformas                                              # noqa: E402

plataformas.inicializar()

from database_conexao import _db                                # noqa: E402
from pipeline import normalizacao_links as nl                   # noqa: E402
from plataformas import amazon, registry                        # noqa: E402
from plataformas.contrato import Afiliacao                      # noqa: E402
from utils.cache_links import consultar_link, registrar_link    # noqa: E402
from utils.urls import _cache_key                               # noqa: E402

TAG = amazon._AMZ_TAG
ASIN = "B0FG1PV5C3"
FONTE_A = f"https://www.amazon.com.br/dp/{ASIN}?tag=fonte-20"
FONTE_B = (f"https://www.amazon.com.br/dp/{ASIN}"
           f"?th=1&psc=1&smid=A1ZZFT5FULY4LN&tag=outrogrupo-20")
LONGA_A = f"https://www.amazon.com.br/dp/{ASIN}?tag={TAG}"
LONGA_B = (f"https://www.amazon.com.br/dp/{ASIN}"
           f"?th=1&psc=1&smid=A1ZZFT5FULY4LN&tag={TAG}")
CHAVE = f"https://www.amazon.com.br/dp/{ASIN}?tag={TAG}"
SHORT = "https://link.amazon/B0curtoOK"
ERRO_503 = (503, "text/html;charset=UTF-8", None, "algo_errado")


def _ok(canonica, short=SHORT):
    return (200, "application/json",
            {"ok": True, "isOk": True, "shortUrl": short,
             "longUrl": canonica}, "")


# ── infraestrutura ────────────────────────────────────────────────
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


class _Rede:
    """Fronteira falsa do SiteStripe: roteiro, contagem e simultaneidade."""
    def __init__(self, *roteiro, demora=0.0):
        self.roteiro, self.demora = list(roteiro), demora
        self.chamadas, self.no_ar, self.pico = [], 0, 0

    async def __call__(self, params):
        self.chamadas.append(params["longUrl"])
        self.no_ar += 1
        self.pico = max(self.pico, self.no_ar)
        try:
            if self.demora:
                await asyncio.sleep(self.demora)
            return self.roteiro.pop(0) if len(self.roteiro) > 1 \
                else self.roteiro[0]
        finally:
            self.no_ar -= 1


def _limpar():
    with g._cache_lock:
        g._final_cache.clear()
        g._raw_cache.clear()
    with _db() as db:
        db.execute("DELETE FROM links_cache")
    amazon._falhas_seguidas = 0
    amazon._disjuntor_ate = 0.0
    amazon._sessao_recusada = False
    amazon._ja_respondeu = True
    config._SEM_HTTP = asyncio.Semaphore(20)


def _so_memoria_limpa():
    with g._cache_lock:
        g._final_cache.clear()


def _linha(url):
    with _db() as db:
        return db.execute(
            "SELECT url_conv, url_canon, ts FROM links_cache WHERE url_orig=?",
            (_cache_key(url),)).fetchone()


def _linhas_amazon():
    with _db() as db:
        return db.execute(
            "SELECT url_orig, url_conv, url_canon FROM links_cache "
            "WHERE plat='amazon'").fetchall()


def _envelhecer(url, dias=20):
    with _db() as db:
        db.execute("UPDATE links_cache SET ts=? WHERE url_orig=?",
                   (time.time() - dias * 86400, _cache_key(url)))


def _publicada(conv):
    return conv.publicada if isinstance(conv, Afiliacao) else conv


def _norm(url, rede):
    amazon._http_getshorturl = rede
    return asyncio.run(nl._normalizar_um(url, None))[1]


# ── a ─────────────────────────────────────────────────────────────
def test_a_503_publica_longa_e_nao_grava(r):
    _limpar()
    rede = _Rede(ERRO_503)
    conv = _norm(FONTE_B, rede)
    r.check(conv == LONGA_B, "longa afiliada: tag trocada, th/psc/smid "
            "preservados, nada inventado", conv)
    r.check(len(rede.chamadas) == 1, "uma chamada", rede.chamadas)
    r.check(_linhas_amazon() == [], "nada gravado no SQLite",
            _linhas_amazon())
    r.check(consultar_link(FONTE_B) is None and consultar_link(CHAVE) is None,
            "nada em memória nem no banco, sob a origem ou a identidade")


# ── b ─────────────────────────────────────────────────────────────
def test_b_mesma_url_tenta_de_novo(r):
    _limpar()
    rede = _Rede(ERRO_503, _ok(LONGA_A))
    primeira = _norm(FONTE_A, rede)
    segunda = _norm(FONTE_A, rede)
    r.check(primeira == LONGA_A, "1ª: 503 → longa", primeira)
    r.check(len(rede.chamadas) == 2, "2ª passagem chamou o SiteStripe de "
            "novo (na main: 1 chamada só, longa do cache)", rede.chamadas)
    r.check(_publicada(segunda) == SHORT, "2ª: short oficial", segunda)


# ── c ─────────────────────────────────────────────────────────────
def test_c_legado_longo_e_removido(r):
    for papel, chave_legado in (("origem", FONTE_A), ("identidade", CHAVE)):
        _limpar()
        # Como a versão anterior gravava: a longa como url_conv.
        registrar_link(chave_legado, LONGA_A, "amazon")
        _envelhecer(chave_legado)
        _so_memoria_limpa()
        r.check(_linha(chave_legado) is not None, f"{papel}: legado no SQLite")
        rede = _Rede(ERRO_503)
        with _Captura() as cap:
            conv = _norm(FONTE_A, rede)
        r.check(len(rede.chamadas) == 1, f"{papel}: legado não bloqueou a "
                "tentativa", rede.chamadas)
        r.check(conv == LONGA_A, f"{papel}: publica a longa", conv)
        r.check(_linha(chave_legado) is None, f"{papel}: linha REMOVIDA do "
                "SQLite (não só ignorada — a leitura renovaria o ts)",
                _linha(chave_legado))
        r.check(consultar_link(chave_legado) is None,
                f"{papel}: nada em memória")
        r.check(any(f"[AMZ_CACHE_DESCARTADO] papel={papel}" in m
                    for m in cap.linhas), f"{papel}: descarte no log",
                cap.linhas)
        r.check(not any("http" in m for m in cap.linhas
                        if "AMZ_CACHE_DESCARTADO" in m),
                f"{papel}: nenhuma URL na linha de descarte")

    # Legado + Amazon respondendo: a linha velha vira o short.
    _limpar()
    registrar_link(FONTE_A, LONGA_A, "amazon")
    _envelhecer(FONTE_A)
    _so_memoria_limpa()
    conv = _norm(FONTE_A, _Rede(_ok(LONGA_A)))
    r.check(_publicada(conv) == SHORT and _linha(FONTE_A)[0] == SHORT,
            "legado substituído pelo short", _linha(FONTE_A))


# ── d ─────────────────────────────────────────────────────────────
def test_d_200_grava_identidade_e_origem(r):
    _limpar()
    rede = _Rede(_ok(LONGA_B))
    conv = _norm(FONTE_B, rede)
    r.check(isinstance(conv, Afiliacao) and conv.publicada == SHORT
            and conv.canonica == LONGA_B, "Afiliacao(short, canônica)", conv)
    r.check(_linha(CHAVE)[:2] == (SHORT, LONGA_B), "short sob a identidade",
            _linha(CHAVE))
    r.check(_linha(FONTE_B)[:2] == (SHORT, LONGA_B),
            "short associado à URL de origem", _linha(FONTE_B))


# ── e ─────────────────────────────────────────────────────────────
def test_e_outra_fonte_reaproveita(r):
    _limpar()
    rede = _Rede(_ok(LONGA_B))
    _norm(FONTE_B, rede)
    conv = _norm(FONTE_A, rede)
    r.check(len(rede.chamadas) == 1, "outra fonte, mesmo ASIN: sem nova "
            "chamada", rede.chamadas)
    r.check(_publicada(conv) == SHORT, "o mesmo short", conv)
    r.check(_linha(FONTE_A)[0] == SHORT, "associação da nova origem ao short",
            _linha(FONTE_A))
    _so_memoria_limpa()
    conv = _norm(FONTE_A, rede)
    r.check(len(rede.chamadas) == 1 and _publicada(conv) == SHORT,
            "do SQLite, depois de reiniciar a memória: sem chamada")


# ── f ─────────────────────────────────────────────────────────────
def test_f_short_antigo_com_nossa_tag_vale(r):
    _limpar()
    registrar_link(FONTE_A, Afiliacao(publicada=SHORT, canonica=LONGA_A),
                   "amazon")
    _envelhecer(FONTE_A, dias=25)
    _so_memoria_limpa()
    rede = _Rede(ERRO_503)
    conv = _norm(FONTE_A, rede)
    r.check(_publicada(conv) == SHORT and not rede.chamadas,
            "servido do cache, sem chamada", (conv, rede.chamadas))
    r.check(_linha(FONTE_A) is not None, "linha preservada")
    for curto in ("https://amzn.to/3abc", "https://a.co/d/xyz"):
        _limpar()
        registrar_link(FONTE_A, Afiliacao(publicada=curto, canonica=LONGA_A),
                       "amazon")
        _so_memoria_limpa()
        conv = _norm(FONTE_A, _Rede(ERRO_503))
        r.check(_publicada(conv) == curto, f"domínio oficial {curto[8:15]}",
                conv)


# ── g ─────────────────────────────────────────────────────────────
def test_g_tag_estrangeira_nao_reaproveita(r):
    estrangeiro = Afiliacao(
        publicada="https://link.amazon/DeOutroAfiliado",
        canonica=f"https://www.amazon.com.br/dp/{ASIN}?tag=outro-20")
    duas_tags = Afiliacao(
        publicada="https://link.amazon/DuasTags",
        canonica=f"https://www.amazon.com.br/dp/{ASIN}?tag={TAG}&tag=outro-20")
    for rot, ruim in (("tag estrangeira", estrangeiro), ("duas tags", duas_tags)):
        for papel, chave_ruim in (("origem", FONTE_A), ("identidade", CHAVE)):
            _limpar()
            registrar_link(chave_ruim, ruim, "amazon")
            _so_memoria_limpa()
            rede = _Rede(_ok(LONGA_A))
            conv = _norm(FONTE_A, rede)
            r.check(_publicada(conv) == SHORT and len(rede.chamadas) == 1,
                    f"{rot}/{papel}: não reutiliza; chama e publica o nosso",
                    (conv, rede.chamadas))
            linha = _linha(chave_ruim)
            r.check(linha is None or linha[0] == SHORT,
                    f"{rot}/{papel}: entrada estrangeira não sobrevive", linha)


# ── h ─────────────────────────────────────────────────────────────
def test_h_concorrencia_mesmo_asin(r):
    async def duas(rede):
        amazon._http_getshorturl = rede
        return await asyncio.gather(nl._normalizar_um(FONTE_A, None),
                                    nl._normalizar_um(FONTE_B, None))

    _limpar()
    rede = _Rede(_ok(LONGA_A), demora=0.05)
    a, b = asyncio.run(duas(rede))
    r.check(len(rede.chamadas) == 1, "sucesso: UMA chamada para as duas",
            rede.chamadas)
    r.check(_publicada(a[1]) == SHORT and _publicada(b[1]) == SHORT,
            "as duas recebem o short", (a[1], b[1]))

    _limpar()
    rede = _Rede(ERRO_503, demora=0.05)
    asyncio.run(duas(rede))
    r.check(rede.pico == 1, "falha: nunca duas chamadas simultâneas",
            rede.pico)
    r.check(len(rede.chamadas) == 2, "falha: a segunda tenta depois — a "
            "falha da primeira não a impede", rede.chamadas)


# ── i ─────────────────────────────────────────────────────────────
def test_i_nenhum_fallback_longo_vira_url_conv(r):
    _limpar()
    original = amazon.desencurtar

    async def expandir(url, sessao):
        return FONTE_B

    amazon.desencurtar = expandir
    try:
        casos = [
            ("produto 503", FONTE_A, True, LONGA_A),
            ("busca 503", "https://www.amazon.com.br/s?k=fone&tag=fonte-20",
             True, f"https://www.amazon.com.br/s?k=fone&tag={TAG}"),
            ("promoção 503",
             "https://www.amazon.com.br/promotion/psp/AMAFK189VQCOC?tag=x-20",
             True, "https://www.amazon.com.br/promotion/psp/AMAFK189VQCOC"
                   f"?tag={TAG}"),
            ("amzn.to expandido 503", "https://amzn.to/3fonteX", True, LONGA_B),
            # Caminho sem afiliação: sai limpo, sem tag — como sempre saiu.
            ("caminho sem afiliação",
             "https://www.amazon.com.br/gp/cart/view.html?tag=fonte-20", False,
             "https://www.amazon.com.br/gp/cart/view.html"),
        ]
        for rot, url, chama, esperada in casos:
            amazon._falhas_seguidas = 0
            rede = _Rede(ERRO_503)
            conv = _norm(url, rede)
            r.check(conv == esperada, f"{rot}: publica a forma de sempre",
                    conv)
            r.check(bool(rede.chamadas) == chama,
                    f"{rot}: SiteStripe {'chamado' if chama else 'não'}",
                    rede.chamadas)
            r.check(consultar_link(url) is None, f"{rot}: nada em cache")
    finally:
        amazon.desencurtar = original
    r.check(_linhas_amazon() == [], "nenhuma linha Amazon no SQLite",
            _linhas_amazon())

    # E quando há sucesso, toda linha gravada é short oficial vigente.
    _norm(FONTE_B, _Rede(_ok(LONGA_B)))
    linhas = _linhas_amazon()
    r.check(linhas and all(
        amazon.afiliacao_vigente(Afiliacao(publicada=c, canonica=k))
        for _, c, k in linhas), "toda url_conv gravada é short vigente",
        linhas)


# ── j ─────────────────────────────────────────────────────────────
def test_j_canonica_longa_legitima_preservada(r):
    _limpar()
    _norm(FONTE_B, _Rede(_ok(LONGA_B)))
    _so_memoria_limpa()
    guardado = consultar_link(FONTE_B)
    r.check(isinstance(guardado, Afiliacao) and guardado.publicada == SHORT
            and guardado.canonica == LONGA_B,
            "url_canon longa (th, psc, smid) preservada com o short",
            guardado)
    conv = _norm(FONTE_B, _Rede(ERRO_503))
    r.check(isinstance(conv, Afiliacao) and conv.canonica == LONGA_B,
            "servida com a canônica longa intacta", conv)


# ── k ─────────────────────────────────────────────────────────────
def test_k_afiliacao_vigente(r):
    v = amazon.afiliacao_vigente
    casos = [
        ("short link.amazon + nossa tag", Afiliacao(SHORT, LONGA_A), True),
        ("short amzn.to + nossa tag", Afiliacao("https://amzn.to/x", LONGA_B),
         True),
        ("short a.co + nossa tag", Afiliacao("https://a.co/d/x", LONGA_A),
         True),
        ("longa como publicada", Afiliacao(LONGA_A, LONGA_A), False),
        ("longa em amazon.com", Afiliacao(
            f"https://www.amazon.com/dp/{ASIN}?tag={TAG}", LONGA_A), False),
        ("short + tag estrangeira", Afiliacao(
            SHORT, f"https://www.amazon.com.br/dp/{ASIN}?tag=outro-20"), False),
        ("short + sem tag", Afiliacao(
            SHORT, f"https://www.amazon.com.br/dp/{ASIN}"), False),
        ("short + duas tags", Afiliacao(
            SHORT, f"{LONGA_A}&tag={TAG}"), False),
        ("short + canônica fora da Amazon", Afiliacao(
            SHORT, f"https://exemplo.com/dp/{ASIN}?tag={TAG}"), False),
        ("short + canônica vazia", Afiliacao(SHORT, ""), False),
        ("encurtador falso", Afiliacao(
            "https://link.amazon.golpe.com/x", LONGA_A), False),
        ("str solta", LONGA_A, False),
        ("None", None, False),
    ]
    for rot, entrada, esperado in casos:
        r.check(v(entrada) is esperado, rot, v(entrada))
    plat = registry.resolver(FONTE_A)
    r.check(plat is not None and plat.afiliacao_vigente is v,
            "declarada no PLATAFORMA, vista pelo core", plat)
    r.check(nl._entrada_vigente(plat, Afiliacao(LONGA_A, LONGA_A)) is False
            and nl._entrada_vigente(plat, Afiliacao(SHORT, LONGA_A)) is True,
            "a etapa de cache do core consulta a regra da Amazon")


# ── l ─────────────────────────────────────────────────────────────
def test_l_falha_nao_espera(r):
    funcoes = (amazon.afilia, amazon._encurtar_identidade, amazon._sitestripe,
               amazon._cache_vigente, amazon.afiliacao_vigente)
    pausas = []
    for f in funcoes:
        arvore = ast.parse(inspect.getsource(f))
        for n in ast.walk(arvore):
            if isinstance(n, ast.Call) and getattr(n.func, "attr", "") in (
                    "sleep", "_dormir"):
                pausas.append(f.__name__)
    r.check(not pausas, "nenhuma pausa no fluxo de afiliação", pausas)


if __name__ == "__main__":
    sys.exit(harness.rodar(globals(), "AMAZON — cache só com short oficial"))
