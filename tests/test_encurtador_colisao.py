"""
ENCURTADOR — o código publicado SEMPRE aponta para a URL certa (P2-6).

O código é sha256(url)[:7] (28 bits): colisão entre duas URLs afiliadas
diferentes acontece na casa das ~20 mil URLs (birthday) — e short_links
nunca é podada. db_set_short fazia INSERT OR IGNORE em silêncio: na
colisão o código seguia apontando para a URL ANTIGA e o bot publicava
esse código para a oferta NOVA — o link levava a outra oferta. No mesmo
caminho, uma falha de banco era engolida e registrar_codigo devolvia
True: o bot publicava um código que nunca foi gravado (404).

Correção: db_set_short devolve a URL que o código REALMENTE aponta
depois da gravação (sem sobrescrever — links já publicados continuam
valendo); registrar_codigo só confirma quando é a URL pedida; encurtar,
na colisão, deriva o próximo código da mesma URL (semente + NUL + n,
determinístico) e, esgotadas as tentativas ou com o banco falhando,
escapa para o link longo (contrato de sempre). Tamanho 7 mantido.

  01  colisão forçada: código já ocupado por outra URL → código novo,
      que aponta para a URL certa; o antigo continua intacto
  02  colisão NATURAL (par real sha256[:7] achado por força bruta):
      as duas ofertas recebem códigos diferentes, cada um no seu destino
  03  idempotência: mesma URL → mesmo código, inclusive o salgado; URL
      sem colisão mantém exatamente o código de antes (sha256[:7])
  04  falha de banco → escape para o link longo (nunca código sem registro)
  05  todas as tentativas ocupadas → escape para o link longo
  06  tamanho do código continua 7; log nunca carrega a URL

    python tests/test_encurtador_colisao.py
"""
import hashlib
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_cupom_sem_codigo import _zerar_banco                   # noqa: E402
from _harness_e5 import rodar                                   # noqa: E402
import database_links                                           # noqa: E402
from database_links import db_set_short                         # noqa: E402
from utils import encurtador, links_curtos                      # noqa: E402
from utils.links_curtos import consultar_codigo                 # noqa: E402

BASE = encurtador.SHORT_BASE_URL


def sha7(url):
    return hashlib.sha256(url.encode()).hexdigest()[:7]


def codigo_de(url_curta):
    assert url_curta.startswith(BASE + "/"), url_curta
    return url_curta[len(BASE) + 1:]


def afiliada(i):
    """Forma real de URL afiliada longa (Amazon /dp/ com a nossa tag)."""
    return f"https://www.amazon.com.br/dp/B0{i:08d}?tag=fullpromotion-20&th=1"


def _par_que_colide():
    vistos = {}
    i = 0
    while True:
        u = afiliada(i)
        c = sha7(u)
        if c in vistos:
            return vistos[c], u
        vistos[c] = u
        i += 1


class _Log(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas = []

    def emit(self, record):
        self.linhas.append(record.getMessage())


def test_01_colisao_forcada(r):
    _zerar_banco()
    a, b = afiliada(901), "https://shopee.com.br/outra-oferta-que-ja-tinha-o-codigo"
    c0 = sha7(a)
    db_set_short(c0, b)                                     # código já é de B
    curta = encurtador.encurtar(a)
    c = codigo_de(curta) if curta != a else None
    r.check(c is not None and c != c0, "01.codigo_novo", f"{curta}")
    r.check(c is not None and consultar_codigo(c) == a, "01.novo_aponta_para_A",
            str(c and consultar_codigo(c)))
    r.check(consultar_codigo(c0) == b, "01.antigo_intacto", str(consultar_codigo(c0)))


def test_02_colisao_natural(r):
    _zerar_banco()
    u1, u2 = _par_que_colide()
    r.check(u1 != u2 and sha7(u1) == sha7(u2), "02.par_real", f"{u1} {u2} {sha7(u1)}")
    s1, s2 = encurtador.encurtar(u1), encurtador.encurtar(u2)
    r.check(s1 != s2, "02.codigos_diferentes", f"{s1} {s2}")
    r.check(consultar_codigo(codigo_de(s1)) == u1 and consultar_codigo(codigo_de(s2)) == u2,
            "02.cada_um_no_seu_destino",
            f"{consultar_codigo(codigo_de(s1))} | {consultar_codigo(codigo_de(s2))}")
    r.check(codigo_de(s1) == sha7(u1), "02.primeiro_mantem_o_codigo_de_sempre")


def test_03_idempotencia_e_compatibilidade(r):
    _zerar_banco()
    u1, u2 = _par_que_colide()
    a = afiliada(777)
    r.check(encurtador.encurtar(a) == f"{BASE}/{sha7(a)}", "03.sem_colisao_codigo_de_sempre")
    r.check(encurtador.encurtar(a) == encurtador.encurtar(a), "03.mesma_url_mesmo_codigo")
    s1, s2 = encurtador.encurtar(u1), encurtador.encurtar(u2)
    r.check(encurtador.encurtar(u2) == s2 and encurtador.encurtar(u1) == s1,
            "03.salgado_tambem_e_estavel", f"{s2} {encurtador.encurtar(u2)}")


def test_04_falha_de_banco_escapa_para_o_longo(r):
    _zerar_banco()
    a = afiliada(4040)
    real_db = database_links._db

    class _Quebrado:
        def __enter__(self):
            raise RuntimeError("disk I/O error")

        def __exit__(self, *e):
            return False
    database_links._db = lambda: _Quebrado()
    try:
        saida = encurtador.encurtar(a)
    finally:
        database_links._db = real_db
    r.check(saida == a, "04.escape_longo", saida)
    r.check(consultar_codigo(sha7(a)) is None, "04.nada_publicado_sem_registro")


def test_05_tentativas_esgotadas_escapam(r):
    _zerar_banco()
    a = afiliada(5050)
    for n in range(encurtador._MAX_TENTATIVAS_CODIGO):
        db_set_short(encurtador._derivar_codigo(a, n), f"https://outra/{n}")
    r.check(encurtador.encurtar(a) == a, "05.escape_longo")
    r.check(encurtador._derivar_codigo(a, 0) == sha7(a), "05.tentativa_0_e_o_codigo_de_sempre")
    r.check(len({encurtador._derivar_codigo(a, n)
                 for n in range(encurtador._MAX_TENTATIVAS_CODIGO)})
            == encurtador._MAX_TENTATIVAS_CODIGO, "05.sementes_distintas")


def test_06_tamanho_e_log_sem_url(r):
    _zerar_banco()
    u1, u2 = _par_que_colide()
    cap = _Log()
    lg = logging.getLogger("SISTEMA")
    lg.addHandler(cap)
    try:
        s1, s2 = encurtador.encurtar(u1), encurtador.encurtar(u2)
    finally:
        lg.removeHandler(cap)
    r.check(encurtador._TAMANHO_CODIGO == 7 and len(codigo_de(s1)) == 7
            and len(codigo_de(s2)) == 7, "06.tamanho_7")
    r.check(not any(u1 in x or u2 in x for x in cap.linhas), "06.log_sem_url", str(cap.linhas))
    r.check(any("colis" in x.lower() for x in cap.linhas), "06.colisao_observavel",
            str(cap.linhas))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "ENCURTADOR · colisão de código curto"))
