"""
AMAZON — os quatro formatos de entrada: todos tentam o SiteStripe; no
sucesso, publica e guarda o short devolvido; na falha, publica na hora a
forma canônica mínima — /dp/ASIN?tag=<nossa> ou
/promotion/psp/ID?tag=<nossa> — sem smid e sem nada da origem, e NÃO a
grava como conversão.

Regra de negócio do dono (02/11). Caso real (31/10 09:28, B0G5SDXLWV):
o SiteStripe falhou e foi ao ar a URL inteira da fonte, com smid,
linkCode, linkId, ref_, ascsubtag e btn_ref de outro afiliado.

Formatos (os exemplos do dono, com a tag de origem trocada por uma de
outro afiliado — a nossa nunca vem da origem):
  F1  produto longo com rastreamento (smid, linkCode, linkId, ref_,
      ascsubtag, btn_type, btn_ref)
  F2  promoção PSP
  F3  produto com smid
  F4  link curto de terceiros (link.amazon): resolve o destino e aplica
      a regra do produto ou da promoção

Por formato: 503 → fallback exato, SiteStripe chamado com a mesma forma,
nada gravado e `smid` em lugar nenhum; 200 → publica e guarda o short
devolvido, e o reaproveita sem nova chamada; registro antigo longo (ou,
em F4, o próprio short de terceiros gravado como "conversão") é
invalidado e removido.

Caminho REAL: etapa de cache do core (normalizacao_links._normalizar_um)
→ amazon.afilia, com SQLite de verdade. Falsas só a rede do SiteStripe e
a expansão do link curto.

    python tests/test_amazon_formatos_entrada.py
"""
import asyncio
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
from plataformas import amazon                                  # noqa: E402
from plataformas.contrato import Afiliacao                      # noqa: E402
from utils.cache_links import consultar_link, registrar_link    # noqa: E402
from utils.urls import _cache_key                               # noqa: E402

TAG = amazon._AMZ_TAG
BR = "https://www.amazon.com.br"

F1 = (f"{BR}/dp/B0G5SDXLWV?smid=A1ZZFT5FULY4LN&linkCode=sl2"
      "&tag=outroafiliado-20&linkId=b9496cc6dee63153d65fac4d4e80fd93"
      "&ref_=as_li_ss_tl&ascsubtag=srctok-4c2db8d055836cea&btn_type=ss"
      "&btn_ref=srctok-4c2db8d055836cea")
F2 = f"{BR}/promotion/psp/AMAFK189VQCOC?tag=outroafiliado-20"
F3 = f"{BR}/dp/B0H3Q1R7P4?smid=A1ZZFT5FULY4LN&tag=outroafiliado-20"
F4 = "https://link.amazon/B0b2DF5Kn"
# Destinos possíveis do link curto de terceiros (a expansão é falsa).
F4_PRODUTO = (f"{BR}/Produto-Qualquer/dp/B0DHLBLFXQ/ref=sr_1_3?th=1&psc=1"
              "&smid=A1ZZFT5FULY4LN&tag=terceiro-20&linkCode=sl1")
F4_PSP = f"{BR}/promotion/psp/A1P4S4HG0ZKYIC?ref=cm_sw_r_cp_ud&tag=terceiro-20"

LIMPA = {
    "F1": f"{BR}/dp/B0G5SDXLWV?tag={TAG}",
    "F2": f"{BR}/promotion/psp/AMAFK189VQCOC?tag={TAG}",
    "F3": f"{BR}/dp/B0H3Q1R7P4?tag={TAG}",
    "F4": f"{BR}/dp/B0DHLBLFXQ?tag={TAG}",
    "F4_PSP": f"{BR}/promotion/psp/A1P4S4HG0ZKYIC?tag={TAG}",
}
SHORT = "https://link.amazon/B0nossoOK"
ERRO_503 = (503, "text/html;charset=UTF-8", None, "algo_errado")
RASTROS = ("smid", "linkCode", "linkId", "ref_", "ascsubtag", "btn_type",
           "btn_ref", "th=", "psc", "outroafiliado", "terceiro", "m=")


def _ok(canonica):
    return (200, "application/json",
            {"ok": True, "isOk": True, "shortUrl": SHORT,
             "longUrl": canonica}, "")


# ── infraestrutura ────────────────────────────────────────────────
class _Rede:
    def __init__(self, *roteiro):
        self.roteiro, self.chamadas = list(roteiro), []

    async def __call__(self, params):
        self.chamadas.append(params["longUrl"])
        return self.roteiro.pop(0) if len(self.roteiro) > 1 \
            else self.roteiro[0]


class _Expansao:
    def __init__(self, destino):
        self.destino, self.chamadas = destino, 0

    async def __call__(self, url, sessao):
        self.chamadas += 1
        if isinstance(self.destino, BaseException):
            raise self.destino
        return self.destino


def _limpar(destino_f4=F4_PRODUTO):
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
    expansao = _Expansao(destino_f4)
    amazon.desencurtar = expansao
    return expansao


def _so_memoria_limpa():
    with g._cache_lock:
        g._final_cache.clear()


def _linhas():
    with _db() as db:
        return db.execute("SELECT url_orig, url_conv, url_canon FROM "
                          "links_cache WHERE plat='amazon'").fetchall()


def _linha(url):
    with _db() as db:
        return db.execute("SELECT url_conv, url_canon FROM links_cache "
                          "WHERE url_orig=?", (_cache_key(url),)).fetchone()


def _norm(url, rede):
    amazon._http_getshorturl = rede
    return asyncio.run(nl._normalizar_um(url, None))[1]


def _publicada(conv):
    return conv.publicada if isinstance(conv, Afiliacao) else conv


FORMATOS = (("F1 produto longo com rastreamento", F1, "F1"),
            ("F2 promoção PSP", F2, "F2"),
            ("F3 produto com smid", F3, "F3"),
            ("F4 link curto de terceiros", F4, "F4"))


# ── 503: fallback limpo, nada gravado ─────────────────────────────
def _caso_503(r, rotulo, entrada, chave):
    expansao = _limpar()
    rede = _Rede(ERRO_503)
    conv = _norm(entrada, rede)
    esperada = LIMPA[chave]
    r.check(rede.chamadas == [esperada], f"{rotulo}: tenta o SiteStripe, "
            "com a forma canônica mínima", rede.chamadas)
    r.check(conv == esperada, f"{rotulo}: publica EXATAMENTE {esperada}",
            conv)
    r.check(isinstance(conv, str) and "smid" not in conv,
            f"{rotulo}: smid não aparece na URL publicada", conv)
    r.check(not any(x in conv for x in RASTROS),
            f"{rotulo}: nenhum rastreamento da origem publicado", conv)
    r.check(_linhas() == [] and consultar_link(entrada) is None,
            f"{rotulo}: falha não grava nada (memória e SQLite)", _linhas())
    r.check(not any("smid" in (c or "") or "smid" in (k or "")
                    for _, c, k in _linhas()),
            f"{rotulo}: smid não aparece em nenhuma URL registrada")
    if chave == "F4":
        r.check(expansao.chamadas == 1, f"{rotulo}: resolveu o destino",
                expansao.chamadas)
        r.check(conv != F4, f"{rotulo}: nunca publica o curto de terceiros",
                conv)


def test_f1_503(r):
    _caso_503(r, *FORMATOS[0])


def test_f2_503(r):
    _caso_503(r, *FORMATOS[1])


def test_f3_503_criterio_principal(r):
    _caso_503(r, *FORMATOS[2])


def test_f4_503(r):
    _caso_503(r, *FORMATOS[3])


# ── 200: publica e guarda o short devolvido ───────────────────────
def _caso_200(r, rotulo, entrada, chave):
    _limpar()
    esperada = LIMPA[chave]
    rede = _Rede(_ok(esperada))
    conv = _norm(entrada, rede)
    r.check(len(rede.chamadas) == 1, f"{rotulo}: tenta o SiteStripe",
            rede.chamadas)
    r.check(isinstance(conv, Afiliacao) and conv.publicada == SHORT,
            f"{rotulo}: publica o short devolvido (não a longa)", conv)
    linha = _linha(entrada)
    r.check(linha == (SHORT, esperada), f"{rotulo}: short gravado como "
            "conversão da URL de origem, com a canônica mínima", linha)
    r.check(not any("smid" in (c or "") or "smid" in (k or "")
                    for _, c, k in _linhas()),
            f"{rotulo}: smid em nenhuma URL registrada", _linhas())
    _so_memoria_limpa()
    de_novo = _norm(entrada, rede)
    r.check(len(rede.chamadas) == 1 and _publicada(de_novo) == SHORT,
            f"{rotulo}: reaproveita o short, sem nova chamada", rede.chamadas)
    if chave == "F4":
        r.check(SHORT != F4 and linha[0] != F4,
                f"{rotulo}: o curto de terceiros não vira conversão", linha)


def test_f1_200(r):
    _caso_200(r, *FORMATOS[0])


def test_f2_200(r):
    _caso_200(r, *FORMATOS[1])


def test_f3_200(r):
    _caso_200(r, *FORMATOS[2])


def test_f4_200(r):
    _caso_200(r, *FORMATOS[3])


# ── F4: destino promoção e expansão que falha ─────────────────────
def test_f4_destino_promocao_e_expansao_falha(r):
    _limpar(destino_f4=F4_PSP)
    rede = _Rede(ERRO_503)
    conv = _norm(F4, rede)
    r.check(rede.chamadas == [LIMPA["F4_PSP"]] and conv == LIMPA["F4_PSP"],
            "curto → promoção: regra da PSP", (rede.chamadas, conv))

    expansao = _limpar(destino_f4=TimeoutError())
    rede = _Rede(ERRO_503)
    conv = _norm(F4, rede)
    r.check(conv is None and not rede.chamadas and expansao.chamadas == 1,
            "expansão falhou: link não convertido — nunca o curto de "
            "terceiros publicado", (conv, rede.chamadas))
    r.check(_linhas() == [], "e nada gravado")


# ── registros antigos ─────────────────────────────────────────────
def test_cache_antigo_longo_e_invalidado(r):
    antigos = (
        # versão anterior: falha gravava a longa da origem (com smid)
        ("F1", F1, F1.replace("tag=outroafiliado-20", f"tag={TAG}")),
        ("F2", F2, LIMPA["F2"]),
        ("F3", F3, F3.replace("tag=outroafiliado-20", f"tag={TAG}")),
        # o próprio curto de terceiros tratado como "conversão"
        ("F4", F4, F4),
    )
    for chave, entrada, gravada in antigos:
        _limpar()
        registrar_link(entrada, gravada, "amazon")
        with _db() as db:
            db.execute("UPDATE links_cache SET ts=? WHERE url_orig=?",
                       (time.time() - 20 * 86400, _cache_key(entrada)))
        _so_memoria_limpa()
        r.check(_linha(entrada) is not None, f"{chave}: registro antigo")
        rede = _Rede(ERRO_503)
        conv = _norm(entrada, rede)
        r.check(len(rede.chamadas) == 1, f"{chave}: não bloqueou a tentativa",
                rede.chamadas)
        r.check(conv == LIMPA[chave], f"{chave}: publica o fallback limpo",
                conv)
        r.check(_linha(entrada) is None and consultar_link(entrada) is None,
                f"{chave}: registro REMOVIDO (memória e SQLite)",
                _linha(entrada))


if __name__ == "__main__":
    sys.exit(harness.rodar(globals(), "AMAZON — quatro formatos de entrada"))
