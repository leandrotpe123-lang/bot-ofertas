"""
AMAZON — os quatro formatos de entrada, com DUAS URLs separadas:

  - ENVIADA ao SiteStripe: a de sempre (_construir_url_afiliada). Produto:
    a URL da fonte com SÓ a tag trocada — smid, th, psc, linkCode e o
    resto chegam preservados. PSP: o tratamento anterior (caminho da
    fonte, query só com a nossa tag). No sucesso, é a url_canon gravada
    junto do short.
  - PUBLICADA na falha: o fallback (_url_fallback), só quando o
    SiteStripe não devolve short. Produto: /dp/{ASIN}?tag=<nossa>; PSP:
    /promotion/psp/{ID}?tag=<nossa>. Nada da origem passa, e nada é
    gravado como conversão.

Regra de negócio do dono (02/11). Caso real (31/10 09:28, B0G5SDXLWV): o
SiteStripe falhou e foi ao ar a URL inteira da fonte, com smid,
linkCode, linkId, ref_, ascsubtag e btn_ref de outro afiliado.

Formatos (exemplos do dono; a tag de origem é de outro afiliado — a
nossa nunca vem da origem):
  F1  produto longo com rastreamento
  F2  promoção PSP (e variante com host sem www, sufixo no caminho e
      parâmetros extras: o comportamento anterior na chamada e o
      fallback mínimo na falha ficam visíveis)
  F3  produto com smid (e variante com smid, th e psc)
  F4  link curto de terceiros (link.amazon): expande primeiro e aplica a
      regra do produto ou da promoção

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
ALHEIA = "tag=outroafiliado-20"

F1 = (f"{BR}/dp/B0G5SDXLWV?smid=A1ZZFT5FULY4LN&linkCode=sl2&{ALHEIA}"
      "&linkId=b9496cc6dee63153d65fac4d4e80fd93&ref_=as_li_ss_tl"
      "&ascsubtag=srctok-4c2db8d055836cea&btn_type=ss"
      "&btn_ref=srctok-4c2db8d055836cea")
F2 = f"{BR}/promotion/psp/AMAFK189VQCOC?{ALHEIA}"
F2_VARIANTE = ("https://amazon.com.br/promotion/psp/AMAFK189VQCOC/ref=cm_sw"
               "?ref_=as_li_ss_tl&tag=fumo-20&smid=X1")
F3 = f"{BR}/dp/B0H3Q1R7P4?smid=A1ZZFT5FULY4LN&{ALHEIA}"
F3_VARIACAO = (f"{BR}/Produto-Qualquer/dp/B0H3Q1R7P4?smid=A1ZZFT5FULY4LN"
               f"&th=1&psc=1&{ALHEIA}")
F4 = "https://link.amazon/B0b2DF5Kn"
# Destinos possíveis do link curto de terceiros (a expansão é falsa).
F4_PRODUTO = (f"{BR}/Produto-Qualquer/dp/B0DHLBLFXQ/ref=sr_1_3?th=1&psc=1"
              "&smid=A1ZZFT5FULY4LN&tag=terceiro-20&linkCode=sl1")
F4_PSP = f"{BR}/promotion/psp/A1P4S4HG0ZKYIC?ref=cm_sw_r_cp_ud&tag=terceiro-20"

# Ao SiteStripe: a URL da fonte, só com a tag trocada (produto); o
# tratamento anterior (PSP: caminho da fonte, query só com a tag).
ENVIADA = {
    "F1": F1.replace(ALHEIA, f"tag={TAG}"),
    "F2": f"{BR}/promotion/psp/AMAFK189VQCOC?tag={TAG}",
    "F2_VARIANTE": ("https://amazon.com.br/promotion/psp/AMAFK189VQCOC/"
                    f"ref=cm_sw?tag={TAG}"),
    "F3": F3.replace(ALHEIA, f"tag={TAG}"),
    "F3_VARIACAO": F3_VARIACAO.replace(ALHEIA, f"tag={TAG}"),
    "F4": F4_PRODUTO.replace("tag=terceiro-20", f"tag={TAG}"),
    "F4_PSP": f"{BR}/promotion/psp/A1P4S4HG0ZKYIC?tag={TAG}",
}
# Na falha: o fallback mínimo.
FALLBACK = {
    "F1": f"{BR}/dp/B0G5SDXLWV?tag={TAG}",
    "F2": f"{BR}/promotion/psp/AMAFK189VQCOC?tag={TAG}",
    "F2_VARIANTE": f"{BR}/promotion/psp/AMAFK189VQCOC?tag={TAG}",
    "F3": f"{BR}/dp/B0H3Q1R7P4?tag={TAG}",
    "F3_VARIACAO": f"{BR}/dp/B0H3Q1R7P4?tag={TAG}",
    "F4": f"{BR}/dp/B0DHLBLFXQ?tag={TAG}",
    "F4_PSP": f"{BR}/promotion/psp/A1P4S4HG0ZKYIC?tag={TAG}",
}
SHORT = "https://link.amazon/B0nossoOK"
ERRO_503 = (503, "text/html;charset=UTF-8", None, "algo_errado")
RASTROS = ("smid", "linkCode", "linkId", "ref_", "ascsubtag", "btn_type",
           "btn_ref", "th=", "psc", "outroafiliado", "terceiro", "fumo-20",
           "m=", "Produto-Qualquer", "/ref=")


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


FORMATOS = {
    "F1": ("F1 produto longo com rastreamento", F1),
    "F2": ("F2 promoção PSP", F2),
    "F2_VARIANTE": ("F2 PSP com host sem www, sufixo e extras", F2_VARIANTE),
    "F3": ("F3 produto com smid", F3),
    "F3_VARIACAO": ("F3 produto com smid, th e psc", F3_VARIACAO),
    "F4": ("F4 link curto de terceiros → produto", F4),
}


# ── 503: SiteStripe recebe a de sempre; publica o fallback ────────
def _caso_503(r, chave):
    rotulo, entrada = FORMATOS[chave]
    expansao = _limpar()
    rede = _Rede(ERRO_503)
    conv = _norm(entrada, rede)
    r.check(rede.chamadas == [ENVIADA[chave]], f"{rotulo}: SiteStripe recebe "
            "a URL de sempre (só a tag trocada)", rede.chamadas)
    r.check(conv == FALLBACK[chave], f"{rotulo}: falha publica EXATAMENTE "
            f"{FALLBACK[chave]}", conv)
    r.check(isinstance(conv, str) and "smid" not in conv,
            f"{rotulo}: smid não aparece na URL publicada", conv)
    r.check(not any(x in conv for x in RASTROS),
            f"{rotulo}: nada da origem na URL publicada", conv)
    r.check(_linhas() == [] and consultar_link(entrada) is None,
            f"{rotulo}: falha não grava nada (memória e SQLite)", _linhas())
    r.check(not any("smid" in (c or "") or "smid" in (k or "")
                    for _, c, k in _linhas()),
            f"{rotulo}: smid em nenhuma URL registrada")
    if chave == "F4":
        r.check(expansao.chamadas == 1, f"{rotulo}: expandiu antes",
                expansao.chamadas)
        r.check(conv != F4, f"{rotulo}: nunca publica o curto de terceiros",
                conv)


def test_f1_503(r):
    _caso_503(r, "F1")


def test_f2_503(r):
    _caso_503(r, "F2")


def test_f2_variante_503_comportamento_anterior_na_chamada(r):
    _caso_503(r, "F2_VARIANTE")


def test_f3_503_criterio_principal(r):
    _caso_503(r, "F3")


def test_f4_503(r):
    _caso_503(r, "F4")


def test_produto_smid_th_psc_preservados_no_sitestripe(r):
    _caso_503(r, "F3_VARIACAO")
    _limpar()
    rede = _Rede(ERRO_503)
    _norm(F3_VARIACAO, rede)
    enviada = rede.chamadas[0] if rede.chamadas else ""
    r.check(all(p in enviada for p in ("smid=A1ZZFT5FULY4LN", "th=1",
                                       "psc=1", "/Produto-Qualquer/dp/")),
            "smid, th, psc e o caminho chegam ao SiteStripe", enviada)
    r.check(enviada.replace(f"tag={TAG}", ALHEIA) == F3_VARIACAO
            and amazon._tag_unica_nossa(enviada),
            "só a tag foi substituída — pela nossa, e só a nossa", enviada)


# ── 200: publica e grava SÓ o short; url_canon = URL enviada ──────
def _caso_200(r, chave):
    rotulo, entrada = FORMATOS[chave]
    _limpar()
    rede = _Rede(_ok(ENVIADA[chave]))
    conv = _norm(entrada, rede)
    r.check(rede.chamadas == [ENVIADA[chave]], f"{rotulo}: SiteStripe recebe "
            "a URL de sempre", rede.chamadas)
    r.check(isinstance(conv, Afiliacao) and conv.publicada == SHORT,
            f"{rotulo}: publica o short oficial devolvido", conv)
    linha = _linha(entrada)
    r.check(linha == (SHORT, ENVIADA[chave]), f"{rotulo}: url_conv = short; "
            "url_canon = a URL efetivamente enviada ao SiteStripe", linha)
    r.check(all(amazon._netloc(c) in amazon._SHORTS_OFICIAIS
                for _, c, _k in _linhas()),
            f"{rotulo}: só short oficial gravado como url_conv", _linhas())
    _so_memoria_limpa()
    de_novo = _norm(entrada, rede)
    r.check(len(rede.chamadas) == 1 and _publicada(de_novo) == SHORT,
            f"{rotulo}: reaproveita o short, sem nova chamada", rede.chamadas)
    if chave == "F4":
        r.check(linha and linha[0] != F4,
                f"{rotulo}: o curto de terceiros não vira conversão", linha)


def test_f1_200(r):
    _caso_200(r, "F1")


def test_f2_200(r):
    _caso_200(r, "F2")


def test_f3_200(r):
    _caso_200(r, "F3_VARIACAO")


def test_f4_200(r):
    _caso_200(r, "F4")


# ── F4: destino promoção e expansão que falha ─────────────────────
def test_f4_destino_promocao_e_expansao_falha(r):
    _limpar(destino_f4=F4_PSP)
    rede = _Rede(ERRO_503)
    conv = _norm(F4, rede)
    r.check(rede.chamadas == [ENVIADA["F4_PSP"]]
            and conv == FALLBACK["F4_PSP"],
            "curto → promoção: chamada e fallback da PSP",
            (rede.chamadas, conv))

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
        # versão anterior: a falha gravava a longa da fonte (com smid)
        ("F1", F1, ENVIADA["F1"]),
        ("F2", F2, ENVIADA["F2"]),
        ("F3", F3, ENVIADA["F3"]),
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
        r.check(rede.chamadas == [ENVIADA[chave]],
                f"{chave}: não bloqueou a tentativa", rede.chamadas)
        r.check(conv == FALLBACK[chave], f"{chave}: publica o fallback",
                conv)
        r.check(_linha(entrada) is None and consultar_link(entrada) is None,
                f"{chave}: registro REMOVIDO (memória e SQLite)",
                _linha(entrada))


if __name__ == "__main__":
    sys.exit(harness.rodar(globals(), "AMAZON — quatro formatos de entrada"))
