"""
SHOPEE — expansão que não resolveu NÃO vira identidade cacheada (P1-5).

utils.url_resolver.desencurtar() devolve a PRÓPRIA URL curta em timeout
ou falha de rede (contrato genérico, intocado). A afiliação Shopee
seguia com a curta: a API de afiliados aceitava, a canônica gravada era
`s.shopee.com.br/<x>` e o cache persistente guardava essa identidade —
única por link, nunca o produto — e a servia para sempre (links_cache
renova o ts a cada leitura).

Correção LOCAL à Shopee (plataformas/shopee):
  · entrada encurtador + expandida ainda em host encurtador → AUSENTE,
    sem chamar a API e sem gravar cache (mesmo contrato da Amazon);
  · `afiliacao_vigente` declarada: entrada de cache cuja canônica ainda
    é encurtador (gravada antes) não é servida — nem pelo normalizador
    (passo 3), nem pelo cache interno de `afilia`; a afiliação refaz e
    sobrescreve.

CORPUS REAL: links curtos de fumotom 35169 (`s.shopee.com.br/50ZTgk4qQ4`)
e Fada 17525 (`AAGNE4pfS9`); URL de live expandida real (test_live_
container, operador). Os encurtadores são bloqueados neste ambiente: a
expansão e a API são simuladas em memória, com contagem de chamadas.

  01  expansão normal → afiliação e cache como antes (produto forte)
  02  timeout (desencurtar devolve a curta) → AUSENTE, 0 API, 0 cache
  03  curta → outra curta (s.shopee → shope.ee) → AUSENTE, 0 API, 0 cache
  04  exceção na expansão → AUSENTE (inalterado)
  05  recuperação pela URL canônica de produto continua funcionando
  06  live: canônica da live preservada
  07  cache envenenado (gravado antes): afilia não serve, refaz e corrige
  08  cache envenenado pelo NORMALIZADOR (_normalizar_um, passo 3): não
      é servido; com expansão em timeout → ausência, nunca a curta
  09  cache saudável continua servido sem rede (cache-first intacto)
  10  contrato puro de afiliacao_vigente
  11  cache envenenado NÃO é renovado: detectado (por afilia ou pelo
      normalizador), é descartado das duas camadas — a leitura renovaria
      o ts e a linha ruim nunca expiraria (o Promotom repete os mesmos
      encurtadores toda noite). Com expansão em timeout: AUSENTE e
      nenhuma linha; com expansão real: a linha nova, correta

    python tests/test_shopee_expansao.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_cupom_sem_codigo import _zerar_banco                   # noqa: E402
from _harness_e5 import rodar                                   # noqa: E402
import config                                                   # noqa: E402
import globals as g                                             # noqa: E402
from pipeline import normalizacao_links                         # noqa: E402
from pipeline.normalizacao_identidade import derivar_produto     # noqa: E402
from plataformas import registry                                # noqa: E402
from plataformas.contrato import AUSENTE, Afiliacao              # noqa: E402
from plataformas.shopee import afiliacao as shp                  # noqa: E402
from utils.cache_links import consultar_link, registrar_link     # noqa: E402

CURTA_35169 = "https://s.shopee.com.br/50ZTgk4qQ4"
CURTA_17525 = "https://s.shopee.com.br/AAGNE4pfS9"
PRODUTO = "https://shopee.com.br/Smart-TV-32-AOC-i.338371559.22593482107?sp_atk=x&xptdk=y"
PRODUTO_LIMPO = "https://shopee.com.br/Smart-TV-32-AOC-i.338371559.22593482107"
PRODUTO_CANONICO = "https://shopee.com.br/product/338371559/22593482107"
URL_LIVE = ("https://live.shopee.com.br/universal-link/share?from=live"
            "&mmp_pid=an_18139140007&session=7187289&share_user_id=293588261"
            "&uls_trackid=56oufm8f00je&utm_medium=affiliates")


class _Rede:
    """Expansão e API de afiliados em memória, com contagem."""

    def __init__(self, expansao, api=None):
        self.expansao = expansao            # url -> str | Exception
        self.api = api or (lambda u: f"https://s.shopee.com.br/NOSSO{abs(hash(u)) % 9999}")
        self.chamadas_api, self.chamadas_exp = [], []

    async def desencurtar(self, url, sessao, depth=0):
        self.chamadas_exp.append(url)
        r = self.expansao(url)
        if isinstance(r, Exception):
            raise r
        return r

    async def servico(self, url, sessao):
        self.chamadas_api.append(url)
        return self.api(url)


def _limpar_memoria():
    """Cache de links em memória (globals não o zera em _init_globals)."""
    with g._cache_lock:
        g._final_cache.clear()
        g._raw_cache.clear()


def _com_rede(rede, coro_fn):
    """Roda `coro_fn()` com a rede simulada, banco e memória limpos."""
    real_d, real_s = shp.desencurtar, shp._chamar_servico_afiliados

    async def _run():
        g._init_globals()
        _limpar_memoria()
        config._SEM_HTTP = asyncio.Semaphore(20)
        shp.desencurtar, shp._chamar_servico_afiliados = rede.desencurtar, rede.servico
        try:
            return await coro_fn()
        finally:
            shp.desencurtar, shp._chamar_servico_afiliados = real_d, real_s
    _zerar_banco()
    return asyncio.run(_run())


def _so_cache_banco(url):
    """Cache persistente, sem a camada de memória."""
    _limpar_memoria()
    return consultar_link(url)


# ══════════════════════════════════════════════════════════════════
def test_01_expansao_normal_intacta(r):
    rede = _Rede(lambda u: PRODUTO)
    res = _com_rede(rede, lambda: shp.afilia(CURTA_35169, None))
    r.check(isinstance(res, Afiliacao) and res.canonica == PRODUTO_LIMPO,
            "01.canonica_do_produto", str(res))
    r.check(rede.chamadas_api == [PRODUTO_LIMPO], "01.api_uma_vez", str(rede.chamadas_api))
    ids, _idents, _sku = derivar_produto([res.canonica])
    r.check(ids == ["shopee:338371559.22593482107"] or len(ids) == 1, "01.produto_forte", str(ids))
    r.check(_so_cache_banco(CURTA_35169) == res, "01.cache_gravado",
            str(_so_cache_banco(CURTA_35169)))


def test_02_timeout_vira_ausente_sem_api_sem_cache(r):
    rede = _Rede(lambda u: u)              # desencurtar em timeout devolve a entrada
    res = _com_rede(rede, lambda: shp.afilia(CURTA_35169, None))
    r.check(res is AUSENTE, "02.ausente", str(res))
    r.check(rede.chamadas_api == [], "02.sem_api", str(rede.chamadas_api))
    r.check(_so_cache_banco(CURTA_35169) is None, "02.sem_cache")


def test_03_curta_para_outra_curta(r):
    rede = _Rede(lambda u: "https://shope.ee/9zXyWvU")
    res = _com_rede(rede, lambda: shp.afilia(CURTA_17525, None))
    r.check(res is AUSENTE and rede.chamadas_api == [], "03.ausente_sem_api",
            f"{res} {rede.chamadas_api}")
    r.check(_so_cache_banco(CURTA_17525) is None, "03.sem_cache")


def test_04_excecao_na_expansao(r):
    rede = _Rede(lambda u: TimeoutError("x"))
    res = _com_rede(rede, lambda: shp.afilia(CURTA_35169, None))
    r.check(res is AUSENTE and rede.chamadas_api == [], "04.ausente_sem_api")
    r.check(_so_cache_banco(CURTA_35169) is None, "04.sem_cache")


def test_05_recuperacao_pela_canonica(r):
    rede = _Rede(lambda u: PRODUTO,
                 api=lambda u: "https://s.shopee.com.br/RECUP1" if u == PRODUTO_CANONICO else None)
    res = _com_rede(rede, lambda: shp.afilia(CURTA_35169, None))
    r.check(rede.chamadas_api[-1:] == [PRODUTO_CANONICO], "05.tentou_a_canonica",
            str(rede.chamadas_api))
    r.check(isinstance(res, Afiliacao) and res.publicada == "https://s.shopee.com.br/RECUP1",
            "05.recuperou", str(res))


def test_06_live_preservada(r):
    rede = _Rede(lambda u: URL_LIVE)
    res = _com_rede(rede, lambda: shp.afilia(CURTA_35169, None))
    r.check(isinstance(res, Afiliacao)
            and res.canonica == "https://live.shopee.com.br/live/7187289",
            "06.canonica_da_live", str(res))


ENVENENADO = Afiliacao(publicada="https://s.shopee.com.br/NOSSOVELHO",
                       canonica=CURTA_35169)


def test_07_cache_envenenado_nao_e_servido_por_afilia(r):
    rede = _Rede(lambda u: PRODUTO)

    async def fluxo():
        registrar_link(CURTA_35169, ENVENENADO, "shopee")
        return await shp.afilia(CURTA_35169, None)
    res = _com_rede(rede, fluxo)
    r.check(rede.chamadas_exp == [CURTA_35169] and rede.chamadas_api == [PRODUTO_LIMPO],
            "07.refez_expansao_e_afiliacao", f"{rede.chamadas_exp} {rede.chamadas_api}")
    r.check(isinstance(res, Afiliacao) and res.canonica == PRODUTO_LIMPO, "07.canonica_certa",
            str(res))
    r.check(_so_cache_banco(CURTA_35169) == res, "07.cache_sobrescrito",
            str(_so_cache_banco(CURTA_35169)))


def test_08_cache_envenenado_pelo_normalizador(r):
    plat = registry.resolver(CURTA_35169)
    r.check(plat is not None and plat.afiliacao_vigente is not None,
            "08.shopee_declara_afiliacao_vigente")
    for rot, expansao, esperado in (("expande", lambda u: PRODUTO, PRODUTO_LIMPO),
                                    ("timeout", lambda u: u, None)):
        rede = _Rede(expansao)

        async def fluxo():
            registrar_link(CURTA_35169, ENVENENADO, "shopee")
            _limpar_memoria()
            return await normalizacao_links._normalizar_um(CURTA_35169, None)
        _orig, conv, ident = _com_rede(rede, fluxo)
        canon = getattr(conv, "canonica", conv)
        r.check(canon == esperado, f"08.{rot}.nunca_serve_a_curta", f"{conv} {ident}")
        r.check(rede.chamadas_exp == [CURTA_35169], f"08.{rot}.passou_pela_afiliacao",
                str(rede.chamadas_exp))


def test_09_cache_saudavel_servido_sem_rede(r):
    saudavel = Afiliacao(publicada="https://s.shopee.com.br/NOSSO1", canonica=PRODUTO_LIMPO)
    rede = _Rede(lambda u: AssertionError("não deveria expandir"))

    async def fluxo():
        registrar_link(CURTA_35169, saudavel, "shopee")
        _limpar_memoria()
        a = await shp.afilia(CURTA_35169, None)
        b = await normalizacao_links._normalizar_um(CURTA_35169, None)
        return a, b
    a, b = _com_rede(rede, fluxo)
    r.check(a == saudavel and getattr(b[1], "canonica", None) == PRODUTO_LIMPO,
            "09.cache_first", f"{a} {b}")
    r.check(rede.chamadas_exp == [] and rede.chamadas_api == [], "09.sem_rede")


def test_10_contrato_afiliacao_vigente(r):
    v = shp.afiliacao_vigente
    r.check(v(ENVENENADO) is False, "10.canonica_curta_invalida")
    r.check(v(Afiliacao(publicada="x", canonica="https://shope.ee/abc")) is False,
            "10.shope_ee_invalida")
    r.check(v(Afiliacao(publicada="https://s.shopee.com.br/N", canonica=PRODUTO_LIMPO)) is True,
            "10.publicada_curta_nossa_com_canonica_longa_valida")
    r.check(v("https://flapremios.com.br/x") is True, "10.repasse_direto_valido")
    r.check(v(Afiliacao(publicada="https://s.shopee.com.br/N",
                        canonica="https://live.shopee.com.br/live/7187289")) is True,
            "10.live_valida")


def test_11_cache_envenenado_nao_e_renovado(r):
    import time
    from database_conexao import _db
    from utils.urls import _cache_key
    velho = time.time() - 10 * 86400

    def linha(url):
        with _db() as db:
            return db.execute("SELECT ts, url_canon FROM links_cache WHERE url_orig=?",
                              (_cache_key(url),)).fetchone()
    casos = (("afilia", lambda: shp.afilia(CURTA_35169, None)),
             ("normalizador", lambda: normalizacao_links._normalizar_um(CURTA_35169, None)))
    for rot, chamar in casos:
        for exp_rot, expansao, canon_final in (("timeout", lambda u: u, None),
                                                ("expande", lambda u: PRODUTO, PRODUTO_LIMPO)):
            rede = _Rede(expansao)

            async def fluxo(chamar=chamar):
                registrar_link(CURTA_35169, ENVENENADO, "shopee")
                with _db() as db:
                    db.execute("UPDATE links_cache SET ts=? WHERE url_orig=?",
                               (velho, _cache_key(CURTA_35169)))
                _limpar_memoria()
                await chamar()
                return linha(CURTA_35169), consultar_link(CURTA_35169)
            depois, servida = _com_rede(rede, fluxo)
            if canon_final is None:
                r.check(depois is None, f"11.{rot}.{exp_rot}.linha_ruim_removida", str(depois))
                r.check(servida is None, f"11.{rot}.{exp_rot}.nada_em_memoria", str(servida))
            else:
                r.check(depois is not None and depois[1] == canon_final,
                        f"11.{rot}.{exp_rot}.linha_nova_correta", str(depois))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "SHOPEE · expansão não resolvida não vira identidade"))
