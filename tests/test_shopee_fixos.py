"""
SHOPEE — carteira e carrinho com o NOSSO link fixo, na hora; cache por
destino; teto de tempo da API; cache de links por 30 dias.

Pedido do operador (02/10): os links "Resgate aqui" e "Carrinho" da Fada
e do Promotom levam sempre às mesmas páginas (carteira de cupons e
carrinho). Igual ao `/sec/` do Mercado Livre: chegou um desses, troca
NA HORA pelo nosso — sem desencurtar, sem chamar a Shopee — e a
identidade do post NÃO muda; só o link.

CORPUS REAL: posts da Fada (17367, 17421) e do Promotom (9150, 9228);
URLs expandidas enviadas pelo operador; nossos links publicados no canal
desde 24/09 (8pkZllbmly carteira, 1qapQu2VFM carrinho). Os encurtadores
são bloqueados neste ambiente: expansão e API simuladas em memória, com
contagem de chamadas. O teto roda contra um servidor HTTP local de
verdade (aiohttp).

  01  os 4 links das fontes (e os nossos) → link fixo, 0 expansão, 0 API
  02  IDENTIDADE IGUAL: post real normalizado antes (expansão + API) e
      depois (link fixo) — mesma canônica, identidades, âncora, cupons;
      só muda o link publicado (e já era o mesmo)
  03  canônica gravada no cache é preservada byte a byte
  04  normalizador: entrada que publicaria OUTRO link para carteira/
      carrinho não é servida — vira o fixo, sem rede; a do fixo é servida
  05  regra de destino: código curto desconhecido que leva à carteira/
      carrinho → fixo após expandir, 0 API; qualquer outra página → API
  06  casamento exato do código (maiúsculas, host, caminho)
  07  produto intacto (expansão + API + canônica de sempre)
  08  cache por destino: 2 fontes, mesmo produto → 1 chamada à API, mesma
      identidade; sobrevive ao reinício (banco)
  09  cache por destino só reusa link feito pela API
  10  recuperação não grava destino
  11  teto: tentativa travada abandonada; prazos crescentes; pior caso
  12  cache de links: 30 dias sem uso → sai; com uso → fica

    python tests/test_shopee_fixos.py
"""
import asyncio
import os
import sys
import time

import aiohttp                                                  # real, antes do harness
from aiohttp import web

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_shopee_expansao import (_Rede, _com_rede, _limpar_memoria,  # noqa: E402
                                  _so_cache_banco, PRODUTO, PRODUTO_LIMPO,
                                  PRODUTO_CANONICO)
from _harness_e5 import rodar                                   # noqa: E402
import config                                                   # noqa: E402
import globals as g                                             # noqa: E402
import plataformas                                              # noqa: E402
from database_conexao import _db                                # noqa: E402
from database_manutencao import db_limpar                       # noqa: E402
from pipeline import normalizacao, normalizacao_links           # noqa: E402
from pipeline.identidade_oferta import identidade_canonica, identidades  # noqa: E402
from pipeline.ingestao import MensagemBruta, _RE_URL            # noqa: E402
from plataformas.contrato import Afiliacao                      # noqa: E402
from plataformas.shopee import afiliacao as shp                 # noqa: E402
from utils.cache_links import consultar_link, registrar_link    # noqa: E402
from utils.urls import _cache_key                               # noqa: E402

plataformas.inicializar()

S = "https://s.shopee.com.br/"
FADA_RESGATE, FADA_CARRINHO = S + "1qU7Zs67MB", S + "7plKiu5H62"
PROMO_RESGATE, PROMO_CARRINHO = S + "70GSVaUJxC", S + "7fW9IpEnZG"
NOSSA_CARTEIRA, NOSSO_CARRINHO = S + "8pkZllbmly", S + "1qapQu2VFM"

# URLs expandidas REAIS (operador, 02/10).
EXPANDIDA = {
    FADA_RESGATE: ("https://shopee.com.br/user/voucher-wallet?mmp_pid=an_18105130010"
                   "&uls_trackid=56pc9irj00je&utm_campaign=id_DT1TnJsf0b&utm_content=----"
                   "&utm_medium=affiliates&utm_source=an_18105130010&utm_term=fn1tn47peqqr"),
    FADA_CARRINHO: ("https://shopee.com.br/cart/?mmp_pid=an_18105130010"
                    "&uls_trackid=56pc9ktc00l7&utm_campaign=id_GDt7o4NJ4T&utm_content=----"
                    "&utm_medium=affiliates&utm_source=an_18105130010&utm_term=fn1tosbytd6g"),
    PROMO_RESGATE: ("https://shopee.com.br/user/voucher-wallet?mmp_pid=an_18139140007"
                    "&uls_trackid=56pc9uf200kf&utm_campaign=id_nrWmx87ljF&utm_content=----"
                    "&utm_medium=affiliates&utm_source=an_18139140007&utm_term=fn1twkwpy64o"),
    PROMO_CARRINHO: ("https://shopee.com.br/cart?mmp_pid=an_18139140007"
                     "&uls_trackid=56pca0lp00ku&utm_campaign=id_Dx7X6tsqV7&utm_content=----"
                     "&utm_medium=affiliates&utm_source=an_18139140007&utm_term=fn1tyecrtqpb"),
}
# O que a API devolvia para cada página (histórico do canal).
API_PAGINA = {"https://shopee.com.br/user/voucher-wallet": NOSSA_CARTEIRA,
              "https://shopee.com.br/cart/": NOSSO_CARRINHO,
              "https://shopee.com.br/cart": NOSSO_CARRINHO}
FIXO = {FADA_RESGATE: NOSSA_CARTEIRA, FADA_CARRINHO: NOSSO_CARRINHO,
        PROMO_RESGATE: NOSSA_CARTEIRA, PROMO_CARRINHO: NOSSO_CARRINHO,
        NOSSA_CARTEIRA: NOSSA_CARTEIRA, NOSSO_CARRINHO: NOSSO_CARRINHO}

# Posts REAIS.
POST_FADA_17367 = ("🚨 CUPOM SHOPEE 🚨\n\n🎟 R$20 OFF em R$60: PLXN24GJHC\n\n"
                   f"✅ Resgate aqui:\n{FADA_RESGATE}\n\n🛒 Carrinho: {FADA_CARRINHO}")
POST_FADA_17421 = ("🚨 Cupom Shopee Voltando\n\n🎟 R$20 OFF em R$60: R1N1T3RS\n\n"
                   f"✅ Resgate aqui:\n{FADA_RESGATE}\n\n🛒 Carrinho: {FADA_CARRINHO}")
POST_PROMO_9150 = ("Cupom Shopee\n\nR$ 20 OFF em R$ 60: V4M0S4PR0V31T4R\n\n"
                   f"-Resgate aqui: \n{PROMO_RESGATE}\n\n-Link carrinho:\n{PROMO_CARRINHO}"
                   "\n\n-Anúncio")
POST_PROMO_9228 = ("Cupom Shopee Voltando!\n\nR$ 50 OFF em R$ 120: M35D0SH0P33L0V3R\n\n"
                   f"-Resgate aqui: \n{PROMO_RESGATE}\n\n-Link carrinho:\n{PROMO_CARRINHO}"
                   "\n\n-Anúncio")
# Sem código (identidade pelo tema) e sem oferta nenhuma (identidade
# pela âncora de URL — o único caminho em que a canônica da página pesa).
POST_SEM_CODIGO = f"Cupom Shopee Voltando!\n\n-Resgate aqui:\n{PROMO_RESGATE}"
POST_SO_LINK = f"Corre que ainda dá tempo\n{FADA_CARRINHO}"

REAIS = (("fada_17367", POST_FADA_17367, ["PLXN24GJHC"]),
         ("fada_17421", POST_FADA_17421, ["R1N1T3RS"]),
         ("promotom_9150", POST_PROMO_9150, ["V4M0S4PR0V31T4R"]),
         ("promotom_9228", POST_PROMO_9228, ["M35D0SH0P33L0V3R"]),
         ("sem_codigo", POST_SEM_CODIGO, []),
         ("so_link", POST_SO_LINK, []))


def _proibida(u):
    return AssertionError(f"rede proibida: {u}")


def _expansao_real(u):
    return EXPANDIDA.get(u, PRODUTO)


def _api_antiga(u):
    return API_PAGINA.get(u, "https://s.shopee.com.br/NOSSOPROD1")


class _SemFixos:
    """O código de ANTES: sem tabela, sem regra de destino, sem a guarda
    de vigência das páginas — expansão e API, como era."""

    def __enter__(self):
        self.reais = (shp._CURTOS_CONHECIDOS, shp._fixo)
        shp._CURTOS_CONHECIDOS, shp._fixo = {}, (lambda c: None)

    def __exit__(self, *a):
        shp._CURTOS_CONHECIDOS, shp._fixo = self.reais


async def _normalizar(texto, codigos, msg_id=17367):
    links = [u.strip().rstrip('.,;)>]}!?') for u in _RE_URL.findall(texto)]
    bruta = MensagemBruta(msg_id=msg_id, chat="-1001768101197", texto=texto,
                          links=links, tem_midia=False, media_obj=None,
                          code_entities=list(codigos))
    try:
        return await normalizacao.normalizar(bruta)
    finally:                                  # a sessão HTTP é do loop deste teste
        if g._http_session is not None and not g._http_session.closed:
            await g._http_session.close()
        g._http_session = None


def _retrato(norm):
    """Tudo o que é identidade/conteúdo do post — menos o link publicado."""
    return {
        "canonica": identidade_canonica(norm), "identidades": identidades(norm),
        "ancora": norm.ancora_url, "ids": norm.ids_globais, "cupons": norm.cupons,
        "campanha": (norm.chave_campanha, tuple(norm.chaves_campanha),
                     norm.tem_host_campanha), "destinos": norm.destinos_declarados,
        "plat": norm.plat,
    }


# ══════════════════════════════════════════════════════════════════
def test_01_links_das_fontes_viram_o_fixo_na_hora(r):
    for curta, nosso in FIXO.items():
        rede = _Rede(_proibida, api=_proibida)
        res = _com_rede(rede, lambda: shp.afilia(curta, None))
        tag = curta.rsplit("/", 1)[1]
        r.check(isinstance(res, Afiliacao) and res.publicada == nosso,
                f"01.{tag}.link_fixo", str(res))
        r.check(rede.chamadas_exp == [] and rede.chamadas_api == [],
                f"01.{tag}.zero_rede", f"{rede.chamadas_exp} {rede.chamadas_api}")
        r.check(shp.afiliacao_vigente(res), f"01.{tag}.vigente")
        r.check(_so_cache_banco(curta) == res, f"01.{tag}.gravado", str(_so_cache_banco(curta)))
    # canônica = a página limpa que a expansão real daria
    for curta, longa in EXPANDIDA.items():
        rede = _Rede(_proibida, api=_proibida)
        res = _com_rede(rede, lambda: shp.afilia(curta, None))
        r.check(res.canonica == shp.limpa_url(longa), "01.canonica_igual_a_da_expansao",
                f"{res.canonica} != {shp.limpa_url(longa)}")


def test_02_identidade_igual_antes_e_depois(r):
    for rot, texto, codigos in REAIS:
        rede_antes = _Rede(_expansao_real, api=_api_antiga)
        with _SemFixos():
            antes = _com_rede(rede_antes, lambda: _normalizar(texto, codigos))
        rede_depois = _Rede(_proibida, api=_proibida)
        depois = _com_rede(rede_depois, lambda: _normalizar(texto, codigos))
        r.check(antes is not None and depois is not None, f"02.{rot}.normalizou")
        if antes is None or depois is None:
            continue
        r.check(len(rede_antes.chamadas_exp) == len(antes.mapa) and rede_antes.chamadas_api,
                f"02.{rot}.antes_ia_a_rede", f"{rede_antes.chamadas_exp}")
        r.check(rede_depois.chamadas_exp == [] and rede_depois.chamadas_api == [],
                f"02.{rot}.depois_zero_rede",
                f"{rede_depois.chamadas_exp} {rede_depois.chamadas_api}")
        ra, rd = _retrato(antes), _retrato(depois)
        r.check(ra == rd, f"02.{rot}.identidade_igual", f"\n  antes={ra}\n  depois={rd}")
        r.check(antes.mapa == depois.mapa, f"02.{rot}.mesmos_links_publicados",
                f"{antes.mapa} {depois.mapa}")
        r.check(set(depois.mapa.values()) <= {NOSSA_CARTEIRA, NOSSO_CARRINHO},
                f"02.{rot}.so_links_nossos", str(depois.mapa))
        r.check(antes.texto_limpo == depois.texto_limpo, f"02.{rot}.mesmo_texto")


def test_03_canonica_gravada_preservada(r):
    casos = (
        # Promotom (tabela: "cart") com a forma "cart/" gravada: fica a gravada
        (PROMO_CARRINHO, "https://shopee.com.br/cart/", "https://shopee.com.br/cart/"),
        (FADA_RESGATE, "https://shopee.com.br/user/voucher-wallet",
         "https://shopee.com.br/user/voucher-wallet"),
        # gravada de OUTRA página (corrompida): vale a da tabela
        (PROMO_CARRINHO, "https://shopee.com.br/user/voucher-wallet", "https://shopee.com.br/cart"),
        # gravada curta (expansão que falhou, F5): vale a da tabela
        (FADA_CARRINHO, FADA_CARRINHO, "https://shopee.com.br/cart/"),
        # gravada de produto (corrompida): vale a da tabela
        (FADA_RESGATE, PRODUTO_LIMPO, "https://shopee.com.br/user/voucher-wallet"),
    )
    for i, (curta, gravada, esperada) in enumerate(casos):
        rede = _Rede(_proibida, api=_proibida)

        async def fluxo(curta=curta, gravada=gravada):
            registrar_link(curta, Afiliacao("https://s.shopee.com.br/VELHO", gravada), "shopee")
            _limpar_memoria()
            return await shp.afilia(curta, None)
        res = _com_rede(rede, fluxo)
        r.check(res.canonica == esperada and res.publicada == FIXO[curta],
                f"03.{i}.canonica", str(res))
        r.check(rede.chamadas_exp == [] and rede.chamadas_api == [], f"03.{i}.zero_rede")


def test_04_normalizador_nunca_serve_outro_link_para_as_paginas(r):
    # entrada antiga publicando OUTRO link para a carteira
    outro = Afiliacao("https://s.shopee.com.br/OUTROLINK", "https://shopee.com.br/user/voucher-wallet")
    r.check(shp.afiliacao_vigente(outro) is False, "04.outro_link_nao_vigente")
    r.check(shp.afiliacao_vigente(Afiliacao(NOSSO_CARRINHO, "https://shopee.com.br/cart/")),
            "04.fixo_vigente")
    for curta in (FADA_RESGATE, PROMO_RESGATE):
        rede = _Rede(_proibida, api=_proibida)

        async def fluxo(curta=curta):
            registrar_link(curta, outro, "shopee")
            _limpar_memoria()
            return await normalizacao_links._normalizar_um(curta, None)
        _orig, conv, ident = _com_rede(rede, fluxo)
        r.check(getattr(conv, "publicada", None) == NOSSA_CARTEIRA and ident == "shopee",
                "04.virou_o_fixo", f"{conv} {ident}")
        r.check(rede.chamadas_exp == [] and rede.chamadas_api == [], "04.zero_rede")
        r.check(_so_cache_banco(curta).publicada == NOSSA_CARTEIRA, "04.cache_corrigido")
    # código DESCONHECIDO com entrada antiga de outro link: refaz pela regra
    # de destino — 1 expansão, 0 API
    samuel = S + "10stAzGFob"
    rede = _Rede(lambda u: EXPANDIDA[PROMO_RESGATE], api=_proibida)

    async def fluxo2():
        registrar_link(samuel, outro, "shopee")
        _limpar_memoria()
        return await normalizacao_links._normalizar_um(samuel, None)
    _o, conv, _i = _com_rede(rede, fluxo2)
    r.check(getattr(conv, "publicada", None) == NOSSA_CARTEIRA and rede.chamadas_api == [],
            "04.desconhecido_vira_fixo_sem_api", f"{conv} {rede.chamadas_api}")
    # entrada do fixo: servida pelo cache, nem chega à afiliação
    chamou = []
    real = shp.afilia

    async def espia(url, sessao):
        chamou.append(url)
        return await real(url, sessao)

    async def fluxo3():
        registrar_link(FADA_CARRINHO, Afiliacao(NOSSO_CARRINHO, "https://shopee.com.br/cart/"),
                       "shopee")
        _limpar_memoria()
        plat = normalizacao_links.registry.resolver(FADA_CARRINHO)
        antes = plat.afilia
        object.__setattr__(plat, "afilia", espia) if hasattr(plat, "__dataclass_fields__") \
            else setattr(plat, "afilia", espia)
        try:
            return await normalizacao_links._normalizar_um(FADA_CARRINHO, None)
        finally:
            object.__setattr__(plat, "afilia", antes) if hasattr(plat, "__dataclass_fields__") \
                else setattr(plat, "afilia", antes)
    _o, conv, _i = _com_rede(_Rede(_proibida, api=_proibida), fluxo3)
    r.check(getattr(conv, "publicada", None) == NOSSO_CARRINHO and chamou == [],
            "04.fixo_servido_do_cache", f"{conv} {chamou}")


def test_05_regra_de_destino(r):
    casos = (
        # Fada "🛒 Carrinho" antigo (23–24/09), código fora da tabela
        (S + "6L4Dk5LIyF", "https://shopee.com.br/cart?mmp_pid=an_18105130010&utm_source=x",
         NOSSO_CARRINHO),
        # Samuel "⭐️ Resgate aqui" levando à carteira
        (S + "10stAzGFob", "https://shopee.com.br/user/voucher-wallet/?mmp_pid=an_1&utm_term=y",
         NOSSA_CARTEIRA),
        (S + "AbCdEf1234", "https://www.shopee.com.br/cart", NOSSO_CARRINHO),
    )
    for curta, longa, nosso in casos:
        rede = _Rede(lambda u, longa=longa: longa, api=_proibida)
        res = _com_rede(rede, lambda: shp.afilia(curta, None))
        r.check(getattr(res, "publicada", None) == nosso, f"05.{curta[-10:]}.fixo", str(res))
        r.check(res.canonica == shp.limpa_url(longa), f"05.{curta[-10:]}.canonica_de_sempre")
        r.check(len(rede.chamadas_exp) == 1 and rede.chamadas_api == [],
                f"05.{curta[-10:]}.so_expansao", f"{rede.chamadas_exp} {rede.chamadas_api}")
    # qualquer outra página segue para a API, como sempre
    for longa in ("https://shopee.com.br/m/cupom-de-desconto?mmp_pid=an_1",   # página própria
                  "https://shopee.com.br/cart?shopid=1&itemid=2",             # não é a página pura
                  "https://shopee.com.br/user/voucher-wallet/detalhe",
                  "https://shopee.com.br/user/purchase",
                  PRODUTO):
        rede = _Rede(lambda u, longa=longa: longa, api=lambda u: "https://s.shopee.com.br/APIX")
        res = _com_rede(rede, lambda: shp.afilia(S + "40aG8LLgaE", None))
        r.check(getattr(res, "publicada", None) == "https://s.shopee.com.br/APIX"
                and rede.chamadas_api == [shp.limpa_url(longa)],
                f"05.api.{longa[22:50]}", f"{res} {rede.chamadas_api}")


def test_06_casamento_exato(r):
    conhecidos = (FADA_RESGATE + "?share=1", FADA_RESGATE + "/", FADA_RESGATE + ".",
                  "http://s.shopee.com.br/1qU7Zs67MB", "https://S.SHOPEE.COM.BR/1qU7Zs67MB",
                  FADA_RESGATE + "#x")
    for u in conhecidos:
        rede = _Rede(_proibida, api=_proibida)
        res = _com_rede(rede, lambda: shp.afilia(u, None))
        r.check(getattr(res, "publicada", None) == NOSSA_CARTEIRA and rede.chamadas_exp == [],
                f"06.conhecido.{u[8:]}", str(res))
    desconhecidos = ("https://s.shopee.com.br/1qu7zs67mb",        # outra caixa = outro código
                     "https://shope.ee/1qU7Zs67MB",                # outro encurtador
                     "https://s.shopee.com/1qU7Zs67MB",
                     "https://s.shopee.com.br/1qU7Zs67MB/extra",
                     "https://s.shopee.com.br/x1qU7Zs67MB")
    for u in desconhecidos:
        rede = _Rede(lambda x: PRODUTO, api=lambda x: "https://s.shopee.com.br/APIY")
        res = _com_rede(rede, lambda: shp.afilia(u, None))
        r.check(rede.chamadas_exp == [u] and getattr(res, "publicada", None) ==
                "https://s.shopee.com.br/APIY", f"06.desconhecido.{u[8:]}", str(res))


def test_07_produto_intacto(r):
    rede = _Rede(lambda u: PRODUTO, api=lambda u: "https://s.shopee.com.br/PROD777")
    res = _com_rede(rede, lambda: shp.afilia(S + "AAGNE4pfS9", None))
    r.check(res == Afiliacao("https://s.shopee.com.br/PROD777", PRODUTO_LIMPO), "07.resultado",
            str(res))
    r.check(rede.chamadas_exp == [S + "AAGNE4pfS9"] and rede.chamadas_api == [PRODUTO_LIMPO],
            "07.expansao_e_api", f"{rede.chamadas_exp} {rede.chamadas_api}")
    r.check(shp.afiliacao_vigente(res), "07.vigente")


def test_08_cache_por_destino(r):
    fontes = (S + "AAGNE4pfS9", S + "50ZTgk4qQ4", S + "Zz9Yy8Xx7W")
    rotas = {fontes[0]: PRODUTO,
             fontes[1]: PRODUTO.replace("sp_atk=x", "sp_atk=outro&utm_source=an_2"),
             fontes[2]: PRODUTO + "&uls_trackid=zz"}
    n = {"api": 0}

    def api(u):
        n["api"] += 1
        return f"https://s.shopee.com.br/PRODAPI{n['api']}"
    rede = _Rede(lambda u: rotas[u], api=api)

    async def fluxo():
        a = await shp.afilia(fontes[0], None)
        b = await shp.afilia(fontes[1], None)
        _limpar_memoria()                                # reinício: só o banco
        c = await shp.afilia(fontes[2], None)
        return a, b, c
    a, b, c = _com_rede(rede, fluxo)
    r.check(rede.chamadas_api == [PRODUTO_LIMPO], "08.uma_chamada_api", str(rede.chamadas_api))
    r.check(len(rede.chamadas_exp) == 3, "08.cada_fonte_expandida_uma_vez")
    r.check(a == b == c and a.canonica == PRODUTO_LIMPO, "08.mesmo_resultado", f"{a} {b} {c}")
    # outro produto: outra chamada
    rede2 = _Rede(lambda u: "https://shopee.com.br/Outro-i.1.2?sp_atk=q",
                  api=lambda u: "https://s.shopee.com.br/OUTROPROD")

    async def fluxo2():
        await shp.afilia(fontes[0], None)
        return await shp.afilia(fontes[1], None)
    _com_rede(rede2, fluxo2)
    r.check(rede2.chamadas_api == ["https://shopee.com.br/Outro-i.1.2"], "08.produto_novo_uma_vez",
            str(rede2.chamadas_api))
    # live: a canônica da live continua a de sempre
    url_live = ("https://live.shopee.com.br/universal-link/share?from=live&mmp_pid=an_1"
                "&session=7187289&share_user_id=293588261&utm_medium=affiliates")
    rede3 = _Rede(lambda u: url_live, api=lambda u: "https://s.shopee.com.br/LIVE1")

    async def fluxo3():
        return (await shp.afilia(fontes[0], None), await shp.afilia(fontes[1], None))
    x, y = _com_rede(rede3, fluxo3)
    r.check(x == y and x.canonica == "https://live.shopee.com.br/live/7187289"
            and len(rede3.chamadas_api) == 1, "08.live", f"{x} {y} {rede3.chamadas_api}")


def test_09_destino_so_reusa_link_da_api(r):
    # flapremios gravado por repasse direto (publicada = a própria URL)
    flap = "https://flapremios.com.br/premio"
    rede = _Rede(lambda u: flap, api=lambda u: "https://s.shopee.com.br/FLAPAPI")

    async def fluxo():
        registrar_link(flap, flap, "shopee")
        return await shp.afilia(S + "AAGNE4pfS9", None)
    res = _com_rede(rede, fluxo)
    r.check(rede.chamadas_api == [flap] and res.publicada == "https://s.shopee.com.br/FLAPAPI",
            "09.repasse_nao_reusado", f"{res} {rede.chamadas_api}")
    # entrada envenenada (canônica curta) sob a chave do destino
    rede = _Rede(lambda u: PRODUTO, api=lambda u: "https://s.shopee.com.br/NOVO1")

    async def fluxo2():
        registrar_link(PRODUTO_LIMPO, Afiliacao("https://s.shopee.com.br/RUIM", S + "AAGNE4pfS9"),
                       "shopee")
        return await shp.afilia(S + "50ZTgk4qQ4", None)
    res = _com_rede(rede, fluxo2)
    r.check(rede.chamadas_api == [PRODUTO_LIMPO] and res.publicada == "https://s.shopee.com.br/NOVO1",
            "09.envenenada_nao_reusada", f"{res} {rede.chamadas_api}")


def test_10_recuperacao_nao_grava_destino(r):
    rede = _Rede(lambda u: PRODUTO,
                 api=lambda u: "https://s.shopee.com.br/RECUP9" if u == PRODUTO_CANONICO else None)
    res = _com_rede(rede, lambda: shp.afilia(S + "AAGNE4pfS9", None))
    r.check(res.publicada == "https://s.shopee.com.br/RECUP9", "10.recuperou", str(res))
    r.check(_so_cache_banco(PRODUTO_LIMPO) is None and _so_cache_banco(PRODUTO_CANONICO) is None,
            "10.sem_destino_gravado")
    r.check(_so_cache_banco(S + "AAGNE4pfS9") == res, "10.fonte_gravada")


# ── teto — servidor HTTP local de verdade ─────────────────────────
def _rodar_api(comportamento, prazos, espera):
    """Chama _chamar_servico_afiliados contra um servidor local cujas
    respostas seguem `comportamento` (lista: 'trava' | 'erro' | link)."""
    async def _run():
        chegadas = []
        liberar = asyncio.Event()       # solta as travadas no fim (sem esperar o servidor)

        async def handler(request):
            i = len(chegadas)
            chegadas.append(time.monotonic())
            acao = comportamento[min(i, len(comportamento) - 1)]
            if acao == "trava":
                try:
                    await asyncio.wait_for(liberar.wait(), 30)
                except asyncio.TimeoutError:
                    pass
            if acao == "erro":
                return web.json_response({"errors": [{"message": "x"}]})
            return web.json_response({"data": {"generateShortLink": {"shortLink": acao}}})
        app = web.Application()
        app.router.add_post("/graphql", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        porta = site._server.sockets[0].getsockname()[1]
        reais = (shp._ENDPOINT_AFILIADOS, shp._TIMEOUTS_AFILIACAO,
                 shp._TENTATIVAS_AFILIACAO, shp._ESPERA_ENTRE_TENTATIVAS)
        shp._ENDPOINT_AFILIADOS = f"http://127.0.0.1:{porta}/graphql"
        shp._TIMEOUTS_AFILIACAO, shp._TENTATIVAS_AFILIACAO = prazos, len(prazos)
        shp._ESPERA_ENTRE_TENTATIVAS = espera
        config._SEM_HTTP = asyncio.Semaphore(20)
        t0 = time.monotonic()
        try:
            async with aiohttp.ClientSession() as sessao:
                link = await shp._chamar_servico_afiliados(PRODUTO_LIMPO, sessao)
            return link, time.monotonic() - t0, [c - t0 for c in chegadas]
        finally:
            (shp._ENDPOINT_AFILIADOS, shp._TIMEOUTS_AFILIACAO,
             shp._TENTATIVAS_AFILIACAO, shp._ESPERA_ENTRE_TENTATIVAS) = reais
            liberar.set()
            await runner.cleanup()
    return asyncio.run(_run())


def test_11_teto_da_api(r):
    # constantes de produção e o pior caso
    r.check(shp._TIMEOUTS_AFILIACAO == (3, 5, 8) and shp._TENTATIVAS_AFILIACAO == 3,
            "11.prazos_crescentes", str(shp._TIMEOUTS_AFILIACAO))
    pior = sum(shp._TIMEOUTS_AFILIACAO) + sum(
        t * shp._ESPERA_ENTRE_TENTATIVAS for t in range(1, shp._TENTATIVAS_AFILIACAO))
    r.check(pior <= 18, "11.pior_caso_ate_18s", str(pior))          # antes: 40,5 s
    # escala 1:10 — travada abandonada no prazo e refeita
    link, total, chegadas = _rodar_api(["trava", "https://s.shopee.com.br/OK2"],
                                       (0.3, 0.5, 0.8), 0.05)
    r.check(link == "https://s.shopee.com.br/OK2", "11.segunda_tentativa_responde", str(link))
    r.check(0.3 <= chegadas[1] < 0.6 and total < 0.8, "11.abandonou_a_travada_no_prazo",
            f"{chegadas} {total:.2f}")
    # tudo travado: devolve None dentro do teto (0,3+0,05+0,5+0,1+0,8)
    link, total, chegadas = _rodar_api(["trava"], (0.3, 0.5, 0.8), 0.05)
    r.check(link is None and len(chegadas) == 3, "11.desiste_apos_3", f"{link} {chegadas}")
    r.check(1.7 <= total < 2.3, "11.dentro_do_teto", f"{total:.2f}")
    r.check(chegadas[1] - chegadas[0] >= 0.3 and chegadas[2] - chegadas[1] >= 0.5,
            "11.prazo_cresce", str(chegadas))
    # erro do serviço (resposta rápida): tenta de novo logo
    link, total, chegadas = _rodar_api(["erro", "erro", "https://s.shopee.com.br/OK3"],
                                       (0.3, 0.5, 0.8), 0.05)
    r.check(link == "https://s.shopee.com.br/OK3" and total < 0.5, "11.erro_rapido",
            f"{link} {total:.2f}")
    # resposta normal: uma chamada
    link, total, chegadas = _rodar_api(["https://s.shopee.com.br/OK1"], shp._TIMEOUTS_AFILIACAO,
                                       shp._ESPERA_ENTRE_TENTATIVAS)
    r.check(link == "https://s.shopee.com.br/OK1" and len(chegadas) == 1, "11.normal")


def test_12_cache_30_dias(r):
    r.check(config.TTL_LINK_INATIVO == 30 * 86400, "12.ttl_30_dias", str(config.TTL_LINK_INATIVO))
    from database_manutencao import TTL_LINK_INATIVO
    r.check(TTL_LINK_INATIVO == 30 * 86400, "12.limpeza_usa_30_dias")
    agora = time.time()

    def gravar(url, idade_dias):
        registrar_link(url, Afiliacao("https://s.shopee.com.br/X", PRODUTO_LIMPO), "shopee")
        with _db() as db:
            db.execute("UPDATE links_cache SET ts=? WHERE url_orig=?",
                       (agora - idade_dias * 86400, _cache_key(url)))

    def existe(url):
        with _db() as db:
            return db.execute("SELECT ts FROM links_cache WHERE url_orig=?",
                              (_cache_key(url),)).fetchone()

    async def fluxo():
        gravar(S + "D8", 8)            # morreria com 7 dias
        gravar(S + "D29", 29)
        gravar(S + "D31", 31)
        gravar(S + "USO", 40)
        _limpar_memoria()
        consultar_link(S + "USO")      # usado hoje: renova
        db_limpar()
        return existe(S + "D8"), existe(S + "D29"), existe(S + "D31"), existe(S + "USO")
    d8, d29, d31, uso = _com_rede(_Rede(_proibida, api=_proibida), fluxo)
    r.check(d8 is not None and d29 is not None, "12.ate_30_dias_fica", f"{d8} {d29}")
    r.check(d31 is None, "12.mais_de_30_sai", str(d31))
    r.check(uso is not None and uso[0] > agora - 60, "12.uso_renova", str(uso))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "SHOPEE · carteira/carrinho fixos, destino, teto, 30 dias"))
