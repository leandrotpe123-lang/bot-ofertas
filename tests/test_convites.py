"""
CONVITES — limpeza cirúrgica de convite de OUTRO grupo (filtros_linha).

Pedido do dono (06/11): remover https://t.me/..., http://t.me/..., t.me/...
sem esquema, telegram.me/..., t.me/+hash, t.me/joinchat/... e @ de grupo
CONHECIDO, com a chamada associada; preservar produto, preço, cupom e link
afiliado; não tocar @ de vendedor; não descartar oferta válida.

Casos reais (prévias públicas, 26/10–04/11/2026):
  fadadoscupons/18169  "Vários produtos de graça aqui: @fadapromos"
  fadadoscupons/16987  "👀 Entrem aqui @fadapromos, às 8h talvez tenha coisa boa…"
  fadadoscupons/15411  "🏆 ENTRE NO GRUPO DE PROMOS:\\nt.me/fadapromos"
  fadadoscupons/15418  "🏆 ENTRE NO GRUPO DE PROMOS: t.me/fadapromos"
  fadadoscupons/18198  "Barato: https://t.me/fadapromos/14338"
  promotom/109960      "Esse aí forrou… Essa saiu primeiro aqui! https://t.me/…"
  promotom/109820      "Cupons Kabum e fuminhos Amazon saem primeiro aqui:\\nt.me/fumotom"
  promotom/111490      "Mais um pego pelo MAIS RÁPIDO: https://t.me/fumotom"
  fadadoscupons/15622  "Azeite por R$0,24: t.me/fadapromos/…" → fica "Azeite por R$0,24"
  fadadoscupons/15767  "Último cupom 40% liberado para resgate: t.me/…" → fica o texto

  01  formas do convite: https, http, sem esquema, telegram.me, +hash,
      joinchat, hiperlink [rótulo](t.me/x), tg://resolve
  02  @ de grupo conhecido sai; @ de vendedor/loja desconhecido fica
  03  chamada associada sai: mesma linha, linha separada (rótulo órfão)
  04  conteúdo fica: mesma linha do preço, junto de link afiliado, cupom,
      título de produto com convite colado
  05  texto sem convite: idêntico (e o @ de e-mail não é tocado)
  06  casos reais do corpus, um a um
  07  normalizar() de ponta a ponta: oferta com convite continua oferta, sem
      o convite (Fada dos Cupons 18169, 15411)
  08  sem_convites() — o mesmo para o texto do comentário repassado
  09  regressão do #43: roleta, mídia visual, texto curto sem mídia

    python3 tests/test_convites.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar  # noqa: E402

preparar()

from database import _init_db                                   # noqa: E402

_init_db()

try:
    import yarl  # noqa: F401
except ImportError:
    _yarl = types.ModuleType("yarl")

    class _URL(str):
        def __new__(cls, valor="", **_kw):
            return str.__new__(cls, valor)
    _yarl.URL = _URL
    sys.modules["yarl"] = _yarl

import plataformas                                              # noqa: E402
from pipeline import normalizacao                               # noqa: E402
from pipeline.filtros_linha import filtrar, sem_convites        # noqa: E402
from pipeline.ingestao import MensagemBruta, links_de           # noqa: E402
from pipeline.institucional import tem_contexto                 # noqa: E402
from pipeline.normalizacao_texto import limpar_texto            # noqa: E402

plataformas.inicializar()

PRODUTO = "Air Fryer Mondial 4L"
PRECO = "R$ 199"
AFILIADO = "https://amzn.to/3AbCdEf"


def f(texto: str) -> str:
    return filtrar(limpar_texto(texto))


def sem_rastro(saida: str) -> bool:
    s = saida.lower()
    return not any(x in s for x in ("t.me", "telegram.me", "joinchat", "@fadapromos",
                                     "@fumotom", "tg://", "fadapromos", "fumotom"))


def test_01_formas_do_convite(r):
    formas = {
        "https": "https://t.me/fadapromos",
        "http": "http://t.me/fadapromos",
        "sem_esquema": "t.me/fadapromos",
        "telegram_me": "telegram.me/fadapromos",
        "telegram_me_https": "https://telegram.me/fadapromos",
        "hash": "https://t.me/+BfqIYp3ni5o2MjMx",
        "hash_sem_esquema": "t.me/+BfqIYp3ni5o2MjMx",
        "joinchat": "https://t.me/joinchat/AAAAAEkk2WdoDrB4-Q8-gg",
        "joinchat_sem_esquema": "t.me/joinchat/AAAAAEkk2WdoDrB4-Q8-gg",
        "post": "https://t.me/fadapromos/14338",
        "markdown": "[clique aqui](https://t.me/fadapromos)",
        "tg": "tg://resolve?domain=fadapromos",
    }
    falhas = {}
    for nome, conv in formas.items():
        texto = f"{PRODUTO}\n{PRECO}\n{AFILIADO}\n\nEntre no grupo: {conv}"
        s = f(texto)
        if not (sem_rastro(s) and PRODUTO in s and PRECO in s and AFILIADO in s
                and "Entre no grupo" not in s):
            falhas[nome] = s
    r.check(falhas == {}, "01.todas_as_formas_saem_com_a_chamada", str(falhas))


def test_02_arroba_conhecido_sai_vendedor_fica(r):
    s = f(f"{PRODUTO}\n{PRECO}\n{AFILIADO}\n\nMais ofertas aqui: @fadapromos")
    r.check(sem_rastro(s) and "Mais ofertas" not in s and PRODUTO in s, "02.conhecido_sai", s)
    for conhecido in ("@fumotom", "@FUMOTOM", "@promotomofertaschat", "@NerdAvisoBot",
                      "@SamuelF3lipePromo", "@cupomlinkbot"):
        s = f(f"{PRODUTO}\n{AFILIADO}\nSegue lá: {conhecido}")
        r.check(conhecido.lower() not in s.lower() and "Segue" not in s,
                f"02.conhecido.{conhecido}", s)
    vendedor = f"{PRODUTO}\nVendido por @lojadojoao\n{PRECO}\n{AFILIADO}"
    r.check(f(vendedor) == f(vendedor.replace("@lojadojoao", "@lojadojoao"))
            and "@lojadojoao" in f(vendedor), "02.vendedor_fica", f(vendedor))
    r.check("@xiaomi_oficial" in f(f"{PRODUTO} da @xiaomi_oficial\n{AFILIADO}"),
            "02.loja_desconhecida_fica")


def test_03_chamada_associada(r):
    mesma = f(f"{PRODUTO}\n{PRECO}\n{AFILIADO}\n\n🏆 ENTRE NO GRUPO DE PROMOS: t.me/fadapromos")
    r.check("ENTRE NO GRUPO" not in mesma and sem_rastro(mesma), "03.mesma_linha", mesma)
    separada = f(f"{PRODUTO}\n{PRECO}\n{AFILIADO}\n\n🏆 ENTRE NO GRUPO DE PROMOS:\nt.me/fadapromos")
    r.check("ENTRE NO GRUPO" not in separada and sem_rastro(separada), "03.linha_separada",
            separada)
    seta = f(f"{PRODUTO}\n{AFILIADO}\nEntrem -> @fadapromos")
    r.check("Entrem" not in seta, "03.seta", seta)
    ponteiro = f(f"{PRODUTO}\n{AFILIADO}\nbugs no t.me/fadapromos")
    r.check("bugs no" not in ponteiro, "03.preposicao_pendurada", ponteiro)


def test_04_conteudo_fica(r):
    preco = f(f"{PRODUTO}\n{PRECO} no PIX t.me/fumotom\n{AFILIADO}")
    r.check(f"{PRECO} no PIX" in preco and sem_rastro(preco), "04.mesma_linha_do_preco", preco)
    junto = f(f"{PRODUTO}\n{PRECO}\n{AFILIADO} https://t.me/fumotom")
    r.check(AFILIADO in junto and sem_rastro(junto), "04.junto_do_afiliado", junto)
    cupom = f(f"{PRODUTO}\n🎟 Cupom: SAUDE10 t.me/fumotom\n{AFILIADO}")
    r.check("SAUDE10" in cupom and sem_rastro(cupom), "04.cupom", cupom)
    cupom_palavra = f(f"{PRODUTO}\nUse o cupom PRIMEIRO no grupo: t.me/fumotom\n{AFILIADO}")
    r.check("PRIMEIRO" in cupom_palavra and sem_rastro(cupom_palavra), "04.cupom_sem_digito",
            cupom_palavra)
    titulo = f(f"{PRODUTO} 🔥 t.me/fumotom\n{PRECO}\n{AFILIADO}")
    r.check(PRODUTO in titulo and sem_rastro(titulo), "04.titulo_com_convite_colado", titulo)
    md = f(f"{PRODUTO}\n🎟 {PRECO} OFF [entra lá](https://t.me/fadapromos)\n{AFILIADO}")
    r.check(f"🎟 {PRECO} OFF" in md and "[" not in md and "]" not in md and "()" not in md
            and sem_rastro(md), "04.hiperlink_sai_inteiro", md)
    pct = f(f"Último cupom 40% liberado para resgate: t.me/fadadoscupons/15764\n{AFILIADO}")
    r.check("40% liberado para resgate" in pct and sem_rastro(pct), "04.porcentagem", pct)


def test_05_texto_sem_convite_identico(r):
    textos = [
        f"{PRODUTO}\n\n✅ {PRECO}\n🎟 Cupom: SAUDE10\n\n{AFILIADO}\n\n-Anúncio",
        "🔥 Smart TV 43\" Philco\n\n✅ R$ 1149\nResgate o cupom de R$ 100 off nessa pagina:\n"
        "https://s.shopee.com.br/50aJ4xdzL0",
        f"Fale com a loja: contato@loja.com.br\n{AFILIADO}",
    ]
    falhas = {}
    for t in textos:
        saida = f(t)
        perdidas = [l for l in limpar_texto(t).split("\n") if l.strip() and l.strip() not in saida]
        if perdidas:
            falhas[t[:30]] = perdidas
    r.check(falhas == {}, "05.nenhuma_linha_some", str(falhas))
    r.check("contato@loja.com.br" in f(textos[2]), "05.email_nao_e_arroba")


REAIS = {
    "fadadoscupons/18169": ("Resgatem logo esse cupom, R$25 sem mínimo!\n"
                            "https://s.shopee.com.br/8pmyRxQumx\n\n"
                            "Vários produtos de graça aqui: @fadapromos",
                            "Resgatem logo esse cupom, R$25 sem mínimo!", "Vários produtos"),
    "fadadoscupons/16987": ("👀 Entrem aqui @fadapromos, às 8h talvez tenha coisa boa na Amazon\n\n"
                            "https://link.amazon/B0gAcKEQ6",
                            "https://link.amazon/B0gAcKEQ6", "Entrem"),
    "fadadoscupons/15411": ("🚨 É AMANHÃ! AMAZON PRIME DAY!\n\nℹ️ Começa meia-noite 01/07 - 07/07\n\n"
                            "👇 ASSINE O PRIME (30 DIAS GRÁTIS): https://link.amazon/B0cC1MtcG\n\n"
                            "🏆 ENTRE NO GRUPO DE PROMOS:\nt.me/fadapromos",
                            "ASSINE O PRIME", "ENTRE NO GRUPO"),
    "fadadoscupons/15418": ("🚨 Prime Day começa em menos de 1 hora!\n\n"
                            "👇 Assine o Prime (30 dias Grátis): https://link.amazon/B0cC1MtcG\n\n"
                            "🏆 ENTRE NO GRUPO DE PROMOS: t.me/fadapromos",
                            "Assine o Prime", "ENTRE NO GRUPO"),
    "fadadoscupons/18198": ("Barato: https://t.me/fadapromos/14338\n"
                            "https://s.shopee.com.br/AAHdNLxGSX", "s.shopee.com.br", "Barato"),
    "promotom/109960": ("Esse aí forrou. 32GB DDR5 6000MHz Corsair RGB que saiu a 800 conto. "
                        "Essa saiu primeiro aqui! https://t.me/promotomofertaschat/1180998",
                        None, "saiu primeiro"),
    "promotom/109820": ("Cupons Kabum e fuminhos Amazon saem primeiro aqui:\nt.me/fumotom",
                        None, "saem primeiro"),
    "promotom/111490": ("Mais um pego pelo MAIS RÁPIDO: https://t.me/fumotom", None, "RÁPIDO"),
    "fadadoscupons/15622": ("Azeite por R$0,24: https://t.me/fadapromos/11521",
                            "Azeite por R$0,24", "t.me"),
}


def test_06_casos_reais(r):
    falhas = {}
    for post, (texto, fica, sai) in REAIS.items():
        s = f(texto)
        ok = sem_rastro(s) and (fica is None or fica in s) and (sai not in s)
        if not ok:
            falhas[post] = s
    r.check(falhas == {}, "06.casos_reais", str(falhas))


def test_07_oferta_com_convite_continua_oferta(r):
    """O normalizar() real; afiliação simulada (sem rede)."""
    async def resolver(converter, msg_id=0):
        from plataformas import registry
        mapa = {u: "https://afiliado.teste/" + str(abs(hash(u)) % 10 ** 8)
                for u in converter if registry.resolver(u) is not None}
        return mapa, dict(mapa), ["shopee"] * len(mapa)

    real = normalizacao.resolver_e_afiliar
    normalizacao.resolver_e_afiliar = resolver
    try:
        res = {}
        for i, post in enumerate(("fadadoscupons/18169", "fadadoscupons/15411")):
            texto = REAIS[post][0]
            b = MensagemBruta(msg_id=970000 + i, chat="-1002050488946", texto=texto,
                              links=links_de(texto), tem_midia=False, media_obj=None)
            n = asyncio.run(normalizacao.normalizar(b))
            res[post] = n.texto_limpo if n else None
    finally:
        normalizacao.resolver_e_afiliar = real
    r.check(all(v is not None for v in res.values()), "07.continua_oferta", str(res))
    r.check(all(sem_rastro(v or "") for v in res.values()), "07.sem_convite", str(res))
    r.check("R$25 sem mínimo" in (res["fadadoscupons/18169"] or ""), "07.cupom_fica",
            str(res))


def test_08_sem_convites_para_comentario(r):
    r.check(sem_convites("Esgotado ❌") == "Esgotado ❌", "08.sem_convite_igual")
    r.check(sem_convites("Precinho\nt.me/fumotom") == "Precinho", "08.linha_do_convite_sai")
    r.check(sem_convites("Entrem -> @fadapromos") == "", "08.so_chamada_vazia")


def test_09_regressao_do_43(r):
    r.check(tem_contexto("Nova Roleta Shopee\n\nhttps://s.shopee.com.br/x") is True,
            "09.nova_roleta_sem_midia")
    r.check(tem_contexto("🔥 Nova Chance Roleta Shopee\n\nhttps://s.shopee.com.br/x") is True,
            "09.nova_chance_roleta")
    r.check(tem_contexto("roleta\n\nhttps://s.shopee.com.br/x") is True, "09.roleta_indicador")
    r.check(tem_contexto("Libera 17h\n\nhttps://meli.la/2a2yYw1", midia_visual=True) is True,
            "09.legenda_curta_com_midia_visual")
    r.check(tem_contexto("Libera 17h\n\nhttps://meli.la/2a2yYw1") is False,
            "09.curta_sem_midia_continua_no_filtro")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CONVITES · limpeza cirúrgica · casos reais"))
