"""
MERCADO LIVRE — cupons: NÃO DUPLICAR e EDITAR POR LINK DE LISTA.

Produção (02/10, @ofertasconvertidas): 1367 (12 códigos, `/sec/`) virou
o post 24553; 1368 (15 códigos, cada um com a sua lista) virou o post
24554 — DUPLICADO. Duas causas, as duas no gate de natureza:
  · "10% OFF (mín. R$79, limite R$100) — PROMOML": o separador "—" não
    era linha de cupom, "R$79" virou PREÇO DE ITEM e o link de resgate
    (vitrine → MLB8923631) ancorou como PRODUTO;
  · o título "🔥 Cupons do Mercado Livre ativos agora:" (seguido de
    linha em branco) saía do post como "rótulo órfão", e sem título o
    post nem era reconhecido como de cupom.
E o pedido: a versão com LISTAS edita o post do mecanismo (`/sec/`)
— ontem (01/10) o Samuel 118773 casou com o Promotom 110373 e a
edição foi BLOQUEADA (COMPOSICAO_PERDERIA_IDENTIDADE), porque a lista
não traz todos os cupons. Agora o post vira a versão com listas e
mantém no ar, num bloco final, os cupons que só ele tinha.

CORPUS REAL (t.me/s, texto e códigos monoespaçados fiéis): Promotom
110373, Samuel 118773 e 118766 (produto), @ofertasconvertidas 1367 e
1368. Links curtos bloqueados aqui: URLs longas sintéticas (lista
`_Container_…?coupon_campaign_id=`, vitrine → produto MLB8923631).

  01  linha de cupom pelo CÓDIGO declarado, qualquer separador;
      negativos (sem código, sem desconto, com URL, preço de produto)
  02  natureza: 1368 com produto-vitrine é CUPOM; produto do Samuel
      continua PRODUTO; sem códigos, o gate é o de antes
  03  título com conteúdo depois não é rótulo órfão; órfão de verdade
      continua saindo
  04  1367 → 1368: UM post; editado para a versão com listas; PROMOAQU,
      DESCONTOSMELI e OFERTA continuam no ar com o resgate; exibida =
      união; canal de cupons igual
  05  ordem inversa (lista antes): UM post, o mecanismo não substitui
  06  real de 01/10: Promotom 110373 → Samuel 118773 dentro da janela:
      UM post, editado, nada some (antes: BLOQUEADO)
  07  sincronização do líder (fonte da lista edita): o bloco retido
      fica; código retido que o líder passa a trazer sai do bloco
  08  o líder tira o PRÓPRIO código: some do post (não é retido)
  09  post com mídia e texto composto > legenda: não compõe — o de
      sempre (bloqueado), sem duplicar
  10  retencao_cupons puro: falta não-cupom / código ausente / limite →
      None; rótulo do link e linha de benefício anterior entram
  11  o LÍDER do mecanismo edita a própria mensagem (tira OFERTA, põe
      uma lista): é SINCRONIZAÇÃO — espelha, não retém o que ele tirou
  12  lista COM foto sobre post SEM foto: edição de TEXTO no lugar — sem
      apagar+reenviar, a foto da lista não sobe (quem só cobre o post com
      os cupons de outra fonte não fala pela mídia dele)

    python tests/test_ml_cupons_lista.py
"""
import os
import re
import sys
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_espelho_cupons as E                                 # noqa: E402
from test_cupom_sem_codigo import PROMOTOM, SAMUEL, msg         # noqa: E402
from _harness_e5 import rodar                                   # noqa: E402
from database import db_exibida, db_get_post                    # noqa: E402
from pipeline import retencao_cupons                            # noqa: E402
from pipeline.assunto import eh_post_cupom, tem_preco_de_item   # noqa: E402
from pipeline.filtros import filtrar, filtrar_blocos            # noqa: E402
from pipeline.natureza import eh_entidade_cupom                 # noqa: E402
from pipeline.normalizacao_texto import limpar_texto            # noqa: E402
from utils.cupom import extrair_todos_cupons, linha_declara_cupom  # noqa: E402

OFERTASCONVERTIDAS = -1003817694320

# ── corpus real ───────────────────────────────────────────────────
PROMOTOM_110373 = (
    "Cupom Mercado Livre\n\n"
    "8% OFF em R$ 150, Limite de R$ 300 OFF: PROMOAQU\n"
    "10% OFF em R$ 79, Limite de R$ 100 OFF: PROMOML\n"
    "10% OFF em R$ 149, Limite de R$ 200 OFF: DESCONTOSMELI\n"
    "15% OFF em R$ 49, Limite de R$ 50 OFF: MODAML\n"
    "15% OFF em R$ 59, Limite de R$ 50 OFF: BRINCAR\n"
    "15% OFF em R$ 79, Limite de R$ 60 OFF: TEMPROMO\n"
    "15% OFF em R$ 79, Limite de R$ 200 OFF: OFERTA\n"
    "20% OFF em R$ 19, Limite de R$ 80 OFF: OFERTAS\n"
    "20% OFF em R$ 79, Limite de R$ 50 OFF: ECONOMIAMELI\n"
    "22% OFF em R$ 1, Limite de R$ 500 OFF: PROMOCERTA\n"
    "25% OFF em R$ 1, Limite de R$ 500 OFF: OFFAQUI, ECONOMIA\n\n"
    "-Resgate aqui: https://mercadolivre.com/sec/2U6U32Q")
COD_110373 = ["PROMOAQU", "PROMOML", "DESCONTOSMELI", "MODAML", "BRINCAR", "TEMPROMO",
              "OFERTA", "OFERTAS", "ECONOMIAMELI", "PROMOCERTA", "OFFAQUI, ECONOMIA"]
SAMUEL_118773 = (
    "🔥 Cupons Mercado Livre\n\n"
    "🎟 10% OFF em R$ 79, Limite de R$ 100 OFF: PROMOML\n\n"
    "🎟 15% OFF acima de R$ 79, limite R$ 60: TEMPROMO\n👉 Lista: https://meli.la/2SxES6P\n\n"
    "🎟 30% OFF, limite R$ 500: OPAECONOMIZEI\n👉 Lista: https://meli.la/2AnW4cU\n\n"
    "🎟 25% OFF, limite R$ 500: OFFAQUI\n👉 Lista: https://meli.la/2wo2vj6\n\n"
    "🎟 15% OFF em R$ 49, Limite de R$ 50 OFF: MODAML\n👉 Lista: https://meli.la/1GbyQqi\n\n"
    "🎟 15% OFF em R$ 59, Limite de R$ 50 OFF: BRINCAR\n👉 Lista: https://meli.la/1H3eMiY\n\n"
    "🎟 20% OFF em R$ 19, Limite de R$ 80 OFF: OFERTAS\n👉 Lista: https://meli.la/1adPLKK\n\n"
    "🎟 20% OFF em R$ 79, Limite de R$ 50 OFF: ECONOMIAMELI\n👉 Lista: https://meli.la/1RB11Mm\n\n"
    "🎟 22% OFF em R$ 1, Limite de R$ 500 OFF: PROMOCERTA\n👉 Lista: https://meli.la/2oz7gvp\n\n"
    "anúncio")
COD_118773 = ["PROMOML", "TEMPROMO", "OPAECONOMIZEI", "OFFAQUI", "MODAML", "BRINCAR",
              "OFERTAS", "ECONOMIAMELI", "PROMOCERTA"]
SAMUEL_118766 = ("🔥 Mixer 2 Em 1 Turbo 500w 2 Velocidades Preto\n\n💵 R$ 68\n"
                 "🎟 Cupom: PROMOML\nhttps://meli.la/1kRE82Z\n\nanúncio")
TESTE_1368 = (
    "🔥 Cupons do Mercado Livre ativos agora:\n\n"
    "🌎 Para TODOS os produtos:\n"
    "🎟️ 10% OFF (mín. R$79, limite R$100) — PROMOML\n"
    "👉 Resgatar: https://meli.la/2XoB7wc\n\n"
    "📋 Para listas selecionadas (o link é a lista de cada um):\n"
    "🎟️ 30% OFF (limite R$500) — OPAECONOMIZEI\n   🔗 https://meli.la/2LKLp7b\n"
    "🎟️ 30% OFF (limite R$500) — OFERTASMELI\n   🔗 https://meli.la/1rdfTqx\n"
    "🎟️ 20% OFF (mín. R$19, limite R$80) — OFERTAS\n   🔗 https://meli.la/2aFtMNj\n"
    "🎟️ 20% OFF (mín. R$79, limite R$50) — ECONOMIAMELI\n   🔗 https://meli.la/1RbzVjA\n"
    "🎟️ 15% OFF (mín. R$49, limite R$50) — MODAML\n   🔗 https://meli.la/1SLJVDQ\n"
    "🎟️ 15% OFF (mín. R$59, limite R$50) — BRINCAR\n   🔗 https://meli.la/27sdvpE\n"
    "🎟️ 25% OFF (limite R$500) — VEMDEOFFMELI\n   🔗 https://meli.la/2XqxPj6\n"
    "🎟️ 22% OFF (limite R$500) — MELICOMPRACERTA\n   🔗 https://meli.la/2bwk59K\n"
    "🎟️ 15% OFF (mín. R$79, limite R$60) — TEMPROMO\n   🔗 https://meli.la/2jnHjdy\n"
    "🎟️ 15% OFF (mín. R$19, limite R$20) — LIVROS15\n   🔗 https://meli.la/2Wu1dgV\n"
    "🎟️ 18% OFF (limite R$500) — MLMELHORESPROMOS\n   🔗 https://meli.la/1s5jvk2\n"
    "🎟️ 25% OFF (limite R$500) — ECONOMIA\n   🔗 https://meli.la/2NTFw4M\n"
    "🎟️ 25% OFF (limite R$500) — OFFAQUI\n   🔗 https://meli.la/17Ywnko\n"
    "🎟️ 22% OFF (limite R$500) — PROMOCERTA\n   🔗 https://meli.la/1V7p2cF\n\n"
    "👍 Bom proveito!")
COD_1368 = ["PROMOML", "OPAECONOMIZEI", "OFERTASMELI", "OFERTAS", "ECONOMIAMELI", "MODAML",
            "BRINCAR", "VEMDEOFFMELI", "MELICOMPRACERTA", "TEMPROMO", "LIVROS15",
            "MLMELHORESPROMOS", "ECONOMIA", "OFFAQUI", "PROMOCERTA"]
SO_PROMOTOM = {"PROMOAQU", "DESCONTOSMELI", "OFERTA"}      # 1367 tem, 1368 não

SEC = "https://www.mercadolivre.com.br/social/promotom?forceInApp=true"   # mecanismo
VITRINE_PRODUTO = "https://www.mercadolivre.com.br/p/MLB8923631"            # 2XoB7wc
LISTA = "https://lista.mercadolivre.com.br/_Container_he-aff-2?coupon_campaign_id={}"
_RE_URL = re.compile(r"https?://\S+")


def real(chat, texto, codigos, urls_longas, msg_id=None):
    """Como a normalização entrega: filtros de linha e de BLOCO (todo link
    convertido) e só os códigos T0 que ficaram no texto."""
    tl = filtrar(limpar_texto(texto))
    tl = filtrar_blocos(tl, {u.rstrip(".,;)>"): u for u in _RE_URL.findall(tl)})
    cup = [c for c in extrair_todos_cupons(tl, codigos) if c in tl.upper()]
    return msg(chat, tl, urls_longas=urls_longas, cupons=cup, plat="mercadolivre",
               msg_id=msg_id)


def m1367(msg_id=None):
    return real(OFERTASCONVERTIDAS, PROMOTOM_110373, COD_110373, [SEC], msg_id)


def m1368(msg_id=None, texto=TESTE_1368, codigos=COD_1368):
    urls = [VITRINE_PRODUTO] + [LISTA.format(14380300 + i) for i in range(14)]
    return real(OFERTASCONVERTIDAS, texto, codigos, urls, msg_id)


def _codigos(texto):
    return {c for c in COD_1368 + list(SO_PROMOTOM) + ["GARIMPEI"]
            if re.search(rf"(?<![A-Z0-9]){c}(?![A-Z0-9])", texto.upper())}


# ══════════════════════════════════════════════════════════════════
def test_01_linha_de_cupom_pelo_codigo(r):
    cods = ["PROMOML", "OFFAQUI"]
    for rot, linha, esperado in (
            ("travessao", "🎟️ 10% OFF (mín. R$79, limite R$100) — PROMOML", True),
            ("dois_pontos", "10% OFF em R$ 79, Limite de R$ 100 OFF: PROMOML", True),
            ("codigo_primeiro", "PROMOML - 10% OFF acima de R$79", True),
            ("seta", "25% OFF ➜ OFFAQUI", True),
            ("desconto_palavra", "R$ 20 de desconto: OFFAQUI", True),
            ("sem_codigo_declarado", "🎟️ 10% OFF (mín. R$79) — OUTROCOD", False),
            ("sem_marcador_desconto", "🎟 Cupom: PROMOML", False),
            ("preco_de_produto", "💵 R$ 68 com PROMOML", False),
            ("com_url", "10% OFF PROMOML https://meli.la/x", False),
            ("prefixo_de_outro", "10% OFF: PROMOMLX", False)):
        r.check(linha_declara_cupom(linha, cods) is esperado, f"01.{rot}", linha)
    r.check(linha_declara_cupom("🎟️ 10% OFF — PROMOML", ()) is False, "01.sem_codigos_nada")


def test_02_natureza(r):
    n = m1368()
    r.check(n.ids_globais == ["MLB8923631"], "02.vitrine_virou_produto", str(n.ids_globais))
    ta = n.texto_analise
    r.check(tem_preco_de_item(ta) is True and tem_preco_de_item(ta, n.cupons) is False,
            "02.R$79_e_condicao_do_cupom_nao_preco")
    r.check(eh_entidade_cupom(n) is True, "02.1368_e_cupom")
    prod = real(SAMUEL, SAMUEL_118766, ["PROMOML"],
                ["https://produto.mercadolivre.com.br/MLB-4012345678-mixer-_JM"])
    r.check(prod.ids_globais and eh_entidade_cupom(prod) is False,
            "02.produto_com_cupom_continua_produto", str(prod.ids_globais))
    r.check(eh_post_cupom(ta, n.cupons) is True, "02.lista_por_codigo")
    # título SEM a palavra cupom: só a lista "… — COD" diz o que o post é
    sem_titulo = ("🔥 Mercado Livre hoje\n\n"
                  "🎟️ 10% OFF (mín. R$79, limite R$100) — PROMOML\n"
                  "🎟️ 25% OFF (limite R$500) — OFFAQUI\n"
                  "👉 Resgatar: https://meli.la/2XoB7wc")
    n2 = real(OFERTASCONVERTIDAS, sem_titulo, ["PROMOML", "OFFAQUI"], [VITRINE_PRODUTO])
    r.check(n2.ids_globais and eh_post_cupom(n2.texto_analise) is False
            and eh_entidade_cupom(n2) is True, "02.lista_sem_palavra_cupom_no_titulo",
            n2.texto_analise)


def test_03_titulo_nao_e_rotulo_orfao(r):
    tl = filtrar(limpar_texto(TESTE_1368))
    mapa = {u.rstrip(".,;)>"): u for u in _RE_URL.findall(tl)}
    r.check(filtrar_blocos(tl, mapa).startswith("🔥 Cupons do Mercado Livre ativos agora:"),
            "03.titulo_fica")
    orfao = "Oferta X\nhttps://meli.la/a\n\nTelegram:"
    r.check(filtrar_blocos(orfao, {"https://meli.la/a": "x"}).endswith("https://meli.la/a"),
            "03.orfao_no_fim_sai")
    so_rotulo = "Cupons de hoje:\n\nhttps://kabum.com.br/x"
    r.check(filtrar_blocos(so_rotulo, {}) == "", "03.titulo_sem_conteudo_sai",
            repr(filtrar_blocos(so_rotulo, {})))


def test_04_1367_depois_1368_um_post_editado_nada_some(r):
    out = {}

    async def corpo(c):
        p1 = m1367()
        await E.pub(p1, score=26)
        out["p1"] = E.principal_de(c, p1)
        p2 = m1368()
        await E.pub(p2, score=22)
        out["p2"] = E.principal_de(c, p2)
        out["exib"] = db_exibida(out["p1"])
        out["estado"] = db_get_post(out["p1"])
    c = E.cenario(corpo)
    posts = list(c.no_canal(E.GRUPO_DESTINO).values())
    r.check(len(posts) == 1 and out["p1"] == out["p2"], "04.um_post_sem_duplicar",
            f"{[p.id for p in posts]} {out['p1']} {out['p2']}")
    txt = posts[0].message if posts else ""
    r.check("2LKLp7b" in txt and "OPAECONOMIZEI" in txt, "04.editado_para_as_listas")
    r.check(SO_PROMOTOM <= _codigos(txt) and "/sec/2U6U32Q" in txt,
            "04.cupons_so_do_promotom_continuam_com_resgate", txt[-300:])
    r.check(retencao_cupons.MARCA in txt, "04.bloco_marcado")
    cups = {k.split("|")[2] for k in out["exib"] if "|cup|" in k}
    r.check(cups == set(COD_1368) | SO_PROMOTOM
            and any("|dest|" in k for k in out["exib"])
            and not any(k == "mercadolivre|MLB8923631" for k in out["exib"]),
            "04.exibida_uniao_sem_produto_vitrine", str(sorted(out["exib"]))[:300])
    r.check(out["estado"]["texto"] == txt, "04.estado_descreve_o_texto_no_ar")
    cupons = list(c.no_canal(E.CANAL).values())
    r.check(len(cupons) == 1 and cupons[0].message == txt, "04.canal_de_cupons_igual")


def test_05_ordem_inversa(r):
    out = {}

    async def corpo(c):
        p2 = m1368()
        await E.pub(p2, score=22)
        p1 = m1367()
        await E.pub(p1, score=26)
        out["ids"] = (E.principal_de(c, p2), E.principal_de(c, p1))
    c = E.cenario(corpo)
    posts = list(c.no_canal(E.GRUPO_DESTINO).values())
    r.check(len(posts) == 1 and out["ids"][0] == out["ids"][1], "05.um_post", str(out))
    r.check(posts and "2LKLp7b" in posts[0].message and "/sec/" not in posts[0].message,
            "05.mecanismo_nao_substitui_a_lista")


def test_06_real_promotom_depois_samuel(r):
    out = {}

    async def corpo(c):
        p = real(PROMOTOM, PROMOTOM_110373, COD_110373, [SEC])
        await E.pub(p, score=26)
        s = real(SAMUEL, SAMUEL_118773, COD_118773,
                 [LISTA.format(14380400 + i) for i in range(8)])
        await E.pub(s, score=22)
        out["ids"] = (E.principal_de(c, p), E.principal_de(c, s))
    c = E.cenario(corpo)
    posts = list(c.no_canal(E.GRUPO_DESTINO).values())
    txt = posts[0].message if posts else ""
    r.check(len(posts) == 1 and out["ids"][0] == out["ids"][1], "06.um_post", str(out))
    r.check("2AnW4cU" in txt and "OPAECONOMIZEI" in txt, "06.editado_para_as_listas_do_samuel")
    todos = {c.strip() for x in COD_110373 for c in x.split(",")}
    r.check(todos <= _codigos(txt) and "/sec/2U6U32Q" in txt,
            "06.nenhum_cupom_do_promotom_some", txt[-400:])
    out["txt"] = txt


def test_07_sincronizacao_mantem_o_bloco(r):
    out = {}

    async def corpo(c):
        await E.pub(m1367(), score=26)
        p2 = m1368()
        await E.pub(p2, score=22)
        mid = E.principal_de(c, p2)
        # o líder (fonte da lista) muda um percentual na PRÓPRIA mensagem
        t2 = TESTE_1368.replace("30% OFF (limite R$500) — OPAECONOMIZEI",
                                "35% OFF (limite R$500) — OPAECONOMIZEI")
        await E.pub(m1368(msg_id=p2.msg_id, texto=t2), score=22, is_edit=True)
        out["sync1"] = c.no_canal(E.GRUPO_DESTINO)[mid].message
        out["exib1"] = db_exibida(mid)
        # ...e passa a trazer PROMOAQU: ele sai do bloco (agora é do líder)
        t3 = t2.replace("👍 Bom proveito!",
                        "🎟️ 8% OFF (mín. R$150, limite R$300) — PROMOAQU\n"
                        "   🔗 https://meli.la/3novo\n\n👍 Bom proveito!")
        await E.pub(m1368(msg_id=p2.msg_id, texto=t3, codigos=COD_1368 + ["PROMOAQU"]),
                    score=22, is_edit=True)
        out["sync2"] = c.no_canal(E.GRUPO_DESTINO)[mid].message
    E.cenario(corpo)
    s1 = out["sync1"]
    r.check("35% OFF (limite R$500)" in s1, "07.lider_sincronizou", s1[:200])
    r.check(SO_PROMOTOM <= _codigos(s1) and "/sec/2U6U32Q" in s1
            and retencao_cupons.MARCA in s1, "07.bloco_retido_ficou", s1[-300:])
    r.check({"mercadolivre|cup|" + c for c in SO_PROMOTOM} <= set(out["exib1"]),
            "07.exibida_mantem_os_retidos")
    s2 = out["sync2"]
    bloco = s2.split(retencao_cupons.MARCA, 1)[1] if retencao_cupons.MARCA in s2 else ""
    r.check("3novo" in s2 and "PROMOAQU" not in bloco.upper()
            and {"DESCONTOSMELI", "OFERTA"} <= _codigos(bloco), "07.codigo_do_lider_sai_do_bloco",
            bloco)


def test_08_lider_tira_o_proprio_codigo(r):
    out = {}

    async def corpo(c):
        await E.pub(m1367(), score=26)
        p2 = m1368()
        await E.pub(p2, score=22)
        mid = E.principal_de(c, p2)
        t2 = TESTE_1368.replace("🎟️ 30% OFF (limite R$500) — OPAECONOMIZEI\n"
                                "   🔗 https://meli.la/2LKLp7b\n", "")
        cods = [x for x in COD_1368 if x != "OPAECONOMIZEI"]
        await E.pub(m1368(msg_id=p2.msg_id, texto=t2, codigos=cods), score=22, is_edit=True)
        out["txt"] = c.no_canal(E.GRUPO_DESTINO)[mid].message
    E.cenario(corpo)
    r.check("OPAECONOMIZEI" not in out["txt"] and SO_PROMOTOM <= _codigos(out["txt"]),
            "08.o_proprio_codigo_sai_os_retidos_ficam", out["txt"][-300:])


def test_09_legenda_curta_nao_compoe(r):
    out = {}

    async def corpo(c):
        p1 = E.foto(m1367())                       # post COM mídia: legenda ≤ 1024
        await E.pub(p1, score=26)
        out["antes"] = c.no_canal(E.GRUPO_DESTINO)[E.principal_de(c, p1)].message
        p2 = m1368()
        await E.pub(p2, score=22)
        out["ids"] = (E.principal_de(c, p1), E.principal_de(c, p2))
    c = E.cenario(corpo)
    posts = list(c.no_canal(E.GRUPO_DESTINO).values())
    r.check(len(posts) == 1 and out["ids"][0] == out["ids"][1], "09.sem_duplicar", str(out))
    r.check(posts and posts[0].message == out["antes"], "09.texto_intacto_nada_some")


def test_10_retencao_pura(r):
    pub = ("**Cupom**\n**🎟 10% OFF:** `AAA111`\n**🎟 Código:** `BBB222`\n"
           "**🎟 15% OFF:** `CCC333`\n**✅ Resgate aqui:**\nhttps://meli.la/sec1")
    cand = "**🔥 Lista**\n**🎟 15% OFF:** `CCC333`\nhttps://meli.la/lista"
    ret = retencao_cupons.reter(cand, pub, {"mercadolivre|cup|AAA111"}, limite=4096)
    r.check(ret is not None and "AAA111" in ret.texto and "meli.la/sec1" in ret.texto
            and "Resgate aqui" in ret.texto and ret.retidas == ("mercadolivre|cup|AAA111",),
            "10.retem_codigo_e_resgate_com_rotulo", ret and ret.texto)
    ret = retencao_cupons.reter(cand, pub, {"mercadolivre|cup|BBB222"}, limite=4096)
    r.check(ret is not None and "10% OFF" in ret.texto, "10.linha_de_beneficio_anterior",
            ret and ret.texto)
    r.check(retencao_cupons.reter(cand, pub, {"mercadolivre|MLB1"}, limite=4096) is None
            and retencao_cupons.reter(cand, pub, {"mercadolivre|cup|AAA111",
                                                  "mercadolivre|MLB1"}, limite=4096) is None,
            "10.falta_produto_nao_compoe")
    r.check(retencao_cupons.reter(cand, pub, {"mercadolivre|cup|ZZZ999"}, limite=4096) is None,
            "10.codigo_fora_do_texto_nao_compoe")
    r.check(retencao_cupons.reter(cand, pub, {"mercadolivre|cup|AAA111"}, limite=40) is None,
            "10.limite")
    r.check(retencao_cupons.manter(cand, pub, [], ["mercadolivre|cup|AAA111"],
                                   limite=4096) is None, "10.manter_sem_bloco_nada")


def test_11_lider_do_mecanismo_edita(r):
    out = {}

    async def corpo(c):
        p1 = m1367()
        await E.pub(p1, score=26)
        mid = E.principal_de(c, p1)
        t2 = (PROMOTOM_110373
              .replace("15% OFF em R$ 79, Limite de R$ 200 OFF: OFERTA\n", "")
              .replace("-Resgate aqui:", "-Lista PROMOML: https://meli.la/9lista\n-Resgate aqui:"))
        cods = [c for c in COD_110373 if c != "OFERTA"]
        await E.pub(real(OFERTASCONVERTIDAS, t2, cods, [LISTA.format(14389999), SEC],
                         msg_id=p1.msg_id), score=26, is_edit=True)
        out["txt"] = c.no_canal(E.GRUPO_DESTINO)[mid].message
        out["exib"] = db_exibida(mid)
    E.cenario(corpo)
    txt = out["txt"]
    r.check("9lista" in txt and "OFERTA" not in _codigos(txt)
            and retencao_cupons.MARCA not in txt, "11.espelhou_sem_reter", txt[-300:])
    r.check("mercadolivre|cup|OFERTA" not in out["exib"], "11.exibida_sem_o_que_saiu")


def test_12_lista_com_foto_edita_no_lugar(r):
    out = {}

    async def corpo(c):
        p1 = m1367()
        await E.pub(p1, score=26)
        out["mid"] = E.principal_de(c, p1)
        await E.pub(E.foto(m1368()), score=22)
        out["estado"] = db_get_post(out["mid"])
    c = E.cenario(corpo)
    posts = c.no_canal(E.GRUPO_DESTINO)
    pr = posts.get(out["mid"])
    r.check(len(posts) == 1 and pr is not None and pr.media is None
            and "OPAECONOMIZEI" in pr.message, "12.mesmo_post_editado_sem_foto",
            f"{list(posts)} {pr and pr.media}")
    r.check(("delete", out["mid"]) not in c.ops_em(E.GRUPO_DESTINO)
            and (out["estado"] or {}).get("midia_chat") == "",
            "12.sem_apagar_reenviar_midia_chat_intacto", str(c.ops_em(E.GRUPO_DESTINO)))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "MERCADO LIVRE · cupons sem duplicar, edição por lista"))
