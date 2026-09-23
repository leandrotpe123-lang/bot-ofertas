"""
FAMÍLIA × DESTINO × MECANISMO — publicação real, árvore real.

O caso de produção (23/09, posts 23238 e 23239):

    @fadadoscupons   lista _Container_13655781… + resgate + MODA2309, LOJASOFICIAIS
    @promotom        só /sec/                  +          MODA2309, LOJASOFICIAIS
    @samuelf3lipe    lista _Container_13655781…  +        MODA2309, LOJASOFICIAIS

A lista e o Samuel casaram; o /sec/ virou um SEGUNDO post. Mesma
campanha de cupom, publicada duas vezes.

O QUE ESTE ARQUIVO PROVA (caminho real: enriquecer → montar → enviar →
família → decisão → aplicador, banco SQLite real; só o Telegram é
simulado, como no resto da suíte):

  A  ordem de produção (lista → /sec/ → lista): UM post
  B  ordem inversa (/sec/ → lista): UM post, e ele vira a versão da lista
  C  a precedência não depende de score (forçado nos dois sentidos)
  D  duas campanhas diferentes com o MESMO cupom: DOIS posts
  E  /sec/ → campanha 1 → campanha 2: campanha 2 não entra na família
  F  produto com cupom continua família de PRODUTO (C3 intacta)
  G  produto-veículo em post de cupom não ancora (C3 intacta)
  H  outras plataformas: nenhum destino, nenhum motivo novo
  I  decidir(): fatos ausentes ≡ comportamento anterior, em grade

    python tests/test_familia_destino.py
"""
import asyncio
import os
import sys
import time
import types
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar  # noqa: E402

preparar()
os.environ.setdefault("ML_TAG", "leoofertas8270")

# Fronteira de terceiros: `yarl` (dependência do aiohttp, instalada em
# produção) é importado pelo adaptador Amazon ao registrar as
# plataformas. Mesma regra do harness: só biblioteca de terceiro é
# simulada — e só a superfície importada (URL), nunca lógica do Foguetão.
try:
    import yarl  # noqa: F401
except ImportError:
    _yarl = types.ModuleType("yarl")

    class _URL(str):
        def __new__(cls, valor="", **_kw):
            return str.__new__(cls, valor)
    _yarl.URL = _URL
    sys.modules["yarl"] = _yarl

import globals as g                                            # noqa: E402
import client as modulo_client                                 # noqa: E402
import plataformas                                             # noqa: E402
from database import _init_db                                   # noqa: E402
from database_posts import db_ofertas_de_post                  # noqa: E402
from database_conexao import _db                               # noqa: E402
from pipeline import identidade, exclusao, origem              # noqa: E402
from pipeline import publicacao                                # noqa: E402
from pipeline.montagem import montar                            # noqa: E402
from pipeline.enriquecimento import enriquecer                  # noqa: E402
from pipeline.normalizacao import MensagemNormalizada           # noqa: E402
from pipeline.normalizacao_identidade import (                  # noqa: E402
    derivar_produto, derivar_campanha, derivar_ancora_url,
    remover_cupons_da_entidade)
from pipeline.decisao import decidir                            # noqa: E402
from pipeline.montagem import MensagemMontada                   # noqa: E402

plataformas.inicializar()
_init_db()

FADA, PROMOTOM, SAMUEL = -1002050488946, -1001825680721, -1001768101197
identidade._CHAT_USERNAME.update({
    FADA: "fadadoscupons", PROMOTOM: "promotom", SAMUEL: "samuelf3lipepromo"})


# ─────────────────────────────────────────────────────────────────
# Telegram (I/O externo) — conta envios e edições
# ─────────────────────────────────────────────────────────────────
class _Msg:
    def __init__(self, i):
        self.id, self.media = i, None


# Contador GLOBAL: o Telegram nunca reusa id de mensagem no destino.
# Um contador por cenário fazia cenários diferentes gravarem no MESMO
# msg_id_dest e contaminarem a família uns dos outros.
_ID_DESTINO = {"n": 9000}


def _proximo_id():
    _ID_DESTINO["n"] += 1
    return _ID_DESTINO["n"]


class Cliente:
    def __init__(self):
        self.novos, self.edits, self.deletes = 0, [], []

    async def send_message(self, dest, texto, parse_mode=None, link_preview=None):
        self.novos += 1
        return _Msg(_proximo_id())

    async def send_file(self, dest, img, caption=None, parse_mode=None,
                        force_document=False):
        self.novos += 1
        return _Msg(_proximo_id())

    async def edit_message(self, dest, msg_id, texto, parse_mode=None, file=None):
        self.edits.append((msg_id, texto))
        return _Msg(msg_id)

    async def delete_messages(self, dest, msg_id):
        self.deletes.append(msg_id)
        return True

    async def download_media(self, media, file=None):
        return None


MOTIVOS = []
_log_real = publicacao._log_decisao


def _espiao(d, *a, **k):
    MOTIVOS.append(d.motivo)
    return _log_real(d, *a, **k)


publicacao._log_decisao = _espiao


def _isolar_loop():
    g._init_globals()
    for pool in (exclusao._IDENTITY_LOCKS, exclusao._IDENTITY_LOCKS_TS,
                 exclusao._POST_LOCKS, exclusao._POST_LOCKS_TS,
                 origem._LOCKS, origem._LOCKS_TS):
        pool.clear()
    origem._LOCKS_LCK = asyncio.Lock()


def cenario(corpo):
    async def _run():
        _isolar_loop()
        c = Cliente()
        modulo_client.client = c
        MOTIVOS.clear()
        await corpo(c)
        return c
    return asyncio.run(_run())


# ─────────────────────────────────────────────────────────────────
# Mensagens — identidade derivada pelas funções REAIS da normalização
# ─────────────────────────────────────────────────────────────────
_SEQ = {"n": 800000}


def msg(chat, texto, urls_longas, cupons, plat="mercadolivre"):
    """Espelha normalizar() a partir das URLs afiliadas LONGAS."""
    _SEQ["n"] += 1
    ids, idents, sku = derivar_produto(urls_longas)
    cupons = remover_cupons_da_entidade(list(cupons), ids)
    c = derivar_campanha(urls_longas, texto)
    mapa = {f"https://orig/{_SEQ['n']}/{i}": u for i, u in enumerate(urls_longas)}
    return MensagemNormalizada(
        msg_id=_SEQ["n"], chat=str(chat), texto_limpo=texto, texto_analise=texto,
        mapa=mapa, preservar=[], plat=plat, sku=sku, tem_midia=False,
        media_obj=None, ids_globais=ids, idents=idents, cupons=cupons,
        chave_campanha=c.chave_campanha, chaves_campanha=c.chaves_campanha,
        tem_host_campanha=c.tem_host_campanha,
        tem_sinal_cashback=c.tem_sinal_cashback,
        destinos_declarados=list(c.destinos_declarados),
        ancora_url=derivar_ancora_url(urls_longas))


async def publicar(n, score=None):
    enr = enriquecer(n)
    if score is not None:
        enr = replace(enr, score=score)
    montada = await montar(n)
    await publicacao.enviar(montada, n, enr=enr, is_edit=False)
    return enr, montada


def lista(container, campanha):
    return (f"https://lista.mercadolivre.com.br/{container}"
            f"?coupon_campaign_id={campanha}")


SEC = "https://mercadolivre.com/sec/23NpLSc"
RESGATE_PRODUTO = "https://www.mercadolivre.com.br/produto-da-vitrine/p/MLB8923631"
CT = "_Container_13655781-335801003"


def texto_fada(c1, c2):
    return ("🔥 Cupons Mercado Livre\n\n"
            f"🎟 R$15 OFF acima de R$89: {c1}\n\n"
            "✅ Resgate aqui:\nhttps://meli.la/1MxJ868\n\n"
            "📋 Para listas selecionadas (o link é a lista de cada um):\n"
            f"🎟 10% OFF acima de R$199, limite R$22: {c2}\n"
            "https://meli.la/31sHqTk")


def texto_promotom(c1, c2):
    return ("🔥 Cupom Mercado Livre\n\n"
            f"🎟 10% OFF, Limite de R$ 20 OFF: {c2}\n"
            f"🎟 R$ 15 OFF em R$ 89: {c1}\n\n"
            f"📌 Resgate aqui: {SEC}")


def texto_samuel(c1, c2):
    return ("🔥 Cupons Mercado Livre\n"
            f"🎟 {c2} — 10% OFF acima de R$199\n"
            f"🎟 {c1} — R$15 OFF acima de R$89\n"
            "https://meli.la/1Pkv3fc")


def n_posts(codigos):
    """Posts distintos que carregam os códigos (via índice da família)."""
    with _db() as db:
        rows = db.execute(
            "SELECT DISTINCT msg_id_dest FROM oferta_index WHERE identity IN "
            f"({','.join('?' * len(codigos))})",
            tuple(f"mercadolivre|cup|{c}" for c in codigos)).fetchall()
    return {r[0] for r in rows}


# ══════════════════════════════════════════════════════════════════
def test_A_ordem_de_producao_um_post(r):
    """lista (Fada) → /sec/ (Promotom) → lista (Samuel): UM post."""
    c1, c2 = "LOJASOFICIAIS", "MODA2309"
    fada = msg(FADA, texto_fada(c1, c2), [RESGATE_PRODUTO, lista(CT, "13655781")], [c1, c2])
    prom = msg(PROMOTOM, texto_promotom(c1, c2), [SEC], [c2, c1])
    samu = msg(SAMUEL, texto_samuel(c1, c2), [lista(CT, "13655781")], [c1, c2])

    async def corpo(c):
        enr_f, _ = await publicar(fada)
        r.check("mercadolivre|dest|mercadolivre:lista:" + CT + ":13655781" in enr_f.ofertas,
                "A.lista_ancora_no_destino", str(enr_f.ofertas))
        r.check(f"mercadolivre|cup|{c1}" in enr_f.ofertas and f"mercadolivre|cup|{c2}" in enr_f.ofertas,
                "A.lista_expoe_codigos_de_encontro", str(enr_f.ofertas))
        await publicar(prom)
        r.check("MECANISMO_NAO_SUBSTITUI_DESTINO" in MOTIVOS,
                "A./sec/_nao_substitui_a_lista", str(MOTIVOS))
        await publicar(samu)
    cli = cenario(corpo)
    r.check(cli.novos == 1, "A.UM_post_publicado", f"novos={cli.novos}")
    r.check(len(n_posts([c1, c2])) == 1, "A.codigos_numa_familia_so", str(n_posts([c1, c2])))


def test_B_ordem_inversa_vira_a_lista(r):
    """/sec/ primeiro, lista depois: UM post, que passa a ser a lista."""
    c1, c2 = "LOJAB1", "MODAB2"
    prom = msg(PROMOTOM, texto_promotom(c1, c2), [SEC], [c2, c1])
    fada = msg(FADA, texto_fada(c1, c2), [RESGATE_PRODUTO, lista(CT, "7700001")], [c1, c2])
    saida = {}

    async def corpo(c):
        await publicar(prom)
        _, m = await publicar(fada)
        saida["texto_lista"] = m.texto
    cli = cenario(corpo)
    r.check(cli.novos == 1, "B.UM_post", f"novos={cli.novos}")
    r.check("DESTINO_PREVALECE" in MOTIVOS, "B.destino_prevalece", str(MOTIVOS))
    r.check(cli.edits and cli.edits[-1][1] == saida["texto_lista"],
            "B.post_final_e_a_versao_da_lista", str(len(cli.edits)))


def test_C_precedencia_independe_do_score(r):
    """Score forçado contra a regra, nos dois sentidos."""
    c1, c2 = "LOJAC1", "MODAC2"
    # lista chega depois com score MENOR → ainda assim prevalece
    prom = msg(PROMOTOM, texto_promotom(c1, c2), [SEC], [c2, c1])
    fada = msg(FADA, texto_fada(c1, c2), [lista(CT, "7700002")], [c1, c2])

    async def corpo1(c):
        await publicar(prom, score=30)
        await publicar(fada, score=5)
    cli = cenario(corpo1)
    r.check(cli.novos == 1 and "DESTINO_PREVALECE" in MOTIVOS,
            "C.lista_com_score_menor_prevalece", f"novos={cli.novos} {MOTIVOS}")

    # /sec/ chega depois com score MAIOR → ainda assim não substitui
    c3, c4 = "LOJAC3", "MODAC4"
    fada2 = msg(FADA, texto_fada(c3, c4), [lista(CT, "7700003")], [c3, c4])
    prom2 = msg(PROMOTOM, texto_promotom(c3, c4), [SEC], [c4, c3])

    async def corpo2(c):
        await publicar(fada2, score=5)
        await publicar(prom2, score=30)
    cli = cenario(corpo2)
    r.check(cli.novos == 1 and not cli.edits
            and "MECANISMO_NAO_SUBSTITUI_DESTINO" in MOTIVOS,
            "C.sec_com_score_maior_nao_substitui", f"novos={cli.novos} edits={len(cli.edits)} {MOTIVOS}")


def test_D_campanhas_distintas_mesmo_cupom(r):
    """Frente 7B: 13657213 x 14194174 com FULL2209 — DOIS posts."""
    cod = "FULL2209D"
    txt = ("🔥 Cupom Mercado Livre em Selecionados\n"
           f"🎟 R$ 50 OFF em R$ 300: {cod}\nhttps://meli.la/x")
    a = msg(PROMOTOM, txt, [lista("_Container_promotions-77-full", "13657213")], [cod])
    b = msg(SAMUEL, txt, [lista("_Container_promotions-77-full", "14194174")], [cod])

    async def corpo(c):
        await publicar(a)
        await publicar(b)
    cli = cenario(corpo)
    r.check(cli.novos == 2, "D.dois_posts", f"novos={cli.novos}")


def test_E_mecanismo_depois_duas_campanhas(r):
    """/sec/ → campanha 1 → campanha 2: /sec/ e campanha 1 juntos; 2 separada."""
    cod = "FULL2209E"
    sec = msg(PROMOTOM, f"🔥 Cupom Mercado Livre\n🎟 R$ 50 OFF em R$ 300: {cod}\n📌 Resgate: {SEC}",
              [SEC], [cod])
    txt = f"🔥 Cupom Mercado Livre em Selecionados\n🎟 R$ 50 OFF em R$ 300: {cod}\nhttps://meli.la/x"
    c1 = msg(FADA, txt, [lista("_Container_promotions-77-full", "23657213")], [cod])
    c2 = msg(SAMUEL, txt, [lista("_Container_promotions-77-full", "24194174")], [cod])

    async def corpo(c):
        await publicar(sec)
        await publicar(c1)
        await publicar(c2)
    cli = cenario(corpo)
    r.check(cli.novos == 2, "E.dois_posts_no_total", f"novos={cli.novos}")
    r.check("DESTINO_PREVALECE" in MOTIVOS, "E.campanha1_assumiu_o_sec", str(MOTIVOS))


def test_F_produto_com_cupom_segue_produto(r):
    """C3: produto com preço + cupom é família de PRODUTO; /sec/ não entra."""
    cod = "ESTOQUEF"
    prod = msg(PROMOTOM, f"🔥 Teclado Attack Shark X68he\n\n✅ R$ 117\n🎟 Use o cupom: {cod}\nhttps://meli.la/p",
               ["https://www.mercadolivre.com.br/teclado/p/MLB51435621"], [cod])
    sec = msg(SAMUEL, f"🔥 Cupom Mercado Livre\n🎟 R$ 20 OFF em R$ 100: {cod}\n📌 Resgate: {SEC}",
              [SEC], [cod])
    saida = {}

    async def corpo(c):
        enr, _ = await publicar(prod)
        saida["ofertas"] = enr.ofertas
        await publicar(sec)
    cli = cenario(corpo)
    r.check(saida["ofertas"] == ["mercadolivre|MLB51435621"], "F.produto_ancora_so_no_produto",
            str(saida["ofertas"]))
    r.check(cli.novos == 2, "F.cupom_solto_nao_vira_produto", f"novos={cli.novos}")


def test_G_produto_veiculo_nao_ancora(r):
    """C3: o link de resgate que virou MLB não ancora o post de cupom."""
    c1, c2 = "LOJAG1", "MODAG2"
    fada = msg(FADA, texto_fada(c1, c2), [RESGATE_PRODUTO, lista(CT, "7700007")], [c1, c2])
    produto = msg(SAMUEL, "🔥 Produto da vitrine\n\n✅ R$ 59,90\nhttps://meli.la/q",
                  [RESGATE_PRODUTO], [])
    saida = {}

    async def corpo(c):
        enr, _ = await publicar(fada)
        saida["ofertas"] = enr.ofertas
        await publicar(produto)
    cli = cenario(corpo)
    r.check("mercadolivre|MLB8923631" not in saida["ofertas"],
            "G.veiculo_fora_das_ancoras", str(saida["ofertas"]))
    r.check(cli.novos == 2, "G.oferta_do_produto_nao_cai_no_post_de_cupom",
            f"novos={cli.novos}")


def test_H_outras_plataformas_intocadas(r):
    """Shopee/Amazon/Magalu: sem destino; família e decisão como antes."""
    cod = "SHPH1"
    t1 = f"🔥 Cupom Shopee\n🎟 R$ 20 OFF em R$ 100: {cod}\nhttps://s.shopee.com.br/a"
    t2 = f"🔥 Cupom Shopee\n🎟 R$ 20 OFF em R$ 100: {cod}\nhttps://s.shopee.com.br/b"
    a = msg(PROMOTOM, t1, ["https://shopee.com.br/m/10-10"], [cod], plat="shopee")
    b = msg(SAMUEL, t2, ["https://shopee.com.br/m/10-10"], [cod], plat="shopee")
    amz = msg(FADA, "🔥 Cupom Amazon\n🎟 R$ 50 OFF em R$ 300: AMZH1\nhttps://amzn.to/x",
              ["https://www.amazon.com.br/promo/ofertas-do-dia"], ["AMZH1"], plat="amazon")
    saida = {}

    async def corpo(c):
        ea, _ = await publicar(a)
        eb, _ = await publicar(b)
        ez, _ = await publicar(amz)
        saida["destinos"] = (ea.destinos, eb.destinos, ez.destinos)
    cli = cenario(corpo)
    r.check(saida["destinos"] == ((), (), ()), "H.nenhum_destino_fora_do_ML", str(saida["destinos"]))
    r.check(cli.novos == 2, "H.shopee_repetido_na_mesma_familia", f"novos={cli.novos}")
    r.check(not any(m.startswith(("DESTINO", "MECANISMO")) for m in MOTIVOS),
            "H.nenhum_motivo_novo", str(MOTIVOS))


def test_I_decidir_fatos_ausentes_equivalem_ao_anterior(r):
    """Grade: omitido ≡ None ≡ (False, False) ≡ (True, True)."""
    agora = time.time()
    n = msg(PROMOTOM, "Oferta R$ 99", ["https://shopee.com.br/m/1"], [], plat="shopee")
    campos = ("acao", "motivo", "novo_score", "exigir_imagem", "permite_substituir",
              "trocar_midia", "motivo_midia", "na_janela", "score_atual")
    difs = 0
    total = 0
    for score_post in (3, 10, 20):
        for score_cand in (3, 10, 20):
            for edit_count in (0, 99):
                for janela in (agora + 900, agora - 1):
                    est = {"score": score_post, "texto": "x", "lider": "",
                           "edit_count": edit_count, "janela_fim": janela,
                           "ts": agora - 10}
                    m = MensagemMontada(n.msg_id, str(PROMOTOM), "shopee", "", "texto candidato", None, {}, n.msg_id)
                    base = decidir(n, m, score_cand, est, agora, False)
                    for kw in ({"destino_candidato": None, "destino_post": None},
                               {"destino_candidato": False, "destino_post": False},
                               {"destino_candidato": True, "destino_post": True}):
                        total += 1
                        d = decidir(n, m, score_cand, est, agora, False, **kw)
                        if any(getattr(d, c) != getattr(base, c) for c in campos):
                            difs += 1
    r.check(difs == 0, "I.equivalencia_em_grade", f"{difs}/{total} divergem")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "FAMÍLIA × DESTINO × MECANISMO · árvore real"))
