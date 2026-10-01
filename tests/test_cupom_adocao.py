"""
CUPOM SEM NOME — ADOÇÃO GENÉRICO × ASSINATURA (Fase 2).

Toda noite às 23:59 o Promotom posta "Cupons Shopee" seco (sem nome e sem
benefício → `cupb|geral`); logo depois Fada/Samuel postam a mesma rodada
com os valores (`cupb|v:30-299`). Sem chave em comum, o seco ficava no ar
e o rico virava OUTRO post — testes do operador em 01/10 04:56
(@ofertasconvertidas 1356/1357/1358 → posts 24418, 24419, 24420).

Regra (familia._adotar_cupom_sem_nome, idioma da Frente LIVE): sem
família por overlap, o candidato só-genérico adota o ÚNICO post vivo
só-assinatura da plataforma, e vice-versa. Mais de um → ninguém adota.
Qualquer outra âncora (nome, código, produto, outra plataforma) tira o
caso da regra. Quem decide o texto é o score de sempre.

  A  seco → rico: um post, o seco é EDITADO para o rico; cópia não duplica
  B  rico → seco: um post; o seco não rebaixa o texto e ensina a âncora
  C  uma adoção só: seco → 30/90 → 10/119 = dois posts
  D  ambíguo: 30/90 e 10/119 vivos → seco não adota ninguém
  E  nome nunca adota nem é adotado (nas duas ordens)
  F  cupom com código não adota
  G  plataforma diferente não adota
  H  post fora da janela (morto) não é adotado
  I  o rico com score menor não sobrescreve o seco (score decide)
  J  replay dos testes do operador 01/10 04:56 (1356 → 1357 → 1358)
  K  contrato direto de post_da_familia: destino, nome junto, prefixo
  L  container LIVE + cupb: a adoção não acontece (nas duas ordens)
  M  corrida: seco vivo; 30/90 e 10/119 escolhem o seco ANTES de qualquer
     escrita (lock_post atrasado) — o primeiro a travar adota, o outro
     revalida sob o lock, refaz a busca e vira post próprio
  N  a mesma corrida com a ordem de chegada ao lock invertida
  O  contrato direto de adocao_obsoleta (revalidação sob o lock)

    python tests/test_cupom_adocao.py
"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_cupom_sem_codigo import (                             # noqa: E402
    FADA, MOTIVOS, PROMOTOM, SAMUEL, _db, cenario, db_ofertas_de_post,
    identidades, msg, publicar)
from _harness_e5 import rodar                                   # noqa: E402
from database import db_posts_vivos_com_prefixo                 # noqa: E402
from pipeline import exclusao, familia                          # noqa: E402

CONVERTIDAS = -1003817694320

SECO = ("Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/2B5snGi9NX\n"
        "2 https://s.shopee.com.br/qaVCprDoA")
RICO_30_90 = ("🚨 Novos Cupons Shopee\n\n🎟 R$30 OFF em R$299\n🎟 R$90 OFF em R$899\n\n"
              "✅ Resgate aqui: \nhttps://s.shopee.com.br/AAGNE4pfS9")
RICO_10_119 = ("🔥 Novo Cupom Shopee\n\n🎟 R$10 OFF em R$119\n\n✅ Resgate aqui:\n"
               "https://s.shopee.com.br/5q8RSLqxsS")
TECH = ("🔥 Novo Cupom Shopee Tech\n\n🎟 R$100 OFF em R$999\n"
        "https://s.shopee.com.br/40YmMTfcfQ")


def _rodar(passos):
    """passos: [(chat, texto, score|None, plat)] → (cliente, posts, motivos)."""
    msgs = [msg(chat, txt, plat=plat) for chat, txt, _s, plat in passos]
    posts, motivos = [], []

    async def corpo(c):
        for (chat, txt, score, plat), n in zip(passos, msgs):
            antes, m0 = c.novos, len(MOTIVOS)
            await publicar(n, score=score)
            posts.append(c.ids[-1] if c.novos > antes else None)
            motivos.append(MOTIVOS[m0:])
    cli = cenario(corpo)
    return cli, posts, motivos, msgs


def _p(chat, txt, score=None, plat="shopee"):
    return (chat, txt, score, plat)


def test_A_seco_depois_rico_edita(r):
    cli, posts, mot, _ = _rodar([_p(PROMOTOM, SECO, 3), _p(FADA, RICO_30_90, 10),
                                 _p(SAMUEL, RICO_30_90.replace("AAGNE", "AUuQa"), 10)])
    r.check(cli.novos == 1, "A.um_post", f"novos={cli.novos} {posts}")
    r.check("EVOLUI" in mot[1], "A.rico_evolui_o_seco", str(mot[1]))
    r.check(any(m == posts[0] and "R$30 OFF em R$299" in t for m, t in cli.edits),
            "A.post_do_seco_ficou_rico", str(cli.edits))
    r.check(posts[2] is None, "A.copia_nao_duplica", str(posts))
    r.check(bool(mot[2]) and not any(m.startswith("JANELA") for m in mot[2]),
            "A.copia_decidida_no_mesmo_post", str(mot[2]))


def test_B_rico_depois_seco_nao_rebaixa(r):
    cli, posts, mot, _ = _rodar([_p(FADA, RICO_30_90, 10), _p(PROMOTOM, SECO, 3)])
    r.check(cli.novos == 1, "B.um_post", f"novos={cli.novos}")
    r.check("SCORE_NAO_EVOLUI" in mot[1] and not cli.edits,
            "B.seco_nao_rebaixa", f"{mot[1]} {cli.edits}")
    r.check("shopee|cupb|geral" in db_ofertas_de_post(posts[0]),
            "B.seco_ensina_a_ancora", str(db_ofertas_de_post(posts[0])))
    # um segundo seco agora casa pelo overlap normal
    cli2, posts2, _m, _ = _rodar([_p(FADA, RICO_30_90, 10), _p(PROMOTOM, SECO, 3),
                                  _p(PROMOTOM, SECO.replace("2B5sn", "9ZZZZ"), 3)])
    r.check(cli2.novos == 1, "B.segundo_seco_mesmo_post", f"novos={cli2.novos}")


def test_C_uma_adocao_so(r):
    cli, posts, mot, _ = _rodar([_p(PROMOTOM, SECO, 3), _p(FADA, RICO_30_90, 10),
                                 _p(FADA, RICO_10_119, 10)])
    r.check(cli.novos == 2, "C.dois_posts", f"novos={cli.novos} {posts}")
    r.check(posts[1] is None and posts[2] is not None,
            "C.segunda_assinatura_e_post_proprio", str(posts))
    r.check(not any(m == posts[0] and "R$10 OFF" in t for m, t in cli.edits),
            "C.10_119_nao_entra_no_30_90", str(cli.edits))


def test_D_ambiguo_ninguem_adota(r):
    cli, posts, mot, _ = _rodar([_p(FADA, RICO_30_90, 10), _p(FADA, RICO_10_119, 10),
                                 _p(PROMOTOM, SECO, 3)])
    r.check(cli.novos == 3, "D.tres_posts", f"novos={cli.novos} {posts}")
    r.check(not cli.edits, "D.nenhuma_edicao", str(cli.edits))


def test_E_nome_nunca_adota_nem_e_adotado(r):
    for ordem in ((SECO, TECH), (TECH, SECO)):
        cli, posts, _m, _ = _rodar([_p(PROMOTOM, ordem[0], 3), _p(FADA, ordem[1], 7)])
        r.check(cli.novos == 2, f"E.{ordem[0][:12]!r}_depois_{ordem[1][:12]!r}",
                f"novos={cli.novos}")
    # família nomeada não é "só-assinatura": o seco não a adota
    cli, posts, _m, _ = _rodar([_p(FADA, TECH, 7), _p(FADA, RICO_30_90, 10),
                                _p(PROMOTOM, SECO, 3)])
    r.check(cli.novos == 2 and posts[2] is None, "E.seco_vai_para_a_assinatura",
            f"novos={cli.novos} {posts}")


def test_F_cupom_com_codigo_nao_adota(r):
    com_codigo = ("🔥 Cupom Shopee\n\n🎟 R$20 OFF em R$60: V4M0S4PR0V3IT4R\n"
                  "✅ Resgate aqui:\nhttps://s.shopee.com.br/1qU7Zs67MB")
    n = msg(FADA, com_codigo, cupons=["V4M0S4PR0V3IT4R"])
    r.check(identidades(n) == ["shopee|cup|V4M0S4PR0V3IT4R"], "F.chave_codigo",
            str(identidades(n)))
    seco = msg(PROMOTOM, SECO)
    out = {}

    async def corpo(c):
        await publicar(seco, score=3)
        await publicar(n, score=10)
        out["n"] = c.novos
    cenario(corpo)
    r.check(out["n"] == 2, "F.dois_posts", str(out))


def test_G_plataforma_diferente_nao_adota(r):
    amz = "🔥 Cupom Amazon\n\n🎟 R$50 OFF em R$300\nhttps://amzn.to/x"
    cli, posts, _m, msgs = _rodar([_p(PROMOTOM, SECO, 3), _p(FADA, amz, 10, "amazon")])
    r.check(identidades(msgs[1]) == ["amazon|cupb|v:50-300"], "G.chave_amazon",
            str(identidades(msgs[1])))
    r.check(cli.novos == 2, "G.dois_posts", f"novos={cli.novos}")


def test_H_post_morto_nao_e_adotado(r):
    seco, rico = msg(PROMOTOM, SECO), msg(FADA, RICO_30_90)
    out = {}

    async def corpo(c):
        await publicar(seco, score=3)
        out["vivo"] = db_posts_vivos_com_prefixo("shopee|cupb|")
        with _db() as db:
            db.execute("UPDATE post_estado SET janela_fim=?", (time.time() - 1,))
        out["morto"] = db_posts_vivos_com_prefixo("shopee|cupb|")
        await publicar(rico, score=10)
        out["n"], out["edits"] = c.novos, list(c.edits)
    cenario(corpo)
    r.check(len(out["vivo"]) == 1 and out["morto"] == [],
            "H.consulta_so_enxerga_vivo", str(out))
    r.check(out["n"] == 2 and not out["edits"], "H.morto_fica_como_esta", str(out))


def test_K_contrato_post_da_familia(r):
    """Direto na família: destino declarado ou vínculo de origem tiram o
    candidato da adoção; sem eles, o único só-genérico vivo é a família."""
    seco = msg(PROMOTOM, SECO)
    out = {}

    async def corpo(c):
        await publicar(seco, score=3)
        g = c.ids[-1]
        ass = ["shopee|cupb|v:30-299"]
        out["sem_nada"] = familia.post_da_familia(ass) == g
        out["com_destino"] = familia.post_da_familia(ass, destinos=("lista:x",))
        out["misturado"] = familia.post_da_familia(ass + ["shopee|cupb|tech"])
        out["prefixo_outro"] = db_posts_vivos_com_prefixo("amazon|cupb|")
    cenario(corpo)
    r.check(out["sem_nada"], "K.adota_o_unico", str(out))
    r.check(out["com_destino"] is None, "K.destino_nao_adota", str(out))
    r.check(out["misturado"] is None, "K.nome_junto_nao_adota", str(out))
    r.check(out["prefixo_outro"] == [], "K.prefixo_isola_plataforma", str(out))


def test_I_score_decide_o_texto(r):
    cli, posts, mot, _ = _rodar([_p(PROMOTOM, SECO, 9), _p(FADA, RICO_30_90, 4)])
    r.check(cli.novos == 1, "I.mesma_familia", f"novos={cli.novos}")
    r.check("SCORE_NAO_EVOLUI" in mot[1] and not cli.edits,
            "I.score_menor_nao_sobrescreve", f"{mot[1]} {cli.edits}")


def test_J_replay_operador_01_10_0456(r):
    """1356 genérico → 1357 30/90 → 1358 Tech (todos da @ofertasconvertidas)."""
    cli, posts, mot, _ = _rodar([_p(CONVERTIDAS, SECO, 3),
                                 _p(CONVERTIDAS, RICO_30_90, 10),
                                 _p(CONVERTIDAS, TECH, 7)])
    r.check(cli.novos == 2, "J.dois_posts", f"novos={cli.novos} {posts}")
    r.check(posts[1] is None and "EVOLUI" in mot[1],
            "J.1357_edita_o_1356", f"{posts} {mot[1]}")
    r.check(posts[2] is not None, "J.tech_post_proprio", str(posts))


LIVE = "https://live.shopee.com.br/live/7187289"
CUPOM_LIVE = ("🔥 Cupom R$ 10 OFF em R$ 50 Shopee\n\n🎟 Resgate o cupom na Live "
              "shopee_br (sacola laranja)\nhttps://s.shopee.com.br/20vruIAJxg")


def test_L_container_live_nao_adota(r):
    live = msg(SAMUEL, CUPOM_LIVE, [LIVE])
    r.check(identidades(live) == ["shopee|cupb|v:10-50"]
            and familia.eh_chave_container(f"shopee|url|{live.ancora_url}"),
            "L.cupb_com_container", f"{identidades(live)} {live.ancora_url}")
    for nome, ordem in (("seco_depois_live", (msg(PROMOTOM, SECO), live)),
                        ("live_depois_seco", (msg(SAMUEL, CUPOM_LIVE, [LIVE]),
                                              msg(PROMOTOM, SECO)))):
        out = {}

        async def corpo(c, ordem=ordem):
            for n in ordem:
                await publicar(n, score=10 if n.chat == str(SAMUEL) else 3)
            out["n"], out["edits"] = c.novos, list(c.edits)
        cenario(corpo)
        r.check(out["n"] == 2 and not out["edits"], f"L.{nome}", str(out))
    # o mesmo cupom SEM live continua sendo adotado (a guarda é só do container)
    sem_live = msg(SAMUEL, CUPOM_LIVE)
    out = {}

    async def corpo2(c):
        await publicar(msg(PROMOTOM, SECO), score=3)
        await publicar(sem_live, score=10)
        out["n"] = c.novos
    cenario(corpo2)
    r.check(out["n"] == 1, "L.sem_container_adota", str(out))


def _corrida(atrasos):
    """seco publicado; depois 30/90 e 10/119 em paralelo, com lock_post
    atrasado por chamada (`atrasos`) — as duas escolhem o seco antes de
    qualquer escrita, e a ordem de chegada ao lock é controlada."""
    real = exclusao.lock_post
    chamadas = {"n": 0}

    async def lento(mid):
        i = chamadas["n"]
        chamadas["n"] += 1
        await asyncio.sleep(atrasos[i] if i < len(atrasos) else 0)
        return await real(mid)

    seco = msg(PROMOTOM, SECO)
    a, b = msg(FADA, RICO_30_90), msg(SAMUEL, RICO_10_119)
    out = {}

    async def corpo(c):
        await publicar(seco, score=3)
        g = c.ids[-1]
        chamadas["n"] = 0
        exclusao.lock_post = lento
        try:
            await asyncio.gather(publicar(a, score=10), publicar(b, score=10))
        finally:
            exclusao.lock_post = real
        out["g"], out["n"], out["ids"] = g, c.novos, list(c.ids)
        out["edits"] = list(c.edits)
        out["fam_g"] = set(db_ofertas_de_post(g))
        out["fam_novo"] = (set(db_ofertas_de_post(c.ids[-1]))
                           if c.ids[-1] != g else set())
    cenario(corpo)
    return out


def _checa_corrida(r, rot, out, vencedor, perdedor):
    r.check(out["n"] == 2, f"{rot}.dois_posts", str(out))
    r.check(vencedor <= out["fam_g"] and not perdedor & out["fam_g"],
            f"{rot}.seco_so_com_o_vencedor", str(out["fam_g"]))
    r.check(perdedor <= out["fam_novo"] and not vencedor & out["fam_novo"],
            f"{rot}.perdedor_em_post_proprio", str(out["fam_novo"]))
    textos_g = [t for m, t in out["edits"] if m == out["g"]]
    r.check(len(textos_g) == 1, f"{rot}.uma_edicao_no_seco", str(out["edits"]))


V30 = {"shopee|cupb|v:30-299", "shopee|cupb|v:90-899"}
V10 = {"shopee|cupb|v:10-119"}


def test_M_corrida_30_90_trava_primeiro(r):
    out = _corrida([0.0, 0.05])
    _checa_corrida(r, "M", out, V30, V10)


def test_N_corrida_ordem_invertida(r):
    out = _corrida([0.05, 0.0])
    _checa_corrida(r, "N", out, V10, V30)



def test_O_contrato_adocao_obsoleta(r):
    out = {}

    async def corpo(c):
        # cenário ambíguo (D): dois só-assinatura vivos → o seco fica só
        await publicar(msg(FADA, RICO_10_119), score=10)
        s_ = c.ids[-1]                      # 10/119: post só-assinatura
        await publicar(msg(SAMUEL, RICO_30_90), score=10)
        await publicar(msg(PROMOTOM, SECO), score=3)
        g = c.ids[-1]                       # seco: post só-genérico
        v30, v10 = ["shopee|cupb|v:30-299"], ["shopee|cupb|v:10-119"]
        out["geral_complementar"] = familia.adocao_obsoleta(g, v30)
        out["assinatura_x_assinatura"] = familia.adocao_obsoleta(s_, v30)
        out["geral_x_assinatura"] = familia.adocao_obsoleta(
            s_, ["shopee|cupb|geral"])
        out["overlap"] = familia.adocao_obsoleta(s_, v10)
        out["nome"] = familia.adocao_obsoleta(g, ["shopee|cupb|tech"])
        out["s_eh_proprio"] = c.novos == 3
    cenario(corpo)
    r.check(out["s_eh_proprio"], "O.tres_posts_puros", str(out))
    r.check(out["geral_complementar"] is False, "O.complementar_vale", str(out))
    r.check(out["assinatura_x_assinatura"] is True,
            "O.mesmo_tipo_e_obsoleto", str(out))
    r.check(out["geral_x_assinatura"] is False, "O.geral_x_assinatura_vale", str(out))
    r.check(out["overlap"] is False, "O.overlap_nao_e_adocao", str(out))
    r.check(out["nome"] is False, "O.nome_fora_da_regra", str(out))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CUPOM SEM NOME · adoção genérico × assinatura"))
