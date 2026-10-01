"""
CORRIDA DE ADOÇÃO — cupom sem nome e container LIVE (P1-1 da auditoria).

A adoção escolhe a família lendo o banco ANTES do lock do post. Os locks
de identidade eram só as âncoras concretas: `cupb|geral` × `cupb|<x>:<y>`
e a live × o forte da mesma live não têm chave em comum. Duas mensagens
complementares quase simultâneas não viam post registrado e cada uma
abria o seu. Correção: mutexes SINTÉTICOS (`<plat>|cupb|*` e
`<plat>|url*|<live>`) somados ao conjunto de locks de identidade de
publicacao.enviar() — nunca âncora, nunca banco, nunca família.

CORPUS REAL (t.me/s/<canal>, texto fiel; coletado em 01/10):
  rodada das 23:59 BRT — Promotom 110370 (seco, 01/10 02:59 UTC),
    Fada 17525 e Samuel 118699 (30/90, 03:00); Fada 17511 (10/119).
  live 7199917, 30/09 21:51 BRT — fumotom 35169 (AOC 32", 21:51:14),
    Promotom 110368 (21:51:21), Samuel 118688 (21:51:28) e fumotom 35170
    (AOC 43", 21:52:09). Canônica da live dos logs de produção:
    shopee|url|https://live.shopee.com.br/live/7199917 (posts 24370/24371).
  Os encurtadores são bloqueados neste ambiente: o item de PRODUTO
  (pid) dos cenários com forte é sintético.

  01  seco + rico juntos → 1 post, com o rico no ar
  02  rico + seco (ordem inversa) → 1 post
  03  dois ricos diferentes juntos → 2 posts; com o seco junto → 2 posts
      e o seco em exatamente um deles
  04  mesma LIVE, títulos diferentes, juntos → separados
  05  mesma LIVE, título equivalente (só-live + forte), juntos, nas duas
      ordens → 1 post (adoção válida)
  06  duas adoções simultâneas da mesma LIVE (fortes diferentes, mesmo
      título) → a primeira adota; a outra é post próprio; nada contamina
  07  as três cópias reais do AOC 32" juntas (live-only, forte, live-only)
      → 1 post
  08  o mutex nunca vira âncora: contrato de mutexes_de_adocao e nenhuma
      linha do banco com `|*`

    python tests/test_corrida_adocao.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_cupom_sem_codigo import (                             # noqa: E402
    FADA, PROMOTOM, SAMUEL, _db, cenario, db_ofertas_de_post, msg, publicar)
from _harness_e5 import rodar                                   # noqa: E402
from database import db_exibida, db_get_post                    # noqa: E402
from pipeline import exclusao, familia, origem                  # noqa: E402
from pipeline.normalizacao import MensagemNormalizada           # noqa: E402
from pipeline.normalizacao_identidade import derivar_ancora_url  # noqa: E402
from pipeline.resolucao_identidade import (                     # noqa: E402
    eh_chave_assinatura, eh_chave_forte)
from plataformas.shopee.links import _canonica_live             # noqa: E402

FUMOTOM = -1003775401737

# ── 23:59 BRT de 01/10 (textos reais) ─────────────────────────────
SECO_110370 = ("Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/2B5snGi9NX\n"
               "2 https://s.shopee.com.br/qaVCprDoA")
RICO_17525 = ("🚨 Novos Cupons Shopee\n\n🎟 R$30 OFF em R$299\n🎟 R$90 OFF em R$899\n\n"
              "✅ Resgate aqui: \nhttps://s.shopee.com.br/AAGNE4pfS9")
RICO_118699 = ("🔥 Novos Cupons Shopee\n\n🎟 R$ 30 OFF em R$ 299\n🎟 R$ 90 OFF em R$ 899\n\n"
               "⭐️ Resgate aqui:\nhttps://s.shopee.com.br/AUuQaBLv6B\n\nanúncio")
RICO_17511 = ("🔥 Novo Cupom Shopee\n\n🎟 R$10 OFF em R$119\n\n✅ Resgate aqui:\n"
              "https://s.shopee.com.br/5q8RSLqxsS")

# ── live 7199917, 30/09 21:51 BRT (textos reais) ──────────────────
LIVE_7199917 = ("https://live.shopee.com.br/universal-link/share?from=live"
                "&session=7199917&utm_medium=affiliates",)
AOC32_35169 = ("Smart TV 32 Polegadas HD 32S5155/78G Roku TV AOC\n\nR$ 699\n"
               "-Resgate todos os cupons na Live (sacola laranja) no APP aqui:\n"
               "https://s.shopee.com.br/50ZTgk4qQ4\n\n"
               "-Link produto (add pela sacola da Live:\nhttps://s.shopee.com.br/50ZTgk4qQ4\n\n"
               "-Anúncio")
AOC32_110368 = AOC32_35169
AOC32_118688 = ("🔥 Smart TV 32 Polegadas HD 32S5155/78G Roku TV AOC\n\n💵 R$ 699\n"
                "🎟 Resgate todos os cupons na Live (sacola laranja) no APP aqui:\n"
                "https://s.shopee.com.br/9AP2eWq9OA\n\n"
                "✅ Link produto, add pela sacola da Live:\nhttps://s.shopee.com.br/9AP2eWq9OA\n\n"
                "anúncio")
AOC43_35170 = ("Smart TV 43 polegadas 43S5155 78G Full HD LED Wi-Fi Roku TV Dolby Audio AOC\n\n"
               "R$ 1162\n-Resgate todos os cupons na Live (sacola laranja) no APP aqui:\n"
               "https://s.shopee.com.br/50ZTgk4qQ4\n\n"
               "-Link produto (add pela sacola da Live:\nhttps://s.shopee.com.br/50ZTgk4qQ4\n\n"
               "-Anúncio")
CHAVE_LIVE = "shopee|url|https://live.shopee.com.br/live/7199917"
MUTEX_LIVE = "shopee|url*|https://live.shopee.com.br/live/7199917"

# Envio ao Telegram com latência: abre a janela entre a escolha da
# família (sem lock do post) e o registro do post — onde a corrida mora.
_LATENCIA_ENVIO = 0.05


def live(chat, texto, msg_id, pids=()):
    """Mensagem Shopee cujos links resolvem para a live 7199917 (mesmo
    molde de test_live_container.live). `pids` = link de PRODUTO."""
    pids = list(pids)
    return MensagemNormalizada(
        msg_id=msg_id, chat=str(chat), texto_limpo=texto, texto_analise=texto,
        mapa={"http://o": "http://a"}, preservar=[], plat="shopee",
        sku=(pids[0] if pids else ""), tem_midia=False, media_obj=None,
        ids_globais=pids, idents=[("shopee", p, "produto") for p in pids],
        cupons=[], midia_key="",
        ancora_url=derivar_ancora_url([_canonica_live(u) for u in LIVE_7199917]))


def _lento(c):
    envio, arquivo, edicao = c.send_message, c.send_file, c.edit_message

    async def send_message(*a, **k):
        await asyncio.sleep(_LATENCIA_ENVIO)
        return await envio(*a, **k)

    async def send_file(*a, **k):
        await asyncio.sleep(_LATENCIA_ENVIO)
        return await arquivo(*a, **k)

    async def edit_message(*a, **k):
        await asyncio.sleep(_LATENCIA_ENVIO)
        return await edicao(*a, **k)
    c.send_message, c.send_file, c.edit_message = send_message, send_file, edit_message


def _juntos(pares):
    """pares: [(norm, score)] publicados ao MESMO tempo. Devolve o
    cliente e o post de cada mensagem (via vínculo de origem)."""
    out = {}

    async def corpo(c):
        _lento(c)
        await asyncio.gather(*(publicar(n, score=s) for n, s in pares))
        out["dest"] = [origem.consultar(n.chat, n.msg_id) for n, _s in pares]
        out["texto"] = {m: (db_get_post(m) or {}).get("texto", "") for m in c.ids}
        out["fam"] = {m: set(db_ofertas_de_post(m)) for m in c.ids}
        out["exib"] = {m: set(db_exibida(m)) for m in c.ids}
    cli = cenario(corpo)
    return cli, out


# ══════════════════════════════════════════════════════════════════
# cupom sem nome — genérico × assinatura
# ══════════════════════════════════════════════════════════════════
def test_01_seco_e_rico_juntos_um_post(r):
    seco, rico = msg(PROMOTOM, SECO_110370), msg(FADA, RICO_17525)
    cli, out = _juntos([(seco, 3), (rico, 10)])
    r.check(cli.novos == 1, "01.um_post", f"novos={cli.novos} dest={out['dest']}")
    r.check(len(set(out["dest"])) == 1, "01.mesma_origem_mesmo_post", str(out["dest"]))
    r.check(all("R$30 OFF em R$299" in t for t in out["texto"].values()),
            "01.rico_no_ar", str(out["texto"]))


def test_02_rico_e_seco_ordem_inversa_um_post(r):
    rico, seco = msg(SAMUEL, RICO_118699), msg(PROMOTOM, SECO_110370)
    cli, out = _juntos([(rico, 10), (seco, 3)])
    r.check(cli.novos == 1, "02.um_post", f"novos={cli.novos} dest={out['dest']}")
    r.check(all("R$ 30 OFF em R$ 299" in t for t in out["texto"].values()),
            "02.seco_nao_rebaixa", str(out["texto"]))
    fam = next(iter(out["fam"].values()), set())
    r.check("shopee|cupb|geral" in fam, "02.seco_ensina_a_ancora", str(fam))


def test_03_dois_ricos_diferentes_nao_colapsam(r):
    a, b = msg(FADA, RICO_17525), msg(SAMUEL, RICO_17511)
    cli, out = _juntos([(a, 10), (b, 10)])
    r.check(cli.novos == 2 and out["dest"][0] != out["dest"][1],
            "03.dois_ricos_dois_posts", f"novos={cli.novos} {out['dest']}")
    # a rodada inteira junta: o seco vai para UM dos dois ricos
    seco = msg(PROMOTOM, SECO_110370)
    a, b = msg(FADA, RICO_17525), msg(SAMUEL, RICO_17511)
    cli, out = _juntos([(seco, 3), (a, 10), (b, 10)])
    d_seco, d_a, d_b = out["dest"]
    r.check(cli.novos == 2 and d_a != d_b and d_seco in (d_a, d_b),
            "03.trio_dois_posts_seco_em_um", f"novos={cli.novos} {out['dest']}")
    com_geral = [m for m, f in out["fam"].items() if "shopee|cupb|geral" in f]
    r.check(len(com_geral) == 1, "03.seco_em_exatamente_um", str(out["fam"]))


# ══════════════════════════════════════════════════════════════════
# container LIVE
# ══════════════════════════════════════════════════════════════════
P32_A, P32_B = "shopee|7199917.32001", "shopee|7199917.32002"


def test_04_mesma_live_titulos_diferentes_separados(r):
    so_live_43 = live(FUMOTOM, AOC43_35170, 35170)
    forte_32 = live(SAMUEL, AOC32_118688, 118688, pids=["7199917.32001"])
    cli, out = _juntos([(so_live_43, 5), (forte_32, 5)])
    d43, d32 = out["dest"]
    r.check(cli.novos == 2 and d43 != d32, "04.dois_posts",
            f"novos={cli.novos} {out['dest']}")
    r.check("43 polegadas" in out["texto"].get(d43, "")
            and "32 Polegadas" in out["texto"].get(d32, ""),
            "04.cada_titulo_no_seu_post", str(out["texto"]))
    r.check(P32_A not in out["fam"].get(d43, set()), "04.forte_nao_entra_no_43",
            str(out["fam"]))


def test_05_mesma_live_titulo_equivalente_adota(r):
    for nome, ordem in (("so_live_primeiro", 0), ("forte_primeiro", 1)):
        so_live = live(FUMOTOM, AOC32_35169, 35169)
        forte = live(SAMUEL, AOC32_118688, 118688, pids=["7199917.32001"])
        pares = [(so_live, 5), (forte, 5)]
        if ordem:
            pares.reverse()
        cli, out = _juntos(pares)
        r.check(cli.novos == 1 and len(set(out["dest"])) == 1,
                f"05.{nome}.um_post", f"novos={cli.novos} {out['dest']}")
        mid = out["dest"][0]
        r.check(P32_A in out["exib"].get(mid, set()) | out["fam"].get(mid, set()),
                f"05.{nome}.forte_e_a_referencia", str(out["exib"]))


def test_06_duas_adocoes_simultaneas_da_mesma_live(r):
    """Post só-live do AOC 32" no ar; dois fortes DIFERENTES com o mesmo
    título chegam juntos. Doutrina LIVE: forte conflitante não entra pelo
    título — a primeira adoção vale, a segunda é post próprio.
    O lock do post é atrasado (post ocupado por outra escrita): as duas
    escolhem o post da live antes de qualquer uma escrever — a janela real
    da corrida (mesmo molde de test_cupom_adocao._corrida)."""
    real = exclusao.lock_post

    async def lock_post_ocupado(mid):
        await asyncio.sleep(0.02)
        return await real(mid)

    for rot, primeiro in (("A_primeiro", 0), ("B_primeiro", 1)):
        out = {}
        a = live(SAMUEL, AOC32_118688, 118688, pids=["7199917.32001"])
        b = live(PROMOTOM, AOC32_110368, 110368, pids=["7199917.32002"])

        async def corpo(c, a=a, b=b, primeiro=primeiro):
            await publicar(live(FUMOTOM, AOC32_35169, 35169), score=5)
            out["live"] = c.ids[-1]
            _lento(c)
            pares = [publicar(a, score=5), publicar(b, score=5)]
            if primeiro:
                pares.reverse()
            exclusao.lock_post = lock_post_ocupado
            try:
                await asyncio.gather(*pares)
            finally:
                exclusao.lock_post = real
            out["da"], out["db"] = (origem.consultar(a.chat, a.msg_id),
                                    origem.consultar(b.chat, b.msg_id))
            out["n"] = c.novos
            out["fam"] = {m: set(db_ofertas_de_post(m)) | set(db_exibida(m))
                          for m in c.ids}
        cenario(corpo)
        vencedor, perdedor = ((out["da"], out["db"]) if not primeiro
                              else (out["db"], out["da"]))
        forte_v, forte_p = (P32_A, P32_B) if not primeiro else (P32_B, P32_A)
        r.check(out["n"] == 2, f"06.{rot}.dois_posts", str(out))
        r.check(vencedor == out["live"], f"06.{rot}.primeiro_adota_o_post_da_live", str(out))
        r.check(perdedor != out["live"], f"06.{rot}.segundo_post_proprio", str(out))
        r.check(forte_p not in out["fam"].get(out["live"], set())
                and forte_v in out["fam"].get(out["live"], set()),
                f"06.{rot}.sem_contaminacao", str(out["fam"]))


def test_07_tres_copias_reais_do_aoc32_juntas(r):
    """21:51:14 fumotom (só live), 21:51:21 Promotom (só live), 21:51:28
    Samuel (live + produto) — um post só, qualquer que seja a ordem."""
    for rot, ordem in (("ordem_real", (0, 1, 2)), ("forte_primeiro", (2, 0, 1))):
        msgs = [live(FUMOTOM, AOC32_35169, 35169),
                live(PROMOTOM, AOC32_110368, 110368),
                live(SAMUEL, AOC32_118688, 118688, pids=["7199917.32001"])]
        cli, out = _juntos([(msgs[i], 5) for i in ordem])
        r.check(cli.novos == 1 and len(set(out["dest"])) == 1,
                f"07.{rot}.um_post", f"novos={cli.novos} {out['dest']}")


# ══════════════════════════════════════════════════════════════════
# o mutex é SÓ mutex
# ══════════════════════════════════════════════════════════════════
def test_08_mutex_nunca_vira_ancora(r):
    m = familia.mutexes_de_adocao
    r.check(m(["shopee|cupb|geral"]) == ["shopee|cupb|*"], "08.geral", str(m(["shopee|cupb|geral"])))
    r.check(m(["shopee|cupb|v:30-299", "shopee|cupb|v:90-899"]) == ["shopee|cupb|*"],
            "08.assinatura")
    r.check(m(["amazon|cupb|v:50-300"]) == ["amazon|cupb|*"], "08.idioma_da_plataforma")
    r.check(m(["shopee|cupb|tech"]) == [], "08.nome_nao_adota_sem_mutex")
    r.check(m(["shopee|cup|V4M0S4PR0V3IT4R"]) == [], "08.codigo_sem_mutex")
    r.check(m(["shopee|cupb|geral"], destinos=("lista:x",)) == [], "08.destino_sem_mutex")
    r.check(m(["shopee|cupb|v:10-50"], container=CHAVE_LIVE) == [MUTEX_LIVE],
            "08.cupb_com_live_so_mutex_da_live")
    r.check(m([CHAVE_LIVE]) == [MUTEX_LIVE], "08.so_live")
    r.check(m([P32_A], container=CHAVE_LIVE) == [MUTEX_LIVE], "08.forte_com_live")
    r.check(m([P32_A]) == [] and m([]) == [] and m([], container=CHAVE_LIVE) == [],
            "08.sem_adocao_sem_mutex")
    sinteticas = m(["shopee|cupb|geral"]) + m([CHAVE_LIVE])
    r.check(not any(familia.eh_chave_container(k) or eh_chave_forte(k)
                    or eh_chave_assinatura(k) or k == "shopee|cupb|geral"
                    for k in sinteticas), "08.mutex_nao_e_chave_real", str(sinteticas))

    # depois de toda a bateria acima, nenhuma tabela tem o mutex
    seco, rico = msg(PROMOTOM, SECO_110370), msg(FADA, RICO_17525)
    out = {}

    async def corpo(c):
        _lento(c)
        await asyncio.gather(publicar(seco, score=3), publicar(rico, score=10),
                             publicar(live(FUMOTOM, AOC32_35169, 35169), score=5),
                             publicar(live(SAMUEL, AOC32_118688, 118688,
                                           pids=["7199917.32001"]), score=5))
        with _db() as db:
            tabelas = [t for (t,) in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'")]
            achados = []
            for t in tabelas:
                cols = [c_[1] for c_ in db.execute(f"PRAGMA table_info({t})")]
                for col in cols:
                    n = db.execute(
                        f"SELECT COUNT(*) FROM {t} WHERE CAST({col} AS TEXT) LIKE '%|*%'"
                        f" OR CAST({col} AS TEXT) LIKE '%|url*|%'"
                    ).fetchone()[0]
                    if n:
                        achados.append((t, col, n))
        out["achados"] = achados
    cenario(corpo)
    r.check(out["achados"] == [], "08.nenhuma_linha_com_mutex", str(out["achados"]))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CORRIDA DE ADOÇÃO · mutex sintético cupb/LIVE"))
