"""
AMAZON — o host inteiro não é campanha (P0-1 da auditoria).

plataformas/amazon.py declarava `amazon.com.br` em _HOSTS_CAMPANHA. A
âncora de campanha é host+CAMINHO, SEM query: toda busca virava
`amazon|camp|amazon.com.br/s`, toda categoria `amazon|camp|amazon.com.br/b`
— páginas diferentes na MESMA família (uma busca de fones EVOLUÍA o post
de uma busca de livros). Correção: só `primevideo.com` continua host de
campanha. Nenhuma heurística nova; normalizador intocado.

O que cada página passa a usar — tudo já existente:
  · /dp/<ASIN>        → âncora de PRODUTO (forte), como sempre;
  · /promotion/psp/ID → fallback de URL sobre a canônica afiliada
    (`/promotion/psp/<ID>?tag=<nossa>`: a query da fonte é descartada),
    estável e convergente entre fontes;
  · /s, /b, /deals    → fallback de URL sobre a canônica afiliada, que
    PRESERVA os parâmetros que definem a página (k, node, rh, keywords).

CORPUS REAL: fumotom 35372 ("20% off em Brinquedos", 01/10 17:04) e
Samuel 118808 (17:08) — a promoção psp A1P4S4HG0ZKYIC dos logs de
produção; Samuel 118332 (Mesa Posta, Dia a Dia). Os encurtadores
(amzn.to, link.amazon) são bloqueados aqui: a URL expandida de cada
fonte é sintética, com rastreamento DIFERENTE por fonte, e passa pela
canônica REAL (amazon._construir_url_afiliada) como no pipeline.

  01  /s?k=fones × /s?k=livros → identidades próprias, dois posts
  02  /b?node=A × /b?node=B → dois posts
  03  /deals × /s × /b → nenhum colapso; /deals tem fallback próprio
  04  psp A1P4S4HG0ZKYIC das duas fontes → MESMA âncora, um post;
      psp diferentes → posts diferentes
  05  /dp/<ASIN> → âncora de produto idêntica; duas fontes, um post
  06  primevideo.com continua campanha; registry só perdeu amazon.com.br
  07  fast-path da tag intacto: /dp/ preserva tudo e troca só a tag

    python tests/test_amazon_campanha.py
"""
import os
import sys

os.environ.setdefault("AMAZON_TAG", "fullpromotion-20")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_cupom_sem_codigo import (                             # noqa: E402
    FADA, MOTIVOS, SAMUEL, cenario, msg, publicar)
from _harness_e5 import rodar                                   # noqa: E402
from pipeline.identidade_oferta import ancoras                  # noqa: E402
from plataformas import amazon, registry                        # noqa: E402

FUMOTOM = -1003775401737
TAG = amazon._AMZ_TAG

T_35372 = "20% off em Brinquedos\nhttps://link.amazon/B060Df8lG"
T_118808 = "🔥 20% off em Brinquedos\n\nhttps://amzn.to/4jz1ssz\n\nanúncio"
T_MESA_POSTA = "🔥 Exclusivo Prime: 15% Off em Mesa Posta -limitado a R$ 100\nhttps://amzn.to/47oWw24"
T_DIA_A_DIA = "🔥 Dia a Dia com 15% off\nhttps://amzn.to/4iZZZZZ"


def canonica(url_expandida):
    """A URL longa que o pipeline entrega à identidade (Afiliacao.canonica)."""
    return amazon._construir_url_afiliada(url_expandida)


def amz(chat, texto, url_expandida):
    return msg(chat, texto, urls_longas=[canonica(url_expandida)], plat="amazon")


def chaves(n):
    return [a.chave for a in ancoras(n)]


def _dois(a, b, sa=7, sb=9):
    out = {}

    async def corpo(c):
        await publicar(a, score=sa)
        await publicar(b, score=sb)
        out["novos"], out["edits"] = c.novos, list(c.edits)
        out["motivos"] = list(MOTIVOS)
    cenario(corpo)
    return out


def _sem_camp_generica(r, rot, *ns):
    for n in ns:
        r.check(not any(k.startswith("amazon|camp|amazon.com.br") for k in chaves(n)),
                f"{rot}.sem_ancora_host_caminho", str(chaves(n)))


def test_01_buscas_diferentes_nao_colapsam(r):
    a = amz(FADA, "🔥 Ofertas de FONES na Amazon\n✅ a partir de R$ 49\nhttps://amzn.to/aaa",
            "https://www.amazon.com.br/s?k=fone+bluetooth&ref=nb_sb_noss&tag=outra-20")
    b = amz(SAMUEL, "📚 Livros com até 70% OFF na Amazon\n✅ a partir de R$ 9,90\nhttps://amzn.to/bbb",
            "https://www.amazon.com.br/s?k=livros&linkCode=ll2&tag=outra-21")
    _sem_camp_generica(r, "01", a, b)
    r.check(set(chaves(a)).isdisjoint(chaves(b)), "01.identidades_proprias",
            f"{chaves(a)} {chaves(b)}")
    out = _dois(a, b)
    r.check(out["novos"] == 2 and not out["edits"], "01.dois_posts_sem_sobrescrita", str(out))


def test_02_categorias_diferentes_nao_colapsam(r):
    a = amz(FADA, "🔥 Semana do Cozinheiro Amazon\nhttps://amzn.to/ccc",
            "https://www.amazon.com.br/b?node=17125300011&ref_=sr_bc&tag=outra-20")
    b = amz(SAMUEL, "🎮 Games em oferta Amazon\nhttps://amzn.to/ddd",
            "https://www.amazon.com.br/b?node=7791985011&tag=outra-21")
    _sem_camp_generica(r, "02", a, b)
    r.check(set(chaves(a)).isdisjoint(chaves(b)), "02.identidades_proprias",
            f"{chaves(a)} {chaves(b)}")
    out = _dois(a, b, 5, 5)
    r.check(out["novos"] == 2, "02.dois_posts", str(out))


def test_03_deals_busca_categoria(r):
    d = amz(FADA, "🔥 Ofertas do Dia Amazon\nhttps://amzn.to/eee",
            "https://www.amazon.com.br/deals?ref_=nav_cs_gb&tag=outra-20")
    s = amz(SAMUEL, "🔥 Busca: air fryer\nhttps://amzn.to/fff",
            "https://www.amazon.com.br/s?k=air+fryer&tag=outra-21")
    b = amz(FUMOTOM, "Semana do Café\nhttps://amzn.to/ggg",
            "https://www.amazon.com.br/b?node=19688716011&tag=outra-22")
    _sem_camp_generica(r, "03", d, s, b)
    kd = chaves(d)
    r.check(kd and all(k.startswith("amazon|url|") for k in kd)
            and "/deals" in kd[0], "03.deals_fallback_proprio", str(kd))
    r.check(len({tuple(chaves(x)) for x in (d, s, b)}) == 3, "03.tres_identidades")
    out = {}

    async def corpo(c):
        for n in (d, s, b):
            await publicar(n, score=6)
        out["novos"] = c.novos
    cenario(corpo)
    r.check(out["novos"] == 3, "03.tres_posts", str(out))


PSP = "A1P4S4HG0ZKYIC"


def test_04_psp_estavel_e_convergente(r):
    a = amz(FUMOTOM, T_35372,
            f"https://www.amazon.com.br/promotion/psp/{PSP}?ref=cm_sw_r_cp_ud&tag=fumo-20&smid=X1")
    b = amz(SAMUEL, T_118808,
            f"https://www.amazon.com.br/promotion/psp/{PSP}?linkCode=ml1&tag=samuel-20&ref_=as_li_ss_tl")
    esperada = f"amazon|url|https://www.amazon.com.br/promotion/psp/{PSP}?tag={TAG}"
    r.check(chaves(a) == [esperada] and chaves(b) == [esperada], "04.mesma_ancora_estavel",
            f"{chaves(a)} {chaves(b)}")
    out = _dois(a, b, 3, 3)
    r.check(out["novos"] == 1, "04.duas_fontes_um_post", str(out))
    m = amz(SAMUEL, T_MESA_POSTA, "https://www.amazon.com.br/promotion/psp/A3KK1ZPL75H5CX?ref=x")
    d = amz(SAMUEL, T_DIA_A_DIA, "https://www.amazon.com.br/promotion/psp/A35PZE8CHMX66X?ref=y")
    out = _dois(m, d, 3, 3)
    r.check(out["novos"] == 2, "04.psp_diferentes_posts_diferentes", str(out))


ASIN = "B0DJDHDXT1"


def test_05_produto_dp_inalterado(r):
    a = amz(FADA, "🔥 Produto Amazon\n✅ R$ 199\nhttps://amzn.to/hhh",
            f"https://www.amazon.com.br/Produto-Qualquer/dp/{ASIN}?th=1&psc=1&tag=outra-20")
    b = amz(SAMUEL, "🔥 Produto Amazon\n💵 R$ 189\nhttps://amzn.to/iii",
            f"https://www.amazon.com.br/dp/{ASIN}?tag=outra-21")
    r.check(chaves(a) == [f"amazon|{ASIN}"] == chaves(b), "05.ancora_de_produto",
            f"{chaves(a)} {chaves(b)}")
    r.check(a.ids_globais == [f"amazon:{ASIN}"] or a.ids_globais == [ASIN]
            or ASIN in str(a.ids_globais), "05.id_global_do_produto", str(a.ids_globais))
    out = _dois(a, b, 7, 9)
    r.check(out["novos"] == 1, "05.duas_fontes_um_post", str(out))


def test_06_primevideo_continua_campanha(r):
    hosts = registry.compor_capacidade("hosts_campanha")
    r.check("primevideo.com" in hosts and "amazon.com.br" not in hosts,
            "06.registry", str(sorted(hosts)))
    r.check(amazon._HOSTS_CAMPANHA == frozenset({"primevideo.com"}), "06.amazon_so_primevideo")
    p = msg(FADA, "🎬 Prime Video: 30 dias grátis\nhttps://amzn.to/jjj",
            urls_longas=["https://www.primevideo.com/offers/nonprimehomepage?tag=x"],
            plat="amazon")
    r.check(any(k.startswith("amazon|camp|primevideo.com") for k in chaves(p)),
            "06.primevideo_ancora_campanha", str(chaves(p)))


def test_07_fast_path_da_tag_intacto(r):
    entrada = (f"https://www.amazon.com.br/Produto/dp/{ASIN}"
               "?th=1&psc=1&linkCode=ll1&tag=outra-20&ref_=as_li_ss_tl")
    saida = amazon._construir_url_afiliada(entrada)
    r.check(saida == entrada.replace("tag=outra-20", f"tag={TAG}"),
            "07.troca_so_a_tag", saida)


if __name__ == "__main__":
    sys.exit(rodar(globals(), "AMAZON · host genérico não é campanha"))
