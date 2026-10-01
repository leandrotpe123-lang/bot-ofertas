"""
SEÇÃO DE LOJA — post multi-loja: a loja cujo link não converteu sai
INTEIRA (nome + cupom + link), não só a linha do link.

Teste do operador (01/10 00:37 UTC, @ofertasconvertidas 1352 → post
24384, God of War Laufey): Kabum / Amazon / ML, cada uma num parágrafo
"-Loja / -CUPOM: código / link". Só a Amazon converteu; o post saiu com
"-Kabum + CUPOM: VIPOFWAR" e "-ML + CUPOM: TODOSITE3009" soltos — e os
três códigos continuavam na oferta (norm.cupons).

Os textos saem do PRÓPRIO Telethon (`markdown.unparse`).

    python tests/test_secao_loja.py
"""
import asyncio
import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# Telethon REAL antes do harness (o stub só entra se ainda não houver).
from telethon.extensions import markdown                        # noqa: E402
from telethon.tl.types import (MessageEntityCode,               # noqa: E402
                               MessageEntityTextUrl,
                               MessageEntityUrl)
from _harness_e5 import preparar, rodar  # noqa: E402

preparar()

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
from pipeline.filtros import filtrar, filtrar_blocos            # noqa: E402
from pipeline.ingestao import MensagemBruta, _RE_URL            # noqa: E402
from pipeline.identidade_oferta import identidades              # noqa: E402
from pipeline.montagem_texto import montar_texto                # noqa: E402
from pipeline.normalizacao_texto import sem_marcacao            # noqa: E402

plataformas.inicializar()

KABUM, AMAZON, ML = ("https://cutt.ly/kyb5dtFB", "https://link.amazon/B08aSbqvn",
                     "https://meli.la/2cJVXse")
AFF_AMAZON = "https://link.amazon/B05Ce0h0q"

# Texto REAL da @ofertasconvertidas 1352.
RAW_1352 = ("Ainda ativo! Pré-venda God of War Laufey™ - PlayStation 5\n\nR$ 333\n\n"
            f"-Kabum\n-CUPOM: VIPOFWAR\n{KABUM}\n\n"
            f"-Amazon\n-CUPOM: PRIMEGAME5\n{AMAZON}\n\n"
            f"-ML\n-CUPOM: TODOSITE3009\n{ML}")
CODIGOS_1352 = ["VIPOFWAR", "PRIMEGAME5", "TODOSITE3009"]

ESPERADO_1352 = ("🔥 Ainda ativo! Pré-venda God of War Laufey™ - PlayStation 5\n\n"
                 "✅ R$ 333\n\n"
                 "-Amazon\n🎟 CUPOM: PRIMEGAME5\n" + AFF_AMAZON)


def _off(texto, sub):
    return len(texto[:texto.index(sub)].encode("utf-16-le")) // 2


def telegram(raw, codigos, urls, embutido=False):
    """`message.text` como o Telethon entrega (código + links)."""
    ents = [MessageEntityCode(offset=_off(raw, c), length=len(c)) for c in codigos]
    for u in urls:
        ents.append(MessageEntityTextUrl(offset=_off(raw, u), length=len(u), url=u)
                    if embutido else MessageEntityUrl(offset=_off(raw, u), length=len(u)))
    return markdown.unparse(raw, ents)


def links_da_ingestao(texto):
    return [u.strip().rstrip('.,;)>]}!?') for u in _RE_URL.findall(texto)]


def normalizar_com(texto, codigos, convertem):
    """normalizar() com a afiliação simulada: só as URLs em `convertem`
    convertem (valor = URL publicada)."""
    async def falso(converter, msg_id=0):
        mapa = {u: convertem[u] for u in converter if u in convertem}
        return mapa, dict(mapa), ["amazon"] * len(mapa)
    real = normalizacao.resolver_e_afiliar
    normalizacao.resolver_e_afiliar = falso
    try:
        bruta = MensagemBruta(msg_id=1352, chat="-1003817694320", texto=texto,
                              links=links_da_ingestao(texto), tem_midia=False,
                              media_obj=None, code_entities=list(codigos))
        return asyncio.run(normalizacao.normalizar(bruta))
    finally:
        normalizacao.resolver_e_afiliar = real


def post(norm):
    return sem_marcacao(montar_texto(norm)).strip()


# ══════════════════════════════════════════════════════════════════
# PARTE 1 — Replay real
# ══════════════════════════════════════════════════════════════════
def test_01_replay_1352_so_amazon_converte(r):
    for embutido in (False, True):
        tag = "embutido" if embutido else "url"
        texto = telegram(RAW_1352, CODIGOS_1352, (KABUM, AMAZON, ML), embutido)
        norm = normalizar_com(texto, CODIGOS_1352, {AMAZON: AFF_AMAZON})
        r.check(norm is not None, f"01.{tag}.publica")
        if norm is None:
            continue
        saida = post(norm)
        r.check(saida == ESPERADO_1352, f"01.{tag}.post_exato", repr(saida))
        r.check(norm.cupons == ["PRIMEGAME5"], f"01.{tag}.so_cupom_que_foi_ao_ar",
                str(norm.cupons))
        r.check(not any(x in saida for x in ("Kabum", "VIPOFWAR", "-ML", "TODOSITE3009")),
                f"01.{tag}.lojas_sem_conversao_fora")


# ══════════════════════════════════════════════════════════════════
# PARTE 2 — Variações
# ══════════════════════════════════════════════════════════════════
def test_02_variacoes_de_quais_lojas_convertem(r):
    texto = telegram(RAW_1352, CODIGOS_1352, (KABUM, AMAZON, ML))
    todos = {KABUM: "https://k.aff/1", AMAZON: AFF_AMAZON, ML: "https://ml.aff/3"}
    casos = {
        "kabum_e_amazon": ({KABUM: todos[KABUM], AMAZON: AFF_AMAZON},
                           ["Kabum", "VIPOFWAR", "Amazon", "PRIMEGAME5"], ["-ML", "TODOSITE3009"]),
        "todas": (todos, ["Kabum", "VIPOFWAR", "Amazon", "PRIMEGAME5", "-ML", "TODOSITE3009"], []),
        "so_a_primeira_falha": ({AMAZON: AFF_AMAZON, ML: todos[ML]},
                                ["Amazon", "PRIMEGAME5", "-ML", "TODOSITE3009"], ["Kabum", "VIPOFWAR"]),
        "so_a_ultima_falha": ({KABUM: todos[KABUM], AMAZON: AFF_AMAZON},
                              ["Kabum", "Amazon"], ["-ML", "TODOSITE3009"]),
    }
    for nome, (conv, ficam, saem) in casos.items():
        norm = normalizar_com(texto, CODIGOS_1352, conv)
        saida = post(norm)
        r.check(all(x in saida for x in ficam), f"02.{nome}.ficam", repr(saida))
        r.check(not any(x in saida for x in saem), f"02.{nome}.saem", repr(saida))
        r.check(saida.startswith("🔥 Ainda ativo!") and "R$ 333" in saida,
                f"02.{nome}.titulo_e_preco")


def test_03_secao_com_rotulo_e_preco_por_loja(r):
    raw = ("Monitor LG UltraGear 27 144Hz\n\n"
           "-Kabum\nR$ 999\nResgate aqui:\nhttps://cutt.ly/abc\n\n"
           "-Amazon\nR$ 1049\nhttps://link.amazon/xyz")
    saida = filtrar_blocos(filtrar(raw), {"https://link.amazon/xyz": "https://a.aff/1"})
    r.check("Kabum" not in saida and "R$ 999" not in saida and "Resgate" not in saida,
            "03.secao_kabum_inteira_sai", repr(saida))
    r.check("Monitor LG" in saida and "-Amazon" in saida and "R$ 1049" in saida,
            "03.resto_fica", repr(saida))


# ══════════════════════════════════════════════════════════════════
# PARTE 3 — Proteções
# ══════════════════════════════════════════════════════════════════
def test_04_primeiro_paragrafo_nunca_sai(r):
    """Título, preço e cupom no MESMO parágrafo do link que falhou: é o
    corpo da oferta — fica (só a linha do link sai, como hoje)."""
    raw = ("Fone JBL Tune 520BT\nR$ 199\n-CUPOM: JBL10\nhttps://cutt.ly/aaa\n\n"
           "-Amazon\nhttps://link.amazon/zzz")
    saida = filtrar_blocos(filtrar(raw), {"https://link.amazon/zzz": "https://a.aff/2"})
    r.check("Fone JBL" in saida and "R$ 199" in saida and "JBL10" in saida,
            "04.corpo_preservado", repr(saida))
    r.check("cutt.ly" not in saida, "04.link_sem_conversao_sai")


def test_05_linha_longa_preserva_o_paragrafo(r):
    raw = ("Kit Ferramentas Bosch\n\nR$ 299\n\n"
           "Na Kabum o kit vem com maleta reforçada e 2 anos de garantia estendida\n"
           "https://cutt.ly/bbb\n\n-Amazon\nhttps://link.amazon/yyy")
    saida = filtrar_blocos(filtrar(raw), {"https://link.amazon/yyy": "https://a.aff/3"})
    r.check("maleta reforçada" in saida, "05.descricao_longa_fica", repr(saida))
    r.check("cutt.ly" not in saida, "05.link_sai")


def test_06_sem_outro_link_publicavel_nao_amplia_remocao(r):
    """Se nenhum parágrafo manteve link publicável, a regra nova não age
    (o post nem é publicável; comportamento atual intacto)."""
    raw = "Titulo X\n\nR$ 10\n\n-Kabum\n-CUPOM: KB5\nhttps://cutt.ly/ccc"
    saida = filtrar_blocos(filtrar(raw), {})
    r.check("-Kabum" in saida and "KB5" in saida, "06.sem_ampliacao", repr(saida))


def test_07_secao_mantem_se_algum_link_dela_converteu(r):
    raw = ("Notebook Acer Nitro\n\n"
           "-Amazon\n-CUPOM: AMZ10\nhttps://cutt.ly/ddd\nhttps://link.amazon/www\n\n"
           "-Shopee\nhttps://s.shopee.com.br/sok")
    saida = filtrar_blocos(filtrar(raw), {"https://link.amazon/www": "https://a.aff/4",
                                          "https://s.shopee.com.br/sok": "https://s.aff/9"})
    r.check("-Amazon" in saida and "AMZ10" in saida and "link.amazon/www" in saida,
            "07.secao_com_link_vivo_fica", repr(saida))
    r.check("cutt.ly" not in saida, "07.so_o_link_morto_sai")


def test_07b_outro_link_precisa_ser_publicavel(r):
    """Só link AUXILIAR mantido (não publicável) em outro parágrafo não
    autoriza remover a seção inteira."""
    raw = ("Cupom Shopee Moda\n\n-Kabum\n-CUPOM: KB7\nhttps://cutt.ly/eee\n\n"
           "-Regras da Shopee:\nhttps://exemplo.org/regras")
    saida = filtrar_blocos(filtrar(raw), {})
    r.check("-Kabum" in saida and "KB7" in saida, "07b.secao_fica", repr(saida))


def test_08_auxiliar_ancorado_regra_atual(r):
    raw = ("Cupom Shopee 20% OFF\n\n-Regras da Shopee:\nhttps://exemplo.org/regras\n\n"
           "-Resgate:\nhttps://s.shopee.com.br/ok")
    saida = filtrar_blocos(filtrar(raw), {"https://s.shopee.com.br/ok": "https://s.aff/1"})
    r.check("exemplo.org" in saida, "08.auxiliar_ancorado_fica", repr(saida))


# ══════════════════════════════════════════════════════════════════
# PARTE 4 — Cupom que não foi ao ar não é identidade
# ══════════════════════════════════════════════════════════════════
def test_09_cupom_de_loja_removida_nao_vira_identidade(r):
    raw = ("Cupons de Games\n\n"
           f"-Kabum\n-CUPOM: KABUMGAME\n{KABUM}\n\n"
           f"-Amazon\n-CUPOM: AMZGAME\n{AMAZON}")
    cods = ["KABUMGAME", "AMZGAME"]
    norm = normalizar_com(telegram(raw, cods, (KABUM, AMAZON)), cods, {AMAZON: AFF_AMAZON})
    r.check(norm is not None and norm.cupons == ["AMZGAME"], "09.cupons",
            str(norm and norm.cupons))
    ids = identidades(norm) if norm else []
    r.check(not any("KABUMGAME" in k for k in ids), "09.identidade_sem_cupom_removido",
            str(ids))
    # código removido que aparece só DENTRO de outro código mantido
    raw2 = ("Cupons de Games\n\n"
            f"-Kabum\n-CUPOM: GAME5\n{KABUM}\n\n"
            f"-Amazon\n-CUPOM: PRIMEGAME5\n{AMAZON}")
    cods2 = ["GAME5", "PRIMEGAME5"]
    norm2 = normalizar_com(telegram(raw2, cods2, (KABUM, AMAZON)), cods2, {AMAZON: AFF_AMAZON})
    r.check(norm2 is not None and norm2.cupons == ["PRIMEGAME5"], "09.codigo_contido_em_outro",
            str(norm2 and norm2.cupons))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "SEÇÃO DE LOJA · post multi-loja · post 24384"))
