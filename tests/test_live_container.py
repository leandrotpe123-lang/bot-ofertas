"""
INCIDENTE 30/09 12:15 BRT — post 24262: produtos diferentes da MESMA live.

Controle Xbox (fumotom 35064), Caixa AIWA (35067) e Caixa Philco
(35068) — e as cópias do Promotom (110352) e do Samuel (118607, 118609,
118610) — vieram só com links da live Shopee 7187289. As URLs reais
expandidas NÃO carregam produto; todas as mensagens ganharam a mesma
âncora fraca `shopee|url|https://live.shopee.com.br/live/7187289` e
caíram no post 24262: edições das caixas viraram SYNC (líder por canal)
e a mídia da Philco foi parar no post do Controle.

Regras testadas:
  C1  a âncora de CONTAINER (live) sozinha não junta produtos: título
      equivalente casa; diferente ou ausente, não casa.
  C2  SYNC só da MENSAGEM cujo texto está no ar (lider_msg); legado
      (NULL) mantém a regra por canal.

Caminho REAL (enriquecer → montar → publicacao.enviar), SQLite real em
diretório temporário, Telegram falso que identifica a ORIGEM de cada
imagem enviada/editada.

    python tests/test_live_container.py
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
from database import _init_db, db_get_post                     # noqa: E402
from database_conexao import _db                               # noqa: E402
from database_posts import db_registrar_post, db_fundir_posts  # noqa: E402
from pipeline import exclusao, origem, familia                 # noqa: E402
from pipeline import publicacao, convergencia                  # noqa: E402
from pipeline.montagem import montar                            # noqa: E402
from pipeline.enriquecimento import enriquecer, enriquecer_edicao  # noqa: E402
from pipeline.normalizacao import MensagemNormalizada           # noqa: E402
from pipeline.normalizacao_identidade import derivar_ancora_url  # noqa: E402
from pipeline.resolucao_identidade import eh_chave_forte        # noqa: E402
from plataformas.contrato import TipoLink                       # noqa: E402
from plataformas.shopee.links import _canonica_live, extrai_identidade  # noqa: E402

plataformas.inicializar()
_init_db()
convergencia._ESPERA_BASE_S = 0.0

FUMOTOM, PROMOTOM, SAMUEL = "-1003775401737", "-1001825680721", "-1001768101197"
CA, CB, CD = "-1009100000001", "-1009100000002", "-1009100000004"

# URLs REAIS expandidas (fornecidas pelo operador) dos dois links do
# Controle na fumotom 35064: ambas são a MESMA live, sem produto.
URL_LIVE_1 = ("https://live.shopee.com.br/universal-link/share?from=live"
              "&mmp_pid=an_18139140007&session=7187289&share_user_id=293588261"
              "&uls_trackid=56oufm8f00je&utm_campaign=id_3WExUOlHAiP"
              "&utm_content=----&utm_medium=affiliates&utm_source=an_18139140007"
              "&utm_term=fmqtiyybq719")
URL_LIVE_2 = ("https://live.shopee.com.br/universal-link/share?mmp_pid=an_18139140007"
              "&session=7187289&uls_trackid=56oufs7202kf&utm_campaign=id_1v5RBVFtXKT"
              "&utm_content=----&utm_medium=affiliates&utm_source=an_18139140007"
              "&utm_term=fmqtoujszh3m#copy_link")
LIVE = derivar_ancora_url([_canonica_live(URL_LIVE_1)])
CHAVE_LIVE = f"shopee|url|{LIVE}"

# ── Textos REAIS (t.me/s/<canal>) ─────────────────────────────────
T_CTL_FUMO_V1 = ("Controle para Xbox Sem Fio Carbon Black\n\nR$ 266\n\n"
                 "-Resgate os cupons e Adicione o produto no carrinho (sacola laranja) aqui:\n"
                 "https://s.shopee.com.br/4VdCfS2qS0\n\n-Anúncio")
T_CTL_FUMO = ("Controle para Xbox Sem Fio Carbon Black\n\nR$ 266\n\n"
              "-Resgate os cupons e Adicione o produto no carrinho (sacola laranja) aqui:\n"
              "1 https://s.shopee.com.br/4VdCfS2qS0\n2 https://s.shopee.com.br/60S0RWjN80\n\n"
              "-Anúncio")
T_CTL_PROM = ("Controle para Xbox Sem Fio Carbon Black\n\nR$ 266\n\n"
              "-Resgate os cupons e Adicione o produto no carrinho (sacola laranja) aqui:\n"
              "https://s.shopee.com.br/60S0RWjN80\n\n-Anúncio")
T_CTL_SAM = ("🔥 Controle para Xbox Sem Fio Carbon Black\n\n💵 R$ 266\n"
             "🎟 Resgate os cupons e Adicione o produto no carrinho (sacola laranja) aqui:\n"
             "https://s.shopee.com.br/40gw3s3iA2\n\nanúncio")
T_AIWA_FUMO = ("Caixa de Som Bluetooth AIWA Boombox BBS-01GR 200W\n\nR$ 909\n"
               "-Resgate todos os cupons na Live (sacola laranja) no APP aqui:\n"
               "https://s.shopee.com.br/4VdCfS2qS0\n\n"
               "-Link produto (add pela sacola da Live:\nhttps://s.shopee.com.br/4VdCfS2qS0\n\n"
               "-Anúncio")
T_PHILCO_FUMO = ("Caixa de Som Philco Extreme 2400W 2x Woofer PCX22000 - Bivolt\n\nR$ 728\n"
                 "-Resgate todos os cupons na Live (sacola laranja) no APP aqui:\n"
                 "https://s.shopee.com.br/4VdCfS2qS0\n\n"
                 "-Link produto (add pela sacola da Live:\nhttps://s.shopee.com.br/4VdCfS2qS0\n\n"
                 "-Anúncio")
T_AIWA_SAM = ("🔥 Caixa de Som Bluetooth AIWA Boombox BBS-01GR 200W\n\n💵 R$ 909\n"
              "🎟 Resgate todos os cupons na Live (sacola laranja) no APP aqui:\n"
              "https://s.shopee.com.br/113KVUziiT\n\n✅ Link produto:\n"
              "https://s.shopee.com.br/113KVUziiT\n\nanúncio")
T_PHILCO_SAM = ("🔥 Caixa de Som Philco Extreme 2400W 2x Woofer PCX22000 Bivolt\n\n💵 R$ 728\n"
                "🎟 Resgate todos os cupons na Live (sacola laranja) no APP aqui:\n"
                "https://s.shopee.com.br/7ptee3ZdYs\n\n✅ Link produto:\n"
                "https://s.shopee.com.br/7ptee3ZdYs\n\nanúncio")


# ─────────────────────────────────────────────────────────────────
# Telegram falso: a imagem carrega a TAG da mensagem de origem
# ─────────────────────────────────────────────────────────────────
class _Msg:
    def __init__(self, i, com_midia=False):
        self.id = i
        self.media = object() if com_midia else None
        self.photo = None


_ID = {"n": 70000}


def _tag(file):
    if file is None:
        return None
    try:
        return bytes(file.getvalue()[:24]).split(b"|", 1)[0].decode()
    except Exception:                                   # noqa: BLE001
        return "?"


class Cliente:
    def __init__(self):
        self.criados = []            # [(id, texto, tag_da_imagem)]
        self.edits = []              # [(id, texto, tag_da_imagem|None)]
        self.deletes = []

    async def send_message(self, dest, texto, parse_mode=None, link_preview=None):
        _ID["n"] += 1
        self.criados.append((_ID["n"], texto, None))
        return _Msg(_ID["n"])

    async def send_file(self, dest, img, caption=None, parse_mode=None,
                        force_document=False):
        _ID["n"] += 1
        self.criados.append((_ID["n"], caption, _tag(img)))
        return _Msg(_ID["n"], com_midia=True)

    async def edit_message(self, dest, msg_id, texto, parse_mode=None, file=None):
        self.edits.append((msg_id, texto, _tag(file)))
        return _Msg(msg_id, com_midia=file is not None)

    async def delete_messages(self, dest, msg_id):
        self.deletes.append(msg_id)
        return True

    async def download_media(self, media, file=None):
        if file is not None:
            file.write((getattr(media, "tag", "?") + "|").encode().ljust(4096, b"x"))
        return "arquivo"

    def ids(self):
        return [i for i, _, _ in self.criados]

    def imagens_em(self, mid):
        """Toda imagem que chegou ao post `mid` (envio ou edição)."""
        out = [t for i, _, t in self.criados if i == mid and t]
        out += [t for i, _, t in self.edits if i == mid and t]
        return out

    def textos_em(self, mid):
        out = [x for i, x, _ in self.criados if i == mid]
        out += [x for i, x, _ in self.edits if i == mid and x]
        return out


def _isolar():
    g._init_globals()
    g._encerrando = False
    for pool in (exclusao._IDENTITY_LOCKS, exclusao._IDENTITY_LOCKS_TS,
                 exclusao._POST_LOCKS, exclusao._POST_LOCKS_TS,
                 origem._LOCKS, origem._LOCKS_TS):
        pool.clear()
    origem._LOCKS_LCK = asyncio.Lock()
    convergencia._EM_CURSO.clear()


def cenario(corpo):
    async def _run():
        _isolar()
        c = Cliente()
        modulo_client.client = c
        await corpo(c)
        for _ in range(50):
            ts = [t for t in convergencia._EM_CURSO.values() if not t.done()]
            if not ts:
                break
            await asyncio.gather(*ts, return_exceptions=True)
        return c
    return asyncio.run(_run())


# ─────────────────────────────────────────────────────────────────
# Mensagens
# ─────────────────────────────────────────────────────────────────
_SEQ = {"n": 800000}


def sessao(n):
    """Outra live (session própria): isola os cenários no banco comum."""
    return (f"https://live.shopee.com.br/universal-link/share?from=live"
            f"&session={n}&utm_medium=affiliates",)


def live(chat, texto, msg_id=None, midia=True, urls=(URL_LIVE_1,), pids=()):
    """Mensagem Shopee cujos links resolvem para a live (identidade de
    fallback pela função REAL). `pids` simula um link de PRODUTO no
    mesmo evento."""
    if msg_id is None:
        _SEQ["n"] += 1
        msg_id = _SEQ["n"]
    pids = list(pids)
    tag = f"m{msg_id}"
    return MensagemNormalizada(
        msg_id=msg_id, chat=str(chat), texto_limpo=texto, texto_analise=texto,
        mapa={"http://o": "http://a"}, preservar=[], plat="shopee",
        sku=(pids[0] if pids else ""), tem_midia=midia,
        media_obj=(types.SimpleNamespace(tag=tag) if midia else None),
        ids_globais=pids, idents=[("shopee", p, "produto") for p in pids],
        cupons=[], midia_key=(f"k-{tag}" if midia else ""),
        ancora_url=derivar_ancora_url([_canonica_live(u) for u in urls]))


def ed(n, texto):
    return replace(n, texto_limpo=texto, texto_analise=texto)


async def pub(n, score=None):
    enr = enriquecer(n)
    if score is not None:
        enr = replace(enr, score=score)
    await publicacao.enviar(await montar(n), n, enr=enr, is_edit=False)
    return enr


async def edit(n, score=None):
    enr = enriquecer_edicao(n)
    if score is not None:
        enr = replace(enr, score=score)
    await publicacao.enviar(await montar(n), n, enr=enr, is_edit=True)
    return enr


def dest(n):
    return origem.consultar(n.chat, n.msg_id)


def texto_post(mid):
    return (db_get_post(mid) or {}).get("texto", "")


def lider_msg(mid):
    return (db_get_post(mid) or {}).get("lider_msg")


def P(pid):
    return f"shopee|{pid}"


# ══════════════════════════════════════════════════════════════════
# PARTE 1 — Regras puras
# ══════════════════════════════════════════════════════════════════
def test_01_urls_reais_sao_a_mesma_live_sem_produto(r):
    """As duas URLs reais do Controle: mesma live, NENHUM produto. A live
    é só fallback/container — nunca âncora forte."""
    for u in (URL_LIVE_1, URL_LIVE_2):
        r.check(extrai_identidade(u).tipo_link != TipoLink.PRODUTO,
                "01.url_real_sem_produto", u[:60])
    r.check(_canonica_live(URL_LIVE_1) == _canonica_live(URL_LIVE_2)
            == "https://live.shopee.com.br/live/7187289", "01.mesma_live")
    r.check(familia.eh_chave_container(CHAVE_LIVE), "01.chave_live_e_container")
    r.check(not eh_chave_forte(CHAVE_LIVE), "01.live_nao_e_forte")
    r.check(not familia.eh_chave_container("shopee|url|https://shopee.com.br/loja-x")
            and not familia.eh_chave_container("shopee|123.456")
            and not familia.eh_chave_container("shopee|txt|abc")
            and not familia.eh_chave_container(""), "01.so_live_e_container")


def test_02_matriz_de_titulos(r):
    """Matriz final, determinística, do discriminador de título."""
    mt = familia.mesmo_titulo
    casa = {
        "controle_xbox_x_controle_xbox": ("Controle Xbox", "Controle Xbox"),
        "controle_xbox_x_controle_para_xbox": ("Controle Xbox", "Controle para Xbox"),
        "controle_incidente_fontes_diferentes": (T_CTL_FUMO, T_CTL_SAM),
        "curto_longo_mesmo_produto": ("Caixa de Som Philco Extreme PCX22000 Bivolt",
                                      "Caixa de Som Philco Extreme PCX22000"),
        "philco_incidente_fontes_diferentes": (T_PHILCO_FUMO, T_PHILCO_SAM),
        "variacao_de_escrita": ("Controle Xbox Series Sem Fio Carbon Black", T_CTL_FUMO),
        "mesmo_titulo_outra_fonte": ("Air Fryer Mondial 4L Preta Inox",
                                     "🔥 Air Fryer Mondial 4L Preta Inox"),
        # cobertura 3/5 = 0,6 (acima do mínimo 0,5): detalhe sem distintivo
        "contido_cobertura_limite": ("Controle Xbox Carbon",
                                     "Controle Xbox Carbon Sem Fio"),
    }
    separa = {
        "controle_xbox_x_controle_xbox_preto": ("Controle Xbox", "Controle Xbox Preto"),
        "cadeira_preta_x_cadeira_cinza": ("Cadeira Preta", "Cadeira Cinza"),
        "cadeira_longa_preta_x_cinza": ("Cadeira Escritório Ergonômica Flexform Preta",
                                        "Cadeira Escritório Ergonômica Flexform Cinza"),
        "controle_x_aiwa": (T_CTL_FUMO, T_AIWA_FUMO),
        "controle_x_philco": (T_CTL_FUMO, T_PHILCO_FUMO),
        "philco_x_aiwa": (T_PHILCO_FUMO, T_AIWA_FUMO),
        "caixa_philco_x_caixa_aiwa_curto": ("Caixa Philco", "Caixa AIWA"),
        "kit_3_x_unidade": ("Kit 3 Controles Xbox Sem Fio Carbon Black",
                            "Controle Xbox Sem Fio Carbon Black"),
        "kit_x_unidade_curto": ("Kit Controle Xbox", "Controle Xbox"),
        "curto_x_longo": ("Controle Xbox", "Controle para Xbox Sem Fio Carbon Black"),
        "um_token": ("Controle", "Controle para Xbox Sem Fio Carbon Black"),
        "generico_2_tokens": ("Caixa de Som", "Caixa de Som Philco Extreme"),
        "generico_contido_em_especifico_longo": (
            "Caixa de Som Bluetooth",
            "Caixa de Som Bluetooth Portátil Resistente Água Festa"),
        "numero_a_mais": ("SSD Kingston NV2 NVMe", "SSD Kingston NV2 NVMe 1TB"),
        # cada lado com token PRÓPRIO (não distintivo): variante → separa
        "cada_lado_com_token_proprio": ("Mouse Logitech G203 Lightsync Gamer",
                                        "Mouse Logitech G203 Lightsync Office"),
        "preto_a_mais": ("Controle Xbox Sem Fio Carbon",
                         "Controle Xbox Sem Fio Carbon Preto"),
        # decisão explícita: acréscimo com NÚMERO (potência, quantidade) é
        # informação de produto — mesmo contido, separa (na dúvida, separa)
        "curto_longo_com_numero_a_mais": (T_PHILCO_FUMO,
                                          "Caixa de Som Philco Extreme PCX22000"),
    }
    for nome, (x, y) in casa.items():
        r.check(mt(x, y) and mt(y, x), f"02.casa_{nome}", f"{x[:40]} × {y[:40]}")
    for nome, (x, y) in separa.items():
        r.check(not mt(x, y) and not mt(y, x), f"02.separa_{nome}", f"{x[:40]} × {y[:40]}")
    r.check(not mt("", T_CTL_FUMO) and not mt(T_CTL_FUMO, "")
            and not mt("🔥\n\nR$ 10", T_CTL_FUMO)
            and not mt("https://s.shopee.com.br/4VdCfS2qS0", T_CTL_FUMO),
            "02.titulo_ausente_nao_casa")


def test_02b_falsos_positivos(r):
    """Título é discriminador FRACO: na dúvida, SEPARA."""
    mt = familia.mesmo_titulo
    casos_separa = {
        "genericos_semelhantes": ("Kit Ferramentas 100 Peças Maleta",
                                  "Kit Ferramentas 129 Peças Maleta"),
        "mesma_categoria_pequena_diferenca": ("Fone Bluetooth Lenovo LP40 Pro Branco",
                                              "Fone Bluetooth Lenovo LP40 Branco"),
        "mesma_marca_produtos_diferentes": ("Fone JBL Tune 520BT Preto",
                                            "Caixa de Som JBL Go 4 Preta"),
        "capacidade": ("SSD Kingston NV2 1TB NVMe", "SSD Kingston NV2 500GB NVMe"),
        "capacidade_a_mais": ("Air Fryer Mondial Inox Digital",
                              "Air Fryer Mondial Inox Digital 4L"),
        "tamanho": ("Smart TV Samsung Crystal 50 polegadas UHD",
                    "Smart TV Samsung Crystal 55 polegadas UHD"),
        "cor": ("Air Fryer Mondial 4L Preta Inox", "Air Fryer Mondial 4L Vermelha Inox"),
        "cor_a_mais": ("Mouse Logitech G203 Lightsync", "Mouse Logitech G203 Lightsync Branco"),
        "modelo": ("iPhone 15 Pro Max Apple", "iPhone 16 Pro Max Apple"),
        "sufixo_de_modelo": ("Echo Dot Alexa Smart Speaker",
                             "Echo Dot Alexa Smart Speaker Max"),
        "generico_x_especifico": ("Caixa de Som Bluetooth",
                                  "Caixa de Som Bluetooth JBL Flip 6 Azul"),
    }
    for nome, (x, y) in casos_separa.items():
        r.check(not mt(x, y) and not mt(y, x), f"02b.separa_{nome}", f"{x} × {y}")


# ══════════════════════════════════════════════════════════════════
# PARTE 2 — Replay do incidente
# ══════════════════════════════════════════════════════════════════
def checar_estado(r, c, msgs, familias, etapa, fortes_permitidos=()):
    """Invariantes após UMA etapa de um replay.
    `msgs` = {rótulo: MensagemNormalizada já processada};
    `familias` = {rótulo: marcador de texto do produto} (ex.: "Controle")."""
    vivos = {}
    for k, n in msgs.items():
        mid = dest(n)
        if mid is not None:
            vivos.setdefault(mid, set()).add(k)
    marcadores = {k: familias[k] for k in msgs}
    for mid, ks in vivos.items():
        produtos = {marcadores[k] for k in ks}
        texto = texto_post(mid)
        r.check(len(produtos) == 1, f"{etapa}.post_{mid}_um_produto", str(sorted(ks)))
        prod = next(iter(produtos))
        r.check(prod in texto, f"{etapa}.post_{mid}_texto_do_produto", texto[:60])
        donos = {msgs[k].msg_id for k in ks}
        r.check(lider_msg(mid) in donos, f"{etapa}.post_{mid}_lider_da_familia",
                f"{lider_msg(mid)} {donos}")
        imgs = c.imagens_em(mid)
        r.check(all(t is None or int(t[1:]) in donos for t in imgs),
                f"{etapa}.post_{mid}_midia_da_familia", f"{imgs} {donos}")
        exib = set(familia.fortes(_exibida(mid)))
        r.check(exib <= set(fortes_permitidos), f"{etapa}.post_{mid}_sem_forte_alheio",
                str(exib))
    for k in fortes_permitidos:
        dono = _dono(k)
        r.check(dono is None or dono in vivos, f"{etapa}.forte_{k}_sem_roubo", str(dono))
    return vivos


def _exibida(mid):
    with _db() as db:
        return {x[0] for x in db.execute(
            "SELECT identity FROM post_exibida WHERE msg_id_dest=?", (mid,)).fetchall()}


def test_03_replay_incidente_24262(r):
    """Sequência REAL (15:15 → 15:35) com TODAS as edições, checando o
    estado depois de cada etapa: posts vivos, texto, composição exibida,
    lider_msg, mídia, origens, posse — e nenhuma mídia de caixa no
    Controle."""
    FAM = {"f64": "Controle", "p110": "Controle", "s607": "Controle",
           "f67": "AIWA", "s609": "AIWA", "f68": "Philco", "s610": "Philco"}
    msgs = {}
    etapas = []

    async def corpo(c):
        async def passo(nome, rot, n, editar=False, esperados=None):
            msgs[rot] = n
            await (edit(n, score=5) if editar else pub(n, score=5))
            vivos = checar_estado(r, c, msgs, FAM, f"03.{nome}")
            etapas.append((nome, sorted(vivos), esperados))

        f64 = live(FUMOTOM, T_CTL_FUMO_V1, 35064)
        await passo("01_35064", "f64", f64, esperados=1)
        await passo("02_35064e1", "f64", ed(f64, T_CTL_FUMO_V1 + " "), True, 1)
        await passo("03_35064e2", "f64", ed(f64, T_CTL_FUMO), True, 1)
        await passo("04_35064e3", "f64", ed(f64, T_CTL_FUMO + "\n"), True, 1)
        p110 = live(PROMOTOM, T_CTL_PROM, 110352, urls=(URL_LIVE_2,))
        await passo("05_110352", "p110", p110, esperados=1)
        await passo("06_110352e", "p110", p110, True, 1)
        s607 = live(SAMUEL, T_CTL_SAM, 118607)
        await passo("07_118607", "s607", s607, esperados=1)
        await passo("08_118607e", "s607", s607, True, 1)
        f67 = live(FUMOTOM, T_AIWA_FUMO, 35067)
        await passo("09_35067", "f67", f67, esperados=2)
        await passo("10_35067e", "f67", ed(f67, T_AIWA_FUMO + "\n"), True, 2)
        f68 = live(FUMOTOM, T_PHILCO_FUMO, 35068)
        await passo("11_35068", "f68", f68, esperados=3)
        await passo("12_35068e", "f68", ed(f68, T_PHILCO_FUMO + "\n"), True, 3)
        s609 = live(SAMUEL, T_AIWA_SAM, 118609)
        await passo("13_118609", "s609", s609, esperados=3)
        await passo("14_118609e", "s609", s609, True, 3)
        await passo("15_35064e4", "f64", ed(f64, T_CTL_FUMO + "\n\n"), True, 3)
        s610 = live(SAMUEL, T_PHILCO_SAM, 118610)
        await passo("16_118610", "s610", s610, esperados=3)
        await passo("17_118610e", "s610", s610, True, 3)

    c = cenario(corpo)
    for nome, vivos, esperados in etapas:
        r.check(len(vivos) == esperados, f"03.{nome}.posts_vivos", str(vivos))
    A, B, C = dest(msgs["f64"]), dest(msgs["f67"]), dest(msgs["f68"])
    r.check(len(c.criados) == 3 and len({A, B, C}) == 3, "03.tres_posts", str(c.ids()))
    r.check(dest(msgs["p110"]) == A and dest(msgs["s607"]) == A, "03.controles_juntos")
    r.check(dest(msgs["s609"]) == B and dest(msgs["s610"]) == C, "03.caixas_samuel_no_post_certo")
    caixas = {"m35067", "m35068", "m118609", "m118610"}
    r.check(not (set(c.imagens_em(A)) & caixas), "03.midia_de_caixa_nunca_no_controle",
            str(c.imagens_em(A)))
    r.check(not any("Caixa" in (t or "") for t in c.textos_em(A)),
            "03.texto_do_controle_nunca_vira_caixa")
    r.check(lider_msg(A) == 35064 and lider_msg(B) == 35067 and lider_msg(C) == 35068,
            "03.lider_por_mensagem", f"{lider_msg(A)} {lider_msg(B)} {lider_msg(C)}")
    r.check(_exibida(A) == _exibida(B) == _exibida(C) == {CHAVE_LIVE},
            "03.composicao_exibida_so_a_live")
    r.check(not c.deletes, "03.nada_apagado", str(c.deletes))


def test_04_edicao_da_caixa_so_no_proprio_post(r):
    """A edição de 35067 só altera o post que 35067 representa."""
    s = {}

    async def corpo(c):
        f64 = live(FUMOTOM, T_CTL_FUMO, 35164, urls=sessao(904))
        await pub(f64, score=5)
        f67 = live(FUMOTOM, T_AIWA_FUMO, 35167, urls=sessao(904))
        await pub(f67, score=5)
        s["A"], s["B"] = dest(f64), dest(f67)
        s["edits_A"] = len([e for e in c.edits if e[0] == s["A"]])
        await edit(ed(f67, T_AIWA_FUMO.replace("R$ 909", "R$ 899")), score=5)
        s["edits_A_depois"] = len([e for e in c.edits if e[0] == s["A"]])
    c = cenario(corpo)
    r.check(s["A"] != s["B"], "04.posts_distintos")
    r.check(s["edits_A"] == s["edits_A_depois"], "04.A_intocado")
    r.check(any("899" in (t or "") for i, t, _ in c.edits if i == s["B"]),
            "04.B_sincronizado")


# ══════════════════════════════════════════════════════════════════
# PARTE 3 — Casos obrigatórios de C1
# ══════════════════════════════════════════════════════════════════
def test_05_mesma_live_mesmo_titulo_fontes_diferentes(r):
    s = {}

    async def corpo(c):
        a = live(CA, "Fone JBL Tune 520BT Azul\n\nR$ 199\nhttps://s.shopee.com.br/x1", urls=sessao(905))
        await pub(a, score=5)
        b = live(CB, "🔥 Fone JBL Tune 520BT Azul\n\n💵 R$ 199\nhttps://s.shopee.com.br/x2", urls=sessao(905))
        await pub(b, score=5)
        s["a"], s["b"] = dest(a), dest(b)
    c = cenario(corpo)
    r.check(len(c.criados) == 1 and s["a"] == s["b"], "05.um_post", str(c.ids()))


def test_06_mesma_live_titulos_diferentes(r):
    s = {}

    async def corpo(c):
        a = live(CA, "Fone JBL Tune 520BT Verde\n\nR$ 199\nhttps://s.shopee.com.br/x1", urls=sessao(906))
        await pub(a, score=5)
        b = live(CA, "Mouse Logitech G203 Lightsync Verde\n\nR$ 99\nhttps://s.shopee.com.br/x1", urls=sessao(906))
        await pub(b, score=50)
        s["a"], s["b"] = dest(a), dest(b)
    c = cenario(corpo)
    r.check(len(c.criados) == 2 and s["a"] != s["b"], "06.dois_posts", str(c.ids()))
    r.check("JBL" in texto_post(s["a"]) and "Logitech" in texto_post(s["b"]),
            "06.score_maior_nao_invade")


def test_07_mesma_live_titulo_ausente(r):
    """Sem título a live não prova nada: não contamina o post existente."""
    s = {}

    async def corpo(c):
        a = live(CA, "Teclado Redragon Kumara K552 Rosa\n\nR$ 159\nhttps://s.shopee.com.br/x1", urls=sessao(907))
        await pub(a, score=5)
        b = live(CB, "https://s.shopee.com.br/4VdCfS2qS0", midia=True, urls=sessao(907))
        await pub(b, score=50)
        s["a"], s["b"] = dest(a), dest(b)
    c = cenario(corpo)
    r.check(s["a"] != s["b"] and not any(i == s["a"] for i, _, _ in c.edits),
            "07.nao_contamina", f"{s} edits={[e[0] for e in c.edits]}")
    r.check("Redragon" in texto_post(s["a"]), "07.A_intacto")


def test_08_produto_forte_mais_live_produto_vence(r):
    """Produto no link + live no mesmo evento: a identidade é o PRODUTO e
    a família forte decide como sempre (a guarda não se aplica)."""
    s = {}

    async def corpo(c):
        a = live(CA, "Monitor AOC 24G2 144Hz\n\nR$ 899\nhttps://s.shopee.com.br/y1",
                 pids=["123.8008"])
        e = await pub(a, score=5)
        s["ofertas"] = e.ofertas
        b = live(CB, "Monitor gamer 24 polegadas oferta\n\nR$ 889\nhttps://s.shopee.com.br/y2",
                 pids=["123.8008"])
        await pub(b, score=5)
        s["a"], s["b"] = dest(a), dest(b)
    c = cenario(corpo)
    r.check(s["ofertas"] == [P("123.8008")], "08.produto_e_a_identidade", str(s["ofertas"]))
    r.check(len(c.criados) == 1 and s["a"] == s["b"], "08.produto_casa_mesmo_com_titulo_diferente")


def test_09_midia_diferente_mesmo_produto_e_produto_diferente(r):
    """Mídia de outra fonte do MESMO produto pode subir (política de mídia);
    de produto DIFERENTE da mesma live, nunca."""
    s = {}

    async def corpo(c):
        a = live(CA, "Smartwatch Amazfit Bip 5 Preto\n\nR$ 399\nhttps://s.shopee.com.br/z1",
                 midia=False, msg_id=880001)
        await pub(a, score=5)
        s["A"] = dest(a)
        outro = live(CB, "Liquidificador Philips Walita 1200W\n\nR$ 199\nhttps://s.shopee.com.br/z2",
                     msg_id=880002)
        await pub(outro, score=50)
        mesmo = live(CB, "🔥 Smartwatch Amazfit Bip 5 Preto\n\n💵 R$ 399\nhttps://s.shopee.com.br/z3",
                     msg_id=880003)
        await pub(mesmo, score=5)
        s["outro"], s["mesmo"] = dest(outro), dest(mesmo)
    c = cenario(corpo)
    A = s["A"]
    r.check(s["outro"] != A and s["mesmo"] == A, "09.familias", str(s))
    r.check("m880002" not in c.imagens_em(A), "09.midia_de_outro_produto_nunca",
            str(c.imagens_em(A)))
    r.check("m880003" in c.imagens_em(A), "09.midia_do_mesmo_produto_sobe",
            str(c.imagens_em(A)))


def test_10_primeira_publicacao_sem_consulta_extra(r):
    """Mensagem de live sem família viva: a guarda não roda (nenhuma
    leitura de exibição/texto de post) — mesmo número de SQL que uma
    oferta nova qualquer."""
    sqls = {"live": [], "comum": []}
    alvo = {"k": None}

    def cb(q):
        if alvo["k"]:
            sqls[alvo["k"]].append(q)

    async def corpo(c):
        with _db() as db:
            db.set_trace_callback(cb)
        try:
            alvo["k"] = "live"
            await pub(live(CA, "Cadeira Gamer ThunderX3 TGC12 Azul\n\nR$ 999\nhttps://s.shopee.com.br/q1",
                           urls=("https://live.shopee.com.br/universal-link/share?session=990001",)))
            alvo["k"] = "comum"
            await pub(live(CA, "Mesa Digitalizadora Wacom One Azul\n\nR$ 299\nhttps://s.shopee.com.br/q2",
                           urls=("https://shopee.com.br/loja-unica-990002",)))
            alvo["k"] = None
        finally:
            with _db() as db:
                db.set_trace_callback(None)
    c = cenario(corpo)
    r.check(len(c.criados) == 2, "10.duas_publicacoes")
    r.check(len(sqls["live"]) == len(sqls["comum"]), "10.mesmo_custo_sql",
            f"live={len(sqls['live'])} comum={len(sqls['comum'])}")
    r.check(not any("SELECT identity FROM post_exibida WHERE msg_id_dest" in q
                    for q in sqls["live"]), "10.guarda_nao_consulta")


# ══════════════════════════════════════════════════════════════════
# PARTE 4 — C2: líder é a MENSAGEM
# ══════════════════════════════════════════════════════════════════
T_CAD = "Cadeira Escritório Ergonômica Flexform Preta\n\nR$ {p}\nhttps://s.shopee.com.br/c1"


def test_11_varias_mensagens_mesmo_canal(r):
    """Mesma fonte, duas mensagens do MESMO produto: a segunda casa o post
    e fica vinculada; editá-la NÃO é SYNC (não é o texto no ar). Só a
    mensagem-líder sincroniza."""
    s = {}

    async def corpo(c):
        lider = live(CA, T_CAD.format(p="700", urls=sessao(911)), msg_id=881001)
        await pub(lider, score=5)
        s["A"] = dest(lider)
        repost = live(CA, T_CAD.format(p="700", urls=sessao(911)) + "\nrepost", msg_id=881002)
        await pub(repost, score=5)
        s["vinc"] = dest(repost)
        await edit(ed(repost, T_CAD.format(p="650") + "\nrepost editado"), score=5)
        s["texto_pos_nao_lider"] = texto_post(s["A"])
        await edit(ed(lider, T_CAD.format(p="690")), score=5)
        s["texto_pos_lider"] = texto_post(s["A"])
    c = cenario(corpo)
    r.check(s["vinc"] == s["A"], "11.repost_vinculado")
    r.check("650" not in s["texto_pos_nao_lider"] and "700" in s["texto_pos_nao_lider"],
            "11.nao_lider_nao_sincroniza", s["texto_pos_nao_lider"][:80])
    r.check("690" in s["texto_pos_lider"], "11.lider_sincroniza")
    r.check(lider_msg(s["A"]) == 881001, "11.lider_msg_intacto")


def test_12_evolucao_textual_troca_o_lider(r):
    """Outra fonte EVOLUI o texto (score maior): lider_msg passa a ser ela.
    Daí em diante a mensagem antiga não sincroniza; a nova sim."""
    s = {}

    async def corpo(c):
        velha = live(CA, T_CAD.format(p="800", urls=sessao(912)), msg_id=882001)
        await pub(velha, score=5)
        s["A"] = dest(velha)
        nova = live(CB, "🔥 " + T_CAD.format(p="780", urls=sessao(912)) + "\nfrete grátis", msg_id=882002)
        await pub(nova, score=40)
        s["lider_apos_evolucao"] = lider_msg(s["A"])
        await edit(ed(velha, T_CAD.format(p="100")), score=5)
        s["t1"] = texto_post(s["A"])
        await edit(ed(nova, "🔥 " + T_CAD.format(p="770") + "\nfrete grátis"), score=40)
        s["t2"] = texto_post(s["A"])
    cenario(corpo)
    r.check(s["lider_apos_evolucao"] == 882002, "12.lider_passa_a_fonte_nova",
            str(s["lider_apos_evolucao"]))
    r.check("100" not in s["t1"], "12.antigo_nao_sincroniza", s["t1"][:80])
    r.check("770" in s["t2"], "12.novo_lider_sincroniza", s["t2"][:80])


def test_13_evolucao_so_de_midia_nao_troca_o_lider(r):
    s = {}

    async def corpo(c):
        a = live(CA, T_CAD.format(p="900", urls=sessao(913)).replace("Preta", "Cinza"), msg_id=883001, midia=False)
        await pub(a, score=5)
        s["A"] = dest(a)
        m = live(CB, "🔥 " + T_CAD.format(p="900", urls=sessao(913)).replace("Preta", "Cinza"), msg_id=883002)
        await pub(m, score=5)                     # mesmo texto/score → só mídia
        s["midia_chat"] = (db_get_post(s["A"]) or {}).get("midia_chat")
    c = cenario(corpo)
    r.check("m883002" in c.imagens_em(s["A"]) and s["midia_chat"] == CB,
            "13.midia_subiu", str(c.imagens_em(s["A"])))
    r.check(lider_msg(s["A"]) == 883001, "13.lider_inalterado", str(lider_msg(s["A"])))


def test_14_legado_sem_lider_msg_sincroniza_por_canal(r):
    s = {}

    async def corpo(c):
        a = live(CA, T_CAD.format(p="500", urls=sessao(914)).replace("Preta", "Branca"), msg_id=884001)
        await pub(a, score=5)
        s["A"] = dest(a)
        with _db() as db:
            db.execute("UPDATE post_estado SET lider_msg=NULL WHERE msg_id_dest=?", (s["A"],))
        outra = live(CA, T_CAD.format(p="500", urls=sessao(914)).replace("Preta", "Branca") + "\nx",
                     msg_id=884002)
        await pub(outra, score=5)
        await edit(ed(outra, T_CAD.format(p="450").replace("Preta", "Branca")), score=5)
        s["t"] = texto_post(s["A"])
    cenario(corpo)
    r.check("450" in s["t"], "14.legado_mantem_regra_por_canal", s["t"][:80])


def test_15_fusao_preserva_lider_do_sobrevivente(r):
    agora = time.time()
    db_registrar_post(78001, [P("5.1"), P("5.2")], 5, "sv", "shopee", CA, agora + 900, 0,
                      exibidas=[P("5.1"), P("5.2")], lider_msg=9101)
    db_registrar_post(78002, [P("5.1")], 5, "pd", "shopee", CB, agora + 900, 0,
                      exibidas=[P("5.1")], lider_msg=9102)
    r.check(db_fundir_posts(78001, [78002], agora) == [78002], "15.fundiu")
    r.check(lider_msg(78001) == 9101, "15.lider_do_sobrevivente_preservado")
    # gravação sem lider_msg (sincronização / mídia) preserva
    db_registrar_post(78001, [P("5.1"), P("5.2")], 6, "sv2", "shopee", CA, agora + 900, 0)
    r.check(lider_msg(78001) == 9101, "15.none_preserva")


def test_16_fusao_trocando_texto(r):
    """A=P1 (fonte CA), B=P2; outra fonte publica P1+P2 → AMPLIA evolui um
    deles com o TEXTO dela e o outro é fundido: o lider_msg do
    sobrevivente passa a ser a fonte vencedora; as mensagens antigas não
    sincronizam mais."""
    s = {}

    async def corpo(c):
        a = live(CA, "Kit A item um\n\nR$ 10", pids=["6.1"], msg_id=886001)
        await pub(a, score=5)
        b = live(CB, "Kit B item dois\n\nR$ 20", pids=["6.2"], msg_id=886002)
        await pub(b, score=5)
        s["A"], s["B"] = dest(a), dest(b)
        k = live(PROMOTOM, "Kit completo item um e dois\n\nR$ 25", pids=["6.1", "6.2"],
                 msg_id=886003)
        await pub(k, score=3)
        s["sv"] = dest(k)
        s["lider"] = lider_msg(s["sv"])
        await edit(ed(a, "Kit A item um\n\nR$ 1"), score=5)
        await edit(ed(b, "Kit B item dois\n\nR$ 2"), score=5)
        s["t"] = texto_post(s["sv"])
    cenario(corpo)
    sv = s["sv"]
    pd = s["B"] if sv == s["A"] else s["A"]
    r.check(sv in (s["A"], s["B"]) and db_get_post(pd)["fused_into"] == sv,
            "16.um_fundido_no_outro", f"sv={sv} pd={pd}")
    r.check(s["lider"] == 886003, "16.lider_e_a_fonte_vencedora", str(s["lider"]))
    r.check("completo" in s["t"], "16.fontes_antigas_nao_sincronizam", s["t"][:80])


def test_17_fonte_lider_corrige_o_titulo(r):
    """A fonte-líder corrige o título da PRÓPRIA mensagem (mandou errado e
    consertou): é SYNC no próprio post — a guarda de container não vale
    para a edição vinculada à origem, e nenhum post novo nasce."""
    s = {}

    async def corpo(c):
        n = live(SAMUEL, "🔥 Caixa de Som JBL Go 4 Azul\n\n💵 R$ 279", msg_id=887001,
                 urls=sessao(917))
        await pub(n, score=5)
        s["A"] = dest(n)
        await edit(ed(n, "🔥 Fone JBL Tune 520BT Azul\n\n💵 R$ 279"), score=5)
        s["t"] = texto_post(s["A"])
    c = cenario(corpo)
    r.check(len(c.criados) == 1, "17.sem_post_novo", str(c.ids()))
    r.check("Tune" in s["t"], "17.correcao_sincronizada", s["t"][:80])


def test_18_forte_em_comum_a_live_nao_interfere(r):
    """Regra da família direto: se o candidato compartilha com o post uma
    âncora FORTE além da live, o título não é exigido (a regra de sempre
    decide). Só quando TUDO o que se compartilha é container o título
    vira discriminador."""
    agora = time.time()
    chave_live = f"shopee|url|{derivar_ancora_url([_canonica_live(sessao(918)[0])])}"
    cup = "shopee|cup|LIVE918X"
    db_registrar_post(78101, [chave_live, cup], 5, "🔥 Caixa de Som JBL Go 4\n\nR$ 279",
                      "shopee", CA, agora + 900, 0, exibidas=[chave_live, cup])
    outro_titulo = "🔥 Fone Bluetooth Philips TAT1209\n\nR$ 99"
    r.check(familia.post_da_familia([chave_live, cup], titulo=outro_titulo) == 78101,
            "18.forte_em_comum_casa")
    r.check(familia.post_da_familia([chave_live], titulo=outro_titulo) is None,
            "18.so_container_titulo_diferente_nao_casa")
    r.check(familia.post_da_familia([chave_live], titulo="Caixa de Som JBL Go 4") == 78101,
            "18.so_container_mesmo_titulo_casa")


def test_19_titulo_nao_sobrescreve_forte_e_produto_prevalece(r):
    """Título forte + produto forte CONFLITANTE: o produto prevalece.
    · A (live, Controle) adota o forte P1 de outra mensagem da live;
    · uma mensagem com OUTRO forte P2 e o MESMO título não entra em A;
    · com A (P1) e outro post (P2) de mesmo título, uma mensagem só-live
      com esse título é AMBÍGUA: não casa nenhum (na dúvida, separa).
    (O título que casa um post forte sem trocar o texto está no replay
    sequencial, etapa 11.)"""
    s = {}
    ses = sessao(919)

    async def corpo(c):
        a = live(CA, "Controle Xbox Sem Fio Robot White\n\nR$ 280", msg_id=889001, urls=ses)
        await pub(a, score=5)
        s["A"] = dest(a)
        f1 = live(CB, "Controle Xbox Sem Fio Robot White\n\nR$ 279", msg_id=889002,
                  urls=ses, pids=["919.1"])
        await pub(f1, score=30)
        s["adotou"] = dest(f1)
        s["exib_A"] = set(db_composicoes_exibida(s["A"]))
        f2 = live(PROMOTOM, "Controle Xbox Sem Fio Robot White\n\nR$ 270", msg_id=889003,
                  urls=ses, pids=["919.2"])
        await pub(f2, score=50)
        s["f2"] = dest(f2)
        so_live = live(SAMUEL, "🔥 Controle Xbox Sem Fio Robot White\n\n💵 R$ 1", msg_id=889004,
                       urls=ses)
        await pub(so_live, score=99)
        s["so_live"] = dest(so_live)
        s["texto_A"] = texto_post(s["A"])
        s["exib_A_fim"] = set(db_composicoes_exibida(s["A"]))
    cenario(corpo)
    r.check(s["adotou"] == s["A"], "19.forte_adota_o_post_da_live", str(s))
    r.check(fortes_de(s["exib_A"]) == {P("919.1")}, "19.forte_vira_referencia", str(s["exib_A"]))
    r.check(s["f2"] != s["A"], "19.forte_conflitante_nao_entra_pelo_titulo")
    r.check(s["so_live"] not in (s["A"], s["f2"]), "19.titulo_ambiguo_entre_fortes_nao_casa",
            f"so_live={s['so_live']} A={s['A']} f2={s['f2']}")
    r.check("R$ 1\n" not in s["texto_A"] + "\n"
            and fortes_de(s["exib_A_fim"]) == {P("919.1")}, "19.forte_de_A_intacto",
            f"{s['texto_A'][:60]} {s['exib_A_fim']}")


def fortes_de(chaves):
    return set(familia.fortes(chaves))


def db_composicoes_exibida(mid):
    with _db() as db:
        return {x[0] for x in db.execute(
            "SELECT identity FROM post_exibida WHERE msg_id_dest=?", (mid,)).fetchall()}


def test_20_replay_sequencial_com_estado_por_etapa(r):
    """Replay SEQUENCIAL numa live só. Depois de CADA etapa: posts vivos,
    texto, composição exibida, lider_msg, mídia publicada, posse das
    fortes e origens coerentes com a família."""
    ses = sessao(920)
    k_live = f"shopee|url|{derivar_ancora_url([_canonica_live(ses[0])])}"
    P_CTL = P("920.1")
    etapas = []
    msgs = {}

    def estado(c, rotulo, esperados):
        posts = sorted({dest(n) for n in msgs.values() if dest(n)})
        foto = {}
        for mid in posts:
            imgs = c.imagens_em(mid)
            foto[mid] = imgs[-1] if imgs else None
        etapas.append(dict(rotulo=rotulo, posts=posts,
                           textos={m: texto_post(m) for m in posts},
                           exib={m: db_composicoes_exibida(m) for m in posts},
                           lider={m: lider_msg(m) for m in posts}, foto=foto,
                           origem={k: dest(n) for k, n in msgs.items()},
                           dono_ctl=_dono(P_CTL), esperados=esperados))

    def titulo_de(k):
        return {"ctl": "Controle", "philco": "Philco", "aiwa": "AIWA",
                "hs": "HyperX"}[k.split("_")[0]]

    async def corpo(c):
        async def passo(chave, n, score, rotulo, esperados, editar=False):
            msgs[chave] = n
            await (edit(n, score=score) if editar else pub(n, score=score))
            estado(c, rotulo, esperados)

        await passo("ctl_f", live(FUMOTOM, T_CTL_FUMO, 920001, urls=ses), 5,
                    "1 live+controle", 1)
        await passo("ctl_s", live(SAMUEL, T_CTL_SAM, 920002, urls=ses), 5,
                    "2 outra fonte live+controle", 1)
        await passo("philco_f", live(FUMOTOM, T_PHILCO_FUMO, 920003, urls=ses), 5,
                    "3 live+philco", 2)
        await passo("aiwa_f", live(FUMOTOM, T_AIWA_FUMO, 920004, urls=ses), 5,
                    "4 live+aiwa", 3)
        await passo("ctl_f", ed(msgs["ctl_f"], T_CTL_FUMO.replace("R$ 266", "R$ 259")), 5,
                    "5 edicao controle", 3, editar=True)
        await passo("philco_f", ed(msgs["philco_f"], T_PHILCO_FUMO.replace("R$ 728", "R$ 699")),
                    5, "6 edicao philco", 3, editar=True)
        await passo("ctl_p", live(PROMOTOM, T_CTL_PROM, 920005, urls=ses), 5,
                    "7 upgrade midia controle", 3)
        await passo("philco_s", live(SAMUEL, T_PHILCO_SAM, 920006, urls=ses), 5,
                    "8 upgrade midia philco", 3)
        await passo("ctl_forte", live(CA, "🔥 Controle para Xbox Sem Fio Carbon Black\n\nR$ 249",
                                      920007, urls=ses, pids=["920.1"]), 40,
                    "9 produto forte + live controle", 3)
        await passo("hs_live", live(CB, "Headset HyperX Cloud Stinger 2 Preto\n\nR$ 199",
                                    920008, urls=ses), 60,
                    "10 live + outro titulo depois do forte", 4)
        await passo("ctl_live_tardio", live(CB, "🔥 Controle para Xbox Sem Fio Carbon Black\n\nR$ 1",
                                            920009, urls=ses), 99,
                    "11 live + titulo do controle apos o forte", 4)
        msgs["sem_titulo"] = live(CD, "https://s.shopee.com.br/4VdCfS2qS0", 920010, urls=ses)
        await pub(msgs["sem_titulo"], score=99)
        estado(c, "12 live + titulo ausente", 5)

    c = cenario(corpo)
    for e in etapas:
        tag = e["rotulo"].split(" ")[0]
        r.check(len(e["posts"]) == e["esperados"], f"20.{tag}.posts_vivos",
                f"{e['rotulo']}: {e['posts']}")
        # cada origem aponta para um post do MESMO produto
        for k, mid in e["origem"].items():
            if k == "sem_titulo" or mid is None:
                continue
            r.check(titulo_de(k) in e["textos"][mid], f"20.{tag}.origem_{k}_na_familia_certa",
                    f"{k}→post:{mid} {e['textos'][mid][:50]!r}")
        # mídia de um post nunca de outro produto
        for mid, t in e["foto"].items():
            donos = {msgs[k].msg_id for k, n in msgs.items() if dest(n) == mid}
            r.check(t is None or int(t[1:]) in donos, f"20.{tag}.midia_do_proprio_post_{mid}",
                    f"{t} donos={donos}")
        # lider_msg é uma mensagem da própria família
        for mid, lm in e["lider"].items():
            donos = {n.msg_id for k, n in msgs.items() if dest(n) == mid}
            r.check(lm in donos, f"20.{tag}.lider_da_familia_{mid}", f"{lm} {donos}")
        # exibida: só live ou o forte do controle; nenhum forte alheio
        for mid, ex in e["exib"].items():
            r.check(ex <= {k_live, P_CTL}, f"20.{tag}.exibida_{mid}", str(ex))
    fim = etapas[-1]
    ctl = fim["origem"]["ctl_f"]
    r.check(fim["exib"][ctl] == {P_CTL, k_live} and fim["dono_ctl"] == ctl,
            "20.fim.forte_e_a_referencia_do_controle", f"{fim['exib'][ctl]} dono={fim['dono_ctl']}")
    r.check(fim["lider"][ctl] == 920007, "20.fim.lider_do_controle_e_o_forte")
    r.check("R$ 1" not in fim["textos"][ctl] and "249" in fim["textos"][ctl],
            "20.fim.titulo_nao_sobrescreveu_forte", fim["textos"][ctl][:80])
    r.check(fim["origem"]["sem_titulo"] not in (fim["origem"][k] for k in msgs
                                                if k != "sem_titulo"),
            "20.fim.titulo_ausente_nao_casou")
    ph = fim["origem"]["philco_f"]
    r.check("699" in fim["textos"][ph] and fim["foto"][ph] in ("m920003", "m920006"),
            "20.fim.philco_editado_midia_de_philco", f"{fim['foto'][ph]}")
    r.check(etapas[4]["textos"][ctl] != etapas[3]["textos"][ctl]
            and etapas[4]["textos"][ph] == etapas[3]["textos"][ph],
            "20.5.edicao_do_controle_so_no_controle")


def _dono(chave):
    with _db() as db:
        x = db.execute("SELECT msg_id_dest FROM oferta_index WHERE identity=?",
                       (chave,)).fetchone()
    return x[0] if x else None


def test_21_adocao_pelo_forte_e_ambiguidade(r):
    """LIVE+Controle → post só-live; LIVE+forte do Controle → o MESMO post é
    adotado e fortificado; LIVE+forte da Philco → não adota o Controle.
    Depois: dois posts só-live com títulos semelhantes + um forte → o
    forte não escolhe nenhum no escuro."""
    ses = sessao(921)
    s = {}

    async def corpo(c):
        ctl = live(CA, "Controle para Xbox Sem Fio Carbon Black\n\nR$ 266", 921001, urls=ses)
        await pub(ctl, score=5)
        s["A"] = dest(ctl)
        ctl_f = live(CB, "🔥 Controle para Xbox Sem Fio Carbon Black\n\nR$ 259", 921002,
                     urls=ses, pids=["921.1"])
        await pub(ctl_f, score=40)
        s["ctl_f"] = dest(ctl_f)
        s["exib_A"] = _exibida(s["A"])
        ph_f = live(CB, "Caixa de Som Philco Extreme 2400W PCX22000\n\nR$ 728", 921003,
                    urls=ses, pids=["921.2"])
        await pub(ph_f, score=40)
        s["ph_f"] = dest(ph_f)
    c = cenario(corpo)
    r.check(s["ctl_f"] == s["A"] and len(c.criados) == 2, "21.forte_adota_o_mesmo_post",
            f"{s} criados={c.ids()}")
    r.check(set(familia.fortes(s["exib_A"])) == {P("921.1")}, "21.post_fortificado",
            str(s["exib_A"]))
    r.check(s["ph_f"] != s["A"] and "Controle" in texto_post(s["A"]),
            "21.forte_de_outro_produto_nao_adota")

    # Dois só-live semelhantes (estado montado direto: a regra de título já
    # impediria o segundo de nascer separado pelo fluxo normal).
    agora = time.time()
    k2 = f"shopee|url|{derivar_ancora_url([_canonica_live(sessao(9212)[0])])}"
    t = "Mouse Gamer Logitech G203 Lightsync"
    db_registrar_post(79001, [k2], 5, t, "shopee", CA, agora + 900, 0, exibidas=[k2])
    db_registrar_post(79002, [k2], 5, "🔥 " + t, "shopee", CB, agora + 900, 0, exibidas=[k2])
    escolhido = familia.post_da_familia([P("921.9")], titulo=t + "\n\nR$ 99", container=k2)
    r.check(escolhido is None, "21.adocao_ambigua_nao_escolhe", str(escolhido))
    um_so = familia.post_da_familia([P("921.8")], titulo="Teclado Redragon Kumara",
                                    container=k2)
    r.check(um_so is None, "21.titulo_diferente_nao_adota")


def test_22_alternativa_de_cor_do_mesmo_anuncio(r):
    """ "ou <cor>" / "/" / lista / "nas cores X e Y": cores ALTERNATIVAS do
    MESMO anúncio não separam. Reconhecida só como cláusula final em que
    toda opção é cor (com qualificador de nome de cor); "ou" sozinho não
    autoriza nada e qualquer atributo distintivo segue separando."""
    mt = familia.mesmo_titulo
    casa = {
        "carbon_black_ou_pulse_red": ("Controle Xbox Carbon Black",
                                      "Controle Xbox Carbon Black ou Pulse Red"),
        "incidente_samuel_118662": ("Controle para Xbox Sem Fio Carbon Black",
                                    "🔥 Controle para Xbox Sem Fio Carbon Black ou Pulse Red"),
        "carbon_ou_pulse_red": ("Controle Xbox Carbon", "Controle Xbox Carbon ou Pulse Red"),
        "cadeira_preta_ou_cinza": ("Cadeira Preta", "Cadeira Preta ou Cinza"),
        "opcao_listada_em_segundo": ("Cadeira Preta", "Cadeira Cinza ou Preta"),
        "barra": ("Cadeira Gamer Preta", "Cadeira Gamer Preta/Branca"),
        "lista_com_virgula": ("Cadeira Gamer Branca", "Cadeira Gamer Preta, Cinza ou Branca"),
        "nas_cores_x_e_y": ("Cadeira Gamer Preta", "Cadeira Gamer nas cores Preta e Cinza"),
        "dois_lados_com_opcoes": ("Controle Xbox Carbon Black ou Pulse Red",
                                  "Controle Xbox Pulse Red ou Carbon Black"),
    }
    separa = {
        "preto_x_branco_sem_alternativa": ("Controle Xbox Preto", "Controle Xbox Branco"),
        "iphone_15_ou_16": ("iPhone 15", "iPhone 15 ou iPhone 16"),
        "pro_ou_carbon_black": ("Controle Xbox", "Controle Xbox Pro ou Carbon Black"),
        "opcao_com_outro_produto": ("Cadeira Preta", "Cadeira Preta ou Mesa Branca"),
        "cor_fora_das_opcoes": ("Controle Xbox Preto", "Controle Xbox Branco ou Azul"),
        "cor_fora_da_lista": ("Cadeira Gamer Verde", "Cadeira Gamer Preta, Cinza ou Branca"),
        "outra_cor_nomeada": ("Controle Xbox Carbon Black",
                              "Controle Xbox Robot White ou Pulse Red"),
        "ou_outro_produto": ("Controle Xbox", "Controle Xbox ou Controle PS5"),
        "ou_numero": ("Controle Xbox Series", "Controle Xbox Series ou 2"),
        "kit_com_cores": ("Controle Xbox Preto", "Kit 2 Controles Xbox Preto ou Branco"),
        "ou_solto_sem_opcao": ("Controle Xbox Preto", "Controle Xbox ou"),
        "so_qualificador_sem_cor": ("Mouse Gamer Robot", "Mouse Gamer Robot ou Pulse"),
        "capacidade_com_cores": ("SSD Kingston NV2 1TB Preto",
                                 "SSD Kingston NV2 2TB Preto ou Azul"),
    }
    for nome, (x, y) in casa.items():
        r.check(mt(x, y) and mt(y, x), f"22.casa_{nome}", f"{x} × {y}")
    for nome, (x, y) in separa.items():
        r.check(not mt(x, y) and not mt(y, x), f"22.separa_{nome}", f"{x} × {y}")


T_7197894 = {
    "p362": ("Controle para Xbox Sem Fio Carbon Black\n\nR$ 266\n\n-Resgate os cupons e "
             "Adicione o produto no carrinho (sacola laranja) aqui:\n"
             "https://s.shopee.com.br/60S0RWjN80\n\n-Anúncio"),
    "s662": ("🔥 Controle para Xbox Sem Fio Carbon Black ou Pulse Red\n\n💵 R$ 266\n🎟 Resgate "
             "os cupons e Adicione o produto no carrinho (sacola laranja) aqui:\n"
             "https://s.shopee.com.br/6q17hzvopY\n\nanúncio"),
    "f133": ("Caixa de Som Philco Extreme 2400W 2x Woofer PCX22000 - Bivolt\n\nR$ 710\n"
             "-Resgate os cupons e Adicione o produto no carrinho (sacola laranja) aqui:\n"
             "https://s.shopee.com.br/AAHZg5AsS4\n\n-Anúncio"),
    "p363": ("Impressora Multifuncional Jato de Tinta Ecotank Epson 3250 Bivolt\n\nR$ 750\n"
             "-Resgate os cupons e Adicione o produto no carrinho (sacola laranja) aqui:\n"
             "https://s.shopee.com.br/3g45wJV2aA\n\n-Anúncio"),
    "s663": ("🔥 Impressora Multifuncional Jato de Tinta Ecotank Epson 3250 Bivolt\n\n💵 R$ 750\n"
             "🎟 Resgate todos os cupons na Live (sacola laranja) no APP aqui:\n"
             "https://s.shopee.com.br/9zy9U5hVCb\n\nanúncio"),
    "f134": ("Caixa de Som Aiwa Speaker 15w Ipx4 - Aws-sp-03-b Preto\n\nR$ 129\n"
             "-Resgate os cupons e Adicione o produto no carrinho (sacola laranja) aqui:\n"
             "https://s.shopee.com.br/3g45wJV2aA\n\n-Anúncio"),
}


def test_23_replay_live_7197894_post_24329(r):
    """Incidente 30/09 19:22–19:27 (post 24329): sequência REAL com as
    edições. Controle (Promotom + Samuel "ou Pulse Red") numa família só;
    Philco, Impressora e AIWA cada um no seu post; a edição de 110363
    (Impressora, mesmo canal do líder) nunca vira SYNC do Controle; foto
    de Impressora nunca no Controle. Estado checado a cada etapa."""
    SES = ("https://live.shopee.com.br/universal-link/share?from=live&session=7197894",)
    FAM = {"p362": "Controle", "s662": "Controle", "f133": "Philco",
           "p363": "Impressora", "s663": "Impressora", "f134": "Aiwa"}
    msgs, etapas = {}, []

    async def corpo(c):
        async def passo(nome, k, n, editar=False, esperados=None):
            msgs[k] = n
            await (edit(n, score=5) if editar else pub(n, score=5))
            vivos = checar_estado(r, c, msgs, FAM, f"23.{nome}")
            etapas.append((nome, sorted(vivos), esperados))

        T = T_7197894
        p362 = live(PROMOTOM, T["p362"], 110362, urls=SES)
        await passo("01_110362", "p362", p362, esperados=1)
        await passo("02_110362e", "p362", ed(p362, T["p362"] + " "), True, 1)
        s662 = live(SAMUEL, T["s662"], 118662, urls=SES)
        await passo("03_118662", "s662", s662, esperados=1)
        await passo("04_118662e", "s662", s662, True, 1)
        f133 = live(FUMOTOM, T["f133"], 35133, urls=SES)
        await passo("05_35133", "f133", f133, esperados=2)
        await passo("06_35133e", "f133", f133, True, 2)
        await passo("07_110362e2", "p362", ed(p362, T["p362"] + "  "), True, 2)
        p363 = live(PROMOTOM, T["p363"], 110363, urls=SES)
        await passo("08_110363", "p363", p363, esperados=3)
        await passo("09_110363e", "p363", ed(p363, T["p363"] + " "), True, 3)
        s663 = live(SAMUEL, T["s663"], 118663, urls=SES)
        await passo("10_118663", "s663", s663, esperados=3)
        await passo("11_118663e", "s663", s663, True, 3)
        await passo("12_110363e2", "p363", ed(p363, T["p363"] + "  "), True, 3)
        f134 = live(FUMOTOM, T["f134"], 35134, urls=SES)
        await passo("13_35134", "f134", f134, esperados=4)
        await passo("14_35134e", "f134", f134, True, 4)
        await passo("15_110363e3", "p363", ed(p363, T["p363"] + "   "), True, 4)

    c = cenario(corpo)
    for nome, vivos, esperados in etapas:
        r.check(len(vivos) == esperados, f"23.{nome}.posts_vivos", str(vivos))
    ctl = dest(msgs["p362"])
    r.check(dest(msgs["s662"]) == ctl, "23.samuel_ou_pulse_red_converge_no_controle")
    r.check(len(c.criados) == 4, "23.quatro_posts_sem_duplicacao", str(c.ids()))
    r.check(lider_msg(ctl) == 110362 and "Controle" in texto_post(ctl),
            "23.controle_intacto_lider_110362", f"{lider_msg(ctl)} {texto_post(ctl)[:50]}")
    impressoras = {"m110363", "m118663"}
    r.check(not (set(c.imagens_em(ctl)) & impressoras), "23.foto_de_impressora_nunca_no_controle",
            str(c.imagens_em(ctl)))
    imp = dest(msgs["p363"])
    r.check(dest(msgs["s663"]) == imp and lider_msg(imp) == 110363,
            "23.impressora_familia_propria")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "LIVE CONTAINER · incidente 24262 · árvore real"))
