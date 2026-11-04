"""
CONTEXTO — tem_contexto(): plurais, "roleta" e post com MÍDIA VISUAL.

Casos reais (prévias públicas, 26/10 → 03/11/2026; 8.024 mensagens com link
de loja em 6 fontes): o filtro barrava 7. Os 6 com FOTO eram promoção — o
conteúdo estava na imagem, que não é lida (sem OCR); o 7º era só um link.

  promotom/113100       "Mais cupons Amazon" + foto (grade de cupons)
  fadadoscupons/17839   "🔥 Nova Roleta Shopee" + foto (o mesmo post passava
                        como "🔥 Nova Chance Roleta Shopee": 25 caracteres)
  fadadoscupons/18222   "👀" + foto (a Roleta Shopee do post seguinte)
  promotom/112082       "Libera 17h" + foto (cupom ML)
  promotom/112017       "Libera 18h" + foto (cupom ML)
  promotom/112322       só foto + link ("Cupons Shopee — Resgate aqui")
  nerdofertas/148331    só um link, sem foto → continua barrado

  01  plurais "cupons"/"promoções" e singulares, em qualquer caixa
  02  "roleta" é indicador: "Nova Roleta Shopee" passa mesmo sem foto
  03  o limite de 20 caracteres continua (texto sem mídia)
  04  texto curto SEM mídia continua barrado ("Libera 17h", "Libera 18h",
      "👀", só link)
  05  com mídia VISUAL, a legenda curta ou vazia não barra: os 6 casos reais
      passam; o só-link sem foto continua barrado
  06  normalizar() de ponta a ponta (afiliação simulada, sem rede): os 6
      reais com foto normalizam; sem mídia continuam vazios; foto SEM link
      de loja, ou SEM legenda, continua vazia
  07  ingerir() → normalizar() com "Libera 17h" (Promotom 112082): só mídia
      VISUAL libera — foto, vídeo (atributo ou só MIME) e imagem em arquivo
      normalizam; PDF, áudio, voz, figurinha, prévia de link e sem mídia
      continuam barrados; tem_midia NÃO muda de significado (PDF, áudio,
      voz e figurinha seguem tem_midia=True)
  08  só link do TELEGRAM (canal concorrente), mesmo com foto, não publica:
      "Pocando"/"Fervendo" → t.me/fumotom (Promotom 112021/112091), "Bom
      preço:" → t.me/fadapromos/13861 (Fada dos Cupons 17867) — o filtro
      de contexto os barrava; agora quem barra é a afiliação REAL (nenhuma
      plataforma reivindica t.me: "Zero links convertidos")
  09  hardware com foto e legenda curta chega à curadoria classificável:
      "RTX 4060" + foto antes morria aqui; agora normaliza, com categoria
      placa_de_video no texto_analise (o texto que a curadoria classifica) e
      a identidade do produto. NÃO executa a decisão da curadoria — a
      retenção é coberta pelos testes da própria curadoria
  10  monitoramento: o post que passa SÓ pela mídia visual sai no log como
      [CONTEXTO_MIDIA] (midia=foto|video|imagem e a legenda); quem passa
      pelo texto, sem mídia, só com prévia de link ou com PDF, não sai
  11  o log [CONTEXTO_MIDIA] não leva NENHUMA URL: em linha própria, na
      mesma linha da legenda, link markdown, domínio sem esquema; limite de
      40 caracteres

  A mesma matriz de mídia contra as classes REAIS do Telethon:
  tests/test_midia_visual.py.

    python3 tests/test_tem_contexto.py
"""
from __future__ import annotations

import asyncio
import logging
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
from pipeline.completude import EventoRecuperado                # noqa: E402
from pipeline.ingestao import MensagemBruta, ingerir, links_de  # noqa: E402
from pipeline.institucional import tem_contexto                 # noqa: E402
from telethon.tl.types import (DocumentAttributeAudio,          # noqa: E402
                               DocumentAttributeSticker, DocumentAttributeVideo,
                               MessageMediaDocument, MessageMediaPhoto,
                               MessageMediaWebPage)

plataformas.inicializar()

LINK = "https://link.amazon/B06rFfHPv"

# Os 6 reais COM FOTO (texto exato da prévia pública).
REAIS_COM_FOTO = {
    "promotom/113100": "Mais cupons Amazon\n\nhttps://link.amazon/B06rFfHPv",
    "fadadoscupons/17839": "🔥 Nova Roleta Shopee\n\nhttps://s.shopee.com.br/4fwzpmkGmr",
    "fadadoscupons/18222": "👀\n\nhttps://s.shopee.com.br/4qGvHWQ1x2",
    "promotom/112082": "Libera 17h\n\nhttps://meli.la/2a2yYw1",
    "promotom/112017": "Libera 18h\n\nhttps://meli.la/2a2yYw1",
    "promotom/112322": "https://s.shopee.com.br/2B5snGi9NX",
}
SO_LINK_SEM_FOTO = "https://s.shopee.com.br/4qGkuboaqV"      # nerdofertas/148331
LIBERA = REAIS_COM_FOTO["promotom/112082"]     # só passa pela mídia visual


def com_link(legenda: str) -> str:
    return f"{legenda}\n\n{LINK}"


PASSAM_SEM_MIDIA = [
    "Mais cupons Amazon", "Cupons Amazon", "Promoções", "Mais promoção Amazon",
    "MAIS CUPONS AMAZON", "mais cupons amazon", "PROMOÇÕES", "promoções",
    "Cupom Amazon", "Promoção",
]
BARRADAS_SEM_MIDIA = ["Libera 17h", "Libera 18h", "👀", ""]


# ── auxiliares ────────────────────────────────────────────────────
async def _falso_resolver(converter, msg_id=0):
    """Afiliação simulada (sem rede): cada link vira um afiliado distinto."""
    mapa = {u: "https://afiliado.teste/" + str(abs(hash(u)) % 10 ** 8) for u in converter}
    return mapa, dict(mapa), ["amazon"] * len(mapa)


class _AfiliacaoSimulada:
    def __init__(self, resolver=_falso_resolver):
        self.resolver = resolver

    def __enter__(self):
        self.real = normalizacao.resolver_e_afiliar
        normalizacao.resolver_e_afiliar = self.resolver

    def __exit__(self, *exc):
        normalizacao.resolver_e_afiliar = self.real


class _Coleta(logging.Handler):
    """As linhas do log NORMAL (o da normalização) durante o bloco."""
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas = []

    def __enter__(self):
        logging.getLogger("NORMAL").addHandler(self)
        return self

    def __exit__(self, *exc):
        logging.getLogger("NORMAL").removeHandler(self)

    def emit(self, rec):
        self.linhas.append(rec.getMessage())

    def marcas(self, mid):
        return [l for l in self.linhas if "[CONTEXTO_MIDIA]" in l and f"id={mid} " in l]


class _Foto:                         # o objeto de mídia não é baixado aqui
    pass


def _bruta(texto, msg_id, visual=""):
    """MensagemBruta como a ingestão entrega: foto ⇒ tem_midia e midia_visual."""
    return MensagemBruta(msg_id=msg_id, chat="-1001825680721", texto=texto,
                         links=links_de(texto), tem_midia=bool(visual),
                         media_obj=_Foto() if visual else None, midia_visual=visual)


async def _canal():
    return types.SimpleNamespace(username="promotom", usernames=None)


def _ingerir(mid, texto, media):
    """A INGESTÃO real (EventoRecuperado → ingerir) sobre uma mensagem com
    a mídia dada (tipos falsos do harness; os reais: test_midia_visual)."""
    m = types.SimpleNamespace(id=mid, chat_id=-1001825680721, text=texto, message=texto,
                              raw_text=texto, media=media, reply_to=None, entities=None,
                              get_chat=_canal)
    return asyncio.run(ingerir(EventoRecuperado(m)))


def _doc(mid, mime, *atributos):
    return MessageMediaDocument(document=types.SimpleNamespace(
        id=mid, mime_type=mime, attributes=list(atributos)))


def _midias():
    """Cada tipo de mídia que um canal pode mandar com legenda."""
    return {
        "foto": MessageMediaPhoto(photo=types.SimpleNamespace(id=1)),
        "video": _doc(2, "video/mp4", DocumentAttributeVideo(duration=12, w=720, h=1280)),
        "video_so_mime": _doc(3, "video/mp4"),
        "imagem_arquivo": _doc(4, "image/png"),
        "pdf": _doc(5, "application/pdf"),
        "audio": _doc(6, "audio/mpeg", DocumentAttributeAudio(duration=30)),
        "voz": _doc(7, "audio/ogg", DocumentAttributeAudio(duration=5, voice=True)),
        "figurinha": _doc(8, "image/webp", DocumentAttributeSticker(alt="🔥")),
        "previa": MessageMediaWebPage(),
        "nenhuma": None,
    }


# ── testes ────────────────────────────────────────────────────────
def test_01_plurais_e_singulares(r):
    falhas = [t for t in PASSAM_SEM_MIDIA if not tem_contexto(com_link(t))]
    r.check(falhas == [], "01.plurais_singulares_e_caixa", str(falhas))


def test_02_roleta_e_indicador(r):
    r.check(all(tem_contexto(com_link(t)) for t in
                ("Nova Roleta Shopee", "🔥 Nova Roleta Shopee", "ROLETA SHOPEE", "roleta")),
            "02.roleta_passa_sem_midia")


def test_03_limite_de_20_caracteres_continua(r):
    r.check(tem_contexto(com_link("Kit de panelas Tramon")) is True
            and len("Kit de panelas Tramon") == 21, "03.vinte_e_um_passa")
    r.check(tem_contexto(com_link("Kit de panelas Tramo")) is False
            and len("Kit de panelas Tramo") == 20, "03.vinte_nao_passa")


def test_04_texto_curto_sem_midia_continua_barrado(r):
    passaram = [t for t in BARRADAS_SEM_MIDIA if tem_contexto(com_link(t))]
    r.check(passaram == [], "04.sem_midia_barrado", str(passaram))
    r.check(tem_contexto(SO_LINK_SEM_FOTO) is False, "04.so_link_sem_foto_nerd_148331")


def test_05_com_midia_visual_a_legenda_curta_nao_barra(r):
    barrados = [p for p, t in REAIS_COM_FOTO.items() if not tem_contexto(t, midia_visual=True)]
    r.check(barrados == [], "05.os_6_reais_com_foto_passam", str(barrados))
    r.check(tem_contexto(SO_LINK_SEM_FOTO, midia_visual=False) is False,
            "05.so_link_sem_foto_continua_barrado")


def test_06_normalizar_ponta_a_ponta(r):
    """O normalizar() real; só a afiliação é simulada (sem rede)."""
    with _AfiliacaoSimulada():
        com_foto = {p: asyncio.run(normalizacao.normalizar(_bruta(t, 800000 + i, "foto")))
                    for i, (p, t) in enumerate(REAIS_COM_FOTO.items())}
        r.check(all(n is not None for n in com_foto.values()), "06.os_6_reais_normalizam",
                str({p: n is not None for p, n in com_foto.items()}))
        n = com_foto["promotom/113100"]
        r.check(n is not None and "Mais cupons Amazon" in n.texto_limpo and n.tem_midia,
                "06.promotom_113100_legenda_e_midia", repr(getattr(n, "texto_limpo", None)))
        sem = {t: asyncio.run(normalizacao.normalizar(_bruta(com_link(t), 900000 + i)))
               for i, t in enumerate(BARRADAS_SEM_MIDIA)}
        r.check(all(v is None for v in sem.values()), "06.sem_midia_continuam_vazios",
                str({t: v is None for t, v in sem.items()}))
        sem_link = asyncio.run(normalizacao.normalizar(_bruta("Bom dia! ☀️", 910000, "foto")))
        r.check(sem_link is None, "06.foto_sem_link_de_loja_continua_vazia")
        # Foto SEM legenda (e sem link): a regra da mídia não a publica.
        r.check(asyncio.run(normalizacao.normalizar(_bruta("", 910001, "foto"))) is None
                and asyncio.run(normalizacao.normalizar(_bruta("  \n ", 910002, "foto"))) is None,
                "06.foto_sem_legenda_continua_vazia")


def test_07_so_midia_visual_libera_a_legenda_curta(r):
    """Ingestão real → normalização real (afiliação simulada). A prévia de
    link (quase todo post só-texto com link chega com ela) nunca conta."""
    assert not tem_contexto(LIBERA)
    res = {}
    with _AfiliacaoSimulada():
        for i, (nome, media) in enumerate(_midias().items()):
            b = _ingerir(112100 + i, LIBERA, media)
            res[nome] = (b.tem_midia, b.midia_visual,
                         asyncio.run(normalizacao.normalizar(b)) is not None)
    esperado = {
        "foto": (True, "foto", True),
        "video": (True, "video", True),
        "video_so_mime": (True, "video", True),
        "imagem_arquivo": (True, "imagem", True),
        "pdf": (True, "", False),
        "audio": (True, "", False),
        "voz": (True, "", False),
        "figurinha": (True, "", False),
        "previa": (False, "", False),
        "nenhuma": (False, "", False),
    }
    erradas = {k: (res.get(k), v) for k, v in esperado.items() if res.get(k) != v}
    r.check(erradas == {}, "07.matriz_tem_midia_visual_normaliza", str(erradas))


def test_08_so_link_do_telegram_com_foto_nao_publica(r):
    """Sem simular a afiliação: o resolver_e_afiliar real (t.me não é
    encurtador genérico nem plataforma — não há rede)."""
    from plataformas import registry

    reais = {
        "promotom/112021": "Pocando\n\nhttps://t.me/fumotom",
        "promotom/112091": "Fervendo\nhttps://t.me/fumotom",
        "fadadoscupons/17867": "Bom preço:\nhttps://t.me/fadapromos/13861",
    }
    r.check(all(registry.resolver(u) is None
                for t in reais.values() for u in links_de(t)), "08.t_me_sem_plataforma")
    res = {p: asyncio.run(normalizacao.normalizar(_bruta(t, 920000 + i, "foto")))
           for i, (p, t) in enumerate(reais.items())}
    r.check(all(v is None for v in res.values()), "08.so_t_me_com_foto_nao_normaliza",
            str({p: v is None for p, v in res.items()}))


def test_09_hardware_com_foto_e_legenda_curta_chega_classificavel(r):
    """A regra da mídia não tira o post do alcance da curadoria: ele
    normaliza com a categoria (lida do texto_analise, o texto que a
    curadoria classifica) e com a identidade do produto. Este teste NÃO
    executa a decisão da curadoria — a retenção é dos testes dela."""
    from pipeline.curadoria import categoria

    async def resolver_amazon(converter, msg_id=0):
        mapa = {u: u + "?tag=teste-20" for u in converter}
        return mapa, dict(mapa), ["amazon"] * len(mapa)

    texto = "RTX 4060\n\nhttps://www.amazon.com.br/dp/B0C8ZQTRD7"
    r.check(not tem_contexto(texto), "09.sem_foto_continua_barrado")
    with _AfiliacaoSimulada(resolver_amazon):
        n = asyncio.run(normalizacao.normalizar(_bruta(texto, 930000, "foto")))
    r.check(n is not None and categoria(n.texto_analise) == "placa_de_video"
            and any("B0C8ZQTRD7" in str(i) for i in (n.ids_globais or [])),
            "09.normaliza_com_categoria_e_identidade",
            repr((getattr(n, "texto_analise", None), getattr(n, "ids_globais", None))))


def test_10_marcador_de_quem_passa_so_pela_midia(r):
    """Revisão do #43: "monitorar o risco de uma foto com legenda irrelevante
    e link de loja passar". Ingestão real (o tipo visual vem dela)."""
    m = _midias()
    cupons = REAIS_COM_FOTO["promotom/113100"]           # passa pelo texto (plural)
    with _AfiliacaoSimulada(), _Coleta() as col:
        ok = {
            "foto": asyncio.run(normalizacao.normalizar(_ingerir(940001, LIBERA, m["foto"]))),
            "video": asyncio.run(normalizacao.normalizar(_ingerir(940002, LIBERA, m["video"]))),
            "texto_com_foto": asyncio.run(normalizacao.normalizar(
                _ingerir(940003, cupons, m["foto"]))),
            "sem_midia": asyncio.run(normalizacao.normalizar(_ingerir(940004, LIBERA, None))),
            "previa": asyncio.run(normalizacao.normalizar(_ingerir(940005, LIBERA, m["previa"]))),
            "pdf": asyncio.run(normalizacao.normalizar(_ingerir(940006, LIBERA, m["pdf"]))),
        }
    ok = {k: v is not None for k, v in ok.items()}
    r.check(ok == {"foto": True, "video": True, "texto_com_foto": True,
                   "sem_midia": False, "previa": False, "pdf": False}, "10.quem_passa", str(ok))
    m1, m2 = col.marcas(940001), col.marcas(940002)
    r.check(len(m1) == 1 and "midia=foto" in m1[0] and "legenda='Libera 17h'" in m1[0],
            "10.foto_marcada_com_legenda", str(m1))
    r.check(len(m2) == 1 and "midia=video" in m2[0], "10.video_marcado", str(m2))
    r.check(not any(col.marcas(i) for i in (940003, 940004, 940005, 940006)),
            "10.texto_sem_midia_previa_e_pdf_nao_marcam",
            str([l for l in col.linhas if "[CONTEXTO_MIDIA]" in l]))


def test_11_log_sem_nenhuma_url(r):
    """A legenda do [CONTEXTO_MIDIA] não leva URL nenhuma, nem pedaço dela."""
    from pipeline.normalizacao import legenda_sem_links
    proibidos = ("http", "://", "www", "meli", "abc123", "a.co", "amazon", "t.me",
                 "fumotom", "tg:", ".la", ".br", "/")
    formas = {
        "linha_propria": ("Libera 17h\n\nhttps://meli.la/abc123", "Libera 17h"),
        "mesma_linha": ("Libera 17h https://meli.la/abc123", "Libera 17h"),
        "colada": ("Libera17hhttps://meli.la/abc123", "Libera17h"),
        "markdown": ("[Libera 17h](https://meli.la/abc123)", "Libera 17h"),
        "sem_esquema": ("Libera 17h meli.la/abc123", "Libera 17h"),
        "www": ("Pega www.amazon.com.br/dp/X já", "Pega já"),
        "t_me": ("t.me/fumotom Pocando", "Pocando"),
        "tg": ("Libera 17h tg://resolve?domain=fumotom", "Libera 17h"),
        "meio": ("👀 https://a.co/d/1Ab 🔥", "👀 🔥"),
    }
    erradas = {k: legenda_sem_links(t) for k, (t, e) in formas.items()
               if legenda_sem_links(t) != e or any(p in legenda_sem_links(t) for p in proibidos)}
    r.check(erradas == {}, "11.helper_tira_toda_url", str(erradas))
    r.check(legenda_sem_links("R$ 1.149 à vista") == "R$ 1.149 à vista"
            and legenda_sem_links("Air fryer 5.5L") == "Air fryer 5.5L",
            "11.preco_e_medida_ficam")

    # Ponta a ponta: o marcador sai (a legenda só passa pela foto) e não
    # leva o link — em linha própria e na MESMA linha da legenda.
    foto = _midias()["foto"]
    casos = {
        950001: "Libera 17h\n\nhttps://meli.la/abc123",
        950002: "👀 https://a.co/d/1A",            # 20 caracteres: só a foto salva
        950003: "👀 meli.la/abc123\nhttps://meli.la/abc123",   # domínio sem esquema
    }
    with _AfiliacaoSimulada(), _Coleta() as col:
        for mid, t in casos.items():
            assert not tem_contexto(t), t
            asyncio.run(normalizacao.normalizar(_ingerir(mid, t, foto)))
    linhas = {mid: col.marcas(mid) for mid in casos}
    vazou = {mid: l for mid, l in linhas.items()
             if len(l) != 1 or any(p in l[0].split("legenda=", 1)[1] for p in proibidos)}
    r.check(vazou == {}, "11.marcador_sem_url_linha_propria_e_mesma_linha", str(linhas))
    r.check("legenda='👀'" in (linhas[950002] or [""])[0], "11.mesma_linha_fica_so_a_legenda",
            str(linhas[950002]))

    # Limite de 40 caracteres continua.
    longa = "x" * 15 + " " + "https://meli.la/abc123 " + "y" * 60
    with _AfiliacaoSimulada(), _Coleta() as col:
        import pipeline.institucional as inst
        original = inst.tem_contexto
        # força o caminho do marcador (texto não basta, a foto salva)
        inst.tem_contexto = lambda t, midia_visual=False: bool(midia_visual)
        try:
            asyncio.run(normalizacao.normalizar(_ingerir(950004, longa, foto)))
        finally:
            inst.tem_contexto = original
    m4 = col.marcas(950004)
    leg = m4[0].split("legenda=", 1)[1].split(" — ")[0] if m4 else ""
    r.check(len(m4) == 1 and leg == repr(("x" * 15 + " " + "y" * 60)[:40])
            and "meli" not in m4[0], "11.limite_40_sem_url", str(m4))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CONTEXTO · plurais, roleta e mídia visual · casos reais"))
