"""
CONTEXTO — tem_contexto(): plurais, "roleta" e post com FOTO.

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
  05  com FOTO, a legenda curta ou vazia não barra: os 6 casos reais passam;
      o só-link sem foto continua barrado
  06  normalizar() de ponta a ponta (afiliação simulada, sem rede): os 6
      reais com mídia normalizam; sem mídia continuam vazios; foto SEM link
      de loja, ou SEM legenda, continua vazia
  07  ingerir() → normalizar(): a PRÉVIA DE LINK não é mídia — "Libera 17h"
      (Promotom 112082) com prévia continua barrado; com foto ou vídeo,
      normaliza
  08  só link do TELEGRAM (canal concorrente), mesmo com foto, não publica:
      "Pocando"/"Fervendo" → t.me/fumotom (Promotom 112021/112091), "Bom
      preço:" → t.me/fadapromos/13861 (Fada dos Cupons 17867) — o filtro
      de contexto os barrava; agora quem barra é a afiliação REAL (nenhuma
      plataforma reivindica t.me: "Zero links convertidos")
  09  hardware com foto e legenda curta não escapa da curadoria: "RTX 4060"
      + foto antes morria aqui; agora normaliza, e o texto que a curadoria
      classifica (texto_analise) dá placa_de_video — fica retido como
      qualquer hardware vigiado de fonte retentora
  10  monitoramento: o post que passa SÓ pela mídia sai no log como
      [CONTEXTO_MIDIA] (tipo da mídia e legenda); quem passa pelo texto, sem
      mídia ou só com prévia de link, não sai

    python3 tests/test_tem_contexto.py
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
from pipeline.ingestao import MensagemBruta, links_de           # noqa: E402
from pipeline.institucional import tem_contexto                 # noqa: E402

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


def com_link(legenda: str) -> str:
    return f"{legenda}\n\n{LINK}"


PASSAM_SEM_MIDIA = [
    "Mais cupons Amazon", "Cupons Amazon", "Promoções", "Mais promoção Amazon",
    "MAIS CUPONS AMAZON", "mais cupons amazon", "PROMOÇÕES", "promoções",
    "Cupom Amazon", "Promoção",
]
BARRADAS_SEM_MIDIA = ["Libera 17h", "Libera 18h", "👀", ""]


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


def test_05_com_foto_a_legenda_curta_nao_barra(r):
    barrados = [p for p, t in REAIS_COM_FOTO.items() if not tem_contexto(t, tem_midia=True)]
    r.check(barrados == [], "05.os_6_reais_com_foto_passam", str(barrados))
    r.check(tem_contexto(SO_LINK_SEM_FOTO, tem_midia=False) is False,
            "05.so_link_sem_foto_continua_barrado")


def test_06_normalizar_ponta_a_ponta(r):
    """O normalizar() real; só a afiliação é simulada (sem rede)."""
    async def falso_resolver(converter, msg_id=0):
        mapa = {u: "https://afiliado.teste/" + str(abs(hash(u)) % 10 ** 8) for u in converter}
        return mapa, dict(mapa), ["amazon"] * len(mapa)

    class Foto:                      # o objeto de mídia não é baixado aqui
        pass

    def bruta(texto, msg_id, midia):
        return MensagemBruta(msg_id=msg_id, chat="-1001825680721", texto=texto,
                             links=links_de(texto), tem_midia=midia,
                             media_obj=Foto() if midia else None)

    real = normalizacao.resolver_e_afiliar
    normalizacao.resolver_e_afiliar = falso_resolver
    try:
        com_foto = {p: asyncio.run(normalizacao.normalizar(bruta(t, 800000 + i, True)))
                    for i, (p, t) in enumerate(REAIS_COM_FOTO.items())}
        r.check(all(n is not None for n in com_foto.values()), "06.os_6_reais_normalizam",
                str({p: n is not None for p, n in com_foto.items()}))
        n = com_foto["promotom/113100"]
        r.check(n is not None and "Mais cupons Amazon" in n.texto_limpo and n.tem_midia,
                "06.promotom_113100_legenda_e_midia", repr(getattr(n, "texto_limpo", None)))
        sem = {t: asyncio.run(normalizacao.normalizar(bruta(com_link(t), 900000 + i, False)))
               for i, t in enumerate(BARRADAS_SEM_MIDIA)}
        r.check(all(v is None for v in sem.values()), "06.sem_midia_continuam_vazios",
                str({t: v is None for t, v in sem.items()}))
        sem_link = asyncio.run(normalizacao.normalizar(bruta("Bom dia! ☀️", 910000, True)))
        r.check(sem_link is None, "06.foto_sem_link_de_loja_continua_vazia")
        # Foto SEM legenda (e sem link): a regra da mídia não a publica.
        r.check(asyncio.run(normalizacao.normalizar(bruta("", 910001, True))) is None
                and asyncio.run(normalizacao.normalizar(bruta("  \n ", 910002, True))) is None,
                "06.foto_sem_legenda_continua_vazia")
    finally:
        normalizacao.resolver_e_afiliar = real



def test_07_previa_de_link_nao_e_midia(r):
    """A regra da mídia só vale porque ingerir() exclui a prévia de link
    (MessageMediaWebPage): quase todo post só-texto com link chega com
    prévia. Ingestão real → normalização real (afiliação simulada)."""
    import types as _t
    from pipeline.completude import EventoRecuperado
    from pipeline.ingestao import ingerir
    from telethon.tl.types import (MessageMediaDocument, MessageMediaPhoto,
                                   MessageMediaWebPage)

    async def falso_resolver(converter, msg_id=0):
        mapa = {u: "https://afiliado.teste/" + str(abs(hash(u)) % 10 ** 8) for u in converter}
        return mapa, dict(mapa), ["amazon"] * len(mapa)

    async def canal():
        return _t.SimpleNamespace(username="promotom", usernames=None)

    # Legenda que SÓ passa pela mídia ("Mais cupons Amazon" já passa pelo
    # plural, com ou sem foto).
    texto = REAIS_COM_FOTO["promotom/112082"]
    assert not tem_contexto(texto)
    midias = {
        "foto": MessageMediaPhoto(photo=_t.SimpleNamespace(id=112082)),
        "video": MessageMediaDocument(document=_t.SimpleNamespace(id=112083)),
        "previa": MessageMediaWebPage(),
        "nenhuma": None,
    }
    real = normalizacao.resolver_e_afiliar
    normalizacao.resolver_e_afiliar = falso_resolver
    try:
        res = {}
        for i, (nome, media) in enumerate(midias.items()):
            m = _t.SimpleNamespace(id=112082 + i, chat_id=-1001825680721, text=texto,
                                   message=texto, raw_text=texto, media=media,
                                   reply_to=None, entities=None, get_chat=canal)
            bruta = asyncio.run(ingerir(EventoRecuperado(m)))
            res[nome] = (bruta.tem_midia, asyncio.run(normalizacao.normalizar(bruta)) is not None)
    finally:
        normalizacao.resolver_e_afiliar = real
    r.check(res == {"foto": (True, True), "video": (True, True),
                    "previa": (False, False), "nenhuma": (False, False)},
            "07.previa_nao_conta_foto_e_video_contam", str(res))



def test_08_so_link_do_telegram_com_foto_nao_publica(r):
    """Sem simular a afiliação: o resolver_e_afiliar real (t.me não é
    encurtador genérico nem plataforma — não há rede)."""
    from plataformas import registry

    class Foto:
        pass

    reais = {
        "promotom/112021": "Pocando\n\nhttps://t.me/fumotom",
        "promotom/112091": "Fervendo\nhttps://t.me/fumotom",
        "fadadoscupons/17867": "Bom preço:\nhttps://t.me/fadapromos/13861",
    }
    r.check(all(registry.resolver(u) is None
                for t in reais.values() for u in links_de(t)), "08.t_me_sem_plataforma")
    res = {p: asyncio.run(normalizacao.normalizar(MensagemBruta(
               msg_id=920000 + i, chat="-1001825680721", texto=t, links=links_de(t),
               tem_midia=True, media_obj=Foto())))
           for i, (p, t) in enumerate(reais.items())}
    r.check(all(v is None for v in res.values()), "08.so_t_me_com_foto_nao_normaliza",
            str({p: v is None for p, v in res.items()}))



def test_09_hardware_com_foto_e_legenda_curta_chega_a_curadoria(r):
    """A regra da mídia não abre atalho para fora da curadoria: a categoria
    é lida do texto_analise da normalização real (afiliação simulada)."""
    from pipeline.curadoria import categoria

    async def falso_resolver(converter, msg_id=0):
        mapa = {u: u + "?tag=teste-20" for u in converter}
        return mapa, dict(mapa), ["amazon"] * len(mapa)

    class Foto:
        pass

    texto = "RTX 4060\n\nhttps://www.amazon.com.br/dp/B0C8ZQTRD7"
    r.check(not tem_contexto(texto), "09.sem_foto_continua_barrado")
    real = normalizacao.resolver_e_afiliar
    normalizacao.resolver_e_afiliar = falso_resolver
    try:
        n = asyncio.run(normalizacao.normalizar(MensagemBruta(
            msg_id=930000, chat="-1001825680721", texto=texto, links=links_de(texto),
            tem_midia=True, media_obj=Foto())))
    finally:
        normalizacao.resolver_e_afiliar = real
    r.check(n is not None and categoria(n.texto_analise) == "placa_de_video"
            and any("B0C8ZQTRD7" in str(i) for i in (n.ids_globais or [])),
            "09.normaliza_com_categoria_e_identidade",
            repr((getattr(n, "texto_analise", None), getattr(n, "ids_globais", None))))



def test_10_marcador_de_quem_passa_so_pela_midia(r):
    """Revisão do #43: "monitorar o risco de uma foto com legenda irrelevante
    e link de loja passar". Ingestão real (o tipo da mídia vem dela)."""
    import logging
    import types as _t
    from pipeline.completude import EventoRecuperado
    from pipeline.ingestao import ingerir
    from telethon.tl.types import (MessageMediaDocument, MessageMediaPhoto,
                                   MessageMediaWebPage)

    class Coleta(logging.Handler):
        def __init__(self):
            super().__init__(logging.DEBUG)
            self.linhas = []

        def emit(self, rec):
            self.linhas.append(rec.getMessage())

    async def falso_resolver(converter, msg_id=0):
        mapa = {u: "https://afiliado.teste/" + str(abs(hash(u)) % 10 ** 8) for u in converter}
        return mapa, dict(mapa), ["amazon"] * len(mapa)

    async def canal():
        return _t.SimpleNamespace(username="promotom", usernames=None)

    def passa(mid, texto, media):
        m = _t.SimpleNamespace(id=mid, chat_id=-1001825680721, text=texto, message=texto,
                               raw_text=texto, media=media, reply_to=None, entities=None,
                               get_chat=canal)
        bruta = asyncio.run(ingerir(EventoRecuperado(m)))
        return asyncio.run(normalizacao.normalizar(bruta)) is not None

    def marcas(mid):
        return [l for l in col.linhas if "[CONTEXTO_MIDIA]" in l and f"id={mid} " in l]

    libera = REAIS_COM_FOTO["promotom/112082"]           # "Libera 17h" + meli.la
    cupons = REAIS_COM_FOTO["promotom/113100"]           # passa pelo texto (plural)
    foto = MessageMediaPhoto(photo=_t.SimpleNamespace(id=1))
    video = MessageMediaDocument(document=_t.SimpleNamespace(id=2))
    col = Coleta()
    lg = logging.getLogger("NORMAL")
    lg.addHandler(col)
    real = normalizacao.resolver_e_afiliar
    normalizacao.resolver_e_afiliar = falso_resolver
    try:
        ok = {
            "foto": passa(940001, libera, foto),
            "video": passa(940002, libera, video),
            "texto_com_foto": passa(940003, cupons, foto),
            "sem_midia": passa(940004, libera, None),
            "previa": passa(940005, libera, MessageMediaWebPage()),
        }
    finally:
        normalizacao.resolver_e_afiliar = real
        lg.removeHandler(col)
    r.check(ok == {"foto": True, "video": True, "texto_com_foto": True,
                   "sem_midia": False, "previa": False}, "10.quem_passa", str(ok))
    m1, m2 = marcas(940001), marcas(940002)
    r.check(len(m1) == 1 and "midia=photo" in m1[0] and "legenda='Libera 17h'" in m1[0]
            and "meli.la" not in m1[0], "10.foto_marcada_com_legenda_sem_link", str(m1))
    r.check(len(m2) == 1 and "midia=doc" in m2[0], "10.video_marcado", str(m2))
    r.check(not any(marcas(i) for i in (940003, 940004, 940005)),
            "10.texto_sem_midia_e_previa_nao_marcam",
            str([l for l in col.linhas if "[CONTEXTO_MIDIA]" in l]))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CONTEXTO · plurais, roleta e post com foto · casos reais"))
