"""MÍDIA VISUAL — ingestao.tipo_midia_visual contra o Telethon REAL.

Revisão do #43: só foto e vídeo REAIS (e imagem enviada como arquivo)
deixam a legenda curta passar pelo filtro de contexto; PDF, áudio, voz,
figurinha e os outros tipos não visuais, não. O significado global de
tem_midia NÃO muda (segue valendo para o envio da mídia).

Caso real da legenda: Promotom 112082 "Libera 17h" + https://meli.la/2a2yYw1
(cupom ML com foto) — sem a mídia visual, a legenda é barrada.

  01  a matriz pela INGESTÃO real (EventoRecuperado → ingerir), com
      Message, Photo, Document e DocumentAttribute* do Telethon:
      tem_midia, midia_visual e chave_midia de cada tipo
  02  a mesma matriz até o normalizar(): só o visual normaliza
  03  tipo_midia_visual nunca levanta (objetos quebrados ⇒ "")

Execução (standalone, stdlib + Telethon):
    python3 tests/test_midia_visual.py
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
import sys
import types as _pytypes

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import telethon  # noqa: E402  — REAL, antes do harness
from telethon import TelegramClient, utils  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import types  # noqa: E402

assert hasattr(telethon.TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from _harness_e5 import preparar, rodar  # noqa: E402

preparar()   # telethon real preservado; só aiohttp (se ausente) é simulado

from database import _init_db  # noqa: E402

_init_db()

try:
    import yarl  # noqa: F401
except ImportError:
    _yarl = _pytypes.ModuleType("yarl")

    class _URL(str):
        def __new__(cls, valor="", **_kw):
            return str.__new__(cls, valor)
    _yarl.URL = _URL
    sys.modules["yarl"] = _yarl

import plataformas  # noqa: E402
from pipeline import identidade, normalizacao  # noqa: E402
from pipeline.completude import EventoRecuperado  # noqa: E402
from pipeline.ingestao import chave_midia, ingerir, tipo_midia_visual  # noqa: E402

plataformas.inicializar()

PROMOTOM_ID = 1825680721
PROMOTOM = types.Channel(id=PROMOTOM_ID, title="promotom", photo=types.ChatPhotoEmpty(),
                         date=dt.datetime.now(dt.timezone.utc), access_hash=77,
                         username="promotom", broadcast=True)
PRO = utils.get_peer_id(PROMOTOM)
TC = TelegramClient(StringSession(), 1, "0" * 32)
TC._mb_entity_cache.set_self_user(424242, False, 0)

LIBERA = "Libera 17h\n\nhttps://meli.la/2a2yYw1"          # Promotom 112082
AGORA = dt.datetime.now(dt.timezone.utc)


def _doc(did, mime, *atributos):
    return types.MessageMediaDocument(document=types.Document(
        id=did, access_hash=1, file_reference=b"", date=AGORA, mime_type=mime,
        size=1000, dc_id=1, attributes=list(atributos)))


def _midias():
    """Cada tipo que um canal manda com legenda — objetos do Telethon real."""
    nome = types.DocumentAttributeFilename
    return {
        "foto": types.MessageMediaPhoto(photo=types.Photo(
            id=11, access_hash=1, file_reference=b"", date=AGORA,
            sizes=[types.PhotoSize(type="x", w=800, h=800, size=1000)], dc_id=1)),
        "video": _doc(12, "video/mp4",
                      types.DocumentAttributeVideo(duration=12, w=720, h=1280),
                      nome(file_name="oferta.mp4")),
        "video_so_mime": _doc(13, "video/mp4", nome(file_name="oferta.mp4")),
        # Vídeo com MIME genérico: quem diz que é vídeo é o ATRIBUTO.
        "video_mime_generico": _doc(22, "application/octet-stream",
                                    types.DocumentAttributeVideo(duration=9, w=720, h=720)),
        "gif": _doc(14, "video/mp4", types.DocumentAttributeAnimated(),
                    types.DocumentAttributeVideo(duration=3, w=320, h=320)),
        "video_redondo": _doc(15, "video/mp4", types.DocumentAttributeVideo(
            duration=8, w=384, h=384, round_message=True)),
        "imagem_arquivo": _doc(16, "image/png", types.DocumentAttributeImageSize(w=800, h=800),
                               nome(file_name="cupom.png")),
        "pdf": _doc(17, "application/pdf", nome(file_name="catalogo.pdf")),
        "audio": _doc(18, "audio/mpeg", types.DocumentAttributeAudio(
            duration=30, title="jingle"), nome(file_name="jingle.mp3")),
        "voz": _doc(19, "audio/ogg", types.DocumentAttributeAudio(duration=5, voice=True)),
        # Áudio em contêiner MP4 (MIME video/mp4): quem diz que é áudio é o
        # ATRIBUTO — não libera.
        "audio_em_mp4": _doc(23, "video/mp4", types.DocumentAttributeAudio(
            duration=60, title="podcast")),
        "figurinha": _doc(20, "image/webp", types.DocumentAttributeImageSize(w=512, h=512),
                          types.DocumentAttributeSticker(
                              alt="🔥", stickerset=types.InputStickerSetEmpty())),
        "zip": _doc(21, "application/zip", nome(file_name="cupons.zip")),
        "previa": types.MessageMediaWebPage(webpage=types.WebPageEmpty(id=1)),
        "enquete": types.MessageMediaPoll(
            poll=types.Poll(id=1, question=types.TextWithEntities(text="Qual?", entities=[]),
                            answers=[], hash=0),
            results=types.PollResults()),
        "nenhuma": None,
    }


ESPERADO = {        # (tem_midia, midia_visual)
    "foto": (True, "foto"),
    "video": (True, "video"),
    "video_so_mime": (True, "video"),
    "video_mime_generico": (True, "video"),
    "gif": (True, "video"),
    "video_redondo": (True, "video"),
    "imagem_arquivo": (True, "imagem"),
    "pdf": (True, ""),
    "audio": (True, ""),
    "voz": (True, ""),
    "audio_em_mp4": (True, ""),
    "figurinha": (True, ""),
    "zip": (True, ""),
    "previa": (False, ""),
    "enquete": (True, ""),
    "nenhuma": (False, ""),
}


def _mensagem(mid, texto, media):
    m = types.Message(id=mid, peer_id=types.PeerChannel(PROMOTOM_ID), date=AGORA,
                      message=texto, media=media, post=True)
    m._finish_init(TC, {PRO: PROMOTOM}, None)
    return m


async def _falso_resolver(converter, msg_id=0):
    mapa = {u: "https://afiliado.teste/" + str(abs(hash(u)) % 10 ** 8) for u in converter}
    return mapa, dict(mapa), ["mercadolivre"] * len(mapa)


def _ingerir(mid, media):
    identidade._CHAT_USERNAME[PRO] = "promotom"
    return asyncio.run(ingerir(EventoRecuperado(_mensagem(mid, LIBERA, media))))


def test_01_matriz_pela_ingestao_real(r):
    res = {}
    for i, (nome, media) in enumerate(_midias().items()):
        b = _ingerir(112200 + i, media)
        res[nome] = (b.tem_midia, b.midia_visual)
    erradas = {k: (res.get(k), v) for k, v in ESPERADO.items() if res.get(k) != v}
    r.check(erradas == {}, "01.tem_midia_e_midia_visual", str(erradas))
    # A chave da mídia (E4.0) não muda: foto e documento seguem identificados.
    b_foto, b_pdf = _ingerir(112290, _midias()["foto"]), _ingerir(112291, _midias()["pdf"])
    r.check(b_foto.midia_key == "photo:11" and b_pdf.midia_key == "doc:17",
            "01.chave_midia_intacta", f"{b_foto.midia_key} {b_pdf.midia_key}")


def test_02_so_o_visual_normaliza(r):
    real = normalizacao.resolver_e_afiliar
    normalizacao.resolver_e_afiliar = _falso_resolver
    try:
        res = {nome: asyncio.run(normalizacao.normalizar(_ingerir(112300 + i, media))) is not None
               for i, (nome, media) in enumerate(_midias().items())}
    finally:
        normalizacao.resolver_e_afiliar = real
    esperado = {k: bool(v[1]) for k, v in ESPERADO.items()}
    erradas = {k: (res.get(k), v) for k, v in esperado.items() if res.get(k) != v}
    r.check(erradas == {}, "02.so_foto_video_e_imagem_normalizam", str(erradas))


def test_03_nunca_levanta(r):
    class Quebrado:
        @property
        def media(self):
            raise RuntimeError("objeto quebrado")

    doc_sem_atributos = types.MessageMediaDocument(document=None)
    casos = [Quebrado(), _pytypes.SimpleNamespace(media=doc_sem_atributos),
             _pytypes.SimpleNamespace(media=types.MessageMediaPhoto(photo=None)),
             _pytypes.SimpleNamespace(media=types.MessageMediaDocument(document=types.Document(
                 id=1, access_hash=1, file_reference=b"", date=AGORA, mime_type=None,
                 size=0, dc_id=1, attributes=[])))]
    r.check([tipo_midia_visual(c) for c in casos] == ["", "", "", ""],
            "03.quebrado_vazio_sem_mime_da_vazio")
    r.check(chave_midia(casos[2]) == "", "03.foto_sem_photo_sem_chave")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "MÍDIA VISUAL · Telethon real · revisão do #43"))
