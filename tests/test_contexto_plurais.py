"""
CONTEXTO — tem_contexto() reconhece os PLURAIS "cupons" e "promoções".

Caso real (03/11/2026, 12:38): Promotom 113100 — imagem com a grade de
cupons Amazon, legenda "Mais cupons Amazon" + https://link.amazon/B06rFfHPv
— caiu como NORMALIZACAO_VAZIA. Sem o link, o texto tem 18 caracteres
(<= 20) e o indicador `cupom` não casava com "cupons" (nem `promoção`
com "promoções"). O texto da imagem não é lido (sem OCR).

  01  plurais e singulares, em qualquer caixa, passam
  02  o limite de 20 caracteres continua: 21 passa, 20 não
  03  os falsos positivos medidos nos canais continuam barrados
      ("Libera 17h", "Libera 18h", "Nova Roleta Shopee", "👀", só link)
  04  normalizar() de ponta a ponta (afiliação simulada, sem rede):
      mídia + legenda + link — 113100 agora normaliza; os falsos
      positivos continuam vazios

    python3 tests/test_contexto_plurais.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import types

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

import plataformas                                              # noqa: E402
from pipeline import normalizacao                               # noqa: E402
from pipeline.ingestao import MensagemBruta, links_de           # noqa: E402
from pipeline.institucional import tem_contexto                 # noqa: E402

plataformas.inicializar()

LINK = "https://link.amazon/B06rFfHPv"


def com_link(legenda: str) -> str:
    return f"{legenda}\n\n{LINK}"


PASSAM = [
    "Mais cupons Amazon",            # Promotom 113100 (o caso real)
    "Cupons Amazon",
    "Promoções",
    "Mais promoção Amazon",
    "MAIS CUPONS AMAZON",
    "mais cupons amazon",
    "PROMOÇÕES",
    "promoções",
    "Cupom Amazon",                  # singular continua
    "Promoção",
]

BARRADAS = [
    "Libera 17h",                    # Promotom 112082
    "Libera 18h",                    # Promotom 112017
    "Nova Roleta Shopee",            # Fada dos Cupons 17839
    "👀",                             # Fada dos Cupons 18222
    "",                              # só o link (Promotom 112322)
]


def test_01_plurais_e_singulares_passam(r):
    falhas = [t for t in PASSAM if not tem_contexto(com_link(t))]
    r.check(falhas == [], "01.plurais_singulares_e_caixa", str(falhas))


def test_02_limite_de_20_caracteres_continua(r):
    r.check(tem_contexto(com_link("Kit de panelas Tramon")) is True
            and len("Kit de panelas Tramon") == 21, "02.vinte_e_um_passa")
    r.check(tem_contexto(com_link("Kit de panelas Tramo")) is False
            and len("Kit de panelas Tramo") == 20, "02.vinte_nao_passa")


def test_03_falsos_positivos_continuam_barrados(r):
    passaram = [t for t in BARRADAS if tem_contexto(com_link(t))]
    r.check(passaram == [], "03.falsos_positivos", str(passaram))


def test_04_normalizar_ponta_a_ponta(r):
    """Mídia + legenda + link pelo normalizar() real; só a afiliação é
    simulada (sem rede)."""
    async def falso_resolver(converter, msg_id=0):
        mapa = {u: "https://amzn.to/AFILIADO1" for u in converter}
        return mapa, dict(mapa), ["amazon"] * len(mapa)

    class Foto:                      # o objeto de mídia não é baixado aqui
        pass

    def bruta(legenda, msg_id):
        texto = com_link(legenda)
        return MensagemBruta(msg_id=msg_id, chat="-1001825680721", texto=texto,
                             links=links_de(texto), tem_midia=True, media_obj=Foto())

    real = normalizacao.resolver_e_afiliar
    normalizacao.resolver_e_afiliar = falso_resolver
    try:
        norm = asyncio.run(normalizacao.normalizar(bruta("Mais cupons Amazon", 113100)))
        r.check(norm is not None and "Mais cupons Amazon" in norm.texto_limpo
                and norm.tem_midia, "04.promotom_113100_normaliza",
                repr(getattr(norm, "texto_limpo", None)))
        vazias = {t: asyncio.run(normalizacao.normalizar(bruta(t, 900000 + i)))
                  for i, t in enumerate(BARRADAS)}
        r.check(all(v is None for v in vazias.values()), "04.falsos_positivos_vazios",
                str({t: v is None for t, v in vazias.items()}))
    finally:
        normalizacao.resolver_e_afiliar = real


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CONTEXTO · plurais cupons/promoções · Promotom 113100"))
