"""
DESTAQUE — a fonte diz que o preço está bom (pipeline.destaque).

Decisão do Léo (03/11/2026): hardware de categoria vigiada com frase de
destaque no TÍTULO ("Ainda ativo!", "Bom preço!", "Vai esgotar!") passa sem
aval — "o contexto disso é que o preço está muito bom". O vocabulário é o
dos títulos REAIS das prévias públicas (26/10 → 03/11) e das palavras do
dono; a frente seguinte (frases fixas no post) lê o MESMO vocabulário.

  01  cada frase real achada nos títulos (Promotom, Fumotom, Samuel, Fada,
      Nerd) e as do dono são destaque
  02  produto com palavra parecida NÃO é destaque (Bombril, Correia, Baixo
      elétrico, Top Mizuno…); "esgotou" (acabou) não é destaque
  03  retorno ("voltando") não mora aqui: é da pipeline.reativacao
  04  pureza: sem I/O, sem banco, sem Telegram, sem log

    python3 tests/test_destaque.py
"""
from __future__ import annotations

import ast
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar, ler_fonte  # noqa: E402

preparar()

from pipeline.destaque import frase_de_destaque  # noqa: E402
from pipeline.reativacao import eh_reativacao     # noqa: E402

# Títulos/cabeçalhos REAIS (canal/id) e as frases ditas pelo dono.
REAIS = [
    ("promotom/112445", "Ainda ativo! Monitor Gamer Philips Evnia 23.8\" 144Hz 0, 5Ms 24M2n3200nf"),
    ("fumotom/43606", "Ainda ativo! Processador Amd Ryzen 7 9800X3d, 8-Core, 16-Threads"),
    ("SamuelF3lipePromo/124721", "-Ainda ativo, bom preço em 12x sem juros"),
    ("SamuelF3lipePromo/124514", "-Bom preço parcelado"),
    ("SamuelF3lipePromo", "🔥 AINDA ATIVO!!  Exclusivo Prime: Ganhe 40% off em compras"),
    ("promotom/112788", "Bom preço! Britânia Panificadora, Multipane, 550W, Branco"),
    ("promotom/112787", "Isso tá mt bom! Fritadeira Air Fryer Britânia 5,5L 1500W"),
    ("promotom/112792", "Pegara esse? Óculos de Sol Ray-Ban Aviator Unissex"),
    ("promotom", "Preção! 3x Creatina Monohidratada Pura 500g Dark Lab Unidade"),
    ("SamuelF3lipePromo", "🔥 Vai esgotar!"),
    ("SamuelF3lipePromo", "PRECINHO!!!!"),
    ("fadapromos", "Precinho!"),
    ("fadapromos", "Só precinho!"),
    ("fadapromos", "Barato!"),
    ("fadapromos", "Ótimo preço!"),
    ("fadapromos", "R$ 5,60 cada, ótimo preço!"),
    ("nerdofertas", "Preção!! Já comba com o 5700x"),
    ("nerdofertas", "🚨 PREÇÃO MEIA NOITE! Sofá 3 Lugares Retrátil Reclinável"),
    ("nerdofertas", "APROVEITEM! PRECINHO!"),
    ("nerdofertas", "CORRE, CADIVEU NO PRECINHO!"),
    ("nerdofertas", "🚨 CORRE QUE VAI ESGOTAR ! CUPOM SHOPEE EXCLUSIVO"),
    ("nerdofertas", "TÃO DANDO! 512 LITROS BRASTEMP INVERTER SEM JUROS"),
    ("nerdofertas", "Aproveita! 😱"),
    ("nerdofertas", "🚨 Esgotando! Cupom Shopee Tecnologia"),
    ("nerdofertas", "🚨 Ainda Ativos! Cupons Amazon"),
    ("nerdofertas", "Precinho Parcelado!"),
    # palavras do dono (03/11)
    ("dono", "Preço muito bom! Placa de Vídeo RTX 5060"),
    ("dono", "Abaixou o preço: Monitor Gamer LG 27"),
    ("dono", "Baixou! Water Cooler Kalkan 360mm"),
    ("dono", "Caiu o preço! Processador Ryzen 5 5600"),
    ("SamuelF3lipePromo", "-Ainda ativo, cupom FORTE"),
    ("nerdofertas/148633", "40% OFF AMAZON APROVEITAA"),
    ("nerdofertas", "PREÇÃOOOO!!"),
    ("dono", "Menor preço! SSD Kingston NV3 1TB"),
    ("dono", "Últimas unidades — Fonte XPG 850W"),
]

NAO = [
    "Esponja Bombril Multiuso 3 unidades",
    "Correia Dentada Gates para Fiat",
    "Tênis de Corrida Olympikus Corre 3",
    "Baixo Elétrico Fender Jazz Bass",
    "Top Mizuno Compress Feminino",
    "Placa de Vídeo RTX 5060 Ti 16GB Gigabyte",
    "Monitor Gamer AOC Agon G42 27 200Hz",
    "TODOSITE2810 Esgotado! ❌",
    "Cupom 40% esgotado!",
    "Promoção Monitor Gamer LG 27",
    "Oferta Placa-Mãe Asus B550M",
    "Boa Noite Cinderela Livro",
    "Bom Gosto Doce de Leite 400g",
    "Armadilha Mata Barata SBP 6 unidades",
    "Inseticida Baygon Barata e Formiga",
    "Esteira Corre Fácil Dobrável",
    "Caiu Perdeu Pro 45 Pçs",
    "🔥 Ar Split Inverter 12000 Midea\n✅ R$ 1596\n🎟 Cupom: APROVEITA",
    "-CUPOM: PRECINHO10",
]


def test_01_frases_reais_sao_destaque(r):
    faltam = [(o, t) for o, t in REAIS if not frase_de_destaque(t)]
    r.check(faltam == [], "01.reais", "\n".join(map(str, faltam)))


def test_02_parecidos_nao_sao_destaque(r):
    pegos = [(t, frase_de_destaque(t)) for t in NAO if frase_de_destaque(t)]
    r.check(pegos == [], "02.parecidos", str(pegos))


def test_03_retorno_e_da_reativacao(r):
    for t in ("Voltando! Carrinho Romano Galzerano", "Voltou! Redragon Water Cooler",
              "Estoque voltou: Monitor Gamer LG", "Está de volta! Placa de vídeo"):
        r.check(frase_de_destaque(t) == "" and eh_reativacao(t), f"03.{t[:20]}")


def test_04_puro(r):
    arv = ast.parse(ler_fonte("pipeline/destaque.py"))
    imps = set()
    for n in ast.walk(arv):
        if isinstance(n, ast.Import):
            imps |= {a.name.split(".")[0] for a in n.names}
        elif isinstance(n, ast.ImportFrom):
            imps.add((n.module or "").split(".")[0])
    r.check(imps <= {"__future__", "re", "unicodedata"}, "04.so_stdlib", str(imps))
    r.check(frase_de_destaque("") == "" and frase_de_destaque(None) == "", "04.vazio")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "DESTAQUE · frase que chama atenção para o preço"))
