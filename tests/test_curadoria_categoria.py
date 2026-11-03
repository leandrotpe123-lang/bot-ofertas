"""
CURADORIA — CATEGORIA VIGIADA (pipeline.curadoria_categoria), contra
títulos REAIS e contra produtos PARECIDOS que não podem ser segurados.

Decisão do Léo (03/11/2026): placa de vídeo, fonte, cooler/fans, monitor
GAMER, processador, placa-mãe, memória, SSD e gabinete só passam com aval
(Fada Promoções ou Nerd Ofertas). Notebook e PC gamer passam normal. Os
outros monitores passam normal. "Tem que pegar a categoria certa e não
pegar outras categorias de produto."

  01  os 1.575 títulos reais de tests/dados/curadoria_titulos_reais.tsv
      (prévia pública dos canais, 26/10 → 03/11), um a um: 956 vigiados
      nas 9 categorias, 619 que passam — cada linha conferida à mão
  02  as 9 categorias aparecem no conjunto real, e cada uma com volume
  03  PARECIDOS que passam: monitor comum, monitor que não é tela, fonte
      de notebook/LED/água, cooler de praia, ventilador, SSD externo,
      gabinete de banheiro, processador de alimentos, cartão de memória,
      celular/TV/tablet que citam processador ou memória, notebook e PC
      gamer com RTX, periférico gamer, acessório de PC
  04  vigiados escritos de outro jeito: sem a palavra do produto (RTX,
      Ryzen, DDR4, NVMe, 80 Plus, Odyssey), em maiúsculas, com prefixo da
      fonte ("Voltando!", "➡️", "🔥"), chipset com soquete
  05  o título: cabeçalho na 1ª linha e produto na 2ª; só as 3 primeiras
      linhas contam; linha só de link é pulada; texto vazio/None
  06  o substantivo que vem PRIMEIRO decide (o resto do título é descrição)
  07  pureza: sem import de I/O, de banco, de Telegram ou de log

Execução (standalone, stdlib):  python3 tests/test_curadoria_categoria.py
"""
from __future__ import annotations

import ast
import collections
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from _harness_e5 import preparar, rodar, RAIZ, ler_fonte  # noqa: E402

preparar()

from pipeline import curadoria_categoria as cc  # noqa: E402
from pipeline.curadoria_categoria import categoria, titulo  # noqa: E402

_DADOS = os.path.join(RAIZ, "tests", "dados", "curadoria_titulos_reais.tsv")


def _reais():
    linhas = []
    with open(_DADOS, encoding="utf-8") as f:
        for n, linha in enumerate(f, 1):
            linha = linha.rstrip("\n")
            if not linha or linha.startswith("#"):
                continue
            cat, origem, texto = linha.split("\t")
            linhas.append((n, cat, origem, texto))
    return linhas


def test_01_titulos_reais_um_a_um(r):
    reais = _reais()
    r.check(len(reais) == 1575, "01.tamanho_do_conjunto", str(len(reais)))
    errados = [(n, origem, cat, categoria(texto), texto[:70])
               for n, cat, origem, texto in reais if categoria(texto) != cat]
    r.check(errados == [], "01.cada_titulo_real", "\n".join(map(str, errados[:20])))
    vig = sum(1 for _, cat, _, _ in reais if cat)
    r.check(vig == 956 and len(reais) - vig == 619, "01.vigiados_e_livres",
            f"vigiados={vig} livres={len(reais) - vig}")


def test_02_as_nove_categorias_no_real(r):
    cont = collections.Counter(cat for _, cat, _, _ in _reais() if cat)
    r.check(set(cont) == cc.CATEGORIAS, "02.todas_as_categorias",
            str(sorted(set(cont) ^ cc.CATEGORIAS)))
    r.check(all(v >= 50 for v in cont.values()), "02.volume_por_categoria", str(cont))
    origens = {o.split("/")[0] for _, cat, o, _ in _reais() if cat}
    r.check({"promotom", "fumotom", "SamuelF3lipePromo", "nerdofertas",
             "fadapromos"} <= origens, "02.todas_as_fontes", str(origens))


# Produtos PARECIDOS que precisam passar normal (escritos à mão, como o
# dono pediu: "coloca o nome de outros produtos parecidos").
PASSAM = [
    # monitor que não é gamer (decisão do dono: os outros monitores passam)
    'Monitor LG 24MR400 24" IPS Full HD 100Hz FreeSync',
    "Monitor Dell 27 P2725H Full HD 100Hz IPS",
    "Monitor Samsung ViewFinity S6 27 QHD 100Hz",
    "Monitor Samsung 32 UHD 60Hz FreeSync Game Mode",
    "Monitor Philips 23,8 75Hz Full HD VGA HDMI",
    "Monitor Portátil 15.6 Full HD USB-C",
    "Smart Monitor Samsung M5 32 Full HD Tizen",
    "Monitor AOC 24B30HM2 100Hz 1ms",
    # "monitor" que não é tela
    "Monitor de Pressão Arterial de Braço Omron HEM-7156",
    "Monitor Cardíaco Polar H10",
    "Babá Eletrônica com Monitor de Vídeo 4.3",
    "Monitor de Áudio Edifier MR4 Par",
    "Monitor de Energia Smart Wi-Fi Tuya",
    "Monitor de Glicose FreeStyle Libre 2",
    "Fone In-Ear Monitor KZ ZSN Pro",
    "Suporte Articulado para Monitor 17 a 32 Pistão a Gás",
    "Braço Articulado para 2 Monitores Gamer",
    "Lightbar para Monitor Gamer RGB",
    # fonte que não é de PC
    "Fonte de Alimentação para Notebook Dell 65W",
    "Fonte Carregador Universal Notebook 90W",
    "Fonte Chaveada 12V 30A 360W Bivolt",
    "Fonte para Fita LED 12V 5A",
    "Fonte de Água para Gatos Inox 2L com Filtro",
    "Fonte de Bancada Digital 30V 10A",
    "Fonte para Câmera CFTV 12V 2A",
    "Fonte USB 5V 2A Bivolt",
    # cooler/fan que não é de PC
    "Caixa Térmica Cooler 32 Litros Coleman",
    "Cooler Térmico 24 Latas com Alça",
    "Bolsa Térmica Cooler 12L",
    "Cooler Coleman 28 QT Azul",
    "Cooler para Notebook Base com 5 Fans RGB",
    "Base Cooler para Notebook 17 Polegadas",
    "Cooler para Celular Gamer Magnético",
    "Mini Cooler Portátil USB de Mesa",
    "Ventilador de Coluna Arno 40cm 8 Pás",
    "Ventilador de Mesa Mondial 30cm",
    "Climatizador de Ar Air Cooler 3,5L",
    "Bebedouro Water Cooler para Galão",
    "Cooler Externo para PS5 Slim",
    "Samsung Galaxy S23 FE (Fan Edition) 256GB",
    # SSD que não é de PC / acessório de SSD
    "SSD Externo Portátil Samsung T7 1TB USB 3.2",
    "SanDisk Extreme Portable SSD 2TB",
    "Case Gaveta para SSD M.2 NVMe USB-C",
    "Adaptador M.2 NVMe para PCIe x4",
    "Dissipador para SSD M.2 2280",
    "HD Externo Seagate 2TB USB 3.0",
    "Console PlayStation 5 Slim SSD 1TB",
    # gabinete que não é de PC
    "Gabinete para Banheiro com Cuba 60cm",
    "Gabinete de Cozinha Aéreo 120cm 3 Portas",
    "Gabinete para HD Externo 2.5 USB 3.0",
    # processador/memória que não são de PC
    "Processador de Alimentos Philips Walita 750W",
    "Multiprocessador Oster 3 em 1",
    "Cartão de Memória SanDisk Extreme 128GB microSDXC",
    "Pendrive Kingston 64GB USB 3.2",
    "Travesseiro Memória Viscoelástico Nasa",
    "Smartphone Samsung Galaxy A55 256GB 8GB RAM",
    "Celular Motorola Moto G85 5G 256GB Processador Snapdragon",
    "Tablet Samsung Galaxy Tab A9 64GB",
    "Smart TV LG 55 4K Processador α7 Ger8",
    # notebook e PC gamer passam normal (decisão do dono)
    "Notebook Gamer Acer Nitro V15 RTX 4050 i5-13420H 8GB",
    "Notebook Lenovo IdeaPad Slim 3 Ryzen 7 8GB 256GB SSD",
    "Lenovo IdeaPad Slim 3 Ryzen 5 7520U 8GB 512GB",
    "Notebook Asus TUF Gaming F16 RTX 3050 144Hz",
    "Dell G15 RTX 4060 i7 16GB 512GB 165Hz",
    "PC Gamer Completo Ryzen 5 5600 RX 7600 16GB SSD 1TB",
    "Computador Gamer Intel Core i5 RTX 3060",
    "CPU Gamer Completa i7 16GB SSD 480GB",
    "Mini PC Intel N100 16GB 512GB",
    # periférico gamer e acessório de PC
    "Kit Teclado e Mouse Gamer Redragon",
    "Headset Gamer HyperX Cloud Stinger 2",
    "Cadeira Gamer ThunderX3 TGC12",
    "Mouse Gamer Logitech G203",
    "Controle Xbox Series sem Fio",
    "Placa de Som Creative Sound Blaster",
    "Placa de Rede Wi-Fi PCIe TP-Link",
    "Placa de Captura Elgato HD60 X",
    "Cabo Extensor PCIe para Placa de Vídeo",
    "Suporte para Placa de Vídeo ARGB",
    "Pasta Térmica Arctic MX-4 4g",
    "Tela para gabinete Turzx 5.2 Polegadas Ips",
    "Sony Cyber-shot RX100 VII",
    "Cupom KaBuM em Placas de Vídeo selecionadas - 10% OFF",
    "Ar Condicionado Split 12000 BTUs Inverter",
    "Air Fryer Mondial 4L",
]

# Vigiados escritos de outro jeito.
SEGURA = [
    ("Placa de Vídeo RTX 5060 Ti 16GB Gigabyte", "placa_de_video"),
    ("Gigabyte GeForce RTX 5070 Windforce OC 12G", "placa_de_video"),
    ("ASUS Dual Radeon RX 7600 8GB GDDR6", "placa_de_video"),
    ("Intel Arc B580 12GB GDDR6", "placa_de_video"),
    ("PLACA DE VIDEO PNY RTX 5050 8GB", "placa_de_video"),
    ("Voltando! Placa de Vídeo MSI RTX 5060 8G", "placa_de_video"),
    ("Corsair RM850e 850W 80 Plus Gold", "fonte"),
    ("Fonte ATX 600W Real Bivolt", "fonte"),
    ("Fonte Corsair CX650 650W 80 Plus Bronze", "fonte"),
    ("Fonte XPG Core Reactor II 850W ATX 3.1 12V-2x6", "fonte"),
    ("Cooler Master Hyper 212 Black Edition", "cooler"),
    ("Water Cooler Corsair H100i 240mm", "cooler"),
    ("Air Cooler DeepCool AK400", "cooler"),
    ("Kit 3 Fans ARGB 120mm", "cooler"),
    ("Ventoinha Corsair 140mm", "cooler"),
    ("Monitor Gamer LG UltraGear 27 180Hz", "monitor_gamer"),
    ("Monitor Samsung Odyssey G5 27 QHD", "monitor_gamer"),
    ("Monitor AOC 24G2 144Hz IPS", "monitor_gamer"),
    ("Monitor Dell Alienware AW2725DF 360Hz", "monitor_gamer"),
    ("Monitor Asus TUF VG249Q1A", "monitor_gamer"),
    ("Monitor Philips 27 0,5ms Gaming", "monitor_gamer"),
    ("Samsung Odyssey G5 2.7 Qhd 180 Hz 0,03 ms G-sync Freesync", "monitor_gamer"),
    ("Processador AMD Ryzen 5 5600", "processador"),
    ("AMD Ryzen 7 5700X3D", "processador"),
    ("Intel Core i5-12400F", "processador"),
    ("Kit Upgrade Ryzen 5 5600 + B550M + 16GB", "processador"),
    ("Placa-Mãe ASUS TUF Gaming B550M-Plus", "placa_mae"),
    ("Gigabyte B650M Aorus Elite AX AM5 DDR5", "placa_mae"),
    ("Memória RAM Kingston Fury Beast 16GB DDR4 3200MHz", "memoria"),
    ("Kingston Fury Beast 16GB DDR5 5600MHz", "memoria"),
    ("Memória RAM para Notebook 8GB DDR4 SODIMM", "memoria"),
    ("SSD Kingston NV3 1TB NVMe M.2", "ssd"),
    ("WD Black SN850X 1TB NVMe", "ssd"),
    ("SSD 1TB para PS5 NVMe Gen4 com dissipador", "ssd"),
    ("Gabinete Gamer Mancer Goblin Mid Tower", "gabinete"),
    ("➡️ Gabinete Gamer Rise Mode Glass 06x", "gabinete"),
    ("🔥 Fonte Gamer Rise Mode Zeus, 500W, PFC Ativo", "fonte"),
]


def test_03_parecidos_passam(r):
    pegos = [(t, categoria(t)) for t in PASSAM if categoria(t)]
    r.check(pegos == [], "03.parecidos_nao_sao_vigiados", "\n".join(map(str, pegos)))
    r.check(len(PASSAM) >= 80, "03.tamanho", str(len(PASSAM)))


def test_04_vigiados_de_outro_jeito(r):
    errados = [(t, esp, categoria(t)) for t, esp in SEGURA if categoria(t) != esp]
    r.check(errados == [], "04.vigiados", "\n".join(map(str, errados)))
    r.check({esp for _, esp in SEGURA} == cc.CATEGORIAS, "04.cobre_as_nove")


def test_05_qual_linha_e_o_titulo(r):
    r.check(categoria("40% OFF AMAZON APROVEITAA\n➡️ Monitor Gamer AOC AGON G42 27\" "
                      "200Hz") == "monitor_gamer", "05.cabecalho_e_produto_embaixo")
    r.check(categoria("-Ainda ativo, bom preço em 12x sem juros\n🔥 Processador Amd Am4 "
                      "Ryzen 7 5700x Novo") == "processador", "05.aviso_e_produto_embaixo")
    r.check(categoria("WD-40 Fórmula Original\n\nR$ 29\n-Anúncio\n\nhttps://x.y/z\n\n"
                      "Placa de vídeo RTX 5060") == "", "05.so_as_tres_primeiras_linhas")
    r.check(categoria("WD-40 Fórmula Original\n\nR$ 29\n\nhttps://x.y/z\n\nPlaca de "
                      "vídeo RTX 5060") == "placa_de_video", "05.link_nao_conta_como_linha")
    r.check(categoria("https://link.amazon/B0x\nPlaca de Vídeo RTX 5060") == "placa_de_video",
            "05.linha_so_de_link_e_pulada")
    r.check(categoria("Cupons Kabum a ativar\nPlacas-Mãe Selecionadas - 10% OFF") == "",
            "05.cupom_na_primeira_linha")
    r.check(categoria("") == "" and categoria(None) == "" and titulo(None) == "",
            "05.vazio_e_none")
    r.check(titulo("Bom preço!\nMonitor Gamer LG 27 180Hz\nR$ 899")
            == "Monitor Gamer LG 27 180Hz", "05.titulo_devolvido")


def test_06_o_primeiro_substantivo_decide(r):
    casos = [
        ("Gabinete Gamer Hayom, Sem Fonte, Com 3 Fans", "gabinete"),
        ("Processador Ryzen 7 5700x (Sem Caixa ou Cooler)", "processador"),
        ("Placa de Vídeo RTX 3060 Dual Fan", "placa_de_video"),
        ("750w Fonte Cooler Master Mwe Gold 750 V3", "fonte"),
        ("Placa-Mãe B550M + Processador Ryzen 5 + Water Cooler", "placa_mae"),
        ("Notebook Gamer com Placa de Vídeo RTX 4050", ""),
        ("Suporte de Monitor Gamer 144Hz", ""),
        ("Câmera de segurança Wi-Fi, visão noturna, monitor", ""),
    ]
    errados = [(t, e, categoria(t)) for t, e in casos if categoria(t) != e]
    r.check(errados == [], "06.substantivo", str(errados))


def test_07_modulo_puro(r):
    arvore = ast.parse(ler_fonte("pipeline/curadoria_categoria.py"))
    importados = set()
    for n in ast.walk(arvore):
        if isinstance(n, ast.Import):
            importados.update(a.name.split(".")[0] for a in n.names)
        elif isinstance(n, ast.ImportFrom):
            importados.add((n.module or "").split(".")[0])
    r.check(importados <= {"__future__", "re", "unicodedata", "typing"},
            "07.so_stdlib_pura", str(importados))
    r.check(all(not isinstance(n, (ast.Await, ast.AsyncFunctionDef))
                for n in ast.walk(arvore)), "07.sem_async")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CURADORIA · categoria vigiada (títulos reais)"))
