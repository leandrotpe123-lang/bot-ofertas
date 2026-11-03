"""
Curadoria — CATEGORIA VIGIADA do produto (módulo PURO).

Responsabilidade ÚNICA: dizer se o produto de um post pertence a uma das
categorias de hardware que só passam com aval de um canal curador
(pipeline.curadoria). Devolve o rótulo da categoria, ou "" quando o post
não é de categoria vigiada.

Puro e determinístico: sem I/O, sem log, sem banco, sem Telegram.

═══════════════════════════════════════════════════════════════════
POR QUE EXISTE — medido nas prévias públicas, 27/10 → 03/11/2026
═══════════════════════════════════════════════════════════════════
Promotom, Fumotom e Samuel Felipe despejam hardware de PC no canal
(~53 posts/dia, 20% do @fullpromotion), quase sempre em vitrine e sem
preço especial. O Fada Promoções e o Nerd Ofertas só soltam hardware
quando o preço vale. Decisão do Léo (03/11): placa de vídeo, fonte,
cooler/fans, monitor GAMER, processador, placa-mãe, memória, SSD e
gabinete só passam com aval; notebook e PC gamer passam normal; os
outros monitores passam normal.

═══════════════════════════════════════════════════════════════════
COMO DECIDE — o SUBSTANTIVO PRINCIPAL do título
═══════════════════════════════════════════════════════════════════
Palavra solta engana: "Fonte de Água para Gatos", "Suporte para
Monitor", "Notebook … RTX 4050", "Processador … (sem Cooler)",
"Gabinete … com 3 Fans", "Caixa Térmica Cooler". Por isso a regra é:

  1. o título é a primeira das 3 primeiras linhas que nomeia um
     produto (há posts com cabeçalho — "40% OFF AMAZON APROVEITAA" — e
     o produto na linha de baixo);
  2. o produto é o substantivo que aparece PRIMEIRO no título, entre
     um vocabulário de produtos VIGIADOS e um de produtos que NÃO são
     (notebook, PC gamer, console, celular, TV, suporte, cabo,
     adaptador, cupom…). Empate na mesma posição: vence o trecho mais
     longo ("processador de alimentos" vence "processador");
  3. a categoria vigiada só vale com a PROVA dela: fonte precisa de
     potência de PC ou selo (80 Plus, PFC, ATX); monitor precisa ser
     gamer (gamer/gaming, linha gamer, ≥ 144 Hz, tempo < 1 ms, G-Sync);
     memória precisa ser RAM; SSD externo/portátil não entra; cooler
     de praia, gabinete de banheiro e processador de alimentos não
     entram.

Na dúvida, NÃO é categoria vigiada: o post segue o fluxo normal. Errar
para o lado de segurar travaria produto que o dono quer no canal.
"""
from __future__ import annotations

import re
import unicodedata
from typing import List, Optional, Tuple

__all__ = ["CATEGORIAS", "categoria", "titulo", "regiao_titulo", "itens_com_preco"]

PLACA_DE_VIDEO = "placa_de_video"
FONTE = "fonte"
COOLER = "cooler"
MONITOR_GAMER = "monitor_gamer"
PROCESSADOR = "processador"
PLACA_MAE = "placa_mae"
MEMORIA = "memoria"
SSD = "ssd"
GABINETE = "gabinete"

CATEGORIAS = frozenset({PLACA_DE_VIDEO, FONTE, COOLER, MONITOR_GAMER,
                        PROCESSADOR, PLACA_MAE, MEMORIA, SSD, GABINETE})

_FORA = "fora"          # substantivo de produto que NÃO é vigiado
_LINHAS_TITULO = 3      # quantas linhas não vazias procurar o título


# ─────────────────────────────────────────────────────────────────
# Normalização do texto (só para LER — nada é devolvido ao post)
# ─────────────────────────────────────────────────────────────────
_SIMBOLOS = str.maketrans({"™": " ", "®": " ", "©": " ", "’": "'", "´": "'",
                           "`": "'", "“": '"', "”": '"', "″": '"'})
# Marcas que contêm substantivo de produto: viram um token só, para
# "Fonte Cooler Master" não virar cooler e "Galaxy S23 FE (Fan Edition)"
# não virar fan.
_MARCAS = (
    (re.compile(r"\bcooler\s*master\b"), "coolermaster"),
    (re.compile(r"\bfan\s+edition\b"), "fanedition"),
)


def _normalizar(t: str) -> str:
    t = (t or "").translate(_SIMBOLOS)
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn").lower()
    t = re.sub(r"\s+", " ", t).strip()
    for rx, troca in _MARCAS:
        t = rx.sub(troca, t)
    return t


# ─────────────────────────────────────────────────────────────────
# Vocabulário de SUBSTANTIVOS de produto
# ─────────────────────────────────────────────────────────────────
# (rótulo, padrão). O rótulo _FORA marca produto que NÃO é vigiado: ele
# só precisa existir quando costuma aparecer ANTES de uma palavra
# vigiada no mesmo título (o notebook que cita a RTX, o suporte que cita
# o monitor, o celular que cita a memória).
_VOCABULARIO: Tuple[Tuple[str, str], ...] = (
    # ── VIGIADOS ──────────────────────────────────────────────────
    (PLACA_DE_VIDEO, r"\bplacas?\s+(?:de\s+)?(?:video|grafica)s?\b"),
    (PLACA_DE_VIDEO, r"\bgpu\b"),
    (PLACA_DE_VIDEO, r"\b(?:geforce\s+)?rtx\s?-?\d{4}(?!\d)"),
    (PLACA_DE_VIDEO, r"\bgtx\s?-?\d{3,4}(?!\d)"),
    # RX das placas AMD (RX 460 em diante); "RX100" de câmera não casa.
    (PLACA_DE_VIDEO, r"\brx\s?-?(?:[4-9]\d{2}|[5-9]\d{3})(?!\d)"),
    (PLACA_DE_VIDEO, r"\bradeon\s+rx\b"),
    (PLACA_DE_VIDEO, r"\barc\s?[ab]\d{3}\b"),
    (FONTE, r"\bfontes?\b"),
    (FONTE, r"\b80\s?plus\b"),
    (FONTE, r"\bcoolermaster\s+(?:mwe|gx\s?\d|v\d{3,4})"),
    (COOLER, r"\b(?:water|air)\s*cool(?:er|ing)s?\b"),
    (COOLER, r"\b(?:watercooler|aircooler)s?\b"),
    (COOLER, r"\bcoolers?\b"),
    (COOLER, r"\bcoolermaster\s+(?:hyper|masterair|masterliquid|ml\d)"),
    (COOLER, r"\bfans?\b"),
    (COOLER, r"\bventoinhas?\b"),
    (MONITOR_GAMER, r"\bmonitor(?:es)?\b"),
    # Linhas que SÓ existem como monitor gamer, para o título sem a palavra
    # "monitor" ("Samsung Odyssey G5 27 180 Hz", "Acer Nitro KG270 144hz").
    (MONITOR_GAMER, r"\bodyssey\s+(?:g\d|oled|neo|ark)\b|\bultragear\b|\bagon\b|\bevnia\b"
                    r"|\bnitro\s+(?:kg|vg|xv|xf|ed)\d|\bpredator\s+(?:xb|x)\d"
                    r"|\balienware\s+aw\d|\brog\s+(?:swift|strix\s+xg)|\btuf\s+(?:gaming\s+)?vg\d"),
    (PROCESSADOR, r"\bprocessador(?:es)?\b"),
    (PROCESSADOR, r"\bryzen\b"),
    (PROCESSADOR, r"\b(?:intel\s+)?core\s?i[3579]\b"),
    (PROCESSADOR, r"\bintel\s+core\b"),
    (PROCESSADOR, r"\bcore\s+ultra\b"),
    (PROCESSADOR, r"\bxeon\b"),
    (PROCESSADOR, r"\bathlon\b"),
    (PLACA_MAE, r"\bplacas?[\s-]*mae\b"),
    (PLACA_MAE, r"\b(?:motherboard|mainboard)s?\b"),
    # Chipset solto (B550M, A520M, H610M, X670, Z790) — só vale com
    # contexto de placa-mãe (conferido em _comprova).
    (PLACA_MAE, r"\b[abhxz][3-9]\d0m?\b"),
    (MEMORIA, r"\bmemorias?\b"),
    (MEMORIA, r"\bddr\s?[345]\b"),
    (SSD, r"\bssds?\b"),
    (SSD, r"\bnvme\b"),
    (SSD, r"\bm\.?2\s+(?:2280|nvme|pcie|sata)\b"),
    (GABINETE, r"\bgabinetes?\b"),

    # ── NÃO VIGIADOS ──────────────────────────────────────────────
    # notebook e PC montado passam normal (decisão do Léo, 03/11)
    (_FORA, r"\b(?:notebooks?|laptops?|chromebooks?|macbooks?|ultrabooks?|netbooks?)\b"),
    (_FORA, r"\b(?:ideapad|thinkpad|thinkbook|vivobook|zenbook|inspiron|vostro|latitude"
            r"|pavilion|victus|avell|vaio|galaxy\s+book|samsung\s+book)\b"),
    (_FORA, r"\baspire\s+(?:go|\d|lite|vero)\b|\bnitro\s+(?:v|v?\d{1,2}|anv)\b"
            r"|\bdell\s+g1[56]\b|\blegion\s+(?:\d|pro|slim|go)\b|\bloq\b"
            r"|\btuf\s+gaming\s+[af]\d{2}\b|\bpredator\s+helios\b|\bomen\s+\d{2}\b"),
    (_FORA, r"\bpc\s+(?:gamer|gaming|completo|montado|desktop|office|home)\b"),
    (_FORA, r"\bcpu\s+(?:gamer|completa|completo|montada|montado)\b"),
    (_FORA, r"\b(?:computador(?:es)?|desktop|workstation|all[\s-]*in[\s-]*one)\b"),
    (_FORA, r"\bmini\s*pc\b"),
    # console e acessório de jogo
    (_FORA, r"\b(?:consoles?|xbox|playstation|ps[2345]|nintendo|steam\s*deck|rog\s+ally)\b"),
    (_FORA, r"\b(?:controles?|joysticks?|gamepads?|volantes?)\b"),
    # celular, tablet, relógio
    (_FORA, r"\b(?:celular(?:es)?|smartphones?|iphones?|ipads?|tablets?|smartwatch\w*"
            r"|relogios?)\b"),
    (_FORA, r"\b(?:redmi|poco\s+[a-z]\d|moto\s+(?:g|e|edge)\s?\d*"
            r"|galaxy\s+(?:tab|watch|buds|[asmz]\d{1,2}|z\s?(?:flip|fold)))\b"),
    # TV, projetor
    (_FORA, r"\b(?:smart\s*)?tvs?\b|\btelevis\w*|\bprojetor\w*|\bchromecast\b"
            r"|\bfire\s*tv\b|\broku\b"),
    # periférico e mobília
    (_FORA, r"\b(?:headsets?|headphones?|fones?|earbuds?|mouses?|mousepads?|teclados?"
            r"|cadeiras?|mesas?|escrivaninhas?|poltronas?)\b"),
    (_FORA, r"\b(?:suportes?|bracos?|base|bases|lightbar|light\s*bar|luminarias?"
            r"|ring\s*light|fita\s+led)\b"),
    (_FORA, r"\btelas?\b"),
    (_FORA, r"\b(?:cameras?|webcams?|microfones?|baba\s+eletronica|caixas?|bolsas?"
            r"|oculos|mochilas?|capas?|capinhas?|peliculas?)\b"),
    # cabo, rede, placa que não é de vídeo, acessório de armazenamento
    (_FORA, r"\b(?:cabos?|adaptador(?:es)?|conversor(?:es)?|extensor(?:es)?|extensao"
            r"|hubs?|docks?|docking|switch|roteador(?:es)?|repetidor(?:es)?|modem)\b"),
    (_FORA, r"\bplacas?\s+(?:de\s+)?(?:som|rede|captura|audio|wi-?fi)\b|\binterfaces?\b"),
    (_FORA, r"\bhds?\s+(?:externo|interno|portatil|\d+\s?(?:tb|gb))\b"),
    (_FORA, r"\b(?:pen\s*drives?|cartao|cartoes|micro\s*sd|leitor(?:es)?|case|gavetas?"
            r"|dissipador(?:es)?|heatsink|pasta\s+termica|backplate|riser"
            r"|controladora)\b"),
    # eletrodoméstico e casa
    (_FORA, r"\b(?:ventilador(?:es)?|climatizador(?:es)?|ar\s+condicionado|bebedouro"
            r"|purificador|geladeira|refrigerador|frigobar|liquidificador|mixer"
            r"|fritadeira|air\s*fryer|micro-?ondas|forno|panela)\b"),
    (_FORA, r"\bprocessador(?:es)?\s+(?:de\s+)?(?:alimentos?|cozinha|legumes|carne)\b"
            r"|\bmultiprocessador\w*"),
    (_FORA, r"\b(?:travesseiro|colchao|espuma)s?\b"),
    # energia
    (_FORA, r"\b(?:nobreak|estabilizador|filtro\s+de\s+linha|carregador(?:es)?"
            r"|power\s*bank|baterias?|impressora)\b"),
    # post de cupom (o portão nem chega a ele: só vale para produto)
    (_FORA, r"\bcupo(?:m|ns)\b"),
)

_PADROES: Tuple[Tuple[str, "re.Pattern[str]"], ...] = tuple(
    (rotulo, re.compile(padrao)) for rotulo, padrao in _VOCABULARIO)


# ─────────────────────────────────────────────────────────────────
# Provas por categoria (sobre o título normalizado)
# ─────────────────────────────────────────────────────────────────
# FONTE de PC: potência típica de PC (300 W a 2999 W) ou selo/forma.
_FONTE_FORTE = re.compile(
    r"\b80\s?plus\b|\bpfc\b|\bmodular\b|\batx\b|\bcybenetics\b|\bpsu\b"
    r"|\bfonte\s+gamer\b|\bfonte\s+de\s+alimentacao\s+(?:atx|pc|gamer)\b"
    r"|\bcoolermaster\s+(?:mwe|gx\s?\d|v\d{3,4})")
_FONTE_WATT = re.compile(r"\b(?:[3-9]\d{2}|[12]\d{3})\s?w(?:atts?)?\b")
_FONTE_NUNCA = re.compile(
    r"\bagua\b|bebedouro|\bpets?\b|\bgatos?\b|\bcaes\b|cachorr|notebook|laptop"
    r"|carregador|celular|smartphone|chaveada|bancada|\bled\b|cftv|\bcameras?\b"
    r"|nobreak|estabilizador|ring\s*light|\btomada\b")
_FONTE_TENSAO = re.compile(r"\b(?:5|9|12|24|48)\s?v\b|\busb\b|universal")

# MONITOR: só o GAMER. Os outros passam normal (decisão do Léo, 03/11).
_MONITOR_NAO_E_TELA = re.compile(
    r"\bmonitor(?:es)?\s+(?:de\s+|da\s+|do\s+|para\s+)?"
    r"(?:pressao|cardiac\w*|frequencia|batimento\w*|glicose|glicemia|bebe\w*|baba"
    r"|energia|consumo|audio|referencia|estudio|retorno|tensao|bateria\w*|pneus?"
    r"|re\b|sinais|sono|fetal|oximetr\w*|temperatura|umidade|ambiente|qualidade"
    r"|co2|rede|multiparametr\w*|pulso|atividade)"
    r"|baba\s+eletronica|in-?ear|oximetro")
_MONITOR_GAMER = re.compile(
    r"\bgamers?\b|\bgaming\b|\bpara\s+jogos\b|\besports?\b|\be-sports?\b"
    r"|\bultragear\b|\bodyssey\b|\bagon\b|\bnitro\b|\bpredator\b|\bevnia\b|\brog\b"
    r"|\btuf\b|\balienware\b|\bomen\b|\blegion\b|\boptix\b|\bmag\b|\bmpg\b|\baorus\b"
    r"|\bg-?sync\b"
    r"|\b(?:14[4-9]|1[5-9]\d|[2-9]\d{2})\s?hz\b"
    r"|\b0[.,]\d{1,2}\s?ms\b")

# PROCESSADOR de cozinha/áudio não é de PC.
_PROCESSADOR_NUNCA = re.compile(
    r"alimentos?|cozinha|legumes|\bde\s+audio\b|\bde\s+voz\b|\bde\s+sinal\b"
    r"|efeitos|guitarra|pedal")

# PLACA-MÃE pelo chipset só com contexto de placa-mãe.
_CHIPSET = re.compile(r"\b[abhxz][3-9]\d0m?\b")
_PLACA_MAE_CONTEXTO = re.compile(
    r"\bam[45]\b|\blga\s?\d{4}\b|\bsocket\b|\bm-?atx\b|\bmicro-?atx\b|\bmini-?itx\b"
    r"|\bitx\b|\batx\b|\bchipset\b|\bddr[345]\b")

# MEMÓRIA tem de ser RAM: cartão, pendrive e espuma "de memória" não são.
_MEMORIA_RAM = re.compile(
    r"\bram\b|\bddr\s?[2345]\b|\b(?:u|so-?)?dimm\b|\bmhz\b|\bcl\d{2}\b")
_MEMORIA_NUNCA = re.compile(
    r"cartao|micro\s*sd|pen\s*drive|\bflash\b|celular|smartphone|espuma"
    r"|travesseiro|colchao|viscoelastic")

# SSD externo/portátil é outra categoria (armazenamento portátil).
_SSD_NUNCA = re.compile(r"\bextern[oa]s?\b|\bportat\w*|\bportable\b")

# GABINETE de banheiro/cozinha e caixa para HD não são de PC.
_GABINETE_NUNCA = re.compile(
    r"banheiro|\bpia\b|cozinha|\bcuba\b|lavatorio|armario|espelh|lavanderia|tanque"
    r"|area\s+de\s+servico|\bparede\b|aereo|para\s+(?:hd|ssd)|\bdisco\b")

# COOLER/FAN de PC: caixa térmica, cooler de notebook/celular/console e
# ventilador de ambiente não são.
_COOLER_NUNCA = re.compile(
    r"termic|\blatas?\b|\b\d+(?:[.,]\d+)?\s?(?:l|litros?|qt)\b|\bbolsas?\b|praia"
    r"|camping|cerveja|\bgelo\b|isopor|garrafa|notebook|laptop|celular|smartphone"
    r"|\bps[45]\b|xbox|console|climatiz|bebedouro|galao|fanedition")
# Cooler de PC COMPROVADO (water/air cooler, AIO, soquete, ARGB, medida de
# fan): aí "ventiladores PWM" é peça dele, não ventilador de ambiente.
_COOLER_DE_PC = re.compile(
    r"\b(?:water|air)\s*cool|\b(?:watercooler|aircooler)\b|\baio\b|\bcpu\b"
    r"|processador|\bintel\b|\bamd\b|\blga\b|\bam[45]\b|heatpipe|\ba?rgb\b|\bpwm\b"
    r"|\btdp\b|\b\d?x?(?:80|92|120|140|200|240|280|360|420)\s?mm\b")
_COOLER_AMBIENTE = re.compile(
    r"ventilador|\bteto\b|\bcoluna\b|\bportat\w*|\busb\b|\bmesa\b")


def _comprova(cat: str, t: str) -> bool:
    """A categoria vigiada só vale com a prova dela no título."""
    if cat == FONTE:
        if _FONTE_NUNCA.search(t):
            return False
        if _FONTE_FORTE.search(t):
            return True
        return bool(_FONTE_WATT.search(t)) and not _FONTE_TENSAO.search(t)
    if cat == MONITOR_GAMER:
        return not _MONITOR_NAO_E_TELA.search(t) and bool(_MONITOR_GAMER.search(t))
    if cat == PROCESSADOR:
        return not _PROCESSADOR_NUNCA.search(t)
    if cat == MEMORIA:
        return bool(_MEMORIA_RAM.search(t)) and not _MEMORIA_NUNCA.search(t)
    if cat == SSD:
        return not _SSD_NUNCA.search(t)
    if cat == GABINETE:
        return not _GABINETE_NUNCA.search(t)
    if cat == COOLER:
        if _COOLER_NUNCA.search(t):
            return False
        return bool(_COOLER_DE_PC.search(t)) or not _COOLER_AMBIENTE.search(t)
    return True


def _substantivo(t: str) -> Optional[Tuple[str, int, int]]:
    """(rótulo, início, fim) do substantivo de produto que vem PRIMEIRO no
    título. Empate na posição: o trecho mais longo. None: nenhum."""
    melhor: Optional[Tuple[str, int, int]] = None
    for rotulo, rx in _PADROES:
        m = rx.search(t)
        if m is None:
            continue
        if rotulo == PLACA_MAE and _CHIPSET.fullmatch(m.group(0)):
            if not _PLACA_MAE_CONTEXTO.search(t):
                continue
        cand = (rotulo, m.start(), m.end())
        if (melhor is None or cand[1] < melhor[1]
                or (cand[1] == melhor[1] and cand[2] > melhor[2])):
            melhor = cand
    return melhor


def _linhas(texto: str) -> List[str]:
    saida = []
    for linha in (texto or "").splitlines():
        linha = linha.strip()
        if not linha or re.fullmatch(r"\W*https?://\S+", linha):
            continue
        saida.append(linha)
        if len(saida) == _LINHAS_TITULO:
            break
    return saida


def titulo(texto: str) -> str:
    """A linha-título do post: a primeira das 3 primeiras linhas não
    vazias que nomeia um produto (vigiado ou não). "" se nenhuma nomeia."""
    for linha in _linhas(texto):
        if _substantivo(_normalizar(linha)) is not None:
            return linha
    return ""


def regiao_titulo(texto: str) -> str:
    """O TÍTULO como o leitor o vê: as linhas do começo do post até a
    linha-título, inclusive — o cabeçalho que a fonte põe acima do produto
    ("-Ainda ativo, bom preço em 12x sem juros") faz parte dele. "" se o
    post não nomeia produto nas 3 primeiras linhas."""
    linhas = _linhas(texto)
    for i, linha in enumerate(linhas):
        if _substantivo(_normalizar(linha)) is not None:
            return "\n".join(linhas[:i + 1])
    return ""


# ITEM com preço próprio: linha com "R$ <número>" que não é valor de cupom,
# forma de pagamento nem moeda/cashback ("R$ 50 OFF em R$ 249", "R$ 1289 -
# Sem Juros", "R$ 549 - PIX", "em 10x"). O MESMO produto em duas formas de
# pagamento não vira dois itens.
_PRECO = re.compile(r"r\$\s?\d")
_NAO_ITEM = re.compile(
    r"\boff\b|cupo|desconto|cashback|moedas|frete|\bparcel|\bem\s+\d+\s?x\b"
    r"|\d+\s?x\s+sem|sem\s+juros|\bpix\b")


def itens_com_preco(texto: str) -> int:
    """Quantas linhas do post são ITEM com preço próprio. Junto com a
    contagem de produtos identificados, é o que separa LISTA ("Placas Mães
    B550M em Oferta: Gigabyte R$ 556 / Asus R$ 588") de um produto só com
    dois links (dois anúncios, "Resgate cupons" + "Link produto")."""
    n = 0
    for linha in (texto or "").splitlines():
        t = _normalizar(linha)
        if _PRECO.search(t) and not _NAO_ITEM.search(t):
            n += 1
    return n


def categoria(texto: str) -> str:
    """Categoria vigiada do produto do post, ou "" se não for vigiada.

    Recebe o texto do post (só a linha-título importa). Puro."""
    linha = titulo(texto)
    if not linha:
        return ""
    t = _normalizar(linha)
    achado = _substantivo(t)
    if achado is None or achado[0] == _FORA:
        return ""
    return achado[0] if _comprova(achado[0], t) else ""
