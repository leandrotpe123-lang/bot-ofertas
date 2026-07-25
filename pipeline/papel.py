"""
Pipeline — Papel semântico da linha.

Responsabilidade ÚNICA: dizer O QUE uma linha é dentro do post.
Devolve um PAPEL. Nunca devolve emoji, nunca devolve texto marcado,
nunca desenha nada.

Separação deliberada:
    papel.py     → o que a linha É        (semântica)
    montagem.py  → como a linha aparece   (paleta PAPEL → emoji)
    marcacao.py  → dialeto do Telegram    (negrito, código, escape)

Trocar um emoji da paleta não toca este módulo. Corrigir uma
classificação errada não toca a paleta. É essa fronteira que impede
o falso positivo de virar mudança visual acidental.

═══════════════════════════════════════════════════════════════════
INVARIANTES (contrato)
═══════════════════════════════════════════════════════════════════
I-P1  Camada pura. Sem I/O, sem estado, sem log, sem rede.

I-P2  A ORDEM das regras é a regra. Um mesmo texto casa mais de um
      vocabulário; quem decide é a precedência declarada em
      `classificar`. Reordenar é mudança de comportamento.

I-P3  CUPOM vence RESGATE. Provado por corpus: "Resgate todos os
      cupons desta página" é cupom; "Resgate aqui" é resgate. A
      presença de âncora de cupom decide, não o verbo.

I-P7  Ponteiro de resgate não repete cupom. "Resgate o cupom aqui:"
      aponta para um cupom já declarado — é RESGATE. Só quando a linha
      é ponteiro "aqui" E não traz benefício próprio; com benefício
      ("20% OFF") ou sem "aqui" ("do anúncio"), permanece CUPOM.

I-P4  Âncora forte, nunca verbo solto. Vocabulário só admite termo
      que identifique o papel sozinho. Verbo genérico ("acesse",
      "clique", "ative") classifica descrição livre como se fosse
      estrutura — é a origem histórica dos falsos positivos.

I-P5  Em dúvida, NENHUM. Linha não classificada não recebe emoji e
      não recebe negrito. Ausência é reversível; marca errada não.

I-P6  Não reconhece cupom. Vocabulário de identificação de código é
      soberania de utils.cupom. Aqui só existe o vocabulário que
      distingue o PAPEL da linha.
"""
from __future__ import annotations

import re
from typing import Optional

__all__ = [
    "TITULO", "PRECO", "CUPOM", "RESGATE", "LINK", "VARIANTE",
    "CARRINHO", "FRETE", "PARCELA", "AVISO", "GATILHO",
    "classificar",
]


# ─────────────────────────────────────────────────────────────────
# PAPÉIS
# ─────────────────────────────────────────────────────────────────
TITULO   = "titulo"
PRECO    = "preco"
CUPOM    = "cupom"
RESGATE  = "resgate"
LINK     = "link"
VARIANTE = "variante"
CARRINHO = "carrinho"
FRETE    = "frete"
PARCELA  = "parcela"
AVISO    = "aviso"
GATILHO  = "gatilho"


# ─────────────────────────────────────────────────────────────────
# VOCABULÁRIO
# ─────────────────────────────────────────────────────────────────

# Linha monetária: começa com valor.
_RE_PRECO = re.compile(r'^\s*r\$\s*[\d.,]+', re.I)

# Âncora FORTE de cupom: identifica o papel sozinha.
_RE_CUPOM_FORTE = re.compile(
    r'\b('
    r'cupom|'
    r'cupons|'
    r'c[oó]digo|'
    r'cashback|'
    r'\d+\s*%\s*off|'
    r'r\$\s*[\d.,]+\s*off|'
    r'off\s+em\s+r\$|'
    r'off\s+acima\s+de|'
    r'off\s+sem\s+m[ií]nimo|'
    r'limite\s*r\$'
    r')\b',
    re.I,
)

# Âncora FRACA: só classifica quando a linha não é monetária.
# "R$ 15 - Desconto na Finalização" é PREÇO, não cupom.
_RE_CUPOM_FRACA = re.compile(r'\bdesconto\b', re.I)

# Benefício próprio: percentual/valor de desconto declarado NA linha.
# É o que distingue uma linha que DECLARA um cupom de uma que só
# aponta para onde resgatá-lo.
_RE_BENEFICIO = re.compile(
    r'('
    r'\d+\s*%\s*off|'
    r'r\$\s*[\d.,]+\s*off|'
    r'off\s+em\s+r\$|'
    r'off\s+acima|'
    r'off\s+sem\s+m[ií]nimo|'
    r'limite\s*r\$'
    r')',
    re.I,
)

# Ponteiro de resgate "aqui": "Resgate ... aqui".
_RE_RESGATE_APONTA = re.compile(r'\bresgat\w*\b.*\baqui\b', re.I)

# Início estrutural: a linha COMEÇA com uma declaração de cupom
# (Cupom:, código, benefício "X% OFF"/"R$ Y OFF") ou com uma ação
# (Resgate...). Frase descritiva que apenas MENCIONA cupom no meio
# ("Shopee está dando cupom de 15% OFF...") começa com um sujeito e
# NÃO casa — deixa de ser cupom e vira texto livre (sem emoji/negrito,
# traço da origem preservado), como no canal de referência.
_RE_INICIO_ESTRUTURAL = re.compile(
    r'^[\s\-–—•·]*('
    r'cupom|cupons|c[oó]digo|voucher|cashback|'
    r'resgat\w*|'
    r'\d+\s*%\s*off|r\$\s*[\d.,]+\s*off|'
    r'ganhe\b'
    r')',
    re.I,
)

_RE_PARCELA = re.compile(r'\b\d+\s*x\s*sem\s+juros\b', re.I)

_RE_FRETE = re.compile(
    r'\b(frete\s+gr[aá]t(?:is)?|entrega\s+gr[aá]tis|sem\s+frete|frete\s+0)\b',
    re.I,
)

# Carrinho é RÓTULO, não palavra. "Carrinho: https://..." é carrinho.
# "Carrinho de Bebê Galzerano - R$ 199" é produto.
_RE_CARRINHO = re.compile(r'^\s*carrinho\s*:?\s*(?:https?://|$)', re.I)

_RE_RESGATE = re.compile(
    r'\b(resgate|resgatar|lista|teste\s+aqui|pegue\s+aqui|use\s+o\s+cupom)\b',
    re.I,
)

_RE_LINK = re.compile(
    r'\b(link\s+(?:produto|oferta|do\s+produto|da\s+oferta)|veja\s+aqui)\b',
    re.I,
)

_RE_AVISO = re.compile(r'^\s*(importante|aten[çc][ãa]o|aviso)\b', re.I)

# Variante de um mesmo produto. Separadores aceitos entre rótulo e
# preço: "-", "–", "=", ":". A âncora r\$ após o separador impede
# linha estrutural com ":" (Cupom:, Resgate aqui:) de casar.
_RE_VARIANTE = re.compile(
    r'^(\d+\/\d+\s*gb|\d+\/\d+|[a-zà-ÿ0-9\s\-\+]{2,50}\s*[-–=:]\s*r\$)',
    re.I,
)

_RE_URL = re.compile(r'https?://')

# Vocabulário de emoji da ORIGEM — usado só para detectar que o
# divulgador já marcou o título ele mesmo (ver GATILHO).
_RE_EMOJI = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF"
    r"\U0001F900-\U0001F9FF\u2B50\u2B55]"
)


# ─────────────────────────────────────────────────────────────────
# CLASSIFICAÇÃO
# ─────────────────────────────────────────────────────────────────
def classificar(
    linha: str,
    eh_titulo: bool,
    is_multi: bool = False,
    proxima_linha: str = "",
    proxima_conteudo: str = "",
) -> Optional[str]:
    """Devolve o papel da linha, ou None quando não há classificação.

    A ORDEM abaixo é o contrato (I-P2). Cada bloco só é alcançado
    quando os anteriores não casaram.
    """
    l = (linha or "").strip()

    if not l:
        return None

    # ── GATILHO ────────────────────────────────────────────────
    # Linha de urgência que o divulgador escreve ACIMA do título
    # ("-PREÇÃO, ainda tem!", "-Liberado para resgate").
    # Só é reconhecida quando a próxima linha de conteúdo já traz
    # emoji da origem — ou seja, o título real já está marcado e
    # esta linha não pode ser ele. Fora dessa condição, a primeira
    # linha continua sendo o título, como sempre foi.
    if eh_titulo and _RE_EMOJI.search(proxima_conteudo or ""):
        return GATILHO

    # ── TÍTULO ─────────────────────────────────────────────────
    if eh_titulo:
        return TITULO

    # ── AVISO ──────────────────────────────────────────────────
    # Antes de CUPOM: "Importante! Aplique os Cupons..." é aviso,
    # apesar de conter âncora de cupom. A âncora é o INÍCIO da
    # linha, o que a torna estreita o bastante para vir primeiro.
    if _RE_AVISO.match(l):
        return AVISO

    # ── CARRINHO ───────────────────────────────────────────────
    if _RE_CARRINHO.match(l):
        return CARRINHO

    # ── PARCELAMENTO ───────────────────────────────────────────
    # Antes de PREÇO: "R$ 3171 em 12x sem juros" é parcela.
    if _RE_PARCELA.search(l):
        return PARCELA

    # ── FRETE ──────────────────────────────────────────────────
    if _RE_FRETE.search(l):
        return FRETE

    # ── CUPOM (âncora forte + início estrutural) ───────────────
    # ANTES de RESGATE (I-P3). Exige âncora de cupom E que a linha
    # COMECE como declaração/ação estrutural — frase descritiva que
    # só menciona cupom no meio não é capturada (I-P8).
    if _RE_CUPOM_FORTE.search(l) and _RE_INICIO_ESTRUTURAL.match(l):

        # Exceção (I-P7): ponteiro de resgate "Resgate o cupom aqui:"
        # menciona cupom mas NÃO declara benefício próprio — ele aponta
        # para um cupom declarado noutra linha. É RESGATE, não repete o
        # 🎟. A âncora "aqui" + ausência de benefício é o que separa de
        # "Resgate cupom 20% OFF aqui:" (tem benefício → CUPOM) e de
        # "Resgate todos os cupons desta página:" (não é ponteiro
        # "aqui" → CUPOM).
        if _RE_RESGATE_APONTA.search(l) and not _RE_BENEFICIO.search(l):
            return RESGATE

        return CUPOM

    # ── PREÇO ──────────────────────────────────────────────────
    # Depois da âncora forte, antes da fraca: linha monetária com
    # a palavra "desconto" é preço.
    if _RE_PRECO.search(l):
        return PRECO

    # ── CUPOM (âncora fraca) ───────────────────────────────────
    if _RE_CUPOM_FRACA.search(l):
        return CUPOM

    # ── RESGATE ────────────────────────────────────────────────
    if _RE_RESGATE.search(l):
        return RESGATE

    # ── LINK DO PRODUTO ────────────────────────────────────────
    if _RE_LINK.search(l):
        return LINK

    # ── VARIANTE ───────────────────────────────────────────────
    if is_multi and _RE_VARIANTE.search(l):
        return VARIANTE

    if _RE_VARIANTE.search(l) and _RE_URL.search(proxima_linha or ""):
        return VARIANTE

    return None
