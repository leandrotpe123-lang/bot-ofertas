# pipeline/assunto.py — CLASSIFICAÇÃO DO ASSUNTO DO POST
#
# Responsabilidade ÚNICA: dado o texto (e fatos derivados pela
# normalização), dizer QUAL É A ESPÉCIE DO ASSUNTO do post.
#
# NÃO faz:
#   - derivação de identidade / âncoras     (identidade_oferta)
#   - memória de códigos / cupom_idx        (identidade_oferta, por ora)
#   - decisão de duplicidade / evolução     (deduplicacao / decisao)
#   - I/O de qualquer natureza              (camada PURA)
#
# CONTRATO PÚBLICO (o resto é interno — regex NUNCA saem daqui):
#   eh_post_cupom(texto)                      -> bool
#   eh_post_cashback(texto, tem_sinal)        -> bool
#   eh_post_evento(texto, tem_host)           -> bool
#   eh_lista_cupons(texto)                    -> bool
#   extrair_pct_cashback(texto)               -> str
#   buscar_calendario_comercial(texto)        -> re.Match | None
#
# C1 (refactor puro): nova casa dos detectores, extraídos byte-a-byte de
# identidade_oferta.py. O classificador AINDA NÃO tem autoridade sobre a
# família — ancoras() segue decidindo como antes. A autoridade só será
# concedida em C3, após o endurecimento (C2).
from __future__ import annotations

import re
from typing import Optional

from pipeline.estado_evento import _KW_EVENTO
from utils.cupom import _KW_CUPOM


# ─────────────────────────────────────────────────────────────────
# Detecção do TIPO do post (define qual identidade usar)
# ─────────────────────────────────────────────────────────────────
_RE_TITULO_CUPOM = re.compile(
    r'\b(?:cupom|cupons|c[oó]digo|coupon|novo\s+cupom|cupom\s+novo)\b',
    re.I,
)
_RE_TITULO_CASHBACK = re.compile(
    r'\b(?:cashback|cash\s*back)\b',
    re.I,
)
_RE_PCT = re.compile(r'(\d{1,2})\s*%')

# Resíduo NOMEADO de calendário comercial — fronteira explícita.
# NÃO pertence à família interativa canônica (_KW_EVENTO, dono:
# pipeline.estado_evento) e NÃO deve subir para lá: no canônico,
# este vocabulário furaria a saturação e mudaria títulos no pico
# comercial. Serve apenas ao gate/chave de identidade de campanha.
_RE_CALENDARIO_COMERCIAL = re.compile(
    r'\b(?:black\s*friday|esquenta)\b',
    re.I,
)

# Padrão "lista de cupons" — formato típico:
#   "🔥 R$ 100 OFF em R$ 900: INFLU100"
_RE_LINHA_CUPOM_LISTA = re.compile(
    r'(?:r\$\s*\d+|\d+\s*%)\s+off(?:\s+em\s+r\$\s*\d+)?\s*:\s*[A-Z0-9][A-Z0-9_-]{3,19}',
    re.I,
)

# Padrão "OFF: CODIGO" no título
_RE_TITULO_OFF_COD = re.compile(
    r'(?:r\$\s*\d+|\d+\s*%)\s+off(?:\s+em\s+r\$\s*\d+)?\s*:\s*[A-Z0-9][A-Z0-9_-]{3,19}',
    re.I,
)

# Linha de cashback (sem precisar de "OFF" literal). Usado pra
# detectar posts Shopee de cashback como post-cupom.
_RE_CASHBACK_LINHA = re.compile(
    r'\d+\s*%\s+cash\s*back',
    re.I,
)


# ── C2: ANÁLISE SINTÁTICA DO TÍTULO — cupom SUJEITO vs COMPLEMENTO ──
# O caso (a) do detector disparava com QUALQUER menção de "cupom" no
# título, sem perguntar se o cupom era o ASSUNTO ou apenas um adorno de
# um produto. Duas evidências de que o cupom é COMPLEMENTO:
#
#   1. Vem após pontuação/conectivo:  "(cupom X)", "- use o cupom X",
#      ", cupom X", "com cupom X", "no cupom X".
#   2. Um PREÇO aparece ANTES dele:   "Air Fryer R$299 cupom AIR20"
#      → o produto é o sujeito; o cupom só barateia.
#
# Sutileza importante: em "Cupom R$50 OFF" o preço vem DEPOIS da palavra
# — é o VALOR DO DESCONTO, não o preço de um produto. Por isso a regra
# compara POSIÇÕES, e não a mera presença de "R$".
_RE_CUPOM_COMPLEMENTO = re.compile(
    r'(?:[(\[\-–—,]\s*|'
    r'\b(?:use|usando|utilize|aplique|com|no|na|c/|via|pelo)\s+(?:o\s+|a\s+)?)'
    r'(?:cupom|cupons|c[oó]digo|coupon)\b',
    re.I,
)
_RE_PRECO_TITULO = re.compile(r'R\$\s?\d[\d.,]*', re.I)


def _cupom_e_sujeito(titulo: str) -> bool:
    """O cupom é o SUJEITO do título (assunto), ou só um complemento?

    True  → "CUPOM Amazon 20% OFF", "Novo codigo liberado"
    False → "Echo Dot R$249 (cupom ECHO10)", "Fone JBL - use o cupom X"
    """
    m = _RE_TITULO_CUPOM.search(titulo)
    if not m:
        return False
    if _RE_CUPOM_COMPLEMENTO.search(titulo):
        return False
    mp = _RE_PRECO_TITULO.search(titulo)
    if mp and mp.start() < m.start():
        return False
    return True


def _eh_lista_cupons(texto: str) -> bool:
    """
    Detecta se o post é uma LISTA DE CUPONS (não um cupom único).
    Critério: 2+ linhas no formato "R$ X OFF em R$ Y: CODIGO".
    """
    linhas = texto.strip().split("\n")
    linhas_lista = sum(
        1 for l in linhas
        if _RE_LINHA_CUPOM_LISTA.search(l)
    )
    return linhas_lista >= 2


def _eh_post_cupom(texto: str) -> bool:
    """
    Detecta se o post é 'tipo cupom' — onde o cupom é o ASSUNTO PRINCIPAL.

    Casos cobertos:
      a) palavra "cupom"/"cupons"/"código" no título
      b) título já é "R$ X OFF: CODIGO" / "X% OFF: CODIGO"
      c) 2+ linhas formato lista de cupons (delegada a _eh_lista_cupons)
      d) cashback nas primeiras linhas + código presente
      e) linha "X% Cashback ... : CODIGO" (mesmo sem palavra cupom)

    Casos (d) e (e) cobrem cards Shopee com título genérico ("Leo Indica
    / Ofertas Insanas") + linha "🎟️ 50% Cashback ... : BRUIANHEZ10".
    """
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]

    # Caso (a) — C2: palavra "cupom" no título E sendo o SUJEITO.
    # Antes bastava a menção, o que lia "Fone JBL R$199 (cupom: DESC10)"
    # como post de cupom. Agora o cupom precisa ser o assunto, não adorno.
    if _cupom_e_sujeito(titulo):
        return True

    # Caso (b): título já é "R$ X OFF: CODIGO" / "X% OFF: CODIGO"
    if _RE_TITULO_OFF_COD.search(titulo):
        return True

    # Caso (c): 2+ linhas formato lista (reusa _eh_lista_cupons)
    if _eh_lista_cupons(texto):
        return True

    # Caso (d): cashback presente nas primeiras 5 linhas + cupom
    # mencionado no texto inteiro
    primeiras = "\n".join(linhas[:5])
    if (_RE_CASHBACK_LINHA.search(primeiras)
            and _KW_CUPOM.search(texto)):
        return True

    # Caso (e): linha "X% Cashback ... : CODIGO" no formato KV
    for linha in linhas[:6]:
        if (_RE_CASHBACK_LINHA.search(linha)
                and re.search(r':\s*[A-Z0-9][A-Z0-9_-]{3,19}\b', linha)):
            return True

    return False


def _eh_post_cashback(texto: str, tem_sinal_cashback: bool) -> bool:
    """
    Detecta se o post é especificamente sobre cashback (sem cupom code).
    Combina o vocabulário genérico de cashback no título (universal)
    com o sinal derivado tem_sinal_cashback — vocabulário específico
    de plataforma, declarado pela própria plataforma e composto pelo
    registry. A dedup não conhece mais termos de marca diretamente.
    """
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]
    if _RE_TITULO_CASHBACK.search(titulo):
        return True
    return tem_sinal_cashback


def _eh_post_evento(texto: str, tem_host_campanha: bool) -> bool:
    """
    Detecta evento/campanha/roleta (sem ASIN, sem cupom claro).

    CONSUMO DE IDENTIDADE DERIVADA:
      Esta função NÃO reinterpreta o mapa de URLs. A identidade de
      campanha é derivada pela normalização — autoridade única dessa
      derivação — sobre as URLs afiliadas LONGAS, antes do
      encurtamento terminal. A deduplicação apenas consome o campo
      derivado tem_host_campanha.

    A detecção combina dois sinais independentes:
      - vocabulário de evento no texto, em duas famílias nomeadas:
        a interativa canônica (_KW_EVENTO, dono: estado_evento) e o
        resíduo local de calendário comercial — classificação de
        conteúdo, legítima nesta camada por operar sobre o texto;
      - presença de host de campanha: consumida do campo derivado.
    """
    if _KW_EVENTO.search(texto[:200]):
        return True
    if _RE_CALENDARIO_COMERCIAL.search(texto[:200]):
        return True
    return tem_host_campanha


def _extrair_pct_cashback(texto: str) -> str:
    """Extrai o percentual de cashback (ex: '30' de 'cashback 30%')."""
    primeiras = " ".join(texto.split("\n")[:5])
    m = _RE_PCT.search(primeiras)
    return m.group(1) if m else ""

# ── C3.1: DESCRITOR DE BENEFÍCIO — identidade do cupom SEM CÓDIGO ──
# Cupom com código usa o código. Cupom sem código precisa de uma
# identidade ESTÁVEL — nunca o hash do texto, que erra nos dois sentidos:
# colapsa distintos ("20% OFF" e "15% OFF" viram a mesma alma, pois _alma
# normaliza percentuais) e separa iguais (reordenação muda o hash).
#
# O descritor é COMPOSTO e CANÔNICO: todos os sinais presentes, sempre na
# mesma ordem (pct → vlr → frete → 1acompra). A mesma oferta descrita de
# formas diferentes produz a MESMA chave.
#
# Vocabulário UNIVERSAL de e-commerce — zero conhecimento de marketplace.
# Um marketplace novo funciona automaticamente.
# \b impede casar SUBSTRING de número maior: sem ele, "1050%" casaria
# "050" (=50) e passaria na guarda de intervalo — lixo disfarçado de válido.
_RE_BEN_PCT = re.compile(r'\b(\d{1,3})\s?%')
_RE_BEN_VLR = re.compile(r'R\$\s?(\d[\d.]*)', re.I)
_RE_BEN_FRETE = re.compile(r'\bfrete\b', re.I)
_RE_BEN_1A_COMPRA = re.compile(
    r'\bprimeir[ao]\s+compra\b|\b1[ªa]\s+compra\b|'
    r'\bnovos?\s+usu[aá]rios?\b|\bnovas?\s+contas?\b',
    re.I,
)
# Valor precedido de condição de MÍNIMO não é benefício — é restrição.
# "Cupom R$30 OFF acima de R$150" → o benefício é R$30; R$150 é condição.
_RE_BEN_CONDICAO = re.compile(
    r'(?:acima\s+de|a\s+partir\s+de|em\s+compras\s+(?:de|acima)|'
    r'min(?:imo)?\.?\s+(?:de)?|nas?\s+compras?\s+de|para\s+compras\s+de)'
    r'\s*R?\$?\s?\d',
    re.I,
)

_ESCOPO_BENEFICIO = 300
_PCT_MIN, _PCT_MAX = 1, 100


def _pct_do_beneficio(texto: str) -> Optional[int]:
    """Primeiro percentual VÁLIDO (1..100). A regex aceita até 3 dígitos
    para cobrir "100% OFF"; a guarda de intervalo descarta lixo ("999%")
    sem invalidar um percentual legítimo que venha depois no texto."""
    for m in _RE_BEN_PCT.finditer(texto):
        valor = int(m.group(1))
        if _PCT_MIN <= valor <= _PCT_MAX:
            return valor
    return None


def _vlr_do_beneficio(texto: str, tem_pct: bool) -> Optional[str]:
    """Valor em R$ que seja BENEFÍCIO (não condição de mínimo).
    Se já há percentual, qualquer R$ no texto é condição, não benefício."""
    if tem_pct:
        return None
    for m in _RE_BEN_VLR.finditer(texto):
        antes = texto[max(0, m.start() - 30):m.start()]
        if _RE_BEN_CONDICAO.search(antes + m.group(0)):
            continue
        return m.group(1).replace(".", "")
    return None


# ─────────────────────────────────────────────────────────────────
# API PÚBLICA — o único contrato exposto. As regex e os detectores
# internos (_RE_*, _eh_*) permanecem privados deste módulo.
# ─────────────────────────────────────────────────────────────────
def beneficio_do_cupom(texto: str) -> str:
    """[F-C4 / INV-E2] REBAIXADA a extratora de ESTADO: benefício é
    atributo evolutivo da campanha, nunca identidade. A identidade da
    campanha sem código é o TEMA (tema_da_campanha). Mantida para
    exibição/score e para o futuro INV-E4 fino (F-C5).
    Descritor CANÔNICO e ESTÁVEL do benefício de um cupom SEM código.

    É a IDENTIDADE do cupom quando não há código extraível — nunca o hash
    do texto. Composto (todos os sinais) e em ordem fixa, para que a mesma
    oferta descrita de formas diferentes produza a MESMA chave.

    Retornos: 'pct:20' | 'vlr:30' | 'frete' | '1acompra' |
              'pct:15+frete' | 'vlr:30+1acompra' | 'geral'

    'geral' é o bucket CONSERVADOR e consciente: cupom sem benefício
    identificável ("Novo Cupom Shopee"). Cupons genéricos da mesma
    plataforma convergem nele dentro da janela. Se em produção ele passar
    a unificar ofertas distintas com frequência, deve ser refinado com
    sinais adicionais — mantendo a natureza genérica, sem regra de
    marketplace.
    """
    t = texto[:_ESCOPO_BENEFICIO]
    partes: list[str] = []

    pct = _pct_do_beneficio(t)
    if pct is not None:
        partes.append(f"pct:{pct}")

    vlr = _vlr_do_beneficio(t, tem_pct=pct is not None)
    if vlr:
        partes.append(f"vlr:{vlr}")

    if _RE_BEN_FRETE.search(t):
        partes.append("frete")
    if _RE_BEN_1A_COMPRA.search(t):
        partes.append("1acompra")

    return "+".join(partes) if partes else "geral"

_RE_ESCOPO_LOJA = re.compile(
    r"\b(em\s+tudo|todo\s+o\s+site|site\s+(?:todo|inteiro)|no\s+app|"
    r"acima\s+de\s+r?\$|para\s+compras\s+de|"
    r"sem\s+(?:valor\s+)?m[ií]nimo|primeira\s+compra)\b", re.I)

# Preço DE ITEM: "por R$249" / "R$1.799" — um valor que NÃO é desconto
# (sem OFF/desconto grudado). É o sinal de que o post vende o item (R1).
_RE_PRECO_ITEM = re.compile(
    r"(?:\bpor\s+)?r\$\s*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?"
    r"(?!\s*(?:de\s+)?(?:off|desc))", re.I)
_RE_DESC_VAL = re.compile(
    r"r\$\s*\d+[.,]?\d*\s*(?:de\s+)?(?:off|desc)", re.I)


# ══════════ CONDIÇÃO DO BENEFÍCIO — PONTO ÚNICO DE EVOLUÇÃO ══════════
# [F-C6] Hipótese de implementação.
# Derivada do corpus observado em produção.
# Se validada, poderá originar uma regra oficial do MB.
#   "Valor monetário usado como CONDIÇÃO para o benefício (compra
#    mínima ou limite do desconto) não deve ser interpretado como
#    preço do produto."
# O CONCEITO é o que importa; as formas abaixo são apenas como os
# grupos o escrevem hoje. Toda evolução de vocabulário acontece AQUI —
# em nenhum outro ponto do código.
#
# Cada forma implementada tem evidência em mensagem real de produção:
_COND_PISO = (          # piso: compra mínima para o cupom valer
    "em",               # Shopee/Amazon: "R$129 OFF em R$599: AQU3C808AF"
    "acima de",         # Mercado Livre: "R$100 OFF acima de R$899"
)
_COND_TETO = (          # teto: desconto máximo concedido
    "limite",           # ML/Shopee: "15% OFF acima de R$79, limite R$60"
    "até",              # Magalu/Pelando: "20% OFF até R$ 500: PELANDO20"
    "ate",              #   idem, sem acento
)
# Mesmo papel semântico, porém SEM evidência no corpus de produção.
# NÃO implementadas por decisão explícita: o vocabulário evolui por
# observação, nunca por antecipação. Ao aparecer em mensagem real,
# mover a forma para _COND_PISO/_COND_TETO — e mais nada muda.
_COND_EXPANSAO_FUTURA = (
    "a partir de", "nas compras de", "em pedidos de", "mínimo",
    "limitado a", "máximo", "para compras de",
)
_RE_COND_BENEFICIO = re.compile(
    r"\b(?:" + "|".join(f.replace(" ", r"\s+")
                        for f in _COND_PISO + _COND_TETO)
    + r")\s*(r?\$)", re.I)


def _condicoes_do_beneficio(t: str) -> set:
    """Posições dos valores que são CONDIÇÃO de um benefício.
    O marcador (piso/teto) só qualifica quando há benefício ANTES dele
    na mesma linha — a condição é *do benefício*. Sem benefício, "em
    R$899" é preço ("sai em R$899"), não compra mínima. Fidelidade ao
    conceito, sem ampliar vocabulário."""
    pos = set()
    base = 0
    for linha in t.splitlines(keepends=True):
        for m in _RE_COND_BENEFICIO.finditer(linha):
            antes = linha[:m.start()]
            if (_RE_BEN_PCT.search(antes) or _RE_DESC_VAL.search(antes)
                    or _RE_BEN_FRETE.search(antes)):
                pos.add(base + m.start(1))
        base += len(linha)
    return pos


def tem_preco_de_item(t: str) -> bool:
    """Existe valor R$ que seja preço do PRODUTO — isto é, que não seja
    desconto (OFF) nem condição do benefício (piso/teto)?"""
    descontos = {m.start() for m in _RE_DESC_VAL.finditer(t)}
    condicoes = _condicoes_do_beneficio(t)
    for m in _RE_PRECO_ITEM.finditer(t):
        if any(abs(m.start() - d) <= 4 for d in descontos):
            continue
        if any(abs(m.start() - c) <= 2 for c in condicoes):
            continue
        return True
    return False


def beneficio_e_de_loja(texto: str) -> bool:
    """
    Discriminador R1×R2 — executa a TABELA do MB ratificado:
      · escopo-loja explícito ("em tudo", "no app", "acima de R$",
        "sem mínimo", "1ª compra") + benefício (%, R$-OFF, frete) → LOJA
      · benefício de desconto SEM preço de item no post → LOJA
        (o MB: "X% OFF ... sem preço de item → R2"; nada vende o item)
      · qualquer preço de item presente e sem escopo → dúvida → ITEM
        (zona cinzenta ratificada: prevalece o produto)
    O dono desta regra é o MB — este código apenas a executa.
    """
    t = texto[:_ESCOPO_BENEFICIO]
    beneficio = bool(_RE_BEN_PCT.search(t) or _RE_BEN_VLR.search(t)
                     or _RE_BEN_FRETE.search(t))
    if not beneficio:
        return False
    if _RE_ESCOPO_LOJA.search(t):
        return True
    return not tem_preco_de_item(t)


_TEMAS_CAMPANHA = {
    # forma escrita → chave canônica. Vocabulário é DETALHE DE
    # IMPLEMENTAÇÃO (MB INV-E3): evolui por observação de produção sem
    # alterar o princípio "elemento textual mais estável da campanha".
    "vip": "vip",
    "moeda": "moeda", "moedas": "moeda",
    "frete gratis": "frete", "frete grátis": "frete",
    "aniversario": "aniversario", "aniversário": "aniversario",
    "relampago": "relampago", "relâmpago": "relampago",
    "primeira compra": "primeira_compra",
    "assinante": "assinante", "assinantes": "assinante",
}
_RE_TEMA_CAMPANHA = re.compile(
    r"\b(vip|moedas?|frete\s+gr[aá]tis|anivers[aá]rio|rel[aâ]mpago|"
    r"primeira\s+compra|assinantes?)\b", re.I)


def tema_da_campanha(texto: str) -> str:
    """
    [F-C4] Identidade da campanha SEM código (INV-E3): o elemento
    textual mais estável — o nome que as fontes repetem ao falar da
    mesma campanha. Sem tema reconhecível → "geral" (bucket temporal
    da plataforma; tolerância conservadora ratificada, R5).
    Números NUNCA entram (INV-E2: limites/percentuais são estado).
    """
    m = _RE_TEMA_CAMPANHA.search(texto[:_ESCOPO_BENEFICIO])
    if not m:
        return "geral"
    bruto = re.sub(r"\s+", " ", m.group(1).lower())
    return _TEMAS_CAMPANHA.get(bruto, "geral")


def eh_lista_cupons(texto: str) -> bool:
    """O post é uma LISTA de cupons (2+ linhas 'R$X OFF em R$Y: COD')?"""
    return _eh_lista_cupons(texto)


def eh_post_cupom(texto: str) -> bool:
    """O assunto do post é um CUPOM (código promocional)?"""
    return _eh_post_cupom(texto)


def eh_post_cashback(texto: str, tem_sinal_cashback: bool) -> bool:
    """O assunto do post é CASHBACK (sem cupom code)?"""
    return _eh_post_cashback(texto, tem_sinal_cashback)


def eh_post_evento(texto: str, tem_host_campanha: bool) -> bool:
    """O assunto do post é uma CAMPANHA/EVENTO?"""
    return _eh_post_evento(texto, tem_host_campanha)


def extrair_pct_cashback(texto: str) -> str:
    """Percentual de cashback nas 5 primeiras linhas ('30' de '30%')."""
    return _extrair_pct_cashback(texto)


def buscar_calendario_comercial(texto: str) -> Optional[re.Match]:
    """Match do vocabulário de calendário comercial (black friday /
    esquenta) nos 200 primeiros chars. Devolve o MATCH — o consumidor
    precisa da posição (.start()) para ordenar candidatos —, nunca a
    regex, que permanece interna a este módulo."""
    return _RE_CALENDARIO_COMERCIAL.search(texto[:200])
 
