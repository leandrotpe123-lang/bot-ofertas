"""
Utilitário — Detecção e validação de cupons.

Responsabilidade única: identificar códigos de cupom em texto livre a
partir de EVIDÊNCIA, não de vocabulário. Dado um texto e, opcionalmente,
trechos formatados como código (capturados pela ingestão), determina
quais sequências foram apresentadas como códigos de cupom.

Consumido por: normalização, deduplicação, assunto e montagem.

NÃO faz:
  - normalização de mensagem (responsabilidade da normalização)
  - classificação de plataforma (responsabilidade da classificação)
  - deduplicação (responsabilidade da deduplicação)
  - limpeza de texto (responsabilidade da normalização)

─────────────────────────────────────────────────────────────────────
DOUTRINA DE EVIDÊNCIA (ratificada)

O módulo NÃO tenta adivinhar se uma palavra "parece" cupom. Ele só
reconhece código onde a FONTE declarou que há um código. Três níveis,
em ordem de força:

  T0  CRASE      MessageEntityCode / MessageEntityPre. A fonte marcou
                 o trecho como copiável. É a evidência mais forte que
                 existe: nada se sobrepõe a ela.
  T1  ESTRUTURA  A linha declara um benefício (valor ou percentual) e
                 entrega o código após um separador — ":" ou "-" — ou
                 usa a forma chave-valor ("Cupom: X").
  T2  LINHA      A linha inteira, tirados marcadores, É um único token.

NÃO EXISTE mais:
  · janela de palavra-chave (colher token MAIÚSCULO perto de "cupom");
  · blacklist de vocabulário (_FALSO_CUPOM);
  · exigência de dígito, de tamanho mínimo arbitrário ou de palavra
    previamente conhecida.

A blacklist existia como REMENDO da janela de palavra-chave: aquela
estratégia colhia qualquer token maiúsculo de qualquer posição —
títulos, escopos de benefício, até trechos de URL — e por isso
precisava de uma lista dizendo quais palavras não eram cupom. Foi ela
que produziu FORTES, ESGOTANDO, ENTREGAS, ITENS e 2U6U32Q. Removida a
janela, some a razão de existir da lista: nas posições que restam a
própria fonte declarou o código, e posição de declaração não produz
vocabulário.

Um código que o sistema nunca viu funciona sem alteração de código.
─────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import re
from typing import List


# ── Palavra-chave de domínio ──────────────────────────────────────
# Detecta a presença de termos que indicam contexto de cupom.
# Esta é a definição canônica de _KW_CUPOM no sistema.
#
# ATENÇÃO: _KW_CUPOM indica CONTEXTO, nunca CÓDIGO. Ela é consumida
# por pipeline.assunto_especie e pipeline.montagem_texto para saber
# que o assunto é cupom. A extração NÃO a usa para colher tokens —
# essa era exatamente a janela-KW removida.
_KW_CUPOM = re.compile(
    r'\b(?:cupom|cupons|c[oó]digo|c[oó]digos|coupon|coupons|'
    r'voucher|vouchers)\b',
    re.I,
)


# ── Forma técnica do recorte ──────────────────────────────────────
# ÚNICA validação que resta no módulo. É lexical, não semântica:
# garante que o recorte é um TOKEN de código e não uma frase. Não
# consulta vocabulário, não exige dígito, não impõe tamanho mínimo
# além do necessário para que "token" signifique alguma coisa.
_RE_TEM_LETRA    = re.compile(r'[A-Z]')
_RE_FORMA_CODIGO = re.compile(r'^[A-Z0-9][A-Z0-9_-]{2,19}$')


def _forma_codigo(c: str) -> bool:
    """Verdadeiro quando o recorte tem forma técnica de código.

    Critério integral: 3 a 20 caracteres, apenas alfanuméricos com
    hífen ou sublinhado, iniciando por alfanumérico, e contendo ao
    menos uma letra (impede que "1200" ou "2026" virem código).
    """
    cu = c.strip().upper()
    return bool(_RE_FORMA_CODIGO.match(cu)) and bool(_RE_TEM_LETRA.search(cu))


# ── T1 — evidência estrutural ─────────────────────────────────────
# A linha declara um benefício em valor ou percentual e entrega o(s)
# código(s) após o separador.
#
#   "R$ 120 OFF em R$ 1000: INFLU120, TOMA120"
#   "R$120 em R$1000: INFLU120"        ← sem OFF, mesma estrutura
#   "15% OFF acima de R$200 - TOMA15"  ← separador travessão
#   "10% em R$ 500: PROMO10"
#
# O literal "OFF" NÃO é condição para que um cupom exista: é uma das
# formas de anunciar o benefício, não a única. A exigência de valor
# ANTES do separador é a barreira que impede "Modelo: AGON32",
# "Cor: PRETO2024" e "SKU: 240419000" de virarem cupom; a exclusão de
# linha com URL impede "Resgate aqui: https://.../2U6U32Q".
#
# DONO ÚNICO desta evidência (MB: soberania de utils.cupom).
# pipeline.assunto_especie e pipeline.assunto_oferta CONSOMEM o
# predicado — não redefinem o padrão. Qualquer evolução do formato
# acontece aqui, e só aqui.
_RE_VALOR_BENEFICIO = re.compile(r'(?:r\$\s*[\d.,]+|\d+\s*%)', re.I)

_RE_ITEM_CUPOM = re.compile(
    r'^(?P<pre>[^:]{0,80}?):\s*(?P<pos>[A-Z0-9][A-Z0-9_,\s/-]{2,80})$'
)

_RE_ITEM_TRACO = re.compile(
    r'^(?P<pre>[^-]{0,80}?)\s*[-\u2013]\s*'
    r'(?P<pos>[A-Z0-9][A-Z0-9_,\s/-]{2,80})$'
)

_RE_KV_CUPOM = re.compile(
    r'(?:OFF|cupom|cupons|c[oó]digo|c[oó]digos|coupon|voucher)\s*[:=]\s*'
    r'([A-Z0-9][A-Z0-9_-]{3,19})\b',
    re.I,
)


# ── T2 — linha dedicada ───────────────────────────────────────────
# A linha inteira, tirados marcadores e pontuação, É um único token.
# O benefício foi declarado no cabeçalho da mensagem:
#
#   "🚨 +1 Cupom iFood R$30 OFF sem mínimo em Mercados"
#   ""
#   "🎟 MERCADOSALDAO303"
_RE_LINHA_DEDICADA = re.compile(
    r'^[^A-Za-z0-9]*([A-Z0-9][A-Z0-9_-]{2,19})[^A-Za-z0-9]*$'
)


# ── Reconhecimento estrutural de item de cupom ────────────────────
def _codigos_por_recorte(linha: str, recortes) -> List[str]:
    """Núcleo comum: aplica os recortes dados e valida só a FORMA."""
    l = linha.strip()
    if "http" in l.lower():
        return []
    achados: List[str] = []
    for rx in recortes:
        m = rx.match(l)
        if not m or not _RE_VALOR_BENEFICIO.search(m.group("pre")):
            continue
        for bruto in re.split(r'[,\s/]+', m.group("pos")):
            c = bruto.upper()
            if bruto and c not in achados and _forma_codigo(c):
                achados.append(c)
    return achados


def codigos_de_item_de_cupom(linha: str) -> List[str]:
    """Códigos que a FORMA de item de cupom reconhece nesta linha.

    Contrato do EXTRATOR. Cobre os DOIS separadores observados no
    corpus real: dois-pontos e travessão.

    GARANTIA DE LITERALIDADE: todo código devolvido é a forma
    maiúscula de um recorte literal da linha.
    """
    return _codigos_por_recorte(linha, (_RE_ITEM_CUPOM, _RE_ITEM_TRACO))


def linha_e_item_de_cupom(linha: str) -> bool:
    """PREDICADO ESTRUTURAL: esta linha tem forma de item de cupom?

    Contrato do GATE — pipeline.assunto_oferta e
    pipeline.assunto_especie precisam saber se a LINHA declara um item
    de cupom, nunca quais códigos ela contém.

    ⚠️ RECORTE DELIBERADAMENTE MAIS ESTREITO que o do extrator: aqui
    entra SÓ o separador dois-pontos. O travessão é evidência válida
    para EXTRAIR CÓDIGO, mas ampliá-lo aqui mudaria quantas linhas
    contam como item de cupom e, por consequência, mudaria
    _faixas_de_item_cupom, tem_preco_de_item, beneficio_e_de_loja e o
    gate eh_entidade_cupom. Medido: com este recorte o predicado é
    IDÊNTICO ao comportamento anterior nas 65 mensagens reais.

    Qualquer ampliação deste recorte é mudança de GATE, não de
    extração, e exige frente própria com regressão de natureza.
    """
    return bool(_codigos_por_recorte(linha, (_RE_ITEM_CUPOM,)))


def codigos_de_linha_dedicada(linha: str) -> List[str]:
    """Código de linha inteiramente dedicada a um código (T2)."""
    l = linha.strip()
    if "http" in l.lower():
        return []
    m = _RE_LINHA_DEDICADA.match(l)
    if not m:
        return []
    c = m.group(1).upper()
    return [c] if _forma_codigo(c) else []


# ── Extração ──────────────────────────────────────────────────────
def extrair_todos_cupons(texto: str, code_entities: list = None) -> List[str]:
    """
    Extrai todos os cupons distintos presentes em um texto, na ordem
    de força da evidência: T0 crase → T1 estrutura → T2 linha
    dedicada.

    GARANTIA DE LITERALIDADE (contratual):
      Quando code_entities NÃO é fornecido, todo código devolvido é
      a forma MAIÚSCULA de um recorte literal do texto de entrada:
      as estratégias textuais extraem exclusivamente por
      correspondência sobre o próprio texto. Consumidores que
      LOCALIZAM o código no texto para aplicar apresentação (ver
      montagem._crases) dependem desta propriedade. Qualquer
      evolução que normalize ou transforme o código devolvido DEVE
      preservá-la ou revisar esses consumidores explicitamente.
    """
    encontrados: List[str] = []
    visto = set()

    def add(c: str):
        cu = c.strip().upper()
        if cu and cu not in visto and _forma_codigo(cu):
            visto.add(cu)
            encontrados.append(cu)

    # ── T0 — CRASE: evidência explícita da fonte ──────────────────
    # A fonte marcou o trecho para ser copiado: isto É um código.
    # Nenhuma validação semântica se sobrepõe a esta declaração.
    for trecho in (code_entities or []):
        for tok in re.split(r'[\s,;/]+', trecho.strip()):
            add(tok)

    # ── T1 — evidência estrutural ────────────────────────────────
    for m in _RE_KV_CUPOM.finditer(texto):
        add(m.group(1))

    for linha in texto.splitlines():
        for c in codigos_de_item_de_cupom(linha):
            add(c)
        # ── T2 — linha dedicada ──────────────────────────────────
        for c in codigos_de_linha_dedicada(linha):
            add(c)

    return encontrados
