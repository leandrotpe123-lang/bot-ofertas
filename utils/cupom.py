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

CUPOM É SOMENTE O QUE A FONTE DECLAROU COMO CÓDIGO.

  T0  CRASE      MessageEntityCode / MessageEntityPre. A fonte marcou
                 o trecho como copiável. É a ÚNICA evidência de cupom.

NÃO EXISTE fallback textual. O módulo NÃO tenta descobrir cupom por:
  · janela de palavra-chave (token MAIÚSCULO perto de "cupom");
  · blacklist de vocabulário (_FALSO_CUPOM);
  · estrutura de linha com valor/percentual antes de ":" ou "-" (T1);
  · linha inteiramente dedicada a um token (T2);
  · caixa alta, posição na frase, contexto semântico ou dois-pontos.

HISTÓRICO — por que a inferência caiu
  A janela de palavra-chave colhia token maiúsculo de qualquer
  posição e produziu FORTES, ESGOTANDO, ENTREGAS, ITENS e 2U6U32Q.
  Foi removida, e com ela a blacklist que existia só para remendá-la.
  Restaram T1 e T2, mais estreitos — e ainda assim T2 produziu
  TUTORIAL a partir de "🚨🚨 TUTORIAL:", medido em produção
  (id=1295), fazendo um post duplicar por âncora `cup|TUTORIAL`.

  A lição é a mesma das duas vezes: toda regra que INFERE código a
  partir da forma do texto acaba colhendo o texto. A correção não é
  uma inferência mais fina — é não inferir.

O QUE PERMANECE, e não é extração de cupom
  `linha_e_item_de_cupom` é PREDICADO ESTRUTURAL consumido por
  pipeline.assunto_oferta e pipeline.assunto_especie para decidir se
  uma LINHA tem forma de item de cupom (gate de natureza). Ele NÃO
  devolve código e NÃO alimenta `cupons`. Continua como estava:
  mexer nele seria mudar o gate, que é outra frente.

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


# ── FORMA de item de cupom — PREDICADO DE GATE, não extração ──────
# A linha declara um benefício em valor ou percentual e entrega algo
# após o separador:
#
#   "R$ 120 OFF em R$ 1000: INFLU120, TOMA120"
#   "10% em R$ 500: PROMO10"
#
# ⚠️ ESTE RECORTE NÃO EXTRAI CUPOM. Ele responde APENAS se a LINHA
# TEM FORMA de item de cupom, e é consumido por
# pipeline.assunto_oferta e pipeline.assunto_especie no gate de
# natureza (_faixas_de_item_cupom, tem_preco_de_item,
# beneficio_e_de_loja, eh_entidade_cupom).
#
# A exigência de valor ANTES do separador é o que impede
# "Modelo: AGON32" e "Cor: PRETO2024" de contarem como item; a
# exclusão de linha com URL impede "Resgate aqui: https://…".
#
# Preservado byte-a-byte: mexer aqui é mudar o GATE, não a extração
# de cupom — outra frente, com regressão de natureza própria.
_RE_VALOR_BENEFICIO = re.compile(r'(?:r\$\s*[\d.,]+|\d+\s*%)', re.I)

_RE_ITEM_CUPOM = re.compile(
    r'^(?P<pre>[^:]{0,80}?):\s*(?P<pos>[A-Z0-9][A-Z0-9_,\s/-]{2,80})$'
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


def linha_e_item_de_cupom(linha: str) -> bool:
    """PREDICADO ESTRUTURAL: esta linha tem forma de item de cupom?

    Contrato do GATE — pipeline.assunto_oferta e
    pipeline.assunto_especie precisam saber se a LINHA declara um item
    de cupom, nunca quais códigos ela contém.

    ⚠️ NÃO EXTRAI CUPOM e NÃO alimenta `cupons` — cupom vem só de T0.
    Entra SÓ o separador dois-pontos. Ampliar este recorte mudaria
    quantas linhas contam como item de cupom e, por consequência,
    _faixas_de_item_cupom, tem_preco_de_item, beneficio_e_de_loja e o
    gate eh_entidade_cupom. Medido: com este recorte o predicado é
    IDÊNTICO ao comportamento anterior nas 65 mensagens reais.

    Preservado byte-a-byte na remoção de T1/T2: qualquer alteração
    aqui é mudança de GATE, e exige frente própria com regressão de
    natureza.
    """
    return bool(_codigos_por_recorte(linha, (_RE_ITEM_CUPOM,)))


# ── Extração ──────────────────────────────────────────────────────
def extrair_todos_cupons(texto: str, code_entities: list = None) -> List[str]:
    """
    Cupons declarados pela FONTE como código (T0), na ordem em que
    aparecem. Sem code_entities não há cupom — e isso é a regra, não
    uma degradação.

    `texto` permanece na assinatura porque é o contrato público
    consumido por normalização e montagem; ele NÃO é mais inspecionado
    para descobrir código. Não existe fallback textual.

    GARANTIA DE LITERALIDADE (contratual):
      Todo código devolvido é a forma MAIÚSCULA de um trecho que a
      fonte marcou como código. Consumidores que LOCALIZAM o código no
      texto para aplicar apresentação (ver montagem._crases) dependem
      desta propriedade.
    """
    encontrados: List[str] = []
    visto = set()

    # ── T0 — CRASE: a única evidência de cupom ────────────────────
    # A fonte marcou o trecho para ser copiado: isto É um código.
    # `_forma_codigo` continua sendo a ÚNICA validação, e é lexical —
    # garante que o recorte é um token, não uma frase. Nenhuma regra
    # semântica, vocabulário ou blacklist participa.
    for trecho in (code_entities or []):
        for tok in re.split(r'[\s,;/]+', trecho.strip()):
            cu = tok.strip().upper()
            if cu and cu not in visto and _forma_codigo(cu):
                visto.add(cu)
                encontrados.append(cu)

    return encontrados
