"""Camada de política de conteúdo — dono declarado por normalizacao.py.

Responsabilidade ÚNICA: decidir quais linhas do texto permanecem e
quais são removidas (política). NÃO transforma a forma do texto
(isso é da normalização) e NÃO decide o que uma URL é (isso é do
classificador/registry).

Este módulo já era o dono declarado desta responsabilidade — ver o
docstring de pipeline.normalizacao, seção "NÃO faz": "filtragem de
conteúdo (responsabilidade de pipeline.filtros)". Até esta frente a
lógica vivia, órfã, dentro de limpar_texto. A migração apenas a
devolve ao dono, sem alterar o comportamento.

NOTA DE MIGRAÇÃO (F-D1, passo 1): as regras foram RELOCADAS verbatim
para garantir saída byte-idêntica. A correção dos falsos positivos
(títulos com "/", bloco de redes engolindo link de produto, etc.) e
o consumo das autoridades de URL (classificar_universal / registry)
são a PRÓXIMA frente, deliberadamente separada desta.
"""
from __future__ import annotations

import re

# _tem_emoji é um primitivo de texto atualmente hospedado em
# normalizacao e compartilhado por montagem. Importado aqui para não
# duplicá-lo. O ciclo de import é quebrado pelo import tardio de
# `filtrar` dentro de normalizacao.normalizar.
from pipeline.normalizacao import _tem_emoji

# ── Regras de política (relocadas de normalizacao, sem alteração) ──
_RE_GRUPO_EXT = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org|chat\.whatsapp\.com)[^\s]*',
    re.I,
)
_RE_LIXO_STRUCT = re.compile(
    r'^\s*(?:-?\s*An[uú]ncio|Publicidade|:::+|---+|===+)\s*$',
    re.I,
)
_RE_CTA = re.compile(
    r'^\s*(?:link\s+(?:do\s+)?produto|link\s+da\s+oferta|resgate\s+aqui|'
    r'clique\s+aqui|acesse\s+aqui|compre\s+aqui|grupo\s+vip|'
    r'entrar\s+no\s+grupo|acessar\s+grupo)\s*:?\s*$',
    re.I,
)
_RE_REDES = re.compile(
    r'^\s*(?:redes\s+\w+|[-–]\s*grupo\s*(?:cupons?|promoções?|vip)?\s*:?\s*$|'
    r'[-–]\s*(?:chat|twitter|whatsapp|instagram|tiktok|youtube)\s*:?\s*$|'
    r'acesse\s+nossas\s+redes)',
    re.I,
)
_RE_ROTULO = re.compile(r'^\s*[-–•]\s*\w[\w\s]{0,30}:\s*$')


def _eh_header_canal(linha: str) -> bool:
    l = linha.strip()
    if not l or _tem_emoji(l[0]):
        return False
    if re.match(r'^[A-ZÀ-Ú][\w\s]{2,30}\s*/\s*[\w\s]{2,30}', l):
        return True
    if re.match(r'^[A-ZÀÁÂÃÉÊÍÓÔÕÚ\s]{4,30}[\s🔥💥⚡🚀]+$', l, re.UNICODE):
        return True
    return False


def filtrar(texto: str) -> str:
    """Aplica a política de conteúdo linha a linha.

    Recebe texto já normalizado na forma (invisíveis/quebras já
    tratados por normalizacao.limpar_texto) e devolve o texto sem as
    linhas consideradas ruído. Em caso de dúvida a regra correta é
    preservar — o ajuste dos falsos positivos é a próxima frente.
    """
    linhas = texto.split("\n")
    saida = []
    vazio = False
    em_redes = False
    primeira = True
    for linha in linhas:
        l = linha.strip()
        if not l:
            if not vazio:
                saida.append("")
            vazio = True
            em_redes = False
            continue
        vazio = False
        if primeira:
            primeira = False
            if _eh_header_canal(l):
                continue
        if _RE_REDES.match(l):
            em_redes = True
            continue
        if em_redes:
            if _RE_ROTULO.match(l) or not l:
                continue
            if not re.match(r'https?://', l):
                em_redes = False
            else:
                continue
        if _RE_CTA.match(l) or _RE_LIXO_STRUCT.match(l):
            continue
        if _RE_GRUPO_EXT.search(l):
            l = _RE_GRUPO_EXT.sub("", l).strip()
            if not l:
                continue
        saida.append(l)
    return "\n".join(saida).strip()
      
