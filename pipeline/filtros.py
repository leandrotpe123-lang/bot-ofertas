"""Camada de política de conteúdo — dono declarado por normalizacao.py.

Responsabilidade ÚNICA: decidir quais linhas do texto permanecem e
quais são removidas. NÃO transforma a forma do texto (normalização) e
NÃO decide o que uma URL é — para isso consulta as autoridades.

═══════════════════════════════════════════════════════════════════
PRINCÍPIO DA PRESERVAÇÃO
═══════════════════════════════════════════════════════════════════
Uma oferta válida deve atravessar o pipeline completa. Filtro remove
RUÍDO; não empobrece conteúdo. Toda regra aqui parte da preservação:

  remover uma linha exige EVIDÊNCIA POSITIVA de que ela é ruído.
  A ausência de evidência de que é conteúdo NÃO é evidência de que
  é ruído. Na dúvida, preserva.

═══════════════════════════════════════════════════════════════════
AUTORIDADES CONSUMIDAS (nunca duplicadas)
═══════════════════════════════════════════════════════════════════
  - plataformas.registry          : o que é URL de plataforma
  - utils.categorias_universais   : categorias universais de URL

Este módulo não reimplementa nenhum desses conhecimentos.
"""
from __future__ import annotations

import re

from plataformas import registry
from utils.categorias_universais import classificar_universal

# _tem_emoji é primitivo de texto hospedado em normalizacao e
# compartilhado com montagem. Importado para não duplicá-lo. O ciclo
# é quebrado pelo import tardio de `filtrar` em normalizacao.normalizar.
from pipeline.normalizacao import _tem_emoji

# ── Reconhecimento de grupo externo ───────────────────────────────
_RE_GRUPO_EXT = re.compile(
    r'https?://(?:t\.me|telegram\.me|telegram\.org|chat\.whatsapp\.com)[^\s]*',
    re.I,
)

# ── Ruído estrutural ──────────────────────────────────────────────
# R6: "-Anúncio" e "Publicidade" NÃO são removidos (decisão de
# negócio ratificada): declaração de publicidade é conteúdo, e a
# montagem já possui ramo próprio que a preserva. Aqui restam apenas
# separadores gráficos, que não carregam informação alguma.
_RE_LIXO_STRUCT = re.compile(r'^\s*(?::::+|---+|===+)\s*$')

# ── Chamada para ação vazia ───────────────────────────────────────
# Ancorada em $: só remove quando a CTA ocupa a linha inteira.
# "Clique aqui" sai; "Clique aqui para ver o preço" permanece.
_RE_CTA = re.compile(
    r'^\s*(?:link\s+(?:do\s+)?produto|link\s+da\s+oferta|resgate\s+aqui|'
    r'clique\s+aqui|acesse\s+aqui|compre\s+aqui|grupo\s+vip|'
    r'entrar\s+no\s+grupo|acessar\s+grupo)\s*:?\s*$',
    re.I,
)

# ── Cabeçalho do bloco de redes sociais ───────────────────────────
# TODAS as alternativas ancoradas em $: a linha precisa SER o
# cabeçalho, não apenas começar com a palavra. Sem a âncora,
# "Redes de Proteção Infantil 3x2m" abria o bloco e a oferta
# inteira era engolida.
_RE_REDES = re.compile(
    r'^\s*(?:'
    r'redes\s+sociais?|'
    r'acesse\s+nossas\s+redes(?:\s+sociais?)?|'
    r'[-–]\s*grupo\s*(?:cupons?|promoções?|vip)?|'
    r'[-–]\s*(?:chat|twitter|whatsapp|instagram|tiktok|youtube)'
    r')\s*:?\s*$',
    re.I,
)

_RE_ROTULO = re.compile(r'^\s*[-–•]\s*\w[\w\s]{0,30}:\s*$')

# ── Vocabulário de cabeçalho de canal ─────────────────────────────
_KW_CANAL = re.compile(
    r'\b(?:ofertas?|promo(?:s|ç(?:ão|ões))?|descontos?|achadinhos?|'
    r'liquida(?:ção|ções)|canal)\b',
    re.I,
)
_RE_DIGITO = re.compile(r'\d')

_LIMITE_HEADER = 30


def _reconhecida_por_autoridade(url: str) -> bool:
    """A autoridade reconhece esta URL como parte da oferta?

    filtros NÃO decide o que uma URL é. Pergunta a quem sabe:
      - registry: fonte única de verdade para plataforma;
      - classificar_universal: fonte única para categorias
        universais. 'mundial' é a própria oferta, 'preservar' deve
        ficar intacto por determinação da autoridade, e 'expandir'
        pode esconder qualquer destino — todos preservados.
    """
    if registry.resolver(url) is not None:
        return True
    return classificar_universal(url) in ("mundial", "preservar", "expandir")


def _eh_header_canal(linha: str) -> bool:
    """Cabeçalho de canal — remoção exige EVIDÊNCIA POSITIVA.

    Quatro condições CUMULATIVAS. Falhando qualquer uma, preserva:

      1. não começa com emoji;
      2. curta (<= 30 caracteres) — nome de canal é curto;
      3. NENHUM dígito — títulos de e-commerce carregam modelo,
         medida ou quantidade ("C/50", "43", "500ml", "M/G");
      4. contém vocabulário de canal.

    O antigo padrão de barra ("^[A-ZÀ-Ú][\\w\\s]{2,30}\\s*/\\s*...")
    foi REMOVIDO: não tinha poder discriminante — casava qualquer
    frase capitalizada com barra, e barra é notação padrão de
    e-commerce brasileiro. Apagava "Paçoca Quadrada C/50 Unidades",
    "Kit Panelas C/5 Peças", "Fralda Pampers M/G".
    """
    l = linha.strip()
    if not l or _tem_emoji(l[0]):
        return False
    if len(l) > _LIMITE_HEADER:
        return False
    if _RE_DIGITO.search(l):
        return False
    return bool(_KW_CANAL.search(l))


def filtrar(texto: str) -> str:
    """Aplica a política de conteúdo linha a linha.

    Recebe texto já normalizado na forma (invisíveis e quebras
    tratados por normalizacao.limpar_texto) e devolve o texto sem as
    linhas com evidência positiva de ruído.
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
            if _RE_ROTULO.match(l):
                continue
            if not re.match(r'https?://', l):
                em_redes = False
            else:
                # URL dentro do bloco de redes. Descartar exige
                # EVIDÊNCIA POSITIVA de grupo externo E que nenhuma
                # autoridade reivindique a URL. Sem as duas coisas,
                # preserva — e encerra o bloco, porque uma URL que
                # não é de grupo indica que o bloco já terminou.
                if (_RE_GRUPO_EXT.search(l)
                        and not _reconhecida_por_autoridade(l)):
                    continue
                em_redes = False
        if _RE_CTA.match(l) or _RE_LIXO_STRUCT.match(l):
            continue
        if _RE_GRUPO_EXT.search(l):
            l = _RE_GRUPO_EXT.sub("", l).strip()
            if not l:
                continue
        saida.append(l)
    return "\n".join(saida).strip()
