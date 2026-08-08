# pipeline/filtros_linha.py — POLÍTICA DE CONTEÚDO LINHA A LINHA
#
# Responsabilidade ÚNICA: decidir quais LINHAS do texto permanecem,
# ANTES da conversão. Trata CTA vazia, separador gráfico, o bloco de
# redes sociais e links de grupo externo.
#
# Vale aqui o PRINCÍPIO DA PRESERVAÇÃO declarado em pipeline.filtros:
# remover exige EVIDÊNCIA POSITIVA de ruído; na dúvida, preserva.
#
# NÃO decide permanência de BLOCO — isso é filtros_bloco, que roda
# depois da conversão. Não importa aquele módulo, nem é importado
# por ele.
#
# Consome as autoridades, nunca as duplica:
#   - plataformas.registry        : o que é URL de plataforma
#   - utils.categorias_universais : categorias universais de URL
#   - filtros_estrutura           : o que é rótulo / URL em linha
#
# Extraído de pipeline.filtros sem qualquer alteração de comportamento.
from __future__ import annotations

import re

from plataformas import registry
from utils.categorias_universais import classificar_universal

# Contrato INTERNO da camada — ver cabeçalho de filtros_estrutura.
from pipeline.filtros_estrutura import _RE_URL_BLOCO, _eh_rotulo

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


def _podar_rotulo_orfao(saida: list) -> None:
    """Remove o rótulo cujo referente acabou de ser descartado.

    A função é agnóstica à causa do descarte: ela não sabe nem deve
    saber por que a linha foi removida. Cabe ao chamador invocá-la
    imediatamente após descartar uma linha que possa ser o referente
    do último rótulo acumulado.

    O rótulo é identificado estruturalmente por `_eh_rotulo`, mantendo
    uma única definição de "linha que anuncia conteúdo, mas não é
    conteúdo" em todo o módulo.

    Pré-condição: deve ser chamada antes de qualquer novo `append` em
    `saida`, pois a decisão é baseada na cauda da lista.
    """
    i = len(saida) - 1
    while i >= 0 and not saida[i]:
        i -= 1
    if i >= 0 and _eh_rotulo(saida[i]) and not _RE_URL_BLOCO.search(saida[i]):
        del saida[i:]


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
    for linha in linhas:
        l = linha.strip()
        if not l:
            if not vazio:
                saida.append("")
            vazio = True
            em_redes = False
            continue
        vazio = False
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
                    _podar_rotulo_orfao(saida)
                    continue
                em_redes = False
        if _RE_CTA.match(l) or _RE_LIXO_STRUCT.match(l):
            continue
        if _RE_GRUPO_EXT.search(l):
            l = _RE_GRUPO_EXT.sub("", l).strip()
            if not l:
                _podar_rotulo_orfao(saida)
                continue
        saida.append(l)
    return "\n".join(saida).strip()

