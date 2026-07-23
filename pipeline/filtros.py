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
import unicodedata

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

# ══════════════════════════════════════════════════════════════════
# POLÍTICA SEMÂNTICA DE BLOCO — PÓS-CONVERSÃO
# ══════════════════════════════════════════════════════════════════
# O filtro não decide pelo TEXTO do rótulo nem por nome de
# plataforma. Ele consome o resultado produzido pela camada de
# resolução/conversão: o mapa de publicação. Um bloco permanece
# quando gerou oferta publicável; caso contrário sai inteiro —
# rótulo, enumeração e linhas órfãs junto.
#
# ZERO-TOUCH: nenhum nome de plataforma vive aqui. Ao adicionar um
# plugin ao Auto Discovery, suas URLs passam a entrar no mapa de
# publicação e os blocos correspondentes passam a ser preservados
# automaticamente, sem alterar uma linha deste módulo.
#
# Este passe roda DEPOIS da conversão (normalizacao), porque só ali
# existe o fato "este link virou oferta publicável".

_RE_URL_BLOCO = re.compile(r'https?://\S+')

# Enumeração de bloco em todas as grafias observadas:
# 1️⃣ | ① | (1) | 1. | 1) | 1 -
_RE_ENUM = re.compile(
    r'^\s*(?:'
    r'[0-9]\uFE0F?\u20E3'
    r'|[\u2460-\u2473]'
    r'|\(\s*\d{1,2}\s*\)'
    r'|\d{1,2}\s*[.)\-–]'
    r'|\d{1,2}\s*(?=https?://)'
    r')\s*'
)

_ENUM_EMOJI = ("1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣",
               "6️⃣", "7️⃣", "8️⃣", "9️⃣")


def _norm_url(u: str) -> str:
    return u.rstrip('.,;:!?)]}>»"\'').rstrip('/')


def _sem_acento(t: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", t)
        if unicodedata.category(c) != "Mn"
    )


def _ancora_em_plataforma(texto: str) -> bool:
    """O bloco nomeia uma plataforma que o registry conhece?

    Usado APENAS como cláusula de preservação para blocos auxiliares
    — mecânica oficial da promoção cujo link não é de plataforma
    alguma (ex.: consulta de ID citando a loja). O vocabulário vem do
    registry, não de lista escrita aqui: plugin novo entra sozinho.
    """
    alvo = re.sub(r'[^a-z]', '', _sem_acento(texto.lower()))
    return any(p in alvo for p in registry.plataformas_registradas())


def _publicavel(url: str, mapa: dict, preservar) -> bool:
    """A conversão produziu publicação para esta URL?"""
    alvo = _norm_url(url)
    for k in mapa:
        if _norm_url(k) == alvo:
            return True
    return any(_norm_url(p) == alvo for p in (preservar or ()))


def _bloco_permanece(bloco: list, mapa: dict, preservar) -> bool:
    """Evidência de pertencimento. Na dúvida, True (preserva).

    Ordem das decisões:
      1. bloco sem link é conteúdo puro — permanece;
      2. algum link publicável — permanece (gerou oferta válida);
      3. link DE PLATAFORMA que não converteu — sai (plugin ausente
         ou ainda não operacional);
      4. link que nenhuma plataforma reivindica — bloco auxiliar:
         permanece só se ancorado numa plataforma conhecida.
    """
    texto = " ".join(bloco)
    urls = _RE_URL_BLOCO.findall(texto)
    if not urls:
        return True
    if any(_publicavel(u, mapa, preservar) for u in urls):
        return True
    if any(registry.resolver(_norm_url(u)) is not None for u in urls):
        return False
    return _ancora_em_plataforma(texto)


def _eh_rotulo(linha: str) -> bool:
    """Linha que anuncia a URL seguinte (rótulo), não conteúdo."""
    l = linha.strip()
    return l.endswith(":") or bool(_RE_ENUM.match(l))


def _segmentar(texto: str) -> list:
    """Quebra em blocos semânticos.

    Uma URL forma bloco com o RÓTULO imediatamente anterior, quando
    existir. Linhas de conteúdo (título, preço, cupom) NUNCA são
    absorvidas pelo bloco de uma URL — do contrário a remoção de um
    link levaria junto o corpo da oferta.
    """
    blocos, atual = [], []
    for linha in texto.split("\n"):
        if not linha.strip():
            if atual:
                blocos.append(atual)
                atual = []
            blocos.append([])
            continue
        if _RE_URL_BLOCO.search(linha):
            rotulo = None
            if atual and _eh_rotulo(atual[-1]):
                rotulo = atual.pop()
            if atual:
                blocos.append(atual)
            blocos.append([rotulo, linha] if rotulo else [linha])
            atual = []
            continue
        atual.append(linha)
    if atual:
        blocos.append(atual)
    return blocos


def _renumerar(blocos: list) -> list:
    """Reescreve a enumeração dos blocos sobreviventes.

    Numeração quebrada denuncia remoção. Com um único bloco restante
    a enumeração some; com vários, recomeça em 1 na grafia original.
    """
    idx = [i for i, b in enumerate(blocos) if b and _RE_ENUM.match(b[0])]
    if not idx:
        return blocos
    usa_emoji = any(
        "\u20E3" in blocos[i][0][:3] or blocos[i][0][:1] in "①②③④⑤⑥⑦⑧⑨"
        for i in idx
    )
    for n, i in enumerate(idx, start=1):
        limpa = _RE_ENUM.sub("", blocos[i][0]).strip()
        if not limpa:
            # o rótulo era só o número ("2)"): sem enumeração ele
            # não carrega informação alguma — a linha some.
            blocos[i] = blocos[i][1:]
            continue
        inline = bool(_RE_URL_BLOCO.match(limpa))
        if len(idx) == 1:
            blocos[i][0] = limpa
        elif usa_emoji and n <= len(_ENUM_EMOJI):
            blocos[i][0] = f"{_ENUM_EMOJI[n - 1]} {limpa}"
        elif inline:
            blocos[i][0] = f"{n} {limpa}"
        else:
            blocos[i][0] = f"{n}. {limpa}"
    return blocos


def _rotulo_orfao(bloco: list) -> bool:
    """Bloco reduzido a um rótulo solto, sem o conteúdo que anunciava."""
    return (len(bloco) == 1
            and bloco[0].rstrip().endswith(":")
            and not _RE_URL_BLOCO.search(bloco[0]))


def filtrar_blocos(texto: str, mapa: dict, preservar=()) -> str:
    """Remove os blocos que não geraram oferta publicável.

    Consome o mapa de publicação produzido pela conversão. Depois de
    remover, renumera, elimina rótulos órfãos e colapsa linhas
    vazias, para que o texto pareça ter nascido assim.
    """
    blocos = [b for b in _segmentar(texto)
              if not b or _bloco_permanece(b, mapa, preservar)]
    blocos = [b for b in blocos if not (b and _rotulo_orfao(b))]
    blocos = _renumerar(blocos)

    saida, vazio = [], True
    for b in blocos:
        if not b:
            if not vazio:
                saida.append("")
                vazio = True
            continue
        saida.extend(b)
        vazio = False
    return "\n".join(saida).strip()

# ══════════════════════════════════════════════════════════════════
# VETO DE POST — mecânica de canal, não oferta
# ══════════════════════════════════════════════════════════════════
# Alguns posts não são ofertas: são mecânica do canal de origem
# (gerar link por bot, mandar produto no chat) ou benefício restrito
# a quem é membro daquele grupo. Republicá-los polui o destino e
# entrega algo que o leitor não consegue usar.
#
# As expressões são DELIBERADAMENTE estreitas: exigem a locução
# inteira, não palavras soltas. "exclusivo" sozinho é comum em
# oferta legítima ("Cupons exclusivos Shopee VIP") e NÃO veta;
# apenas "exclusivo do grupo" veta. Mesmo critério para "gere o
# link" e "mande no chat", que descrevem a mecânica do canal.
#
# Na dúvida, publica: só veta com a locução completa presente.

_RE_VETO_POST = re.compile(
    r'exclusiv[oa]s?\s+d[oe]\s+(?:grupo|canal)'
    r'|d[oe]\s+(?:grupo|canal)\s+exclusiv'
    r'|ger(?:e|ar|a)\s+(?:o\s+|seu\s+|um\s+)?link'
    r'|man(?:de|da|dar)\s+(?:o\s+)?(?:produto\s+)?no\s+chat'
    r'|envi(?:e|ar)\s+(?:o\s+)?(?:produto\s+)?no\s+chat',
    re.I,
)


def deve_descartar(texto: str) -> str:
    """Motivo do veto do post inteiro, ou cadeia vazia se publicável."""
    m = _RE_VETO_POST.search(_sem_acento(texto or ""))
    return m.group(0).strip() if m else ""
