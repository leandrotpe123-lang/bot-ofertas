# pipeline/filtros_bloco.py — POLÍTICA SEMÂNTICA DE BLOCO
#
# Responsabilidade ÚNICA: decidir quais BLOCOS permanecem, DEPOIS da
# conversão, consumindo o mapa de publicação. Segmenta, decide
# permanência, renumera e poda rótulo órfão.
#
# NÃO decide permanência de LINHA — isso é filtros_linha, que roda
# antes da conversão. Não importa aquele módulo, nem é importado
# por ele.
#
# Consome as autoridades, nunca as duplica:
#   - plataformas.registry : o que é URL de plataforma
#   - filtros_estrutura    : o que é rótulo / enumeração / URL
#
# Extraído de pipeline.filtros sem qualquer alteração de comportamento.
from __future__ import annotations

import re
import unicodedata

from plataformas import registry

# Contrato INTERNO da camada — ver cabeçalho de filtros_estrutura.
from pipeline.filtros_estrutura import (_RE_ENUM, _RE_URL_BLOCO, _eh_rotulo,
                                       urls_do_bloco)
from pipeline.normalizacao_texto import sem_marcacao

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
    urls = urls_do_bloco(texto)
    if not urls:
        return True
    if any(_publicavel(u, mapa, preservar) for u in urls):
        return True
    if any(registry.resolver(_norm_url(u)) is not None for u in urls):
        return False
    return _ancora_em_plataforma(texto)


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
    """Bloco reduzido a um rótulo solto, sem o conteúdo que anunciava.

    Testa a projeção sem marcação pela mesma razão de `_eh_rotulo`.
    NÃO delega a `_eh_rotulo` de propósito: aquele predicado também
    aceita enumeração, e um bloco de uma linha enumerada é CONTEÚDO
    ("1️⃣ Envie o seu produto"), não rótulo. Delegar apagaria o corpo
    da oferta. A duplicação do teste `endswith(":")` fica registrada
    como dívida, não resolvida aqui.
    """
    return (len(bloco) == 1
            and sem_marcacao(bloco[0]).rstrip().endswith(":")
            and not _RE_URL_BLOCO.search(bloco[0]))


# ─────────────────────────────────────────────────────────────────
# SEÇÃO DE LOJA (post 24384 — God of War Laufey)
#
# Post multi-loja organiza um PARÁGRAFO por loja: "-Kabum / -CUPOM:
# VIPOFWAR / link". `_segmentar` nunca deixa cupom ou conteúdo entrar
# no bloco da URL (protege título e preço de oferta única), então só a
# linha do link saía e "-Kabum + CUPOM" ficavam soltos no post.
#
# Um parágrafo sai INTEIRO quando é a seção de uma loja cujo link não
# converteu — todas as condições:
#   · teve URL removida e não sobrou nenhum link nele;
#   · não é o 1º parágrafo com conteúdo (título);
#   · tudo o que sobrou nele é linha CURTA (loja, cupom, preço da loja);
#     linha longa é descrição e preserva o parágrafo;
#   · outro parágrafo manteve link PUBLICÁVEL (a oferta continua).
# Fora disso, comportamento de antes.
# ─────────────────────────────────────────────────────────────────
_SECAO_LINHA_CURTA = 40


def _tem_url(bloco: list) -> bool:
    return bool(bloco) and bool(urls_do_bloco(" ".join(bloco)))


def _secoes_de_loja_sem_conversao(blocos: list, mantidos: list, mapa: dict,
                                  preservar) -> set:
    paragrafos, atual = [], []
    for i, b in enumerate(blocos):
        if not b:
            if atual:
                paragrafos.append(atual)
                atual = []
            continue
        atual.append(i)
    if atual:
        paragrafos.append(atual)

    def publicavel(i):
        return mantidos[i] and any(_publicavel(u, mapa, preservar)
                                   for u in urls_do_bloco(" ".join(blocos[i])))

    fora = set()
    for n, par in enumerate(paragrafos):
        if n == 0:
            continue                                    # título
        removeu = any(not mantidos[i] and _tem_url(blocos[i]) for i in par)
        sobrou_link = any(mantidos[i] and _tem_url(blocos[i]) for i in par)
        if not removeu or sobrou_link:
            continue
        curtas = all(len(sem_marcacao(l).strip()) <= _SECAO_LINHA_CURTA
                     for i in par if mantidos[i] for l in blocos[i])
        if not curtas:
            continue
        if any(publicavel(i) for m, q in enumerate(paragrafos) if m != n for i in q):
            fora.update(par)
    return fora


def filtrar_blocos(texto: str, mapa: dict, preservar=()) -> str:
    """Remove os blocos que não geraram oferta publicável.

    Consome o mapa de publicação produzido pela conversão. Depois de
    remover, renumera, elimina rótulos órfãos e colapsa linhas
    vazias, para que o texto pareça ter nascido assim.
    """
    originais = _segmentar(texto)
    mantidos = [not b or _bloco_permanece(b, mapa, preservar) for b in originais]
    fora = _secoes_de_loja_sem_conversao(originais, mantidos, mapa, preservar)
    blocos = [b for i, b in enumerate(originais)
              if mantidos[i] and i not in fora]
    # O TÍTULO (1º bloco com conteúdo) não é rótulo órfão enquanto houver
    # conteúdo depois dele: "🔥 Cupons do Mercado Livre ativos agora:"
    # seguido de linha em branco e da lista saía do post (id=1368, 02/10)
    # — e, sem título, o post de cupom deixava de ser reconhecido.
    titulo = next((i for i, b in enumerate(blocos) if b), None)
    blocos = [b for i, b in enumerate(blocos)
              if not (b and _rotulo_orfao(b)
                      and not (i == titulo and any(blocos[i + 1:])))]

    # Renumerar só faz sentido depois de REMOVER. É a própria razão de
    # ser de _renumerar: "numeração quebrada denuncia remoção". Sem
    # remoção não há numeração quebrada, e reescrever a enumeração de
    # um texto íntegro só pode piorá-la — _renumerar conta enumeração
    # por BLOCO (b[0]), enquanto a fonte enumera por LINHA. Nas formas
    # "rótulo: / 1 url / 2 url" e "1️⃣/2️⃣/3️⃣" o primeiro item fica
    # invisível para a contagem, len(idx)==1 dispara e o número de um
    # item legítimo é apagado.
    if len(blocos) != len(originais):
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

