# pipeline/assunto_oferta.py — DESCRIÇÃO DA OFERTA DECLARADA NO POST
#
# Responsabilidade ÚNICA: dado o cabeçalho do post (janela de
# _ESCOPO_BENEFICIO chars), descrever a OFERTA — seu benefício, seu
# tema de campanha, se há preço de item e se o benefício é de loja.
#
# A janela de análise é parâmetro INTERNO deste módulo: as três
# funções que a usam moram todas aqui. Ela não atravessa fronteira
# e não é reexportada.
#
# NÃO classifica espécie de assunto (cupom/cashback/evento): isso é
# pipeline.assunto_especie. Não importa aquele módulo, nem é
# importado por ele. Não conhece _KW_CUPOM nem _KW_EVENTO. CONSOME de
# utils.cupom a evidência ESTRUTURAL de item de cupom
# (linha_e_item_de_cupom) — recorte de linha, não vocabulário.
#
# Camada PURA: zero I/O. As regex NÃO saem daqui; o contrato público
# da camada é reexportado por pipeline.assunto.
#
# Extraído de pipeline.assunto sem qualquer alteração de comportamento.
from __future__ import annotations

import re
import unicodedata
from typing import Optional
from pipeline.estado_evento import _RE_RETORNO
from utils.cupom import linha_declara_cupom, linha_e_item_de_cupom

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


def _faixas_de_item_cupom(t: str, codigos=()) -> list:
    """Intervalos [inicio, fim) das linhas que são ITEM DE CUPOM.

    [F-C6] Terceira forma do mesmo conceito. Numa linha cuja ESTRUTURA
    é a de um item de cupom, todo valor monetário pertence ao cupom —
    um é o benefício, o outro é a sua condição. Nenhum descreve o preço
    de um produto. Vale mesmo sem o literal OFF, que é o que as duas
    formas anteriores (_RE_DESC_VAL e _COND_PISO/_COND_TETO) exigiam.

    A evidência estrutural é SOBERANIA de utils.cupom
    (linha_e_item_de_cupom). Este módulo CONSOME o predicado; não redefine o
    padrão nem passa a conhecer vocabulário de palavra-chave.

    Exclusão POR LINHA: preço de produto em qualquer outra linha
    continua contando normalmente.

    `codigos` (os T0 da mensagem): a linha que declara um deles com
    marcador de desconto também é item de cupom, qualquer que seja o
    separador (utils.cupom.linha_declara_cupom). Vazio: só a forma
    "…: COD", exatamente como antes.
    """
    faixas = []
    base = 0
    for linha in t.splitlines(keepends=True):
        if linha_e_item_de_cupom(linha) or linha_declara_cupom(linha, codigos):
            faixas.append((base, base + len(linha)))
        base += len(linha)
    # LISTA, não linha solta. "Fone R$ 199: JBL10" é ambíguo — pode ser
    # produto com cupom. Duas ou mais linhas com a mesma forma são uma
    # tabela de cupons. Abaixo de dois, nada é excluído e a leitura
    # anterior prevalece.
    return faixas if len(faixas) >= 2 else []


def tem_preco_de_item(t: str, codigos=()) -> bool:
    """Existe valor R$ que seja preço do PRODUTO — isto é, que não seja
    desconto (OFF), condição do benefício (piso/teto), nem valor de uma
    linha estruturalmente reconhecida como item de cupom? `codigos`:
    ver _faixas_de_item_cupom."""
    descontos = {m.start() for m in _RE_DESC_VAL.finditer(t)}
    condicoes = _condicoes_do_beneficio(t)
    itens_cupom = _faixas_de_item_cupom(t, codigos)
    for m in _RE_PRECO_ITEM.finditer(t):
        if any(ini <= m.start() < fim for ini, fim in itens_cupom):
            continue
        if any(abs(m.start() - d) <= 4 for d in descontos):
            continue
        if any(abs(m.start() - c) <= 2 for c in condicoes):
            continue
        return True
    return False


def beneficio_e_de_loja(texto: str, codigos=()) -> bool:
    """
    Discriminador R1×R2 — executa a TABELA do MB ratificado:
      · escopo-loja explícito ("em tudo", "no app", "acima de R$",
        "sem mínimo", "1ª compra") + benefício (%, R$-OFF, frete) → LOJA
      · benefício de desconto SEM preço de item no post → LOJA
        (o MB: "X% OFF ... sem preço de item → R2"; nada vende o item)
      · qualquer preço de item presente e sem escopo → dúvida → ITEM
        (zona cinzenta ratificada: prevalece o produto)
    O dono desta regra é o MB — este código apenas a executa.
    `codigos`: ver _faixas_de_item_cupom.
    """
    t = texto[:_ESCOPO_BENEFICIO]
    beneficio = bool(_RE_BEN_PCT.search(t) or _RE_BEN_VLR.search(t)
                     or _RE_BEN_FRETE.search(t))
    if not beneficio:
        return False
    if _RE_ESCOPO_LOJA.search(t):
        return True
    return not tem_preco_de_item(t, codigos)


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


def tema_da_campanha(texto: str, plat: str = "") -> str:
    """
    [F-C4] Identidade da campanha SEM código (INV-E3): o elemento
    textual mais estável — o nome que as fontes repetem ao falar da
    mesma campanha. Sem tema reconhecível → "geral" (bucket temporal
    da plataforma; tolerância conservadora ratificada, R5).
    Números NUNCA entram (INV-E2: limites/percentuais são estado).

    [Cupom sem código] Dois níveis, nesta ordem:
      1. NOME DECLARADO no título (`nome_declarado`) — só com `plat`;
      2. vocabulário legado sobre o escopo do benefício (intocado).
    Sem `plat`, só o nível 2: exatamente o comportamento anterior.
    """
    if plat:
        nome = nome_declarado(texto, plat)
        if nome:
            return nome
    m = _RE_TEMA_CAMPANHA.search(texto[:_ESCOPO_BENEFICIO])
    if not m:
        return "geral"
    bruto = re.sub(r"\s+", " ", m.group(1).lower())
    return _TEMAS_CAMPANHA.get(bruto, "geral")


# ══════════ CUPOM SEM CÓDIGO — NOME DECLARADO DA CAMPANHA ══════════
# Incidente 01/10 00:00: Fidelidade, Tech e "R$30/R$90" — campanhas
# distintas, sem código — caíram todas em `cupb|geral` porque o nome
# delas não está no vocabulário fechado acima. As fontes DECLARAM o nome
# no título ("Novos Cupons Shopee Fidelidade (APP)", "Novo Cupom Shopee
# Tech"); o que falta é reconhecer a declaração.
#
# A pergunta é estrutural — "este título DECLAROU um nome?" — e só
# depois vem a chave. Cada palavra do título é de UMA classe; nome é o
# que SOBRA quando todas as outras saem:
#   plataforma ........ o próprio `plat` (pode vir separado: "mercado livre")
#   substantivo cupom . cupom, cupons, cupão, cuponzão
#   moldura ........... novidade, estado, urgência, chamada, horário
#   reativação ........ vocabulário CANÔNICO (estado_evento._RE_RETORNO)
#                       + a família de "renovar"
#   benefício/valor ... R$, %, OFF, número, limite, mínimo, desconto
#   qualificador ...... entre parênteses: "(APP)", "(Site)"
#   conectivo ......... de, em, para, nos, ...
# Declarou nome ⇔ sobram de 1 a 3 palavras de ≥ 3 letras. Nada sobrou →
# genérico. Mais de 3 → prosa ou título de produto, não nome.
# A chave é a sobra INTEIRA (nunca "a primeira palavra"): Black Friday ≠
# Black Week, Clube Fidelidade ≠ Fidelidade. Na dúvida, não funde.
#
# Vocabulário é DETALHE DE IMPLEMENTAÇÃO (mesma doutrina de
# _TEMAS_CAMPANHA): cada entrada tem evidência no corpus Shopee de
# 31/08–01/10 (Fada, Samuel, Promotom, Fumotom).
_NOME_CUPOM = frozenset({"cupom", "cupons", "cupao", "cuponzao", "cupoes"})
_NOME_MOLDURA = frozenset({
    # novidade / estado
    "novo", "nova", "novos", "novas", "liberado", "liberada", "liberados",
    "liberadas", "ativo", "ativa", "ativos", "ativas", "exclusivo",
    "exclusiva", "exclusivos", "confirmado", "confirmados", "disponivel",
    "disponiveis", "geral",
    # urgência / chamada
    "esgotando", "ultima", "ultimo", "chance", "vai", "sair", "comecou",
    "comeca", "lembrando", "lembrete", "alerta", "atencao", "corre",
    "urgente", "imperdivel", "resgate", "resgata", "aqui",
    # horário
    "hoje", "agora", "amanha", "meia", "noite", "partir", "horas",
    # canal de resgate / autorreferência do grupo
    "app", "site", "link", "links", "nosso", "nossa", "nossos", "nossas",
    # família de "renovar" (fora do vocabulário canônico de retorno)
    "renovado", "renovada", "renovados", "renovadas", "renova", "renovar",
    "renovou",
})
_NOME_CONECTIVO = frozenset({
    "de", "do", "da", "dos", "das", "em", "no", "na", "nos", "nas", "para",
    "pra", "com", "por", "ate", "e", "o", "a", "os", "as", "um", "uma",
    "seu", "sua",
})
# Benefício, percentual e valor/limite NUNCA viram nome (INV-E2).
_RE_NOME_VALOR = re.compile(
    r"r\$\s*\d[\d.,]*|\d+\s*%|\b\d+(?:[.,/]\d+)*\b|"
    r"\b(?:off|limite|limitad[oa]|acima|minimo|desconto|descontos|"
    r"cashback)\b")
# Data de EVENTO ("9.9", "10.10"): é o nome quando nada mais sobra
# ("CUPONS RENOVADOS! 9.9 SHOPEE"). Não é valor: não tem R$ nem %.
_RE_NOME_EVENTO = re.compile(r"(?<![\d$])\b(\d{1,2}\.\d{1,2})\b(?![.,]?\d|\s*%)")
_NOME_MIN_LETRAS = 3
_NOME_MAX_PALAVRAS = 3


def _sem_acento(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")


def _linha_titulo(texto: str) -> str:
    """1ª linha não vazia. Espaço duplo também encerra o título: há
    fonte que cola o corpo no título sem quebra de linha."""
    for linha in (texto or "").split("\n"):
        if linha.strip():
            return re.split(r"\s{2,}", linha.strip())[0]
    return ""


def nome_declarado(texto: str, plat: str) -> str:
    """Nome de campanha DECLARADO no título, ou "" se não houver.

    Responde primeiro "o título declarou um nome?" — e só então gera a
    chave. Portão: o título é cabeçalho de campanha da PRÓPRIA
    plataforma (cita `plat`); sem isso não há declaração."""
    plat = _sem_acento((plat or "").lower())
    if not plat:
        return ""
    t = _sem_acento(_linha_titulo(texto).lower())
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"\([^)]*\)", " ", t)
    # a plataforma pode vir com separação ("mercado livre")
    re_plat = re.compile(r"\b" + r"[\s\-_.]*".join(map(re.escape, plat)) + r"\b")
    if not re_plat.search(t):
        return ""
    t = re_plat.sub(" ", t)
    # valor monetário sai ANTES da busca de evento: "R$ 9.90" não é 9.9
    t = re.sub(r"r\$\s*\d[\d.,]*", " ", t)
    eventos = _RE_NOME_EVENTO.findall(t)
    t = _RE_RETORNO.sub(" ", t)
    t = _RE_NOME_VALOR.sub(" ", t)
    sobra = [w for w in re.findall(r"[a-z]+", t)
             if len(w) >= _NOME_MIN_LETRAS
             and w not in _NOME_CUPOM
             and w not in _NOME_MOLDURA
             and w not in _NOME_CONECTIVO]
    if not sobra:
        return eventos[0] if eventos else ""
    if len(sobra) > _NOME_MAX_PALAVRAS:
        return ""
    # o nome que JÁ era tema legado mantém a chave de sempre
    return _TEMAS_CAMPANHA.get(" ".join(sobra), "-".join(sobra))


# ══════════ CUPOM SEM CÓDIGO E SEM NOME — ASSINATURA DO BENEFÍCIO ══════════
# Sem código e sem nome declarado, o que a fonte declara da campanha é o
# BENEFÍCIO: "R$30 OFF em R$299", "20% OFF limitado a R$20". No corpus,
# a mesma campanha traz os MESMOS valores em todos os grupos (FADA =
# SAMUEL = PROMOTOM em 100% dos pares); campanhas simultâneas diferem
# justamente neles (30/299 × 10/119 × 20%-lim-20 na mesma madrugada).
#
# Cada cupom declarado vira UMA assinatura, como cada código vira uma
# chave. É ÂNCORA FRACA (cupb), com a semântica de sempre: encontra a
# família por overlap; nunca é identidade estrutural (Frente 8).
# Refina INV-E2 no molde do cashback (`cash|<pct>`): o valor segue
# sendo ESTADO onde há nome ou código; aqui é o único discriminante.
_RE_ASS_PAR = re.compile(
    r"r\$\s*(\d[\d.,]*)\s*off\s*(?:em\s+compras\s+acima\s+de|acima\s+de|em)"
    r"\s*r\$\s*(\d[\d.,]*)")
_RE_ASS_PCT = re.compile(
    r"\b(\d{1,3})\s*%\s*off"
    r"(?:\s*,?\s*(?:limite|limitad[oa]\s+a)\s*(?:de\s+)?r\$\s*(\d[\d.,]*))?")
_RE_ASS_VLR = re.compile(r"r\$\s*(\d[\d.,]*)\s*off")
# "Cupom Shopee 20/69" no título: R$20 OFF em R$69. O mínimo maior que
# o dobro do desconto separa da data ("30/09").
_RE_ASS_ABREV = re.compile(r"(?<![\d/.,])(\d{1,4})/(\d{2,5})(?![\d/])")


def _valor(bruto: str) -> str:
    """Valor canônico: "R$ 1.299" = "R$1299"; "69,90" = "69.9"."""
    v = bruto.strip(".,")
    if re.fullmatch(r"\d{1,3}(?:\.\d{3})+", v):
        v = v.replace(".", "")
    v = v.replace(",", ".")
    try:
        n = float(v)
    except ValueError:
        return v
    return str(int(n)) if n == int(n) else f"{n:g}"


def assinatura_do_beneficio(texto: str) -> tuple:
    """Assinaturas dos cupons declarados no escopo do benefício, em
    ordem canônica: `v:<desconto>-<mínimo>`, `p:<pct>[-<limite>]` e,
    só sem nenhuma das duas, `v:<desconto>`. Vazio = nada declarado."""
    t = _sem_acento(texto[:_ESCOPO_BENEFICIO].lower())
    ass = {f"v:{_valor(a)}-{_valor(b)}" for a, b in _RE_ASS_PAR.findall(t)}
    for a, b in _RE_ASS_ABREV.findall(_sem_acento(_linha_titulo(texto))):
        if int(a) >= 5 and int(b) > 2 * int(a):
            ass.add(f"v:{a}-{b}")
    for p, lim in _RE_ASS_PCT.findall(t):
        if _PCT_MIN <= int(p) <= _PCT_MAX:
            ass.add(f"p:{p}-{_valor(lim)}" if lim else f"p:{p}")
    if not ass:
        ass = {f"v:{_valor(a)}" for a in _RE_ASS_VLR.findall(t)}
    return tuple(sorted(ass))
