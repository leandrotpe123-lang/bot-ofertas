"""Pré-filtro (grátis, determinístico): o que vale uma chamada de IA.

v0 só tem o que a prévia pública mostra — texto e links encurtados. Por isso
o tipo do post é inferido do TEXTO (preço de produto × benefício de cupom);
no v1, a identidade vem pronta nos eventos do worker.

Classes (medidas em 7 dias de log, docs/arquitetura/brain-v0.md §A):
  1. post sem âncora de produto × posts sem âncora da mesma plataforma na
     janela (cupom sem código, código diferente, live, destino);
  2. produto × produto da mesma plataforma na janela, título e preço quase
     iguais e link diferente (ex.: o mesmo item em MLB × MLBU);
  3. mensagem de cupom de uma fonte que não virou post próprio.
Fica FORA (determinístico, sem IA): mesmo código de cupom; mesmo link.

Código de cupom segue a doutrina do worker (utils/cupom.py): é SÓ o que foi
marcado como código — na prévia, o `<code>` do HTML (o worker publica o
cupom entre crases). Nada é inferido do texto ("🚨🚨 TUTORIAL:" já virou
cupom falso por inferência, id=1295)."""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

JANELA_S = 3600
SIMILARIDADE_TITULO = 0.8
PRECO_TOLERANCIA = 0.02


def _sa(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", (s or "").lower())
                   if unicodedata.category(c) != "Mn")


_PLAT = (("mercadolivre", r"meli\.la|mercadolivre|mercado livre|\bmeli\b|\bmlb"),
         ("shopee", r"shopee"), ("amazon", r"amzn\.to|amazon"),
         ("magalu", r"magalu|magazine ?luiza"), ("netshoes", r"netshoes"))
RE_PRECO = re.compile(r"r\$\s*(\d[\d.]*(?:,\d{1,2})?)(?!\s*(?:off|de desconto|em compras|acima|em r\$))")
RE_BENEFICIO = re.compile(r"\bcupo(?:m|ns|nzao|es)\b|\bcupao\b|\d+\s*%\s*off|r\$\s*\d[\d.,]*\s*off")
# Forma técnica do recorte, igual a utils/cupom._forma_codigo: 3–20
# caracteres alfanuméricos com - ou _, ao menos uma letra.
RE_FORMA_CODIGO = re.compile(r"^[A-Z0-9][A-Z0-9_-]{2,19}$")
RE_LIVE = re.compile(r"live\.shopee|/live/|\blive\b")


@dataclass(frozen=True)
class PostPrevia:
    id: str
    ts: float
    texto: str
    links: tuple = ()
    origem: str = "canal"          # canal | fonte
    grupo: str = ""
    marcados: tuple = ()           # trechos <code> da prévia (evidência T0)


def plataforma(p: PostPrevia) -> str:
    alvo = _sa(p.texto + " " + " ".join(p.links))
    for nome, rx in _PLAT:
        if re.search(rx, alvo):
            return nome
    return ""


def codigos(p: PostPrevia) -> frozenset:
    """Códigos que a prévia marcou como código; sem marca, nenhum."""
    achados = set()
    for bruto in p.marcados or ():
        c = (bruto or "").strip().upper()
        if RE_FORMA_CODIGO.match(c) and re.search(r"[A-Z]", c):
            achados.add(c)
    return frozenset(achados)


# Post do CANAL sai da montagem do worker (pipeline/montagem_texto.py): o
# papel PREÇO é a linha "✅ R$ …". É a classificação do próprio worker,
# mais fiel que inferir do texto; a mensagem de FONTE não tem esse formato.
RE_LINHA_PRECO = re.compile(r"(?m)^\s*(?:✅|💵)\s*R\$\s*\d")


def tipo(p: PostPrevia) -> str:
    t = _sa(p.texto)
    if RE_LIVE.search(t + " " + " ".join(p.links).lower()):
        return "live"
    if p.origem == "canal":
        if RE_LINHA_PRECO.search(p.texto or ""):
            return "produto"
    elif RE_PRECO.search(t):
        return "produto"
    if RE_BENEFICIO.search(t):
        return "cupom"
    return "outro"


def preco(p: PostPrevia):
    m = RE_PRECO.search(_sa(p.texto))
    if not m:
        return None
    try:
        return float(m.group(1).replace(".", "").replace(",", "."))
    except ValueError:
        return None


def titulo(p: PostPrevia) -> frozenset:
    linha = next((x for x in (p.texto or "").split("\n") if x.strip()), "")
    return frozenset(w for w in re.findall(r"[a-z0-9]+", _sa(linha)) if len(w) > 2)


def _jaccard(a: frozenset, b: frozenset) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def _vizinhos(alvo: PostPrevia, vivos, filtro):
    pa = plataforma(alvo)
    return [v for v in vivos if v.id != alvo.id and plataforma(v) == pa and pa
            and 0 <= alvo.ts - v.ts <= JANELA_S and filtro(v)]


def classe1(alvo: PostPrevia, vivos) -> list:
    """Sem âncora de produto × sem âncora da mesma plataforma na janela;
    mesmo código de cupom fica fora (junção determinística)."""
    if tipo(alvo) == "produto":
        return []
    ca = codigos(alvo)
    return _vizinhos(alvo, vivos, lambda v: tipo(v) != "produto"
                     and not (ca and ca & codigos(v)))


def classe2(alvo: PostPrevia, vivos) -> list:
    """Produto × produto: título quase igual e mesmo preço, link diferente."""
    if tipo(alvo) != "produto":
        return []
    ta, pa = titulo(alvo), preco(alvo)

    def parecido(v):
        pv = preco(v)
        return (tipo(v) == "produto" and set(v.links) != set(alvo.links)
                and _jaccard(ta, titulo(v)) >= SIMILARIDADE_TITULO
                and pa is not None and pv is not None
                and abs(pa - pv) <= PRECO_TOLERANCIA * max(pa, pv))
    return _vizinhos(alvo, vivos, parecido)


def classe3(msg_fonte: PostPrevia, vivos, virou_post: bool) -> list:
    """Mensagem de cupom de fonte que NÃO virou post próprio: confere contra
    os posts sem âncora da plataforma (liberação perdida?). Mesmo código de
    um post vivo = junção certa, fica fora."""
    if virou_post or tipo(msg_fonte) != "cupom":
        return []
    cm = codigos(msg_fonte)
    if cm and any(cm & codigos(v) for v in vivos):
        return []
    return _vizinhos(msg_fonte, vivos, lambda v: tipo(v) != "produto")
