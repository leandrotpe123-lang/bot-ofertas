"""Contexto do dia, em tempo real e sem banco — decisão do dono (16/10).

O Brain olha o canal (e, quando precisa, as fontes) pela prévia pública e
guarda SÓ EM RAM o que leu nas últimas 24 h CORRIDAS — nunca o dia do
calendário: os casos fortes cruzam a meia-noite (rodada Shopee de 00h BRT,
relâmpago ML "use meia-noite"). Ao reiniciar, relê a prévia. Nada de banco,
arquivo ou volume para isso.

Para cada ALVO, o código escolhe no dia os candidatos relevantes, no máximo
N, nesta ordem de força:
  1. mesmo link (convertido)        P4: indício forte de mesma campanha
  2. mesmo código (entre crases)    P7: código igual = mesma campanha
  3. mesmo produto (título)         P1: mesmo produto e preço = duplicata
  4. texto parecido                 seco × específico, mesma campanha (P5)
  5. link de página                 o mesmo link em muitos posts do dia
                                    (página de cupons, tutorial): sinal fraco
  6. mesma plataforma               o resto do dia
Os dois sinais fracos (5 e 6) só valem entre posts do mesmo tipo (cupom ×
cupom, produto × produto).
Dentro de cada motivo: mais parecido e mais perto no tempo primeiro
(semelhança − λ·Δ/24 h). Medido nos casos do dono (07–16/10): sem o "link de
página" e sem o peso do tempo, o link de página enchia os 15 lugares e o par
certo ficava de fora em 28 de 98 casos.
A IA nunca recebe o dia inteiro, só esses N, cada um com o motivo da escolha.

Puro: sem rede, sem banco, sem log. Quem lê a prévia alimenta a Janela."""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable, Optional

JANELA_S = 24 * 3600             # 24 h corridas (nunca o dia do calendário)
N_CANDIDATOS = 15                # teto de candidatos por chamada
MAX_ITENS = 20_000               # guarda de memória (24 h reais: ~1–4 mil)
TEXTO_PARECIDO_MIN = 0.35        # Jaccard das palavras (sem links)
PRODUTO_PARECIDO_MIN = 0.75      # Jaccard das palavras do título
LINK_RARO_MAX = 3                # link em até 3 posts do dia = "mesmo link"
PESO_TEMPO = 1.0                 # λ: 24 h de distância valem 1,0 de semelhança

MOTIVOS = ("mesmo_link", "mesmo_codigo", "mesmo_produto", "texto_parecido",
           "link_de_pagina", "mesma_plataforma")

# Domínio do link → plataforma. Inclui os encurtadores que o canal publica
# depois da conversão (meli.la, s.shopee.com.br, link.amazon…).
_DOMINIOS = (
    ("mercadolivre", ("meli.la", "mercadolivre.com", "mercadolibre.com", "mercadopago.com")),
    ("shopee", ("shopee.com", "shope.ee", "shp.ee")),
    ("amazon", ("amzn.to", "amazon.com", "link.amazon", "a.co")),
    ("magalu", ("magazineluiza.com", "magalu.com", "mglu.co")),
    ("netshoes", ("netshoes.com",)),
)
_NOMES = (
    ("mercadolivre", ("mercado livre", "meli")),
    ("shopee", ("shopee",)),
    ("amazon", ("amazon",)),
    ("magalu", ("magalu", "magazine luiza")),
    ("netshoes", ("netshoes",)),
)
_RE_URL = re.compile(r"https?://\S+|\b[\w.-]+\.(?:com|br|la|to|ee|co)(?:\.br)?/\S*", re.I)
_RE_PALAVRA = re.compile(r"[a-z0-9]+")
_RE_PRECO = re.compile(r"r\$\s*\d")
_RE_TITULO_CUPOM = re.compile(r"\bcupo(?:m|ns)\b")
_PARADAS = frozenset({
    "de", "da", "do", "das", "dos", "em", "no", "na", "nos", "nas", "para",
    "pra", "com", "por", "e", "o", "a", "os", "as", "um", "uma", "aqui",
    "r", "link", "anuncio", "resgate", "clique", "acesse", "compre",
})


@dataclass(frozen=True)
class Post:
    """Um post do canal ou uma mensagem de fonte, como a prévia mostra."""
    id: str                        # "canal:29047" | "fonte:promotom/111375"
    origem: str                    # "canal" | "fonte"
    ts: float                      # data da prévia (epoch, UTC)
    texto: str
    links: tuple = ()
    codigos: tuple = ()            # SÓ o que veio entre crases (<code>)
    grupo: str = ""                # fonte de origem, quando conhecida
    imagem: str = ""               # URL da imagem na prévia
    estado: str = "vivo"           # vivo | apagado ("Post not found")
    plataforma: str = ""           # conhecida pela fonte; "" = deduzir do link/texto


@dataclass(frozen=True)
class Candidato:
    post: Post
    motivo: str                    # um de MOTIVOS (o mais forte que vale)
    semelhanca: float              # 0–1, texto (ou título, se produto)
    delta_s: float                 # alvo.ts − post.ts (> 0: antes do alvo)


# ─────────────────────────────────────────────────────────────────
# Sinais (puros)
# ─────────────────────────────────────────────────────────────────
def _sem_acento(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")


def _raiz(w: str) -> str:
    """Plural não separa: "cupons" = "cupom", "ofertas" = "oferta"."""
    if len(w) > 4 and w.endswith("ns"):
        return w[:-2] + "m"
    if len(w) > 4 and w.endswith("s") and not w.endswith("ss"):
        return w[:-1]
    return w


def palavras(texto: str) -> frozenset:
    """Palavras do texto sem links, acento, caixa, plural nem palavras vazias."""
    t = _sem_acento(_RE_URL.sub(" ", texto or "").lower())
    return frozenset(_raiz(w) for w in _RE_PALAVRA.findall(t)
                     if w not in _PARADAS and (len(w) >= 3 or w.isdigit()))


def jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def link_normalizado(url: str) -> str:
    """O link como identidade: sem espaço, fragmento nem barra final. O
    caminho do encurtador diferencia maiúscula (meli.la/1yww8rC)."""
    u = (url or "").strip().split("#", 1)[0].rstrip("/")
    m = re.match(r"(?i)(https?://)([^/]+)(.*)", u)
    return (m.group(2).lower() + m.group(3)) if m else u


def plataforma(post: Post) -> str:
    """A informada; senão pelo domínio dos links; sem link reconhecido, pelo
    nome no texto."""
    if post.plataforma:
        return post.plataforma
    for url in post.links:
        host = link_normalizado(url).split("/", 1)[0]
        host = host[:-3] if host.endswith(".br") else host     # s.shopee.com.br
        for plat, doms in _DOMINIOS:
            if any(host == d or host.endswith("." + d) for d in doms):
                return plat
    t = _sem_acento((post.texto or "").lower())
    for plat, nomes in _NOMES:
        if any(n in t for n in nomes):
            return plat
    return ""


def _titulo(texto: str) -> str:
    for linha in (texto or "").split("\n"):
        if linha.strip():
            return linha
    return ""


def eh_produto(texto: str) -> bool:
    """Post de produto: tem linha de preço ("✅ R$ 64") que não é benefício
    de cupom (sem "OFF" nem "%"). Título com "cupom" é cupom, mesmo listando
    produtos com preço (canal 26449/27937 "Cupom Magalu … em Bicicletas",
    26856 "Prime 3 meses por R$ 9,90"): medido no histórico 07–16/10, 262 de
    262 títulos com "cupom" são post de cupom."""
    if _RE_TITULO_CUPOM.search(_sem_acento(_titulo(texto).lower())):
        return False
    for linha in (texto or "").split("\n"):
        l = _sem_acento(linha.lower())
        if _RE_PRECO.search(l) and "off" not in l and "%" not in l and len(l) <= 60:
            return True
    return False


def codigos(post: Post) -> frozenset:
    return frozenset(c.strip().upper() for c in post.codigos if c.strip())


# ─────────────────────────────────────────────────────────────────
# Janela — o que o Brain leu nas últimas 24 h, SÓ em RAM
# ─────────────────────────────────────────────────────────────────
class Janela:
    """Memória de trabalho do dia. `ver` registra (edição substitui pelo
    mesmo id); `apagado` marca "Post not found"; `podar` esquece o que
    passou de 24 h. Reiniciou: começa vazia e a prévia é relida."""

    def __init__(self, janela_s: float = JANELA_S, max_itens: int = MAX_ITENS):
        self.janela_s = janela_s
        self.max_itens = max_itens
        self._itens: dict = {}

    def ver(self, post: Post) -> None:
        self._itens[post.id] = post
        if len(self._itens) > self.max_itens:          # guarda: esquece o mais velho
            mais_velho = min(self._itens.values(), key=lambda p: p.ts)
            del self._itens[mais_velho.id]

    def apagado(self, post_id: str) -> None:
        p = self._itens.get(post_id)
        if p is not None:
            self._itens[post_id] = Post(**{**p.__dict__, "estado": "apagado"})

    def podar(self, agora: float) -> None:
        corte = agora - self.janela_s
        for pid in [k for k, p in self._itens.items() if p.ts < corte]:
            del self._itens[pid]

    def itens(self) -> list:
        return list(self._itens.values())

    def __len__(self) -> int:
        return len(self._itens)

    def candidatos(self, alvo: Post, n: int = N_CANDIDATOS) -> list:
        return candidatos(alvo, self._itens.values(), n=n, janela_s=self.janela_s)


# ─────────────────────────────────────────────────────────────────
# Escolha dos candidatos de um alvo
# ─────────────────────────────────────────────────────────────────
def _motivo(alvo: Post, p: Post, pal_alvo, prod_alvo, plat_alvo, cod_alvo,
            links_raros, links_comuns) -> Optional[tuple]:
    """(motivo, semelhança) do sinal mais forte que liga p ao alvo; None se
    nenhum (outra plataforma, sem link/código/texto em comum)."""
    sem = jaccard(pal_alvo, palavras(p.texto))
    links_p = {link_normalizado(u) for u in p.links}
    if links_raros & links_p:
        return "mesmo_link", sem
    if cod_alvo and cod_alvo & codigos(p):
        return "mesmo_codigo", sem
    if prod_alvo is not None and eh_produto(p.texto):
        sem_t = jaccard(prod_alvo, palavras(_titulo(p.texto)))
        if sem_t >= PRODUTO_PARECIDO_MIN:
            return "mesmo_produto", sem_t
    if sem >= TEXTO_PARECIDO_MIN:
        return "texto_parecido", sem
    # Sinais fracos só entre posts do MESMO tipo (cupom × cupom, produto ×
    # produto): o link da página de cupons vai em todo post de produto da
    # Shopee ("Resgate todos os cupons"), e um produto nunca é o mesmo
    # anúncio de um cupom.
    if eh_produto(p.texto) != (prod_alvo is not None):
        return None
    if links_comuns & links_p:
        return "link_de_pagina", sem
    if plat_alvo and plataforma(p) == plat_alvo:
        return "mesma_plataforma", sem
    return None


def candidatos(alvo: Post, itens: Iterable[Post], n: int = N_CANDIDATOS,
               janela_s: float = JANELA_S) -> list:
    """Até `n` posts das 24 h CORRIDAS antes do alvo (vivos, outro id),
    ordenados pela força do motivo; dentro do motivo, pela semelhança menos
    o peso da distância no tempo."""
    pal_alvo = palavras(alvo.texto)
    prod_alvo = palavras(_titulo(alvo.texto)) if eh_produto(alvo.texto) else None
    plat_alvo = plataforma(alvo)
    cod_alvo = codigos(alvo)
    links_alvo = {link_normalizado(u) for u in alvo.links}
    janela = [p for p in itens
              if p.id != alvo.id and p.estado == "vivo"
              and 0 <= alvo.ts - p.ts <= janela_s]          # o mesmo segundo entra; depois, não
    uso = {}
    for p in janela:
        for l in {link_normalizado(u) for u in p.links} & links_alvo:
            uso[l] = uso.get(l, 0) + 1
    links_raros = {l for l in links_alvo if uso.get(l, 0) <= LINK_RARO_MAX}
    links_comuns = links_alvo - links_raros
    achados = []
    for p in janela:
        m = _motivo(alvo, p, pal_alvo, prod_alvo, plat_alvo, cod_alvo, links_raros, links_comuns)
        if m is not None:
            achados.append(Candidato(p, m[0], round(m[1], 3), alvo.ts - p.ts))
    achados.sort(key=lambda c: (MOTIVOS.index(c.motivo),
                                -(c.semelhanca - PESO_TEMPO * c.delta_s / janela_s), c.delta_s))
    return achados[:n]


__all__ = ["Post", "Candidato", "Janela", "candidatos", "plataforma", "palavras",
           "jaccard", "link_normalizado", "eh_produto", "codigos", "JANELA_S",
           "N_CANDIDATOS", "MOTIVOS", "LINK_RARO_MAX", "PESO_TEMPO"]
