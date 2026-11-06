"""Comentário da fonte → SINAL OBJETIVO de oferta, ou recusa. PURO.

Sem Telegram, sem banco, sem log: recebe texto e devolve decisão. Quem
orquestra (ler o evento, achar o post, responder) é pipeline.comentarios.

O que é "comentário" aqui: a mensagem do PRÓPRIO canal-fonte respondendo
(marcando) um post dele — "Precinho", "Esgotado ❌", "VOLTOU!" (prints do
Léo, 04/11; corpus das prévias públicas: 183 respostas sem link nas fontes
que publicam). Não é a área de comentários de usuários (grupo de
discussão): essa nunca é lida.

POLÍTICA FECHADA (decisão do dono, 06/11): só passa o SINAL OBJETIVO —
  preço        precinho, preção, bom/ótimo preço, barato, de graça
  urgência     corre, vai esgotar, esgota rápido, aproveita, só vai
  estoque      esgotado/esgotou, voltou/voltando, ativou
  preço mudou  baixou (inclui "baixou no PIX")
  cupom/bug    liberado/liberou, ainda ativo, disponível, bug
Todo o resto é recusado, mesmo que pareça bom: autopromoção do grupo vem
de jeitos demais, e uma lista aberta uma hora deixa escapar. "Esse aí
forrou... saiu primeiro aqui!" (Promotom 109960, chamando pro Fumotom) é
recusado.

Recusa explícita, mesmo com sinal: o grupo falando de si (grupo, canal,
chat, notificações, "saiu primeiro", nome de grupo...), @ de qualquer
conta, link de loja (comentário com link é OFERTA — vai pelo pipeline,
nunca por aqui), link para OUTRO post, primeira pessoa, pergunta,
palavrão, sinal negado ("n ativou") e texto longo.
"""
from __future__ import annotations

import re
import unicodedata
from typing import NamedTuple, Tuple

TAMANHO_MAX = 80

# Família → padrão, sobre o texto SEM acento e minúsculo.
FAMILIAS = {
    "preco": re.compile(
        r"\bprecin\w*|\bprecao+\b|\b(?:bom|otimo)\s+preco\b|"
        r"\bbarat(?:o|a|os|as|inho|inha|issimo|issima)\b|\bde\s+graca\b"),
    "urgencia": re.compile(
        r"\bcorr+e+\b|\bcorram\b|\bvai\s+esgotar\b|\besgota\s+rapido\b|"
        r"\baproveit(?:a+|em|e)\b|\bso\s+vai\b"),
    "estoque": re.compile(
        r"\besgot(?:ado|ada|ados|adas|ou)\b|\bvolt(?:ou|ando)\b|\bde\s+volta\b|\bativou\b"),
    "preco_mudou": re.compile(r"\bbaixou\b"),
    "cupom_bug": re.compile(
        r"\bliberad[oa]s?\b|\bliberou\b|\bliberaram\b|\bainda\s+ativ[oa]\b|"
        r"\bdisponivel\b|\bbug\b|\bbugad[oa]\b"),
}

_NEGACAO = re.compile(r"\b(?:n|nao|nem|nunca)\s+(?:\w+\s+)?(?:ativou|liberou|voltou|baixou|"
                      r"esgotou|liberad[oa]|disponivel)\b")
# O grupo falando DE SI (nome, convite, comunidade, prioridade).
_AUTOPROMO = re.compile(
    r"\bgrupos?\b|\bcana(?:l|is)\b|\bchat\b|notifica|\bsino\b|🔔|\binscrev|\bbot\b|"
    r"\bsa(?:iu|indo|em|i)\s+(?:em\s+)?primeir|\bprimeira\s+mao\b|\bprimeiro\s+(?:aqui|grupo)\b|"
    r"\bmais\s+rapido\b|\bnoss[oa]s?\b|\bsig(?:a|am)\b|\bsegue\b|"
    r"\bentr(?:e|em|a)\s+(?:no|na|em|aqui)\b|\bcomentario|\breag|\blink\b|\bvcs\b|\bvoces\b|"
    r"\bfumo\b|promotom|fumotom|\bfada\b|fadapromo|fadadoscupo|samuel|\bnerd|convertida|"
    r"chatcupons|cupomlink|clubedotom")
_PRIMEIRA_PESSOA = re.compile(
    r"\b(?:eu|meu|minha|meus|minhas|comprei|garanti|aproveitei|peguei|acho|estou|"
    r"vou|adicionei|aviso|avisarei|consegui)\b")
_PALAVRAO = re.compile(
    r"\b(?:carai|caralh\w*|porra|merda|foda\w*|puta\w*|pqp|krl|crl|vsf|fdp|buceta)\b")
_ARROBA = re.compile(r"(?<![\w.])@\w")
_TME_POST = re.compile(r"(?:https?://)?(?:t\.me|telegram\.(?:me|dog))/(?:s/)?([A-Za-z]\w+)/(\d+)\S*",
                       re.I)
_QUALQUER_URL = re.compile(r"(?:https?://|www\.)\S+|\b(?:t\.me|telegram\.(?:me|dog))/\S+|"
                           r"\b[\w-]+\.(?:com|com\.br|br|net|ly|la|ee|to|io|me|app)(?:/\S*)?", re.I)


def _sa(texto: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", (texto or "").lower())
                   if unicodedata.category(c) != "Mn")


class Decisao(NamedTuple):
    aceito: bool
    motivo: str            # "" quando aceito; o motivo da recusa
    texto: str             # o texto que vai ao ar (original, sem o link do próprio post)
    familias: Tuple[str, ...]


def familias_de(texto: str) -> Tuple[str, ...]:
    n = _sa(texto)
    return tuple(f for f, rx in FAMILIAS.items() if rx.search(n))


def avaliar(texto: str, canal_fonte: str, alvo_msg_id: int) -> Decisao:
    """`canal_fonte`: o @ (sem @) do canal do comentário; `alvo_msg_id`: o
    post que ele responde. Link t.me para ESSE post sai do texto (ele só
    repete a marcação); para qualquer outro post, o comentário fala de
    outra coisa e é recusado."""
    original = (texto or "").strip()
    if not original:
        return Decisao(False, "vazio", "", ())
    if _ARROBA.search(original):
        return Decisao(False, "arroba", "", ())
    alvo = f"{(canal_fonte or '').lower()}/{int(alvo_msg_id)}"
    for m in _TME_POST.finditer(original):
        if f"{m.group(1).lower()}/{m.group(2)}" != alvo:
            return Decisao(False, "link_outro_post", "", ())
    limpo = _TME_POST.sub(" ", original)
    if _QUALQUER_URL.search(limpo):
        return Decisao(False, "link", "", ())
    limpo = "\n".join(" ".join(l.split()) for l in limpo.split("\n") if l.strip())
    limpo = limpo.strip().rstrip(" :-–—→>👇👉").strip()
    if not limpo:
        return Decisao(False, "vazio", "", ())
    n = _sa(limpo)
    if _AUTOPROMO.search(n):
        return Decisao(False, "grupo_falando_de_si", "", ())
    if _PALAVRAO.search(n):
        return Decisao(False, "palavrao", "", ())
    if len(limpo) > TAMANHO_MAX:
        return Decisao(False, "longo", "", ())
    if "?" in limpo:
        return Decisao(False, "pergunta", "", ())
    if _PRIMEIRA_PESSOA.search(n):
        return Decisao(False, "primeira_pessoa", "", ())
    if _NEGACAO.search(n):
        return Decisao(False, "sinal_negado", "", ())
    fams = familias_de(limpo)
    if not fams:
        return Decisao(False, "sem_sinal_objetivo", "", ())
    return Decisao(True, "", limpo, fams)


def ja_no_post(familias, texto_post: str) -> bool:
    """O destaque já está no post: TODA família do comentário já aparece
    no texto publicado ("Precinho" num post que já diz "Precinho!")."""
    n = _sa(texto_post or "")
    return bool(familias) and all(FAMILIAS[f].search(n) for f in familias)
