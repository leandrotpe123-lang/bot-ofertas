"""
Camada 5 — Montagem: RETENÇÃO DE CUPONS na edição por link de lista.

Responsabilidade ÚNICA: compor o texto de um post de cupom quando a
versão COM DESTINO (cada cupom com a sua lista) chega sobre o post que
só tinha o MECANISMO (`/sec/`, código solto) — e não traz todos os
cupons que ele exibe.

O caso real (01/10): Promotom 110373 publica 12 códigos com um `/sec/`;
o Samuel 118773 e a Fada 17530 trazem os mesmos cupons, cada um com a
sua lista, mas não TODOS (faltavam PROMOAQU, DESCONTOSMELI, OFERTA…).
Destino prevalece sobre mecanismo (decisao, DECISÃO 0), mas outra fonte
nunca faz cupom exibido sumir (Frente 8b) — então a edição era
bloqueada e o post ficava sem as listas.

Composição: o texto da versão com listas + um bloco final, sob a MARCA,
com as linhas do texto PUBLICADO que mantêm no ar os cupons que só ele
exibia e o link de resgate deles. O conjunto exibido passa a ser a
união — cobre tudo o que estava no ar e acrescenta a lista: AMPLIA, a
única evolução legítima (doutrina, invariante 4).

PURO: não decide nada (quem decide é pipeline.decisao, sobre a
composição do conjunto composto), não lê banco, não loga. Devolve
None quando não dá para reter com segurança — e então vale o de
sempre (a edição é bloqueada, nada some):
  · algo exibido que falta não é cupom com código (produto, destino);
  · um código que falta não está no texto publicado;
  · o texto composto não cabe no limite da mensagem.

SINCRONIZAÇÃO do líder (a fonte da lista edita a própria mensagem):
o bloco sob a MARCA não é dela — o espelho reescreve só a parte do
líder e mantém o bloco, menos os códigos que o líder passou a trazer.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional

from pipeline.normalizacao_texto import sem_marcacao
from utils import marcacao

__all__ = ["MARCA", "LIMITE_LEGENDA", "LIMITE_TEXTO", "Retencao",
           "reter", "manter"]

MARCA = "➕ Mais cupons ativos:"
LIMITE_LEGENDA = 1024     # legenda de mídia no Telegram
LIMITE_TEXTO = 4096       # mensagem de texto

_RE_URL = re.compile(r"https?://[^\s)\]]+")
_RE_DESCONTO = re.compile(r"\bOFF\b|%|\bdesconto\b", re.I)


@dataclass(frozen=True)
class Retencao:
    texto: str                 # o texto composto que vai ao ar
    retidas: tuple             # chaves `<plat>|cup|<COD>` mantidas no ar


def reter(texto_candidato: str, texto_publicado: str, faltantes: Iterable[str],
          *, limite: int) -> Optional[Retencao]:
    """Versão com destino sobre post só-mecanismo: o candidato + as
    linhas do texto publicado que mantêm no ar os `faltantes` (chaves
    fortes que o post exibe e o candidato não traz) e o link de resgate
    deles. None se não der para reter com segurança."""
    chaves = sorted(set(faltantes or ()))
    codigos = [_codigo(k) for k in chaves]
    if not codigos or any(c is None for c in codigos):
        return None
    linhas = (texto_publicado or "").split("\n")
    escolhidas = _selecionar(linhas, codigos, _urls(texto_candidato))
    if escolhidas is None:
        return None
    return _compor(texto_candidato, [linhas[i] for i in escolhidas],
                   tuple(chaves), limite)


def manter(texto_lider: str, texto_publicado: str, ofertas_lider: Iterable[str],
           exibida: Iterable[str], *, limite: int) -> Optional[Retencao]:
    """Sincronização de post composto: o texto novo do líder + o bloco
    retido que já estava no ar, sem os códigos que o líder passou a
    trazer. None se o post não tem bloco retido (ou nada sobra dele)."""
    linhas = (texto_publicado or "").split("\n")
    marca = next((i for i, l in enumerate(linhas)
                  if sem_marcacao(l).strip() == MARCA), None)
    if marca is None:
        return None
    bloco = linhas[marca + 1:]
    do_lider = {_codigo(k) for k in (ofertas_lider or ())} - {None}
    chaves = sorted(k for k in set(exibida or ())
                    if _codigo(k) and _codigo(k) not in do_lider
                    and any(_tem(l, _codigo(k)) for l in bloco))
    if not chaves:
        return None
    escolhidas = _selecionar(bloco, [_codigo(k) for k in chaves],
                             _urls(texto_lider))
    if escolhidas is None:
        return None
    return _compor(texto_lider, [bloco[i] for i in escolhidas],
                   tuple(chaves), limite)


# ── internos ──────────────────────────────────────────────────────
def _codigo(chave: str) -> Optional[str]:
    """O código de uma chave `<plat>|cup|<COD>`; None para o resto."""
    partes = (chave or "").split("|", 2)
    if len(partes) == 3 and partes[1] == "cup" and partes[2]:
        return partes[2].upper()
    return None


def _tem(linha: str, codigo: str) -> bool:
    return bool(re.search(
        rf"(?<![A-Z0-9_-]){re.escape(codigo)}(?![A-Z0-9_-])", linha.upper()))


def _urls(texto: str) -> set:
    return {u.rstrip(".,;:!?") for u in _RE_URL.findall(texto or "")}


def _selecionar(linhas: List[str], codigos: List[str], urls_candidato: set):
    """Índices (em ordem) das linhas que mantêm cada código no ar e dos
    links de resgate que o candidato não tem. None se algum código não
    estiver em linha nenhuma."""
    escolhidas = set()
    for cod in codigos:
        com = [i for i, l in enumerate(linhas) if _tem(l, cod)]
        if not com:
            return None
        i = next((j for j in com if _RE_DESCONTO.search(linhas[j])), com[0])
        escolhidas.add(i)
        # "Código: X" sem o benefício na linha: o benefício vem na anterior
        if (not _RE_DESCONTO.search(linhas[i]) and i > 0
                and _RE_DESCONTO.search(linhas[i - 1])
                and not _RE_URL.search(linhas[i - 1])):
            escolhidas.add(i - 1)
    for i, l in enumerate(linhas):
        urls = _urls(l)
        if not urls or urls <= urls_candidato:
            continue
        escolhidas.add(i)
        # link sozinho na linha: o rótulo ("✅ Resgate aqui:") vem antes
        if (_RE_URL.sub("", sem_marcacao(l)).strip() == "" and i > 0
                and linhas[i - 1].strip() and not _RE_URL.search(linhas[i - 1])
                and sem_marcacao(linhas[i - 1]).rstrip().endswith(":")):
            escolhidas.add(i - 1)
    return sorted(escolhidas)


def _compor(base: str, bloco: List[str], retidas: tuple,
            limite: int) -> Optional[Retencao]:
    texto = (f"{(base or '').rstrip()}\n\n{marcacao.negrito(MARCA, True)}\n"
             + "\n".join(bloco))
    if len(texto) > limite:
        return None
    return Retencao(texto, retidas)
