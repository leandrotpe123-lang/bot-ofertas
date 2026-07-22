"""
RASTRO — instrumentação TEMPORÁRIA de perda de informação.

Acompanha cada elemento da mensagem (título, preço, cupom, link,
anúncio) em três estágios e diz EM QUAL CAMADA ele desapareceu:

    BRUTO    -> bruta.texto        (saída da ingestão)
    LIMPO    -> norm.texto_limpo   (saída de limpar_texto)
    MONTADO  -> montada.texto      (saída de montar_texto)

Não altera nada. Não decide nada. Só observa e loga.

ATIVAÇÃO — uma linha em orchestrator.py, logo APÓS a Camada 4
(montagem) e ANTES da Camada 5 (publicação), na linha 217:

    from pipeline.rastro import rastrear
    rastrear(bruta, norm, montada)

BLINDAGEM: nenhuma função deste módulo pode lançar. Qualquer falha
interna é engolida e o pipeline segue intacto.

Remover após o diagnóstico.
"""
from __future__ import annotations

import re
from typing import List

# ── Logger próprio, rótulo [RASTRO] ───────────────────────────────
try:
    from logger import _mk_log
    log_rst = _mk_log('RASTRO', '1;38;5;213')
except Exception:                                    # pragma: no cover
    from logger import log_sys as log_rst

# Só loga o detalhe quando houve perda. Ponha False para ver tudo.
SO_PERDAS = True

log_rst.info("🧪 RASTRO carregado — instrumentação temporária ativa")


# ── Extratores (só leem, nunca decidem) ───────────────────────────

_RE_URL = re.compile(r'https?://[^\s\)\]>,"\'<]+')
_RE_PRECO = re.compile(r'r\$\s*[\d.,]+', re.I)
_RE_ANUN = re.compile(
    r'^\s*[-#]?\s*(?:an[uú]ncio|publicidade|patrocinado)\s*$', re.I)


def _linhas(t) -> List[str]:
    return [l.strip() for l in (t or "").split("\n") if l.strip()]


def _primeira(t) -> str:
    ls = _linhas(t)
    return ls[0] if ls else ""


def _titulo(t, ref: str) -> List[str]:
    """A 1a linha do BRUTO ainda existe neste estagio?

    Comparacao por presenca, nao por posicao: a montagem pode
    prefixar emoji, entao o texto original segue sendo substring.
    """
    if not ref:
        return []
    return [ref[:50]] if ref in (t or "") else []


def _precos(t) -> List[str]:
    return [m.group(0).lower().replace(" ", "")
            for m in _RE_PRECO.finditer(t or "")]


def _urls(t) -> List[str]:
    return _RE_URL.findall(t or "")


def _anuncios(t) -> int:
    return sum(1 for l in _linhas(t) if _RE_ANUN.match(l))


def _cupons(t, cupons) -> List[str]:
    alvo = (t or "").upper()
    return [c for c in (cupons or []) if str(c).upper() in alvo]


# ── Núcleo ────────────────────────────────────────────────────────

def _onde(bruto, limpo, montado) -> str:
    """Camada onde o elemento sumiu, ou '' se não sumiu."""
    if not bruto:
        return ""                       # nunca existiu → nada a perder
    if not limpo:
        return "NORMALIZACAO"
    if not montado:
        return "MONTAGEM"
    return ""


def rastrear(bruta, norm, montada) -> None:
    """Loga o rastro de uma mensagem. Nunca lança."""
    try:
        txt_bruto = getattr(bruta, "texto", "") or ""
        txt_limpo = getattr(norm, "texto_limpo", "") or ""
        txt_mont = getattr(montada, "texto", "") or ""
        msg_id = getattr(norm, "msg_id", None)
        chat = getattr(norm, "chat", None)
        cups = getattr(norm, "cupons", None) or []

        ref = _primeira(txt_bruto)

        itens = [
            ("titulo", _titulo(txt_bruto, ref), _titulo(txt_limpo, ref),
             _titulo(txt_mont, ref)),
            ("preco", _precos(txt_bruto), _precos(txt_limpo),
             _precos(txt_mont)),
            ("cupom", _cupons(txt_bruto, cups), _cupons(txt_limpo, cups),
             _cupons(txt_mont, cups)),
            ("link", _urls(txt_bruto), _urls(txt_limpo), _urls(txt_mont)),
            ("anuncio", _anuncios(txt_bruto), _anuncios(txt_limpo),
             _anuncios(txt_mont)),
        ]

        estados, perdidos = [], []
        for nome, b, l, m in itens:
            onde = _onde(b, l, m)
            if onde:
                estados.append(f"{nome}=PERDA({onde})")
                perdidos.append((nome, b, l, m, onde))
            elif not b:
                estados.append(f"{nome}=-")
            else:
                estados.append(f"{nome}=ok")

        cab = (f"🧪 id={msg_id} chat={chat} | " + " ".join(estados) +
               f" | linhas {len(_linhas(txt_bruto))}→"
               f"{len(_linhas(txt_limpo))}→{len(_linhas(txt_mont))}")

        if perdidos:
            log_rst.warning(cab)
        else:
            log_rst.info(cab)

        if not perdidos and SO_PERDAS:
            return

        for nome, b, l, m, onde in perdidos:
            log_rst.warning(
                f"🧪 id={msg_id} | {nome.upper()} sumiu na {onde} | "
                f"bruto={b!r} limpo={l!r} montado={m!r}")

        # Linha que existia no estágio anterior e não existe no seguinte
        if perdidos:
            camadas = {p[4] for p in perdidos}
            if "NORMALIZACAO" in camadas:
                sumidas = [x for x in _linhas(txt_bruto)
                           if x not in _linhas(txt_limpo)]
                log_rst.warning(
                    f"🧪 id={msg_id} | linhas apagadas na NORMALIZACAO: "
                    f"{[s[:50] for s in sumidas]}")
            if "MONTAGEM" in camadas:
                sumidas = [x for x in _linhas(txt_limpo)
                           if x not in _linhas(txt_mont)]
                log_rst.warning(
                    f"🧪 id={msg_id} | linhas apagadas na MONTAGEM: "
                    f"{[s[:50] for s in sumidas]}")

    except Exception as e:                            # pragma: no cover
        try:
            log_rst.debug(f"🧪 rastro falhou (ignorado): {e}")
        except Exception:
            pass
    
