"""
RASTRO — instrumentação TEMPORÁRIA de perda de informação.

Não altera o pipeline. Não decide nada. Só observa e reporta.
Compara o mesmo elemento em três estágios:

    BRUTO      -> texto como chegou da ingestão
    LIMPO      -> depois de normalizacao.limpar_texto()
    MONTADO    -> depois de montagem.montar_texto()

Uso (uma linha no orchestrator, depois da montagem):

    from pipeline.rastro import rastrear
    rastrear(bruta.texto, norm.texto_limpo, montada.texto,
             msg_id=norm.msg_id, cupons=norm.cupons)

Remover após o diagnóstico.
"""
from __future__ import annotations

import re
from typing import List, Tuple

# ── Extratores de elemento (só leem, nunca decidem) ───────────────

_RE_URL   = re.compile(r'https?://[^\s\)\]>,"\'<]+')
_RE_PRECO = re.compile(r'r\$\s*[\d.,]+', re.I)
_RE_ANUN  = re.compile(r'^\s*[-#]?\s*(?:an[uú]ncio|publicidade|patrocinado)\s*$', re.I)


def _linhas(t: str) -> List[str]:
    return [l.strip() for l in (t or "").split("\n") if l.strip()]


def _titulo(t: str) -> str:
    ls = _linhas(t)
    return ls[0][:60] if ls else ""


def _precos(t: str) -> List[str]:
    return [m.group(0).lower().replace(" ", "") for m in _RE_PRECO.finditer(t or "")]


def _urls(t: str) -> List[str]:
    return _RE_URL.findall(t or "")


def _anuncios(t: str) -> int:
    return sum(1 for l in _linhas(t) if _RE_ANUN.match(l))


def _cupons_presentes(t: str, cupons: List[str]) -> List[str]:
    alvo = (t or "").upper()
    return [c for c in (cupons or []) if c.upper() in alvo]


# ── Relatório ─────────────────────────────────────────────────────

def _linha_rel(nome: str, b, l, m) -> Tuple[str, bool]:
    def fmt(v):
        if isinstance(v, list):
            return f"{len(v)}" + (f" {v}" if v and len(str(v)) < 40 else "")
        return str(v) if v != "" else "—"
    perdeu = bool(b) and not bool(m)
    marca = "  <<< PERDA" if perdeu else ""
    onde = ""
    if perdeu:
        onde = "  [na NORMALIZAÇÃO]" if not bool(l) else "  [na MONTAGEM]"
    return (f"  {nome:<10} bruto={fmt(b):<28} limpo={fmt(l):<28} "
            f"montado={fmt(m):<28}{marca}{onde}"), perdeu


def rastrear(bruto: str, limpo: str, montado: str,
             msg_id=None, cupons: List[str] = None) -> bool:
    """Devolve True se houve perda de algum elemento."""
    cupons = cupons or []

    itens = [
        ("titulo",  _titulo(bruto),   _titulo(limpo),   _titulo(montado)),
        ("preco",   _precos(bruto),   _precos(limpo),   _precos(montado)),
        ("cupom",   _cupons_presentes(bruto, cupons),
                    _cupons_presentes(limpo, cupons),
                    _cupons_presentes(montado, cupons)),
        ("link",    _urls(bruto),     _urls(limpo),     _urls(montado)),
        ("anuncio", _anuncios(bruto), _anuncios(limpo), _anuncios(montado)),
    ]

    linhas, houve = [], False
    for nome, b, l, m in itens:
        txt, perdeu = _linha_rel(nome, b, l, m)
        linhas.append(txt)
        houve |= perdeu

    cab = f"🧪 RASTRO id={msg_id}" + ("  ⚠️ PERDA DETECTADA" if houve else "  ok")
    print(cab)
    print("\n".join(linhas))

    if houve:
        print(f"  linhas: bruto={len(_linhas(bruto))} "
              f"limpo={len(_linhas(limpo))} montado={len(_linhas(montado))}")
    return houve
    
