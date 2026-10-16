"""Exemplos do dono — decisão de 16/10: os casos que ELE marcou são a única
coisa guardada. Cada chamada leva os 3–5 mais parecidos com o alvo, com a
decisão dele, como referência de critério (não como resposta a copiar).

Regras (testadas):
  · só caso com rótulo do DONO (fonte_rotulo == "dono");
  · nunca do HOLDOUT (brain.divisao) — o holdout mede, não ensina;
  · nunca o próprio caso que está sendo julgado (nem o mesmo alvo);
  · o id do exemplo vai em outro espaço ("exemplo:SN03"): a IA não pode
    devolvê-lo em ids_relacionados (o juiz derruba para INCERTO).

Puro: recebe os dicts do casos.json; sem rede, sem banco."""
from __future__ import annotations

from dataclasses import dataclass

from brain import contexto, divisao

K_EXEMPLOS = 4                   # 3–5 por chamada
K_MIN, K_MAX = 3, 5
CHARS_TEXTO = 280                # cada texto do exemplo vai cortado (custo)


@dataclass(frozen=True)
class Exemplo:
    id: str                      # id do caso no casos.json (ex.: "SN03")
    decisao: str                 # UM_POST | DOIS_POSTS (a decisão do dono)
    veredito: str                # o veredito correspondente
    plataforma: str
    alvo_grupo: str
    alvo_texto: str
    par_grupo: str
    par_texto: str
    delta_min: float             # do par até o alvo (negativo = par antes)

    @property
    def rotulo_id(self) -> str:
        return f"exemplo:{self.id}"


def do_conjunto(casos: list) -> list:
    """Os exemplos possíveis: casos do DONO, fora do holdout, com par."""
    out = []
    for d in casos:
        if d.get("fonte_rotulo") != "dono" or divisao.em_holdout(d):
            continue
        cands = d.get("candidatos") or []
        if not cands:
            continue
        par, alvo = cands[0], d["alvo"]
        out.append(Exemplo(d["id"], d["deve"], d.get("rotulo", ""),
                           alvo.get("plataforma", ""), alvo.get("grupo", ""),
                           alvo.get("texto", ""), par.get("grupo", ""),
                           par.get("texto", ""),
                           float(par.get("delta_min", 0.0)) - float(alvo.get("delta_min", 0.0))))
    return out


def escolher(alvo_texto: str, plataforma: str, exemplos: list, k: int = K_EXEMPLOS,
             excluir: frozenset = frozenset(), excluir_textos: frozenset = frozenset()) -> list:
    """Os k exemplos mais parecidos com o alvo: semelhança das palavras do
    alvo, com um bônus para a mesma plataforma. `excluir` tira ids (o
    próprio caso); `excluir_textos` tira o exemplo cujo alvo é o mesmo texto
    (o mesmo post em outro caso)."""
    k = max(K_MIN, min(K_MAX, k))
    pal = contexto.palavras(alvo_texto)
    pontuados = []
    for ex in exemplos:
        if ex.id in excluir or ex.alvo_texto in excluir_textos:
            continue
        nota = contexto.jaccard(pal, contexto.palavras(ex.alvo_texto))
        if plataforma and ex.plataforma == plataforma:
            nota += 0.15
        pontuados.append((-nota, ex.id, ex))
    pontuados.sort(key=lambda t: (t[0], t[1]))
    return [ex for _n, _i, ex in pontuados[:k]]


def cortar(texto: str, n: int = CHARS_TEXTO) -> str:
    t = (texto or "").strip()
    return t if len(t) <= n else t[:n].rstrip() + "…"


__all__ = ["Exemplo", "do_conjunto", "escolher", "cortar", "K_EXEMPLOS"]
