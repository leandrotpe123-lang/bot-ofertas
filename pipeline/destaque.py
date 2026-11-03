"""
Autoridade de DESTAQUE — a fonte diz que o PREÇO está bom (módulo PURO).

Pergunta de negócio, uma só: "este trecho traz uma frase que chama a
atenção do comprador para o preço ou para a urgência da oferta?" —
"Ainda ativo!", "Bom preço!", "Preção!", "Vai esgotar!", "Baixou!"…

Devolve a frase achada (normalizada), ou "" se não houver. NÃO decide o
que fazer com ela: hoje quem consome é pipeline.curadoria (hardware com
frase de destaque no título passa sem aval — decisão do Léo, 03/11/2026);
a frente seguinte (frases fixas no post quando as fontes comentam o preço)
consome o MESMO vocabulário, daqui. Sem estado, sem I/O, sem log.

O retorno de oferta ("voltando", "voltou", "de volta") NÃO mora aqui: o
dono é pipeline.reativacao (vocabulário canônico em
pipeline.estado_evento) — quem precisa dos dois consulta os dois.

VOCABULÁRIO — medido nos títulos reais das prévias públicas (Promotom,
Fumotom, Samuel Felipe, Fada, Nerd; 26/10 → 03/11/2026) e nas palavras do
dono: "preço muito bom", "ainda ativo", "vai esgotar", "abaixou o preço".
Expressões inteiras, com fronteira de palavra: "Bombril", "Correia",
"Baixo elétrico" e "Top Mizuno" não são destaque. "Corre" só como chamada
("Corre!", "Corre,", "corre que"): "Olympikus Corre 3" é nome de tênis.
"Barata" fica de fora ("Mata Barata" é inseticida); "caiu" só com preço
("Caiu Perdeu" é brinquedo). Código de cupom ("Cupom: APROVEITA") não é
frase. "Esgotou" (acabou) não é destaque — é o contrário.
"""
from __future__ import annotations

import re
import unicodedata

__all__ = ["frase_de_destaque"]

_DESTAQUE = re.compile(
    r"\bainda\s+ativ[oa]s?\b"
    r"|\b(?:bom|boa|otimo|excelente|melhor|super)\s+preco\b"
    r"|\bpreco\s+(?:muito\s+)?(?:bom|otimo|top|baixo|imperdivel|incrivel|absurdo)\b"
    r"|\bprecao+\b|\bprecinho+\b"
    r"|\bbarat(?:o+|inho+|eou)\b"
    r"|\b(?:abaixou|baixou)\b|\bcaiu\s+(?:o\s+)?preco\b|\bpreco\s+caiu\b"
    r"|\bmenor\s+preco\b|\bminimo\s+historico\b"
    r"|\bvai\s+esgotar\b|\besgotando\b|\bultimas\s+unidades\b"
    r"|\bcorre\s*[!,]|\bcorre\s+que\b|\bcorram\b|\baproveit(?:a+|em|e+)\b"
    r"|\bisso\s+ta\s+(?:mt\s+|muito\s+)?bom\b"
    r"|\bpegar(?:a|ia)\s+esse\b|\btao\s+dando\b"
    r"|\bimperdivel\b|\berro\s+de\s+preco\b")


# CÓDIGO de cupom não é frase: "🎟 Cupom: APROVEITA" é o nome do cupom.
_CODIGO = re.compile(r"\bcupo(?:m|ns)\s*:\s*\S+", re.I)


def _normalizar(t: str) -> str:
    t = _CODIGO.sub(" ", t or "")
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn").lower()
    return re.sub(r"\s+", " ", t)


def frase_de_destaque(texto: str) -> str:
    """A frase de destaque achada em `texto` (normalizada: minúsculas, sem
    acento), ou "". Quem chama escolhe o trecho (o título, o post…)."""
    m = _DESTAQUE.search(_normalizar(texto))
    return m.group(0) if m else ""
