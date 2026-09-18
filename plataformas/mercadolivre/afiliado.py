"""
Identidade do afiliado — Mercado Livre.

Módulo FOLHA: não importa nenhum outro módulo do pacote e não
contém lógica de fluxo. Declara exclusivamente QUEM somos perante
o Mercado Livre e valida se a atribuição de um resultado é nossa.

Existe como módulo próprio porque a identidade é consumida por
camadas de natureza distinta — `cliente` (transporte) e
`afiliacao` (fluxo). Sem esta folha, uma delas importaria a outra
apenas para conhecer a tag, invertendo a direção de dependência.
"""
from __future__ import annotations

import os
import re
import urllib.parse


# ── Identidade da plataforma ──────────────────────────────────────
# String estável exigida pelo contrato. Não muda.
IDENTIFICADOR = "mercadolivre"


# ── Etiqueta do afiliado ──────────────────────────────────────────
# A tag é a nossa atribuição no programa. É enviada no corpo do
# createLink e devolvida na resposta — o que a torna a prova
# objetiva de que o link gerado é nosso.
TAG = (os.environ.get("ML_TAG") or "").strip()


def configurado() -> bool:
    """
    Verdadeiro se há tag configurada.

    Sem tag não há afiliação possível: o corpo do createLink a
    exige, e sem ela não haveria com o que comparar a resposta.
    """
    return bool(TAG)


# ── Validação de atribuição ───────────────────────────────────────
_RE_MATT_WORD = re.compile(r"[?&]matt_word=([^&#]+)", re.I)


def tag_confere(tag_resposta: object) -> bool:
    """
    Verdadeiro se a tag devolvida pelo servidor é exatamente a
    nossa.

    Comparação estrita, sem normalização de caixa ou espaços: a tag
    é um identificador exato do programa. Tag ausente, vazia ou de
    outro afiliado reprova.
    """
    if not TAG:
        return False
    if not isinstance(tag_resposta, str):
        return False
    return tag_resposta == TAG


def matt_word_de(url: str) -> str:
    """
    Extrai o parâmetro matt_word de uma URL, ou string vazia.

    É este parâmetro que determina quem recebe a comissão — não o
    campo `tag` da resposta, que apenas ecoa o que pedimos. Por
    isso as duas validações existem e são independentes.
    """
    if not isinstance(url, str) or not url:
        return ""
    try:
        query = urllib.parse.urlparse(url).query
        valores = urllib.parse.parse_qs(query).get("matt_word") or []
        if valores:
            return valores[0].strip()
    except Exception:
        pass
    achado = _RE_MATT_WORD.search(url)
    return achado.group(1).strip() if achado else ""


def atribuicao_propria(url_longa: str) -> bool:
    """
    Verdadeiro se a URL longa devolvida credita a NÓS.

    Ausência de matt_word na URL não reprova: nem toda resposta o
    traz, e a validação de tag já cobre esse caso. O que reprova é
    a presença de um matt_word de OUTRO afiliado — situação em que
    publicar o link daria a comissão a terceiro.
    """
    dono = matt_word_de(url_longa)
    if not dono:
        return True
    return dono == TAG
