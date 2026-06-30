"""
Plataforma Shopee — composição.

Raiz de composição do pacote: reúne as capacidades dos módulos de
domínio (`links` para o conhecimento de URL; `afiliacao` para a
integração externa), declara ao contrato os parâmetros próprios da
plataforma e monta a instância `PLATAFORMA`.

É o único ponto que conhece os módulos internos em conjunto e a
única superfície pública do pacote: a descoberta de plugins importa
`plataformas.shopee` e registra a `PLATAFORMA` aqui exposta.

Dependência: importa de `links` e `afiliacao`; nenhum deles importa
esta composição (fluxo unidirecional, sem ciclos).
"""
from __future__ import annotations

from plataformas.contrato import (
    CONTRACT_VERSION,
    ParametrosTemporais,
    Plataforma,
)

from .afiliacao import afilia
from .links import (
    _IDENTIFICADOR,
    _eh_url_campanha,
    extrai_identidade,
    reconhece,
)


# ── Quirk HTTP: hosts que exigem GET na resolução ─────────────────
# Hosts cujos servidores não respondem corretamente a HEAD, exigindo
# GET direto no resolver de redirecionamento. Para a Shopee, todos os
# encurtadores próprios respondem a HEAD; o conjunto é declarado
# EXPLICITAMENTE VAZIO — registra que a consideração foi feita e a
# resposta é nula, semântica distinta de não declarar a capacidade
# (que ficaria None no contrato).
_ENCURTADORES_FORCA_GET = frozenset()

# ── Sinais textuais de cashback (vocabulário próprio da Shopee) ────
_SINAIS_CASHBACK = frozenset({
    r"\bmoedas?\s+shopee\b",
})

# ── Parâmetros temporais de deduplicação ──────────────────────────
_PARAMETROS_TEMPORAIS = ParametrosTemporais(
    janela_s=60.0,
    ttl_restock_s=3600.0,
)


# ── Definição da plataforma ───────────────────────────────────────
# hosts_campanha é deliberadamente OMITIDO (default None no contrato):
# a classificação de campanha da Shopee é POR HOST/CAMINHO dentro de
# extrai_identidade, e eh_url_campanha deriva dela. Não há lista de
# hosts a declarar ao core — o conhecimento vive inteiro em links.py,
# sem duplicação.
PLATAFORMA = Plataforma(
    identificador=_IDENTIFICADOR,
    versao_contrato=CONTRACT_VERSION,
    reconhece=reconhece,
    extrai_identidade=extrai_identidade,
    afilia=afilia,
    parametros_temporais=_PARAMETROS_TEMPORAIS,
    encurtadores_forca_get=_ENCURTADORES_FORCA_GET,
    sinais_cashback=_SINAIS_CASHBACK,
    eh_url_campanha=_eh_url_campanha,
)
