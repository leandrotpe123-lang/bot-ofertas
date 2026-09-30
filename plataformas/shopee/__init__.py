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
    Plataforma,
)

from .afiliacao import afilia
from .links import (
    _IDENTIFICADOR,
    _ENCURTADORES,
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

# ── Hosts de campanha ─────────────────────────────────────────────
# Hosts cujas URLs afiliadas LONGAS caracterizam uma página de
# campanha da Shopee. O core compõe a UNIÃO entre plataformas
# (registry.compor_capacidade("hosts_campanha")) para derivar
# tem_host_campanha e chave_campanha — o mecanismo host-based vigente
# no contrato atual. NÃO incluir o host pelado "shopee.com.br":
# páginas genéricas de marketing (/m/*, /shopeevip) fundiriam
# produtos distintos no overlap. O produto já é identificado pelo
# item id em extrai_identidade, com precedência.
_HOSTS_CAMPANHA = frozenset({
    "premios.shopee.com.br", "flapremios.com.br",
})

# ── Hosts de CONTAINER ────────────────────────────────────────────
# A live da Shopee é um CONTAINER: a mesma transmissão (session) vende
# vários produtos pela sacola, e o link da live NÃO carrega o produto
# (URLs reais de 30/09: universal-link/share?session=7187289&…, sem
# loja/item). A identidade canônica da live (`/live/<session>`) segue
# sendo a âncora de fallback, mas o core não a aceita, sozinha, como
# prova de que duas mensagens são o mesmo produto.
_HOSTS_CONTAINER = frozenset({
    "live.shopee.com.br",
})

# ── Definição da plataforma ───────────────────────────────────────
PLATAFORMA = Plataforma(
    identificador=_IDENTIFICADOR,
    versao_contrato=CONTRACT_VERSION,
    reconhece=reconhece,
    extrai_identidade=extrai_identidade,
    afilia=afilia,
    encurtadores_forca_get=_ENCURTADORES_FORCA_GET,
    encurtadores=_ENCURTADORES,
    hosts_campanha=_HOSTS_CAMPANHA,
    sinais_cashback=_SINAIS_CASHBACK,
    hosts_container=_HOSTS_CONTAINER,
)
