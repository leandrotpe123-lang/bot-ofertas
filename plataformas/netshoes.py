"""
Plataforma — Netshoes.

Responsabilidade ÚNICA: representar a Netshoes perante o contrato.

Entrada: a URL FINAL da Netshoes (netshoes.com.br), já expandida pelo
core. A única transformação é substituir o VALOR dos parâmetros de
afiliação EXISTENTES pelos do dono do bot. Todo o resto — caminho do
produto, sellerId, demais parâmetros, encoding, ordem e fragmento —
permanece byte a byte.

Sem rede, sem banco, sem cache, sem normalização. O encurtamento é do
core, acionado por requer_encurtamento=True.
"""
from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

from logger import log_nrm
from plataformas.contrato import (
    AUSENTE,
    CONTRACT_VERSION,
    IdentidadeProduto,
    Plataforma,
    TipoLink,
)
from utils.urls import _netloc


_IDENTIFICADOR = "netshoes"

_DOMINIOS = frozenset({"netshoes.com.br"})


# ── Credenciais de afiliação ──────────────────────────────────────
# Valores JÁ PERCENT-ENCODED, exatamente como aparecem num link oficial
# do afiliado. São escritos crus na query — nenhuma codificação é
# aplicada aqui, logo o que estiver escrito é o que sai.

# Token de rastreio (Rakuten + AppsFlyer).
_TOKEN_AFILIADO = "ZYv0JCDsx8s-9xNIeotgY4LUjnyPdzuQqw"

# utm_campaign completo do afiliado. NÃO é o token: carrega o nome do
# afiliado junto de um código de campanha.Afeta apenas o relatório da Netshoes — a
# comissão é atribuída por ranSiteID/ranEAID/af_sub_siteID/clickid.
# utm_campaign oficial do afiliado.
# Validado em múltiplos produtos Netshoes:
# produtos diferentes mantiveram o mesmo valor.
# Portanto é tratado como credencial de campanha do afiliado,
# não como atributo do produto.
_UTM_CAMPAIGN = (
    "me-s_pgit--tnfild__tca__--post--var_me_pgit"
    "%3A%3AN3T%3AAF-PG-00-00-00-TD-00-MAS-00%3AN3T%3A%3A"
    "_Leandro%20Gon%C3%A7alves%20Silva_"
)


# ── Os ÚNICOS parâmetros que este módulo escreve ──────────────────
# Lista declarada a partir do diff entre um link oficial do afiliado e
# um link de terceiro para o MESMO produto: são exatamente os que
# identificam o afiliado.
#   ranSiteID / ranEAID      — atribuição Rakuten
#   af_sub_siteID / clickid  — atribuição AppsFlyer
#   utm_term / utm_content   — repetem o token
#   utm_campaign             — valor próprio, não o token
#
# Fora deste mapa, NADA é tocado. Em especial:
#   af_sub1, af_adset_id, ranMID = 43984  → é o MERCHANT (Netshoes),
#                                           não o afiliado.
_PARAMS_AFILIADO = {
    "af_sub_siteID": _TOKEN_AFILIADO,
    "clickid":       _TOKEN_AFILIADO,
    "utm_term":      _TOKEN_AFILIADO,
    "utm_content":   _TOKEN_AFILIADO,
    "ranEAID":       _TOKEN_AFILIADO,
    "ranSiteID":     _TOKEN_AFILIADO,
    "utm_campaign":  _UTM_CAMPAIGN,
}


# ── Contrato: reconhecimento ──────────────────────────────────────
def reconhece(url: str) -> bool:
    """URL da Netshoes. Pura, sem I/O.

    Reconhece também a saída de afilia() — a URL afiliada segue no mesmo
    domínio —, o que normalizacao._encurtar_mapa exige para acionar o
    encurtador.
    """
    if not url:
        return False
    host = _netloc(url)
    return any(host == d or host.endswith("." + d) for d in _DOMINIOS)


# ── Contrato: identidade ──────────────────────────────────────────
def extrai_identidade(url: str) -> IdentidadeProduto:
    """Identidade do produto. Pura.

    O caminho é o identificador — nenhum padrão de SKU é presumido. A
    query (sellerId, UTMs, afiliação) não participa da identidade: o
    mesmo produto postado por grupos diferentes tem a mesma identidade.
    """
    caminho = urlsplit(url).path.rstrip("/")
    if caminho.startswith("/p/"):
        return IdentidadeProduto(
            tipo_link=TipoLink.PRODUTO,
            id_produto=caminho,
            id_global=f"{_IDENTIFICADOR}:{caminho}",
        )
    return IdentidadeProduto(tipo_link=TipoLink.CAMPANHA, id_produto=AUSENTE)


# ── A única transformação ─────────────────────────────────────────
def _trocar_afiliacao(url: str) -> str:
    """Substitui o valor de cada parâmetro de _PARAMS_AFILIADO pelo do
    afiliado. Cada parâmetro recebe o SEU valor — o token para os de
    rastreio, o utm_campaign completo para o utm_campaign.

    A query é fatiada no `&` e a chave comparada por igualdade EXATA.
    Pares fora do mapa são reemitidos como a string ORIGINAL — nunca
    decodificados, logo nunca corrompidos (sellerId, ranMID e o encoding
    seguem intactos). Nada é adicionado: um parâmetro ausente não é
    criado. Sem nenhum parâmetro de afiliação, a URL volta idêntica.
    """
    partes = urlsplit(url)

    pares = []
    for par in partes.query.split("&"):
        chave, sep, _valor = par.partition("=")
        novo_valor = _PARAMS_AFILIADO.get(chave) if sep else None
        if novo_valor is not None:
            pares.append(f"{chave}={novo_valor}")
        else:
            pares.append(par)

    nova_query = "&".join(pares)
    if nova_query == partes.query:
        return url
    return urlunsplit(partes._replace(query=nova_query))


# ── Contrato: afiliação ───────────────────────────────────────────
async def afilia(url: str, sessao=None) -> object:
    """URL afiliada LONGA: a mesma URL da Netshoes, com as credenciais
    do afiliado no lugar das do terceiro.

    Não propaga exceção — falha resulta em AUSENTE.
    """
    try:
        if not reconhece(url):
            return AUSENTE
        return _trocar_afiliacao(url)
    except Exception as exc:
        log_nrm.warning(f"⚠️ NSH afiliação falhou | erro={type(exc).__name__}")
        return AUSENTE


# ── Definição da plataforma ───────────────────────────────────────
PLATAFORMA = Plataforma(
    identificador=_IDENTIFICADOR,
    versao_contrato=CONTRACT_VERSION,
    reconhece=reconhece,
    extrai_identidade=extrai_identidade,
    afilia=afilia,
    requer_encurtamento=True,
)

