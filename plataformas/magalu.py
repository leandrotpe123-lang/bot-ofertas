"""
Plataforma — Magalu.

Módulo autocontido que descreve integralmente a plataforma Magalu
e cumpre o contrato de plataforma. Consolida o conhecimento antes
disperso entre os módulos de classificação, de estado de evento e
de limpeza de parâmetros.

Expõe a instância PLATAFORMA, registrada no registry durante a
inicialização do sistema.

A capacidade de afiliação produz a URL afiliada longa. O
encurtamento NÃO pertence a este módulo: é comportamento do core,
acionado pela declaração requer_encurtamento.

Depende do contrato, dos utilitários do core e dos recursos
externos. Não depende da pipeline, do registry nem da orquestração.
Não acessa o banco de dados.

Baseline arquitetural: Documento 1 — Especificação do Contrato.
"""
from __future__ import annotations

import re
from urllib.parse import unquote, urlparse

import aiohttp

import config
import os

_MGL_PARTNER = os.environ.get("MAGALU_PARTNER_ID", "")
_MGL_PROMOTER = os.environ.get("MAGALU_PROMOTER_ID", "")
_MGL_PID = os.environ.get("MAGALU_PID", "")
_MGL_SLUG = os.environ.get("MAGALU_SLUG", "")
from logger import log_nrm
from plataformas.contrato import (
    AUSENTE,
    CONTRACT_VERSION,
    IdentidadeProduto,
    Plataforma,
    TipoLink,
)
from utils.cache_links import consultar_link, registrar_link
from utils.url_resolver import desencurtar
from utils.urls import _netloc, _sanitizar_url


# ── Identidade da plataforma ──────────────────────────────────────
_IDENTIFICADOR = "magalu"


# ── Domínios e encurtadores ───────────────────────────────────────
_DOMINIOS = frozenset({
    "magazineluiza.com.br", "m.magazineluiza.com.br",
    "sacola.magazineluiza.com.br", "magazinevoce.com.br",
    "maga.lu", "divulgador.magalu.com",
})
_ENCURTADORES = frozenset({
    "maga.lu", "divulgador.magalu.com",
})


# ── Quirk HTTP: hosts que exigem GET na resolução ─────────────────
# Hosts cujos servidores não respondem corretamente a requisições
# HEAD, exigindo GET direto no resolver de redirecionamento. Para
# Magalu, esta declaração materializa empiricamente a distinção
# entre identidade de encurtador e quirk HTTP: `maga.lu` requer
# GET, mas `divulgador.magalu.com` — também encurtador próprio da
# plataforma — responde corretamente a HEAD e portanto NÃO compõe
# este conjunto. A relação com _ENCURTADORES é factual, não
# definicional, e deve ser revisada por host individualmente.
_ENCURTADORES_FORCA_GET = frozenset({
    "maga.lu",
})

# ── Hosts de campanha ─────────────────────────────────────────────
_HOSTS_CAMPANHA = frozenset({
    "magazineluiza.com.br", "magazinevoce.com.br",
})

# ── Padrões de extração do identificador de produto ───────────────
# Rotas OFICIAIS de produto da Magalu. Cada padrão é ancorado numa
# rota explícita: a ancoragem é requisito de SEGURANÇA, não de
# estilo. O fragmento [a-z0-9]{5,} casaria segmentos de slug (ex.:
# "mochila"), de modo que qualquer extração posicional não ancorada
# produziria falsos positivos — e um falso positivo funde produtos
# distintos numa mesma entidade: falha silenciosa e destrutiva.
# Ambas as rotas partilham o MESMO namespace de identificador; a
# rota do divulgador é apenas outra gramática de URL para o mesmo
# produto. Para ampliar quando surgir um formato novo, acrescente o
# padrão aqui — é o ÚNICO lugar de decisão.
_P_PRODUTO = [
    re.compile(r'/(?:[^/]+/)?p/([a-z0-9]{5,})(?:/|$|[?#])', re.I),
    re.compile(r'/divulgador/oferta/([a-z0-9]{5,})(?:/|$|[?#])', re.I),
]

# ── Identidade de afiliação ───────────────────────────────────────
# Campos que CARREGAM identidade de divulgador e, portanto, os
# ÚNICOS que podem ser reescritos. Todo o resto da URL — produto,
# SKU, rota, categoria, seleção, barra final, fragmento e qualquer
# parâmetro desconhecido — é preservado por requisito de segurança.
#
# Duas naturezas na mesma tabela porque a operação sobre ambas é
# idêntica: trocar o valor quando, e somente quando, o campo existe.
#   identidade numérica do divulgador : promoter_id, utm_campaign, c
#   configuração do programa          : partner_id, pid
#
# utm_source e utm_medium NÃO entram: são origem do programa, não
# identidade individual do divulgador, e não devem ser tocados.
_IDENTIDADE = {
    "promoter_id":  _MGL_PROMOTER,
    "utm_campaign": _MGL_PROMOTER,
    "c":            _MGL_PROMOTER,
    "partner_id":   _MGL_PARTNER,
    "pid":          _MGL_PID,
}

# Subconjunto de natureza numérica. O guard só reconhece como
# identificador estrangeiro um valor que ocupe um destes campos.
_CAMPOS_DIVULGADOR = frozenset({"promoter_id", "utm_campaign", "c"})

# Parâmetros cujo valor é, ele próprio, uma URL da Magalu.
_PARAMS_ANINHADOS = ("deep_link_value",)

# Parâmetros que carregam a URL de DESTINO na forma de bounce do
# divulgador. Distintos de _PARAMS_ANINHADOS: ali o valor aninhado é
# um espelho a corrigir; aqui o valor aninhado É a URL efetiva, e o
# invólucro deve ser descartado.
_PARAMS_DESTINO = ("url",)

# Teto de desembrulhos encadeados de destino.
_PROFUNDIDADE_DESTINO = 3

# Domínio da gramática do divulgador, onde a identidade vive no slug
# do caminho. Conjunto — e não cadeia — para ser consumido por
# _bate_dominio, mantendo o mesmo reconhecimento de `reconhece`.
_DOMINIOS_DIVULGADOR = frozenset({"magazinevoce.com.br"})

# Reconhecimento de campo de identidade nos DOIS níveis de
# codificação: separador literal (& =) e percent-encoded (%26 %3D).
_P_CAMPO_IDENTIDADE = re.compile(
    r'(?:(?<=[?&])|(?<=%26))'
    r'(promoter_id|utm_campaign|c|partner_id|pid)(?:=|%3D)([^&#%]*)',
    re.I,
)


# ── Funções de apoio ──────────────────────────────────────────────
def _bate_dominio(netloc: str, dominios: frozenset) -> bool:
    """Verdadeiro se o netloc pertence ao conjunto de domínios."""
    for d in dominios:
        if netloc == d or netloc.endswith("." + d):
            return True
    return False


def _extrair_id_produto(parsed) -> str:
    """
    Extrai o identificador de produto da Magalu a partir do caminho,
    ou cadeia vazia se nenhuma rota oficial de produto casar.

    Pura e determinística. A ordem de _P_PRODUTO é a ordem de
    precedência: a rota canônica /p/<id> é avaliada primeiro.
    """
    for padrao in _P_PRODUTO:
        m = padrao.search(parsed.path)
        if m:
            return m.group(1)
    return ""


def _eh_url_magalu(url: str) -> bool:
    """Verdadeiro se a URL pertence a um domínio da Magalu."""
    return _bate_dominio(_netloc(url), _DOMINIOS)


# ── Capacidade obrigatória: reconhecimento ────────────────────────
def reconhece(url: str) -> bool:
    """
    Verdadeiro se a URL pertence à Magalu (domínio próprio ou
    encurtador próprio). Pura, determinística, não falha.
    """
    if not url:
        return False
    netloc = _netloc(url)
    if not netloc:
        return False
    return _bate_dominio(netloc, _DOMINIOS)


# ── Capacidade obrigatória: extração de identidade ────────────────
def extrai_identidade(url: str) -> IdentidadeProduto:
    """
    Extrai a identidade estruturada de uma URL da Magalu.

    Pura e determinística. Para qualquer URL reconhecida, produz
    sempre uma IdentidadeProduto válida.

    Classificação do tipo de link:
      - encurtador próprio    → ENCURTADO
      - identificador presente → PRODUTO
      - caminho de lista       → BUSCA
      - caminho de seleção     → CAMPANHA
      - demais casos           → CAMPANHA
    """
    netloc = _netloc(url)

    if netloc in _ENCURTADORES:
        return IdentidadeProduto(
            tipo_link=TipoLink.ENCURTADO, id_produto=AUSENTE,
        )

    parsed = urlparse(url)
    id_produto = _extrair_id_produto(parsed)
    if id_produto:
        return IdentidadeProduto(
            tipo_link=TipoLink.PRODUTO,
            id_produto=id_produto,
            id_global=f"{_IDENTIFICADOR}:{id_produto}",
        )

    if "/l/" in parsed.path:
        tipo = TipoLink.BUSCA
    elif "/selecao/" in parsed.path:
        tipo = TipoLink.CAMPANHA
    else:
        tipo = TipoLink.CAMPANHA

    return IdentidadeProduto(tipo_link=tipo, id_produto=AUSENTE)


# ── Capacidade obrigatória: afiliação ─────────────────────────────
def _eh_divulgador(url: str) -> bool:
    """
    Verdadeiro se a URL pertence ao host da gramática do divulgador,
    onde a identidade do afiliado vive no slug do caminho.

    Reusa _bate_dominio e _netloc, de modo que o reconhecimento aqui
    seja EXATAMENTE o mesmo de `reconhece`: prefixo www. normalizado,
    caixa normalizada, subdomínio aceito e sufixo forjado rejeitado.
    Comparação por igualdade de cadeia seria mais frouxa num sentido
    e mais estreita no outro — não usar.
    """
    return _bate_dominio(_netloc(url), _DOMINIOS_DIVULGADOR)

def _destino_embutido(query: str) -> str:
    """
    Extrai a URL de destino carregada num parâmetro de bounce.

    A Magalu redireciona rotas do divulgador para a forma
    `magazinevoce.com.br/<slug>/?url=<destino>`. Nessa forma o slug
    do caminho é decorativo: quem manda é o destino. Devolve cadeia
    vazia quando não há parâmetro de destino utilizável.
    """
    for nome in _PARAMS_DESTINO:
        m = re.search(
            rf'(?<![^&])({re.escape(nome)})=([^&#]*)', query, re.I
        )
        if m:
            destino = unquote(m.group(2))
            if destino.lower().startswith(("http://", "https://")):
                return destino
    return ""


def _partir_url(url: str) -> tuple:
    """
    Separa a URL em (base, query, fragmento, tinha_query) por corte
    de cadeia, sem reserializar coisa alguma. A recomposição por
    _juntar_url é exata: nenhum byte fora da query é tocado.
    """
    frag = ""
    if "#" in url:
        url, resto = url.split("#", 1)
        frag = "#" + resto
    if "?" in url:
        base, query = url.split("?", 1)
        return base, query, frag, True
    return url, "", frag, False


def _juntar_url(base: str, query: str, frag: str, tinha: bool) -> str:
    """Recompõe exatamente o que _partir_url separou."""
    return f"{base}?{query}{frag}" if tinha else f"{base}{query}{frag}"


def _reescrever_identidade(query: str, nivel: str = "externo") -> str:
    """
    Reescreve o VALOR dos campos de identidade presentes na query.

    Cirurgia de span: casa `campo=valor` e troca APENAS o valor. Não
    acrescenta campo ausente, não remove campo, não reordena, não
    reserializa e preserva a caixa original do nome do campo.

    `nivel` seleciona o nível de codificação dos separadores:
      externo → "&" e "="
      interno → "%26" e "%3D", a query dentro de deep_link_value.

    O nível interno é tratado NA FORMA CODIFICADA, sem decodificar e
    recodificar. Decodificar e recodificar reescreveria também o
    conteúdo que não é nosso; operar sobre a forma codificada não.
    """
    for nome, valor in _IDENTIDADE.items():
        n = re.escape(nome)
        if nivel == "externo":
            padrao = re.compile(rf'(?<![^&])({n})=([^&#]*)', re.I)
            igual = "="
        else:
            padrao = re.compile(
                rf'(?:(?<=%26)|(?<=%3F))({n})%3D(.*?)(?=%26|$)', re.I
            )
            igual = "%3D"
        query = padrao.sub(
            lambda m, v=valor, e=igual: f"{m.group(1)}{e}{v}", query
        )
    return query


def _reescrever_aninhado(query: str) -> str:
    """
    Aplica a mesma reescrita dentro dos parâmetros cujo valor é, ele
    próprio, uma URL da Magalu. Preserva integralmente o conteúdo
    aninhado: esquema, host, caminho e parâmetros desconhecidos.

    Um segundo nível de aninhamento (deep_link_value dentro de
    deep_link_value) não é tratado aqui deliberadamente: nunca foi
    observado, e se ocorrer o guard de vazamento descarta a oferta
    em vez de publicar identidade alheia.
    """
    for nome in _PARAMS_ANINHADOS:
        padrao = re.compile(rf'(?<![^&])({re.escape(nome)})=([^&#]*)',
                            re.I)
        query = padrao.sub(
            lambda m: f"{m.group(1)}="
                      f"{_reescrever_identidade(m.group(2), 'interno')}",
            query,
        )
    return query


def _reescrever_slug(base: str) -> str:
    """
    Gramática do divulgador: no host magazinevoce a identidade do
    afiliado é o PRIMEIRO segmento do caminho. Substitui apenas esse
    segmento; o restante do caminho, inclusive a barra final e os
    SKUs unidos por '+', permanece intacto. Cadeia vazia quando não
    há slug configurado ou não há segmento a substituir.
    """
    if not _MGL_SLUG:
        return ""
    m = re.match(r'(https?://[^/?#]+)(/.*)?$', base, re.I)
    if not m:
        return ""
    origem, caminho = m.group(1), m.group(2) or ""
    segmentos = caminho.split("/")
    indice = next((n for n, s in enumerate(segmentos) if s), None)
    if indice is None:
        return ""
    segmentos[indice] = _MGL_SLUG
    return origem + "/".join(segmentos)


def _ids_estrangeiros(url: str) -> set:
    """
    Identificadores numéricos de divulgador presentes na URL que não
    são o nosso. Reconhecidos por CAMPO, nunca por busca cega de
    número solto — um número do produto jamais entra aqui.
    """
    return {
        v for n, v in (
            (m.group(1).lower(), m.group(2))
            for m in _P_CAMPO_IDENTIDADE.finditer(url)
        )
        if n in _CAMPOS_DIVULGADOR and v.isdigit() and v != _MGL_PROMOTER
    }


def _incoerencias_identidade(url: str) -> list:
    """
    Campos de identidade presentes na URL cujo valor não é o nosso,
    nos dois níveis de codificação. Lista vazia significa identidade
    integralmente coerente.
    """
    return sorted({
        f"{m.group(1).lower()}={m.group(2)}"
        for m in _P_CAMPO_IDENTIDADE.finditer(url)
        if m.group(2) != _IDENTIDADE[m.group(1).lower()]
    })


def _vazamento(url: str, estrangeiros: set) -> set:
    """
    Identificadores estrangeiros ainda presentes na QUERY da saída.
    A varredura exclui o caminho por desenho: o caminho é preservado
    integralmente e pode conter SKU numérico coincidente.
    """
    _, query, _, _ = _partir_url(url)
    return {
        i for i in estrangeiros
        if re.search(rf'(?<!\d){re.escape(i)}(?!\d)', query)
    }

def _slug_estrangeiro(url: str) -> str:
    """
    Slug de divulgador presente na URL de entrada que não é o nosso.
    Cadeia vazia quando a URL não é da gramática do divulgador ou já
    está sob a nossa identidade.
    """
    if not _eh_divulgador(url):
        return ""
    base, _, _, _ = _partir_url(url)
    m = re.match(r'https?://[^/?#]+(/[^?#]*)?$', base, re.I)
    caminho = (m.group(1) or "") if m else ""
    segmento = next((s for s in caminho.split("/") if s), "")
    return "" if segmento.lower() == _MGL_SLUG.lower() else segmento


def _vazamento_slug(url: str, slug: str) -> bool:
    """
    Verdadeiro se o slug estrangeiro sobrevive em qualquer ponto da
    URL de saída, inclusive dentro de parâmetro aninhado. O slug é
    alfanumérico, logo a forma codificada é idêntica à literal e uma
    única varredura cobre os dois níveis.
    """
    if not slug:
        return False
    return bool(re.search(
        rf'(?<![A-Za-z0-9]){re.escape(slug)}(?![A-Za-z0-9])', url, re.I
    ))


def _construir_url_afiliada(url: str, profundidade: int = 0) -> str:
    """
    Transforma a URL afiliada recebida de um grupo monitorado na URL
    equivalente sob a NOSSA identidade.

    Princípio: preservar tudo e trocar só a identidade do divulgador.
    Não reconstrói a URL, não inventa campo ausente, não altera
    produto, SKU, rota, categoria, seleção, barra final, fragmento
    nem parâmetros desconhecidos.

    Pura, determinística e idempotente. Cadeia vazia quando não há
    identidade a substituir — nunca devolve a entrada intacta, pois
    entrada intacta é atribuição de outro divulgador.

    Despacha por host. Cada host tem UMA gramática de identidade, e
    as duas nunca se misturam:
      magazineluiza.com.br → identidade nos parâmetros da query
      magazinevoce.com.br  → identidade no slug do caminho
    """
    base, query, frag, tinha = _partir_url(url)

    if _eh_divulgador(url):
        # Forma de bounce: magazinevoce.com.br/<slug>/?url=<destino>.
        # Trocar apenas o slug do wrapper NÃO reafilia coisa alguma —
        # a Magalu leva ao destino, e o destino carrega a identidade
        # real do outro divulgador. Desembrulha e reprocessa desde o
        # início, o que também permite mudar de gramática quando o
        # destino é da loja e não do divulgador.
        destino = _destino_embutido(query)
        if (destino and _eh_url_magalu(destino)
                and profundidade < _PROFUNDIDADE_DESTINO):
            return _construir_url_afiliada(destino, profundidade + 1)
        nova_base = _reescrever_slug(base)
        if not nova_base:
            return ""
        return _juntar_url(nova_base, query, frag, tinha)

    if not query:
        return ""

    query = _reescrever_identidade(query)
    query = _reescrever_aninhado(query)
    return _juntar_url(base, query, frag, tinha)

async def afilia(url: str, sessao: aiohttp.ClientSession) -> object:
    """
    Converte uma URL da Magalu na sua forma afiliada longa.

    Capacidade com efeito colateral controlado: acessa a rede para
    expandir encurtadores e consulta o cache de links mediado. Não
    propaga exceções: qualquer falha legítima resulta em AUSENTE.

    Devolve a URL afiliada LONGA. O encurtamento é etapa posterior,
    conduzida pelo core a partir da declaração requer_encurtamento.

    Validação pós-expansão: se a expansão de um encurtador não
    resultar numa URL da Magalu, devolve AUSENTE.
    """
    url = _sanitizar_url(url)

    cache = consultar_link(url)
    if cache:
        return cache

    # Expansão de encurtador próprio, com validação pós-expansão.
    if _netloc(url) in _ENCURTADORES:
        try:
            async with config._SEM_HTTP:
                url_expandida = await desencurtar(url, sessao)
        except Exception as exc:
            log_nrm.warning(
                f"⚠️ MGL expansão falhou | erro={type(exc).__name__}"
            )
            return AUSENTE

        if not _eh_url_magalu(url_expandida):
            log_nrm.warning(
                "⚠️ MGL expansão não resultou em URL Magalu — descarta"
            )
            return AUSENTE
        url = url_expandida

    # Construção da URL afiliada longa.
    estrangeiros = _ids_estrangeiros(url)
    slug_alheio  = _slug_estrangeiro(url)
    try:
        afiliada = _construir_url_afiliada(url)
    except Exception as exc:
        log_nrm.warning(
            f"⚠️ MGL afiliação falhou | erro={type(exc).__name__}"
        )
        return AUSENTE

    if not afiliada:
        log_nrm.warning(
            "⚠️ MGL sem identidade de afiliação a substituir — descarta"
        )
        return AUSENTE

    if "magazineluiza" not in _netloc(afiliada) \
            and "magazinevoce" not in _netloc(afiliada):
        log_nrm.warning(f"⚠️ MGL afiliação inválida: {afiliada[:60]}")
        return AUSENTE

    # Validação estrutural: todo campo de identidade PRESENTE na
    # saída tem de conter o NOSSO valor, nos dois níveis de
    # codificação. Não se assume que a transformação deu certo.
    incoerentes = _incoerencias_identidade(afiliada)
    if incoerentes:
        log_nrm.warning(
            f"⚠️ MGL identidade incoerente — descarta | {incoerentes}"
        )
        return AUSENTE

    # Guard de vazamento: nenhum identificador de divulgador visto na
    # entrada pode sobreviver na query da saída. Restrito à query, e
    # nunca ao caminho — o caminho é preservado por desenho e pode
    # conter SKU numérico que coincida com um identificador.
    residuais = _vazamento(afiliada, estrangeiros)
    if residuais:
        log_nrm.warning(
            f"⚠️ MGL vazamento de divulgador — descarta "
            f"| ids={sorted(residuais)}"
        )
        return AUSENTE

    # Guard de slug: na gramática do divulgador não existe ID
    # numérico, logo o guard de vazamento é cego ali. O slug do
    # divulgador de origem não pode sobreviver em ponto algum da
    # saída — nem no caminho, nem dentro de parâmetro aninhado.
    if _vazamento_slug(afiliada, slug_alheio):
        log_nrm.warning(
            f"⚠️ MGL slug estrangeiro sobreviveu — descarta "
            f"| slug={slug_alheio}"
        )
        return AUSENTE 

    # Confirmação do slug na gramática do divulgador.
    if _eh_divulgador(afiliada):
        segmentos = [s for s in urlparse(afiliada).path.split("/") if s]
        if not segmentos or segmentos[0] != _MGL_SLUG:
            log_nrm.warning("⚠️ MGL slug não confirmado — descarta")
            return AUSENTE

    registrar_link(url, afiliada, _IDENTIFICADOR)
    log_nrm.info("✅ MGL afiliada (longa)")
    return afiliada


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
    requer_encurtamento=True,
)
