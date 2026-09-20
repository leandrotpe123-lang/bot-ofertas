"""
Conhecimento de URL — Mercado Livre.

Módulo PURO e SÍNCRONO. Concentra tudo que o sistema sabe sobre a
forma das URLs do Mercado Livre: quais domínios pertencem à
plataforma, como classificar um link, como extrair a identidade do
produto e — decisivo nesta frente — se a URL é ELEGÍVEL para o
createLink.

═══════════════════════════════════════════════════════════════════
INVARIANTE: PUREZA
═══════════════════════════════════════════════════════════════════
Nenhuma função aqui faz rede, disco, banco ou navegador. Qualquer
`await` neste arquivo é defeito de contrato.

A razão é dura: `reconhece` e `extrai_identidade` são chamadas
SINCRONAMENTE pelo core. Torná-las assíncronas quebraria o contrato
de plataforma em todo o sistema.

Consequência: este módulo é dono do CONHECIMENTO de expansão
(`precisa_expandir`), nunca do ATO de expandir — que pertence a
`afiliacao.py`, onde o I/O é legítimo.

═══════════════════════════════════════════════════════════════════
ELEGIBILIDADE — o que os testes provaram
═══════════════════════════════════════════════════════════════════
ACEITO pelo createLink (comprovado):
    /p/MLB<dígitos>              produto de catálogo
    /up/MLBU<dígitos>            catálogo unificado
    MLB-<dígitos>-...-_JM        anúncio direto
    lista.mercadolivre.com.br/…  listagem do próprio Mercado Livre

RECUSADO pelo createLink (comprovado, error_code 111):
    /social/<slug>               vitrine de um afiliado
    /social/<slug>/lists/<uuid>  lista de um afiliado
    /sec/XXXX                    link curto de afiliado

A razão é a mesma nos três casos: já SÃO o produto da afiliação de
alguém, não conteúdo do Mercado Livre. A prova apareceu na resposta
de sucesso, cuja `long_url` devolvida é justamente
`/social/<nossa_tag>?matt_word=<nossa_tag>` — a vitrine é a SAÍDA
da afiliação, nunca a entrada.

Enviar uma URL inelegível ao createLink gasta requisição, gasta
retry e falha de forma previsível. `eh_elegivel` existe para que
isso nunca aconteça.

A listagem DIRETA está ligada: `lista.mercadolivre.com.br` com
`_Container_<slug>?coupon_campaign_id=<id>` foi comprovada contra o
endpoint real e gerou `meli.la/2TzpFAP` com a nossa tag.

Descobrir o destino A PARTIR de uma vitrine de terceiro deixou de
estar em aberto: a estrutura do documento servido declara, em campo
nomeado, se a vitrine representa um produto ou uma lista. Isso NÃO
torna a vitrine elegível — o servidor continua recusando-a, e ela
continua fora de `_CENARIOS_ELEGIVEIS`. O que se afilia é o DESTINO
descoberto, nunca a vitrine.

A leitura da página é de `descoberta.py`. Este módulo só declara,
em `precisa_descobrir`, que vale a pena ir olhar — conhecimento de
FORMA DE URL, que é o que lhe cabe.
"""
from __future__ import annotations

import re
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from plataformas.contrato import AUSENTE, IdentidadeProduto, TipoLink

from .afiliado import IDENTIFICADOR


# ── Domínios reivindicados ────────────────────────────────────────
_DOMINIOS = frozenset({
    "mercadolivre.com.br", "mercadolivre.com",
    "produto.mercadolivre.com.br", "click1.mercadolivre.com.br",
    "lista.mercadolivre.com.br",
    "meli.la",
})

# Encurtador próprio: host que é ETAPA INTERMEDIÁRIA de resolução.
ENCURTADORES = frozenset({"meli.la"})

# Quirk HTTP: hosts que não respondem corretamente a HEAD.
# Declarado EXPLICITAMENTE VAZIO — meli.la responde bem a HEAD.
# Vazio registra que a consideração foi feita; não declarar (None)
# significaria que ninguém olhou.
ENCURTADORES_FORCA_GET = frozenset()

# Página de listagem do próprio Mercado Livre.
_HOSTS_LISTAGEM = frozenset({"lista.mercadolivre.com.br"})

# Subdomínios de anúncio direto.
_HOSTS_PRODUTO_DIRETO = frozenset({
    "produto.mercadolivre.com.br",
    "click1.mercadolivre.com.br",
})


# ── Padrões de identidade ─────────────────────────────────────────
# A ordem de tentativa importa: MLBU antes de MLB, porque a regex de
# MLB casaria o prefixo de um MLBU e produziria identidade errada.
_RE_UP_MLBU = re.compile(r"/up/MLBU[-]?(\d{5,})", re.I)
_RE_MLBU = re.compile(r"/MLBU[-]?(\d{5,})", re.I)
_RE_P_MLB = re.compile(r"/p/MLB[-]?(\d{5,})", re.I)
_RE_MLB = re.compile(r"/MLB[-]?(\d{5,})", re.I)

_RE_SEC = re.compile(r"/sec/", re.I)
_RE_SOCIAL = re.compile(r"/social/", re.I)


# ── Parâmetros de terceiros ───────────────────────────────────────
# Telemetria de device e tracking de canal.
_PARAMS_TELEMETRIA = frozenset({
    "matt_event_ts", "matt_d2id", "matt_tracing_id",
    "origin", "sid", "tracking_id", "source",
    "polycard_client", "reco_backend", "reco_client",
    "reco_item_pos", "reco_backend_type", "reco_id",
    "c_id", "c_uid",
})

# Identidade externa do afiliado que originou o link recebido.
_PARAMS_IDENTIDADE_EXTERNA = frozenset({"matt_word", "matt_tool"})

_PARAMS_REMOVER = _PARAMS_TELEMETRIA | _PARAMS_IDENTIDADE_EXTERNA


# ── Cenários ──────────────────────────────────────────────────────
CENARIO_PRODUTO = "produto"
CENARIO_LISTAGEM = "listagem"
CENARIO_VITRINE = "vitrine"
CENARIO_LISTA_AFILIADO = "lista_afiliado"
CENARIO_SEC = "sec"
CENARIO_ENCURTADO = "encurtado"
CENARIO_DESCONHECIDO = ""

# Cenários que o createLink aceita — os DOIS comprovados em teste
# real contra o endpoint:
#
#   produto   /p/MLB16016316        → meli.la/2B59gJm, tag confirmada
#   listagem  /_Container_…         → meli.la/2TzpFAP, tag confirmada
#
# Vitrine, lista de afiliado e /sec/ ficam de fora porque o servidor
# os RECUSA (error_code 111) — não é escolha de escopo, é o
# comportamento do programa de afiliados.
_CENARIOS_ELEGIVEIS = frozenset({CENARIO_PRODUTO, CENARIO_LISTAGEM})


# ── Apoio interno ─────────────────────────────────────────────────
def _netloc(url: str) -> str:
    """
    Host limpo, sem www. e sem porta.

    Réplica local deliberada: manter este módulo puro e sem
    dependência de utils do core vale a duplicação de seis linhas
    triviais, e evita que a pureza dependa de terceiros.
    """
    try:
        host = (urlparse(url).hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host.strip(".")
    except Exception:
        return ""


def _bate_dominio(netloc: str) -> bool:
    """Verdadeiro se o host pertence ao Mercado Livre."""
    for dominio in _DOMINIOS:
        if netloc == dominio or netloc.endswith("." + dominio):
            return True
    return False


def _identidade_do_path(path: str) -> str:
    """
    Extrai o identificador do produto do path.

    Cobre os três formatos observados em links reais de grupo:
        /p/MLB45513180        catálogo comum
        /up/MLBU3929091094    catálogo unificado
        /MLB-6009607732-..._JM  anúncio direto

    O formato MLBU foi a lacuna que derrubava links legítimos: a
    regex antiga exigia dígito logo após MLB e devolvia INVALIDO
    para toda URL de catálogo unificado.
    """
    achado = _RE_UP_MLBU.search(path) or _RE_MLBU.search(path)
    if achado:
        return f"MLBU{achado.group(1)}"
    achado = _RE_P_MLB.search(path) or _RE_MLB.search(path)
    return f"MLB{achado.group(1)}" if achado else ""


# ── Capacidade obrigatória: reconhecimento ────────────────────────
def reconhece(url: str) -> bool:
    """
    Verdadeiro se a URL pertence ao Mercado Livre.

    Pura, determinística, não falha: URL malformada simplesmente
    não é reconhecida.
    """
    if not url or not isinstance(url, str):
        return False
    host = _netloc(url)
    return bool(host) and _bate_dominio(host)


# ── Classificação de cenário ──────────────────────────────────────
def cenario_de(url: str) -> str:
    """
    Classifica a URL num cenário do conjunto declarado acima.

    A ordem das verificações importa e não é arbitrária:
      1. /sec/ antes de tudo — é opaco e nunca elegível
      2. encurtador — natureza desconhecida até expandir
      3. /social/ com /lists/ antes de /social/ sozinho, porque
         toda lista de afiliado também contém /social/
      4. listagem por host
      5. produto por identidade no path

    Pura, sem I/O.
    """
    if not reconhece(url):
        return CENARIO_DESCONHECIDO

    host = _netloc(url)
    try:
        path = urlparse(url).path or "/"
    except Exception:
        return CENARIO_DESCONHECIDO

    if _RE_SEC.search(path):
        return CENARIO_SEC
    if host in ENCURTADORES:
        return CENARIO_ENCURTADO
    if _RE_SOCIAL.search(path):
        if "/lists/" in path.lower():
            return CENARIO_LISTA_AFILIADO
        return CENARIO_VITRINE
    if host in _HOSTS_LISTAGEM:
        return CENARIO_LISTAGEM
    if _identidade_do_path(path):
        return CENARIO_PRODUTO
    if host in _HOSTS_PRODUTO_DIRETO:
        return CENARIO_PRODUTO
    return CENARIO_DESCONHECIDO


def eh_elegivel(url: str) -> bool:
    """
    Verdadeiro se a URL pode ser enviada ao createLink.

    Portão anterior a qualquer I/O. Uma URL inelegível nunca chega
    ao cliente HTTP: não gasta requisição, não gasta retry, não
    produz erro previsível no log.
    """
    return cenario_de(url) in _CENARIOS_ELEGIVEIS


def precisa_expandir(url: str) -> bool:
    """
    Verdadeiro se a URL precisa ser resolvida antes de revelar sua
    natureza real.

    CONHECIMENTO, não ATO: quem expande é `afiliacao.py`, com o
    desencurtador do core.

    Links /sec/ ficam de fora deliberadamente. Eles expandem para
    a vitrine do afiliado que os publicou, que é inelegível de
    qualquer forma — expandir seria gastar uma requisição para
    confirmar uma recusa já conhecida.
    """
    return cenario_de(url) == CENARIO_ENCURTADO


def precisa_descobrir(url: str) -> bool:
    """
    Verdadeiro se a URL é uma VITRINE, que é porta de entrada e não
    destino.

    A vitrine `/social/<afiliado>` continua INELEGÍVEL — o servidor
    a recusa com error_code 111, e isso não mudou. O que mudou é
    que ela deixou de ser beco sem saída: o documento servido
    carrega, em estrutura nomeada, QUAL conteúdo ela representa.

    CONHECIMENTO, não ATO: quem busca e lê a página é
    `descoberta.py`. Aqui só se declara que vale a pena ir olhar.

    A lista de afiliado (`/social/<slug>/lists/<uuid>`) fica de
    fora: é a vitrine genérica do perfil, sem item compartilhado a
    descobrir.
    """
    return cenario_de(url) == CENARIO_VITRINE


# ── Capacidade obrigatória: extração de identidade ────────────────
def extrai_identidade(url: str) -> IdentidadeProduto:
    """
    Extrai a identidade estruturada de uma URL do Mercado Livre.

    Pura e determinística: para qualquer URL reconhecida, produz
    sempre uma IdentidadeProduto válida.

    Mapeamento de cenário para tipo de link:
        produto         → PRODUTO, com id_produto e id_global
        listagem        → BUSCA
        vitrine         → CAMPANHA
        lista_afiliado  → CAMPANHA
        sec, encurtado  → ENCURTADO
        desconhecido    → INVALIDO
    """
    cenario = cenario_de(url)

    if cenario == CENARIO_PRODUTO:
        try:
            path = urlparse(url).path or ""
        except Exception:
            path = ""
        identificador = _identidade_do_path(path)
        if identificador:
            return IdentidadeProduto(
                tipo_link=TipoLink.PRODUTO,
                id_produto=identificador,
                id_global=f"{IDENTIFICADOR}:{identificador}",
            )
        # Subdomínio de produto sem identidade extraível do path.
        return IdentidadeProduto(
            tipo_link=TipoLink.CAMPANHA, id_produto=AUSENTE,
        )

    if cenario == CENARIO_LISTAGEM:
        return IdentidadeProduto(
            tipo_link=TipoLink.BUSCA, id_produto=AUSENTE,
        )

    if cenario in (CENARIO_VITRINE, CENARIO_LISTA_AFILIADO):
        return IdentidadeProduto(
            tipo_link=TipoLink.CAMPANHA, id_produto=AUSENTE,
        )

    if cenario in (CENARIO_SEC, CENARIO_ENCURTADO):
        return IdentidadeProduto(
            tipo_link=TipoLink.ENCURTADO, id_produto=AUSENTE,
        )

    return IdentidadeProduto(
        tipo_link=TipoLink.INVALIDO, id_produto=AUSENTE,
    )


# ── Preparação da URL ─────────────────────────────────────────────
def sanitizar(url: str) -> str:
    """
    Prepara a URL para ser enviada ao createLink.

    ESTRITAMENTE SUBTRATIVA: remove identidade de afiliado externo
    (matt_word, matt_tool, qualquer matt_*), telemetria de device e
    parâmetros de recomendação. NÃO injeta nada nosso e NÃO altera
    path, host ou destino.

    Preserva o que descreve o item — `searchVariation`, por exemplo,
    identifica a variação escolhida do produto e faz parte da oferta.

    O fragmento é descartado: o servidor o ignora (comprovado — o
    `origin_url` devolvido nunca o contém).

    Em qualquer falha de parsing devolve a URL original: preservar
    o destino vale mais do que garantir a limpeza.
    """
    try:
        partes = urlparse(url)
        originais = {
            chave: valores[0]
            for chave, valores in parse_qs(
                partes.query, keep_blank_values=True
            ).items()
        }
        limpos = {
            chave: valor
            for chave, valor in originais.items()
            if chave.lower() not in _PARAMS_REMOVER
            and not chave.lower().startswith("matt_")
        }
        return urlunparse(partes._replace(
            query=urlencode(limpos) if limpos else "",
            fragment="",
        ))
    except Exception:
        return url


def tem_identidade_externa(url: str) -> bool:
    """
    Verdadeiro se ainda restam marcas de afiliado de terceiro.

    Última barreira antes de enviar a URL. Em qualquer dúvida
    devolve True — tratar como suja e degradar é sempre mais seguro
    do que enviar e torcer.
    """
    try:
        partes = urlparse(url)
    except Exception:
        return True
    for chave in parse_qs(partes.query, keep_blank_values=True):
        if chave.lower().startswith("matt_"):
            return True
    return False
