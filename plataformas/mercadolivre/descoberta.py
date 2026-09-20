"""
Descoberta do destino real de uma vitrine — Mercado Livre.

Responsabilidade ÚNICA: dada a URL de uma vitrine `/social/<afiliado>`,
descobrir QUAL conteúdo ela representa — um produto ou uma lista — e
devolver a URL desse conteúdo.

NÃO afilia. NÃO conhece cache, credencial, createLink nem publicação.
NÃO sabe se a URL devolvida é elegível: isso é de `links`.

═══════════════════════════════════════════════════════════════════
FRONTEIRA COM `links`
═══════════════════════════════════════════════════════════════════
Este módulo é dono do conhecimento de ESTRUTURA DA PÁGINA.
`links` continua dono do conhecimento de FORMA DE URL.

Por isso a validação do que foi extraído é delegada a
`links.cenario_de` — reimplementar "isto parece produto?" aqui
espalharia a mesma regra por dois módulos, que é exatamente o que a
arquitetura proíbe.

═══════════════════════════════════════════════════════════════════
A ESTRUTURA, COMPROVADA
═══════════════════════════════════════════════════════════════════
No documento servido, dentro de um <script> inline (atribuição JS de
~80–100 KB):

    $.appProps.pageProps.data
      .components[1]                     ← o item compartilhado
        .recommendation_data.recommendation_info
            .seeMoreLink                 vazio | URL da lista
            .polycards[…]

Regra medida em 4 alvos reais, 2 produto e 2 lista:

    seeMoreLink preenchido  → LISTA, e o valor JÁ É a URL
    seeMoreLink vazio       → PRODUTO em polycards[0].url,
                              com len(polycards) == 1

`polycards` ter tamanho 1 no caso de produto não é detalhe: é o que
dispensa qualquer heurística de escolha. Se vier diferente disso, a
estrutura mudou e a resposta correta é AUSENTE — nunca escolher um
candidato por conta própria.

`components[3].tabs[…]` é a vitrine "Mais vendidos"/"Ofertas" do
PERFIL e não tem relação com o link recebido. Jamais é consultada.

═══════════════════════════════════════════════════════════════════
DETALHES QUE VIERAM DO DADO REAL
═══════════════════════════════════════════════════════════════════
  - `polycards[0].url` vem SEM esquema
    (`www.mercadolivre.com.br/…`), então o esquema é acrescentado
    antes de qualquer validação;
  - `seeMoreLink` vem COM esquema e já sem fragmento;
  - a query da vitrine, em especial o `ref`, é OBRIGATÓRIA: sem ela
    o servidor responde 302 para `/social/<afiliado>/lists`, a
    vitrine genérica, e o item compartilhado se perde.

═══════════════════════════════════════════════════════════════════
SEGREDO
═══════════════════════════════════════════════════════════════════
Requisição ANÔNIMA. Nenhum cookie de afiliado, nenhum CSRF. O
conteúdo é público e a credencial só entra no `createLink`.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Optional

import aiohttp

import config
from logger import log_nrm

from . import ajustes, links


TIPO_PRODUTO = "produto"
TIPO_LISTA = "lista"

_TIMEOUT_S = ajustes.numero("ML_DESCOBERTA_TIMEOUT_S", 25)

# Teto de leitura. Documento observado: 290–320 KB. O teto existe
# para que uma resposta anômala não vire consumo de memória.
_TETO_BYTES = int(ajustes.numero("ML_DESCOBERTA_TETO_KB", 2048)) * 1024

_UA = ajustes.texto(
    "ML_DESCOBERTA_UA",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36",
)

_CABECALHOS = {
    "User-Agent": _UA,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "*/*;q=0.8"),
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}

_RE_SCRIPT = re.compile(r"<script([^>]*)>(.*?)</script>", re.I | re.S)
_RE_SRC = re.compile(r'src\s*=\s*["\']([^"\']+)["\']', re.I)

# Caminho até o componente do item compartilhado.
_CAMINHO = ("appProps", "pageProps", "data", "components", 1,
            "recommendation_data", "recommendation_info")

# Marcadores que identificam o objeto de estado dentro do script.
_MARCADORES = ('"recommendation_info"', '"appProps"')


@dataclass(frozen=True)
class Destino:
    """O conteúdo real que a vitrine representa."""
    tipo: str          # TIPO_PRODUTO | TIPO_LISTA
    url: str


# ══════════════════════════════════════════════════════════════════
# EXTRAÇÃO DO ESTADO
# ══════════════════════════════════════════════════════════════════
def _fim_do_objeto(texto: str, inicio: int) -> int:
    """
    Índice logo após a `}` que fecha o objeto aberto em `inicio`.

    Conta chaves respeitando string e escape: sem isso, uma `}`
    dentro de um título de produto fecharia o objeto errado.
    """
    prof, i, n = 0, inicio, len(texto)
    em_string = escapado = False
    while i < n:
        c = texto[i]
        if em_string:
            if escapado:
                escapado = False
            elif c == "\\":
                escapado = True
            elif c == '"':
                em_string = False
        elif c == '"':
            em_string = True
        elif c == "{":
            prof += 1
        elif c == "}":
            prof -= 1
            if prof == 0:
                return i + 1
        i += 1
    return -1


def _estado_do_script(corpo: str) -> Optional[Any]:
    """
    O maior objeto JSON atribuído que CONTÉM os marcadores.

    O critério é CONTEÚDO, não tamanho: um limiar de bytes rejeita
    objeto pequeno legítimo e aceita objeto grande irrelevante.
    """
    melhor = None
    for m in re.finditer(r"=\s*(\{)", corpo):
        fim = _fim_do_objeto(corpo, m.start(1))
        if fim <= 0:
            continue
        cru = corpo[m.start(1):fim]
        if not any(marca in cru for marca in _MARCADORES):
            continue
        try:
            obj = json.loads(cru)
        except Exception:
            continue
        if melhor is None or len(cru) > melhor[0]:
            melhor = (len(cru), obj)
    return melhor[1] if melhor else None


def _descer(no: Any, caminho) -> Any:
    """Desce pelo caminho; None se qualquer degrau faltar."""
    for passo in caminho:
        try:
            no = no[passo]
        except Exception:
            return None
    return no


def _desescapar(valor: str) -> str:
    """Formas em que o Mercado Livre escreve barra dentro de script."""
    return (valor.replace("\\u002F", "/").replace("\\u002f", "/")
                 .replace("\\/", "/").replace("\\u0026", "&"))


def _com_esquema(url: str) -> str:
    """
    Acrescenta o esquema quando falta.

    `polycards[0].url` chega como `www.mercadolivre.com.br/…`, e sem
    esquema o `urlparse` não enxerga o host — a validação de domínio
    falharia num link perfeitamente válido.
    """
    url = _desescapar((url or "").strip())
    if not url:
        return ""
    if url.startswith(("http://", "https://")):
        return url
    return "https://" + url.lstrip("/")


# ══════════════════════════════════════════════════════════════════
# LEITURA DA ESTRUTURA
# ══════════════════════════════════════════════════════════════════
def interpretar(documento: str) -> Optional[Destino]:
    """
    Lê o destino a partir do documento servido. Pura, sem I/O.

    Separada de `descobrir` de propósito: é ela que carrega a regra,
    e assim a regra fica testável sem rede.

    Devolve None sempre que a estrutura não for exatamente a
    esperada — nunca escolhe candidato por conta própria.
    """
    if not documento:
        return None

    blocos: list = []

    def _coletar(m):
        if not _RE_SRC.search(m.group(1)):
            blocos.append(m.group(2))
        return " "

    _RE_SCRIPT.sub(_coletar, documento)

    info = None
    for corpo in blocos:
        # Filtro por CONTEÚDO, nunca por tamanho. Um limiar de bytes
        # descarta bloco legítimo pequeno e aceita bloco grande
        # irrelevante; o marcador diz se vale abrir as chaves.
        if not any(marca in corpo for marca in _MARCADORES):
            continue
        estado = _estado_do_script(corpo)
        if estado is None:
            continue
        achado = _descer(estado, _CAMINHO)
        if isinstance(achado, dict):
            info = achado
            break

    if info is None:
        log_nrm.info("🛒 ML descoberta | estrutura não reconhecida")
        return None

    # ── LISTA: campo nomeado, o valor já é a URL ──────────────────
    ver_mais = info.get("seeMoreLink")
    if isinstance(ver_mais, str) and ver_mais.strip():
        url = _com_esquema(ver_mais)
        if links.cenario_de(url) == links.CENARIO_LISTAGEM:
            return Destino(tipo=TIPO_LISTA, url=url)
        log_nrm.info("🛒 ML descoberta | seeMoreLink não é listagem")
        return None

    # ── PRODUTO: exatamente um polycard ───────────────────────────
    cartoes = info.get("polycards")
    if not isinstance(cartoes, list) or len(cartoes) != 1:
        log_nrm.info(
            f"🛒 ML descoberta | polycards inesperado "
            f"({len(cartoes) if isinstance(cartoes, list) else 'ausente'})"
        )
        return None

    cartao = cartoes[0]
    if not isinstance(cartao, dict):
        return None
    bruta = cartao.get("url") or (cartao.get("metadata") or {}).get("url")
    url = _com_esquema(str(bruta or ""))
    if links.cenario_de(url) != links.CENARIO_PRODUTO:
        log_nrm.info("🛒 ML descoberta | polycard não é produto")
        return None

    return Destino(tipo=TIPO_PRODUTO, url=url)


# ══════════════════════════════════════════════════════════════════
# BUSCA
# ══════════════════════════════════════════════════════════════════
async def descobrir(
    url_social: str,
    sessao_http: aiohttp.ClientSession,
) -> Optional[Destino]:
    """
    Busca a vitrine e devolve o destino real, ou None.

    A URL é usada INTEIRA, com a query preservada: o `ref` é o que
    faz o servidor entregar a vitrine do item compartilhado em vez
    da vitrine genérica do afiliado.

    Anônima. Nunca levanta exceção.
    """
    if not url_social:
        return None

    try:
        async with config._SEM_HTTP:
            async with sessao_http.get(
                url_social,
                headers=_CABECALHOS,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=_TIMEOUT_S),
            ) as resposta:
                if resposta.status != 200:
                    log_nrm.info(
                        f"🛒 ML descoberta | HTTP {resposta.status}"
                    )
                    return None
                documento = await resposta.text(errors="ignore")
    except Exception as exc:
        log_nrm.warning(
            f"🛒 ML descoberta falhou: {type(exc).__name__}"
        )
        return None

    if len(documento) > _TETO_BYTES:
        log_nrm.info("🛒 ML descoberta | documento acima do teto")
        return None

    return interpretar(documento)
