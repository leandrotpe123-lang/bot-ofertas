"""
Cliente HTTP do createLink — Mercado Livre.

Responsabilidade ÚNICA: falar com
`/affiliate-program/api/v2/affiliates/createLink`.

Recebe a credencial pronta, monta a requisição, interpreta a
resposta e devolve um resultado estruturado.

NÃO decide como renovar sessão. NÃO conhece cache, cenário,
elegibilidade ou publicação. Ao detectar credencial inválida,
sinaliza para a camada de cima e para por aí.

═══════════════════════════════════════════════════════════════════
ERRO VEM DENTRO DE HTTP 200
═══════════════════════════════════════════════════════════════════
O envelope traz `status: 200` mesmo quando o item falha:

    {"status": 200, "total_items": 1, "total_success": 0,
     "total_error": 1,
     "urls": [{"error_code": 111,
               "message": "URL not allowed in affiliates program"}]}

Qualquer leitura que olhe só o status HTTP conclui "deu certo".
Por isso a validação aqui é do ITEM, nunca do envelope.

═══════════════════════════════════════════════════════════════════
NOMENCLATURA TOLERANTE, COMPARAÇÃO ESTRITA
═══════════════════════════════════════════════════════════════════
A busca dos campos aceita snake_case e camelCase, para que uma
diferença de nomenclatura nunca produza falso negativo.

A comparação da tag é estrita e não perdoa nada — é a barreira que
impede publicar link de outro afiliado.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp
from yarl import URL

from logger import log_nrm

from . import afiliado, ajustes
from .sessao import Credencial


# ── Endpoint e cabeçalhos ─────────────────────────────────────────
ENDPOINT = (
    "https://www.mercadolivre.com.br"
    "/affiliate-program/api/v2/affiliates/createLink"
)
_ORIGIN = "https://www.mercadolivre.com.br"
_REFERER = "https://www.mercadolivre.com.br/affiliate-program"

# User-Agent de navegador. Cliente HTTP com UA de biblioteca é
# recusado pelo endpoint — verificado nos testes.
_USER_AGENT = ajustes.texto(
    "ML_USER_AGENT",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36",
)

_TIMEOUT_S = ajustes.numero("ML_TIMEOUT_S", 20)

# Formas de link curto que o programa devolve.
_RE_CURTO = re.compile(
    r"^https?://(?:meli\.la/\S+|(?:www\.)?mercadolivre\.com/sec/\S+)$",
    re.I,
)


class ErroCliente(Exception):
    """Falha na comunicação ou resposta inutilizável."""


class CredencialRecusada(ErroCliente):
    """
    O servidor recusou a credencial.

    Sinal para a camada de cima marcar a sessão como expirada.
    Este módulo não decide o que fazer a respeito.
    """


class UrlRecusada(ErroCliente):
    """
    O servidor recusou a URL enviada (ex.: error_code 111).

    Falha de ELEGIBILIDADE, não de credencial: a sessão continua
    válida e outras URLs seguem funcionando.
    """


@dataclass(frozen=True)
class LinkAfiliado:
    """
    Resultado validado de uma afiliação.

      - publicada : o link curto, o que vai ao canal
      - canonica  : a URL longa devolvida, que carrega a identidade
      - tag       : a etiqueta confirmada pelo servidor
    """
    publicada: str
    canonica: str
    tag: str


def _primeiro(dados: object, *chaves: str) -> object:
    """Primeiro valor não-vazio entre as chaves informadas."""
    if not isinstance(dados, dict):
        return None
    for chave in chaves:
        if chave in dados and dados[chave] not in (None, ""):
            return dados[chave]
    return None


def _localizar_item(dados: dict) -> dict:
    """
    Localiza o objeto de resultado, tolerando variações de envelope.

    Aceita `urls[0]` como objeto, `urls[0]` como string e campos no
    próprio topo da resposta.
    """
    lista = _primeiro(dados, "urls", "links", "results")
    if isinstance(lista, list) and lista:
        primeiro = lista[0]
        if isinstance(primeiro, dict):
            return primeiro
        if isinstance(primeiro, str):
            return {"short_url": primeiro}
    if _primeiro(dados, "short_url", "shortUrl"):
        return dados
    return {}


def _interpretar(corpo: str, status: int) -> LinkAfiliado:
    """
    Interpreta a resposta e devolve o link validado.

    Levanta CredencialRecusada, UrlRecusada ou ErroCliente. Nunca
    devolve link sem tag confirmada.
    """
    # Página de login em vez de JSON: credencial morta.
    inicio = corpo.lstrip()[:200].lower()
    if inicio.startswith(("<!doctype", "<html")):
        raise CredencialRecusada("servidor devolveu HTML em vez de JSON")

    try:
        dados = json.loads(corpo)
    except json.JSONDecodeError as exc:
        raise ErroCliente(f"resposta não é JSON: {exc}") from exc

    if not isinstance(dados, dict):
        raise ErroCliente("resposta JSON de tipo inesperado")

    if status in (401, 403):
        raise CredencialRecusada(f"HTTP {status}")
    if status >= 400:
        mensagem = _primeiro(dados, "message", "error", "msg") or ""
        raise ErroCliente(f"HTTP {status}: {str(mensagem)[:120]}")

    item = _localizar_item(dados)

    # Erro por item, dentro de HTTP 200.
    codigo = _primeiro(item, "error_code", "errorCode")
    mensagem = _primeiro(item, "message", "msg", "error")
    if codigo is not None or mensagem is not None:
        texto = f"error_code={codigo} {str(mensagem or '')[:100]}"
        if str(codigo) in ("401", "403"):
            raise CredencialRecusada(texto)
        raise UrlRecusada(texto)

    total_sucesso = _primeiro(dados, "total_success", "totalSuccess")
    if total_sucesso is not None and not total_sucesso:
        raise UrlRecusada("total_success = 0")

    criado = _primeiro(item, "created", "isCreated", "is_created")
    if criado is False:
        raise UrlRecusada("created = false")

    curto = _primeiro(item, "short_url", "shortUrl", "shortURL")
    if not isinstance(curto, str) or not _RE_CURTO.match(curto.strip()):
        raise ErroCliente(f"link curto irreconhecível: {str(curto)[:60]}")
    curto = curto.strip()

    # ── Barreira de atribuição ────────────────────────────────────
    tag = _primeiro(item, "tag", "affiliate_tag", "affiliateTag")
    if not afiliado.tag_confere(tag):
        raise ErroCliente(
            f"tag da resposta não é a nossa (recebida={tag!r})"
        )

    longa = _primeiro(item, "long_url", "longUrl") or curto
    longa = str(longa)
    if not afiliado.atribuicao_propria(longa):
        raise ErroCliente(
            "matt_word da URL devolvida pertence a outro afiliado"
        )

    return LinkAfiliado(publicada=curto, canonica=longa, tag=str(tag))


# ══════════════════════════════════════════════════════════════════
# SESSÃO HTTP PRIVADA
# ══════════════════════════════════════════════════════════════════
# Privada de propósito, exatamente como a Amazon faz com o
# SiteStripe: os cookies de afiliado NÃO podem entrar na sessão
# compartilhada do sistema, que é usada por todas as plataformas.
#
# O CookieJar é o que resolve a questão que a auditoria de sessão
# levantou. Medido na captura real do operador:
#
#     Set-Cookie: _mldataSessionId=…; Max-Age=1800
#
# O Mercado Livre rotaciona o id de sessão de dados a cada 30 min.
# O jar aceita esse Set-Cookie e o devolve na chamada seguinte —
# manutenção automática, sem código de renovação, sem navegador.
# É o comportamento normal de um cliente HTTP com cookies.
#
# Isso NÃO renova a sessão de LOGIN (o `ssid`), que continua sendo
# credencial provisionada manualmente. São coisas diferentes.
_sessao_ml: Optional[aiohttp.ClientSession] = None
_lock_sessao: Optional[asyncio.Lock] = None
_geracao_sessao = -1


# ══════════════════════════════════════════════════════════════════
# DISJUNTOR
# ══════════════════════════════════════════════════════════════════
# Herdado do amazon.py, pelo mesmo motivo que o comentário de lá
# registra: cookie morto vira centenas de tentativas por hora contra
# o Mercado Livre com sessão inválida — que é como uma conta de
# afiliado entra na mira.
#
# Aberto o circuito, as chamadas seguintes nem saem: devolvem falha
# imediata e a plataforma degrada para AUSENTE.
_LIMITE_FALHAS = ajustes.inteiro("ML_LIMITE_FALHAS", 3)
_PAUSA_DISJUNTOR = ajustes.numero("ML_PAUSA_DISJUNTOR_S", 300)

_falhas_seguidas = 0
_disjuntor_ate = 0.0


class DisjuntorAberto(ErroCliente):
    """
    Chamadas suspensas após falhas seguidas.

    Não é falha desta URL nem desta credencial: é proteção. A
    camada de cima degrada sem tentar de novo.
    """


def _obter_lock() -> asyncio.Lock:
    """Lock preguiçoso, instanciado no loop correto."""
    global _lock_sessao
    if _lock_sessao is None:
        _lock_sessao = asyncio.Lock()
    return _lock_sessao


async def _obter_sessao(credencial: Credencial,
                        geracao: int) -> aiohttp.ClientSession:
    """
    Sessão HTTP privada, semeada com a credencial provisionada.

    Recriada quando a geração da credencial muda — troca de
    `ML_SESSION_COOKIE` invalida o jar inteiro, para que cookies da
    sessão antiga não sobrevivam à nova.
    """
    global _sessao_ml, _geracao_sessao

    if (_sessao_ml is not None and not _sessao_ml.closed
            and _geracao_sessao == geracao):
        return _sessao_ml

    async with _obter_lock():
        if (_sessao_ml is not None and not _sessao_ml.closed
                and _geracao_sessao == geracao):
            return _sessao_ml

        if _sessao_ml is not None and not _sessao_ml.closed:
            try:
                await _sessao_ml.close()
            except Exception:
                pass

        jar = aiohttp.CookieJar()
        pares = {}
        for parte in credencial.cookie.split(";"):
            nome, sep, valor = parte.strip().partition("=")
            if sep and nome:
                pares[nome] = valor
        if pares:
            jar.update_cookies(pares, response_url=URL(_ORIGIN))

        _sessao_ml = aiohttp.ClientSession(cookie_jar=jar)
        _geracao_sessao = geracao
        log_nrm.info(
            f"🛒 ML sessão HTTP criada | cookies={len(pares)} "
            f"geracao={geracao}"
        )
    return _sessao_ml


async def encerrar() -> None:
    """Fecha a sessão privada. Idempotente."""
    global _sessao_ml, _geracao_sessao
    if _sessao_ml is not None and not _sessao_ml.closed:
        try:
            await _sessao_ml.close()
        except Exception:
            pass
    _sessao_ml = None
    _geracao_sessao = -1


def _registrar_falha(motivo: str) -> None:
    """Contabiliza a falha e abre o disjuntor no limite."""
    global _falhas_seguidas, _disjuntor_ate
    _falhas_seguidas += 1
    log_nrm.warning(
        f"🛒 ML createLink falhou ({_falhas_seguidas}/{_LIMITE_FALHAS}): "
        f"{motivo}"
    )
    if _falhas_seguidas >= _LIMITE_FALHAS:
        _disjuntor_ate = time.monotonic() + _PAUSA_DISJUNTOR
        log_nrm.error(
            f"🛒 ML createLink suspenso por {int(_PAUSA_DISJUNTOR)}s "
            f"— protegendo a conta de afiliado"
        )


def _registrar_sucesso() -> None:
    """Zera o contador. Sucesso fecha o circuito."""
    global _falhas_seguidas
    _falhas_seguidas = 0


def estado_disjuntor() -> dict:
    """Retrato seguro para log e diagnóstico."""
    restante = max(0.0, _disjuntor_ate - time.monotonic())
    return {
        "falhas_seguidas": _falhas_seguidas,
        "aberto": restante > 0,
        "reabre_em_s": round(restante),
    }


def _resetar_para_teste() -> None:
    """Restaura o estado do módulo. Uso exclusivo de teste."""
    global _sessao_ml, _lock_sessao, _geracao_sessao
    global _falhas_seguidas, _disjuntor_ate
    _sessao_ml = None
    _lock_sessao = None
    _geracao_sessao = -1
    _falhas_seguidas = 0
    _disjuntor_ate = 0.0


async def criar_link(
    url: str,
    credencial: Credencial,
    geracao: int = 0,
    sessao_http: Optional[aiohttp.ClientSession] = None,
) -> LinkAfiliado:
    """
    Chama o createLink para uma URL já sanitizada e elegível.

    Body mínimo comprovado em teste real:
        {"urls": ["<URL>"], "tag": "<ML_TAG>"}

    `sessao_http` existe para teste: quando informada, é usada no
    lugar da sessão privada. Em produção fica vazia, e o módulo usa
    o seu próprio jar.

    Não valida elegibilidade — isso é de `links.py`, e checar duas
    vezes espalharia a mesma regra por dois módulos.
    """
    if time.monotonic() < _disjuntor_ate:
        raise DisjuntorAberto(
            f"suspenso por mais "
            f"{int(_disjuntor_ate - time.monotonic())}s"
        )

    corpo = json.dumps({"urls": [url], "tag": afiliado.TAG})

    cabecalhos = {
        "User-Agent": _USER_AGENT,
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Origin": _ORIGIN,
        "Referer": _REFERER,
        "Cookie": credencial.cookie,
        "x-csrf-token": credencial.csrf,
    }

    sessao = sessao_http or await _obter_sessao(credencial, geracao)

    try:
        async with sessao.post(
            ENDPOINT,
            data=corpo.encode("utf-8"),
            headers=cabecalhos,
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT_S),
            allow_redirects=False,
        ) as resposta:
            # Redirecionamento para login é credencial morta.
            if resposta.status in (301, 302, 303, 307, 308):
                raise CredencialRecusada(
                    f"redirecionamento HTTP {resposta.status}"
                )
            texto = await resposta.text(errors="ignore")
            resultado = _interpretar(texto, resposta.status)

    except UrlRecusada:
        # Recusa de URL NÃO conta para o disjuntor: a sessão está
        # viva e outras URLs seguem funcionando. Contar aqui abriria
        # o circuito por três vitrines seguidas, punindo o sistema
        # por um comportamento correto.
        raise
    except ErroCliente as exc:
        _registrar_falha(str(exc)[:80])
        raise
    except aiohttp.ClientError as exc:
        _registrar_falha(f"rede: {type(exc).__name__}")
        raise ErroCliente(f"rede: {type(exc).__name__}") from exc
    except Exception as exc:
        # Nunca deixa vazar exceção não classificada, e nunca inclui
        # o objeto original na mensagem: ele pode conter cabeçalhos.
        _registrar_falha(f"inesperado: {type(exc).__name__}")
        raise ErroCliente(f"inesperado: {type(exc).__name__}") from exc

    _registrar_sucesso()
    return resultado
