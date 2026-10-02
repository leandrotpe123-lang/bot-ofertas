"""
Plataforma Shopee — conexão.

Domínio único: o POOL DE CONEXÕES HTTP da Shopee — onde a conexão com
a API de afiliados fica sempre aberta e a do site/encurtador fica
aberta entre usos próximos.

═══════════════════════════════════════════════════════════════════
POR QUE EXISTE — medido em produção (SHP_PERF, 30/09–01/10)
═══════════════════════════════════════════════════════════════════
Abrir conexão nova (TCP + TLS) custa idas e voltas de rede antes de a
requisição sequer sair. Na sessão comum do core a conexão ociosa fecha
em 15 s (padrão do aiohttp): entre um post e outro da Shopee quase toda
chamada pagava a abertura de novo. Média medida: expansão 590 ms (até
875 ms de madrugada), API 190 ms.

═══════════════════════════════════════════════════════════════════
COMO
═══════════════════════════════════════════════════════════════════
  - pool PRÓPRIO: a sessão comum do core e as outras plataformas não
    mudam em nada. Mesmos parâmetros da sessão comum, exceto a conexão
    ociosa, que fica aberta 60 s em vez de 15 s;
  - API de afiliados: SEMPRE aberta. A cada 20 s sem uso real, um HEAD
    na raiz do host da API mantém a conexão viva — não chama o serviço,
    não cria link, não gasta cota;
  - SITE e ENCURTADOR da Shopee: NUNCA recebem esse toque. Tráfego
    periódico de servidor contra a loja é o padrão que sistemas
    anti-robô bloqueiam; um IP marcado faria a expansão cair numa
    página de verificação — a identidade do produto sairia errada.
    Lá vale só o tempo maior de conexão aberta, sem requisição a mais.

═══════════════════════════════════════════════════════════════════
PROTEÇÕES — o toque é auxiliar e nunca pode atrapalhar a conversão
═══════════════════════════════════════════════════════════════════
  - nunca dois toques ao mesmo tempo; não toca com uso real recente;
  - 403 ou 429 (o servidor pediu calma) → PAUSA: 60 s, dobra a cada
    pausa seguida, teto 10 min; resposta normal zera;
  - 3 falhas de rede seguidas → a mesma PAUSA;
  - defeito (exceção inesperada) → o toque se desliga; o pool segue;
  - pool que não pôde ser criado → a Shopee usa a sessão comum, como
    antes desta frente;
  - encerramento: o toque para; o pool só fecha no fim do processo,
    depois do drain (que ainda pode converter links);
  - SHP_CONEXAO_QUENTE=0 → desliga tudo (sessão comum, como antes).

LOG: uma linha quando liga; um aviso por pausa ou desligamento. Nada
por toque. Com SHP_PERF=1, o resumo da Shopee ganha a contagem de
conexões novas × reaproveitadas — é o que mede o ganho em produção.
"""
from __future__ import annotations

import asyncio
import os
import random
import time
from urllib.parse import urlparse

import aiohttp

import config
import globals as g
from logger import log_nrm

__all__ = ["sessao", "usada", "resumo"]


# ── Parâmetros ────────────────────────────────────────────────────
_VARIAVEL = "SHP_CONEXAO_QUENTE"
_ABERTA_S = 60.0          # conexão ociosa no pool (sessão comum: 15 s)
_TOQUE_S = 20.0           # toque na API a cada 20 s sem uso (< _ABERTA_S)
_TIMEOUT_TOQUE_S = 5.0
_FALHAS_PARA_PAUSAR = 3
_PAUSA_BASE_S = 60.0
_PAUSA_MAX_S = 600.0
_PEDE_CALMA = frozenset({403, 429})

# Instrumentação TEMPORÁRIA — a mesma chave da Shopee (SHP_PERF=1).
_PERF = os.environ.get("SHP_PERF", "") == "1"


# ── Estado (processo único, event loop único) ─────────────────────
_estado: dict = {"sessao": None, "loop": None, "tarefa": None,
                 "ultimo_uso": 0.0, "indisponivel": False}
_conta: dict = {}
_tempo: dict = {}


def _ligada() -> bool:
    return (os.environ.get(_VARIAVEL) or "1").strip() != "0"


async def sessao(padrao, aquecer: str = ""):
    """O pool da Shopee — ou `padrao` (a sessão do core), intocada,
    quando desligado, indisponível ou fora do processo do bot.

    `aquecer`: URL da API de afiliados; a conexão com o host dela fica
    sempre aberta. Nunca levanta."""
    if not _ligada() or not isinstance(padrao, aiohttp.ClientSession):
        return padrao
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return padrao
    atual = _estado["sessao"]
    if atual is not None and not atual.closed and _estado["loop"] is loop:
        return atual
    if g._encerrando or _estado["indisponivel"]:
        return padrao
    try:
        nova = _criar()
    except Exception as e:
        _estado["indisponivel"] = True
        log_nrm.warning(f"⚠️ SHP conexão própria indisponível ({type(e).__name__}) "
                        f"— segue na sessão comum")
        return padrao
    # Toda pool tem uma dona: é ela que a fecha no fim do processo.
    _estado.update(sessao=nova, loop=loop, tarefa=loop.create_task(_dona(nova, aquecer)))
    return nova


def usada() -> None:
    """Uso real da API agora: o toque não precisa acontecer."""
    _estado["ultimo_uso"] = time.monotonic()


def resumo() -> str:
    """Contagem de conexões (SHP_PERF=1); cadeia vazia sem dados."""
    if not _conta:
        return ""
    partes = []
    for tipo in ("api", "loja"):
        nova, reuso = _conta.get(f"{tipo}_nova", 0), _conta.get(f"{tipo}_reuso", 0)
        if nova or reuso:
            media = _tempo.get(f"{tipo}_nova", 0.0) / max(nova, 1) * 1000
            partes.append(f"{tipo} nova={nova} reuso={reuso} abrir={media:.0f}ms")
    toques = _conta.get("toque", 0)
    if toques:
        partes.append(f"toques={toques} pausas={_conta.get('pausa', 0)}")
    return " · ".join(partes)


# ── internos ──────────────────────────────────────────────────────
def _criar() -> aiohttp.ClientSession:
    """Mesmos parâmetros da sessão comum (globals._get_session), exceto
    o tempo de conexão ociosa aberta."""
    conn = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, ssl=False,
                                keepalive_timeout=_ABERTA_S)
    return aiohttp.ClientSession(
        connector=conn,
        timeout=aiohttp.ClientTimeout(total=40, connect=8),
        headers={"User-Agent": random.choice(config.USER_AGENTS)},
        trace_configs=[_rastreio()] if _PERF else None,
    )


async def _dona(pool: aiohttp.ClientSession, endpoint: str) -> None:
    """Dona do pool até o fim do processo: toca a API enquanto puder e,
    cancelada no fim do processo (depois do drain), fecha o pool."""
    try:
        if endpoint:
            await _laco(pool, endpoint)
        await asyncio.Event().wait()        # sem toque: o pool segue servindo
    finally:
        if not pool.closed:
            await pool.close()


async def _laco(pool: aiohttp.ClientSession, endpoint: str) -> None:
    p = urlparse(endpoint)
    raiz = f"{p.scheme}://{p.netloc}/"
    log_nrm.info(f"🔥 SHP conexão quente | {p.netloc} a cada {_TOQUE_S:g}s "
                 f"· pool aberto {_ABERTA_S:g}s")
    espera, falhas, pausa = 0.0, 0, 0.0      # 1º toque já: abre a conexão agora
    try:
        while True:
            await asyncio.sleep(espera)
            espera = _TOQUE_S
            if g._encerrando or pool.closed:
                continue
            if time.monotonic() - _estado["ultimo_uso"] < _TOQUE_S:
                continue
            try:
                async with pool.head(
                    raiz, allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=_TIMEOUT_TOQUE_S),
                    trace_request_ctx={"toque": True},
                ) as r:
                    status = r.status
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
                falhas += 1
                if falhas < _FALHAS_PARA_PAUSAR:
                    continue
                status = None
            _conta["toque"] = _conta.get("toque", 0) + 1
            if status is None or status in _PEDE_CALMA:
                pausa = min(pausa * 2 or _PAUSA_BASE_S, _PAUSA_MAX_S)
                falhas, espera = 0, pausa
                _conta["pausa"] = _conta.get("pausa", 0) + 1
                motivo = f"http={status}" if status else "falhas de rede"
                log_nrm.warning(f"⚠️ SHP conexão quente em pausa {pausa:g}s ({motivo})")
                continue
            falhas, pausa = 0, 0.0
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log_nrm.warning(f"⚠️ SHP conexão quente desligada ({type(e).__name__}) "
                        f"— a conversão segue normal")


def _rastreio() -> aiohttp.TraceConfig:
    """Conexões novas × reaproveitadas por tipo de host (SHP_PERF=1).
    Nunca levanta: medição não pode derrubar requisição."""
    tc = aiohttp.TraceConfig()

    async def inicio(_s, ctx, params):
        try:
            extra = ctx.trace_request_ctx or {}
            ctx.tipo = ("toque" if extra.get("toque") else
                        "api" if (params.url.host or "").startswith("open-api.") else "loja")
        except Exception:
            ctx.tipo = "loja"

    async def abrindo(_s, ctx, _params):
        ctx.t0 = time.monotonic()

    async def aberta(_s, ctx, _params):
        try:
            if ctx.tipo != "toque":
                chave = f"{ctx.tipo}_nova"
                _conta[chave] = _conta.get(chave, 0) + 1
                _tempo[chave] = _tempo.get(chave, 0.0) + (time.monotonic() - ctx.t0)
        except Exception:
            pass

    async def reaproveitada(_s, ctx, _params):
        try:
            if ctx.tipo != "toque":
                chave = f"{ctx.tipo}_reuso"
                _conta[chave] = _conta.get(chave, 0) + 1
        except Exception:
            pass

    tc.on_request_start.append(inicio)
    tc.on_connection_create_start.append(abrindo)
    tc.on_connection_create_end.append(aberta)
    tc.on_connection_reuseconn.append(reaproveitada)
    return tc
