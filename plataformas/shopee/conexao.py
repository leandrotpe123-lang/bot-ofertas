"""
Plataforma Shopee — conexão.

Domínio único: o POOL DE CONEXÕES HTTP próprio da Shopee, onde a
conexão ociosa fica aberta por mais tempo entre usos.

═══════════════════════════════════════════════════════════════════
POR QUE EXISTE — medido em produção
═══════════════════════════════════════════════════════════════════
Abrir conexão nova (TCP + TLS) custa idas e voltas de rede antes de a
requisição sair. Na sessão comum do core a conexão ociosa fecha em
15 s (padrão do aiohttp). Média medida (SHP_PERF, 30/09–01/10):
expansão 590 ms, API 190 ms — quanto disso é abrir conexão NÃO foi
medido; é o que a contagem abaixo (SHP_PERF=1) passa a mostrar.

Intervalo entre chamadas consecutivas à API (produção, 27/09–02/10,
460 chamadas): 0 s (em paralelo) 35,5% · 1–15 s 17,9% (já reaproveita
hoje) · 16–60 s 21,8% · mais de 60 s 24,9%. Os 16–60 s são o que o
pool aberto por 60 s passa a reaproveitar — na API E na expansão,
sem nenhuma requisição a mais.

DESCARTADO: manter a API aberta com um toque periódico (HEAD a cada
20 s). Alcançaria só a API, nunca a expansão, que é o custo maior; e
não há evidência pública da Shopee sobre como esse tráfego conta em
limite ou proteção do servidor. Nenhuma requisição sai deste módulo.

═══════════════════════════════════════════════════════════════════
COMO
═══════════════════════════════════════════════════════════════════
  - pool PRÓPRIO: a sessão comum do core e as outras plataformas não
    mudam. Mesmos parâmetros da sessão comum, exceto a conexão ociosa,
    aberta 60 s em vez de 15 s. Quem decide fechar antes é o servidor;
    a conexão que ele fechou o aiohttp descarta do pool;
  - o pool é registrado no core e fechado EXPLICITAMENTE no
    encerramento (main._encerrar, passo 4, depois do drain);
  - pool que não pôde ser criado → a Shopee usa a sessão comum, como
    antes desta frente, sem nova tentativa;
  - no encerramento não se cria pool novo;
  - SHP_POOL_PROPRIO=0 → desliga (sessão comum, como antes).

LOG: uma linha quando o pool nasce; um aviso se não puder nascer. Com
SHP_PERF=1, o resumo da Shopee ganha a contagem de conexões novas ×
reaproveitadas (api/loja) e o tempo médio de abrir uma conexão.
"""
from __future__ import annotations

import asyncio
import os
import random
import time

import aiohttp

import config
import globals as g
from logger import log_nrm

__all__ = ["sessao", "resumo"]


# ── Parâmetros ────────────────────────────────────────────────────
_VARIAVEL = "SHP_POOL_PROPRIO"
_ABERTA_S = 60.0          # conexão ociosa no pool (sessão comum: 15 s)

# Instrumentação TEMPORÁRIA — a mesma chave da Shopee (SHP_PERF=1).
_PERF = os.environ.get("SHP_PERF", "") == "1"


# ── Estado (processo único, event loop único) ─────────────────────
_estado: dict = {"sessao": None, "loop": None, "indisponivel": False}
_conta: dict = {}
_tempo: dict = {}


def _ligado() -> bool:
    return (os.environ.get(_VARIAVEL) or "1").strip() != "0"


async def sessao(padrao):
    """O pool da Shopee — ou `padrao` (a sessão do core), intocada,
    quando desligado, indisponível ou fora do processo do bot. Nunca
    levanta."""
    if not _ligado() or not isinstance(padrao, aiohttp.ClientSession):
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
        log_nrm.warning(f"⚠️ SHP pool próprio indisponível ({type(e).__name__}) "
                        f"— segue na sessão comum")
        return padrao
    _estado.update(sessao=nova, loop=loop)
    g.registrar_sessao_extra(nova)          # fechado no encerramento (passo 4)
    log_nrm.info(f"🔌 SHP pool próprio | conexão ociosa aberta {_ABERTA_S:g}s")
    return nova


def resumo() -> str:
    """Contagem de conexões (SHP_PERF=1); cadeia vazia sem dados."""
    partes = []
    for tipo in ("api", "loja"):
        nova, reuso = _conta.get(f"{tipo}_nova", 0), _conta.get(f"{tipo}_reuso", 0)
        if nova or reuso:
            media = _tempo.get(f"{tipo}_nova", 0.0) / max(nova, 1) * 1000
            partes.append(f"{tipo} nova={nova} reuso={reuso} abrir={media:.0f}ms")
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


def _rastreio() -> aiohttp.TraceConfig:
    """Conexões novas × reaproveitadas por tipo de host (SHP_PERF=1).
    Nunca levanta: medição não pode derrubar requisição."""
    tc = aiohttp.TraceConfig()

    async def inicio(_s, ctx, params):
        try:
            ctx.tipo = "api" if (params.url.host or "").startswith("open-api.") else "loja"
        except Exception:
            ctx.tipo = "loja"

    async def abrindo(_s, ctx, _params):
        ctx.t0 = time.monotonic()

    async def aberta(_s, ctx, _params):
        try:
            chave = f"{ctx.tipo}_nova"
            _conta[chave] = _conta.get(chave, 0) + 1
            _tempo[chave] = _tempo.get(chave, 0.0) + (time.monotonic() - ctx.t0)
        except Exception:
            pass

    async def reaproveitada(_s, ctx, _params):
        try:
            chave = f"{ctx.tipo}_reuso"
            _conta[chave] = _conta.get(chave, 0) + 1
        except Exception:
            pass

    tc.on_request_start.append(inicio)
    tc.on_connection_create_start.append(abrindo)
    tc.on_connection_create_end.append(aberta)
    tc.on_connection_reuseconn.append(reaproveitada)
    return tc
