"""S6.8-A — Heartbeat de sessão do Telegram (recebimento). Desligado por padrão.

═══════════════════════════════════════════════════════════════════
POR QUE EXISTE — medido em produção (S6.2 / S6.4 / S6.5)
═══════════════════════════════════════════════════════════════════
O servidor do Telegram segura os updates da sessão e os libera ~1 RTT
depois de receber uma requisição dela. Um PING (relógio nosso, sem
pedido de conteúdo, com zero ack pendente) liberou 100% das retenções
observadas. Hoje a única requisição proativa é o keepalive nativo do
Telethon, a cada 60 s: é esse o teto do atraso (mediana 16 s, máx 49 s
entre a criação da mensagem e a chegada no socket).

═══════════════════════════════════════════════════════════════════
COMO
═══════════════════════════════════════════════════════════════════
Uma única task sequencial envia `functions.PingRequest` pela API
pública (`client(...)`) a cada HEARTBEAT_PING_S segundos. É atividade
ADICIONAL: o keepalive nativo do Telethon segue intacto (ele só reage
ao próprio ping_id) e nada do pipeline é tocado.

CADÊNCIA — intervalo entre INÍCIOS de pings ≈ T. O tempo gasto no ping
é descontado da espera seguinte. O próximo ping só começa depois que
o anterior terminou: nunca há dois em voo. Se um ping demorar mais que
T, o seguinte sai assim que ele termina, sem rajada de recuperação.

PROTEÇÕES — o heartbeat é auxiliar e nunca pode comprometer o bot.
Falha TRANSITÓRIA pausa e volta; só falha PERMANENTE desativa:
  - timeout explícito por ping;
  - 3 falhas transitórias seguidas (timeout, erro de conexão) → PAUSA
    (sucesso zera o contador);
  - RPCError transitório (500, 503, 303, flood sem prazo, código
    desconhecido) → PAUSA na hora (o servidor respondeu com erro);
  - a PAUSA cresce em backoff exponencial conservador — 30 s, 60 s,
    120 s, 240 s, teto 300 s — e zera no primeiro sucesso; depois dela
    o laço volta à cadência normal. Em falha persistente: no máximo
    uma rodada de tentativas a cada 5 min, nunca uma tempestade;
  - FloodWait → dorme EXATAMENTE o prazo pedido e volta;
  - permanente de autenticação/configuração (400, 401, 403, 404, 406)
    → desativa na hora, sem nenhuma nova tentativa;
  - desconectado → pula o ciclo;
  - qualquer outra exceção (defeito, não rede) → desativa.

VARIÁVEL HEARTBEAT_PING_S — ausente, vazia, 0, negativa ou inválida:
DESLIGADO. Decimal aceito. Abaixo de 1 s vale 1 s (piso desta frente).

LOGS — nenhum por ping: uma linha no boot, um resumo a cada ~10 min,
um WARNING por PAUSA/FLOODWAIT, um INFO ao RETOMAR e um WARNING se
desativar.

REVERSÃO: remover a variável (ou 0) e reiniciar.
"""
from __future__ import annotations

import asyncio
import math
import os
import random
import statistics
import time
from typing import Optional

from telethon import errors, functions

import globals as g
from logger import log_sys

__all__ = ["iniciar", "intervalo_configurado"]


# ── Parâmetros ────────────────────────────────────────────────────
_VARIAVEL = "HEARTBEAT_PING_S"
_PISO_S = 1.0            # nesta frente, nunca abaixo de 1 s
_TIMEOUT_S = 10.0        # ~100x o RTT medido (85 ms); cobre fila atrás de upload
_MAX_TIMEOUTS = 3        # falhas transitórias SEGUIDAS antes de pausar
_RESUMO_S = 600.0        # um resumo a cada ~10 min
_PAUSA_BASE_S = 30.0     # 1ª pausa; dobra a cada pausa seguida sem sucesso
_PAUSA_MAX_S = 300.0     # teto da pausa: falha persistente = 1 rodada a cada 5 min

# Erros de autenticação/configuração: nenhuma nova tentativa os resolve.
# Todo outro RPCError é tratado como transitório (pausa com backoff).
_PERMANENTES = (errors.BadRequestError, errors.UnauthorizedError,
                errors.ForbiddenError, errors.NotFoundError,
                errors.AuthKeyError)


def intervalo_configurado() -> Optional[float]:
    """Intervalo efetivo em segundos, ou None quando desligado."""
    bruto = (os.environ.get(_VARIAVEL) or "").strip()
    if not bruto:
        return None
    try:
        valor = float(bruto)
    except ValueError:
        return None
    if not math.isfinite(valor) or valor <= 0:
        return None
    return max(valor, _PISO_S)


class _Estatistica:
    __slots__ = ("pings", "ok", "timeouts", "erros_conn", "desconectado",
                 "erros_rpc", "pausas", "floodwaits",
                 "rtts", "ultimo_rtt_ms", "inicio")

    def __init__(self) -> None:
        self.inicio = time.monotonic()
        self.pings = self.ok = self.timeouts = 0
        self.erros_conn = self.desconectado = 0
        self.erros_rpc = self.pausas = self.floodwaits = 0
        self.rtts: list = []
        self.ultimo_rtt_ms = -1

    def resumir(self) -> None:
        med = round(statistics.median(self.rtts)) if self.rtts else -1
        log_sys.info(
            f"💓 HEARTBEAT|RESUMO|pings={self.pings}|ok={self.ok}"
            f"|timeouts={self.timeouts}|erros_conn={self.erros_conn}"
            f"|desconectado={self.desconectado}"
            f"|rtt_ult_ms={self.ultimo_rtt_ms}|rtt_med_ms={med}"
            f"|erros_rpc={self.erros_rpc}|pausas={self.pausas}"
            f"|floodwaits={self.floodwaits}")
        self.__init__()


def _desativar(motivo: str) -> None:
    log_sys.warning(f"💓 HEARTBEAT|DESATIVADO|motivo={motivo}")


def _sair_se_cancelado() -> None:
    """Python < 3.12: wait_for ENGOLE o cancelamento quando o ping
    termina (com sucesso ou erro) no mesmo instante do cancel() — o laço
    seguiria e o shutdown esperaria a task para sempre. O pedido fica
    registrado em cancelling(); aqui ele é honrado. Em 3.12+ (produção:
    3.13) é no-op: wait_for já propaga o cancelamento."""
    tarefa = asyncio.current_task()
    if tarefa is not None and tarefa.cancelling():
        raise asyncio.CancelledError


async def _laco(client, intervalo: float, timeout: float = _TIMEOUT_S,
                resumo_s: float = _RESUMO_S, pausa_base: float = _PAUSA_BASE_S,
                pausa_max: float = _PAUSA_MAX_S) -> None:
    """Task única e sequencial. Termina por cancelamento, encerramento
    do processo ou erro PERMANENTE. Falha transitória nunca a mata:
    pausa (backoff exponencial com teto) e volta à cadência normal."""
    est = _Estatistica()
    falhas = 0               # transitórias seguidas (timeout, conexão)
    pausas = 0               # pausas seguidas sem nenhum sucesso no meio
    retomar = ""             # motivo da última interrupção, até o 1º sucesso
    ultimo_inicio = time.monotonic()

    async def _pausar(motivo: str) -> None:
        nonlocal falhas, pausas, retomar, ultimo_inicio
        _sair_se_cancelado()
        pausas += 1
        est.pausas += 1
        pausa = min(pausa_base * 2 ** (pausas - 1), pausa_max)
        log_sys.warning(
            f"💓 HEARTBEAT|PAUSA|motivo={motivo}|pausa_s={pausa:g}|pausas={pausas}")
        await asyncio.sleep(pausa)
        falhas, retomar = 0, motivo
        ultimo_inicio = time.monotonic() - intervalo   # tenta logo após a pausa

    try:
        while True:
            espera = intervalo - (time.monotonic() - ultimo_inicio)
            await asyncio.sleep(max(0.0, espera))
            _sair_se_cancelado()
            ultimo_inicio = time.monotonic()

            if time.monotonic() - est.inicio >= resumo_s:
                est.resumir()
            if g._encerrando:
                return
            if not client.is_connected():
                est.desconectado += 1
                continue

            est.pings += 1
            t0 = time.monotonic()
            try:
                await asyncio.wait_for(
                    client(functions.PingRequest(
                        ping_id=random.randrange(-2 ** 63, 2 ** 63))),
                    timeout)
            except asyncio.TimeoutError:
                est.timeouts += 1
                falhas += 1
                if falhas >= _MAX_TIMEOUTS:
                    await _pausar(f"{falhas}_falhas_seguidas")
                continue
            except errors.FloodError as e:
                segundos = getattr(e, "seconds", None)
                if not isinstance(segundos, int) or segundos < 0:
                    est.erros_rpc += 1
                    await _pausar(type(e).__name__)
                    continue
                est.floodwaits += 1
                log_sys.warning(f"💓 HEARTBEAT|FLOODWAIT|s={segundos}")
                _sair_se_cancelado()
                await asyncio.sleep(segundos)       # exatamente o prazo pedido
                falhas, retomar = 0, type(e).__name__
                ultimo_inicio = time.monotonic() - intervalo
                continue
            except _PERMANENTES as e:
                _desativar(type(e).__name__)
                return
            except errors.RPCError as e:
                est.erros_rpc += 1
                await _pausar(type(e).__name__)
                continue
            except (ConnectionError, OSError):
                est.erros_conn += 1
                falhas += 1
                if falhas >= _MAX_TIMEOUTS:
                    await _pausar(f"{falhas}_falhas_seguidas")
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                _desativar(f"inesperado_{type(e).__name__}")
                return

            if retomar:
                log_sys.info(
                    f"💓 HEARTBEAT|RETOMADO|apos={retomar}|pausas={pausas}")
            falhas, pausas, retomar = 0, 0, ""
            est.ok += 1
            est.ultimo_rtt_ms = round((time.monotonic() - t0) * 1000)
            est.rtts.append(est.ultimo_rtt_ms)
    except asyncio.CancelledError:
        return


def iniciar(client) -> Optional[asyncio.Task]:
    """Cria a task do heartbeat se HEARTBEAT_PING_S estiver ligada.
    Devolve a task (para o shutdown cancelar) ou None quando desligado.
    Não bloqueia: só agenda."""
    intervalo = intervalo_configurado()
    if intervalo is None:
        log_sys.info("💓 HEARTBEAT|OFF")
        return None
    log_sys.info(
        f"💓 HEARTBEAT|ATIVO|intervalo={intervalo:g}s|timeout={_TIMEOUT_S:g}s")
    return asyncio.get_running_loop().create_task(_laco(client, intervalo))
