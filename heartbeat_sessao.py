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

PROTEÇÕES — o heartbeat é auxiliar e nunca pode comprometer o bot:
  - timeout explícito por ping;
  - 3 timeouts seguidos → desativa (sucesso zera o contador);
  - FloodWait ou qualquer RPCError → desativa;
  - erro de conexão → registra e segue (a reconexão é do Telethon);
  - desconectado → pula o ciclo;
  - qualquer outra exceção → desativa.

VARIÁVEL HEARTBEAT_PING_S — ausente, vazia, 0, negativa ou inválida:
DESLIGADO. Decimal aceito. Abaixo de 1 s vale 1 s (piso desta frente).

LOGS — nenhum por ping: uma linha no boot, um resumo a cada ~10 min e
um WARNING se desativar.

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
_MAX_TIMEOUTS = 3        # timeouts SEGUIDOS antes de desativar
_RESUMO_S = 600.0        # um resumo a cada ~10 min


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
                 "rtts", "ultimo_rtt_ms", "inicio")

    def __init__(self) -> None:
        self.inicio = time.monotonic()
        self.pings = self.ok = self.timeouts = 0
        self.erros_conn = self.desconectado = 0
        self.rtts: list = []
        self.ultimo_rtt_ms = -1

    def resumir(self) -> None:
        med = round(statistics.median(self.rtts)) if self.rtts else -1
        log_sys.info(
            f"💓 HEARTBEAT|RESUMO|pings={self.pings}|ok={self.ok}"
            f"|timeouts={self.timeouts}|erros_conn={self.erros_conn}"
            f"|desconectado={self.desconectado}"
            f"|rtt_ult_ms={self.ultimo_rtt_ms}|rtt_med_ms={med}")
        self.__init__()


def _desativar(motivo: str) -> None:
    log_sys.warning(f"💓 HEARTBEAT|DESATIVADO|motivo={motivo}")


async def _laco(client, intervalo: float, timeout: float = _TIMEOUT_S,
                resumo_s: float = _RESUMO_S) -> None:
    """Task única e sequencial. Termina por cancelamento, encerramento
    do processo ou desativação pelo circuit breaker."""
    est = _Estatistica()
    seguidos = 0
    ultimo_inicio = time.monotonic()
    try:
        while True:
            espera = intervalo - (time.monotonic() - ultimo_inicio)
            await asyncio.sleep(max(0.0, espera))
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
                seguidos += 1
                if seguidos >= _MAX_TIMEOUTS:
                    _desativar(f"{seguidos}_timeouts_seguidos")
                    return
                continue
            except errors.RPCError as e:
                _desativar(type(e).__name__)
                return
            except (ConnectionError, OSError):
                est.erros_conn += 1
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                _desativar(f"inesperado_{type(e).__name__}")
                return

            seguidos = 0
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
