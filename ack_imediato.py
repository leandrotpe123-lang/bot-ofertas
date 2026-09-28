"""S6.4 — ACK imediato do Telegram (transporte). Ligado no boot.

═══════════════════════════════════════════════════════════════════
POR QUE EXISTE — medido em produção (S6.2, deploy 3ffacce5)
═══════════════════════════════════════════════════════════════════
O Telethon 1.45.0 só confirma (MsgsAck) as mensagens recebidas no
PRÓXIMO envio: `_process_message` apenas acumula o msg_id em
`_pending_ack`, e o `_send_loop` só monta o MsgsAck no topo do laço —
que fica parado em `_send_queue.get()` enquanto nada sai. Em período
ocioso o único envio é o ping do keepalive, a cada 60 s, e o servidor
segurava os updates até ele: NEWs chegavam em rajada ~1 RTT depois de
PING+MsgsAck, com até ~60 s de idade.

═══════════════════════════════════════════════════════════════════
COMO
═══════════════════════════════════════════════════════════════════
Um handler Raw (API pública) agenda, a cada update despachado, no
máximo UM empurrão por janela fixa de 0,2 s. O empurrão só acorda o
`_send_loop` (`_send_queue._ready.set()`) quando há ack pendente.

Quem monta o MsgsAck, registra em `_last_acks`, limpa `_pending_ack`,
atribui o msg_id e envia é o PRÓPRIO `_send_loop` do Telethon. Nenhuma
linha do Telethon é alterada, substituída ou envolvida.

═══════════════════════════════════════════════════════════════════
POR QUE É SEGURO (validado no código 1.45.0 — S6.3/S6.3.1)
═══════════════════════════════════════════════════════════════════
  - Acordado com a fila vazia, `MessagePacker.get()` devolve
    (None, None) e o `_send_loop` volta ao topo, onde monta o ack. Se
    ele já estava enviando, volta ao topo sozinho. Nenhum ack se perde.
  - Nada é enfileirado: FIFO, `_pending_state`, reconnect, disconnect e
    o flood control de RPC ficam intocados (MsgsAck não é TLRequest e
    não passa por `client._call`).
  - Janela FIXA, não debounce: um fluxo contínuo de updates nunca adia
    o ack indefinidamente.
  - Nenhum TimerHandle guardado: a trava é um instante no relógio do
    loop, que expira sozinho. Se um empurrão se perder, o próximo
    update agenda outro — a trava nunca fica presa.
  - Nenhuma task: só `call_later` enquanto chegam updates.
  - A coalescência limita o despertar a <= 5/s. Flood de transporte
    (429) no 1.45 derruba a conexão; um ack por pacote seria o risco.
  - Só o sender PRINCIPAL (`client._sender`): os senders de outras DCs
    (mídia) não são tocados.

PRÉ-CONDIÇÕES — falhando qualquer uma, NÃO instala, loga o motivo e o
Telethon segue exatamente nativo:
  telethon == 1.45.0; client._sender; sender._pending_ack;
  sender._send_queue; sender._send_queue._ready.

REVERSÃO: remover a chamada `ack_imediato.instalar(client)` em main.py.
"""
from __future__ import annotations

import asyncio

import telethon
from telethon import events

from logger import log_sys

__all__ = ["instalar"]


# ── Parâmetros ────────────────────────────────────────────────────
# A estrutura interna usada abaixo foi validada SÓ nesta versão.
_VERSAO_TELETHON = "1.45.0"

# Janela de coalescência: a rajada inteira vira ~1 MsgsAck. 0,2 s é
# desprezível diante dos ~60 s medidos e mantém o despertar <= 5/s.
_JANELA_S = 0.2

# Falha interna nunca propaga, mas também nunca fica calada.
_MAX_ERROS = 5


# ── Estado (processo único, event loop único) ─────────────────────
# `prox`: instante (relógio do loop) em que o empurrão agendado dispara.
# Enquanto não chega, updates novos já estão cobertos por ele.
_estado: dict = {"cliente": None, "prox": 0.0, "erros": 0}


def _erro(onde: str, exc: BaseException) -> None:
    try:
        n = _estado["erros"] = _estado["erros"] + 1
        if n <= _MAX_ERROS:
            log_sys.warning(
                f"⚠️ ACK imediato: falha em {onde} ({type(exc).__name__}) — "
                f"o ack segue no próximo envio nativo")
    except Exception:
        pass


def _empurrar() -> None:
    """Callback do call_later. Só acorda o _send_loop; o ack é dele."""
    try:
        sender = _estado["cliente"]._sender
        if sender._pending_ack:
            sender._send_queue._ready.set()
    except Exception as e:
        _erro("empurrar", e)


async def _ao_update(_update) -> None:
    """Handler Raw. Sem await: só agenda — nunca espera o pipeline."""
    try:
        loop = asyncio.get_running_loop()
        agora = loop.time()
        if agora >= _estado["prox"]:
            _estado["prox"] = agora + _JANELA_S
            loop.call_later(_JANELA_S, _empurrar)
    except Exception as e:
        _erro("agendar", e)


def _motivo_recusa(client) -> str:
    """Vazio quando todas as pré-condições valem; senão, o motivo."""
    versao = getattr(telethon, "__version__", "?")
    if versao != _VERSAO_TELETHON:
        return f"telethon={versao} (exige {_VERSAO_TELETHON})"
    sender = getattr(client, "_sender", None)
    if sender is None:
        return "client._sender ausente"
    if not hasattr(sender, "_pending_ack"):
        return "sender._pending_ack ausente"
    fila = getattr(sender, "_send_queue", None)
    if fila is None:
        return "sender._send_queue ausente"
    if not callable(getattr(getattr(fila, "_ready", None), "set", None)):
        return "sender._send_queue._ready ausente"
    return ""


def instalar(client) -> bool:
    """Liga o ACK imediato no cliente PRINCIPAL. UMA vez por processo.

    Deve ser chamada ANTES dos handlers da casa: o `_dispatch_update`
    aguarda os callbacks em sequência. Devolve True se ativo."""
    if _estado["cliente"] is not None:
        return _estado["cliente"] is client
    try:
        motivo = _motivo_recusa(client)
        if not motivo:
            client.add_event_handler(_ao_update, events.Raw())
    except Exception as e:
        motivo = f"falha ao instalar ({type(e).__name__})"
    if motivo:
        log_sys.warning(
            f"⚠️ ACK imediato NÃO instalado | motivo={motivo} — "
            f"comportamento nativo do Telethon")
        return False
    _estado["cliente"] = client
    log_sys.info(
        f"⚡ ACK imediato ativo | telethon={telethon.__version__} "
        f"janela={_JANELA_S:g}s")
    return True
