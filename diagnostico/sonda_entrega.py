"""S6.1 — Sonda de ENTREGA do Telegram. DIAGNÓSTICO, desligada por padrão.

Pergunta única que esta sonda responde:
    quanto tempo passa entre a mensagem existir no canal monitorado
    (message.date) e o update chegar a este processo — e o que a conexão
    estava fazendo naquele instante (requisições de saída, ping do
    keepalive, pong, reconexão, FloodWait).

O QUE ELA FAZ
  1. Registra UM handler Raw adicional, restrito aos canais monitorados.
     Os handlers da casa não são tocados; este handler não tem await,
     não lança exceção e não chama StopPropagation.
  2. Liga um GRAMPO nos loggers que o próprio Telethon já possui
     (telethon.extensions.messagepacker, telethon.network.mtprotosender,
     telethon.client.users, telethon.client.updates). Nenhuma linha de
     código do Telethon é alterada, substituída ou envolvida.

O QUE ELA NÃO FAZ
  - não conhece pipeline, oferta, plataforma, banco, decisão;
  - não altera keepalive, reconexão, lanes, orçamento, envio;
  - não faz requisição nenhuma ao Telegram.

PRIVACIDADE — REGRA ESTRUTURAL
  Só sai deste módulo: ids numéricos, datas, milissegundos, contagens e
  NOMES DE CLASSE de requisição/update. Nunca texto, URL, mídia,
  credencial, argumento de requisição, corpo de resposta ou endereço.
  O grampo NUNCA reemite a mensagem original do Telethon: várias delas
  carregam o objeto da requisição ou o corpo da resposta
  ("Received response without parent request: %s",
  "Request caused struct.error: %s: %s", ...). Cada linha emitida é
  montada aqui, campo a campo.

PRESERVAÇÃO DO COMPORTAMENTO DE LOG
  Hoje o Telethon não tem handler; seus WARNING+ saem pelo
  logging.lastResort. Ao pendurar o grampo, o lastResort deixaria de ser
  usado para esses registros. Por isso o grampo repassa ao lastResort
  todo registro >= WARNING, exatamente como antes. Registros abaixo de
  WARNING continuam não impressos (só viram linhas SONDA quando estão
  na lista branca).

ATIVAÇÃO E REVERSÃO
  main.py importa este módulo SOMENTE com SONDA_ENTREGA=1.
  Reverter: remover a variável (próximo boot volta ao normal) ou apagar
  este pacote e o bloco marcado [S6.1] em main.py.

FORMATO (uma linha por fato, chave=valor, t em epoch ms)
  SONDA|INICIO|t=..|canais=N|telethon=X.Y.Z|gap_lote_ms=..
  SONDA|UPD|t=..|lote=N|chat=-100..|id=..|tipo=new|edit|date_ms=..|edit_ms=..|idade_ms=..|desde_req_ms=..|req_ant=Classe|desde_ping_ms=..
  SONDA|REQ|t=..|req=Classe
  SONDA|PING|t=..|req=PingRequest
  SONDA|PONG|t=..|rtt_ms=..
  SONDA|NET|t=..|classe=Updates|UpdateShort|...
  SONDA|CONN|t=..|evento=conectando|conectado|reconectando|...
  SONDA|FLOOD|t=..|seg=..|req=Classe|antecipado=0|1
  SONDA|DIFF|t=..|evento=pedindo_conta|pedindo_canal|recebeu_conta|recebeu_canal|timeout_espera
  SONDA|ERRO|t=..|onde=upd|grampo|exc=ClasseDaExcecao|n=..   (falha interna da própria sonda)
"""
from __future__ import annotations

import logging
import time
from typing import Dict, FrozenSet, Optional

# ── Parâmetros ────────────────────────────────────────────────────
# Updates cujo intervalo para o anterior é <= este valor pertencem ao
# mesmo LOTE. 250 ms separa com folga "chegaram no mesmo pacote/rajada
# de rede" de "chegaram em momentos distintos".
_GAP_LOTE_MS = 250

_NOME_LOG = "SONDA"
_LOGGERS_TELETHON = {
    # logger                               nível mínimo a observar
    "telethon.extensions.messagepacker":   logging.DEBUG,
    "telethon.network.mtprotosender":      logging.DEBUG,
    "telethon.client.users":               logging.INFO,
    "telethon.client.updates":             logging.DEBUG,
}

# Lista BRANCA de modelos de mensagem do Telethon → evento emitido.
# A comparação é pelo MODELO (record.msg), nunca pelo texto formatado.
_MODELO_REQ   = "Assigned msg_id = %d to %s (%x)"
_MODELO_NET   = "Handling update %s"
_MODELO_PONG  = "Handling pong for message %d"
_MODELO_FLOOD = "Sleeping%s for %ds (%s) on %s flood wait"
_CONEXAO = {
    "Connecting to %s...":                                   "conectando",
    "Connection to %s complete!":                            "conectado",
    "Closing current connection to begin reconnect...":      "reconectando",
    "Failed reconnection attempt %d with %s":                "reconexao_falhou",
    "Automatic reconnection failed %d time(s)":              "reconexao_desistiu",
    "Connection closed while receiving data: %s":            "fechada_recebendo",
    "Connection closed while sending data":                  "fechada_enviando",
    "Disconnecting from %s...":                              "desconectando",
    "Disconnection from %s complete!":                       "desconectado",
    "Server indicated flood error at transport level: %s":   "flood_transporte",
}
_DIFF = {
    "Getting difference for account updates":         "pedindo_conta",
    "Getting difference for channel %s updates":      "pedindo_canal",
    "Got difference for account updates":             "recebeu_conta",
    "Got difference for channel %d updates":          "recebeu_canal",
    "Timeout waiting for updates expired":            "timeout_espera",
}
_PINGS = frozenset({"PingRequest", "PingDelayDisconnectRequest"})


# ── Estado (processo único, event loop único) ─────────────────────
_estado: Dict[str, object] = {
    "chats": frozenset(),
    "ult_req_ms": 0,
    "ult_req_cls": "",
    "ult_ping_ms": 0,
    "ult_upd_ms": 0,
    "lote": 0,
}
_instalada: bool = False
_handler_raw = None
_grampo: Optional[logging.Handler] = None
_niveis_antigos: Dict[str, int] = {}

_log = logging.getLogger(_NOME_LOG)


def _agora_ms() -> int:
    return int(time.time() * 1000)


def _emitir(evento: str, t: int, /, **campos) -> None:
    # Posicionais-apenas: nenhum nome de campo (ex.: tipo=new|edit) pode
    # colidir com os parâmetros desta função.
    partes = [f"SONDA|{evento}|t={t}"]
    partes.extend(f"{k}={v}" for k, v in campos.items())
    _log.info("|".join(partes))


_erros: Dict[str, int] = {}
_MAX_ERROS_POR_PONTO = 5


def _erro_interno(onde: str, exc: BaseException) -> None:
    """A sonda nunca propaga exceção, mas também nunca falha CALADA:
    uma sonda muda em produção seria lida como "zero updates". Emite só
    o NOME DA CLASSE da exceção (a mensagem pode carregar dados), no
    máximo _MAX_ERROS_POR_PONTO vezes por ponto."""
    try:
        n = _erros.get(onde, 0) + 1
        _erros[onde] = n
        if n <= _MAX_ERROS_POR_PONTO:
            _emitir("ERRO", _agora_ms(), onde=onde,
                    exc=type(exc).__name__, n=n)
    except Exception:
        pass


def _epoch_ms(data) -> int:
    """datetime tz-aware do Telethon → epoch ms. -1 quando ausente."""
    try:
        return int(data.timestamp() * 1000) if data is not None else -1
    except Exception:
        return -1


# ── 1. Handler Raw: o update chegou ao despacho ───────────────────
def _registrar_update(update) -> None:
    """Núcleo síncrono do handler Raw. Nunca lança."""
    try:
        from telethon import utils
        from telethon.tl.types import (UpdateEditChannelMessage,
                                       UpdateNewChannelMessage)
        if isinstance(update, UpdateNewChannelMessage):
            tipo = "new"
        elif isinstance(update, UpdateEditChannelMessage):
            tipo = "edit"
        else:
            return
        msg = getattr(update, "message", None)
        peer = getattr(msg, "peer_id", None)
        if peer is None:
            return
        chat = utils.get_peer_id(peer)
        if chat not in _estado["chats"]:
            return

        t = _agora_ms()
        if t - int(_estado["ult_upd_ms"]) > _GAP_LOTE_MS:
            _estado["lote"] = int(_estado["lote"]) + 1
        _estado["ult_upd_ms"] = t

        date_ms = _epoch_ms(getattr(msg, "date", None))
        edit_ms = _epoch_ms(getattr(msg, "edit_date", None))
        # Idade medida contra o fato que gerou o update: a criação da
        # mensagem (new) ou a última edição (edit).
        ref = edit_ms if (tipo == "edit" and edit_ms > 0) else date_ms
        ult_req = int(_estado["ult_req_ms"])
        ult_ping = int(_estado["ult_ping_ms"])
        _emitir(
            "UPD", t,
            lote=_estado["lote"], chat=chat, id=getattr(msg, "id", -1),
            tipo=tipo, date_ms=date_ms, edit_ms=edit_ms,
            idade_ms=(t - ref) if ref > 0 else -1,
            desde_req_ms=(t - ult_req) if ult_req else -1,
            req_ant=_estado["ult_req_cls"] or "-",
            desde_ping_ms=(t - ult_ping) if ult_ping else -1,
        )
    except Exception as exc:
        # A sonda jamais interfere no processamento real — mas avisa.
        _erro_interno("upd", exc)


async def _handler(update) -> None:
    _registrar_update(update)


# ── 2. Grampo nos loggers do Telethon ─────────────────────────────
class _Grampo(logging.Handler):
    """Converte registros SELECIONADOS do Telethon em linhas SONDA.

    Nunca usa record.getMessage() para emitir: só campos extraídos de
    posições conhecidas de record.args, e só de modelos na lista branca.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._traduzir(record)
        except Exception as exc:
            _erro_interno("grampo", exc)
        # Preserva o comportamento anterior: sem handler, os WARNING+
        # do Telethon saíam pelo lastResort. Continua exatamente igual.
        try:
            ultimo = logging.lastResort
            if ultimo is not None and record.levelno >= ultimo.level:
                ultimo.handle(record)
        except Exception:
            pass

    @staticmethod
    def _traduzir(record: logging.LogRecord) -> None:
        modelo = record.msg
        args = record.args if isinstance(record.args, tuple) else ()
        t = _agora_ms()

        if modelo == _MODELO_REQ and len(args) >= 2:
            cls = str(args[1])
            _estado["ult_req_ms"] = t
            _estado["ult_req_cls"] = cls
            if cls in _PINGS:
                _estado["ult_ping_ms"] = t
                _emitir("PING", t, req=cls)
            else:
                _emitir("REQ", t, req=cls)
            return
        if modelo == _MODELO_NET and args:
            _emitir("NET", t, classe=str(args[0]))
            return
        if modelo == _MODELO_PONG:
            ult_ping = int(_estado["ult_ping_ms"])
            _emitir("PONG", t, rtt_ms=(t - ult_ping) if ult_ping else -1)
            return
        if modelo == _MODELO_FLOOD and len(args) >= 4:
            _emitir("FLOOD", t, seg=int(args[1]), req=str(args[3]),
                    antecipado=1 if args[0] else 0)
            return
        evento = _CONEXAO.get(modelo)
        if evento:
            _emitir("CONN", t, evento=evento)
            return
        evento = _DIFF.get(modelo)
        if evento:
            _emitir("DIFF", t, evento=evento)


# ── Instalação / desinstalação ────────────────────────────────────
def _preparar_log_saida() -> None:
    if not _log.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(message)s"))
        _log.addHandler(h)
    _log.setLevel(logging.INFO)
    _log.propagate = False


def instalar(client, fontes) -> None:
    """Liga a sonda. Idempotente. `fontes`: entidades já resolvidas dos
    canais monitorados (as mesmas passadas aos handlers da casa)."""
    global _instalada, _handler_raw, _grampo
    if _instalada:
        return
    from telethon import events, utils
    from telethon import __version__ as versao_telethon
    from telethon.tl.types import (UpdateEditChannelMessage,
                                   UpdateNewChannelMessage)

    _preparar_log_saida()
    chats: FrozenSet[int] = frozenset(utils.get_peer_id(e) for e in fontes)
    _estado["chats"] = chats

    _handler_raw = _handler
    client.add_event_handler(
        _handler_raw,
        events.Raw(types=[UpdateNewChannelMessage, UpdateEditChannelMessage]),
    )

    _grampo = _Grampo()
    logging.getLogger("telethon").addHandler(_grampo)
    for nome, nivel in _LOGGERS_TELETHON.items():
        lg = logging.getLogger(nome)
        _niveis_antigos[nome] = lg.level
        lg.setLevel(nivel)

    _instalada = True
    _emitir("INICIO", _agora_ms(), canais=len(chats),
            telethon=versao_telethon, gap_lote_ms=_GAP_LOTE_MS)


def desinstalar(client) -> None:
    """Desliga a sonda e restaura exatamente o estado anterior."""
    global _instalada, _handler_raw, _grampo
    if not _instalada:
        return
    try:
        client.remove_event_handler(_handler_raw)
    except Exception:
        pass
    if _grampo is not None:
        logging.getLogger("telethon").removeHandler(_grampo)
    for nome, nivel in _niveis_antigos.items():
        logging.getLogger(nome).setLevel(nivel)
    _niveis_antigos.clear()
    _handler_raw = None
    _grampo = None
    _estado.update(chats=frozenset(), ult_req_ms=0, ult_req_cls="",
                   ult_ping_ms=0, ult_upd_ms=0, lote=0)
    _erros.clear()
    _instalada = False
