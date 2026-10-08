"""
Entrada — Completude (antes da orquestração).

Responsabilidade ÚNICA: nenhuma mensagem das fontes monitoradas pode
deixar de entrar no pipeline por ter se perdido no transporte do
Telegram.

═══════════════════════════════════════════════════════════════════
POR QUE EXISTE — medido em produção, 23/09/2026
═══════════════════════════════════════════════════════════════════
O Telegram nem sempre entrega o update de uma mensagem de canal. No
@samuelf3lipepromo, das 13:34 às 20:23: 59 mensagens; 4 chegaram SÓ
pela edição (a mensagem nova nunca veio) e a 117876 não chegou de
forma nenhuma — nem nova, nem edição. Era a lista dos cupons
DECORACAO2309 / SUPERPETSHOP / CUIDADO20OFF. Sem ela, o post ficou no
/sec/ do Promotom: a regra "a lista prevalece" nunca teve a lista.

═══════════════════════════════════════════════════════════════════
COMO
═══════════════════════════════════════════════════════════════════
Em canal, o id de mensagem é sequencial. Todo update recebido — nova
OU edição — marca o seu id como visto. Um id acima do maior visto + 1
revela um BURACO: os ids do meio não chegaram.

Depois de uma espera curta (o update atrasado costuma chegar sozinho),
os ids que continuam faltando são buscados UMA vez, numa única
requisição por id, e entregues ao MESMO ponto de entrada dos handlers,
como mensagem nova. Dali em diante é o pipeline de sempre: travas de
idade, idempotência, família, decisão, publicação.

═══════════════════════════════════════════════════════════════════
O QUE NÃO FAZ
═══════════════════════════════════════════════════════════════════
  - não decide nada de oferta: não normaliza, não deduplica, não
    publica. A mensagem recuperada é tratada exatamente como o update
    que se perdeu — inclusive pela trava de idade da mensagem nova;
  - não busca histórico: o primeiro id visto de cada canal só inicia a
    contagem;
  - não recupera buraco grande (> _MAX_BURACO): isso é reconexão ou
    atraso longo, e publicar em massa seria pior do que perder;
  - não repete busca: falhou, registra e segue;
  - não faz requisição nenhuma enquanto não há buraco.

IDADE: a recuperada passa pela MESMA trava da mensagem nova
(_MAX_IDADE_NOVA_S, 120 s, em `processar`). O buraco só é percebido
quando chega a mensagem seguinte do canal; se isso demorar, a
recuperada chega velha e é descartada como NOVA_ANTIGA — registrado,
como qualquer nova. Afrouxar essa trava para recuperadas é decisão de
negócio, não desta camada.

REPETIÇÃO É SEGURA: se o update original chegar DEPOIS da recuperação,
a idempotência do pipeline (chat:msg_id) o descarta como
JA_PROCESSADO. Um id buscado é marcado como visto e nunca é buscado de
novo.

PROVENIÊNCIA [F1.2-A2-R] — só observação, nunca regra. Cada fato daqui
sai também como evento, logo depois do log de sempre e FORA de qualquer
execução: a completude não abre execução, e a recuperada ganha a dela
no `processar` (via RECUPERACAO). origem.buraco é o buraco detectado;
origem.recuperada, a busca que devolveu a mensagem (antes da entrega);
origem.recuperacao_falhou, o resto (ERRO_BUSCA, AUSENTE, SERVICO,
ENTREGA_FALHOU, ABORTADA), com a exceção só pela classe, em `excecao`.
A task de recuperação nasce no `observar`, que o handler chama antes do
`processar`: o contexto que ela copia não tem execução. Desligada, a
proveniência não roda nada daqui.

Dependências INJETADAS em `instalar` (cliente, fontes, ponto de
entrada): este módulo não importa o orquestrador nem o cliente.
"""
from __future__ import annotations

import asyncio
from typing import Callable, Dict, Optional, Set

import eventos
import globals as g
from logger import log_ing, _idade_seg, _idade_str
from pipeline.identidade import username_de

__all__ = ["instalar", "observar", "EventoRecuperado"]


# ── Parâmetros ────────────────────────────────────────────────────
# Espera antes de buscar: dá tempo ao update atrasado de chegar
# sozinho. O Telethon segura um buraco de pts por 0,5 s; 3 s cobre
# reordenação de rede com folga e ainda fica muito abaixo da trava de
# idade da mensagem nova (120 s).
_ESPERA_S = 3.0

# Maior buraco que vale recuperar. Acima disso não é perda pontual.
_MAX_BURACO = 20

# Quantos ids para trás cada canal lembra. Limita a memória; ids mais
# velhos que isso nunca abrem buraco (só o maior visto avança). A poda
# só roda ao passar do dobro, para não custar uma varredura por update.
_JANELA_IDS = 500


class EventoRecuperado:
    """A superfície de evento que o pipeline consome — `message`,
    `chat_id` e `get_chat()` — sobre uma mensagem buscada por id.

    Não é um evento do Telethon e não finge ser: só expõe o que
    `processar`, `ingerir` e a identidade leem do evento. A mensagem é
    a do próprio Telethon, devolvida pela busca.

    [F1.2-A2-E] `via` diz por onde a mensagem voltou ao ponto de entrada:
    RECUPERACAO (esta completude, o padrão) ou SUCESSAO (a sucessão da
    chefe). Só a observação lê (origem.recebida); o pipeline não."""
    __slots__ = ("message", "via")

    def __init__(self, message, via: str = "RECUPERACAO") -> None:
        self.message = message
        self.via = via

    @property
    def chat_id(self):
        return self.message.chat_id

    async def get_chat(self):
        return await self.message.get_chat()


class _Canal:
    __slots__ = ("maior", "vistos", "pendentes", "tarefa")

    def __init__(self, primeiro: int) -> None:
        self.maior = primeiro
        self.vistos: Set[int] = {primeiro}
        self.pendentes: Set[int] = set()
        self.tarefa: Optional[asyncio.Task] = None


_estado: dict = {"cliente": None, "entidades": {}, "despachar": None}
_canais: Dict[int, _Canal] = {}


def instalar(cliente, fontes, despachar: Callable) -> None:
    """Liga a completude às fontes resolvidas. UMA vez por processo.

    `despachar(evento, is_edit=False)` é o ponto de entrada dos
    handlers (`pipeline.orchestrator.processar`)."""
    from telethon.utils import get_peer_id

    entidades = {}
    for ent in fontes:
        try:
            entidades[get_peer_id(ent)] = ent
        except Exception as e:
            log_ing.warning(f"🧩 completude: fonte sem id ({type(e).__name__})")
    _estado.update(cliente=cliente, entidades=entidades, despachar=despachar)
    _canais.clear()
    log_ing.info(
        f"🧩 completude da entrada ativa | canais={len(entidades)} "
        f"espera={_ESPERA_S:g}s max_buraco={_MAX_BURACO}")


def observar(chat_id, msg_id) -> None:
    """Marca o id como visto e abre buraco se preciso.

    Chamado pelos handlers ANTES do pipeline, para nova E edição.
    Síncrono, sem I/O, e nunca levanta: a completude jamais pode
    impedir a entrada de uma mensagem que chegou."""
    try:
        _observar(int(chat_id), int(msg_id))
    except Exception as e:
        log_ing.warning(f"🧩 completude: observar falhou ({type(e).__name__})")


def _nome(chat_id: int) -> str:
    u = username_de(str(chat_id))
    return f"@{u}" if u else str(chat_id)


def _observar(chat_id: int, msg_id: int) -> None:
    if _estado["despachar"] is None or chat_id not in _estado["entidades"]:
        return

    canal = _canais.get(chat_id)
    if canal is None:
        _canais[chat_id] = _Canal(msg_id)
        return

    canal.vistos.add(msg_id)
    canal.pendentes.discard(msg_id)
    if msg_id <= canal.maior:
        return

    anterior, canal.maior = canal.maior, msg_id
    buraco = msg_id - anterior - 1
    if buraco > _MAX_BURACO:
        log_ing.warning(
            f"🧩 [BURACO_GRANDE] {_nome(chat_id)} {buraco} ids entre "
            f"{anterior} e {msg_id} — não recupera (reconexão ou atraso longo)")
        # [F1.2-A2-R] Os valores vão como padrão do lambda, nunca como
        # closure: _observar roda a CADA mensagem, e uma closure faria dos
        # locais dela células — custo em toda chamada, com ou sem buraco.
        eventos.emitir_de("origem.buraco", lambda chat=chat_id, de=anterior, ate=msg_id,
                          tamanho=buraco: (
            {"chat": str(chat)},
            {"de": de, "ate": ate, "tamanho": tamanho, "faltam": None, "recupera": False}),
            local="completude.observar.buraco_grande")
    elif buraco > 0:
        faltam = [i for i in range(anterior + 1, msg_id) if i not in canal.vistos]
        if faltam:
            canal.pendentes.update(faltam)
            log_ing.info(
                f"🧩 [BURACO] {_nome(chat_id)} ids={faltam} entre {anterior} "
                f"e {msg_id} — confere em {_ESPERA_S:g}s")
            eventos.emitir_de("origem.buraco", lambda chat=chat_id, de=anterior, ate=msg_id,
                              tamanho=buraco, faltam=faltam: (
                {"chat": str(chat)},
                {"de": de, "ate": ate, "tamanho": tamanho, "faltam": faltam,
                 "recupera": True}),
                local="completude.observar.buraco")
            _agendar(chat_id, canal)

    _podar(canal)


def _podar(canal: _Canal) -> None:
    piso = canal.maior - _JANELA_IDS
    if len(canal.vistos) > 2 * _JANELA_IDS:
        canal.vistos = {i for i in canal.vistos if i > piso}
    if canal.pendentes and min(canal.pendentes) <= piso:
        canal.pendentes = {i for i in canal.pendentes if i > piso}


def _agendar(chat_id: int, canal: _Canal) -> None:
    # Uma tarefa por canal. A que já está em curso relê `pendentes`
    # ao acordar e repete enquanto houver — nada fica esquecido.
    if canal.tarefa is not None and not canal.tarefa.done():
        return
    canal.tarefa = asyncio.get_running_loop().create_task(_recuperar(chat_id))


async def _recuperar(chat_id: int) -> None:
    """[F1.2-A2-R] A rodada abortada sai como origem.recuperacao_falhou
    ABORTADA com os ids dela (os já tratados têm o seu evento): `faltam`
    nasce antes do try, porque o coletor o lê."""
    canal = _canais.get(chat_id)
    faltam: list = []
    try:
        while canal is not None and canal.pendentes:
            await asyncio.sleep(_ESPERA_S)
            if g._encerrando:
                return
            faltam = sorted(i for i in canal.pendentes if i not in canal.vistos)
            canal.pendentes.clear()
            if not faltam:
                log_ing.debug(f"🧩 [BURACO_FECHADO] {_nome(chat_id)} — chegou sozinho")
                continue
            await _buscar_e_entregar(chat_id, canal, faltam)
    except Exception as e:
        log_ing.error(f"🧩 completude: recuperação abortada ({type(e).__name__})",
                      exc_info=True)
        eventos.emitir_de("origem.recuperacao_falhou", lambda: (
            {"chat": str(chat_id)},
            {"motivo": "ABORTADA", "ids": faltam, "excecao": type(e).__name__}),
            local="completude.recuperar.abortada")


async def _buscar_e_entregar(chat_id: int, canal: _Canal, faltam: list) -> None:
    """[F1.2-A2-R] Cada desfecho sai como evento, logo depois do log, fora
    de execução: a busca que falhou, o id ausente ou de serviço, a
    recuperada (ANTES do processar, que abre a execução dela) e a entrega
    que falhou (DEPOIS do execucao.fim dela)."""
    nome = _nome(chat_id)
    # Marcados ANTES da busca: sucesso ou falha, nenhum id é buscado
    # duas vezes.
    canal.vistos.update(faltam)
    try:
        msgs = await _estado["cliente"].get_messages(
            _estado["entidades"][chat_id], ids=faltam)
    except Exception as e:
        log_ing.warning(
            f"🧩 [RECUPERACAO_FALHOU] {nome} ids={faltam} "
            f"erro={type(e).__name__} — sem nova tentativa")
        eventos.emitir_de("origem.recuperacao_falhou", lambda: (
            {"chat": str(chat_id)},
            {"motivo": "ERRO_BUSCA", "ids": faltam, "excecao": type(e).__name__}),
            local="completude.buscar_e_entregar.erro_busca")
        return

    # Casamento por id, nunca por posição: o servidor pode omitir ids
    # inválidos da resposta.
    achadas = {m.id: m for m in (msgs or []) if m is not None}
    for mid in faltam:
        m = achadas.get(mid)
        if m is None:
            log_ing.info(f"🧩 [AUSENTE_NO_CANAL] {nome} id={mid} — apagada ou inexistente")
            eventos.emitir_de("origem.recuperacao_falhou", lambda: (
                {"chat": str(chat_id), "msg": mid},
                {"motivo": "AUSENTE", "ids": [mid]}),
                local="completude.buscar_e_entregar.ausente")
            continue
        if getattr(m, "action", None) is not None:
            log_ing.debug(f"🧩 [SERVICO] {nome} id={mid} — mensagem de serviço, ignorada")
            eventos.emitir_de("origem.recuperacao_falhou", lambda: (
                {"chat": str(chat_id), "msg": mid},
                {"motivo": "SERVICO", "ids": [mid]}),
                local="completude.buscar_e_entregar.servico")
            continue
        log_ing.warning(
            f"🧩 [RECUPERADA] {nome} id={mid} idade={_idade_str(m.date)} — "
            f"o Telegram não entregou; entra no pipeline como nova")
        eventos.emitir_de("origem.recuperada", lambda: (
            {"chat": str(chat_id), "msg": mid},
            {"idade_s": round(_idade_seg(m.date), 1)}),
            local="completude.buscar_e_entregar.recuperada",
            minimo=lambda: {"chat": str(chat_id), "msg": mid})
        try:
            await _estado["despachar"](EventoRecuperado(m), is_edit=False)
        except Exception as e:
            log_ing.error(
                f"🧩 entrega da recuperada falhou: {type(e).__name__}", exc_info=True)
            eventos.emitir_de("origem.recuperacao_falhou", lambda: (
                {"chat": str(chat_id), "msg": mid},
                {"motivo": "ENTREGA_FALHOU", "ids": [mid], "excecao": type(e).__name__}),
                local="completude.buscar_e_entregar.entrega_falhou")
