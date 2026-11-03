"""
CURADORIA — hardware só vai ao canal com o AVAL de um canal curador.

POR QUE EXISTE (Léo, 03/11/2026): Promotom, Fumotom e Samuel Felipe
despejam hardware de PC no canal — placa de vídeo, fonte, cooler, monitor
gamer… — quase sempre em vitrine, sem preço especial (~53 posts/dia, 20% do
@fullpromotion; medido nas prévias públicas de 27/10 a 03/11). O Fada
Promoções e o Nerd Ofertas só soltam esse tipo de produto quando o preço
vale. A regra: hardware de categoria vigiada só passa quando o MESMO
produto sai também no Fada Promoções ou no Nerd Ofertas.

═══════════════════════════════════════════════════════════════════
A REGRA
═══════════════════════════════════════════════════════════════════
  · categoria vigiada: pipeline.curadoria_categoria (notebook, PC gamer e
    monitor comum ficam de fora);
  · só vale para post de PRODUTO (natureza "produto"): cupom e campanha
    seguem o fluxo de sempre;
  · CURADOR (config.CURADORES): o post dele passa normal e é o aval. O Fada
    Promoções é fonte comum (publica); o Nerd Ofertas só é lido
    (config.GRUPOS_AVAL) — nada dele vai ao canal;
  · "mesmo produto" = mesma identidade de produto que a família já usa
    (ASIN, MLB, item da Shopee…), derivada pelo mesmo caminho dos dois lados
    (normalizacao.identificar_produtos). Título parecido NÃO é aval;
  · janela (config.CURADORIA_JANELA_S, 5 min) contada pelas DATAS das
    mensagens no Telegram, nos dois sentidos:
      aval ANTES  → a mensagem da fonte passa direto;
      aval DEPOIS → a mensagem da fonte fica SEGURADA e o aval a SOLTA;
      fora da janela → a segurada expira e nunca vai ao canal (o post do
      Fada, se vier depois, sai normal pelo caminho dele).

═══════════════════════════════════════════════════════════════════
COMO SOLTA — reinjeção no ponto de entrada (CLAUDE.md §3)
═══════════════════════════════════════════════════════════════════
A mensagem segurada não é guardada nem publicada por aqui: o aval marca a
linha dela como `liberando` (database_curadoria, na mesma transação que
grava o aval) e uma task própria a BUSCA de novo na fonte (texto atual; se a
fonte apagou, nada sai) e a entrega a `processar` como EventoRecuperado com
via CURADORIA — o pipeline de sempre: travas, identidade, família, decisão,
publicação. Nenhum segundo caminho de escrita no canal.

A reinjeção é mensagem NOVA de até ~7 min: a trava de frescor (120 s) e a
idempotência em memória a barrariam. `reinjecao()` é a única exceção, e
exige as três provas juntas: via CURADORIA, estado `liberando` marcado há
menos de _PRAZO_LIBERACAO_S no banco, e mensagem dentro da janela.

═══════════════════════════════════════════════════════════════════
ROBUSTEZ
═══════════════════════════════════════════════════════════════════
  · sem corrida: "há aval?" e "há segurada?" rodam cada uma numa transação
    (database_curadoria);
  · edição de mensagem segurada continua segurada (a janela não recomeça);
    edição de mensagem já publicada (origem vinculada) passa;
  · reinício: avais e seguradas estão no banco; a liberação interrompida é
    retomada no boot; a varredura relê o canal de aval desde a janela;
  · o canal de aval é LIDO a cada _VARREDURA_S (canal público: não precisa
    entrar nele) e também chega pelo handler, se a conta segue o canal;
  · falha de banco no portão: deixa passar e registra (a mesma escolha da
    deduplicação: nunca travar o canal por defeito interno);
  · modo SOMBRA: só registra; DESLIGADO: nada roda.

Dependências INJETADAS em `instalar` (cliente, fontes, canais de aval,
ponto de entrada): este módulo não importa a orquestração nem o cliente.
"""
from __future__ import annotations

import asyncio
import contextvars
import time
from typing import Callable, Dict, Iterable, List, Optional

import config
import eventos
import globals as g
from database import (db_curadoria_avaliar, db_curadoria_desfecho,
                      db_curadoria_em_liberacao, db_curadoria_expirar,
                      db_curadoria_registrar_aval, db_curadoria_retomar)
from database_curadoria import (AVAL, AUSENTE, ERRO, FALHOU, JA_LIBERADA,
                                LIBERACAO, LIBERADA)
from logger import _idade_seg, log_ded
from pipeline import ingestao, origem
from pipeline.completude import EventoRecuperado
from pipeline.curadoria_categoria import categoria
from pipeline.identidade import username_de

__all__ = ["instalar", "portao", "reinjecao", "encerrar", "VIA"]

ATIVO, SOMBRA, DESLIGADO = "ATIVO", "SOMBRA", "DESLIGADO"
VIA = "CURADORIA"

# ── Parâmetros ────────────────────────────────────────────────────
# Quanto o aval pode ser VISTO depois da data dele (varredura, fila, reinício
# curto) e ainda soltar a segurada. A segurada mais velha que se solta tem
# JANELA + ATRASO (7 min).
_ATRASO_MAX_S = 120.0
# Validade da prova `liberando` para a admissão: a reinjeção acontece em
# segundos depois de marcada.
_PRAZO_LIBERACAO_S = 120.0
# Leitura do canal de aval: a cada 15 s, até 20 mensagens por página e 5
# páginas por rodada (rajada de 100 posts em 15 s).
_VARREDURA_S = 15.0
_LOTE = 20
_PAGINAS = 5
_VISTOS_MAX = 2000
_DRENO_S = 5.0


def _modo() -> str:
    return config.CURADORIA_MODO if config.CURADORIA_MODO in (ATIVO, SOMBRA) else DESLIGADO


def _janela() -> float:
    return float(config.CURADORIA_JANELA_S)


_estado: dict = {"cliente": None, "despachar": None, "fontes": {}, "avais": {},
                 "curadores": frozenset(), "varredura": None}
_TAREFAS: set = set()
_aberto = False
_vistos: Dict[tuple, None] = {}
_maior: Dict[int, int] = {}
_falha_varredura: Dict[int, bool] = {}
_pausa_ate: Dict[int, float] = {}


# ─────────────────────────────────────────────────────────────────
# Instalação e encerramento — UMA vez por processo
# ─────────────────────────────────────────────────────────────────
def instalar(cliente, fontes: Iterable, avais: Iterable,
             despachar: Callable) -> Optional[asyncio.Task]:
    """Liga a curadoria (main._preparar_processo, depois dos handlers das
    fontes). `fontes` são as entidades de GRUPOS_ORIGEM, `avais` as de
    GRUPOS_AVAL, `despachar(evento, is_edit=)` o ponto de entrada. Devolve a
    task da varredura, ou None com o modo DESLIGADO."""
    global _aberto
    from telethon import events
    from telethon.utils import get_peer_id

    if _modo() == DESLIGADO:
        log_ded.info("🎯 curadoria DESLIGADA (config.CURADORIA_MODO)")
        return None
    fontes, avais = list(fontes), list(avais)
    _estado["cliente"], _estado["despachar"] = cliente, despachar
    _estado["fontes"] = {get_peer_id(e): e for e in fontes}
    _estado["avais"] = {get_peer_id(e): e for e in avais}
    curadores, nomes = set(), []
    for e in fontes + avais:
        nome = (getattr(e, "username", None) or "").lower()
        if nome in config.CURADORES:
            curadores.add(str(get_peer_id(e)))
            nomes.append(f"@{nome}")
    _estado["curadores"] = frozenset(curadores)
    _vistos.clear()
    _maior.clear()
    _pausa_ate.clear()
    _aberto = True
    if avais:
        cliente.add_event_handler(_ao_aval, events.NewMessage(chats=avais))
    faltam = sorted(set(config.CURADORES) - {n[1:] for n in nomes})
    lidos = [_nome(get_peer_id(e)) for e in avais]
    log_ded.info(
        f"🎯 curadoria {_modo()} | janela={_janela():g}s curadores={sorted(set(nomes))} "
        f"aval_lido={lidos} varredura={_VARREDURA_S:g}s")
    if faltam:
        log_ded.warning(f"⚠️ curadoria: curador sem canal resolvido {faltam} — "
                        f"o aval dele não chega")
    _retomar()
    t = asyncio.get_running_loop().create_task(_varrer(), context=contextvars.Context())
    _estado["varredura"] = t
    return t


async def encerrar() -> None:
    """Shutdown (main._encerrar, depois do drain e antes do HTTP, do
    Telegram e do banco): fecha a admissão, para a varredura, dá até
    _DRENO_S às liberações em curso e cancela o resto, aguardando. A
    liberação que não terminou fica `liberando` e é retomada no boot."""
    global _aberto
    _aberto = False
    v = _estado.get("varredura")
    if v is not None and not v.done():
        v.cancel()
        await asyncio.gather(v, return_exceptions=True)
    loop = asyncio.get_running_loop()
    prazo = loop.time() + _DRENO_S
    pendentes: list = []
    while True:
        pendentes = [t for t in _TAREFAS if not t.done()]
        restante = prazo - loop.time()
        if not pendentes or restante <= 0:
            break
        await asyncio.wait(pendentes, timeout=restante)
    for t in pendentes:
        t.cancel()
    if pendentes:
        await asyncio.gather(*pendentes, return_exceptions=True)


def _agendar(coro) -> None:
    """Task própria, contexto VAZIO: vive mais que quem a pediu (a execução
    da mensagem do Fada, o handler do Nerd) e não leva a execução dele."""
    if not _aberto or g._encerrando:
        coro.close()
        return
    t = asyncio.get_running_loop().create_task(coro, context=contextvars.Context())
    _TAREFAS.add(t)
    t.add_done_callback(_TAREFAS.discard)


# ─────────────────────────────────────────────────────────────────
# Utilidades
# ─────────────────────────────────────────────────────────────────
def _nome(chat) -> str:
    u = username_de(str(chat))
    return f"@{u}" if u else str(chat)


def _chaves(idents) -> List[str]:
    """Chave de produto no formato da identidade canônica: plataforma|id."""
    return sorted({f"{p}|{i}" for p, i, *_ in (idents or ()) if p and i})


def _ts(data) -> float:
    try:
        return float(data.timestamp())
    except Exception:
        return time.time()


# ─────────────────────────────────────────────────────────────────
# PORTÃO — no pipeline, depois do enriquecimento e antes da deduplicação
# ─────────────────────────────────────────────────────────────────
def portao(norm, enr, is_edit: bool, data_msg=None) -> bool:
    """True: a mensagem segue o pipeline. False: ficou SEGURADA (o funil
    para aqui; o log e o evento saem daqui). Nunca levanta."""
    try:
        modo = _modo()
        if modo == DESLIGADO or getattr(enr, "tipo", None) != "produto":
            return True
        cat = categoria(getattr(norm, "texto_analise", "") or "")
        if not cat:
            return True
        chaves = _chaves(getattr(norm, "idents", None))
        if not chaves:
            return True
        chat, msg_id = str(norm.chat), int(norm.msg_id)
        ts_msg = _ts(data_msg)
        if chat in _estado["curadores"]:
            _registrar_aval(chat, msg_id, chaves, cat, ts_msg)
            return True
        if origem.consultar(chat, msg_id):
            return True             # já publicada/vinculada: edição e afins seguem
        resposta, nova = db_curadoria_avaliar(chat, msg_id, chaves, cat, ts_msg,
                                              _janela(), time.time())
        if resposta in (AVAL, LIBERACAO, JA_LIBERADA):
            if resposta != JA_LIBERADA:
                log_ded.info(f"🎯 [CURADORIA_{'LIBERADA' if resposta == LIBERACAO else 'COM_AVAL'}] "
                             f"{_nome(chat)} id={msg_id} cat={cat} — segue")
            return True
        if resposta == ERRO:
            log_ded.error(f"❌ curadoria: banco indisponível — {_nome(chat)} id={msg_id} "
                          f"cat={cat} passa sem conferir aval")
            return True
        # SEGURADA
        if modo == SOMBRA:
            if nova:
                log_ded.info(f"👻 [CURADORIA_SOMBRA] {_nome(chat)} id={msg_id} cat={cat} "
                             f"produtos={chaves} — seguraria (sem aval); segue normal")
            return True
        if nova:
            log_ded.info(f"🎯 [CURADORIA_RETIDA] {_nome(chat)} id={msg_id} cat={cat} "
                         f"produtos={chaves} — espera aval (Fada/Nerd) até "
                         f"{_janela():g}s")
        log_ded.info(f"🧭 TL | id={msg_id} chat={chat} | DESCARTE | "
                     f"motivo=CURADORIA_RETIDA cat={cat} edit={is_edit}")
        eventos.emitir_de("curadoria.retida", lambda: (
            {"chat": chat, "msg": msg_id},
            {"categoria": cat, "produtos_n": len(chaves), "nova": nova,
             "is_edit": bool(is_edit)}),
            local="curadoria.portao.retida")
        return False
    except Exception as e:                              # noqa: BLE001
        log_ded.error(f"❌ curadoria.portao: {type(e).__name__}: {e} — segue sem conferir",
                      exc_info=True)
        return True


def reinjecao(event) -> bool:
    """A mensagem que chega ao ponto de entrada é a REINJEÇÃO de uma
    segurada que o aval soltou? Só com as três provas: via CURADORIA, estado
    `liberando` no banco marcado há menos de _PRAZO_LIBERACAO_S e mensagem
    dentro da janela. Síncrona, barata quando a via é outra; nunca levanta.
    É o que deixa a reinjeção passar pela trava de frescor e pela
    idempotência em memória — e nada mais passa por ela."""
    try:
        if getattr(event, "via", None) != VIA:
            return False
        m = event.message
        idade = _idade_seg(m.date)
        if idade < 0 or idade > _janela() + _ATRASO_MAX_S + _PRAZO_LIBERACAO_S:
            return False
        return db_curadoria_em_liberacao(str(event.chat_id), int(m.id),
                                         time.time() - _PRAZO_LIBERACAO_S)
    except Exception:                                   # noqa: BLE001
        return False


# ─────────────────────────────────────────────────────────────────
# AVAL — post de curador (Fada pelo portão; Nerd pelo canal de aval)
# ─────────────────────────────────────────────────────────────────
def _registrar_aval(chat: str, msg_id: int, chaves: List[str], cat: str,
                    ts_aval: float) -> None:
    agora = time.time()
    soltas = db_curadoria_registrar_aval(
        chaves, chat, msg_id, ts_aval, _janela(),
        agora - _janela() - _ATRASO_MAX_S, agora)
    if soltas is None:
        log_ded.error(f"❌ curadoria: aval de {_nome(chat)} id={msg_id} não gravado")
        return
    log_ded.info(f"✅ [CURADORIA_AVAL] {_nome(chat)} id={msg_id} cat={cat} "
                 f"produtos={chaves} solta={[f'{_nome(c)}/{m}' for c, m, _ in soltas]}")
    eventos.emitir_de("curadoria.aval", lambda: (
        {"chat": chat, "msg": msg_id},
        {"categoria": cat, "produtos_n": len(chaves), "solta_n": len(soltas)}),
        local="curadoria.aval.registrado")
    for c, m, cat_r in soltas:
        if _modo() == SOMBRA:
            log_ded.info(f"👻 [CURADORIA_SOMBRA] {_nome(c)} id={m} cat={cat_r} — o aval "
                         f"de {_nome(chat)} soltaria")
            db_curadoria_desfecho(c, m, LIBERADA, agora)
            continue
        _agendar(_liberar(c, m, cat_r))


async def _liberar(chat: str, msg_id: int, cat: str) -> None:
    """Busca a segurada na fonte e a entrega ao ponto de entrada como
    mensagem nova (via CURADORIA). O portão a reconhece pelo estado
    `liberando` e a deixa seguir."""
    agora = time.time()
    try:
        ent = _estado["fontes"].get(int(chat))
    except (TypeError, ValueError):
        ent = None
    if ent is None or _estado["cliente"] is None:
        log_ded.warning(f"⚠️ [CURADORIA_LIBERACAO_FALHOU] {_nome(chat)} id={msg_id} — "
                        f"fonte não monitorada")
        db_curadoria_desfecho(chat, msg_id, FALHOU, agora)
        return
    try:
        msgs = await _estado["cliente"].get_messages(ent, ids=[msg_id])
    except asyncio.CancelledError:
        raise
    except Exception as e:                              # noqa: BLE001
        log_ded.warning(f"⚠️ [CURADORIA_LIBERACAO_FALHOU] {_nome(chat)} id={msg_id} "
                        f"busca: {type(e).__name__}")
        db_curadoria_desfecho(chat, msg_id, FALHOU, time.time())
        return
    m = (msgs or [None])[0]
    if m is None or getattr(m, "action", None) is not None:
        log_ded.info(f"🗑 [CURADORIA_AUSENTE] {_nome(chat)} id={msg_id} — sumiu da fonte "
                     f"antes de soltar; nada vai ao canal")
        db_curadoria_desfecho(chat, msg_id, AUSENTE, time.time())
        return
    log_ded.info(f"🔓 [CURADORIA_SOLTA] {_nome(chat)} id={msg_id} cat={cat} — aval chegou; "
                 f"volta ao ponto de entrada")
    try:
        await _estado["despachar"](EventoRecuperado(m, via=VIA), is_edit=False)
    except asyncio.CancelledError:
        raise
    except Exception as e:                              # noqa: BLE001
        log_ded.error(f"❌ [CURADORIA_LIBERACAO_FALHOU] {_nome(chat)} id={msg_id} "
                      f"entrega: {type(e).__name__}", exc_info=True)
        db_curadoria_desfecho(chat, msg_id, FALHOU, time.time())


def _retomar() -> None:
    """Liberações interrompidas por um reinício: as ainda dentro da janela
    voltam a ser entregues (o banco renova a prova)."""
    agora = time.time()
    for chat, mid, cat in db_curadoria_retomar(
            agora - _janela() - _ATRASO_MAX_S - _PRAZO_LIBERACAO_S, agora):
        log_ded.info(f"🔁 [CURADORIA_RETOMADA] {_nome(chat)} id={mid} — liberação "
                     f"interrompida pelo reinício")
        _agendar(_liberar(chat, mid, cat))


# ─────────────────────────────────────────────────────────────────
# CANAL DE AVAL (Nerd Ofertas) — só leitura
# ─────────────────────────────────────────────────────────────────
async def _ao_aval(event) -> None:
    """Handler NewMessage dos canais de aval (só chega se a conta segue o
    canal). Só agenda e volta."""
    try:
        _agendar(_observar_aval(int(event.chat_id), event.message))
    except Exception as e:                              # noqa: BLE001
        log_ded.warning(f"⚠️ curadoria: aval do handler ({type(e).__name__})")


async def _observar_aval(chat_id: int, message) -> None:
    """Uma mensagem do canal de aval: categoria vigiada + produto
    identificado → aval. Cada mensagem é olhada uma vez (handler e
    varredura podem trazer a mesma)."""
    chave = (chat_id, int(getattr(message, "id", 0) or 0))
    if chave in _vistos:
        return
    _vistos[chave] = None
    if len(_vistos) > _VISTOS_MAX:
        for k in list(_vistos)[:_VISTOS_MAX // 2]:
            _vistos.pop(k, None)
    try:
        idade = _idade_seg(getattr(message, "date", None))
        if idade < 0 or idade > _janela() + _ATRASO_MAX_S:
            return                  # aval velho não solta nada
        texto = ingestao.texto_de(message)
        cat = categoria(texto)
        if not cat:
            return
        from pipeline.normalizacao import identificar_produtos
        idents = await identificar_produtos(ingestao.links_de(texto), chave[1])
        chaves = _chaves(idents)
        if not chaves:
            log_ded.info(f"🎯 [CURADORIA_AVAL_SEM_PRODUTO] {_nome(chat_id)} id={chave[1]} "
                         f"cat={cat} — loja sem identidade de produto no bot; não é aval")
            return
        _registrar_aval(str(chat_id), chave[1], chaves, cat, _ts(message.date))
    except asyncio.CancelledError:
        raise
    except Exception as e:                              # noqa: BLE001
        log_ded.warning(f"⚠️ curadoria: aval {_nome(chat_id)} id={chave[1]} "
                        f"não lido ({type(e).__name__})")


async def _varrer() -> None:
    """Lê os canais de aval a cada _VARREDURA_S (não depende de a conta
    seguir o canal nem de o Telegram entregar o update) e expira as
    seguradas que passaram da janela."""
    while True:
        await asyncio.sleep(_VARREDURA_S)
        if g._encerrando or not _aberto:
            return
        try:
            _expirar()
        except Exception as e:                          # noqa: BLE001
            log_ded.warning(f"⚠️ curadoria: expirar ({type(e).__name__})")
        for chat_id, ent in list(_estado["avais"].items()):
            if g._encerrando or not _aberto:
                return
            await _varrer_canal(chat_id, ent)


async def _varrer_canal(chat_id: int, ent) -> None:
    cliente = _estado["cliente"]
    maior = _maior.get(chat_id)
    if time.monotonic() < _pausa_ate.get(chat_id, 0.0):
        return                      # FloodWait pedido pelo Telegram: respeita
    try:
        lote: list = []
        teto = None
        for _ in range(_PAGINAS):
            kw = {"limit": _LOTE}
            if maior is not None:
                kw["min_id"] = maior
            if teto is not None:
                kw["max_id"] = teto
            pagina = [m for m in (await cliente.get_messages(ent, **kw) or [])
                      if m is not None]
            lote += pagina
            if maior is None or len(pagina) < _LOTE:
                break
            teto = min(m.id for m in pagina)
    except asyncio.CancelledError:
        raise
    except Exception as e:                              # noqa: BLE001
        espera = getattr(e, "seconds", None)
        if isinstance(espera, (int, float)) and espera > 0:
            # FloodWait acima do limite do Telethon: não pergunta de novo
            # antes do prazo (a sessão é do worker inteiro).
            _pausa_ate[chat_id] = time.monotonic() + float(espera)
        if not _falha_varredura.get(chat_id):
            log_ded.warning(f"⚠️ [CURADORIA_VARREDURA_FALHOU] {_nome(chat_id)} "
                            f"({type(e).__name__}"
                            f"{f', espera {espera}s' if espera else ''}) — segue tentando")
        _falha_varredura[chat_id] = True
        return
    if _falha_varredura.pop(chat_id, False):
        log_ded.info(f"🎯 curadoria: varredura de {_nome(chat_id)} voltou")
    if lote:
        _maior[chat_id] = max([maior or 0] + [m.id for m in lote])
    elif maior is None:
        _maior[chat_id] = 0
    for m in sorted(lote, key=lambda x: x.id):
        if getattr(m, "action", None) is None:
            await _observar_aval(chat_id, m)


def _expirar() -> None:
    agora = time.time()
    sombra = _modo() == SOMBRA
    for chat, mid, cat in db_curadoria_expirar(agora - _janela() - _ATRASO_MAX_S, agora):
        if sombra:
            log_ded.info(f"👻 [CURADORIA_SOMBRA] {_nome(chat)} id={mid} cat={cat} — "
                         f"{_janela():g}s sem aval: no ATIVO não iria ao canal")
        else:
            log_ded.info(f"⌛ [CURADORIA_EXPIROU] {_nome(chat)} id={mid} cat={cat} — "
                         f"{_janela():g}s sem aval do Fada/Nerd; não vai ao canal")
