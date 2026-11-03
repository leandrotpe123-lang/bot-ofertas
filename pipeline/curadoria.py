"""
CURADORIA — hardware só vai ao canal com o AVAL de um canal avalista.

POR QUE EXISTE (Léo, 03/11/2026): Promotom, Fumotom e Samuel Felipe
despejam hardware de PC no canal — placa de vídeo, fonte, cooler, monitor
gamer… — quase sempre em vitrine, sem preço especial (~53 posts/dia, 20% do
@fullpromotion; medido nas prévias públicas de 27/10 a 03/11). O Fada
Promoções e o Nerd Ofertas só soltam esse tipo de produto quando o preço
vale. A regra: hardware de categoria vigiada só passa quando o MESMO
produto, na MESMA loja, sai também no Fada Promoções ou no Nerd Ofertas.

═══════════════════════════════════════════════════════════════════
A REGRA
═══════════════════════════════════════════════════════════════════
  · categoria vigiada: pipeline.curadoria_categoria (notebook, PC gamer e
    monitor comum ficam de fora);
  · só vale para post de PRODUTO (natureza "produto"): cupom e campanha
    seguem o fluxo de sempre;
  · vale para TODA fonte monitorada, menos as ISENTAS — Fada Promoções, Fada
    dos Cupons e Oferta Convertida —, sempre pelo ID REAL do chat
    (config.GRUPO_ID / CURADORIA_ISENTOS); fonte nova entra retida;
  · AVALISTAS: o Fada Promoções (fonte comum: o post dele sai normal E é
    aval — config.CURADORIA_AVALISTAS_FONTE) e o Nerd Ofertas (canal só de
    sinal, config.GRUPOS_AVAL: o bot só LÊ; nada dele entra na fila nem vai
    ao canal). Fada dos Cupons e Oferta Convertida são isentas, mas não dão
    aval;
  · "mesmo produto" = a mesma identidade de produto que a família usa, só
    nas lojas em que ela é exata (config.CURADORIA_LOJAS_AVAL: ASIN da
    Amazon, MLB do Mercado Livre, loja+item da Shopee), com a loja na chave
    ("amazon|B0…"). Título parecido NÃO é aval; identidade incerta não é
    aval. Para o Nerd a identidade vem SEM afiliar (_identificar);
  · janela (config.CURADORIA_JANELA_S, 300 s) contada pelas DATAS ORIGINAIS
    das mensagens no Telegram, nos dois sentidos:
      aval ANTES (até 300 s)  → a mensagem da fonte passa direto;
      aval DEPOIS (até 300 s) → a mensagem da fonte fica SEGURADA e o aval a
                                SOLTA, na hora em que o aval é visto;
      fora da janela → a segurada nunca vai ao canal (o post do Fada, se
      vier depois, sai normal pelo caminho dele);
  · passam SEM aval (decisão do Léo, 03/11): LISTA de produtos distintos com
    preço próprio (variante de cor/tamanho/capacidade e vendedor não contam
    — curadoria_categoria.produtos_com_preco); frase de DESTAQUE no texto da
    PRÓPRIA mensagem ("Ainda ativo!", "Bom preço!" — pipeline.destaque);
    retorno de oferta ("Voltando!" — pipeline.reativacao). Quem passa por
    exceção NUNCA vira aval. Nada de comentário, resposta ou outra mensagem
    entra na conta: só a mensagem original.

═══════════════════════════════════════════════════════════════════
COMO SOLTA — reinjeção no ponto de entrada (CLAUDE.md §3)
═══════════════════════════════════════════════════════════════════
O aval marca a linha da segurada como `liberando` (database_curadoria, na
mesma transação que grava o aval) e uma task própria entrega a MESMA
mensagem a `processar` como EventoRecuperado com via CURADORIA — o pipeline
de sempre: travas, identidade, família, decisão, publicação. Nenhum segundo
caminho de escrita no canal. VELOCIDADE: a mensagem guardada na memória
quando foi segurada (a última versão, se a fonte editou) sai na hora, sem
nova consulta ao Telegram; só depois de um reinício ela é buscada na fonte.

A reinjeção é mensagem NOVA de até ~7 min: a trava de frescor (120 s) e a
idempotência em memória a barrariam. `reinjecao()` é a única exceção, e
exige as três provas juntas: via CURADORIA, estado `liberando` marcado há
menos de _PRAZO_LIBERACAO_S no banco, e mensagem dentro da janela.

═══════════════════════════════════════════════════════════════════
ROBUSTEZ
═══════════════════════════════════════════════════════════════════
  · sem corrida: "há aval?" e "há segurada?" rodam cada uma numa transação;
  · edição de mensagem segurada continua segurada (a janela não recomeça);
    edição de mensagem já publicada (origem vinculada) passa;
  · a fonte apagou a segurada: nada sai (origem_apagada + busca na fonte);
  · reinício: avais e seguradas estão no banco; a liberação interrompida é
    retomada no boot; a leitura do canal de aval recomeça da janela;
  · canal de aval: chega NA HORA pelo handler quando a conta segue o canal;
    a leitura a cada _VARREDURA_S é a rede de segurança (update perdido,
    conta que não segue) e respeita FloodWait; cada rodada LÊ o aval antes
    de expirar; o aval visto com até _ATRASO_MAX_S de atraso ainda solta;
  · identificação do aval que falhou (rede, encurtador que não abriu) é
    refeita (_REPETIR_S); a busca da segurada na fonte também (_BUSCA_
    REPETIR_S); falha persistente fecha sem inventar aval;
  · post de avalista com cabeçalho ("🚨 Produtos Tech…") vale pela primeira
    linha de produto com categoria (_categoria_aval);
  · relógio local atrás do Telegram (data "no futuro") não perde aval nem
    reinjeção;
  · a edição que traz uma exceção a uma segurada a solta UMA vez e fecha a
    linha (db_curadoria_passou): nenhum aval posterior a reinjeta;
  · falha de banco no portão: deixa passar e registra (a mesma escolha da
    deduplicação: nunca travar o canal por defeito interno);
  · modo SOMBRA: só registra; DESLIGADO: nada roda.

Dependências INJETADAS em `instalar` (cliente, fontes, canais de aval,
ponto de entrada): este módulo não importa a orquestração nem o cliente.
"""
from __future__ import annotations

import asyncio
import contextlib
import contextvars
import time
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import config
import eventos
import globals as g
from database import (db_curadoria_avaliar, db_curadoria_desfecho,
                      db_curadoria_em_liberacao, db_curadoria_expirar,
                      db_curadoria_passou, db_curadoria_registrar_aval,
                      db_curadoria_retomar)
from database_curadoria import (AVAL, AUSENTE, ERRO, FALHOU, JA_LIBERADA,
                                LIBERACAO, LIBERADA)
from logger import _idade_seg, log_ded
from pipeline import ingestao, origem, origem_apagada
from pipeline.completude import EventoRecuperado
from pipeline.curadoria_categoria import categoria, produtos_com_preco
from pipeline.destaque import frase_de_destaque
from pipeline.identidade import username_de
from pipeline.reativacao import eh_reativacao
from plataformas import registry
from plataformas.contrato import AUSENTE as SEM_ID, TipoLink
from utils.categorias_universais import classificar_universal, eh_encurtador_generico
from utils.url_resolver import desencurtar

__all__ = ["instalar", "portao", "reinjecao", "encerrar", "fontes_sem_aval", "VIA"]

ATIVO, SOMBRA, DESLIGADO = "ATIVO", "SOMBRA", "DESLIGADO"
VIA = "CURADORIA"

# ── Parâmetros ────────────────────────────────────────────────────
# Folga para VER o aval depois da data dele (fila, rede, varredura de 15 s,
# reinício curto) e ainda soltar a segurada. Não alarga a regra: o par
# continua exigindo |data do aval − data da fonte| <= 300 s; isto só evita
# que a demora em ENXERGAR um aval válido o perca. A segurada mais velha que
# se solta tem 300 + 120 s.
_ATRASO_MAX_S = 120.0
# Validade da prova `liberando` para a admissão: a reinjeção acontece em
# segundos depois de marcada.
_PRAZO_LIBERACAO_S = 120.0
# Leitura do canal de aval (rede de segurança do handler): a cada 15 s, até
# 20 mensagens por página e 5 páginas por rodada.
_VARREDURA_S = 15.0
_LOTE = 20
_PAGINAS = 5
_VISTOS_MAX = 2000
_DRENO_S = 5.0
# LISTA: a partir de quantos produtos DISTINTOS, cada um com preço próprio,
# o post é lista (passa sem aval).
_MIN_ITENS_LISTA = 2
# Links de uma mensagem do canal de aval que valem uma identificação (a
# lista de monitores do Nerd de 01/11, id 147989, tem 12).
_MAX_LINKS_AVAL = 16
# Identificação do aval que FALHOU (rede, encurtador que não abriu): novas
# tentativas depois destes atrasos. Cabe na folga _ATRASO_MAX_S.
_REPETIR_S = (5.0, 15.0)
# Busca da segurada na fonte (só depois de reinício): novas tentativas.
_BUSCA_REPETIR_S = (1.0, 3.0)


def _modo() -> str:
    return config.CURADORIA_MODO if config.CURADORIA_MODO in (ATIVO, SOMBRA) else DESLIGADO


def _janela() -> float:
    return float(config.CURADORIA_JANELA_S)


_estado: dict = {"cliente": None, "despachar": None, "fontes": {}, "avais": {},
                 "varredura": None}
_TAREFAS: set = set()
_aberto = False
_vistos: Dict[tuple, None] = {}
_maior: Dict[int, int] = {}
_falha_varredura: Dict[int, bool] = {}
_pausa_ate: Dict[int, float] = {}
_tentativas: Dict[tuple, int] = {}
# A mensagem segurada, como chegou (ou na última edição): (chat, msg) ->
# (Message, instante). É o que sai na hora quando o aval chega.
_SEGURADAS: Dict[tuple, Tuple[object, float]] = {}


def _papel(chat_id: int) -> str:
    if chat_id in config.CURADORIA_AVALISTAS_FONTE:
        return "ISENTA+AVAL"
    if chat_id in config.CURADORIA_ISENTOS:
        return "ISENTA"
    return "RETÉM"


# ─────────────────────────────────────────────────────────────────
# Instalação e encerramento — UMA vez por processo
# ─────────────────────────────────────────────────────────────────
def fontes_sem_aval(fontes: Iterable, avais: Iterable) -> list:
    """As fontes de publicação SEM nenhum canal de aval. Um canal de aval
    (Nerd Ofertas) nunca publica — nem se alguém o puser em GRUPOS_ORIGEM
    por engano: aqui ele sai da lista das fontes, antes dos handlers."""
    from telethon.utils import get_peer_id
    ids_aval = {get_peer_id(e) for e in avais}
    saida = []
    for e in fontes:
        if get_peer_id(e) in ids_aval:
            log_ded.error(f"❌ curadoria: @{getattr(e, 'username', '')} é canal de AVAL e "
                          f"estava nas fontes — retirado: canal de aval nunca publica")
            continue
        saida.append(e)
    return saida


def instalar(cliente, fontes: Iterable, avais: Iterable,
             despachar: Callable) -> Optional[asyncio.Task]:
    """Liga a curadoria (main._preparar_processo, depois dos handlers das
    fontes). `fontes` são as entidades de GRUPOS_ORIGEM, `avais` as de
    GRUPOS_AVAL, `despachar(evento, is_edit=)` o ponto de entrada. Confere
    cada ID de config.GRUPO_ID contra o @ resolvido e registra o papel de
    cada fonte. Devolve a task da varredura, ou None com o modo DESLIGADO."""
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
    _vistos.clear()
    _maior.clear()
    _pausa_ate.clear()
    _tentativas.clear()
    _SEGURADAS.clear()
    _aberto = True
    if avais:
        cliente.add_event_handler(_ao_aval, events.NewMessage(chats=avais))
    # Conferência: o ID configurado é a autoridade; o @ só confirma.
    por_id = {get_peer_id(e): (getattr(e, "username", None) or "").lower() for e in fontes}
    for nome, cid in config.GRUPO_ID.items():
        if cid not in por_id:
            log_ded.warning(f"⚠️ curadoria: {nome} ({cid}) não está entre as fontes "
                            f"resolvidas — papel {_papel(cid)} sem efeito")
        elif por_id[cid] != nome.lower():
            log_ded.error(f"❌ curadoria: o chat {cid} configurado como @{nome} responde "
                          f"como @{por_id[cid]} — vale o ID")
    papeis = ", ".join(f"@{u or cid}={cid}:{_papel(cid)}" for cid, u in por_id.items())
    lidos = ", ".join(f"@{(getattr(e, 'username', None) or '').lower()}={get_peer_id(e)}"
                      for e in avais)
    log_ded.info(f"🎯 curadoria {_modo()} | janela={_janela():g}s | fontes: {papeis} | "
                 f"aval só-sinal: {lidos or 'nenhum'} | lojas: "
                 f"{sorted(config.CURADORIA_LOJAS_AVAL)}")
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
    """Chave de produto no formato da identidade canônica: plataforma|id.
    Só link de PRODUTO (busca, campanha e evento não são produto)."""
    return sorted({f"{p}|{i}" for p, i, *resto in (idents or ())
                   if p and i and (not resto or resto[0] == TipoLink.PRODUTO.value)})


def _categoria_aval(texto: str) -> str:
    """Categoria vigiada no post de um AVALISTA: a do título ou, se o
    título é cabeçalho ("🚨 Produtos Tech para usar o cupom meia noite" —
    Nerd 147990), a da primeira linha de produto que tiver uma."""
    cat = categoria(texto or "")
    if cat:
        return cat
    for linha in (texto or "").splitlines():
        if linha.strip():
            cat = categoria(linha)
            if cat:
                return cat
    return ""


def _ts(data) -> float:
    try:
        return float(data.timestamp())
    except Exception:
        return time.time()


# ─────────────────────────────────────────────────────────────────
# PORTÃO — no pipeline, depois do enriquecimento e antes da deduplicação
# ─────────────────────────────────────────────────────────────────
def portao(norm, enr, is_edit: bool, data_msg=None, texto_original: str = "") -> bool:
    """True: a mensagem segue o pipeline. False: ficou SEGURADA (o funil
    para aqui; o log e o evento saem daqui). `data_msg` é a data ORIGINAL
    da mensagem no Telegram; `texto_original`, o texto da PRÓPRIA mensagem
    (a frase de destaque é procurada nele inteiro). Nunca levanta."""
    try:
        modo = _modo()
        if modo == DESLIGADO:
            return True
        try:
            chat_id = int(getattr(norm, "chat", ""))
        except (TypeError, ValueError):
            return True
        if chat_id in _estado["avais"]:
            # Canal de aval no funil (configuração errada): NUNCA publica.
            log_ded.error(f"❌ curadoria: mensagem do canal de aval {_nome(chat_id)} no "
                          f"funil de publicação — barrada")
            return False
        if getattr(enr, "tipo", None) != "produto":
            return True
        analise = getattr(norm, "texto_analise", "") or ""
        chaves = _chaves(getattr(norm, "idents", None))
        if not chaves:
            return True
        chat, msg_id = str(norm.chat), int(norm.msg_id)
        ts_msg = _ts(data_msg)
        if chat_id in config.CURADORIA_AVALISTAS_FONTE:
            cat = _categoria_aval(analise)
            if cat:
                _registrar_aval(chat, msg_id, chaves, cat, ts_msg)
            return True
        if chat_id in config.CURADORIA_ISENTOS:
            return True
        cat = categoria(analise)
        if not cat:
            return True
        if origem.consultar(chat, msg_id):
            return True             # já publicada/vinculada: edição e afins seguem
        livre = _isencao(norm, chaves, texto_original)
        if livre:
            log_ded.info(f"🎯 [CURADORIA_LIVRE] {_nome(chat)} id={msg_id} cat={cat} "
                         f"motivo={livre} — passa sem aval (não é aval de ninguém)")
            # A edição de uma segurada trouxe a exceção: fecha a linha (nenhum
            # aval posterior a reinjeta de novo).
            _SEGURADAS.pop((chat, msg_id), None)
            if is_edit and modo == ATIVO:
                db_curadoria_passou(chat, msg_id, time.time())
            return True
        resposta, nova = db_curadoria_avaliar(chat, msg_id, chaves, cat, ts_msg,
                                              _janela(), time.time())
        if resposta in (AVAL, LIBERACAO, JA_LIBERADA):
            if resposta != JA_LIBERADA:
                log_ded.info(f"🎯 [CURADORIA_{'LIBERADA' if resposta == LIBERACAO else 'COM_AVAL'}] "
                             f"{_nome(chat)} id={msg_id} cat={cat} — segue")
            _SEGURADAS.pop((chat, msg_id), None)
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
        mensagem = getattr(norm, "media_obj", None)
        if getattr(mensagem, "id", None) == msg_id:
            _SEGURADAS[(chat, msg_id)] = (mensagem, time.monotonic())
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


def _isencao(norm, chaves: List[str], texto_original: str = "") -> str:
    """Por que este post de categoria vigiada passa SEM aval (decisão do
    Léo, 03/11/2026), ou "" se não há motivo. Só a mensagem ORIGINAL conta:
    nada de comentário, resposta ou outra mensagem.
      LISTA     >= 2 produtos identificados E >= 2 produtos DISTINTOS com
                preço próprio no texto (curadoria_categoria.
                produtos_com_preco: variante de cor/tamanho/capacidade e
                outro vendedor do mesmo produto não contam);
      DESTAQUE  frase de destaque em QUALQUER ponto do texto da mensagem
                ("Ainda ativo!", "Bom preço!" — dono: pipeline.destaque);
      VOLTANDO  retorno de oferta pela MESMA detecção que a deduplicação e a
                decisão usam (pipeline.reativacao — sem regex própria)."""
    analise = getattr(norm, "texto_analise", "") or ""
    produtos = produtos_com_preco(analise)
    if len(chaves) >= _MIN_ITENS_LISTA and produtos >= _MIN_ITENS_LISTA:
        return f"LISTA({len(chaves)} identificados, {produtos} distintos com preço)"
    frase = frase_de_destaque(texto_original or analise)
    if frase:
        return f"DESTAQUE({frase})"
    if eh_reativacao(getattr(norm, "texto_limpo", "") or ""):
        return "VOLTANDO"
    return ""


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
        # Idade negativa = relógio local atrás do Telegram: não é velha.
        if _idade_seg(m.date) > _janela() + _ATRASO_MAX_S + _PRAZO_LIBERACAO_S:
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
    """Grava o aval (só nas lojas de identidade exata) e solta as seguradas
    do MESMO produto na MESMA loja, com data até 300 s da data do aval."""
    agora = time.time()
    chaves = [c for c in chaves if c.split("|", 1)[0] in config.CURADORIA_LOJAS_AVAL]
    if not chaves:
        log_ded.info(f"🎯 [CURADORIA_AVAL_SEM_LOJA] {_nome(chat)} id={msg_id} cat={cat} — "
                     f"loja fora de {sorted(config.CURADORIA_LOJAS_AVAL)}; não é aval")
        return
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
    """Entrega a segurada ao ponto de entrada como mensagem nova (via
    CURADORIA). NA HORA: a mensagem guardada quando foi segurada (ou na
    última edição) sai sem nova consulta ao Telegram. Sem ela na memória
    (reinício), é buscada na fonte. A fonte apagou: nada sai. O portão a
    reconhece pelo estado `liberando` e a deixa seguir."""
    guardada = _SEGURADAS.pop((chat, msg_id), None)
    m = guardada[0] if guardada else None
    if origem_apagada.apagada(chat, msg_id):
        m, apagada = None, True
    else:
        apagada = False
    if m is None and not apagada:
        try:
            ent = _estado["fontes"].get(int(chat))
        except (TypeError, ValueError):
            ent = None
        if ent is None or _estado["cliente"] is None:
            log_ded.warning(f"⚠️ [CURADORIA_LIBERACAO_FALHOU] {_nome(chat)} id={msg_id} — "
                            f"fonte não monitorada")
            db_curadoria_desfecho(chat, msg_id, FALHOU, time.time())
            return
        msgs, erro = None, None
        for espera in (0.0, *_BUSCA_REPETIR_S):
            if espera:
                await asyncio.sleep(espera)
            try:
                msgs, erro = await _estado["cliente"].get_messages(ent, ids=[msg_id]), None
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:                      # noqa: BLE001
                erro = e
        if erro is not None:
            log_ded.warning(f"⚠️ [CURADORIA_LIBERACAO_FALHOU] {_nome(chat)} id={msg_id} "
                            f"busca: {type(erro).__name__} "
                            f"({1 + len(_BUSCA_REPETIR_S)} tentativas)")
            db_curadoria_desfecho(chat, msg_id, FALHOU, time.time())
            return
        m = (msgs or [None])[0]
        if m is not None and getattr(m, "action", None) is not None:
            m = None
    if m is None:
        log_ded.info(f"🗑 [CURADORIA_AUSENTE] {_nome(chat)} id={msg_id} — sumiu da fonte "
                     f"antes de soltar; nada vai ao canal")
        db_curadoria_desfecho(chat, msg_id, AUSENTE, time.time())
        return
    log_ded.info(f"🔓 [CURADORIA_SOLTA] {_nome(chat)} id={msg_id} cat={cat} — aval chegou; "
                 f"volta ao ponto de entrada{'' if guardada else ' (buscada na fonte)'}")
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
    """Handler NewMessage dos canais de aval: o aval NA HORA (o Telegram só
    entrega o update se a conta segue o canal). Só agenda e volta."""
    try:
        _agendar(_observar_aval(int(event.chat_id), event.message))
    except Exception as e:                              # noqa: BLE001
        log_ded.warning(f"⚠️ curadoria: aval do handler ({type(e).__name__})")


async def _observar_aval(chat_id: int, message, repeticao: bool = False) -> None:
    """Uma mensagem do canal de aval: categoria vigiada + produto
    identificado (sem afiliar) → aval. Cada mensagem é olhada uma vez
    (handler e varredura podem trazer a mesma); se a identificação FALHOU
    (rede, encurtador que não abriu), ela é refeita depois de _REPETIR_S.
    Nada desta mensagem entra no funil de publicação."""
    if chat_id not in _estado["avais"]:
        return
    chave = (chat_id, int(getattr(message, "id", 0) or 0))
    if not repeticao:
        if chave in _vistos:
            return
        _vistos[chave] = None
        if len(_vistos) > _VISTOS_MAX:
            for k in list(_vistos)[:_VISTOS_MAX // 2]:
                _vistos.pop(k, None)
    try:
        # Idade negativa = relógio local atrás do Telegram: é recente.
        if _idade_seg(getattr(message, "date", None)) > _janela() + _ATRASO_MAX_S:
            return                  # aval velho não solta nada
        texto = ingestao.texto_de(message)
        cat = _categoria_aval(texto)
        if not cat:
            return
        chaves, incerto = await _identificar(ingestao.links_de(texto))
        if not chaves:
            n = _tentativas.get(chave, 0)
            if incerto and n < len(_REPETIR_S):
                _tentativas[chave] = n + 1
                log_ded.info(f"🎯 [CURADORIA_AVAL_REPETE] {_nome(chat_id)} id={chave[1]} "
                             f"cat={cat} — identificação falhou; nova tentativa em "
                             f"{_REPETIR_S[n]:g}s")
                _agendar(_repetir_aval(chat_id, message, _REPETIR_S[n]))
                return
            _tentativas.pop(chave, None)
            log_ded.info(f"🎯 [CURADORIA_AVAL_SEM_PRODUTO] {_nome(chat_id)} id={chave[1]} "
                         f"cat={cat} — sem identidade exata de produto "
                         f"({sorted(config.CURADORIA_LOJAS_AVAL)}"
                         f"{', identificação falhou' if incerto else ''}); não é aval")
            return
        _tentativas.pop(chave, None)
        _registrar_aval(str(chat_id), chave[1], chaves, cat, _ts(message.date))
    except asyncio.CancelledError:
        raise
    except Exception as e:                              # noqa: BLE001
        log_ded.warning(f"⚠️ curadoria: aval {_nome(chat_id)} id={chave[1]} "
                        f"não lido ({type(e).__name__})")


async def _repetir_aval(chat_id: int, message, atraso: float) -> None:
    await asyncio.sleep(atraso)
    await _observar_aval(chat_id, message, repeticao=True)


def _sem_http():
    sem = config._SEM_HTTP
    return sem if sem is not None else contextlib.nullcontext()


_INCERTO = object()   # a identificação falhou (não é o mesmo que "não é produto")


async def _identificar_um(url: str, sessao):
    """Identidade de produto de UM link, SEM afiliar: expansão do encurtador
    (memo em memória do resolvedor, o mesmo do pipeline) e, na plataforma, a
    capacidade `identifica` (Mercado Livre: lê a vitrine) ou
    `extrai_identidade`. Nunca chama `afilia`, SiteStripe ou createLink;
    nunca grava o cache de links. Só produto, só loja de identidade exata.
    Devolve "loja|id", None (não é produto de loja exata) ou _INCERTO
    (encurtador que não abriu: pode ser rede — vale nova tentativa)."""
    if classificar_universal(url) in ("bloqueado", "preservar", "mundial"):
        return None
    alvo = url
    if eh_encurtador_generico(url):
        async with _sem_http():
            alvo = await desencurtar(url, sessao)
        if eh_encurtador_generico(alvo):
            return _INCERTO
    plataforma = registry.resolver(alvo)
    if plataforma is None or plataforma.identificador not in config.CURADORIA_LOJAS_AVAL:
        return None
    curto = plataforma.extrai_identidade(alvo).tipo_link == TipoLink.ENCURTADO
    if plataforma.identifica is not None:
        ident = await plataforma.identifica(alvo, sessao)
        if curto and ident.tipo_link == TipoLink.INVALIDO:
            return _INCERTO
    else:
        ident = plataforma.extrai_identidade(alvo)
        if curto:
            async with _sem_http():
                expandida = await desencurtar(alvo, sessao)
            if registry.resolver(expandida) is not plataforma:
                return None
            ident = plataforma.extrai_identidade(expandida)
            if ident.tipo_link == TipoLink.ENCURTADO:
                return _INCERTO
    if (ident is None or ident.tipo_link != TipoLink.PRODUTO
            or ident.id_produto is SEM_ID or not ident.id_produto):
        return None
    return f"{plataforma.identificador}|{ident.id_produto}"


async def _identificar(links: List[str]) -> Tuple[List[str], bool]:
    """As chaves de produto (loja|id) dos links de uma mensagem do canal de
    aval, todos ao mesmo tempo (velocidade), e se ALGUM link falhou
    (exceção ou encurtador que não abriu)."""
    links = list(dict.fromkeys(links))[:_MAX_LINKS_AVAL]
    if not links:
        return [], False
    sessao = await g._get_session()
    res = await asyncio.gather(*[_identificar_um(u, sessao) for u in links],
                               return_exceptions=True)
    incerto = any(r is _INCERTO or isinstance(r, Exception) for r in res)
    return sorted({r for r in res if isinstance(r, str)}), incerto


async def _varrer() -> None:
    """Lê os canais de aval a cada _VARREDURA_S (não depende de a conta
    seguir o canal nem de o Telegram entregar o update) e expira as
    seguradas que passaram da janela (depois da leitura)."""
    while True:
        await asyncio.sleep(_VARREDURA_S)
        if g._encerrando or not _aberto:
            return
        # Primeiro LÊ o aval, depois expira: o aval desta rodada nunca
        # perde para a expiração da mesma rodada.
        for chat_id, ent in list(_estado["avais"].items()):
            if g._encerrando or not _aberto:
                return
            await _varrer_canal(chat_id, ent)
        try:
            _expirar()
        except Exception as e:                          # noqa: BLE001
            log_ded.warning(f"⚠️ curadoria: expirar ({type(e).__name__})")


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
    velho = time.monotonic() - (_janela() + _ATRASO_MAX_S + _PRAZO_LIBERACAO_S)
    for k in [k for k, (_m, t) in _SEGURADAS.items() if t < velho]:
        _SEGURADAS.pop(k, None)
    for chat, mid, cat in db_curadoria_expirar(agora - _janela() - _ATRASO_MAX_S, agora):
        _SEGURADAS.pop((chat, mid), None)
        if sombra:
            log_ded.info(f"👻 [CURADORIA_SOMBRA] {_nome(chat)} id={mid} cat={cat} — "
                         f"{_janela():g}s sem aval: no ATIVO não iria ao canal")
        else:
            log_ded.info(f"⌛ [CURADORIA_EXPIROU] {_nome(chat)} id={mid} cat={cat} — "
                         f"{_janela():g}s sem aval do Fada/Nerd; não vai ao canal")
