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
  · vale para todo post que não é CUPOM: produto e também "evento" (a
    natureza do post sem identidade de produto — campanha, busca,
    encurtador que não abriu, produto sem código); cupom segue o fluxo de
    sempre;
  · vale para TODA fonte monitorada, menos as ISENTAS — Fada Promoções, Fada
    dos Cupons e Oferta Convertida —, sempre pelo ID REAL do chat
    (config.GRUPO_ID / CURADORIA_ISENTOS); fonte nova entra retida;
  · AVALISTAS: o Fada Promoções (fonte comum: o post dele sai normal E é
    aval — config.CURADORIA_AVALISTAS_FONTE) e o Nerd Ofertas (canal só de
    sinal, config.GRUPOS_AVAL: o bot só LÊ; nada dele entra na fila nem vai
    ao canal). Fada dos Cupons e Oferta Convertida são isentas, mas não dão
    aval;
  · IDENTIDADE = o conjunto de produtos da VERSÃO ATUAL da mensagem (link
    de produto, só nas lojas de identidade exata — config.
    CURADORIA_LOJAS_AVAL: ASIN da Amazon, MLB do Mercado Livre, loja+item
    da Shopee —, com a loja na chave: "amazon|B0…"). Aval de QUALQUER
    produto atual solta a mensagem inteira; produto que a edição tirou não
    conta mais; título parecido não é aval; identidade incerta não é aval;
    mensagem SEM identidade exata nunca tem aval (fica segurada);
  · janela (config.CURADORIA_JANELA_S, 300 s) pelas DATAS do Telegram, nos
    dois sentidos, contada da ENTRADA de cada produto na mensagem (a data
    da mensagem; numa edição que traz produto novo, o edit_date dela):
      aval ANTES (até 300 s)  → a versão passa direto;
      aval DEPOIS (até 300 s) → a versão fica SEGURADA e o aval a SOLTA, na
                                hora em que o aval é visto;
      fora da janela → a segurada nunca vai ao canal;
  · EDIÇÃO de mensagem JÁ PUBLICADA: preço, cupom, texto — tudo que
    mantém os produtos que o post de destino já tem (o que ele exibe e a
    família dele) — segue normal. Produto NOVO de categoria vigiada passa
    pela mesma regra; sem aval, a edição fica PENDENTE e o post continua
    com o produto aprovado; o aval aplica a edição pelo caminho normal de
    edição (o mesmo post, nunca um segundo) — também depois de a mensagem
    passar de 1 h (trava EDIT_ANTIGO), se a edição é recente e há a prova;
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
versão a `processar` como EventoRecuperado com via CURADORIA — mensagem
nova (nunca publicada) ou edição (alteração pendente de post publicado) —,
e o pipeline de sempre faz o resto: travas, identidade, família, decisão,
publicação. Nenhum segundo caminho de escrita no canal. VELOCIDADE: a
versão guardada na memória quando foi segurada (a última, se a fonte
editou) sai na hora, sem nova consulta ao Telegram; só depois de um
reinício ela é buscada na fonte. O portão só a deixa passar se o conjunto
dela é exatamente o que o aval soltou.

A reinjeção de mensagem NOVA tem até ~7 min: a trava de frescor (120 s) e
a idempotência em memória a barrariam; a de EDIÇÃO pendente pode chegar
quando a mensagem já passou de 1 h (trava EDIT_ANTIGO). `reinjecao()` é a
única exceção às três travas, e exige as três provas juntas: via
CURADORIA, estado `liberando` ou `reavaliando` marcado há menos de
_PRAZO_LIBERACAO_S no banco, e a VERSÃO (edit_date, ou a data da mensagem)
dentro da janela.

═══════════════════════════════════════════════════════════════════
FALHA — fechada para o hardware vigiado
═══════════════════════════════════════════════════════════════════
Banco fora ou exceção interna nunca viram autorização. A versão de
hardware vigiado de fonte retentora fica BLOQUEADA, guardada SÓ NA
MEMÓRIA (fila limitada, _MAX_PENDENTES; o aval que não pôde ser gravado
também, _MAX_AVAIS_PENDENTES); a gravação é tentada de novo em
_REPOR_PRIMEIRO_S e a cada rodada; com o banco de volta, ela volta ao
ponto de entrada com a prova `reavaliando` e o portão a avalia de novo.
RISCO DOCUMENTADO: o que está só na memória NÃO é durável — um reinício
com o banco fora o perde (fica fora do canal; nada é publicado sem aval).
Os logs dizem isso em cada caso. Outras categorias, cupom e campanha nunca
passam pelo banco no portão e não são bloqueadas. Modo SOMBRA nunca
bloqueia.

═══════════════════════════════════════════════════════════════════
ROBUSTEZ
═══════════════════════════════════════════════════════════════════
  · sem corrida: "há aval para esta versão?" (grava o conjunto atual) e
    "há segurada para este aval?" rodam cada uma numa transação;
  · edição da segurada continua segurada (a janela do produto que
    PERMANECE não recomeça); produto novo entra com o edit_date dela;
    produto que sai não conta; o que sai e VOLTA é ocorrência nova (data da
    edição que o trouxe de volta: o aval da ocorrência antiga não vale);
  · a fonte apagou a segurada: nada sai (origem_apagada + busca na fonte);
  · reinício: avais e seguradas estão no banco; a liberação interrompida é
    retomada no boot; a que parou no caminho (_PRESA_S) é entregue de novo,
    até _MAX_REENTREGAS vezes; a leitura do canal de aval recomeça da
    janela;
  · canal de aval: mensagem NOVA e EDIÇÃO chegam NA HORA pelos handlers
    (NewMessage e MessageEdited) quando a conta segue o canal; cada VERSÃO
    (id + edit_date) é lida uma vez, inteira: o produto que permanece
    conserva a data, o acrescentado (ou que volta) entra pelo edit_date;
    update atrasado de versão mais velha é ignorado; leitura que falha em
    parte não tira produto. A leitura a cada _VARREDURA_S — sempre a página
    mais recente, que traz o novo E a edição — é a rede de segurança
    (update perdido) e respeita FloodWait; cada rodada LÊ o aval antes de
    expirar; o aval visto com até _ATRASO_MAX_S de atraso ainda solta;
  · identificação do aval que falhou (rede, encurtador que não abriu) é
    refeita (_REPETIR_S); a busca da segurada na fonte também (_BUSCA_
    REPETIR_S); falha persistente fecha sem inventar aval;
  · post de avalista com cabeçalho ("🚨 Produtos Tech…") vale pela primeira
    linha de produto com categoria (_categoria_aval);
  · relógio local atrás do Telegram (data "no futuro") não perde aval nem
    reinjeção;
  · a versão que SEGUE sem aval (exceção, identidade publicada preservada,
    deixou de ser hardware) fecha a linha (db_curadoria_seguiu): nenhum
    aval posterior reinjeta uma versão que a fonte já trocou;
  · modo SOMBRA: só registra; DESLIGADO: nada roda.

Dependências INJETADAS em `instalar` (cliente, fontes, canais de aval,
ponto de entrada): este módulo não importa a orquestração nem o cliente.
"""
from __future__ import annotations

import asyncio
import contextlib
import contextvars
import time
from collections import OrderedDict
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import config
import eventos
import globals as g
from database import (db_curadoria_avaliar, db_curadoria_desfecho,
                      db_curadoria_em_liberacao, db_curadoria_expirar,
                      db_curadoria_reavaliar, db_curadoria_registrar_aval,
                      db_curadoria_retomar, db_curadoria_seguiu, db_exibida,
                      db_ofertas_de_post)
from database_curadoria import (AVAL, AUSENTE, EDICAO, ERRO, FALHOU, LIBERACAO,
                                LIBERADA, NOVA, OBSOLETA, SUPERADA)
from logger import _idade_seg, log_ded
from pipeline import ingestao, origem_apagada
from pipeline.completude import EventoRecuperado
from pipeline.curadoria_categoria import categoria, produtos_com_preco
from pipeline.destaque import frase_de_destaque
from pipeline.identidade import username_de
from pipeline.publicacao_estado import destino_vivo_de_origem
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
# continua exigindo |data do aval − entrada do produto| <= 300 s; isto só
# evita que a demora em ENXERGAR um aval válido o perca. O produto mais
# velho que se solta entrou há 300 + 120 s.
_ATRASO_MAX_S = 120.0
# Validade da prova `liberando`/`reavaliando` para a admissão: a reinjeção
# acontece em segundos depois de marcada.
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
# Reinjeção marcada e que não chegou ao portão em _PRESA_S (prova perdida
# no banco, normalização que falhou): entregue de novo, até
# _MAX_REENTREGAS vezes; depois, `falhou`.
_PRESA_S = 30.0
_MAX_REENTREGAS = 3
# Banco fora: o que fica SÓ NA MEMÓRIA (não durável) e quando tentar de novo.
_MAX_PENDENTES = 500
_MAX_AVAIS_PENDENTES = 500
_REPOR_PRIMEIRO_S = 2.0
# Exceção interna que se repete na MESMA mensagem: depois de tantas, desiste
# (a mensagem fica fora do canal; o log diz).
_MAX_FALHAS = 5


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
# A versão mais recente já lida de cada mensagem do canal de aval: um update
# atrasado de versão MAIS VELHA não desfaz o conjunto da mais nova.
_ultima_versao: Dict[tuple, int] = {}
# A versão segurada, como chegou (ou na última edição): (chat, msg) ->
# (Message, instante). É o que sai na hora quando o aval chega.
_SEGURADAS: Dict[tuple, Tuple[object, float]] = {}
# Banco fora / exceção interna — SÓ NA MEMÓRIA, não durável:
#   _PENDENTES       (chat, msg) -> {is_edit, cat, ts_msg, limite}
#   _AVAIS_PENDENTES (fonte, msg) -> (chaves, cat, ts_aval, parcial)
_PENDENTES: "OrderedDict[tuple, dict]" = OrderedDict()
_AVAIS_PENDENTES: "OrderedDict[tuple, tuple]" = OrderedDict()
_reentregas: Dict[tuple, int] = {}
_falhas: Dict[tuple, int] = {}
_repor_marcado = False


class _BancoFora(Exception):
    """db_curadoria_avaliar respondeu ERRO: a versão NÃO passa."""


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
    global _aberto, _repor_marcado
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
    _ultima_versao.clear()
    _SEGURADAS.clear()
    _PENDENTES.clear()
    _AVAIS_PENDENTES.clear()
    _reentregas.clear()
    _falhas.clear()
    _repor_marcado = False
    _aberto = True
    if avais:
        # Mensagem NOVA e EDIÇÃO do canal de aval: a edição que acrescenta
        # (ou troca) o link de produto é aval pela data dela.
        cliente.add_event_handler(_ao_aval, events.NewMessage(chats=avais))
        cliente.add_event_handler(_ao_aval, events.MessageEdited(chats=avais))
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


def _datas(mensagem) -> Tuple[float, float]:
    """(data original, data desta versão): a versão editada vale pelo
    edit_date — é quando o produto que ela trouxe entrou na mensagem."""
    ts_msg = _ts(getattr(mensagem, "date", None))
    editada = getattr(mensagem, "edit_date", None)
    return ts_msg, (_ts(editada) if editada else ts_msg)


def _aprovadas(dest: int) -> set:
    """Os produtos que o post de destino JÁ tem: o que ele exibe e a
    família dele (oferta_index). Falha de leitura devolve vazio — o que só
    faz a edição passar pela curadoria, nunca a libera."""
    return set(db_exibida(dest) or ()) | set(db_ofertas_de_post(dest) or ())


def _soltar_memoria(k: tuple) -> None:
    _SEGURADAS.pop(k, None)
    _PENDENTES.pop(k, None)
    _reentregas.pop(k, None)
    _falhas.pop(k, None)


def _seguiu(chat: str, msg_id: int, chaves: List[str], ts_versao: float,
            estado: str, motivo: str) -> None:
    """A versão atual seguiu sem aval: fecha a linha aberta (se houver) e
    larga a memória. Nunca bloqueia: falha do banco aqui só é registrada
    (o que está na memória sai — nenhum aval posterior reinjeta a versão
    antiga sem antes buscá-la de novo na fonte)."""
    _soltar_memoria((chat, msg_id))
    r = db_curadoria_seguiu(chat, msg_id, chaves, ts_versao, estado, time.time())
    if r:
        log_ded.info(f"🎯 [CURADORIA_{'SUPERADA' if estado == SUPERADA else 'SEGUIU'}] "
                     f"{_nome(chat)} id={msg_id} — {motivo}; a pendência desta mensagem "
                     f"foi fechada")
    elif r is None:
        log_ded.error(f"❌ curadoria: banco indisponível ao fechar a pendência de "
                      f"{_nome(chat)} id={msg_id} ({motivo})")


def _emitir_retida(chat: str, msg_id: int, cat: str, n: int, nova: bool,
                   is_edit: bool, situacao: str) -> None:
    eventos.emitir_de("curadoria.retida", lambda: (
        {"chat": chat, "msg": msg_id},
        {"categoria": cat, "produtos_n": n, "nova": nova, "is_edit": bool(is_edit),
         "situacao": situacao}),
        local="curadoria.portao.retida")


# ─────────────────────────────────────────────────────────────────
# PORTÃO — no pipeline, depois do enriquecimento e antes da deduplicação
# ─────────────────────────────────────────────────────────────────
def portao(norm, enr, is_edit: bool, mensagem=None, texto_original: str = "",
           via: Optional[str] = None) -> bool:
    """True: a versão segue o pipeline. False: ficou SEGURADA ou BLOQUEADA
    (o funil para aqui; o log e o evento saem daqui). `mensagem` é a
    mensagem do Telegram desta versão (datas originais e a cópia que sai na
    hora); `texto_original`, o texto da PRÓPRIA mensagem (a frase de
    destaque é procurada nele inteiro); `via`, a via do evento (CURADORIA =
    reinjeção). Nunca levanta. Nenhuma exceção vira autorização para o
    hardware de fonte retentora: ela BLOQUEIA (_falha_fechada)."""
    modo = _modo()
    if modo == DESLIGADO:
        return True
    try:
        chat_id = int(getattr(norm, "chat", ""))
        chat, msg_id = str(norm.chat), int(norm.msg_id)
    except (TypeError, ValueError, AttributeError):
        return True                 # não é mensagem de fonte numérica
    ctx: dict = {"cat": "", "is_edit": bool(is_edit), "mensagem": mensagem}
    retentora = False
    try:
        if chat_id in _estado["avais"]:
            # Canal de aval no funil (configuração errada): NUNCA publica.
            log_ded.error(f"❌ curadoria: mensagem do canal de aval {_nome(chat_id)} no "
                          f"funil de publicação — barrada")
            return False
        if chat_id in config.CURADORIA_AVALISTAS_FONTE:
            _aval_da_fonte(norm, enr, mensagem)
            return True
        if chat_id in config.CURADORIA_ISENTOS:
            return True
        retentora = True
        return _portao_retentora(norm, enr, is_edit, mensagem, texto_original,
                                 via == VIA, modo, chat, msg_id, ctx)
    except Exception as e:                              # noqa: BLE001
        if not retentora:
            log_ded.error(f"❌ curadoria.portao: {type(e).__name__}: {e} — fonte isenta: "
                          f"segue", exc_info=True)
            return True
        return _falha_fechada(chat, msg_id, ctx, e, modo)


def _portao_retentora(norm, enr, is_edit: bool, mensagem, texto_original: str,
                      reinjetada: bool, modo: str, chat: str, msg_id: int,
                      ctx: dict) -> bool:
    ts_msg, ts_versao = _datas(mensagem)
    ctx.update(ts_msg=ts_msg, ts_versao=ts_versao)
    analise = getattr(norm, "texto_analise", "") or ""
    chaves = _chaves(getattr(norm, "idents", None))
    # Só o CUPOM fica fora (é outra categoria de post). Produto e "evento" —
    # a natureza do post SEM identidade de produto: link de campanha, de
    # busca, encurtador que não abriu, produto sem código — entram: hardware
    # vigiado sem identidade exata fica retido (nenhum aval o solta).
    # Natureza desconhecida também entra (fechado por padrão).
    if getattr(enr, "tipo", None) == "cupom" or not (cat := categoria(analise)):
        # Não é hardware vigiado: nunca é bloqueado. Se esta versão substitui
        # uma pendente (edição), a pendente fecha.
        if is_edit or reinjetada:
            _seguiu(chat, msg_id, chaves, ts_versao, SUPERADA,
                    "a versão atual não é hardware vigiado")
        return True
    ctx["cat"] = cat
    dest = destino_vivo_de_origem(chat, msg_id)
    tipo = EDICAO if dest else NOVA
    aprovadas: set = set()
    if dest:
        aprovadas = _aprovadas(dest)
        if chaves and set(chaves) <= aprovadas:
            # Edição que mantém os produtos que o post já tem (preço, cupom,
            # texto): segue normal.
            _seguiu(chat, msg_id, chaves, ts_versao, LIBERADA if reinjetada else SUPERADA,
                    f"edição preserva a identidade do post:{dest}")
            return True
    livre = _isencao(norm, chaves, texto_original)
    if livre:
        log_ded.info(f"🎯 [CURADORIA_LIVRE] {_nome(chat)} id={msg_id} cat={cat} "
                     f"motivo={livre} — passa sem aval (não é aval de ninguém)")
        if is_edit or reinjetada or dest:
            _seguiu(chat, msg_id, chaves, ts_versao, LIBERADA, f"exceção {livre}")
        else:
            _soltar_memoria((chat, msg_id))
        return True
    resposta, nova = db_curadoria_avaliar(chat, msg_id, tipo, chaves, cat, ts_msg, ts_versao,
                                          _janela(), _ATRASO_MAX_S, time.time(),
                                          reinjetada=reinjetada, ja_publicadas=aprovadas)
    if resposta == ERRO:
        raise _BancoFora()
    if resposta in (AVAL, LIBERACAO):
        rotulo = "LIBERADA" if resposta == LIBERACAO else "COM_AVAL"
        if tipo == EDICAO:
            rotulo = "EDICAO_" + rotulo
        log_ded.info(f"🎯 [CURADORIA_{rotulo}] {_nome(chat)} id={msg_id} cat={cat} "
                     f"produtos={chaves} — segue")
        _soltar_memoria((chat, msg_id))
        return True
    if resposta == OBSOLETA:
        log_ded.info(f"🎯 [CURADORIA_OBSOLETA] {_nome(chat)} id={msg_id} — reinjeção que "
                     f"não é mais a versão aprovada; descartada")
        log_ded.info(f"🧭 TL | id={msg_id} chat={chat} | DESCARTE | "
                     f"motivo=CURADORIA_OBSOLETA cat={cat} edit={is_edit}")
        _emitir_retida(chat, msg_id, cat, len(chaves), False, is_edit, "OBSOLETA")
        return False
    # SEGURADA
    sem_id = "" if chaves else " (SEM identidade exata de produto: nenhum aval a solta)"
    if modo == SOMBRA:
        if nova:
            log_ded.info(f"👻 [CURADORIA_SOMBRA] {_nome(chat)} id={msg_id} cat={cat} "
                         f"produtos={chaves}{sem_id} — seguraria (sem aval); segue normal")
        return True
    if getattr(mensagem, "id", None) == msg_id:
        _SEGURADAS[(chat, msg_id)] = (mensagem, time.monotonic())
    if tipo == EDICAO:
        if nova:
            log_ded.info(f"✏️ [CURADORIA_EDICAO_PENDENTE] {_nome(chat)} id={msg_id} "
                         f"cat={cat} produtos={chaves}{sem_id} — produto NOVO sem aval: "
                         f"post:{dest} segue com o aprovado; alteração pendente até "
                         f"{_janela():g}s")
        motivo = "CURADORIA_EDICAO_PENDENTE"
    else:
        if nova:
            log_ded.info(f"🎯 [CURADORIA_RETIDA] {_nome(chat)} id={msg_id} cat={cat} "
                         f"produtos={chaves}{sem_id} — espera aval (Fada/Nerd) até "
                         f"{_janela():g}s")
        motivo = "CURADORIA_RETIDA"
    log_ded.info(f"🧭 TL | id={msg_id} chat={chat} | DESCARTE | "
                 f"motivo={motivo} cat={cat} edit={is_edit}")
    _emitir_retida(chat, msg_id, cat, len(chaves), nova, is_edit,
                   "EDICAO_PENDENTE" if tipo == EDICAO else "SEGURADA")
    return False


def _falha_fechada(chat: str, msg_id: int, ctx: dict, erro: Exception, modo: str) -> bool:
    """Banco fora ou exceção interna numa mensagem de fonte retentora: a
    versão é BLOQUEADA (nunca autorizada) e guardada SÓ NA MEMÓRIA para
    nova tentativa. SOMBRA nunca bloqueia."""
    banco = isinstance(erro, _BancoFora)
    causa = "banco indisponível" if banco else f"falha interna {type(erro).__name__}: {erro}"
    if modo == SOMBRA:
        log_ded.error(f"👻 [CURADORIA_SOMBRA] {_nome(chat)} id={msg_id} — {causa}; "
                      f"no ATIVO ficaria bloqueada")
        return True
    k = (chat, msg_id)
    _falhas[k] = _falhas.get(k, 0) + 1
    rotulo = "CURADORIA_SEM_BANCO" if banco else "CURADORIA_FALHA_INTERNA"
    if _falhas[k] > _MAX_FALHAS:
        log_ded.error(f"❌ [{rotulo}] {_nome(chat)} id={msg_id} — {causa} pela "
                      f"{_falhas[k]}ª vez: desiste; NÃO vai ao canal", exc_info=not banco)
        _soltar_memoria(k)
    else:
        log_ded.error(f"❌ [{rotulo}] {_nome(chat)} id={msg_id} cat={ctx.get('cat') or '?'} "
                      f"— {causa}: BLOQUEADA, guardada SÓ NA MEMÓRIA (não persistida; um "
                      f"reinício agora a perde e ela não vai ao canal); nova tentativa em "
                      f"{_REPOR_PRIMEIRO_S:g}s", exc_info=not banco)
        _pendurar(k, ctx)
    log_ded.info(f"🧭 TL | id={msg_id} chat={chat} | DESCARTE | motivo={rotulo} "
                 f"edit={ctx.get('is_edit')}")
    try:
        _emitir_retida(chat, msg_id, ctx.get("cat") or "", 0, False,
                       bool(ctx.get("is_edit")), "SEM_BANCO" if banco else "FALHA_INTERNA")
    except Exception:                                   # noqa: BLE001
        pass
    return False


def _pendurar(k: tuple, ctx: dict) -> None:
    mensagem = ctx.get("mensagem")
    if getattr(mensagem, "id", None) == k[1]:
        _SEGURADAS[k] = (mensagem, time.monotonic())
    ts_msg = ctx.get("ts_msg") or time.time()
    ts_versao = ctx.get("ts_versao") or ts_msg
    _PENDENTES[k] = {"is_edit": bool(ctx.get("is_edit")), "cat": ctx.get("cat") or "",
                     "ts_msg": ts_msg, "limite": ts_versao + _janela() + _ATRASO_MAX_S}
    _PENDENTES.move_to_end(k)
    while len(_PENDENTES) > _MAX_PENDENTES:
        velho, _ = _PENDENTES.popitem(last=False)
        _SEGURADAS.pop(velho, None)
        log_ded.error(f"❌ [CURADORIA_PENDENTE_DESCARTADA] {_nome(velho[0])} id={velho[1]} — "
                      f"fila da memória cheia ({_MAX_PENDENTES}); NÃO vai ao canal")
    _marcar_repor()


def _marcar_repor() -> None:
    """Primeira nova tentativa de gravar o que ficou na memória, logo depois
    da falha (as seguintes vêm a cada rodada da varredura)."""
    global _repor_marcado
    if _repor_marcado or not _aberto or g._encerrando:
        return
    _repor_marcado = True
    _agendar(_repor_em(_REPOR_PRIMEIRO_S))


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
                decisão usam (pipeline.reativacao — sem regex própria).
    As exceções dispensam o AVAL (e por isso a identidade, que só serve ao
    aval): valem também para a mensagem sem identidade exata."""
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
    segurada (aval) ou de uma bloqueada que volta com o banco (`reavaliando`)?
    Só com as três provas: via CURADORIA, estado `liberando`/`reavaliando`
    no banco marcado há menos de _PRAZO_LIBERACAO_S e mensagem dentro da
    janela. Síncrona, barata quando a via é outra; nunca levanta. É o que
    deixa a reinjeção passar pela trava de frescor e pela idempotência em
    memória — e nada mais passa por ela. Quem decide se a versão vai ao
    canal continua sendo o portão."""
    try:
        if getattr(event, "via", None) != VIA:
            return False
        m = event.message
        # A versão vale pela data DELA: a edição que trouxe o produto novo
        # (edit_date) abre a janela dele, mesmo que a mensagem seja antiga.
        # Idade negativa = relógio local atrás do Telegram: não é velha.
        ref = getattr(m, "edit_date", None) or m.date
        if _idade_seg(ref) > _janela() + _ATRASO_MAX_S + _PRAZO_LIBERACAO_S:
            return False
        return db_curadoria_em_liberacao(str(event.chat_id), int(m.id),
                                         time.time() - _PRAZO_LIBERACAO_S)
    except Exception:                                   # noqa: BLE001
        return False


# ─────────────────────────────────────────────────────────────────
# AVAL — Fada Promoções pelo portão; Nerd pelo canal de aval
# ─────────────────────────────────────────────────────────────────
def _aval_da_fonte(norm, enr, mensagem) -> None:
    """O post do Fada Promoções (isento) também é aval: cada produto vale
    pela data em que entrou no post (a da mensagem, ou o edit_date da
    edição que o trouxe). Nunca bloqueia o post do Fada."""
    try:
        if getattr(enr, "tipo", None) != "produto":
            return
        chaves = _chaves(getattr(norm, "idents", None))
        cat = _categoria_aval(getattr(norm, "texto_analise", "") or "")
        if chaves and cat:
            _registrar_aval(str(norm.chat), int(norm.msg_id), chaves, cat, _datas(mensagem)[1])
    except Exception as e:                              # noqa: BLE001
        log_ded.error(f"❌ curadoria: aval do Fada não registrado ({type(e).__name__}: {e})",
                      exc_info=True)


def _registrar_aval(chat: str, msg_id: int, chaves: List[str], cat: str,
                    ts_aval: float, parcial: bool = False) -> None:
    """Grava a versão atual da mensagem do avalista (só as lojas de
    identidade exata) e solta as seguradas com o MESMO produto na MESMA loja
    entrado até 300 s da data em que o produto entrou no avalista. `ts_aval`
    é a data DESTA versão (edit_date na edição): vale para o produto
    acrescentado ou reintroduzido; o que permanece conserva a data original
    (database_curadoria). Banco fora: o aval fica SÓ NA MEMÓRIA e é gravado
    quando o banco voltar."""
    agora = time.time()
    chaves = [c for c in chaves if c.split("|", 1)[0] in config.CURADORIA_LOJAS_AVAL]
    if not chaves:
        log_ded.info(f"🎯 [CURADORIA_AVAL_SEM_LOJA] {_nome(chat)} id={msg_id} cat={cat} — "
                     f"loja fora de {sorted(config.CURADORIA_LOJAS_AVAL)}; não é aval")
        return
    soltas = db_curadoria_registrar_aval(
        chaves, chat, msg_id, ts_aval, _janela(),
        agora - _janela() - _ATRASO_MAX_S, agora, parcial=parcial)
    if soltas is None:
        _AVAIS_PENDENTES[(chat, msg_id)] = (chaves, cat, ts_aval, parcial)
        _AVAIS_PENDENTES.move_to_end((chat, msg_id))
        while len(_AVAIS_PENDENTES) > _MAX_AVAIS_PENDENTES:
            velho, _ = _AVAIS_PENDENTES.popitem(last=False)
            log_ded.error(f"❌ [CURADORIA_AVAL_DESCARTADO] {_nome(velho[0])} id={velho[1]} — "
                          f"fila de avais da memória cheia ({_MAX_AVAIS_PENDENTES})")
        log_ded.error(f"❌ [CURADORIA_AVAL_SEM_BANCO] {_nome(chat)} id={msg_id} "
                      f"produtos={chaves} — banco indisponível: aval guardado SÓ NA "
                      f"MEMÓRIA (um reinício agora o perde); nova tentativa em "
                      f"{_REPOR_PRIMEIRO_S:g}s")
        _marcar_repor()
        return
    log_ded.info(f"✅ [CURADORIA_AVAL] {_nome(chat)} id={msg_id} cat={cat} "
                 f"produtos={chaves} solta={[f'{_nome(c)}/{m}' for c, m, _, _ in soltas]}")
    eventos.emitir_de("curadoria.aval", lambda: (
        {"chat": chat, "msg": msg_id},
        {"categoria": cat, "produtos_n": len(chaves), "solta_n": len(soltas)}),
        local="curadoria.aval.registrado")
    _soltar(soltas, chat, agora)


def _soltar(soltas, quem: str, agora: float) -> None:
    for c, m, cat_r, tipo in soltas:
        if _modo() == SOMBRA:
            log_ded.info(f"👻 [CURADORIA_SOMBRA] {_nome(c)} id={m} cat={cat_r} — o aval "
                         f"de {_nome(quem)} soltaria")
            db_curadoria_desfecho(c, m, LIBERADA, agora)
            continue
        _agendar(_liberar(c, m, cat_r, tipo))


async def _liberar(chat: str, msg_id: int, cat: str, tipo: str = NOVA) -> None:
    """Entrega a versão segurada ao ponto de entrada (via CURADORIA): como
    mensagem NOVA (nunca publicada) ou como EDIÇÃO (alteração pendente de
    post já publicado — o caminho normal de edição atualiza o mesmo post).
    NA HORA: a versão guardada na memória (a última que a fonte mandou) sai
    sem nova consulta ao Telegram; sem ela (reinício), é buscada na fonte.
    A fonte apagou: nada sai. A cópia da memória só sai da memória quando o
    portão a aceita (uma reentrega ainda a encontra)."""
    guardada = _SEGURADAS.get((chat, msg_id))
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
        _soltar_memoria((chat, msg_id))
        db_curadoria_desfecho(chat, msg_id, AUSENTE, time.time())
        return
    edicao = tipo == EDICAO
    log_ded.info(f"🔓 [CURADORIA_{'EDICAO_SOLTA' if edicao else 'SOLTA'}] {_nome(chat)} "
                 f"id={msg_id} cat={cat} — volta ao ponto de entrada como "
                 f"{'EDIÇÃO (o mesmo post)' if edicao else 'mensagem nova'}"
                 f"{'' if guardada else ' (buscada na fonte)'}")
    try:
        await _estado["despachar"](EventoRecuperado(m, via=VIA), is_edit=edicao)
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
    for chat, mid, cat, tipo in db_curadoria_retomar(
            agora - _janela() - _ATRASO_MAX_S - _PRAZO_LIBERACAO_S, agora):
        log_ded.info(f"🔁 [CURADORIA_RETOMADA] {_nome(chat)} id={mid} — liberação "
                     f"interrompida pelo reinício")
        _agendar(_liberar(chat, mid, cat, tipo))


def _reentregar_presas() -> None:
    """Reinjeções marcadas que não chegaram ao portão em _PRESA_S (a prova
    não foi lida — banco —, a normalização falhou, a fila perdeu): entregues
    de novo, até _MAX_REENTREGAS vezes. Reentrega a mais é inofensiva: a
    segunda chega ao portão como OBSOLETA ou à admissão sem prova."""
    agora = time.time()
    for chat, mid, cat, tipo in db_curadoria_retomar(
            agora - _janela() - _ATRASO_MAX_S - _PRAZO_LIBERACAO_S, agora,
            presas_desde=agora - _PRESA_S):
        k = (chat, mid)
        n = _reentregas.get(k, 0) + 1
        if n > _MAX_REENTREGAS:
            _soltar_memoria(k)
            db_curadoria_desfecho(chat, mid, FALHOU, agora)
            log_ded.warning(f"⚠️ [CURADORIA_LIBERACAO_FALHOU] {_nome(chat)} id={mid} — "
                            f"{_MAX_REENTREGAS} reentregas sem chegar ao portão")
            continue
        _reentregas[k] = n
        log_ded.info(f"🔁 [CURADORIA_REENTREGA] {_nome(chat)} id={mid} — a reinjeção não "
                     f"chegou ao portão em {_PRESA_S:g}s; entrega {n}/{_MAX_REENTREGAS}")
        _agendar(_liberar(chat, mid, cat, tipo))


async def _repor_em(atraso: float) -> None:
    global _repor_marcado
    try:
        await asyncio.sleep(atraso)
        _repor_pendentes()
    except asyncio.CancelledError:
        raise
    except Exception as e:                              # noqa: BLE001
        log_ded.warning(f"⚠️ curadoria: repor pendentes ({type(e).__name__}: {e}) — a "
                        f"próxima rodada tenta de novo")
    finally:
        _repor_marcado = False


def _repor_pendentes() -> None:
    """Banco de volta: grava os avais que ficaram na memória e devolve ao
    ponto de entrada (prova `reavaliando`) as versões bloqueadas — o portão
    as avalia de novo. Banco ainda fora: tenta na próxima rodada. O que
    passou da janela sai da memória sem ir ao canal."""
    agora = time.time()
    for k, p in list(_PENDENTES.items()):
        if agora > p["limite"]:
            _PENDENTES.pop(k, None)
            _SEGURADAS.pop(k, None)
            log_ded.error(f"⌛ [CURADORIA_PENDENTE_EXPIROU] {_nome(k[0])} id={k[1]} — a "
                          f"janela fechou com ela só na memória; NÃO vai ao canal")
    for k, (chaves, cat, ts_aval, parcial) in list(_AVAIS_PENDENTES.items()):
        if ts_aval < agora - 2 * _janela() - _ATRASO_MAX_S:
            _AVAIS_PENDENTES.pop(k, None)
            continue
        soltas = db_curadoria_registrar_aval(chaves, k[0], k[1], ts_aval, _janela(),
                                             agora - _janela() - _ATRASO_MAX_S, agora,
                                             parcial=parcial)
        if soltas is None:
            return                  # banco ainda fora
        _AVAIS_PENDENTES.pop(k, None)
        log_ded.info(f"✅ [CURADORIA_AVAL_GRAVADO] {_nome(k[0])} id={k[1]} produtos={chaves} "
                     f"— estava só na memória; solta="
                     f"{[f'{_nome(c)}/{m}' for c, m, _, _ in soltas]}")
        _soltar(soltas, k[0], agora)
    for k, p in list(_PENDENTES.items()):
        tipo = EDICAO if (p["is_edit"] and destino_vivo_de_origem(k[0], k[1])) else NOVA
        ok = db_curadoria_reavaliar(k[0], k[1], tipo, p["cat"], p["ts_msg"], agora)
        if ok is None:
            return                  # banco ainda fora
        _PENDENTES.pop(k, None)
        if ok:
            log_ded.info(f"🔁 [CURADORIA_REAVALIA] {_nome(k[0])} id={k[1]} — o banco voltou; "
                         f"a versão bloqueada volta ao portão")
            _agendar(_liberar(k[0], k[1], p["cat"], tipo))
        else:
            _SEGURADAS.pop(k, None)


# ─────────────────────────────────────────────────────────────────
# CANAL DE AVAL (Nerd Ofertas) — só leitura
# ─────────────────────────────────────────────────────────────────
async def _ao_aval(event) -> None:
    """Handler NewMessage e MessageEdited dos canais de aval: o aval NA HORA
    (o Telegram só entrega o update se a conta segue o canal). Só agenda e
    volta: nada daqui entra no funil de publicação."""
    try:
        _agendar(_observar_aval(int(event.chat_id), event.message))
    except Exception as e:                              # noqa: BLE001
        log_ded.warning(f"⚠️ curadoria: aval do handler ({type(e).__name__})")


def _versao(message) -> int:
    """A versão da mensagem: 0 a original; o edit_date (s) de cada edição."""
    editada = getattr(message, "edit_date", None)
    return int(_ts(editada)) if editada else 0


async def _observar_aval(chat_id: int, message, repeticao: bool = False) -> None:
    """Uma VERSÃO de mensagem do canal de aval (a original ou uma edição):
    categoria vigiada + produto identificado (sem afiliar) → aval do
    conteúdo ATUAL. Cada versão é olhada uma vez (handler e varredura podem
    trazer a mesma); a edição é uma versão nova e é relida inteira — o
    produto que permanece conserva a data, o acrescentado entra pelo
    edit_date, nenhum aval anterior é duplicado (database_curadoria). Se a
    identificação FALHOU (rede, encurtador que não abriu), ela é refeita
    depois de _REPETIR_S. Nada desta mensagem entra no funil de
    publicação."""
    if chat_id not in _estado["avais"]:
        return
    chave = (chat_id, int(getattr(message, "id", 0) or 0), _versao(message))
    if chave[2] < _ultima_versao.get(chave[:2], -1):
        log_ded.info(f"🎯 [CURADORIA_AVAL_VERSAO_VELHA] {_nome(chat_id)} id={chave[1]} — "
                     f"update atrasado de versão anterior à já lida; ignorado")
        return
    _ultima_versao[chave[:2]] = chave[2]
    if not repeticao:
        if chave in _vistos:
            return
        _vistos[chave] = None
        if len(_vistos) > _VISTOS_MAX:
            for k in list(_vistos)[:_VISTOS_MAX // 2]:
                _vistos.pop(k, None)
            for k in list(_ultima_versao)[:len(_ultima_versao) // 2]:
                _ultima_versao.pop(k, None)
    try:
        # A versão vale pela data DELA (a edição, pelo edit_date). Idade
        # negativa = relógio local atrás do Telegram: é recente.
        ref = getattr(message, "edit_date", None) or getattr(message, "date", None)
        if _idade_seg(ref) > _janela() + _ATRASO_MAX_S:
            return                  # aval velho não solta nada
        texto = ingestao.texto_de(message)
        cat = _categoria_aval(texto)
        if not cat:
            return
        chaves, incerto = await _identificar(ingestao.links_de(texto))
        n = _tentativas.get(chave, 0)
        repete = incerto and n < len(_REPETIR_S)
        if repete:
            _tentativas[chave] = n + 1
            log_ded.info(f"🎯 [CURADORIA_AVAL_REPETE] {_nome(chat_id)} id={chave[1]} "
                         f"cat={cat} — identificação falhou{' em parte' if chaves else ''}; "
                         f"nova tentativa em {_REPETIR_S[n]:g}s")
            _agendar(_repetir_aval(chat_id, message, _REPETIR_S[n]))
        else:
            _tentativas.pop(chave, None)
        if not chaves:
            if not repete:
                log_ded.info(f"🎯 [CURADORIA_AVAL_SEM_PRODUTO] {_nome(chat_id)} "
                             f"id={chave[1]} cat={cat} — sem identidade exata de produto "
                             f"({sorted(config.CURADORIA_LOJAS_AVAL)}"
                             f"{', identificação falhou' if incerto else ''}); não é aval")
            return
        # Leitura que falhou em parte: o produto não lido não é dado como
        # removido da mensagem do avalista.
        _registrar_aval(str(chat_id), chave[1], chaves, cat, _ts(ref), parcial=incerto)
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
    """A cada _VARREDURA_S: devolve o que ficou na memória com o banco fora,
    lê os canais de aval (não depende de a conta seguir o canal nem de o
    Telegram entregar o update), reentrega as reinjeções paradas e expira
    as seguradas que passaram da janela (depois da leitura)."""
    while True:
        await asyncio.sleep(_VARREDURA_S)
        if g._encerrando or not _aberto:
            return
        try:
            _repor_pendentes()
        except Exception as e:                          # noqa: BLE001
            log_ded.warning(f"⚠️ curadoria: repor pendentes ({type(e).__name__})")
        # Primeiro LÊ o aval, depois expira: o aval desta rodada nunca
        # perde para a expiração da mesma rodada.
        for chat_id, ent in list(_estado["avais"].items()):
            if g._encerrando or not _aberto:
                return
            await _varrer_canal(chat_id, ent)
        try:
            _reentregar_presas()
        except Exception as e:                          # noqa: BLE001
            log_ded.warning(f"⚠️ curadoria: reentregar ({type(e).__name__})")
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
        # A 1ª página é SEMPRE a mais recente (as últimas _LOTE mensagens):
        # traz o que é novo E a edição de mensagem já vista — a versão
        # editada é outra versão e é relida. As páginas seguintes só vêm
        # quando a 1ª é toda nova (rajada maior que _LOTE).
        lote: list = []
        teto = None
        for _ in range(_PAGINAS):
            kw = {"limit": _LOTE}
            if teto is not None:
                kw["max_id"] = teto
                if maior is not None:
                    kw["min_id"] = maior
            pagina = [m for m in (await cliente.get_messages(ent, **kw) or [])
                      if m is not None]
            lote += pagina
            if (maior is None or len(pagina) < _LOTE
                    or min(m.id for m in pagina) <= maior):
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
    velho = time.monotonic() - (_janela() + _ATRASO_MAX_S + _PRAZO_LIBERACAO_S + _PRESA_S
                                * _MAX_REENTREGAS)
    for k in [k for k, (_m, t) in _SEGURADAS.items() if t < velho and k not in _PENDENTES]:
        _SEGURADAS.pop(k, None)
        _reentregas.pop(k, None)
    for chat, mid, cat, tipo in db_curadoria_expirar(agora - _janela() - _ATRASO_MAX_S, agora):
        _soltar_memoria((chat, mid))
        if sombra:
            log_ded.info(f"👻 [CURADORIA_SOMBRA] {_nome(chat)} id={mid} cat={cat} — "
                         f"{_janela():g}s sem aval: no ATIVO não iria ao canal")
        elif tipo == EDICAO:
            log_ded.info(f"⌛ [CURADORIA_EDICAO_EXPIROU] {_nome(chat)} id={mid} cat={cat} — "
                         f"{_janela():g}s sem aval do Fada/Nerd: a alteração pendente não "
                         f"vai ao ar; o post segue com o produto aprovado")
        else:
            log_ded.info(f"⌛ [CURADORIA_EXPIROU] {_nome(chat)} id={mid} cat={cat} — "
                         f"{_janela():g}s sem aval do Fada/Nerd; não vai ao canal")
