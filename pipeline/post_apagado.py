"""
Entrada — o DONO APAGOU um post do canal de destino (frente M).

POR QUE EXISTE (produção):
  · 15/10: o dono apagou à mão o 29156 (duplicata do 29152, relâmpago ML)
    e o 29161 (duplicata do 29160, "Renovados" Shopee).
  · 16, 17 e 18/10: toda noite o mesmo — o relâmpago ML de 20% saiu em dois
    posts e o dono apagou o da Promotom (29533, 29631, 29716).
  · 18/10 22:37: o Samuel 121892 trouxe o mesmo cupom e o worker o pôs no
    29716 — que já não existia: DUP_SILENCIOSO num post invisível e 6 ×
    "❌ edit … message ID is invalid" (UPGRADE_MIDIA_FALHOU). O worker só
    escutava exclusão nas FONTES (pipeline.origem_apagada).

DECISÃO DO DONO (16/10): quando ele apaga a duplicata, a mensagem seguinte
da mesma campanha vai para o POST QUE FICOU — não abre post novo — e o
espelho do post apagado no canal de cupons sai junto.

COMO (nenhum caminho de escrita novo):
  1. DETECÇÃO, duas vias que caem no mesmo tratamento:
       · TELEGRAM — handler MessageDeleted só sobre o canal de destino; só
         AGENDA uma task (o despacho do Telethon nunca espera lock/banco);
       · CONFERENCIA — de tempos em tempos, um get_messages em lote sobre
         os posts vivos (até 100 ids, uma chamada): o que falta DUAS vezes
         seguidas (a 2ª só pelos sumidos) foi apagado. Cobre o aviso
         perdido (deploy, restart, rede).
       · SUSPEITA — o I/O do principal viu a mensagem sumida
         (MessageIdInvalid na edição; delete da substituição sem prova):
         pipeline.apagados_pelo_worker.suspeitar agenda a MESMA conferência
         por id, só daquele post. A suspeita nunca decide sozinha.
     A conferência compara por ID (conjuntos), nunca por posição: o
     Telethon 1.45.0 avisa que o Telegram pode omitir ids inválidos da
     resposta. Exceção ou resposta malformada não prova ausência.
     MASSA: lote que confirma mais de _LIMITE_EM_MASSA (5) sumidos não
     muda NADA (nem parcial) — ❌ [CONFERENCIA_EM_MASSA] com ids e via, e
     os ids vão para a QUARENTENA (fora do lote). Reavaliação a partir do
     ciclo seguinte: um id por vez (a mesma resposta em lote nunca conta
     de novo), só com controle positivo no ciclo (o lote achou post no
     ar) e no máximo 5 mudanças por ciclo. Sem controle (canal ilegível,
     id errado), espera; sai sozinha quando o post deixa de estar vivo. A
     trava é da CONFERENCIA: o aviso do Telegram (exclusão explícita) e a
     suspeita unitária seguem por id.
     O que o PRÓPRIO worker apagou (substituição, remoção da fusão) está
     anotado em pipeline.apagados_pelo_worker e é ignorado; post que não
     está vivo no banco (fundido, encerrado, remoção marcada) também.
  2. POST QUE FICOU (`sobrevivente`): só para post de cupom sem código
     (todas as âncoras `<plat>|cupb|…`, a classe das duplicatas medidas),
     entre os posts `cupb` vivos da mesma plataforma, de OUTRA fonte,
     nascidos a até CUPOM_RAJADA_S do apagado e sem nome ou benefício em
     conflito (cupom_lancamento.compativeis). Um único com benefício igual
     vence; senão, um único compatível. Mais de um → ambíguo, sem fusão.
  3. Sob os locks de POST (ordem crescente de msg_id), relido e DECIDIDO DE
     NOVO (o item 2 inteiro; a escolha fora dos locks só diz quais locks
     pedir). Mudou o candidato e a prova não vale mais → não funde:
       · com sobrevivente → database.db_fundir_posts (a fusão de sempre:
         âncoras e origens vão para o que ficou; a próxima mensagem da
         campanha chega a ele pelo vínculo ou pela família);
       · sem sobrevivente → database.db_silenciar_post_apagado: APAGADO
         FICA APAGADO até o fim da vida. O post segue dono da identidade e
         das origens, sem nada exibido; a publicação absorve o que chegar
         para ele (edição da fonte, mesma oferta de outra fonte) sem tocar
         o canal. Ele não entra em adoção, regra de cupom nem convergência:
         outra versão da campanha (outra identidade) publica normalmente.
         Esquecer a identidade, como na exclusão pela FONTE, faria a
         edição da própria fonte trazer o post apagado de volta.
  4. Fora dos locks: a mensagem já não existe no canal — nenhuma chamada ao
     Telegram para o principal; status 'ok' e o espelho sai pelo caminho de
     sempre (espelho_cupons.removido); a mídia aceita é solta.

LIMITES: o aviso de exclusão não é garantido (por isso a conferência); a
exclusão de um post fora da vida só tira o espelho; a fonte que apaga e
reposta a mesma oferta enquanto o post silenciado vive é absorvida (o dono
já tinha apagado o post).
"""
from __future__ import annotations

import asyncio
import contextlib
import contextvars
import time
from typing import Optional

from telethon.errors import FloodWaitError

import config
import globals as g
from database import (db_exibida, db_fundir_posts, db_get_post,
                      db_ofertas_de_post, db_origens_do_post,
                      db_posts_vivos_com_prefixo, db_posts_vivos_recentes,
                      db_set_delete_status, db_silenciar_post_apagado)
from logger import log_out
from pipeline import (apagados_pelo_worker, cupom_lancamento, espelho_cupons,
                      exclusao)
from pipeline.resolucao_identidade import prefixo_cupom_sem_codigo
from pipeline.vida_oferta import VIDA_OFERTA_S, viva

__all__ = ["instalar", "agendar", "apagados", "sobrevivente", "conferir",
           "confirmar", "suspeito", "registrar_suspeita", "iniciar_conferencia"]

# Conferência do destino: um get_messages a cada _CONFERIR_S, até _LOTE ids
# (o teto da chamada). O 1º ciclo sai logo depois do boot (cobre o que se
# perdeu na janela do deploy). FloodWait adia o próximo ciclo.
_CONFERIR_S = 10 * 60
_PRIMEIRA_S = 60
_LOTE = 100
_FLOOD_MAX_S = 30 * 60
# Um "vazio" isolado do get_messages nunca mexe no banco: a conferência
# pergunta de novo, só pelos sumidos, depois desta pausa.
_CONFIRMAR_S = 2.0
# A escolha do post que ficou é refeita sob os locks; se apontar outro post
# (cujo lock não temos), recomeça — no máximo isto, depois silencia.
_TENTATIVAS = 3
# Conferência em LOTE que confirma mais que isto de sumidos não muda nada:
# quarentena (ver conferir/_reavaliar). Também o teto de mudanças por ciclo
# da reavaliação. Aviso do Telegram e suspeita unitária não passam por ela.
_LIMITE_EM_MASSA = 5

_TAREFAS: set = set()
_EM_VOO: set = set()          # ids com verificação por suspeita em curso
_QUARENTENA: dict = {}        # id → ciclo da conferência em que entrou
_CICLO = {"n": 0}             # ciclos de conferência deste processo


# ─────────────────────────────────────────────────────────────────
# Detecção
# ─────────────────────────────────────────────────────────────────
def instalar(cliente, destino) -> None:
    """Liga o handler de exclusão SÓ sobre o canal de destino (entidade
    resolvida). Uma vez por processo (main._preparar_processo)."""
    from telethon import events

    async def on_delete_destino(event):
        try:
            agendar(event.deleted_ids, "TELEGRAM")
        except Exception as e:                      # noqa: BLE001
            log_out.error(f"❌ on_delete_destino: {e}", exc_info=True)

    cliente.add_event_handler(on_delete_destino,
                              events.MessageDeleted(chats=[destino]))


def agendar(ids, via: str) -> None:
    """Agenda o tratamento numa task própria (contexto vazio) e volta."""
    if g._encerrando or not ids:
        return
    t = asyncio.get_running_loop().create_task(
        apagados(list(ids), via), context=contextvars.Context())
    _TAREFAS.add(t)
    t.add_done_callback(_TAREFAS.discard)


# ─────────────────────────────────────────────────────────────────
# Post que ficou (puro sobre leituras do banco)
# ─────────────────────────────────────────────────────────────────
def _fontes(mid: int) -> frozenset:
    return frozenset(str(c) for c, _m in db_origens_do_post(mid))


def _plataforma_cupb(mid: int) -> str:
    return cupom_lancamento.candidato(set(db_ofertas_de_post(mid)) | db_exibida(mid))


def sobrevivente(apagado: int, est: dict, agora: float) -> tuple:
    """(post que ficou ou None, motivo). Ver o item 2 do cabeçalho."""
    plat = _plataforma_cupb(apagado)
    rajada = config.CUPOM_RAJADA_S.get(plat) if plat else None
    if rajada is None:
        return None, "fora_do_escopo"
    nasc = float(est.get("janela_fim") or 0.0) - VIDA_OFERTA_S
    fontes = _fontes(apagado)
    perfil = cupom_lancamento.perfil(est.get("texto") or "", plat)
    iguais, compativeis = [], []
    for mid in db_posts_vivos_com_prefixo(prefixo_cupom_sem_codigo(plat)):
        if mid == apagado:
            continue
        e = db_get_post(mid)
        if (not e or e.get("fused_into") or e.get("delete_status")
                or not viva(float(e.get("janela_fim") or 0.0), agora)):
            continue
        if _plataforma_cupb(mid) != plat:
            continue
        if abs(float(e["janela_fim"]) - VIDA_OFERTA_S - nasc) > rajada:
            continue
        if fontes & _fontes(mid):
            continue                    # mesma fonte: não é duplicata entre grupos
        rel = cupom_lancamento.compativeis(
            perfil, cupom_lancamento.perfil(e.get("texto") or "", plat))
        if rel == "igual":
            iguais.append(mid)
        elif rel == "compativel":
            compativeis.append(mid)
    if len(iguais) == 1:
        return iguais[0], "mesmo_lancamento"
    if iguais:
        return None, "ambiguo"
    if len(compativeis) == 1:
        return compativeis[0], "mesmo_momento"
    return None, ("ambiguo" if compativeis else "sem_par")


# ─────────────────────────────────────────────────────────────────
# Tratamento
# ─────────────────────────────────────────────────────────────────
async def apagados(ids, via: str = "TELEGRAM") -> list:
    """Trata cada id apagado no destino. Devolve [(post, situação,
    sobrevivente)] dos que mudaram o banco."""
    feitos = []
    for mid in ids:
        try:
            mid = int(mid)
            situacao, sob, motivo = await _um(mid)
        except asyncio.CancelledError:
            raise
        except Exception as e:                          # noqa: BLE001
            log_out.error(f"❌ post apagado {mid}: {e}", exc_info=True)
            continue
        if situacao not in ("fundido", "silenciado", "fora_da_vida"):
            log_out.debug(f"🗑 [APAGADO_IGNORADO] post:{mid} situacao={situacao} via={via}")
            continue
        # ── fora de todos os locks: a mensagem já não existe no canal ──
        g.midia_aceita_drop(mid)
        db_set_delete_status(mid, "ok")
        espelho_cupons.removido(mid)
        if situacao == "fundido":
            log_out.info(f"🗑 [APAGADO_DONO] post:{mid} → post:{sob} "
                         f"motivo={motivo} via={via} — a campanha segue no que ficou")
        else:
            log_out.info(f"🗑 [APAGADO_DONO] post:{mid} {situacao} "
                         f"motivo={motivo} via={via} — fica apagado até o fim da vida")
        feitos.append((mid, situacao, sob))
    return feitos


def _decidir(apagado: int, est: dict, agora: float) -> tuple:
    """(post que ficou ou None, motivo) para o apagado no estado `est`."""
    if not viva(float(est.get("janela_fim") or 0.0), agora):
        return None, "fora_da_vida"
    return sobrevivente(apagado, est, agora)


async def _um(apagado: int) -> tuple:
    """(situação, sobrevivente, motivo) de UM id apagado no destino.

    A escolha fora dos locks é só para saber QUAIS locks pedir. Sob eles
    (ordem crescente de msg_id) tudo é relido e a decisão inteira refeita
    — sobrevivente() de novo: plataforma/cupb, benefício, nome, outra
    fonte, rajada e unicidade. Entre a releitura e a escrita não há await
    (uma conexão por processo, um loop): nada intercala. Funde só se a
    decisão sob os locks é a MESMA; se a prova sumiu (sem par, ambíguo),
    silencia; se apontou outro post (cujo lock não temos), recomeça — até
    _TENTATIVAS vezes, depois silencia. O filtro do worker roda antes
    (atalho barato) e de novo sob o lock: a substituição anota o id e
    apaga segurando o lock do post."""
    for tentativa in range(1, _TENTATIVAS + 1):
        if apagados_pelo_worker.foi_o_worker(apagado):
            return "do_worker", None, ""
        est = db_get_post(apagado)
        if not est:
            return "sem_post", None, ""
        if est.get("fused_into") or est.get("delete_status"):
            return "ja_removido", None, ""
        sob, _motivo = _decidir(apagado, est, time.time())
        envolvidos = sorted({apagado} | ({sob} if sob else set()))
        async with contextlib.AsyncExitStack() as pilha:
            for mid in envolvidos:                   # ordem CRESCENTE
                await pilha.enter_async_context(await exclusao.lock_post(mid))
            # ── sob os locks: relido e decidido de novo, sem await ──
            if apagados_pelo_worker.foi_o_worker(apagado):
                return "do_worker", None, ""        # anotado enquanto esperava
            est = db_get_post(apagado)
            if not est:
                return "sem_post", None, ""
            if est.get("fused_into") or est.get("delete_status"):
                return "ja_removido", None, ""
            agora = time.time()
            sob2, motivo = _decidir(apagado, est, agora)
            if sob2 is not None and sob2 == sob:
                if db_fundir_posts(sob, [apagado], agora):
                    return "fundido", sob, motivo
                motivo = "fusao_recusada"
            elif sob2 is not None:
                if tentativa < _TENTATIVAS:
                    continue                        # outro post: recomeça com os locks dele
                motivo = "sobrevivente_instavel"
            elif sob is not None:
                motivo = f"sobrevivente_mudou:{motivo}"   # a prova não vale mais
            return db_silenciar_post_apagado(apagado, agora), None, motivo
    return "erro", None, "tentativas"               # inalcançável: a última tentativa decide


# ─────────────────────────────────────────────────────────────────
# Conferência (cobre o aviso perdido)
# ─────────────────────────────────────────────────────────────────
def _presentes(msgs) -> Optional[set]:
    """Ids que o Telegram devolveu. None = resposta malformada (não lista,
    ou item sem id inteiro): não prova nada. `None` na lista é o
    MessageEmpty do Telethon — o id simplesmente não entra."""
    if not isinstance(msgs, (list, tuple)):
        return None
    ids = set()
    for m in msgs:
        if m is None:
            continue
        i = getattr(m, "id", None)
        if not isinstance(i, int) or isinstance(i, bool):
            return None
        ids.add(i)
    return ids


async def _perguntar(ids: list, via: str) -> tuple:
    """(confirmados, presentes). Confirmado = FALTA duas vezes seguidas (a
    2ª pergunta só pelos que faltaram). confirmados None = resposta
    malformada: não prova nada. Exceção sobe a quem chamou."""
    from client import client
    presentes = _presentes(await client.get_messages(config.GRUPO_DESTINO, ids=ids))
    if presentes is None:
        log_out.warning(f"⚠️ [CONFERENCIA_DESTINO] resposta malformada via={via} — nada muda")
        return None, set()
    sumidos = [i for i in ids if i not in presentes]
    if not sumidos:
        return [], presentes
    # Um "falta" isolado (rede, Telegram) fundiria um post que está no ar.
    # O que o worker apagou é filtrado no tratamento (_um), sob o lock.
    await asyncio.sleep(_CONFIRMAR_S)
    de_novo = _presentes(await client.get_messages(config.GRUPO_DESTINO, ids=sumidos))
    if de_novo is None:
        log_out.warning(f"⚠️ [CONFERENCIA_DESTINO] resposta malformada via={via} — nada muda")
        return None, presentes
    return [i for i in sumidos if i not in de_novo], presentes | de_novo


async def confirmar(ids, via: str) -> list:
    """Pergunta ao Telegram por `ids` e só trata o que FALTA duas vezes
    seguidas. Exceção sobe a quem chamou e nada muda; resposta malformada
    não muda nada. Caminho da SUSPEITA (um id): sem a trava de massa."""
    ids = [int(i) for i in ids]
    if not ids:
        return []
    confirmados, _presentes_ = await _perguntar(ids, via)
    if not confirmados:
        return []
    return await apagados(confirmados, via)


# ─────────────────────────────────────────────────────────────────
# Conferência em lote: trava de MASSA e quarentena
# ─────────────────────────────────────────────────────────────────
def _quarentenar(ids: list, via: str) -> None:
    for i in ids:
        _QUARENTENA.setdefault(i, _CICLO["n"])
    log_out.error(f"❌ [CONFERENCIA_EM_MASSA] {len(ids)} sumidos > {_LIMITE_EM_MASSA} "
                  f"via={via} ids={sorted(ids)} — nada muda; quarentena "
                  f"(reavaliação um id por vez, com controle positivo)")


def _limpar_quarentena(agora: float) -> None:
    """Sai da quarentena o que já não está vivo no banco (tratado por
    outra via — aviso do Telegram, suspeita —, fundido ou fim da vida)."""
    for mid in list(_QUARENTENA):
        e = db_get_post(mid)
        if (not e or e.get("fused_into") or e.get("delete_status")
                or not viva(float(e.get("janela_fim") or 0.0), agora)):
            _QUARENTENA.pop(mid, None)


async def _reavaliar(controle_ok: bool) -> list:
    """Quarentena. Cada id é perguntado SOZINHO (get_messages só dele, duas
    vezes) — nunca pelo lote que o acusou —, a partir do ciclo seguinte ao
    da quarentena, só com controle positivo neste ciclo (o lote achou ao
    menos um post no ar: canal legível, ids certos) e no máximo
    _LIMITE_EM_MASSA mudanças por ciclo. No ar → sai sem mudar nada."""
    pendentes = sorted(i for i, c in _QUARENTENA.items() if c < _CICLO["n"])
    if not pendentes:
        return []
    if not controle_ok:
        log_out.warning(f"⚠️ [CONFERENCIA_EM_MASSA] quarentena={sorted(_QUARENTENA)} "
                        f"sem controle positivo neste ciclo — nada muda")
        return []
    feitos, mudancas = [], 0
    for mid in pendentes:
        if mudancas >= _LIMITE_EM_MASSA:
            break
        confirmados, _p = await _perguntar([mid], "QUARENTENA")
        if confirmados is None:
            continue                                # malformada: fica para o próximo ciclo
        _QUARENTENA.pop(mid, None)
        if not confirmados:
            log_out.info(f"🔎 [CONFERENCIA_EM_MASSA] post:{mid} está no ar — sai da quarentena")
            continue
        mudancas += 1
        feitos += await apagados([mid], "QUARENTENA")
    return feitos


async def conferir() -> list:
    """Um ciclo: confere por id, num lote, os posts vivos mais recentes
    (fora da quarentena). Lote que CONFIRMA mais de _LIMITE_EM_MASSA
    sumidos não muda nada — nem parcial: todos vão para a quarentena antes
    de qualquer ação de banco ou espelho. Depois, a reavaliação."""
    _CICLO["n"] += 1
    agora = time.time()
    _limpar_quarentena(agora)
    lote = [i for i in db_posts_vivos_recentes(agora, _LOTE + len(_QUARENTENA))
            if i not in _QUARENTENA][:_LOTE]
    feitos, controle_ok = [], False
    if lote:
        confirmados, presentes = await _perguntar(lote, "CONFERENCIA")
        if confirmados is not None and len(confirmados) > _LIMITE_EM_MASSA:
            _quarentenar(confirmados, "CONFERENCIA")
        elif confirmados is not None:
            controle_ok = bool(presentes)
            if confirmados:
                feitos = await apagados(confirmados, "CONFERENCIA")
    if _QUARENTENA:
        feitos += await _reavaliar(controle_ok)
    return feitos


def suspeito(msg_id_dest, via: str) -> None:
    """Sinal do I/O (apagados_pelo_worker.suspeitar): agenda a conferência
    por id SÓ deste post, numa task própria (contexto vazio), sem repetir
    o mesmo id em voo. Nunca decide nem levanta."""
    try:
        mid = int(msg_id_dest)
    except (TypeError, ValueError):
        return
    if g._encerrando or mid in _EM_VOO:
        return
    _EM_VOO.add(mid)
    t = asyncio.get_running_loop().create_task(
        _verificar(mid, via), context=contextvars.Context())
    _TAREFAS.add(t)
    t.add_done_callback(_TAREFAS.discard)


async def _verificar(mid: int, via: str) -> None:
    try:
        await confirmar([mid], via)
    except asyncio.CancelledError:
        raise
    except Exception as e:                              # noqa: BLE001
        log_out.warning(f"⚠️ [CONFERENCIA_DESTINO] post:{mid} via={via} "
                        f"{type(e).__name__}: {e} — nada muda")
    finally:
        _EM_VOO.discard(mid)


def registrar_suspeita() -> None:
    """Liga o sinal de suspeita do I/O à conferência por id."""
    apagados_pelo_worker.ao_suspeitar(suspeito)


async def _laco() -> None:
    espera = _PRIMEIRA_S
    while not g._encerrando:
        await asyncio.sleep(espera)
        espera = _CONFERIR_S
        try:
            await conferir()
        except asyncio.CancelledError:
            raise
        except FloodWaitError as e:
            espera = min(max(float(e.seconds), _CONFERIR_S), _FLOOD_MAX_S)
            log_out.warning(f"⚠️ [CONFERENCIA_DESTINO] FloodWait {e.seconds}s")
        except Exception as e:                          # noqa: BLE001
            log_out.warning(f"⚠️ [CONFERENCIA_DESTINO] {type(e).__name__}: {e}")


def iniciar_conferencia() -> Optional[asyncio.Task]:
    """A task da conferência — UMA por processo (main._preparar_processo).
    Nasce em contexto vazio (vive mais que quem a criou). Liga também o
    sinal de suspeita do I/O (roda sempre no boot, mesmo se o handler do
    destino não instalar)."""
    registrar_suspeita()
    return asyncio.get_running_loop().create_task(_laco(),
                                                  context=contextvars.Context())
