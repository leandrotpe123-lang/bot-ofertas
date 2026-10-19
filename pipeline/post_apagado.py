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
         os posts vivos (até 100 ids, uma chamada): o que volta vazio DUAS
         vezes seguidas (a 2ª só pelos sumidos) foi apagado. Cobre o aviso
         perdido (deploy, restart, rede).
     O que o PRÓPRIO worker apagou (substituição, remoção da fusão) está
     anotado em pipeline.apagados_pelo_worker e é ignorado; post que não
     está vivo no banco (fundido, encerrado, remoção marcada) também.
  2. POST QUE FICOU (`sobrevivente`): só para post de cupom sem código
     (todas as âncoras `<plat>|cupb|…`, a classe das duplicatas medidas),
     entre os posts `cupb` vivos da mesma plataforma, de OUTRA fonte,
     nascidos a até CUPOM_RAJADA_S do apagado e sem nome ou benefício em
     conflito (cupom_lancamento.compativeis). Um único com benefício igual
     vence; senão, um único compatível. Mais de um → ambíguo, sem fusão.
  3. Sob os locks de POST (ordem crescente de msg_id), relido:
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
           "iniciar_conferencia"]

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

_TAREFAS: set = set()


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


async def _um(apagado: int) -> tuple:
    """(situação, sobrevivente, motivo) de UM id apagado no destino. O
    filtro do worker roda antes (atalho barato) e de novo sob o lock: a
    substituição anota o id e apaga segurando o lock do post."""
    if apagados_pelo_worker.foi_o_worker(apagado):
        return "do_worker", None, ""
    est = db_get_post(apagado)
    if not est:
        return "sem_post", None, ""
    if est.get("fused_into") or est.get("delete_status"):
        return "ja_removido", None, ""
    agora = time.time()
    sob, motivo = (sobrevivente(apagado, est, agora)
                   if viva(float(est.get("janela_fim") or 0.0), agora)
                   else (None, "fora_da_vida"))
    envolvidos = sorted({apagado} | ({sob} if sob else set()))
    async with contextlib.AsyncExitStack() as pilha:
        for mid in envolvidos:                       # ordem CRESCENTE
            await pilha.enter_async_context(await exclusao.lock_post(mid))
        if apagados_pelo_worker.foi_o_worker(apagado):
            return "do_worker", None, ""            # anotado enquanto esperava
        agora = time.time()
        if sob is not None:
            e = db_get_post(sob)
            if (e and not e.get("fused_into") and not e.get("delete_status")
                    and viva(float(e.get("janela_fim") or 0.0), agora)
                    and db_fundir_posts(sob, [apagado], agora)):
                return "fundido", sob, motivo
            motivo = "sobrevivente_mudou"
        res = db_silenciar_post_apagado(apagado, agora)
        return res, None, motivo


# ─────────────────────────────────────────────────────────────────
# Conferência (cobre o aviso perdido)
# ─────────────────────────────────────────────────────────────────
async def conferir() -> list:
    """Um ciclo: pergunta ao Telegram pelos posts vivos; os que voltam
    vazios (e não foram apagados pelo worker) vão ao tratamento."""
    ids = db_posts_vivos_recentes(time.time(), _LOTE)
    if not ids:
        return []
    from client import client
    msgs = await client.get_messages(config.GRUPO_DESTINO, ids=ids)
    sumidos = [mid for mid, m in zip(ids, msgs or []) if m is None]
    if not sumidos:
        return []
    # Confirmação: um vazio isolado (rede, Telegram) fundiria um post que
    # está no ar. Só age no que continua vazio na 2ª pergunta. O que o
    # worker apagou é filtrado no tratamento (_um), sob o lock.
    await asyncio.sleep(_CONFIRMAR_S)
    de_novo = await client.get_messages(config.GRUPO_DESTINO, ids=sumidos)
    confirmados = [mid for mid, m in zip(sumidos, de_novo or []) if m is None]
    if not confirmados:
        return []
    return await apagados(confirmados, "CONFERENCIA")


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
    Nasce em contexto vazio (vive mais que quem a criou)."""
    return asyncio.get_running_loop().create_task(_laco(),
                                                  context=contextvars.Context())
