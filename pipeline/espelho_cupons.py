"""
Espelho de cupons — o post de CUPOM também no canal de cupons.

Responsabilidade ÚNICA: manter, no canal de cupons (CANAL_CUPONS), uma
cópia fiel de cada post do canal principal cuja composição EXIBIDA é de
cupom (resolucao_identidade.eh_identidade_cupom: cupom com ou sem código
e nenhum produto). É PROJEÇÃO do canal principal:
  - não decide nada: texto, mídia, família e score continuam decididos
    UMA vez, para o canal principal, sob os locks de sempre;
  - não toca identidade, família, convergência nem locks;
  - copia o que JÁ ESTÁ NO AR no principal — texto, formatação e mídia
    por referência (sem novo upload) — no momento em que processa. A
    classificação lê post_exibida (o que o post mostra, nunca memória).

VELOCIDADE: o canal principal nunca espera o espelho. Os aplicadores só
NOTIFICAM (put_nowait, sem await) DEPOIS de o post principal estar no
ar e gravado. Um trabalhador único e sequencial processa em ordem FIFO:
a cópia sai ~1 RTT depois do principal. Falha do espelho (rede,
FloodWait, permissão) nunca afeta o principal — é registrada e segue.

CICLO DE VIDA espelhado: publicação/renascimento (cria), evolução,
sincronização do líder e upgrade de mídia (edita), substituição
apagar+reenviar (edita o MESMO espelho e o reaponta para o id novo),
fusão (apaga). Post que deixa de ser cupom perde o espelho; post que
passa a ser cupom ganha.

LIGADO por CANAL_CUPONS (ex.: FullPromotionCupons, @FullPromotionCupons
ou -100…). Ausente/vazia: DESLIGADO — notificações são no-op e nenhuma
task é criada (comportamento idêntico ao de antes).
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import time
from typing import Optional

from telethon.errors import FloodWaitError, MessageNotModifiedError

import config
from config import GRUPO_DESTINO
from database import (db_espelho_del, db_espelho_get, db_espelho_mover,
                      db_espelho_set, db_exibida)
from logger import log_out, log_sys
from pipeline.resolucao_identidade import eh_identidade_cupom

__all__ = ["canal_configurado", "iniciar", "encerrar", "publicado",
           "conteudo", "substituido", "removido"]

_VARIAVEL = "CANAL_CUPONS"
_FILA_MAX = 1000          # ~14 cupons/dia medidos: folga de ordens de grandeza
_TENTATIVAS = 3           # por operação, só para falha transitória
_FLOOD_MAX_S = 300        # FloodWait acima disso: a operação é abandonada
_DRENO_S = 5.0            # no shutdown, espera a fila esvaziar até isto

_canal = None             # destino do espelho (str/int); None = desligado
_fila: Optional[asyncio.Queue] = None


def canal_configurado():
    """Canal de cupons configurado (username sem @ ou id numérico), ou
    None quando desligado."""
    bruto = (os.environ.get(_VARIAVEL) or "").strip()
    if not bruto:
        return None
    if bruto.lstrip("-").isdigit():
        return int(bruto)
    return bruto.lstrip("@") or None


def iniciar() -> Optional[asyncio.Task]:
    """Cria o trabalhador se CANAL_CUPONS estiver ligada. Devolve a task
    (para o shutdown) ou None quando desligado. Não bloqueia."""
    global _canal, _fila
    canal = canal_configurado()
    if canal is None:
        _canal, _fila = None, None
        log_sys.info("🎟 ESPELHO_CUPONS|OFF")
        return None
    _canal, _fila = canal, asyncio.Queue(maxsize=_FILA_MAX)
    log_sys.info(f"🎟 ESPELHO_CUPONS|ATIVO|canal={canal}")
    return asyncio.get_running_loop().create_task(_laco(_fila))


async def encerrar(tarefa: Optional[asyncio.Task]) -> None:
    """Shutdown: dá até _DRENO_S para a fila esvaziar e cancela limpo."""
    global _fila
    fila, _fila = _fila, None          # novas notificações viram no-op
    if tarefa is None or tarefa.done():
        return
    if fila is not None and not fila.empty():
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(fila.join(), _DRENO_S)
        if not fila.empty():
            log_out.warning(
                f"⚠️ ESPELHO_CUPONS|SHUTDOWN|pendentes={fila.qsize()}")
    tarefa.cancel()
    await asyncio.gather(tarefa, return_exceptions=True)


# ── Notificações (chamadas pelos aplicadores; NUNCA aguardam) ─────
def publicado(msg_id_dest: int, mensagem) -> None:
    """Post NOVO no ar e gravado (publicação ou renascimento)."""
    _notificar("publicado", msg_id_dest, mensagem)


def conteudo(msg_id_dest: int, midia: bool = False) -> None:
    """O post mudou no lugar (evolução, sincronização, upgrade de mídia).
    `midia` = a imagem publicada mudou."""
    _notificar("conteudo", msg_id_dest, bool(midia))


def substituido(antigo: int, mensagem) -> None:
    """Apagar+reenviar: o post `antigo` agora é `mensagem` (id novo)."""
    _notificar("substituido", antigo, mensagem)


def removido(msg_id_dest: int) -> None:
    """O post saiu do canal principal (fusão)."""
    _notificar("removido", msg_id_dest, None)


def _notificar(tipo: str, msg_id_dest: int, extra) -> None:
    if _fila is None:
        return
    try:
        _fila.put_nowait((tipo, msg_id_dest, extra, time.monotonic()))
    except asyncio.QueueFull:
        log_out.warning(
            f"⚠️ ESPELHO_CUPONS|FILA_CHEIA|descartado={tipo}|post:{msg_id_dest}")


# ── Trabalhador único, sequencial ─────────────────────────────────
async def _laco(fila: asyncio.Queue) -> None:
    try:
        while True:
            tipo, mid, extra, t0 = await fila.get()
            try:
                await _processar(tipo, mid, extra, t0)
            except asyncio.CancelledError:
                raise
            except Exception as e:                     # noqa: BLE001
                log_out.warning(
                    f"⚠️ ESPELHO_CUPONS|FALHOU|{tipo}|post:{mid}|"
                    f"{type(e).__name__}: {e}")
            finally:
                fila.task_done()
    except asyncio.CancelledError:
        return


async def _processar(tipo: str, mid: int, extra, t0: float) -> None:
    if tipo == "removido":
        esp = db_espelho_get(mid)
        if esp is not None:
            await _apagar(mid, esp, "post_removido", t0)
        return

    if tipo == "substituido":
        antigo, principal = mid, extra
        mid = principal.id
        if db_espelho_get(antigo) is not None:
            db_espelho_mover(antigo, mid)
    elif tipo == "publicado":
        principal = extra
        if db_espelho_get(mid) is not None:
            return                                   # evento repetido
    else:
        principal = await _io(lambda c: c.get_messages(GRUPO_DESTINO, ids=mid))
        if principal is None:
            return                                   # saiu do principal

    cupom = eh_identidade_cupom(db_exibida(mid))
    esp = db_espelho_get(mid)
    if esp is None:
        if cupom:
            await _criar(mid, principal, t0)
        return
    if not cupom:
        await _apagar(mid, esp, "deixou_de_ser_cupom", t0)
        return
    midia = tipo == "substituido" or (tipo == "conteudo" and extra)
    await _editar(mid, esp, principal, midia, t0)


def _ms(t0: float) -> int:
    return round((time.monotonic() - t0) * 1000)


async def _criar(mid: int, principal, t0: float) -> None:
    copia = await _io(lambda c: _copiar(c, principal))
    db_espelho_set(mid, copia.id)
    log_out.info(
        f"🎟 ESPELHO_CUPONS|CRIADO|post:{mid}→espelho:{copia.id}|ms={_ms(t0)}")


async def _editar(mid: int, esp: int, principal, midia: bool, t0: float) -> None:
    arquivo = _midia_copiavel(principal) if midia else None
    try:
        await _io(lambda c: c.edit_message(
            _canal, esp, principal.message or "",
            formatting_entities=principal.entities or None, parse_mode=None,
            file=arquivo, link_preview=True))
    except MessageNotModifiedError:
        return
    except FloodWaitError:
        raise
    except Exception as e:                             # noqa: BLE001
        if arquivo is None:
            raise
        # Espelho nasceu sem mídia (o principal ganhou a primeira imagem
        # agora): a mesma limitação do Telegram do canal principal. O
        # espelho é refeito como cópia fiel do que está no ar.
        log_out.info(
            f"🎟 ESPELHO_CUPONS|REFAZ|post:{mid}|{type(e).__name__}")
        await _apagar(mid, esp, "refaz_com_midia", t0)
        await _criar(mid, principal, t0)
        return
    log_out.info(
        f"🎟 ESPELHO_CUPONS|EDITADO|post:{mid}→espelho:{esp}"
        f"{'|+midia' if arquivo is not None else ''}|ms={_ms(t0)}")


async def _apagar(mid: int, esp: int, motivo: str, t0: float) -> None:
    with contextlib.suppress(Exception):
        await _io(lambda c: c.delete_messages(_canal, esp))
    db_espelho_del(mid)
    log_out.info(
        f"🎟 ESPELHO_CUPONS|APAGADO|post:{mid}→espelho:{esp}|{motivo}|ms={_ms(t0)}")


def _midia_copiavel(mensagem):
    """A mídia do post (foto/documento) para reuso por referência; None
    para texto puro ou prévia de link (que o Telegram gera sozinho)."""
    midia = getattr(mensagem, "media", None)
    if midia is None or type(midia).__name__ == "MessageMediaWebPage":
        return None
    return midia


async def _copiar(c, principal):
    """Cópia fiel do que está no ar: texto cru + entidades de formatação
    (parse_mode=None — reprocessar markdown corromperia `_` de URLs) e a
    mídia por referência, sem novo upload."""
    midia = _midia_copiavel(principal)
    if midia is not None:
        return await c.send_file(
            _canal, midia, caption=principal.message or "",
            formatting_entities=principal.entities or None, parse_mode=None,
            force_document=False)
    return await c.send_message(
        _canal, principal.message or "",
        formatting_entities=principal.entities or None, parse_mode=None,
        link_preview=True)


async def _io(op):
    """Uma operação no Telegram sob o orçamento global de envio
    (_SEM_ENVIO). FloodWait e falha de conexão: espera FORA do semáforo
    e tenta de novo (até _TENTATIVAS); o resto sobe."""
    from client import client
    for tentativa in range(1, _TENTATIVAS + 1):
        sem = config._SEM_ENVIO or contextlib.nullcontext()
        try:
            async with sem:
                return await op(client)
        except FloodWaitError as e:
            if e.seconds > _FLOOD_MAX_S or tentativa == _TENTATIVAS:
                raise
            espera = e.seconds
        except (ConnectionError, OSError, asyncio.TimeoutError):
            if tentativa == _TENTATIVAS:
                raise
            espera = 2 ** tentativa
        await asyncio.sleep(espera)
