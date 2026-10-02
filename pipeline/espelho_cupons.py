"""
Canal de cupons — o post de CUPOM sai nos DOIS canais ao mesmo tempo.

Responsabilidade ÚNICA: publicar no canal de cupons (CANAL_CUPONS),
SIMULTANEAMENTE com o canal principal, todo post cuja composição é de
cupom (resolucao_identidade.eh_identidade_cupom: cupom com ou sem código
e nenhum produto), e manter as duas mensagens iguais depois.
  - não decide nada: texto, mídia, família e score continuam decididos
    UMA vez, sob os locks de sempre; o canal de cupons recebe a MESMA
    operação, com o mesmo texto e a mesma imagem;
  - não toca identidade, família, convergência nem locks;
  - não loga.

AO MESMO TEMPO, sem um esperar o outro:
  - publicação: a imagem sobe UMA vez (upload_file, com o mesmo preparo
    que o send_file faria) e o mesmo handle vai aos dois canais; o envio
    ao canal de cupons é disparado no mesmo instante do envio ao
    principal, pela MESMA rotina de saída (saida._enviar_msg_no_sem), e
    o principal nunca o aguarda;
  - evolução, sincronização do líder, upgrade de mídia: a mesma edição
    sai nos dois canais em paralelo (saida._editar_inner_no_sem).

ORDEM POR POST: toda operação do canal de cupons de um post entra numa
fila própria daquele post (_cadeia) no momento em que o aplicador a pede
— sob o lock do post, na mesma ordem das operações do principal — e só
começa quando a anterior daquele post terminar. Posts diferentes não
esperam um ao outro.

CONFERÊNCIA (depois que o principal termina, nunca antes): se o canal
de cupons não saiu igual ao principal — falhou, FloodWait longo, só um
dos dois trocou a mídia, o principal falhou ou degradou para texto —
a mensagem de lá é refeita copiando o que está no ar no principal
(texto cru + entidades, mídia por referência). O mesmo vale para o que
não pode ser simultâneo: substituição apagar+reenviar (a mensagem do
canal de cupons segue o id novo), fusão (sai junto, depois do principal),
post que passou a ser cupom (ganha a mensagem) ou deixou de ser (perde).
A classificação lê post_exibida (o que o post mostra, nunca memória).
Falha do canal de cupons nunca afeta o principal.

LIGADO por CANAL_CUPONS (ex.: FullPromotionCupons, @FullPromotionCupons
ou -100…). Ausente/vazia (ou igual ao canal principal): DESLIGADO — tudo
aqui é no-op e nenhuma task é criada (comportamento idêntico ao de antes).
"""
from __future__ import annotations

import asyncio
import contextlib
import io
import os
from typing import Optional

from telethon.errors import FloodWaitError, MessageNotModifiedError

import config
from config import GRUPO_DESTINO
from database import (db_espelho_del, db_espelho_get, db_espelho_mover,
                      db_espelho_set, db_exibida)
from pipeline import saida
from pipeline.resolucao_identidade import eh_identidade_cupom

__all__ = ["canal_configurado", "iniciar", "encerrar", "vai_para_cupons",
           "preparar_arquivo", "publicar_junto", "vincular", "abortar",
           "edicao_junto", "conferir", "conteudo", "substituido", "removido"]

_VARIAVEL = "CANAL_CUPONS"
_TENTATIVAS = 3           # por operação, só para falha transitória
_FLOOD_MAX_S = 300        # FloodWait acima disso: a operação é abandonada
_DRENO_S = 5.0            # no shutdown, espera o que está em curso até isto

_canal = None             # canal de cupons (str/int)
_aberto = False           # False = desligado (ou encerrado): tudo é no-op
_cadeia: dict = {}        # msg_id_dest -> última operação daquele post
_tarefas: set = set()     # toda operação em curso (para o shutdown)


def canal_configurado():
    """Canal de cupons configurado (username sem @ ou id numérico), ou
    None quando desligado."""
    bruto = (os.environ.get(_VARIAVEL) or "").strip()
    if not bruto:
        return None
    if bruto.lstrip("-").isdigit():
        return int(bruto)
    return bruto.lstrip("@") or None


def iniciar() -> None:
    """Liga se CANAL_CUPONS estiver configurada (e não for o próprio
    canal principal — isso duplicaria cada cupom lá). Não cria task nem
    bloqueia: cada operação é uma task própria, criada quando pedida."""
    global _canal, _aberto
    _cadeia.clear()
    _tarefas.clear()
    canal = canal_configurado()
    if canal is not None and str(canal).lower() == str(GRUPO_DESTINO).lstrip("@").lower():
        canal = None
    _canal, _aberto = canal, canal is not None


async def encerrar() -> None:
    """Shutdown: nada novo entra; dá até _DRENO_S para o que está em
    curso terminar (inclusive o que nascer disso) e cancela limpo o resto."""
    global _aberto
    _aberto = False
    loop = asyncio.get_running_loop()
    prazo = loop.time() + _DRENO_S
    while True:
        pendentes = [t for t in _tarefas if not t.done()]
        restante = prazo - loop.time()
        if not pendentes or restante <= 0:
            break
        await asyncio.wait(pendentes, timeout=restante)
    for t in pendentes:
        t.cancel()
    if pendentes:
        await asyncio.gather(*pendentes, return_exceptions=True)


# ── Simultâneo (chamado pelos aplicadores ANTES do I/O do principal) ──
def vai_para_cupons(chaves) -> bool:
    """O post (por estas âncoras exibidas) sai também no canal de cupons?"""
    return _aberto and eh_identidade_cupom(chaves)


async def preparar_arquivo(img):
    """Sobe a imagem UMA vez para os dois canais. Devolve
    (para_o_principal, para_o_canal_de_cupons).

    O upload é o MESMO que o send_file/edit_message do principal faria
    (mesmo preparo da imagem, mesma vaga do _SEM_ENVIO): o principal só
    deixa de subir a imagem dentro do envio. Falhou: o principal recebe
    a imagem original, como sempre, e o canal de cupons uma cópia dela
    (dois envios simultâneos nunca leem o mesmo buffer)."""
    if img is None or not _aberto:
        return img, None
    from client import client
    try:
        arquivo = _preparo_do_envio(img)
        async with (config._SEM_ENVIO or contextlib.nullcontext()):
            handle = await client.upload_file(arquivo)
        return handle, handle
    except Exception:                                  # noqa: BLE001
        return img, _copia(img)
    finally:
        with contextlib.suppress(Exception):
            img.seek(0)                    # a imagem original segue íntegra


def _preparo_do_envio(img):
    """Exatamente o que o Telethon faz com a imagem antes do upload num
    send_file/edit_message (redimensiona/converte se precisar). Sem isso
    o post de cupom poderia sair diferente de qualquer outro post."""
    from telethon import utils
    from telethon.client.uploads import _resize_photo_if_needed
    return _resize_photo_if_needed(img, utils.is_image(img))


def _copia(img):
    try:
        copia = io.BytesIO(img.getvalue())
        copia.name = getattr(img, "name", "imagem.jpg")
        return copia
    except Exception:                                  # noqa: BLE001
        return None        # sai sem imagem; a conferência refaz com a do principal


def publicar_junto(texto: str, arquivo) -> Optional[asyncio.Task]:
    """Dispara AGORA o envio ao canal de cupons — no mesmo instante do
    envio ao principal, mesma rotina de saída, mesmo texto, mesma imagem.
    Devolve a task (o principal NUNCA a aguarda)."""
    if not _aberto:
        return None
    return _tarefa(_io(lambda _c: saida._enviar_msg_no_sem(
        texto, arquivo, destino=_canal)))


def vincular(msg_id_dest: int, tarefa: Optional[asyncio.Task], principal) -> None:
    """O principal saiu como `principal` (msg_id_dest). Quando o envio ao
    canal de cupons terminar: igual ao principal → vínculo gravado;
    falhou ou saiu diferente (o principal degradou para texto, p.ex.) →
    refaz copiando o principal."""
    if tarefa is None or not _aberto:
        return
    _encadear(msg_id_dest, lambda: _concluir_publicacao(msg_id_dest, tarefa, principal))


def abortar(tarefa: Optional[asyncio.Task]) -> None:
    """O principal NÃO saiu: a mensagem do canal de cupons não fica
    sozinha — é apagada assim que terminar de sair."""
    if tarefa is None:
        return
    _tarefa(_descartar(tarefa))


async def edicao_junto(msg_id_dest: int, chaves, texto: str, imagem,
                       trocar_midia: bool):
    """A edição do principal vai sair AGORA: a mesma edição no canal de
    cupons, em paralelo, quando o post é de cupom e já tem a mensagem lá
    (ou ela está nascendo). Devolve (imagem_para_o_principal, task|None);
    com troca de mídia, a imagem sobe uma vez para os dois. Com task, o
    aplicador chama conferir() quando o principal terminar."""
    if chaves is None:
        chaves = db_exibida(msg_id_dest)
    if not vai_para_cupons(chaves) or not _tem_mensagem(msg_id_dest):
        return imagem, None
    arquivo = None
    if trocar_midia and imagem is not None:
        imagem, arquivo = await preparar_arquivo(imagem)
        if not _aberto:
            return imagem, None            # encerrou durante o upload
    junto = _encadear(msg_id_dest, lambda: _editar_junto(
        msg_id_dest, texto, arquivo, trocar_midia))
    return imagem, junto


def conferir(msg_id_dest: int, junto: Optional[asyncio.Task], res) -> None:
    """A edição do principal TERMINOU com `res` (ResultadoEdicao, ok ou
    não). Se a do canal de cupons não deu o mesmo resultado, ou se o
    principal falhou, reconcilia copiando o que está no ar."""
    if junto is None or not _aberto:
        return
    _encadear(msg_id_dest, lambda: _conferir(msg_id_dest, junto, res))


# ── Depois do principal (sem simultâneo): reconciliação, nunca aguardada ──
def conteudo(msg_id_dest: int, midia: bool = False) -> None:
    """O post mudou e nada saiu em paralelo no canal de cupons: cria,
    edita ou apaga lá conforme o que está no ar. `midia` = a imagem
    publicada mudou."""
    if _aberto:
        _encadear(msg_id_dest, lambda: _reconciliar(msg_id_dest, bool(midia)))


def substituido(antigo: int, mensagem) -> None:
    """Apagar+reenviar: o post `antigo` agora é `mensagem` (id novo)."""
    if _aberto:
        _encadear(mensagem.id, lambda: _substituir(antigo, mensagem), apos=antigo)


def removido(msg_id_dest: int) -> None:
    """O post saiu do canal principal (fusão): sai do de cupons também."""
    if _aberto:
        _encadear(msg_id_dest, lambda: _remover(msg_id_dest))


# ── Fila por post ─────────────────────────────────────────────────
def _tarefa(coro) -> asyncio.Task:
    t = asyncio.get_running_loop().create_task(_protegido(coro))
    _tarefas.add(t)
    t.add_done_callback(_tarefas.discard)
    return t


async def _protegido(coro):
    """Falha no canal de cupons vira None — nunca exceção solta."""
    try:
        return await coro
    except Exception:                                  # noqa: BLE001
        return None


def _encadear(msg_id_dest: int, fabrica, apos: Optional[int] = None) -> asyncio.Task:
    """Põe a operação no fim da fila do post: ela só começa quando a
    anterior daquele post (ou do post `apos`, na substituição) terminar."""
    anterior = _cadeia.get(msg_id_dest if apos is None else apos)
    t = _tarefa(_em_ordem(anterior, fabrica))
    _cadeia[msg_id_dest] = t
    t.add_done_callback(
        lambda t, m=msg_id_dest: _cadeia.pop(m, None) if _cadeia.get(m) is t else None)
    return t


async def _em_ordem(anterior, fabrica):
    await _aguardar(anterior)
    return await fabrica()


async def _aguardar(tarefa: Optional[asyncio.Task]) -> None:
    """Espera a operação (sucesso ou falha)."""
    if tarefa is not None and not tarefa.done():
        await asyncio.wait({tarefa})


def _resultado(tarefa: asyncio.Task):
    if tarefa.cancelled() or tarefa.exception() is not None:
        return None
    return tarefa.result()


def _tem_mensagem(msg_id_dest: int) -> bool:
    return msg_id_dest in _cadeia or db_espelho_get(msg_id_dest) is not None


# ── Operações ─────────────────────────────────────────────────────
async def _concluir_publicacao(mid: int, tarefa: asyncio.Task, principal) -> None:
    await _aguardar(tarefa)
    copia = _resultado(tarefa)
    if copia is not None and _tem_midia(copia) == _tem_midia(principal):
        db_espelho_set(mid, copia.id)
        return
    if copia is not None:
        await _apagar_mensagem(copia.id)
    await _criar(mid, principal)


async def _descartar(tarefa: asyncio.Task) -> None:
    await _aguardar(tarefa)
    copia = _resultado(tarefa)
    if copia is not None:
        await _apagar_mensagem(copia.id)


async def _editar_junto(mid: int, texto: str, arquivo, trocar_midia: bool):
    esp = db_espelho_get(mid)
    if esp is None:
        return None                       # a mensagem não nasceu: conferir() cria
    return await _io(lambda _c: saida._editar_inner_no_sem(
        esp, texto, arquivo, exigir_imagem=False, trocar_midia=trocar_midia,
        destino=_canal))


async def _conferir(mid: int, junto: asyncio.Task, res) -> None:
    feito = _resultado(junto)
    if (res.ok and feito is not None and feito.ok
            and feito.midia_aplicada == res.midia_aplicada):
        return                            # os dois saíram iguais
    await _reconciliar(mid, True)


async def _substituir(antigo: int, principal) -> None:
    if db_espelho_get(antigo) is not None:
        db_espelho_mover(antigo, principal.id)
    await _reconciliar(principal.id, True, principal)


async def _remover(mid: int) -> None:
    esp = db_espelho_get(mid)
    if esp is not None:
        await _apagar(mid, esp)


async def _reconciliar(mid: int, midia: bool, principal=None) -> None:
    """Deixa o canal de cupons igual ao que está no ar no principal."""
    cupom = eh_identidade_cupom(db_exibida(mid))
    esp = db_espelho_get(mid)
    if esp is None and not cupom:
        return
    if esp is not None and not cupom:
        await _apagar(mid, esp)
        return
    if principal is None:
        principal = await _io(lambda c: c.get_messages(GRUPO_DESTINO, ids=mid))
        if principal is None:
            return                                   # saiu do principal
    if esp is None:
        await _criar(mid, principal)
        return
    await _editar(mid, esp, principal, midia)


async def _criar(mid: int, principal) -> None:
    copia = await _io(lambda c: _copiar(c, principal))
    db_espelho_set(mid, copia.id)


async def _editar(mid: int, esp: int, principal, midia: bool) -> None:
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
    except Exception:                                  # noqa: BLE001
        if arquivo is None:
            raise
        # A mensagem do canal de cupons nasceu sem mídia (o principal
        # ganhou a primeira imagem agora): a mesma limitação do Telegram
        # do canal principal. Refaz como cópia fiel do que está no ar.
        await _apagar(mid, esp)
        await _criar(mid, principal)


async def _apagar(mid: int, esp: int) -> None:
    await _apagar_mensagem(esp)
    db_espelho_del(mid)


async def _apagar_mensagem(msg_id: int) -> None:
    with contextlib.suppress(Exception):
        await _io(lambda c: c.delete_messages(_canal, msg_id))


def _midia_copiavel(mensagem):
    """A mídia do post (foto/documento) para reuso por referência; None
    para texto puro ou prévia de link (que o Telegram gera sozinho)."""
    midia = getattr(mensagem, "media", None)
    if midia is None or type(midia).__name__ == "MessageMediaWebPage":
        return None
    return midia


def _tem_midia(mensagem) -> bool:
    return _midia_copiavel(mensagem) is not None


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
