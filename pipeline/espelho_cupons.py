"""
Canal de cupons — o post de CUPOM sai nos DOIS canais.

Responsabilidade ÚNICA: publicar no canal de cupons (CANAL_CUPONS),
junto com o canal principal, todo post cuja composição é de
cupom (resolucao_identidade.eh_identidade_cupom: cupom com ou sem código
e nenhum produto), e manter as duas mensagens iguais depois.
  - não decide nada: texto, mídia, família e score continuam decididos
    UMA vez, sob os locks de sempre; o canal de cupons recebe a MESMA
    operação, com o mesmo texto e a mesma imagem;
  - não toca identidade, família, convergência nem locks;
  - não loga — só a medição (⏱ [ENVIO_MS] da publicação e ⏱ [EDICAO_MS]
    das edições, pelo medicao_edicao: destino, operação, ms, resultado;
    nada de texto, link ou código).

SEM um esperar o outro:
  - publicação SEM mídia: o envio ao canal de cupons é disparado no
    mesmo instante do envio ao principal, pela MESMA rotina de saída
    (saida._enviar_msg_no_sem), e o principal nunca o aguarda;
  - publicação COM mídia (publicar_depois): o principal sai SOZINHO e a
    cópia começa assim que ele sai — a foto JÁ PUBLICADA vai por
    referência (sem novo upload, sem baixar de novo). Nunca dois envios de
    mídia do mesmo post ao mesmo tempo: em produção (02–23/10/2026), os
    dois SendMedia simultâneos com a mesma imagem faziam um deles receber
    FLOOD_WAIT de ~10 s, dormido em silêncio pelo Telethon
    (flood_sleep_threshold), que ainda adia todo SendMedia seguinte do
    processo até o prazo — o principal atrasava 10 s em 37 de 343 cupons
    com foto e posts de produto ficavam presos junto. A cópia sai alguns
    décimos de segundo depois do principal; o principal não a aguarda;
  - edição SEM troca de mídia (evolução, sincronização do líder): a
    mesma edição sai nos dois canais em paralelo
    (saida._editar_inner_no_sem);
  - edição COM troca de mídia (upgrade de mídia, evolução ou líder com
    imagem nova): o principal edita SOZINHO e a do canal de cupons vem
    depois (conteudo), copiando a foto que ficou no ar por referência —
    em produção (23–27/10/2026, 18 vezes, TODAS em troca de mídia de
    cupom), as duas edições simultâneas com a mesma imagem deram
    ⏳ EditMessageRequest seg=10, e o Telethon ainda adiou a edição de
    outros posts até o prazo (27/10 12:01:46: o upgrade do produto 31661
    esperou 6 s pela cópia do cupom 31658).

ORDEM POR POST: toda operação do canal de cupons de um post entra numa
fila própria daquele post (_cadeia) no momento em que o aplicador a pede
— sob o lock do post, na mesma ordem das operações do principal — e só
começa quando a anterior daquele post terminar. Posts diferentes não
esperam um ao outro.

A MAIS NOVA VALE: a reconciliação lê o principal na hora em que roda, nunca
o que valia quando foi pedida. Se, quando ela chega a vez, já há um pedido
mais novo do mesmo post atrás dela na fila, ela não faz nada (obsoleta) e
o mais novo faz por ela — herdando o "a mídia mudou". Nenhuma edição velha
é aplicada depois de uma mais nova ser pedida.

FALHA: do principal (ok=False) → reconcilia pelo que está no ar (a edição
pode ter entrado e só a resposta se perdido). Do canal de cupons, duas
provas distintas, nunca uma pela outra:
  · APAGADA — o worker apagou a cópia: só pts_count > 0 na resposta do
    delete (saida.exclusao_provada, a mesma regra da substituição do
    principal). Só com ela a cópia é refeita (apagar + copiar) ou o vínculo
    sai;
  · AUSENTE — a cópia sumiu sem ter sido o worker (o dono a apagou à mão):
    o Telegram não a acha (MessageIdInvalid na edição, ou o delete
    respondeu sem apagar nada) E a leitura por id confirma. A cópia NÃO é
    recriada — apagado fica apagado, como o post do principal (frente M) —
    e o vínculo vira AUSENTE: não é tratado como cópia saudável e nenhuma
    operação seguinte daquele post toca o canal de cupons.
Sem nenhuma das duas provas (delete ou leitura falhou, ou respondeu 0 com a
cópia ainda lá): nada de segunda mensagem e o vínculo FICA como estava — a
falha sobe e a próxima operação do post tenta de novo. Nunca uma mensagem no
canal de cupons sem vínculo, por este módulo, nos caminhos de edição,
reconciliação e remoção (publicação: ver _apagar_mensagem).

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
import contextvars
import io
import itertools
import os
import time
from typing import Optional

from telethon.errors import (FloodWaitError, MessageIdInvalidError,
                             MessageNotModifiedError)

import config
import medicao_edicao
from config import GRUPO_DESTINO
from logger import log_sys
from database import (db_espelho_del, db_espelho_get, db_espelho_mover,
                      db_espelho_set, db_exibida)
from pipeline import saida
from pipeline.resolucao_identidade import eh_identidade_cupom

__all__ = ["canal_configurado", "iniciar", "encerrar", "vai_para_cupons",
           "publicar_junto", "publicar_depois", "vincular",
           "abortar", "edicao_junto", "conferir", "conteudo", "substituido",
           "removido"]

_VARIAVEL = "CANAL_CUPONS"
_TENTATIVAS = 3           # por operação, só para falha transitória
_FLOOD_MAX_S = 300        # FloodWait acima disso: a operação é abandonada
_DRENO_S = 5.0            # no shutdown, espera o que está em curso até isto

_canal = None             # canal de cupons (str/int)
_aberto = False           # False = desligado (ou encerrado): tudo é no-op
_cadeia: dict = {}        # msg_id_dest -> última operação daquele post
_tarefas: set = set()     # toda operação em curso (para o shutdown)
_pedidos: dict = {}       # msg_id_dest -> (versão da reconciliação mais nova, mídia mudou)
_versoes = itertools.count(1)
_AUSENTE = 0              # vínculo de cópia AUSENTE (ver FALHA): nunca recriada nem tocada


class CopiaNoAr(Exception):
    """O delete respondeu sem provar a exclusão e a leitura ainda acha a
    cópia: ela continua publicada — nada de segunda mensagem, vínculo fica."""


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
    _pedidos.clear()
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
    return _tarefa(_medido("publicar", _io(lambda _c: saida._enviar_msg_no_sem(
        texto, arquivo, destino=_canal))))


def publicar_depois(msg_id_dest: int, principal, texto: str, img) -> None:
    """Post de cupom COM mídia: o principal JÁ saiu como `principal`. A
    cópia começa agora — nunca junto com o envio de mídia do principal — e
    o principal não a espera. Cópia fiel do que está no ar: texto cru +
    entidades e a foto POR REFERÊNCIA (_copiar).

    Texto longo (> legenda): a foto saiu numa mensagem própria antes do
    texto e `principal` é o texto. A cópia faz o mesmo, pela mesma rotina
    de saída, com os bytes já em memória num buffer próprio (sem baixar de
    novo) e a conferência de sempre (vincular)."""
    if not _aberto:
        return
    if img is not None and len(texto) > saida.LIMITE_LEGENDA and not _tem_midia(principal):
        vincular(msg_id_dest, publicar_junto(texto, _copia(img)), principal)
        return
    _encadear(msg_id_dest, lambda: _medido("copia", _publicar_copia(msg_id_dest, principal)))


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
                       trocar_midia: bool, *, rastro=None):
    """A edição do principal vai sair AGORA: a mesma edição no canal de
    cupons, em paralelo, quando o post é de cupom e já tem a mensagem lá
    (ou ela está nascendo). Devolve (imagem_para_o_principal, task|None);
    com task, o aplicador chama conferir() quando o principal terminar.

    Com troca de mídia não há paralelo: devolve (imagem, None) e o
    aplicador, quando o principal terminar, chama conteudo() — a do canal
    de cupons sai depois, com a foto do principal por referência (nunca
    duas edições com a mesma imagem ao mesmo tempo; ver o cabeçalho).

    `rastro` = (cid, op) da medição do principal (medicao_edicao)."""
    if chaves is None:
        chaves = db_exibida(msg_id_dest)
    if not vai_para_cupons(chaves) or not _tem_mensagem(msg_id_dest):
        return imagem, None
    if trocar_midia:
        return imagem, None
    med = _medida(msg_id_dest, rastro, False)
    junto = _encadear(msg_id_dest, lambda: _editar_junto(msg_id_dest, texto, med))
    return imagem, junto


def conferir(msg_id_dest: int, junto: Optional[asyncio.Task], res, *,
             rastro=None) -> None:
    """A edição do principal TERMINOU com `res` (ResultadoEdicao, ok ou
    não). Se a do canal de cupons não deu o mesmo resultado, ou se o
    principal falhou, reconcilia copiando o que está no ar."""
    if junto is None or not _aberto:
        return
    med = _medida(msg_id_dest, rastro, False)
    _encadear(msg_id_dest, lambda: _conferir(msg_id_dest, junto, res, med))


# ── Depois do principal (sem simultâneo): reconciliação, nunca aguardada ──
def conteudo(msg_id_dest: int, midia: bool = False, *, rastro=None) -> None:
    """O post mudou (ou a edição do principal falhou) e nada saiu em
    paralelo no canal de cupons: cria, edita ou apaga lá conforme o que
    está no ar QUANDO a operação roda. `midia` = a imagem publicada mudou
    (ou pode ter mudado). Um pedido mais novo do mesmo post torna este
    obsoleto (ver A MAIS NOVA VALE no cabeçalho)."""
    if not _aberto or db_espelho_get(msg_id_dest) == _AUSENTE:
        return            # cópia AUSENTE: apagado fica apagado (ver FALHA)
    if not (_tem_mensagem(msg_id_dest) or eh_identidade_cupom(db_exibida(msg_id_dest))):
        return            # nem é cupom, nem tem mensagem lá (produto): nada a fazer
    versao = next(_versoes)
    _pedidos[msg_id_dest] = (versao, _pedidos.get(msg_id_dest, (0, False))[1] or bool(midia))
    med = _medida(msg_id_dest, rastro, midia)
    _encadear(msg_id_dest, lambda: _reconciliar_pedido(msg_id_dest, versao, med))


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
    # [F1.2-A2-O] Contexto VAZIO: o principal nunca aguarda a operação, que
    # vive mais que a execução de quem a pediu — não leva a execução dele.
    # ContextVar nova no processo obriga a reavaliar isto (test_eventos_tarefas).
    t = asyncio.get_running_loop().create_task(_protegido(coro),
                                               context=contextvars.Context())
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
    if db_espelho_get(msg_id_dest) == _AUSENTE:
        return False                      # cópia ausente: nada a editar lá
    return msg_id_dest in _cadeia or db_espelho_get(msg_id_dest) is not None


def _mensagem_de(msg_id_dest: int) -> Optional[int]:
    """msg_id da cópia viva do post, ou None (sem vínculo, ou AUSENTE)."""
    esp = db_espelho_get(msg_id_dest)
    return None if esp == _AUSENTE else esp


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


async def _medido(op: str, coro):
    """⏱ duração de uma publicação no canal de cupons (fila do _SEM_ENVIO,
    envio e espera automática do Telethon): destino, operação, ms e
    resultado (classe da exceção). Nada de texto, link ou id."""
    t0 = time.monotonic()
    resultado = "ok"
    try:
        return await coro
    except BaseException as e:
        resultado = f"falha:{type(e).__name__}"
        raise
    finally:
        log_sys.info(f"⏱ [ENVIO_MS] destino=cupons op={op} "
                     f"ms={round((time.monotonic() - t0) * 1000)} resultado={resultado}")


async def _publicar_copia(mid: int, principal) -> None:
    try:
        copia = await _io(lambda c: _copiar(c, principal))
    except Exception:                                  # noqa: BLE001
        if not _aberto:
            raise
        # Uma 2ª tentativa: a mesma rede de segurança que a conferência dava
        # ao envio simultâneo. Só o ENVIO repete — nada foi publicado ainda.
        copia = await _io(lambda c: _copiar(c, principal))
    db_espelho_set(mid, copia.id)


async def _descartar(tarefa: asyncio.Task) -> None:
    await _aguardar(tarefa)
    copia = _resultado(tarefa)
    if copia is not None:
        await _apagar_mensagem(copia.id)


def _medida(mid: int, rastro, troca) -> "medicao_edicao.Medida":
    """Medição de uma edição do canal de cupons, pedida AGORA (o relógio
    da fila começa aqui). `rastro` = (cid, op) da edição do principal."""
    cid, op = rastro if rastro else (medicao_edicao.novo_cid(mid), "reconciliacao")
    return medicao_edicao.Medida(cid, "cupons", op, troca)


async def _editar_junto(mid: int, texto: str, med):
    with medicao_edicao.medir(med):
        esp = _mensagem_de(mid)
        if esp is None:                   # não nasceu (conferir() cria) ou AUSENTE
            med.recon = "ausente" if db_espelho_get(mid) == _AUSENTE else "sem_mensagem"
            return None
        res = await _io(lambda _c: saida._editar_inner_no_sem(
            esp, texto, None, exigir_imagem=False, trocar_midia=False,
            destino=_canal))
        med.resultado = "ok" if res.ok else "falha"
        return res


async def _conferir(mid: int, junto: asyncio.Task, res, med) -> None:
    feito = _resultado(junto)
    if (res.ok and feito is not None and feito.ok
            and feito.midia_aplicada == res.midia_aplicada):
        return                            # os dois saíram iguais
    med.troca = True
    with medicao_edicao.medir(med):
        med.recon = await _reconciliar(mid, True)


async def _reconciliar_pedido(mid: int, versao: int, med) -> None:
    with medicao_edicao.medir(med):
        mais_nova, midia = _pedidos.get(mid, (versao, med.troca))
        if mais_nova != versao:
            med.recon = "obsoleta"        # a mais nova, atrás desta na fila, faz por ela
            return
        _pedidos.pop(mid, None)
        med.troca = midia
        med.recon = await _reconciliar(mid, midia)


async def _substituir(antigo: int, principal) -> None:
    if db_espelho_get(antigo) is not None:
        db_espelho_mover(antigo, principal.id)
    await _reconciliar(principal.id, True, principal)


async def _remover(mid: int) -> None:
    esp = db_espelho_get(mid)
    if esp == _AUSENTE:
        db_espelho_del(mid)               # nada no ar: só o vínculo AUSENTE sai
    elif esp is not None:
        await _apagar(mid, esp)


async def _reconciliar(mid: int, midia: bool, principal=None) -> str:
    """Deixa o canal de cupons igual ao que está no ar no principal.
    Devolve o que fez (o `recon` da medição)."""
    esp = db_espelho_get(mid)
    if esp == _AUSENTE:
        return "ausente"                  # sumiu sem ter sido o worker: não recria
    cupom = eh_identidade_cupom(db_exibida(mid))
    if esp is None and not cupom:
        return "nao_cupom"
    if esp is not None and not cupom:
        await _apagar(mid, esp)
        return "apagou"
    if principal is None:
        principal = await _io(lambda c: c.get_messages(GRUPO_DESTINO, ids=mid))
        if principal is None:
            return "sem_principal"                   # saiu do principal
    if esp is None:
        await _criar(mid, principal)
        return "criou"
    return await _editar(mid, esp, principal, midia)


async def _criar(mid: int, principal) -> None:
    copia = await _io(lambda c: _copiar(c, principal))
    db_espelho_set(mid, copia.id)


async def _editar(mid: int, esp: int, principal, midia: bool) -> str:
    arquivo = _midia_copiavel(principal) if midia else None
    try:
        await _io(lambda c: c.edit_message(
            _canal, esp, principal.message or "",
            formatting_entities=principal.entities or None, parse_mode=None,
            file=arquivo, link_preview=True))
    except MessageNotModifiedError:
        return "igual"
    except FloodWaitError:
        raise
    except MessageIdInvalidError:
        # O Telegram não acha a cópia. AUSENTE só com a leitura por id
        # confirmando (ver FALHA): então nunca é recriada. Ainda no ar, ou
        # leitura que falha: a falha sobe e o vínculo fica.
        if await _ler(esp) is None:
            db_espelho_set(mid, _AUSENTE)
            return "ausente"
        raise
    except Exception:                                  # noqa: BLE001
        if arquivo is None:
            raise
        # A cópia nasceu sem mídia e o principal ganhou a primeira imagem
        # (o Telegram não põe mídia em mensagem de texto — a mesma
        # limitação do canal principal): refaz como cópia fiel do que está
        # no ar, SÓ com a exclusão PROVADA. Ausente: não recria. Sem prova
        # (CopiaNoAr ou o delete/leitura falhou): sobe, o vínculo fica.
        if await _apagar_copia(esp) == "ausente":
            db_espelho_set(mid, _AUSENTE)
            return "ausente"
        db_espelho_del(mid)
        await _criar(mid, principal)
        return "recriou"
    return "editou_ref" if arquivo is not None else "editou"


async def _apagar(mid: int, esp: int) -> None:
    """Tira a cópia do canal de cupons e SÓ ENTÃO o vínculo (apagada com
    prova, ou ausente). Sem prova, a exceção sobe e o vínculo fica: nunca
    uma mensagem publicada sem rastro no banco."""
    await _apagar_copia(esp)
    db_espelho_del(mid)


async def _apagar_copia(msg_id: int) -> str:
    """Apaga a cópia e diz o que ficou PROVADO: "apagada" (pts_count > 0,
    saida.exclusao_provada) ou "ausente" (o delete não apagou nada e a
    leitura por id não a acha). Delete ou leitura que falha: a exceção
    sobe; respondeu sem prova e a cópia segue lá: CopiaNoAr."""
    resultado = await _io(lambda c: c.delete_messages(_canal, msg_id))
    if saida.exclusao_provada(resultado):
        return "apagada"
    if await _ler(msg_id) is None:
        return "ausente"
    raise CopiaNoAr(msg_id)


async def _ler(msg_id: int):
    """A cópia lida por id no canal de cupons, ou None quando o Telegram
    não a tem. Falha de leitura sobe (nunca vira "não está lá")."""
    return await _io(lambda c: c.get_messages(_canal, ids=msg_id))


async def _apagar_mensagem(msg_id: int) -> None:
    """Publicação (_concluir_publicacao, _descartar): apagar sem prova e
    sem levantar — LIMITE conhecido, anterior a este módulo de edição: se
    o delete falhar, a cópia pode ficar no ar sem vínculo (ver o PR)."""
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
                medicao_edicao.comecou()       # fim da fila da edição medida
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
