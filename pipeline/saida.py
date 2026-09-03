"""
Módulo 3 — Saída no destino.

Responsabilidade ÚNICA: conversar com o Telegram do grupo de destino —
enviar, editar, substituir (apagar+reenviar). I/O puro.

NÃO faz (é do orquestrador, publicacao.py):
  - db_set_estado / qualquer persistência;
  - edit_count / contadores;
  - mapas origem→destino;
  - decisão de score / janela;
  - conhecimento da identidade da oferta.

_SEM_ENVIO é consumido aqui (declarado em config, instanciado em
globals._init_globals): não é estado de negócio, é controle de
throughput / proteção de FloodWait do Telegram.

[E3.3] ESTE MÓDULO É O ÚNICO DONO DO _SEM_ENVIO
------------------------------------------------
O semáforo limita RECURSO TELEGRAM. CONSISTÊNCIA DE DOMÍNIO é dos
locks (origem → identidade → post) e não passa por aqui. As duas
responsabilidades não se misturam.

Por isso a aquisição vive na FRONTEIRA DE I/O, e não a montante:
decisão, banco, trabalho síncrono e espera por lock_post não podem
consumir vaga de envio. Antes da E3.3 a aquisição ficava em
publicacao._enviar_inner, acima de decidir() e do lock do post —
caminhos que terminam em DESCARTE gastavam vaga sem fazer uma única
chamada ao Telegram.

CONTRATO DE AQUISIÇÃO — uma operação de saída, UMA aquisição:

  PÚBLICAS (ADQUIREM o semáforo; por isso NUNCA podem ser chamadas
  de dentro de outra pública):
      _enviar_msg
      editar_msg
      editar_por_id               (herdada; mesma família de editar_msg)
      _substituir_post_com_midia

  INTERNAS (NÃO adquirem; uso EXCLUSIVO de dentro de uma pública):
      _enviar_msg_no_sem
      _editar_inner_no_sem
      _substituir_inner_no_sem

asyncio.Semaphore NÃO é reentrante — e com 3 vagas a reentrada não
falha na hora: ela só trava quando 3 tarefas estiverem aninhadas ao
mesmo tempo. É um deadlock que aparece somente sob carga. Daí a regra
ser ESTRUTURAL (duas famílias de nomes) e não uma recomendação.
"""
from __future__ import annotations

import asyncio
from typing import Optional

from telethon.errors import FloodWaitError, MessageNotModifiedError

import config
from config import GRUPO_DESTINO
from logger import log_out
from pipeline.montagem import MensagemMontada


# ── Envio ─────────────────────────────────────────────────────────
async def _enviar_msg_no_sem(texto: str, img) -> object:
    """Envia SEM adquirir _SEM_ENVIO. Use APENAS de dentro de uma
    função pública que já segura o semáforo.

    Os fallbacks internos (send_file+caption → send_file sem caption →
    send_message) fazem parte DESTA operação e ficam sob a mesma vaga."""
    from client import client
    if img:
        if len(texto) <= 1024:
            try:
                return await client.send_file(GRUPO_DESTINO, img, caption=texto,
                                              parse_mode="md", force_document=False)
            except Exception as e:
                log_out.warning(f"⚠️ send_file+caption: {e}")
                try:
                    await client.send_file(GRUPO_DESTINO, img, force_document=False)
                    return await client.send_message(GRUPO_DESTINO, texto,
                                                     parse_mode="md", link_preview=True)
                except Exception as e2:
                    log_out.warning(f"⚠️ send_file sem caption: {e2}")
        else:
            try:
                await client.send_file(GRUPO_DESTINO, img, force_document=False)
                return await client.send_message(GRUPO_DESTINO, texto,
                                                 parse_mode="md", link_preview=False)
            except Exception as e:
                log_out.warning(f"⚠️ send_file longo: {e}")
    return await client.send_message(GRUPO_DESTINO, texto,
                                     parse_mode="md", link_preview=True)


async def _enviar_msg(texto: str, img) -> object:
    """Envio PÚBLICO — UMA aquisição do _SEM_ENVIO cobrindo a operação
    inteira, inclusive os fallbacks internos.

    O laço de re-tentativa de _aplicar_novo_envio fica FORA: cada
    tentativa envia um payload DIFERENTE (a 2ª degrada para img=None),
    logo é outra operação de saída, com a própria aquisição. Nunca
    duas ao mesmo tempo, nunca aninhadas."""
    async with config._SEM_ENVIO:
        return await _enviar_msg_no_sem(texto, img)


# ─────────────────────────────────────────────────────────────────
# Edição sem deadlock de semáforo.
# asyncio.Semaphore não é reentrante: se uma função que já segura
# _SEM_ENVIO chamar uma pública (que tenta o mesmo semáforo), trava.
# Por isso há duas versões:
#   - _editar_inner_no_sem: SEM semáforo, uso interno apenas
#   - editar_msg:           COM semáforo, callers externos
# ─────────────────────────────────────────────────────────────────
async def _editar_inner_no_sem(msg_id_dest: int, texto_novo: str,
                                imagem_nova=None,
                                exigir_imagem: bool = False,
                                trocar_midia: bool = True) -> bool:
    """Edita mensagem sem adquirir _SEM_ENVIO. Use APENAS dentro de
    funções que já seguram o semáforo.

    Com exigir_imagem=True, se a edição COM imagem falhar (post nasceu
    sem mídia), devolve False SEM editar só o texto, para o chamador cair
    no fallback de substituição (deletar+repostar).

    [FASE 1] SEPARAÇÃO CONTEÚDO × MÍDIA
    ------------------------------------
    `trocar_midia` é a AUTORIZAÇÃO EXPLÍCITA para a mídia ser tocada.
    Sem ela, `imagem_nova` é IGNORADA e a chamada edita apenas a
    legenda — a mídia publicada permanece intacta.

    Isto existe porque, no primitivo original, decidir o TEXTO
    arrastava a MÍDIA junto: passar `imagem_nova` fazia o
    edit_message usar `file=`, que SUBSTITUI a mídia do post. Não
    havia caminho para "evolui o texto, mantém a imagem".

    Default True: preserva byte-a-byte o comportamento de todo caller
    que não se pronuncia. A separação é OPT-IN de quem decide.

    Invariante: `trocar_midia=False` NUNCA altera a mídia publicada.
    Invariante: `trocar_midia=True` reproduz exatamente o comportamento
                anterior, inclusive o fallback de exigir_imagem.
    ATENÇÃO: `exigir_imagem=True` com `trocar_midia=False` é
    contraditório — sem autorização não há imagem a exigir. A Fase 2
    (política de mídia) é quem deve impedir essa combinação."""
    from client import client
    # A mídia só entra na chamada se houver AUTORIZAÇÃO e imagem.
    midia = imagem_nova if trocar_midia else None
    for t in range(1, 4):
        try:
            if midia:
                try:
                    await client.edit_message(
                        GRUPO_DESTINO, msg_id_dest, texto_novo,
                        parse_mode="md", file=midia,
                    )
                except Exception as e_img:
                    if exigir_imagem:
                        log_out.info(
                            f"🖼 imagem não entrou (post sem mídia) "
                            f"dest_id={msg_id_dest}: {e_img}"
                        )
                        return False
                    await client.edit_message(
                        GRUPO_DESTINO, msg_id_dest, texto_novo,
                        parse_mode="md",
                    )
            else:
                await client.edit_message(
                    GRUPO_DESTINO, msg_id_dest, texto_novo,
                    parse_mode="md",
                )
            log_out.info(f"✏️ Editado | dest_id={msg_id_dest}")
            return True
        except MessageNotModifiedError:
            return True
        except FloodWaitError as e:
            if e.seconds > 120:
                log_out.warning(
                    f"⚠️ FloodWait longo {e.seconds}s — abortando edição"
                )
                return False
            await asyncio.sleep(e.seconds)
        except Exception as e:
            log_out.error(f"❌ edit t={t}: {e}")
            if t < 3:
                await asyncio.sleep(2 ** t)
    return False


async def editar_msg(msg_id_dest: int, texto_novo: str,
                     imagem_nova=None,
                     exigir_imagem: bool = False,
                     trocar_midia: bool = True) -> bool:
    """Edição PÚBLICA — UMA aquisição do _SEM_ENVIO cobrindo a operação
    inteira: as até 3 tentativas, o backoff exponencial e o sleep de
    FloodWait desta edição ficam TODOS sob a mesma vaga (regra: o
    semáforo limita o I/O e o retry/FloodWait daquela operação).

    Assinatura idêntica à de _editar_inner_no_sem: esta função só
    acrescenta a aquisição, não interpreta nem altera parâmetro algum."""
    async with config._SEM_ENVIO:
        return await _editar_inner_no_sem(
            msg_id_dest, texto_novo, imagem_nova,
            exigir_imagem=exigir_imagem, trocar_midia=trocar_midia
        )


async def editar_por_id(msg_id_dest: int, texto_novo: str,
                        imagem_nova=None) -> bool:
    """Versão pública (com semáforo) pra callers externos que não
    seguram _SEM_ENVIO. Delega pra _editar_inner_no_sem."""
    async with config._SEM_ENVIO:
        return await _editar_inner_no_sem(
            msg_id_dest, texto_novo, imagem_nova
        )


# ─────────────────────────────────────────────────────────────────
# Substituição com mídia (deletar + reenviar).
# Se delete falha, ABORTA (retorna None). O caller cai pra edição comum
# (que não duplica). Sem essa proteção, o canal duplicaria.
# ─────────────────────────────────────────────────────────────────
async def _substituir_inner_no_sem(
    msg_id_dest_antigo: int, montada: MensagemMontada,
) -> Optional[object]:
    """Apaga a mensagem antiga e reenvia com a imagem nova.

    SEM adquirir _SEM_ENVIO: usa _enviar_msg_no_sem no reenvio, porque
    a vaga já é segurada pela pública _substituir_post_com_midia e
    readquiri-la aqui seria aninhamento."""
    from client import client
    try:
        # 1. Apaga a mensagem antiga
        try:
            await client.delete_messages(GRUPO_DESTINO, msg_id_dest_antigo)
        except FloodWaitError as e:
            if e.seconds <= 30:
                await asyncio.sleep(e.seconds)
                try:
                    await client.delete_messages(
                        GRUPO_DESTINO, msg_id_dest_antigo,
                    )
                except Exception as e2:
                    log_out.warning(
                        f"⚠️ delete 2ª tentativa: {e2} — abortando substituição"
                    )
                    return None
            else:
                log_out.warning(
                    f"⚠️ FloodWait {e.seconds}s no delete — abortando substituição"
                )
                return None
        except Exception as e:
            log_out.warning(
                f"⚠️ delete_messages: {e} — abortando substituição "
                f"(caller cai pra edição)"
            )
            return None

        # 2. Reenvia com a imagem nova
        sent = None
        for t in range(1, 4):
            try:
                sent = await _enviar_msg_no_sem(montada.texto, montada.imagem)
                break
            except FloodWaitError as e:
                if e.seconds > 60:
                    log_out.warning(
                        f"⚠️ FloodWait longo {e.seconds}s — abortando reenvio"
                    )
                    return None
                await asyncio.sleep(e.seconds)
            except Exception as e:
                log_out.error(f"❌ reenvio t={t}: {e}")
                if t < 3:
                    await asyncio.sleep(2 ** t)

        if sent:
            log_out.info(
                f"🔄 [REENVIO_OK] {msg_id_dest_antigo} → {sent.id} "
                f"@{montada.chat}"
            )
        return sent
    except Exception as e:
        log_out.error(f"❌ _substituir_post_com_midia: {e}", exc_info=True)
        return None


async def _substituir_post_com_midia(
    msg_id_dest_antigo: int, montada: MensagemMontada,
) -> Optional[object]:
    """Substituição PÚBLICA — UMA aquisição do _SEM_ENVIO cobrindo
    DELETE + REPOST como UMA ÚNICA operação de Telegram.

    A vaga NÃO é solta entre o delete e o reenvio. Soltá-la alargaria a
    janela em que o post já foi apagado e ainda não foi reenviado, pelo
    tempo de espera por uma vaga — regressão inaceitável.

    A SEMÂNTICA do fallback é preservada byte a byte: quem decide se
    ele acontece continua sendo d.permite_substituir, rebaixado por
    decidir() quando a política de mídia diz PRESERVA. Esta frente só
    move a fronteira do limitador de I/O."""
    async with config._SEM_ENVIO:
        return await _substituir_inner_no_sem(msg_id_dest_antigo, montada)
