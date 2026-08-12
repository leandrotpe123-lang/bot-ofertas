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
async def _enviar_msg(texto: str, img) -> object:
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


# ─────────────────────────────────────────────────────────────────
# Edição sem deadlock de semáforo.
# asyncio.Semaphore não é reentrante: se uma função que já segura
# _SEM_ENVIO chamar editar_por_id (que tenta o mesmo semáforo), trava.
# Por isso há duas versões:
#   - _editar_inner_no_sem: SEM semáforo, uso interno apenas
#   - editar_por_id:        COM semáforo, callers externos
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
async def _substituir_post_com_midia(
    msg_id_dest_antigo: int, montada: MensagemMontada,
) -> Optional[object]:
    """Apaga a mensagem antiga e reenvia com a imagem nova."""
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
                sent = await _enviar_msg(montada.texto, montada.imagem)
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
