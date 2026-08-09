"""Camada 6 — Publicação / Aplicadores de caminho decidido.

Responsabilidade ÚNICA: EXECUTAR um caminho JÁ DECIDIDO — evoluir um
post existente, sincronizar o conteúdo do líder, ou publicar um post
novo. Envia, persiste o estado e registra o resultado.

NENHUMA DECISÃO acontece aqui. A escolha do caminho é de
pipeline.publicacao, que chama decidir() sob o lock do post. Esta
fronteira é física justamente porque as docstrings originais já
proibiam decidir aqui — agora o módulo não tem como.

Consome _marcar de publicacao_estado (contrato interno da camada;
ver cabeçalho daquele módulo).

Extraído de pipeline.publicacao sem qualquer alteração de
comportamento.
"""
from __future__ import annotations

import asyncio
import time

from telethon.errors import FloodWaitError

from config import GRUPO_DESTINO
from database import db_registrar_post, db_remover_post
from logger import log_out, log_sys, _idade_str
from pipeline.saida import (
    _enviar_msg,
    _editar_inner_no_sem,
    _substituir_post_com_midia,
)
from pipeline.vida_oferta import estampar

# Contrato INTERNO da camada — ver cabeçalho de publicacao_estado.
from pipeline.publicacao_estado import _marcar

async def _aplicar_evolucao(montada, norm, d, estado, msg_id_dest,
                            edit_count, ofertas_familia, identity,
                            cupons_novos: int = 0) -> bool:
    """Executa a EVOLUÇÃO de um post existente: edita no lugar ou, em
    fallback autorizado, substitui com mídia. Persiste o novo estado com
    a união da família. Sem decisão — o caminho já foi decidido a montante."""
    ok = await _editar_inner_no_sem(
        msg_id_dest, montada.texto, montada.imagem,
        exigir_imagem=d.exigir_imagem)
    if ok:
        # Edição preserva o msg_id: NÃO removemos o post.
        # db_registrar_post faz upsert atômico e, como
        # ofertas_familia ⊇ ofertas do post, reescreve todas
        # sem apagar nenhuma — sem a janela do delete+insert
        # em que a família sumiria do índice e outra task
        # poderia duplicar. (db_remover_post é só p/ substituição.)
        db_registrar_post(
            msg_id_dest, ofertas_familia, d.novo_score, montada.texto,
            montada.plat, norm.chat,
            estado.get("janela_fim", 0), edit_count + 1)
        if d.motivo == "CUPOM_ENRIQUECIDO":
            log_out.info(
                f"💎 [CUPOM_ENRIQUECIDO] {identity} "
                f"novos={cupons_novos} "
                f"score={d.novo_score} chat={norm.chat}")
        elif d.motivo == "TROCA_IMG_BOA":
            log_out.info(f"✅ [IMG_TROCADA_OK] {identity}")
        else:
            log_out.info(
                f"✏️ [EDITADO_OK] {identity} novo_score={d.novo_score}")
        log_out.info(
            f"🧭 TL | id={montada.msg_id} chat={norm.chat} | "
            f"PROMOVIDO | dest={msg_id_dest} "
            f"novo_score={d.novo_score} "
            f"na_janela={'sim' if d.na_janela else 'nao'}")
        return True

    if not d.permite_substituir:
        if d.motivo == "CUPOM_ENRIQUECIDO":
            log_out.warning(f"⚠️ [CUPOM_ENRIQUECIDO_FALHOU] {identity}")
        else:
            log_out.warning(
                f"⚠️ [EDIT_FALHOU] {identity} motivo={d.motivo}")
        return True

    log_out.info(
        f"🔄 [SUBSTITUI_FALLBACK] {identity} "
        f"score {d.score_atual}→{d.novo_score} chat={norm.chat}")
    sent = await _substituir_post_com_midia(msg_id_dest, montada)
    if sent:
        db_remover_post(msg_id_dest)
        db_registrar_post(
            sent.id, ofertas_familia, d.novo_score, montada.texto,
            montada.plat, norm.chat,
            estado.get("janela_fim", 0), edit_count + 1,
            chat_origem=norm.chat if norm else "",
            msg_id_origem=montada.msg_id)
        if d.motivo == "TROCA_IMG_BOA":
            log_out.info(f"✅ [IMG_TROCADA_OK] {identity} (substitui)")
        else:
            log_out.info(
                f"✅ [SUBSTITUIDO_OK] {identity} "
                f"novo_id={sent.id} score={d.novo_score}")
    else:
        log_out.warning(f"⚠️ [SUBSTITUI_FALHOU] {identity}")
    return True

async def _aplicar_sincronizacao(montada, norm, score, estado, msg_id_dest,
                                 ofertas_familia, identity) -> bool:
    """Executa a SINCRONIZAÇÃO: espelha no post o conteúdo editado pelo
    LÍDER. Distinta da evolução — NÃO incrementa edit_count, não disputa
    score. Preserva líder, janela e contador; atualiza texto/score e a
    família (união). Sem decisão — o caminho já foi decidido a montante."""
    ok = await _editar_inner_no_sem(
        msg_id_dest, montada.texto, montada.imagem, exigir_imagem=False)
    if not ok:
        log_out.warning(f"⚠️ [SYNC_FALHOU] {identity} chat={norm.chat}")
        return True
    db_registrar_post(
        msg_id_dest, ofertas_familia, score, montada.texto,
        montada.plat, estado.get("lider", "") or norm.chat,
        estado.get("janela_fim", 0), estado.get("edit_count", 0))
    log_out.info(
        f"🔁 [SINCRONIZADO] {identity} chat={norm.chat} score={score} "
        f"edit_count={estado.get('edit_count', 0)} (preservado)")
    return True



async def _aplicar_novo_envio(montada, norm, ofertas, score,
                              identity) -> bool:
    """Executa a PUBLICAÇÃO de um post novo: envia com retry, registra a
    janela e dispara os efeitos colaterais (idempotência, saturação,
    burst). Sem decisão — chamado quando não há post parente vivo."""
    img = montada.imagem
    sent = None
    for t in range(1, 4):
        try:
            sent = await _enviar_msg(montada.texto, img); break
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except Exception as e:
            log_out.error(f"❌ envio t={t}: {e}")
            if t == 1: img = None
            elif t < 3: await asyncio.sleep(2 ** t)

    if not sent:
        log_out.error(f"❌ Envio falhou | @{montada.chat}")
        return False

    # Gravar estado IMEDIATAMENTE após envio. Falhas secundárias
    # (mapa, sat, burst) NÃO devem deixar o post sem registro — senão
    # um próximo post pode duplicar silenciosamente.
    if ofertas:
        try:
            # V1 — nascimento do ciclo: a estampa vem da autoridade única
            # da vida operacional (vida_oferta). Um valor, um dono.
            agora_v = time.time()
            janela_fim = estampar(agora_v)
            db_registrar_post(
                sent.id, ofertas, score, montada.texto,
                montada.plat, norm.chat if norm else "",
                janela_fim, 0,
                chat_origem=norm.chat if norm else "",
                msg_id_origem=montada.msg_id)
            log_out.info(
                f"🧭 TL | id={montada.msg_id} "
                f"chat={norm.chat if norm else ''} | JANELA_CRIADA | "
                f"dest={sent.id} dur={janela_fim - agora_v:.0f}s score={score}")
        except Exception as e:
            log_sys.error(
                f"❌ db_registrar_post FALHOU pós-envio: {e}", exc_info=True)

    try:
        await _marcar(montada.msg_id)
    except Exception as e:
        log_sys.error(f"❌ _marcar: {e}")
    if norm is not None and norm.is_override:
        log_out.info(f"🔓 [OVERRIDE_OK] Post liberado publicado | id={sent.id}")

    log_out.info(
        f"🚀 [OK] @{montada.chat}→{GRUPO_DESTINO} | "
        f"{montada.msg_id}→{sent.id} | "
        f"{montada.plat.upper()} score={score} sku={montada.sku} "
        f"identity={identity}")
    log_out.debug(
        f"🧭 TL | id={montada.msg_id} chat={norm.chat if norm else ''} | "
        f"ENVIADO | dest={sent.id} "
        f"idade_envio={_idade_str(norm.media_obj.date) if norm else '?'}")
    return True

