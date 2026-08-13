"""Camada 6 — Publicação: envio, edição e disputa."""
#
# Implementação: pipeline.publicacao_estado (idempotência e ponte de
# Origem), pipeline.publicacao_aplicadores (execução do caminho
# decidido) e pipeline.publicacao_log (observabilidade da decisão).
#
# Este arquivo retém a ORQUESTRAÇÃO: a cadeia de locks
# (origem → identidade → post → semáforo de envio), a chamada a
# decidir() sob o lock do post, e o roteamento para o aplicador.
# A DECISÃO mora aqui; a EXECUÇÃO mora nos aplicadores.
#
# _marcar e _foi_processado NÃO são reexportados: são contrato
# interno da camada, não público.
from __future__ import annotations
import contextlib
import time
from typing import Optional

import config
from database import db_get_post
from logger import log_out
from pipeline.decisao import decidir
from pipeline import exclusao
from pipeline import familia
from pipeline import origem
from pipeline.montagem import MensagemMontada
from pipeline.normalizacao import MensagemNormalizada
from pipeline.enriquecimento import MensagemEnriquecida
from pipeline.publicacao_aplicadores import (
    _aplicar_evolucao,
    _aplicar_novo_envio,
    _aplicar_sincronizacao,
)
from pipeline.publicacao_estado import destino_vivo_de_origem
from pipeline.publicacao_log import _log_decisao


async def enviar(montada: MensagemMontada,
                 norm: Optional[MensagemNormalizada] = None,
                 *, enr: MensagemEnriquecida,
                 is_edit: bool = False) -> bool:
    """
    Publica ou edita mensagem no grupo destino.
    Aceita `is_edit` por coerência contratual com o orchestrator.
    Camada 1 do lock: serializa por OFERTA (ordem fixa) entre tasks que
    compartilham qualquer oferta. A camada 2 (lock do post) é aplicada
    dentro de _enviar_inner.

    ofertas/score vêm PRONTOS do enriquecimento em AMBOS os caminhos —
    esta camada CONSOME e nunca deriva (P2/P5). A edição recebe a porta
    pura (derivar); o efeito de memória de cupom ocorre 1x, só na
    publicação nova, dentro de enriquecer().
    `enr` é obrigatório e keyword-only: depois da F1d o orchestrator
    sempre o produz (derivar na edição, enriquecer no novo). Um guard
    tolerando None faria a publicação seguir com ofertas=[] — sem locks
    de identidade e sem família — em silêncio. O contrato recusa.
    """
    ofertas: list = enr.ofertas
    score:   int  = enr.score
    cupons_novos: int = enr.cupons_novos

# ── Camada 0: ORIGEM (Fase 1 do MB) — lock mais externo (I6) ──
    if norm is not None:
        async with await origem.lock_origem(norm.chat, norm.msg_id):
            dest_fix = destino_vivo_de_origem(norm.chat, norm.msg_id)
            if dest_fix and not is_edit:
                log_out.info(
                    f"🔗 [ORIGEM_JA_PUBLICADA] ({norm.chat},{norm.msg_id})"
                    f"→post:{dest_fix} — NEW absorvido (I3)")
                return True
            if ofertas:
                async with contextlib.AsyncExitStack() as stack:
                    for of in sorted(ofertas):
                        await stack.enter_async_context(
                            await exclusao.lock_identidade(of))
                    return await _enviar_inner(
                        montada, norm, ofertas, score, is_edit, dest_fix,
                        cupons_novos)
            return await _enviar_inner(
                montada, norm, ofertas, score, is_edit, dest_fix,
                cupons_novos)

    if ofertas:
        async with contextlib.AsyncExitStack() as stack:
            for of in sorted(ofertas):
                await stack.enter_async_context(await exclusao.lock_identidade(of))
            return await _enviar_inner(montada, norm, ofertas, score, is_edit,
                                       cupons_novos=cupons_novos)

    return await _enviar_inner(montada, norm, ofertas, score, is_edit,
                               cupons_novos=cupons_novos)

async def _enviar_inner(montada: MensagemMontada,
                        norm: Optional[MensagemNormalizada],
                        ofertas: list,
                        score: int,
                        is_edit: bool = False,
                        dest_fix=None,
                        cupons_novos: int = 0) -> bool:
    """Corpo real de enviar() — dentro dos locks de oferta. Acha o post
    parente por sobreposição, trava o post candidato, re-verifica sob o
    lock e decide pelo score (decisão intocada)."""
    async with config._SEM_ENVIO:
        identity = ofertas[0] if ofertas else None   # rótulo de log

        if norm is not None and (ofertas or dest_fix):
            # Alvo FIXADO pelo vínculo de Origem (I2): edit de origem
            # vinculada nunca re-casa por conteúdo em outro post.
            msg_id_rel = familia.post_da_familia(ofertas, dest_fix)
            if msg_id_rel is not None:
                post_lock = await exclusao.lock_post(msg_id_rel)
                async with post_lock:
                    estado = db_get_post(msg_id_rel)   # re-verifica sob o lock
                    agora = time.time()
                    d = decidir(norm, montada, score, estado, agora, is_edit,
                                cupons_novos)
                    if d.acao != "PUBLICAR":
                        if norm is not None:
                            # Encontro registra (I1): edits futuros desta
                            # origem roteiam direto ao mesmo post lógico.
                            origem.registrar(norm.chat, norm.msg_id, msg_id_rel)
                        identity = f"post:{msg_id_rel}"
                        _log_decisao(d, montada, norm, estado, score, agora, identity)

                        
                        if d.acao == "IGNORAR" and d.trocar_midia:
                            # [FASE 2] O texto não evolui, mas a mídia
                            # pode. Uma decisão não mata a outra.
                            return await _aplicar_upgrade_midia(
                                montada, norm, d, estado, msg_id_rel,
                                identity)

                        if d.acao == "RENASCER":
                            # Reativação em ciclo vivo → post NOVO. Absorve a
                            # UNIÃO (família antiga + candidato) para que o
                            # INSERT OR REPLACE em oferta_index reaponte TODAS
                            # as âncoras ao post novo — o antigo é orfanado do
                            # índice e vira histórico (a mensagem antiga
                            # permanece no canal, por decisão de negócio).
                            ofertas_renasce = familia.unir(msg_id_rel, ofertas)
                            log_out.info(
                                f"🐣 TL | id={montada.msg_id} chat={norm.chat} | "
                                f"RENASCIMENTO | supersede={msg_id_rel}")
                            return await _aplicar_novo_envio(
                                montada, norm, ofertas_renasce, score,
                                identity)

                        if d.acao == "SINCRONIZAR":
                            # Edição do líder → espelha o conteúdo no post,
                            # SEM incrementar edit_count (não é evolução).
                            # Preserva família (união), líder, janela e o
                            # próprio contador.
                            ofertas_familia = familia.unir(msg_id_rel, ofertas)
                            return await _aplicar_sincronizacao(
                                montada, norm, score, estado, msg_id_rel,
                                ofertas_familia, identity, d)

                        if d.acao != "EVOLUIR":
                            log_out.info(
                                f"🧭 TL | id={montada.msg_id} chat={norm.chat} | "
                                f"DESCARTE | motivo={d.motivo}")
                            return True

                        msg_id_dest = estado["msg_id_dest"]
                        edit_count  = estado.get("edit_count", 0) or 0

                        # UNIÃO DA FAMÍLIA — regra de negócio: ao evoluir, as
                        # ofertas registradas passam a ser a UNIÃO das do post
                        # existente com as da mensagem (X + Y). Lê o post ANTES
                        # de remover/regravar; a família só cresce, então a união
                        # é sempre superconjunto do post e nada legítimo se perde.
                        # Vale para os dois caminhos abaixo (edição e
                        # substituição), que partem do mesmo msg_id_dest. Sem
                        # isto, o registro gravaria só as ofertas da mensagem e
                        # descartaria as exclusivas do post — quebrando a
                        # conectividade e duplicando a família.
                        ofertas_familia = familia.unir(msg_id_dest, ofertas)

                        return await _aplicar_evolucao(
                            montada, norm, d, estado, msg_id_dest,
                            edit_count, ofertas_familia, identity,
                            cupons_novos)
                    # d.acao == PUBLICAR: estado sumiu sob o lock (substituído/
                    # limpo por outra task) → cai para NOVO ENVIO

        # ═════════════════════════════════════════════════════════════
        # NOVO ENVIO (sem post parente vivo)
        # ═════════════════════════════════════════════════════════════
        return await _aplicar_novo_envio(
            montada, norm, ofertas, score, identity)

