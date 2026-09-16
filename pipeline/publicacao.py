"""Camada 6 — Publicação: envio, edição e disputa."""
#
# Implementação: pipeline.publicacao_estado (idempotência e ponte de
# Origem), pipeline.publicacao_aplicadores (execução do caminho
# decidido) e pipeline.publicacao_log (observabilidade da decisão).
#
# Este arquivo retém a ORQUESTRAÇÃO: a cadeia de locks
# (origem → identidade → post), a chamada a
# decidir() sob o lock do post, e o roteamento para o aplicador.
# A DECISÃO mora aqui; a EXECUÇÃO mora nos aplicadores.
#
# _marcar e _foi_processado NÃO são reexportados: são contrato
# interno da camada, não público.
from __future__ import annotations
import contextlib
import time
from dataclasses import replace
from typing import Optional

import globals as g

from database import db_get_post
from logger import log_out
from pipeline.decisao import decidir
from pipeline import exclusao
from pipeline import familia
from pipeline import origem
from pipeline.montagem import MensagemMontada, materializar_imagem
from pipeline.normalizacao import MensagemNormalizada
from pipeline.enriquecimento import MensagemEnriquecida
from pipeline.publicacao_aplicadores import (
    _aplicar_evolucao,
    _aplicar_novo_envio,
    _aplicar_sincronizacao,
    _aplicar_upgrade_midia,
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
                        montada, norm, ofertas, score, is_edit, dest_fix)
            return await _enviar_inner(
                montada, norm, ofertas, score, is_edit, dest_fix)
    if ofertas:
        async with contextlib.AsyncExitStack() as stack:
            for of in sorted(ofertas):
                await stack.enter_async_context(await exclusao.lock_identidade(of))
            return await _enviar_inner(montada, norm, ofertas, score, is_edit)
    return await _enviar_inner(montada, norm, ofertas, score, is_edit)
async def _enviar_inner(montada: MensagemMontada,
                        norm: Optional[MensagemNormalizada],
                        ofertas: list,
                        score: int,
                        is_edit: bool = False,
                        dest_fix=None) -> bool:
    """Corpo real de enviar() — dentro dos locks de oferta. Acha o post
    parente por sobreposição, trava o post candidato, re-verifica sob o
    lock e decide pelo score (decisão intocada)."""
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
                # [E4.0] O fato da mídia aceita é lido AQUI, pelo
                # orquestrador, e entregue à decisão. decisao.py
                # permanece pura. A leitura acontece sob o lock do
                # post, o mesmo que serializa a escrita do mapa.
                # [E5.0] CONGELADO numa variável: a eventual segunda
                # decisão do Portão A reusa exatamente este valor —
                # reler _MIDIA_ACEITA entre as duas seria decidir com
                # dois fatos diferentes.
                chave_aceita = g.midia_aceita_get(msg_id_rel)
                d = decidir(norm, montada, score, estado, agora, is_edit,
                            midia_key_aceita=chave_aceita,
                            midia_candidata=norm.tem_midia)

                # ══ [E5.0] PORTÃO A — MATERIALIZAÇÃO SOB DEMANDA ══
                # Só quando a política AUTORIZOU tocar a imagem
                # publicada. Fica aqui, antes de _log_decisao, do
                # roteamento e dos efeitos de família: o log jamais
                # pode registrar a decisão otimista se a aplicada for
                # a provada.
                if d.trocar_midia:
                    montada = replace(
                        montada, imagem=await materializar_imagem(norm))
                    if montada.imagem is None:
                        # Materialização falhou. REDECIDE com o fato
                        # provado, reusando estado, `agora` e chave
                        # aceita — só o fato muda. Nenhum campo de `d`
                        # é editado à mão: a política continua soberana
                        # e é ela que rebaixa trocar_midia,
                        # exigir_imagem e permite_substituir. É o que
                        # impede "Telegram tem mídia" + "download
                        # falhou" de alcançar o delete+repost.
                        d = decidir(norm, montada, score, estado, agora,
                                    is_edit,
                                    midia_key_aceita=chave_aceita,
                                    midia_candidata=False)

                if d.acao != "PUBLICAR":
                    if norm is not None:
                        # Encontro registra (I1): edits futuros desta
                        # origem roteiam direto ao mesmo post lógico.
                        origem.registrar(norm.chat, norm.msg_id, msg_id_rel)
                    identity = f"post:{msg_id_rel}"
                    _log_decisao(d, montada, norm, estado, score, agora, identity)

                    if d.acao == "IGNORAR" and d.na_janela:
                        # [F6] APRENDER ≠ EVOLUIR. A mensagem já
                        # casou validamente com este post (linha
                        # 110) e o ciclo está vivo; se ela trouxe
                        # âncora nova, a FAMÍLIA aprende — o texto
                        # vencedor não muda. Sem isto a âncora se
                        # perde e o próximo grupo que a use abre um
                        # post duplicado.
                        # A guarda `na_janela` é obrigatória: post
                        # com ciclo encerrado não absorve nada.
                        familia.absorver(msg_id_rel, ofertas)

                    
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
                        # ══ [E5.0] PORTÃO B — RENASCER ══
                        # RENASCER é PUBLICAÇÃO NOVA e NÃO passa por
                        # _com_midia: chega aqui com trocar_midia=False.
                        # Ainda assim precisa dos bytes, porque
                        # _aplicar_novo_envio usa montada.imagem
                        # independentemente da decisão de troca. Se a
                        # materialização falhar, não há redecisão: o
                        # próprio aplicador degrada para envio sem
                        # imagem, como já fazia.
                        montada = replace(
                            montada, imagem=await materializar_imagem(norm))
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
                            f"DESCARTE | motivo={d.motivo} "
                            f"midia={d.motivo_midia}")
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
                        edit_count, ofertas_familia, identity)
                # d.acao == PUBLICAR: estado sumiu sob o lock (substituído/
                # limpo por outra task) → cai para NOVO ENVIO

    # ═════════════════════════════════════════════════════════════
    # NOVO ENVIO (sem post parente vivo)
    # ═════════════════════════════════════════════════════════════
    # ══ [E5.0] PORTÃO B — PUBLICAÇÃO NOVA ══
    # Sítio ÚNICO onde convergem todas as rotas de publicação nova:
    # msg_id_rel is None, norm ausente / sem ofertas / sem dest_fix, e
    # PUBLICAR com o estado desaparecido sob o lock. Nenhuma delas
    # passou pelo Portão A (todas chegam com trocar_midia=False ou sem
    # decisão nenhuma), então não há dupla materialização. A fachada
    # tolera norm=None e devolve None sem tocar a rede.
    montada = replace(montada, imagem=await materializar_imagem(norm))
    return await _aplicar_novo_envio(
        montada, norm, ofertas, score, identity)
 
