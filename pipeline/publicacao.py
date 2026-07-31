"""Camada 6 — Publicação: envio, edição, controle de saturação e disputa."""
from __future__ import annotations
import asyncio
import contextlib
import time
from typing import Optional

from telethon.errors import FloodWaitError

import config
from config import GRUPO_DESTINO
from pipeline.vida_oferta import estampar
from database import (db_registrar_sat, db_get_post, db_overlap_posts,
                      db_registrar_post, db_remover_post,
                      db_ofertas_de_post)
import globals as g
from logger import log_out, log_sys, _idade_str
from pipeline.deduplicacao import calcular_score, identidades
from pipeline.decisao import decidir
from pipeline import origem
from pipeline.vida_oferta import viva
from pipeline.saida import (
    _enviar_msg,
    _editar_inner_no_sem,
    _substituir_post_com_midia,
)
from pipeline.montagem import MensagemMontada
from pipeline.normalizacao import MensagemNormalizada
from pipeline.enriquecimento import MensagemEnriquecida
from pipeline.estado_evento import _KW_EVENTO


# ── Constantes de saturação ──────────────────────────────────────
_SAT_MAX_PLAT  = 10
_SAT_BURST_LIM = 6
_SAT_BURST_JAN = 60


# ─────────────────────────────────────────────────────────────────
# Lock pessimista por identidade
# Garante exclusão mútua entre tasks processando a mesma oferta.
# ─────────────────────────────────────────────────────────────────
_IDENTITY_LOCKS: dict = {}        # identity -> asyncio.Lock
_IDENTITY_LOCKS_TS: dict = {}     # identity -> last_access_monotonic
_IDENTITY_LOCK_TTL = 600.0        # 10 min sem uso → descarta


async def _get_identity_lock(identity: str) -> asyncio.Lock:
    """Lock dedicado por identidade. Garante exclusão mútua entre
    tasks processando o mesmo cupom/produto/evento."""
    async with g._identity_locks_lck:
        lock = _IDENTITY_LOCKS.get(identity)
        if lock is None:
            lock = asyncio.Lock()
            _IDENTITY_LOCKS[identity] = lock
        _IDENTITY_LOCKS_TS[identity] = time.monotonic()

        # Cleanup oportunista
        if len(_IDENTITY_LOCKS) > 200:
            agora = time.monotonic()
            antigos = [
                k for k, ts in _IDENTITY_LOCKS_TS.items()
                if agora - ts > _IDENTITY_LOCK_TTL
            ]
            for k in antigos:
                lk = _IDENTITY_LOCKS.get(k)
                if lk is not None and not lk.locked():
                    _IDENTITY_LOCKS.pop(k, None)
                    _IDENTITY_LOCKS_TS.pop(k, None)
            if antigos:
                log_sys.debug(
                    f"🧹 identity_locks cleanup: removidos {len(antigos)} | "
                    f"restam {len(_IDENTITY_LOCKS)}"
                )
        return lock

_POST_LOCKS: dict = {}        # msg_id_dest -> asyncio.Lock
_POST_LOCKS_TS: dict = {}     # msg_id_dest -> last_access_monotonic


async def _get_post_lock(msg_id_dest: int) -> asyncio.Lock:
    """Camada 2: lock dedicado por post de destino. Protege a mutação
    concorrente do MESMO post quando duas mensagens de famílias por
    ofertas DISTINTAS convergem para ele (ex.: {A} e {B} → post {A,B})."""
    async with g._identity_locks_lck:
        lock = _POST_LOCKS.get(msg_id_dest)
        if lock is None:
            lock = asyncio.Lock()
            _POST_LOCKS[msg_id_dest] = lock
        _POST_LOCKS_TS[msg_id_dest] = time.monotonic()
        if len(_POST_LOCKS) > 200:
            agora = time.monotonic()
            antigos = [
                k for k, ts in _POST_LOCKS_TS.items()
                if agora - ts > _IDENTITY_LOCK_TTL
            ]
            for k in antigos:
                lk = _POST_LOCKS.get(k)
                if lk is not None and not lk.locked():
                    _POST_LOCKS.pop(k, None)
                    _POST_LOCKS_TS.pop(k, None)
        return lock


def _escolher_post(candidatos: list) -> int:
    """Post da família: maior sobreposição; empate → desempate estável
    (maior score → ts mais recente → maior msg_id_dest). `candidatos`
    já vem ordenado por sobreposição desc de db_overlap_posts."""
    max_n = candidatos[0][1]
    empatados = [mid for mid, n in candidatos if n == max_n]
    if len(empatados) == 1:
        return empatados[0]
    def _chave(mid: int):
        p = db_get_post(mid) or {}
        return (p.get("score", 0), p.get("ts", 0.0), mid)
    return max(empatados, key=_chave)


async def _marcar(msg_id: int):
    async with g._IDS_LOCK:
        g._IDS_PROC.add(msg_id)
        if len(g._IDS_PROC) > 5000:
            for _ in range(len(g._IDS_PROC) - 4000):
                g._IDS_PROC.pop()


async def _foi_processado(msg_id: int) -> bool:
    async with g._IDS_LOCK:
        return msg_id in g._IDS_PROC


async def _burst_add():
    async with g._BURST_LOCK:
        agora = time.monotonic(); g._burst.append(agora)
        while g._burst and agora - g._burst[0] > _SAT_BURST_JAN:
            g._burst.pop(0)


async def _burst_count() -> int:
    async with g._BURST_LOCK:
        agora = time.monotonic()
        return sum(1 for t in g._burst if agora - t <= _SAT_BURST_JAN)


async def delay_saturacao(plat: str, texto: str) -> float:
    from database import db_count_sat
    if _KW_EVENTO.search(texto): return 0.0
    delay = 0.0
    if db_count_sat(plat) >= _SAT_MAX_PLAT: delay += 6.0
    if await _burst_count() >= _SAT_BURST_LIM: delay += 4.0
    return delay

def destino_vivo_de_origem(chat: str, msg_id: int):
    """Ponte Origem→Oferta: consulta o vínculo (infra pura, I7) e valida
    aqui a VIDA do post apontado (autoridade: vida_oferta). Devolve o
    dest do post lógico VIVO, ou None (vínculo morto → fluxo normal,
    coerente com F4/renascimento por ciclo novo)."""
    if not chat or not msg_id:
        return None
    dest = origem.consultar(chat, msg_id)
    if not dest:
        return None
    estado = db_get_post(dest)
    if estado and viva(estado.get("janela_fim") or 0.0, time.time()):
        return dest
    return None


async def enviar(montada: MensagemMontada,
                 norm: Optional[MensagemNormalizada] = None,
                 enr: Optional[MensagemEnriquecida] = None,
                 is_edit: bool = False) -> bool:
    """
    Publica ou edita mensagem no grupo destino.
    Aceita `is_edit` por coerência contratual com o orchestrator.
    Camada 1 do lock: serializa por OFERTA (ordem fixa) entre tasks que
    compartilham qualquer oferta. A camada 2 (lock do post) é aplicada
    dentro de _enviar_inner.

    ofertas/score vêm PRONTOS do enriquecimento no caminho NOVO (enr
    presente). Na EDIÇÃO (enr ausente) derivam de norm, como antes — sem
    redisparar o efeito de cupom, pois nem identidades() nem calcular_score()
    o produzem; identidade_canonica (único ponto do efeito) roda 1x, só no
    enriquecimento.
    """
    ofertas: list = []
    score: int = 0
    if enr is not None:
        ofertas = enr.ofertas
        score   = enr.score
    elif norm is not None:
        ofertas = identidades(norm)
        score   = calcular_score(norm)

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
                            await _get_identity_lock(of))
                    return await _enviar_inner(
                        montada, norm, ofertas, score, is_edit, dest_fix)
            return await _enviar_inner(
                montada, norm, ofertas, score, is_edit, dest_fix)

    if ofertas:
        async with contextlib.AsyncExitStack() as stack:
            for of in sorted(ofertas):
                await stack.enter_async_context(await _get_identity_lock(of))
            return await _enviar_inner(montada, norm, ofertas, score, is_edit)

    return await _enviar_inner(montada, norm, ofertas, score, is_edit)

def _log_decisao(d, montada, norm, estado: dict, score: int,
                 agora: float, identity: str) -> None:
    """Observabilidade da decisão de evolução. Sem efeito de fluxo:
    apenas registra o veredito de decidir() e o detalhe por motivo.
    O controle de fluxo (evoluir/descartar) permanece no chamador."""
    log_out.debug(
        f"🧭 TL | id={montada.msg_id} chat={norm.chat} | DECISAO | "
        f"motivo={d.motivo} "
        f"na_janela={'sim' if d.na_janela else 'nao'} "
        f"score {d.score_atual}→{score} janela_restante="
        f"{max(0.0, (estado.get('janela_fim', 0) or 0) - agora):.1f}s")

    if d.motivo == "JANELA_ENCERRADA":
        log_out.info(
            f"🔒 [JANELA_ENCERRADA] {identity} "
            f"oferta encerrada (ciclo de vida expirado) "
            f"candidato={norm.chat}")
    elif d.motivo == "EVOLUCAO_LIMITE_ATINGIDO":
        log_out.info(
            f"🔒 [EVOLUCAO_LIMITE_ATINGIDO] {identity} "
            f"já evoluiu {estado.get('edit_count', 0) or 0}x na janela "
            f"candidato={norm.chat}")
    elif d.motivo == "EVOLUI":
        log_out.info(
            f"✳️ [EVOLUI] {identity} "
            f"score {d.score_atual}→{score} "
            f"{'(janela)' if d.na_janela else '(lider)'} "
            f"chat={norm.chat} "
            f"img_nova={'sim' if montada.imagem else 'não'}")
    elif d.motivo == "TROCA_IMG_BOA":
        log_out.info(
            f"🖼 [TROCA_IMG_BOA] {identity} "
            f"de {estado.get('lider','')} (ruim) → {norm.chat} (bom) "
            f"delta={d.delta}s")
    elif d.motivo == "DUP_SILENCIOSO":
        log_out.debug(
            f"🔁 [DUP_SILENCIOSO] {identity} sim={d.sim:.2f}")
    elif d.motivo == "SCORE_NAO_EVOLUI":
        log_out.info(
            f"🔁 [SCORE_NAO_EVOLUI] {identity} "
            f"atual={score} salvo={d.score_atual} chat={norm.chat}")

async def _aplicar_evolucao(montada, norm, d, estado, msg_id_dest,
                            edit_count, ofertas_familia, identity) -> bool:
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
                f"novos={getattr(norm, '_cupom_novos', 0)} "
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

    try:
        db_registrar_sat(montada.plat, montada.sku)
    except Exception as e:
        log_sys.error(f"❌ db_registrar_sat: {e}")

    try:
        await _burst_add()
    except Exception:
        pass

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


async def _enviar_inner(montada: MensagemMontada,
                        norm: Optional[MensagemNormalizada],
                        ofertas: list,
                        score: int,
                        is_edit: bool = False,
                        dest_fix=None) -> bool:
    """Corpo real de enviar() — dentro dos locks de oferta. Acha o post
    parente por sobreposição, trava o post candidato, re-verifica sob o
    lock e decide pelo score (decisão intocada)."""
    async with config._SEM_ENVIO:
        identity = ofertas[0] if ofertas else None   # rótulo de log

        if norm is not None and (ofertas or dest_fix):
            # Alvo FIXADO pelo vínculo de Origem (I2): edit de origem
            # vinculada nunca re-casa por conteúdo em outro post.
            candidatos = ([(dest_fix, 0)] if dest_fix
                          else db_overlap_posts(ofertas)) 
            if len(candidatos) > 1:
                log_out.debug(
                    f"🧬 [FAMILIA_MULTI] {len(candidatos)} posts em sobreposição "
                    f"p/ ofertas={ofertas} — escolhendo o melhor candidato")
            if candidatos:
                msg_id_rel = _escolher_post(candidatos)
                log_out.info(f"🔎 [OVERLAP_MATCH] post:{msg_id_rel} casou por ofertas_compartilhadas={sorted(set(ofertas) & set(db_ofertas_de_post(msg_id_rel)))} | candidato={sorted(ofertas)}")
                post_lock = await _get_post_lock(msg_id_rel)
                async with post_lock:
                    estado = db_get_post(msg_id_rel)   # re-verifica sob o lock
                    agora = time.time()
                    d = decidir(norm, montada, score, estado, agora, is_edit)
                    if d.acao != "PUBLICAR":
                        if norm is not None:
                            # Encontro registra (I1): edits futuros desta
                            # origem roteiam direto ao mesmo post lógico.
                            origem.registrar(norm.chat, norm.msg_id, msg_id_rel)
                        identity = f"post:{msg_id_rel}"
                        _log_decisao(d, montada, norm, estado, score, agora, identity)


                        if d.acao == "RENASCER":
                            # Reativação em ciclo vivo → post NOVO. Absorve a
                            # UNIÃO (família antiga + candidato) para que o
                            # INSERT OR REPLACE em oferta_index reaponte TODAS
                            # as âncoras ao post novo — o antigo é orfanado do
                            # índice e vira histórico (a mensagem antiga
                            # permanece no canal, por decisão de negócio).
                            ofertas_renasce = sorted(
                                set(db_ofertas_de_post(msg_id_rel)) | set(ofertas))
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
                            ofertas_familia = sorted(
                                set(db_ofertas_de_post(msg_id_rel)) | set(ofertas))
                            return await _aplicar_sincronizacao(
                                montada, norm, score, estado, msg_id_rel,
                                ofertas_familia, identity)

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
                        ofertas_familia = sorted(
                            set(db_ofertas_de_post(msg_id_dest)) | set(ofertas))

                        return await _aplicar_evolucao(
                            montada, norm, d, estado, msg_id_dest,
                            edit_count, ofertas_familia, identity)
                    # d.acao == PUBLICAR: estado sumiu sob o lock (substituído/
                    # limpo por outra task) → cai para NOVO ENVIO

        # ═════════════════════════════════════════════════════════════
        # NOVO ENVIO (sem post parente vivo)
        # ═════════════════════════════════════════════════════════════
        return await _aplicar_novo_envio(
            montada, norm, ofertas, score, identity)

