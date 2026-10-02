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
# [Frente 8] Dois acréscimos, ambos de roteamento:
#   · post FUNDIDO nunca recebe decisão: relido sob o lock, se tiver
#     fused_into a mensagem é redirecionada ao sobrevivente (nunca vira
#     post novo);
#   · depois que um caminho de CONTEÚDO persistiu (evolução,
#     sincronização, substituição, renascimento), a convergência roda
#     com o msg_id efetivo — FORA do lock do post e ainda dentro dos
#     locks de origem e identidade. A primeira publicação de uma oferta
#     sem família viva não dispara nada.
#
# _marcar e _foi_processado NÃO são reexportados: são contrato
# interno da camada, não público.
from __future__ import annotations
import contextlib
import time
from dataclasses import replace
from typing import Optional

import globals as g

from database import db_exibida, db_get_post
from logger import log_out
from pipeline.decisao import (EVOLUIR, IGNORAR, RENASCER, SINCRONIZAR,
                              decidir)
from pipeline import convergencia
from pipeline import exclusao
from pipeline import familia
from pipeline import origem
from pipeline import origem_apagada
from pipeline import retencao_cupons
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
    # Destinos declarados do candidato — prontos do enriquecimento,
    # consumidos pela família e pela decisão. Vazio = comportamento
    # de antes.
    destinos: tuple = tuple(getattr(enr, "destinos", ()) or ())
    # [Corrida de adoção] Locks de identidade = ofertas concretas + os
    # mutexes SINTÉTICOS de adoção (cupom sem nome, container LIVE), numa
    # única ordenação — a ordem global de aquisição continua total. Os
    # mutexes só serializam: nunca entram em `ofertas` (família, banco,
    # decisão). Ver familia.mutexes_de_adocao.
    travas = sorted(set(ofertas) | set(familia.mutexes_de_adocao(
        ofertas, destinos, _container_de(norm, ofertas))))

# ── Camada 0: ORIGEM (Fase 1 do MB) — lock mais externo (I6) ──
    if norm is not None:
        async with await origem.lock_origem(norm.chat, norm.msg_id):
            if origem_apagada.apagada(norm.chat, norm.msg_id):
                # [Origem apagada] a fonte apagou esta mensagem enquanto
                # ela estava na fila: nada publica nem edita. Conferido
                # sob o MESMO lock em que a exclusão a marca.
                log_out.info(
                    f"🗑 [ORIGEM_APAGADA] ({norm.chat},{norm.msg_id}) — "
                    f"apagada na fonte antes de publicar; descartada")
                return True
            dest_fix = destino_vivo_de_origem(norm.chat, norm.msg_id)
            if dest_fix and not is_edit:
                log_out.info(
                    f"🔗 [ORIGEM_JA_PUBLICADA] ({norm.chat},{norm.msg_id})"
                    f"→post:{dest_fix} — NEW absorvido (I3)")
                return True
            if ofertas:
                async with contextlib.AsyncExitStack() as stack:
                    for chave in travas:
                        await stack.enter_async_context(
                            await exclusao.lock_identidade(chave))
                    return await _enviar_resolvido(
                        montada, norm, ofertas, score, is_edit, dest_fix,
                        destinos=destinos)
            return await _enviar_resolvido(
                montada, norm, ofertas, score, is_edit, dest_fix,
                destinos=destinos)
    if ofertas:
        async with contextlib.AsyncExitStack() as stack:
            for chave in travas:
                await stack.enter_async_context(await exclusao.lock_identidade(chave))
            return await _enviar_resolvido(montada, norm, ofertas, score,
                                           is_edit, destinos=destinos)
    return await _enviar_resolvido(montada, norm, ofertas, score, is_edit,
                                   destinos=destinos)


# ── [Frente 8] Redirecionamento de post fundido ────────────────────
_MAX_REDIRECIONAMENTOS = 3


class _PostFundido(Exception):
    """O post alvo foi fundido entre a resolução da família e o lock.
    Levantada ANTES de decidir(): nenhuma decisão sobre post fundido."""

    def __init__(self, perdedor: int, sobrevivente: int, status: str):
        super().__init__(f"post:{perdedor} fundido em post:{sobrevivente}")
        self.perdedor = perdedor
        self.sobrevivente = sobrevivente
        self.status = status


class _AdocaoObsoleta(Exception):
    """[Cupom sem nome] o post escolhido por adoção genérico × assinatura
    deixou de ser complementar entre a resolução da família e o lock.
    Levantada ANTES de decidir(): refaz a busca, sem alvo fixo."""


async def _enviar_resolvido(montada: MensagemMontada,
                            norm: Optional[MensagemNormalizada],
                            ofertas: list,
                            score: int,
                            is_edit: bool = False,
                            dest_fix=None,
                            *, destinos: tuple = ()) -> bool:
    """_enviar_inner + as duas pontas da Frente 8, dentro dos locks de
    origem e identidade que enviar() já segura:
      · post alvo fundido → refaz com o sobrevivente como alvo fixo;
      · caminhos de conteúdo que persistiram → convergência, FORA do
        lock do post (o finally roda depois de o lock ser solto)."""
    escritas: list = []
    try:
        for _ in range(_MAX_REDIRECIONAMENTOS + 1):
            try:
                return await _enviar_inner(
                    montada, norm, ofertas, score, is_edit, dest_fix,
                    destinos=destinos, escritas=escritas)
            except _PostFundido as f:
                log_out.info(
                    f"🔀 [REDIRECIONADO_FUSAO] id={montada.msg_id} "
                    f"chat={getattr(norm, 'chat', '')} post:{f.perdedor} "
                    f"→ post:{f.sobrevivente}")
                if f.status in ("pendente", "falhou"):
                    convergencia.agendar_remocao(f.perdedor)
                dest_fix = f.sobrevivente
            except _AdocaoObsoleta:
                continue
        log_out.warning(
            f"⚠️ [REDIRECIONAMENTO_ESGOTADO] id={montada.msg_id} — cadeia "
            f"de fusão com mais de {_MAX_REDIRECIONAMENTOS} saltos; "
            f"nada publicado")
        return True
    finally:
        for escrito in escritas:
            await convergencia.consolidar(escrito)


def _container_de(norm: Optional[MensagemNormalizada], ofertas: list) -> str:
    """[Container] a chave da live da mensagem, quando a identidade dela
    é outra (forte): só serve para ADOTAR o post da mesma live com
    título equivalente se o forte não achou família (familia). Vazio
    quando não há live ou quando a live já é a própria oferta."""
    if norm is None:
        return ""
    ancora = getattr(norm, "ancora_url", "") or ""
    container = f"{norm.plat}|url|{ancora}" if ancora else ""
    if container in ofertas or not familia.eh_chave_container(container):
        return ""
    return container


def _limite_texto(estado: dict, chave_aceita: str) -> int:
    """Tamanho máximo do texto do post: legenda quando ele tem (ou pode
    ter) mídia, mensagem de texto quando nasceu sem. Na dúvida, legenda."""
    midia = estado.get("midia_chat")
    if midia is None or midia or chave_aceita:
        return retencao_cupons.LIMITE_LEGENDA
    return retencao_cupons.LIMITE_TEXTO


async def _enviar_inner(montada: MensagemMontada,
                        norm: Optional[MensagemNormalizada],
                        ofertas: list,
                        score: int,
                        is_edit: bool = False,
                        dest_fix=None,
                        *, destinos: tuple = (),
                        escritas: Optional[list] = None) -> bool:
    """Corpo real de enviar() — dentro dos locks de oferta. Acha o post
    parente por sobreposição, trava o post candidato, re-verifica sob o
    lock e decide pelo score (decisão intocada).

    [Frente 8] `escritas` recebe o msg_id efetivo de cada caminho de
    conteúdo que persistiu — insumo da convergência."""
    identity = ofertas[0] if ofertas else None   # rótulo de log
    pos = escritas.append if escritas is not None else None
    # Houve post parente? Só então uma publicação NOVA (estado sumido
    # sob o lock) pode ter vizinhos a convergir. Sem família viva, a
    # primeira publicação não dispara convergência nenhuma.
    alvo_existente = False
    exibidas_msg = None          # None = as próprias ofertas (aplicadores)

    if norm is not None and (ofertas or dest_fix):
        # Alvo FIXADO pelo vínculo de Origem (I2): edit de origem
        # vinculada nunca re-casa por conteúdo em outro post.
        container = _container_de(norm, ofertas)
        # O texto publicado traz o link da live: a composição EXIBIDA
        # inclui a chave do container (fraca — não conta como estrutura),
        # para que mensagens só-live do mesmo produto continuem achando o
        # post depois que um forte virou a referência dele.
        exibidas_msg = list(ofertas) + ([container] if container else [])
        msg_id_rel = familia.post_da_familia(ofertas, dest_fix, destinos,
                                             titulo=montada.texto,
                                             container=container)
        if msg_id_rel is not None:
            alvo_existente = True
            post_lock = await exclusao.lock_post(msg_id_rel)
            async with post_lock:
                estado = db_get_post(msg_id_rel)   # re-verifica sob o lock
                if estado and estado.get("fused_into"):
                    # [Frente 8] fundido: nenhuma decisão aqui — e nunca
                    # o fallback "PUBLICAR" (ciclo morto → post novo).
                    raise _PostFundido(msg_id_rel, estado["fused_into"],
                                       estado.get("delete_status") or "")
                if not dest_fix and familia.adocao_obsoleta(msg_id_rel, ofertas):
                    # [Cupom sem nome] adoção revalidada sob o lock: o
                    # alvo deixou de ser complementar → nova busca.
                    raise _AdocaoObsoleta()
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
                # FATOS DE FAMÍLIA, também lidos sob o lock e CONGELADOS
                # pela mesma razão: as duas decisões do Portão A usam os
                # mesmos fatos. Destino > mecanismo (decisao, DECISÃO 0).
                destino_candidato = bool(destinos)
                destino_post = familia.tem_destino(msg_id_rel)
                # [Frente 8b] FATO estrutural, também congelado: a
                # composição forte do candidato contra o que o post EXIBE
                # (nunca o que ele só aprendeu, nunca a imagem).
                exibida_rel = db_exibida(msg_id_rel)
                composicao = familia.relacao_composicao(ofertas, exibida_rel)
                if (composicao is None and familia.so_container(ofertas)
                        and familia.fortes(exibida_rel)):
                    # [Container] o título casou, mas a mensagem só tem a
                    # live e o post já exibe identidade FORTE: substituir
                    # o texto apagaria o forte — o título nunca sobrescreve
                    # identidade forte. Como todo REDUZ, também não troca
                    # a imagem do post (decisao, [24423]).
                    composicao = familia.REDUZ
                # [ML lista] A versão COM DESTINO (cada cupom com a sua
                # lista) sobre o post só-mecanismo (`/sec/`) que não traz
                # todos os cupons exibidos: o texto composto mantém no ar
                # os que só o post tinha (retencao_cupons). A decisão
                # recebe a composição do que vai ser EXIBIDO — que cobre
                # tudo e acrescenta a lista (AMPLIA). Sem retenção segura,
                # nada muda: a edição segue bloqueada. O texto composto só
                # vai ao ar se a decisão for EVOLUIR (ver abaixo).
                composta = None
                if (destino_candidato and not destino_post
                        and composicao in (familia.REDUZ, familia.PARCIAL)):
                    composta = retencao_cupons.reter(
                        montada.texto, estado.get("texto") or "",
                        familia.fortes(exibida_rel) - familia.fortes(ofertas),
                        limite=_limite_texto(estado, chave_aceita))
                    if composta is not None:
                        composicao = familia.relacao_composicao(
                            list(ofertas) + list(composta.retidas),
                            exibida_rel)
                d = decidir(norm, montada, score, estado, agora, is_edit,
                            midia_key_aceita=chave_aceita,
                            midia_candidata=norm.tem_midia,
                            destino_candidato=destino_candidato,
                            destino_post=destino_post,
                            composicao=composicao,
                            retencao=composta is not None)

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
                                    midia_candidata=False,
                                    destino_candidato=destino_candidato,
                                    destino_post=destino_post,
                                    composicao=composicao,
                                    retencao=composta is not None)

                # [ML lista] Só a EVOLUÇÃO leva o texto composto. Na
                # SINCRONIZAÇÃO o líder espelha a PRÓPRIA mensagem e o
                # bloco retido (de outra fonte) que já está no ar fica —
                # nunca um código que o próprio líder tirou. Qualquer
                # outra ação: nada composto.
                if d.acao == SINCRONIZAR:
                    composta = retencao_cupons.manter(
                        montada.texto, estado.get("texto") or "", ofertas,
                        exibida_rel,
                        limite=_limite_texto(estado, chave_aceita))
                elif d.acao != EVOLUIR:
                    composta = None
                exibir = list(ofertas)
                if composta is not None:
                    montada = replace(montada, texto=composta.texto)
                    exibir = list(ofertas) + list(composta.retidas)
                    exibidas_msg = list(exibidas_msg) + list(composta.retidas)
                    if d.acao == SINCRONIZAR:
                        d = replace(d, texto_igual=(
                            montada.texto == (estado.get("texto") or "")))
                    log_out.info(
                        f"🧩 [CUPONS_RETIDOS] post:{msg_id_rel} {d.motivo} "
                        f"manteve {list(composta.retidas)} "
                        f"(id={norm.msg_id} chat={norm.chat})")

                # ══ [Frente 8b] VEREDITO ESTRUTURAL — autoridade única ══
                # Toda escrita de CONTEÚDO num post existente (evolução,
                # sincronização, renascimento) passa pelo MESMO veredito,
                # antes de ir ao Telegram. BLOQUEAR = a escrita criaria
                # duplicidade que nenhum conteúdo real resolve: o texto
                # não é aplicado (IGNORAR) e o estado válido anterior
                # fica. A mídia continua sendo decidida pela política —
                # IGNORAR + trocar_midia segue o caminho de upgrade.
                if d.acao in (EVOLUIR, SINCRONIZAR, RENASCER):
                    v = convergencia.avaliar(
                        -1 if d.acao == RENASCER else msg_id_rel,
                        exibir if d.acao != RENASCER else ofertas, score,
                        superar=(msg_id_rel if d.acao == RENASCER else None),
                        chat=norm.chat, msg_id=norm.msg_id)
                    if not v.permitido:
                        d = replace(d, acao=IGNORAR, motivo=v.motivo)
                elif d.motivo == "COMPOSICAO_PERDERIA_IDENTIDADE":
                    # Texto já bloqueado pela composição; a auditoria só
                    # registra se o conteúdo do candidato ficou sem
                    # representação real (mesma autoridade, mesmo registro).
                    convergencia.auditar_composicao(
                        msg_id_rel, ofertas, score,
                        chat=norm.chat, msg_id=norm.msg_id)

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
                            identity, exibidas=exibidas_msg,
                            superar=msg_id_rel, pos_escrita=pos)

                    if d.acao == "SINCRONIZAR":
                        # Edição do líder → espelha o conteúdo no post,
                        # SEM incrementar edit_count (não é evolução).
                        # Preserva família (união), líder, janela e o
                        # próprio contador.
                        ofertas_familia = familia.unir(msg_id_rel, ofertas)
                        return await _aplicar_sincronizacao(
                            montada, norm, score, estado, msg_id_rel,
                            ofertas_familia, identity, d,
                            exibidas=exibidas_msg, pos_escrita=pos)

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
                        edit_count, ofertas_familia, identity,
                        exibidas=exibidas_msg, pos_escrita=pos)
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
        montada, norm, ofertas, score, identity,
        exibidas=exibidas_msg,
        pos_escrita=(pos if alvo_existente else None))
 
