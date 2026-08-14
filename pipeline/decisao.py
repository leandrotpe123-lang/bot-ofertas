"""
Módulo 2 — Decisão de evolução.

Responsabilidade ÚNICA: dado o candidato (norm/montada/score) e o estado
atual da identidade, DECIDIR — ignorar, evoluir o post existente, ou (sem
estado) publicar novo. NÃO aplica nada no destino: publicar/editar/
substituir é o Módulo 3 (publicação).

Camada PURA: não toca Telegram, não toca banco, não loga. Recebe fatos e
devolve uma Decisao. O chamador registra os logs e executa a ação.

Política preservada (idêntica à atual; afinar é passo posterior):
  - cupom com código(s) novo(s) → evolui (edita), mesmo de outro grupo;
  - score MAIOR → evolui;
  - fora da janela, outro grupo com score <= líder → ignora (LIDER_TRAVADO);
  - teto de edições → ignora SOMENTE o ramo de evolução (Frente 0 §5):
    não encerra a família, não bloqueia sincronização/reativação/renascimento;
  - score igual e texto quase idêntico → ignora (DUP_SILENCIOSO);
  - resto → ignora (SCORE_NAO_EVOLUI).

[FASE 2] SEPARAÇÃO CONTEÚDO × MÍDIA
------------------------------------
A decisão de MÍDIA deixou de morar aqui. Este módulo decide o CONTEÚDO;
pipeline.midia_politica decide a IMAGEM. As duas são calculadas em
paralelo e viajam juntas na mesma Decisao, mas nenhuma manda na outra:

  - `acao`/`motivo`      → o que acontece com o TEXTO
  - `trocar_midia`       → o que acontece com a IMAGEM
  - IGNORAR + trocar_midia=True é um resultado VÁLIDO e frequente
    (o texto não evolui, mas a imagem sobe de classe).

O antigo `TROCA_IMG_BOA` foi REMOVIDO. Ele era um EVOLUIR, e evolução
grava o texto novo — trocar a imagem sobrescrevia o texto vencedor.
Agora é TROCA/upgrade na política de mídia, sem consumir edit_count e
sem depender de config._JANELA_REENVIO_MIDIA_S (uma imagem boa que
chega 45s depois precisa poder substituir uma ruim).

INVARIANTE: nenhum caminho de publicação pode alterar a mídia publicada
contra politica_midia(). Por isso `permite_substituir` e
`exigir_imagem` são REBAIXADOS por _com_midia() quando a política diz
PRESERVA — senão o fallback de substituição (delete+repost) reintroduzia
a imagem nova pela porta dos fundos, e de forma irrecuperável: guardamos
midia_chat, não os bytes da imagem publicada.
"""
from __future__ import annotations

from dataclasses import dataclass

import config
from config import _MAX_EDITS
from pipeline.midia_politica import politica_midia
from pipeline.score import V_CONTEUDO, V_LEGADO, score_na_escala
from pipeline.vida_oferta import viva
from pipeline.reativacao import eh_reativacao

# Ações possíveis
IGNORAR  = "IGNORAR"
EVOLUIR  = "EVOLUIR"
PUBLICAR = "PUBLICAR"   # sem estado: não há post parente vivo
RENASCER = "RENASCER"   # reativação em ciclo vivo → post NOVO (novo ciclo)
SINCRONIZAR = "SINCRONIZAR"   # edição do líder → espelha conteúdo (sem edit_count)


@dataclass
class Decisao:
    acao: str                      # IGNORAR | EVOLUIR | PUBLICAR
    motivo: str = ""               # rótulo p/ log
    novo_score: int = 0
    exigir_imagem: bool = False
    permite_substituir: bool = False
    # [FASE 2] decisão de MÍDIA — independente da de conteúdo.
    # Governa TODOS os caminhos que podem tocar a imagem publicada:
    # edição, fallback de substituição e sincronização.
    trocar_midia: bool = False
    motivo_midia: str = ""
    # contexto p/ logs fiéis no chamador:
    na_janela: bool = False
    score_atual: int = 0
    delta: int = 0
    sim: float = 0.0


def decidir(norm, montada, score: int, estado: dict | None,
            agora: float, is_edit: bool = False,
            cupons_novos: int = 0) -> Decisao:
    """Decide a ação para um candidato: PUBLICAR (sem estado vivo),
    EVOLUIR ou IGNORAR (com estado). Não executa nada."""
    if not estado:
        return Decisao(PUBLICAR, "SEM_ESTADO")

    na_janela   = viva(estado.get("janela_fim", 0) or 0, agora)
    lider_atual = estado.get("lider", "") or ""
    edit_count  = estado.get("edit_count", 0) or 0
    texto_atual = estado.get("texto", "") or ""
    ts_anterior = estado.get("ts", 0) or 0
    score_atual = estado["score"]

    # [FASE 4] Toda comparacao acontece na ESCALA DO POST. Posts
    # anteriores ao deploy (score_versao NULL) estao em v1; o candidato
    # e projetado para v1 antes de comparar. Nunca cruzamos escalas.
    versao_post = estado.get("score_versao") or V_LEGADO
    score_cmp = score_na_escala(score, norm.chat, norm.tem_midia,
                                versao_post)

    # ══ [FASE 2] POLÍTICA DE MÍDIA ══
    # Calculada UMA vez, no topo, e anexada a toda Decisao que sair
    # daqui. É independente de score, texto, líder, edit_count e
    # janela — só olha a classe da imagem publicada contra a nova.
    trocar_midia, motivo_midia = politica_midia(
        montada.imagem, norm.chat, estado, is_edit)

    def _com_midia(d: Decisao) -> Decisao:
        """Anexa a decisão de mídia e faz a política GOVERNAR os
        caminhos de escrita de imagem. Sob PRESERVA, nem edit_message
        (file=) nem o fallback de substituição podem entrar."""
        d.trocar_midia = trocar_midia
        d.motivo_midia = motivo_midia
        d.permite_substituir = d.permite_substituir and trocar_midia
        d.exigir_imagem = d.exigir_imagem and trocar_midia
        return d

    # ══ VIDA DA OFERTA — a autoridade é vida_oferta.viva() ══
    # Fora da vida do ciclo: estado FINAL, congelado — nada mais evolui
    # (nem score, nem imagem, nem cupom). O valor mora no dono, não aqui.
    # Mensagem NOVA que casa em ciclo morto nasce como CICLO NOVO
    # (Frente 0 §6 / Fase 3: fora da vida não há veto — há renascimento
    # por novo envio). Edição não cria ciclo: mantém o veto.
    #
    # [FASE 2] Ciclo morto NÃO recebe upgrade de mídia: a Doutrina diz
    # "estado FINAL, congelado — nem score, nem imagem". Estas duas
    # saídas são as ÚNICAS que não passam por _com_midia(), e é
    # deliberado: trocar_midia fica False.
    if not na_janela:
        if is_edit:
            return Decisao(IGNORAR, "JANELA_ENCERRADA",
                           na_janela=na_janela, score_atual=score_atual)
        return Decisao(PUBLICAR, "JANELA_ENCERRADA_NOVO_CICLO",
                       na_janela=na_janela, score_atual=score_atual)

    # ══ RENASCIMENTO — regra de negócio F-R (#1) ══
    # Ciclo vivo + sinal explícito de retorno ("voltou", "restock"...):
    # a oferta voltou a ficar disponível → publica post NOVO (novo ciclo),
    # sem evoluir o atual. Fica ACIMA da evolução: o teto (§5) não o afeta.
    # Cupom novo SOZINHO não dispara (regra #2): sem sinal de retorno, segue
    # para o ramo de cupom abaixo. Frequência (1 por identidade/janela) é do
    # throttle da dedup — esta camada é pura e só reconhece o evento.
    # Só em mensagem NOVA (not is_edit): edição é sincronização (F‑S), não retorno.
    #
    # [FASE 2] RENASCER cria post NOVO: a mídia dele é a da própria
    # mensagem, não uma troca no post antigo. trocar_midia fica False.
    if not is_edit and eh_reativacao(norm.texto_limpo):
        return Decisao(RENASCER, "RENASCIMENTO",
                       na_janela=na_janela, score_atual=score_atual)

    # ══ SINCRONIZAÇÃO — regra de negócio F-S ══
    # Edição vinda do LÍDER (dono do conteúdo no ar) → espelha o conteúdo
    # completo no post, SEM consumir edit_count e SEM limite de vezes no
    # ciclo. Não é disputa por score (isso é evolução, entre candidatos):
    # é o espelho do vencedor editando a própria mensagem. Só edição
    # (is_edit); mensagem nova segue para evolução abaixo. Se a edição mudou
    # a identidade a ponto de não casar, o overlap nem trouxe este post →
    # vira post novo pelo fluxo normal (regra #6), não aqui.
    #
    # [FASE 2] A sincronização espelha o TEXTO do líder — a imagem
    # continua governada pela política. Um líder de mídia ruim editando
    # a própria mensagem NÃO rebaixa mais a imagem boa publicada.
    if is_edit and lider_atual and norm.chat == lider_atual:
        return _com_midia(Decisao(SINCRONIZAR, "SINCRONIZACAO",
                                  na_janela=na_janela,
                                  score_atual=score_atual))

    # ══ TETO DE EVOLUÇÃO — Doutrina Frente 0 §5 ══
    # O teto trava SOMENTE o ramo de evolução; NÃO encerra a família.
    # A família só termina na fronteira do histórico (janela_fim <= agora),
    # já tratada acima (JANELA_ENCERRADA). Reativação, sincronização e
    # renascimento, quando existirem, entram ACIMA deste ponto e não são
    # afetados pelo teto. Por isso a verificação vive DENTRO de cada saída
    # de evolução — nunca como um portão acima de todas elas.
    #
    # [FASE 2] O teto é orçamento de EVOLUÇÃO TEXTUAL. Upgrade de mídia
    # não o consome e não é bloqueado por ele: as saídas de
    # EVOLUCAO_LIMITE_ATINGIDO passam por _com_midia() e podem sair com
    # trocar_midia=True.
    tem_orcamento = edit_count < _MAX_EDITS

    # ── CUPOM ENRIQUECIDO: código(s) novo(s) → edita o mesmo post ──
    if cupons_novos > 0:
        if not tem_orcamento:
            return _com_midia(Decisao(IGNORAR, "EVOLUCAO_LIMITE_ATINGIDO",
                                      na_janela=na_janela,
                                      score_atual=score_atual))
        # [FASE 4] O max() só é válido DENTRO de uma escala. Contra um
        # post v1, score_atual carrega peso histórico de mídia e `score`
        # é conteúdo puro: o max() escolheria o maior NÚMERO, não o
        # maior conteúdo, e _aplicar_evolucao gravaria esse valor com
        # score_versao=V_CONTEUDO — um score v1 rotulado v2, que
        # envenenaria toda comparação seguinte.
        # Contra v1 usamos o score do candidato direto: é o único valor
        # que sabemos ser conteúdo puro. O score do post v1 não é
        # decomponível, então não há piso legítimo a preservar.
        novo = (max(score, score_atual) if versao_post == V_CONTEUDO
                else score)
        return _com_midia(Decisao(
            EVOLUIR, "CUPOM_ENRIQUECIDO",
            novo_score=novo,
            exigir_imagem=False, permite_substituir=False,
            na_janela=na_janela, score_atual=score_atual))

    # ── DECISÃO 1: score MAIOR → evolui (edita; fallback substitui) ──
    if score_cmp > score_atual:
        if not tem_orcamento:
            return _com_midia(Decisao(IGNORAR, "EVOLUCAO_LIMITE_ATINGIDO",
                                      na_janela=na_janela,
                                      score_atual=score_atual))
        return _com_midia(Decisao(
            EVOLUIR, "EVOLUI", novo_score=score,
            exigir_imagem=bool(montada.imagem), permite_substituir=True,
            na_janela=na_janela, score_atual=score_atual))

    # ── DECISÃO 2: score IGUAL ──
    # [FASE 2] O ramo TROCA_IMG_BOA foi REMOVIDO daqui. Ele era um
    # EVOLUIR — e evolução grava o texto novo, então trocar a imagem
    # sobrescrevia o texto vencedor por um texto que não venceu.
    # A troca virou TROCA/upgrade na política de mídia: acontece via
    # IGNORAR + trocar_midia=True, sem tocar texto/score/líder/orçamento
    # e sem a janela de config._JANELA_REENVIO_MIDIA_S.
    if score_cmp == score_atual:
        delta = int(agora - ts_anterior)

        from utils.textos import _alma, _sim
        sim_v = _sim(_alma(montada.texto), _alma(texto_atual))
        if sim_v > 0.85:
            # DUP_SILENCIOSO ignora o TEXTO — mas a mídia segue
            # governada pela política: uma imagem boa chegando num
            # texto duplicado ainda pode fazer upgrade.
            return _com_midia(Decisao(IGNORAR, "DUP_SILENCIOSO",
                                      na_janela=na_janela,
                                      score_atual=score_atual,
                                      sim=sim_v, delta=delta))

    # ── DECISÃO 3: resto → ignora ──
    return _com_midia(Decisao(IGNORAR, "SCORE_NAO_EVOLUI",
                              na_janela=na_janela, score_atual=score_atual))
              
