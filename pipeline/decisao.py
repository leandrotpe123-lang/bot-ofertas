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
  - score igual, imagem boa trocando imagem ruim na janela de mídia →
    evolui (troca imagem);
  - score igual e texto quase idêntico → ignora (DUP_SILENCIOSO);
  - resto → ignora (SCORE_NAO_EVOLUI).
"""
from __future__ import annotations

from dataclasses import dataclass

import config
from config import _MAX_EDITS
from pipeline.score import midia_ruim
from pipeline.vida_oferta import viva
from pipeline.reativacao import eh_reativacao

# Ações possíveis
IGNORAR  = "IGNORAR"
EVOLUIR  = "EVOLUIR"
PUBLICAR = "PUBLICAR"   # sem estado: não há post parente vivo
RENASCER = "RENASCER"   # reativação em ciclo vivo → post NOVO (novo ciclo)


@dataclass
class Decisao:
    acao: str                      # IGNORAR | EVOLUIR | PUBLICAR
    motivo: str = ""               # rótulo p/ log
    novo_score: int = 0
    exigir_imagem: bool = False
    permite_substituir: bool = False
    # contexto p/ logs fiéis no chamador:
    na_janela: bool = False
    score_atual: int = 0
    delta: int = 0
    sim: float = 0.0


def decidir(norm, montada, score: int, estado: dict | None,
            agora: float, is_edit: bool = False) -> Decisao:
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

    # ══ VIDA DA OFERTA — a autoridade é vida_oferta.viva() ══
    # Fora da vida do ciclo: estado FINAL, congelado — nada mais evolui
    # (nem score, nem imagem, nem cupom). O valor mora no dono, não aqui.
    if not na_janela:
        return Decisao(IGNORAR, "JANELA_ENCERRADA",
                       na_janela=na_janela, score_atual=score_atual)

    # ══ RENASCIMENTO — regra de negócio F-R (#1) ══
    # Ciclo vivo + sinal explícito de retorno ("voltou", "restock"...):
    # a oferta voltou a ficar disponível → publica post NOVO (novo ciclo),
    # sem evoluir o atual. Fica ACIMA da evolução: o teto (§5) não o afeta.
    # Cupom novo SOZINHO não dispara (regra #2): sem sinal de retorno, segue
    # para o ramo de cupom abaixo. Frequência (1 por identidade/janela) é do
    # throttle da dedup — esta camada é pura e só reconhece o evento.
    # Só em mensagem NOVA (not is_edit): edição é sincronização (F‑S), não retorno.
    if not is_edit and eh_reativacao(norm.texto_limpo):
        return Decisao(RENASCER, "RENASCIMENTO",
                       na_janela=na_janela, score_atual=score_atual)
    # ══ TETO DE EVOLUÇÃO — Doutrina Frente 0 §5 ══
    # O teto trava SOMENTE o ramo de evolução; NÃO encerra a família.
    # A família só termina na fronteira do histórico (janela_fim <= agora),
    # já tratada acima (JANELA_ENCERRADA). Reativação, sincronização e
    # renascimento, quando existirem, entram ACIMA deste ponto e não são
    # afetados pelo teto. Por isso a verificação vive DENTRO de cada saída
    # de evolução — nunca como um portão acima de todas elas.
    tem_orcamento = edit_count < _MAX_EDITS

    # ── CUPOM ENRIQUECIDO: código(s) novo(s) → edita o mesmo post ──
    novos_cup = getattr(norm, "_cupom_novos", 0)
    if novos_cup > 0:
        if not tem_orcamento:
            return Decisao(IGNORAR, "EVOLUCAO_LIMITE_ATINGIDO",
                           na_janela=na_janela, score_atual=score_atual)
        return Decisao(
            EVOLUIR, "CUPOM_ENRIQUECIDO",
            novo_score=max(score, score_atual),
            exigir_imagem=False, permite_substituir=False,
            na_janela=na_janela, score_atual=score_atual)

    # ── DECISÃO 1: score MAIOR → evolui (edita; fallback substitui) ──
    if score > score_atual:
        if not tem_orcamento:
            return Decisao(IGNORAR, "EVOLUCAO_LIMITE_ATINGIDO",
                           na_janela=na_janela, score_atual=score_atual)
        return Decisao(
            EVOLUIR, "EVOLUI", novo_score=score,
            exigir_imagem=bool(montada.imagem), permite_substituir=True,
            na_janela=na_janela, score_atual=score_atual)

    # ── DECISÃO 2: score IGUAL ──
    if score == score_atual:
        delta = int(agora - ts_anterior)
        if (midia_ruim(lider_atual) and not midia_ruim(norm.chat)
                and montada.imagem
                and (agora - ts_anterior) < config._JANELA_REENVIO_MIDIA_S):
            if not tem_orcamento:
                return Decisao(IGNORAR, "EVOLUCAO_LIMITE_ATINGIDO",
                               na_janela=na_janela, score_atual=score_atual)
            return Decisao(
                EVOLUIR, "TROCA_IMG_BOA", novo_score=score,
                exigir_imagem=True, permite_substituir=True,
                na_janela=na_janela, score_atual=score_atual, delta=delta)

        from utils.textos import _alma, _sim
        sim_v = _sim(_alma(montada.texto), _alma(texto_atual))
        if sim_v > 0.85:
            return Decisao(IGNORAR, "DUP_SILENCIOSO",
                           na_janela=na_janela, score_atual=score_atual,
                           sim=sim_v)

    # ── DECISÃO 3: resto → ignora ──
    return Decisao(IGNORAR, "SCORE_NAO_EVOLUI",
                   na_janela=na_janela, score_atual=score_atual)
