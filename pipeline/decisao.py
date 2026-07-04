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
  - teto de edições fora da janela → ignora (MAX_EDITS);
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

# Ações possíveis
IGNORAR  = "IGNORAR"
EVOLUIR  = "EVOLUIR"
PUBLICAR = "PUBLICAR"   # sem estado: não há post parente vivo


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
            agora: float) -> Decisao:
    """Decide a ação para um candidato: PUBLICAR (sem estado vivo),
    EVOLUIR ou IGNORAR (com estado). Não executa nada."""
    if not estado:
        return Decisao(PUBLICAR, "SEM_ESTADO")

    na_janela   = agora < (estado.get("janela_fim", 0) or 0)
    lider_atual = estado.get("lider", "") or ""
    edit_count  = estado.get("edit_count", 0) or 0
    texto_atual = estado.get("texto", "") or ""
    ts_anterior = estado.get("ts", 0) or 0
    score_atual = estado["score"]

    # ══ VIDA DA OFERTA — só evolui DENTRO da janela e no MÁXIMO 1x ══
    # Fora da janela (_JANELA_DISPUTA_S = 90s): estado FINAL, congelado —
    # nada mais evolui (nem score, nem imagem, nem cupom).
    if not na_janela:
        return Decisao(IGNORAR, "JANELA_ENCERRADA",
                       na_janela=na_janela, score_atual=score_atual)
    # Dentro da janela, mas já evoluiu 1x → limite atingido, trava total.
    if edit_count >= _MAX_EDITS:
        return Decisao(IGNORAR, "EVOLUCAO_LIMITE_ATINGIDO",
                       na_janela=na_janela, score_atual=score_atual)

    # ── Daqui pra baixo: DENTRO da janela e com 1 evolução disponível ──

    # ── CUPOM ENRIQUECIDO: código(s) novo(s) → edita o mesmo post ──
    novos_cup = getattr(norm, "_cupom_novos", 0)
    if novos_cup > 0:
        return Decisao(
            EVOLUIR, "CUPOM_ENRIQUECIDO",
            novo_score=max(score, score_atual),
            exigir_imagem=False, permite_substituir=False,
            na_janela=na_janela, score_atual=score_atual)

    # ── DECISÃO 1: score MAIOR → evolui (edita; fallback substitui) ──
    if score > score_atual:
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
