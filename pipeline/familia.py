"""
Camada — Família de ofertas.

Responsabilidade ÚNICA: dado um conjunto de ofertas, responder
  (a) qual post lógico as acolhe, e
  (b) qual passa a ser a composição da família.

A regra "a família só cresce" (união) vive aqui, uma única vez.

NÃO faz:
  - decidir publicar/editar/ignorar     (pipeline.decisao)
  - travar o post                       (pipeline.exclusao)
  - aplicar o efeito no destino         (pipeline.publicacao)
  - falar com o Telegram                (pipeline.saida)

Contrato público:
  post_da_familia(ofertas, dest_fix)   -> int | None
  unir(msg_id_dest, ofertas)           -> list[str]
  compartilhadas(msg_id_dest, ofertas) -> list[str]
"""
from __future__ import annotations

from database import db_get_post, db_overlap_posts, db_ofertas_de_post
from logger import log_out

__all__ = ["post_da_familia", "unir", "compartilhadas"]


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


def compartilhadas(msg_id_dest: int, ofertas: list) -> list:
    """Interseção entre as ofertas do post e as da mensagem (uso: log)."""
    return sorted(set(ofertas) & set(db_ofertas_de_post(msg_id_dest)))


def unir(msg_id_dest: int, ofertas: list) -> list:
    """UNIÃO DA FAMÍLIA — regra de negócio: as ofertas registradas passam
    a ser a UNIÃO das do post existente com as da mensagem (X + Y). Lê o
    post ANTES de remover/regravar; a família só cresce, então a união é
    sempre superconjunto do post e nada legítimo se perde. Sem isto, o
    registro gravaria só as ofertas da mensagem e descartaria as
    exclusivas do post — quebrando a conectividade e duplicando a
    família."""
    return sorted(set(db_ofertas_de_post(msg_id_dest)) | set(ofertas))


def post_da_familia(ofertas: list, dest_fix=None):
    """Post vivo que acolhe estas ofertas, ou None se não houver.
    `dest_fix` fixa o alvo pelo vínculo de Origem (I2) e curto-circuita
    a busca por sobreposição."""
    candidatos = ([(dest_fix, 0)] if dest_fix
                  else db_overlap_posts(ofertas))
    if len(candidatos) > 1:
        log_out.debug(
            f"🧬 [FAMILIA_MULTI] {len(candidatos)} posts em sobreposição "
            f"p/ ofertas={ofertas} — escolhendo o melhor candidato")
    if not candidatos:
        return None
    msg_id_rel = _escolher_post(candidatos)
    log_out.info(f"🔎 [OVERLAP_MATCH] post:{msg_id_rel} casou por ofertas_compartilhadas={compartilhadas(msg_id_rel, ofertas)} | candidato={sorted(ofertas)}")
    return msg_id_rel
