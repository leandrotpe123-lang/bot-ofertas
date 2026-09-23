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
  post_da_familia(ofertas, dest_fix, destinos) -> int | None
  tem_destino(msg_id_dest)             -> bool
  unir(msg_id_dest, ofertas)           -> list[str]
  absorver(msg_id_dest, ofertas)       -> int
  compartilhadas(msg_id_dest, ofertas) -> list[str]
"""
from __future__ import annotations

from database import (db_absorver_ofertas, db_get_post,
                      db_ofertas_de_post, db_overlap_posts)
from logger import log_out
from pipeline.resolucao_identidade import eh_chave_destino

__all__ = ["post_da_familia", "unir", "absorver", "compartilhadas",
           "tem_destino"]


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

def absorver(msg_id_dest: int, ofertas: list) -> int:
    """APRENDIZADO DE ÂNCORA — a família passa a reconhecer estas formas
    de encontrar a mesma oferta, sem que nada do post mude.

    Complementa `unir`, que compõe a família para quem VAI REGRAVAR o
    post. Aqui não há regravação: a decisão textual foi IGNORAR e o
    texto vencedor, o score, o líder, o edit_count, a janela e a mídia
    permanecem exatamente como estavam. Descobrir uma nova forma de
    reconhecer a oferta não dá a essa mensagem o direito de substituir
    o texto vencedor.

    Devolve quantas âncoras foram aprendidas (0 = nada novo).
    """
    novas = db_absorver_ofertas(msg_id_dest, ofertas)
    if novas:
        log_out.info(
            f"🧬 [ANCORA_ABSORVIDA] post:{msg_id_dest} aprendeu {novas} "
            f"âncora(s) | candidato={sorted(ofertas)} "
            f"| familia={sorted(db_ofertas_de_post(msg_id_dest))}")
    return novas


def destinos_do_post(msg_id_dest: int) -> set:
    """Destinos declarados que a família do post já reconhece."""
    return {k for k in db_ofertas_de_post(msg_id_dest) if eh_chave_destino(k)}


def tem_destino(msg_id_dest: int) -> bool:
    """FATO para a decisão: a família do post tem destino declarado?"""
    return bool(destinos_do_post(msg_id_dest))


def _acolhe(msg_id_dest: int, destinos: tuple) -> bool:
    """UM DESTINO NÃO ENTRA NA FAMÍLIA DE OUTRO DESTINO.

    Candidato sem destino declarado (só mecanismo: cupom, `/sec/`) é
    acolhido por qualquer família com que compartilhe âncora — é assim
    que o `/sec/` encontra a lista que traz o mesmo código.

    Candidato COM destino é acolhido por família sem destino (a lista
    encontra o `/sec/` que chegou antes) ou pela família do MESMO
    destino. Família de OUTRO destino não o acolhe, ainda que os dois
    compartilhem o código: duas campanhas diferentes com o mesmo cupom
    não colapsam.
    """
    if not destinos:
        return True
    do_post = destinos_do_post(msg_id_dest)
    return not do_post or not do_post.isdisjoint(destinos)


def post_da_familia(ofertas: list, dest_fix=None, destinos: tuple = ()):
    """Post vivo que acolhe estas ofertas, ou None se não houver.
    `dest_fix` fixa o alvo pelo vínculo de Origem (I2) e curto-circuita
    a busca por sobreposição (e a regra de destino: é a mesma origem).

    `destinos` são os destinos declarados do candidato (espécie
    "destino"). Vazio — toda mensagem sem destino declarado, e toda
    plataforma que não os declara — é exatamente o comportamento de
    antes: nenhum candidato é recusado."""
    candidatos = ([(dest_fix, 0)] if dest_fix
                  else db_overlap_posts(ofertas))
    if candidatos and destinos and not dest_fix:
        recusados = [mid for mid, _n in candidatos
                     if not _acolhe(mid, destinos)]
        if recusados:
            log_out.info(
                f"🧬 [FAMILIA_OUTRO_DESTINO] candidato={sorted(destinos)} "
                f"recusado por post(s) {recusados} — destinos distintos "
                f"não colapsam, mesmo com cupom em comum")
            candidatos = [(mid, n) for mid, n in candidatos
                          if mid not in recusados]
    if len(candidatos) > 1:
        log_out.debug(
            f"🧬 [FAMILIA_MULTI] {len(candidatos)} posts em sobreposição "
            f"p/ ofertas={ofertas} — escolhendo o melhor candidato")
    if not candidatos:
        return None
    msg_id_rel = _escolher_post(candidatos)
    log_out.info(f"🔎 [OVERLAP_MATCH] post:{msg_id_rel} casou por ofertas_compartilhadas={compartilhadas(msg_id_rel, ofertas)} | candidato={sorted(ofertas)} | candidatos={[c[0] for c in candidatos]} fix={dest_fix or '-'} post_ofertas={db_ofertas_de_post(msg_id_rel)}")
    return msg_id_rel
