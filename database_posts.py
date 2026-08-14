"""Camada 7 — Banco / Persistência de posts, ofertas e origem.

Responsabilidade ÚNICA: o modelo container/oferta — `post_estado` e
`oferta_index` — e o vínculo `origem_post`.

Por que origem_post mora AQUI e não em módulo próprio: db_registrar_post
grava o vínculo de Origem na MESMA transação do post, por exigência dos
invariantes I4/I5 ("cobertura por construção", ver a docstring da
função). Separar as tabelas separaria a transação e quebraria o
invariante. Origem é apêndice transacional do post, não domínio
autônomo.

NÃO conhece link, cupom nem encurtador.

Usa _db de database_conexao — contrato interno da camada.

Extraído de database sem qualquer alteração de comportamento.
"""
from __future__ import annotations
import time
from typing import Optional

from logger import log_db

# Contrato INTERNO da camada — ver cabeçalho de database_conexao.
from database_conexao import _db

# ── post_estado / oferta_index (modelo container/oferta) ──────────
def db_get_post(msg_id_dest: int) -> Optional[dict]:
    """Estado real do post, consumido por decidir(...)."""
    try:
        with _db() as db:
            row = db.execute(
                "SELECT msg_id_dest,score,texto,plat,lider,janela_fim,"
                "edit_count,ts,midia_chat,score_versao"
                " FROM post_estado WHERE msg_id_dest=?",
                (msg_id_dest,)).fetchone()
        if row:
            return {
                "msg_id_dest": row[0], "score": row[1], "texto": row[2],
                "plat": row[3], "lider": row[4] or "",
                "janela_fim": row[5] or 0.0, "edit_count": row[6] or 0,
                "ts": row[7],
                # NÃO normalizar com `or ""`: None (legado) e ""
                # (post sem mídia) são estados DISTINTOS.
                "midia_chat": row[8],
                # NÃO normalizar: None = legado v1, 2 = conteúdo puro.
                "score_versao": row[9],
            }
    except Exception as e:
        log_db.error(f"❌ db_get_post: {e}")
    return None

def db_overlap_posts(ofertas: list[str]) -> list[tuple[int, int]]:
    """Posts VIVOS que compartilham >=1 oferta com a lista dada, como
    (msg_id_dest, n_sobreposicao), ordenados por sobreposicao desc.
    V3: só entram posts cujo ciclo de vida ainda está aberto
    (post_estado.janela_fim > agora) — a estampa gravada no nascimento
    (vida_oferta) É a autoridade; o overlap apenas não enxerga os mortos.
    Fora da vida, a oferta não casa e renasce como post novo. A retenção
    do banco (30d) volta a ser só lixeira, nunca regra de família.
    Lista vazia → []."""
    if not ofertas:
        return []
    try:
        agora = time.time()
        marcadores = ",".join("?" * len(ofertas))
        with _db() as db:
            rows = db.execute(
                f"SELECT oi.msg_id_dest, COUNT(*) AS n"
                f"  FROM oferta_index oi"
                f"  JOIN post_estado pe ON pe.msg_id_dest = oi.msg_id_dest"
                f" WHERE oi.identity IN ({marcadores})"
                f"   AND pe.janela_fim > ?"
                f" GROUP BY oi.msg_id_dest ORDER BY n DESC",
                tuple(ofertas) + (agora,)).fetchall()
        return [(r[0], r[1]) for r in rows]
    except Exception as e:
        log_db.error(f"❌ db_overlap_posts: {e}")
        return []


def db_ofertas_de_post(msg_id_dest: int) -> list[str]:
    """Todas as ofertas que compõem um post (caminho inverso, via
    idx_oi_dest). Usado para calcular ofertas novas na evolução e
    re-apontar na substituição."""
    try:
        with _db() as db:
            rows = db.execute(
                "SELECT identity FROM oferta_index WHERE msg_id_dest=?",
                (msg_id_dest,)).fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        log_db.error(f"❌ db_ofertas_de_post: {e}")
        return []

def db_registrar_post(msg_id_dest: int, ofertas: list[str], score: int,
                      texto: str, plat: str, lider: str = "",
                      janela_fim: float = 0.0, edit_count: int = 0,
                      *, chat_origem: str = "", msg_id_origem: int = 0,
                      midia_chat: Optional[str] = None,
                      score_versao: Optional[int] = None):

    """Upsert do estado do post + mapeamento de cada oferta→post.
    Serve para publicação nova E evolução (idempotente).
    Se (chat_origem, msg_id_origem) vierem, grava o vínculo Origem na
    MESMA transação (invariantes I4/I5 — cobertura por construção)."""
    try:
        agora = time.time()
        with _db() as db:
            # midia_chat=None significa PRESERVAR o valor atual (o
            # COALESCE resolve contra a própria linha). Só quem decide
            # mídia passa valor explícito — "" (post sem mídia) ou o
            # chat de origem. Nunca gravamos NULL de propósito: NULL é
            # exclusivamente o legado pré-Fase 2.
            db.execute(
                "INSERT OR REPLACE INTO post_estado"
                "(msg_id_dest,score,texto,plat,lider,"
                "janela_fim,edit_count,ts,midia_chat,score_versao)"
                " VALUES(?,?,?,?,?,?,?,?,"
                " COALESCE(?,(SELECT midia_chat FROM post_estado"
                "             WHERE msg_id_dest=?)),"
                " COALESCE(?,(SELECT score_versao FROM post_estado"
                "             WHERE msg_id_dest=?)))",
                (msg_id_dest, score, texto, plat, lider,
                 janela_fim, edit_count, agora,
                 midia_chat, msg_id_dest,
                 score_versao, msg_id_dest))
            for oferta in ofertas:
                db.execute(
                    "INSERT OR REPLACE INTO oferta_index"
                    "(identity,msg_id_dest,ts) VALUES(?,?,?)",
                    (oferta, msg_id_dest, agora))
            if chat_origem and msg_id_origem:
                db.execute(
                    "INSERT OR REPLACE INTO origem_post"
                    "(chat,msg_id,dest,ts) VALUES(?,?,?,?)",
                    (chat_origem, msg_id_origem, msg_id_dest, agora))
    except Exception as e:
        log_db.error(f"❌ db_registrar_post: {e}")

def db_origem_get(chat: str, msg_id: int):
    """Vínculo Origem (Fase 1): dest do post lógico desta origem, ou None."""
    try:
        with _db() as db:
            row = db.execute(
                "SELECT dest FROM origem_post WHERE chat=? AND msg_id=?",
                (chat, msg_id)).fetchone()
        return int(row[0]) if row else None
    except Exception as e:
        log_db.error(f"❌ db_origem_get: {e}")
        return None


def db_origem_set(chat: str, msg_id: int, dest: int):
    """REPLACE idempotente do vínculo Origem (I1)."""
    try:
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO origem_post(chat,msg_id,dest,ts)"
                " VALUES(?,?,?,?)", (chat, msg_id, dest, time.time()))
    except Exception as e:
        log_db.error(f"❌ db_origem_set: {e}")


def db_remover_post(msg_id_dest: int):
    """Apaga o post e TODAS as suas ofertas do índice. Uso restrito ao
    fallback de SUBSTITUIÇÃO (msg antiga apagada no Telegram, nova criada),
    chamado no id ANTIGO antes de db_registrar_post(novo, ...). As ofertas
    levadas à nova msg são reapontadas pelo próprio INSERT OR REPLACE do
    db_registrar_post; esta função existe para (a) remover o post_estado
    órfão e (b) impedir que ofertas do post antigo NÃO levadas à nova msg
    fiquem apontando para uma mensagem já apagada. NÃO usar no caminho de
    EDIÇÃO (msg_id preservado) — ali apagaria o post vivo."""
    try:
        with _db() as db:
            db.execute("DELETE FROM post_estado WHERE msg_id_dest=?",
                       (msg_id_dest,))
            db.execute("DELETE FROM oferta_index WHERE msg_id_dest=?",
                       (msg_id_dest,))
    except Exception as e:
        log_db.error(f"❌ db_remover_post: {e}")

