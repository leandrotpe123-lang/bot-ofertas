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
from pipeline.resolucao_identidade import eh_chave_forte

# Contrato INTERNO da camada — ver cabeçalho de database_conexao.
from database_conexao import _db

# ─────────────────────────────────────────────────────────────────
# [Frente 8] POSSE × EXIBIÇÃO
#
#   oferta_index  — MEMÓRIA da família: quem é o DONO de cada âncora.
#                   Cresce por união e por aprendizado (absorver).
#   post_exibida  — o que o post EXIBE agora: as âncoras da mensagem
#                   cujo texto está no ar. Muda só quando o conteúdo
#                   muda (publicação, evolução, sincronização).
#
# A fusão se prova SÓ pelo que é exibido: uma âncora aprendida não diz
# que o post mostra aquele produto, cupom ou destino.
#
# POSSE DE ÂNCORA FORTE: um post vivo nunca perde uma âncora forte por
# gravação normal. Só a fusão (db_fundir_posts) e o renascimento
# (`superar`) transferem posse. Âncoras fracas seguem a política de
# sempre (a última gravação aponta).
# ─────────────────────────────────────────────────────────────────
_VIVO_SQL = "janela_fim > ? AND fused_into IS NULL"
_VIVO_PE = "pe.janela_fim > ? AND pe.fused_into IS NULL"


# ── post_estado / oferta_index (modelo container/oferta) ──────────
def db_get_post(msg_id_dest: int) -> Optional[dict]:
    """Estado real do post, consumido por decidir(...)."""
    try:
        with _db() as db:
            row = db.execute(
                "SELECT msg_id_dest,score,texto,plat,lider,janela_fim,"
                "edit_count,ts,midia_chat,score_versao,fused_into,"
                "delete_status"
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
                # [Frente 8] None = post nunca fundido.
                "fused_into": row[10],
                "delete_status": row[11],
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
    [Frente 8] A família de um post é o que ele POSSUI (oferta_index) e o
    que ele EXIBE (post_exibida); post fundido não é candidato.
    Lista vazia → []."""
    if not ofertas:
        return []
    try:
        agora = time.time()
        marcadores = ",".join("?" * len(ofertas))
        with _db() as db:
            rows = db.execute(
                f"SELECT x.msg_id_dest, COUNT(DISTINCT x.identity) AS n"
                f"  FROM (SELECT identity, msg_id_dest FROM oferta_index"
                f"         WHERE identity IN ({marcadores})"
                f"        UNION"
                f"        SELECT identity, msg_id_dest FROM post_exibida"
                f"         WHERE identity IN ({marcadores})) x"
                f"  JOIN post_estado pe ON pe.msg_id_dest = x.msg_id_dest"
                f" WHERE pe.janela_fim > ? AND pe.fused_into IS NULL"
                f" GROUP BY x.msg_id_dest ORDER BY n DESC",
                tuple(ofertas) * 2 + (agora,)).fetchall()
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

def db_absorver_ofertas(msg_id_dest: int, ofertas: list[str]) -> int:
    """Aponta ofertas para um post SEM tocar em post_estado.

    Escreve EXCLUSIVAMENTE em oferta_index. Nenhuma coluna de
    post_estado — texto, score, lider, edit_count, janela_fim,
    midia_chat, ts — é lida ou escrita aqui. É essa restrição, e não
    disciplina do chamador, que garante que aprender uma âncora nova
    jamais altere o vencedor textual, o orçamento de edição, a vida do
    ciclo ou a mídia publicada.

    NÃO REAPONTA âncora de outro post. `identity` é PRIMARY KEY de
    oferta_index: um INSERT OR REPLACE cego roubaria a âncora do post
    a que ela já pertence. Absorção é APRENDIZADO, não evolução — só
    entram âncoras órfãs (ou já deste post). Reapontar é privilégio de
    db_registrar_post, no caminho de evolução/renascimento.

    Idempotente. Devolve quantas âncoras foram efetivamente
    aprendidas; reexecutar com o mesmo conjunto devolve 0.
    """
    if not ofertas:
        return 0
    try:
        agora = time.time()
        with _db() as db:
            aprendidas = 0
            for oferta in ofertas:
                row = db.execute(
                    "SELECT msg_id_dest FROM oferta_index WHERE identity=?",
                    (oferta,)).fetchone()
                if row is not None:
                    continue          # já tem dono — nunca rouba
                db.execute(
                    "INSERT INTO oferta_index(identity,msg_id_dest,ts)"
                    " VALUES(?,?,?)",
                    (oferta, msg_id_dest, agora))
                aprendidas += 1
        return aprendidas
    except Exception as e:
        log_db.error(f"❌ db_absorver_ofertas: {e}")
        return 0

def db_registrar_post(msg_id_dest: int, ofertas: list[str], score: int,
                      texto: str, plat: str, lider: str = "",
                      janela_fim: float = 0.0, edit_count: int = 0,
                      *, chat_origem: str = "", msg_id_origem: int = 0,
                      midia_chat: Optional[str] = None,
                      score_versao: Optional[int] = None,
                      exibidas: Optional[list[str]] = None,
                      superar: Optional[int] = None):

    """Upsert do estado do post + mapeamento de cada oferta→post.
    Serve para publicação nova E evolução (idempotente).
    Se (chat_origem, msg_id_origem) vierem, grava o vínculo Origem na
    MESMA transação (invariantes I4/I5 — cobertura por construção).

    [Frente 8]
    `exibidas` — composição que o post passa a EXIBIR (âncoras da
      mensagem cujo texto está no ar). None = conteúdo não mudou:
      a composição gravada é preservada.
    `superar` — post que este SUPERA por renascimento: ele cede as
      âncoras e deixa de ser considerado exibidor (vira histórico).
    Âncora FORTE com dono vivo diferente NÃO é tomada: sobreposição
    parcial não é prova de nada; só a fusão transfere posse."""
    try:
        agora = time.time()
        with _db() as db:
            # midia_chat=None significa PRESERVAR o valor atual (o
            # COALESCE resolve contra a própria linha). Só quem decide
            # mídia passa valor explícito — "" (post sem mídia) ou o
            # chat de origem. Nunca gravamos NULL de propósito: NULL é
            # exclusivamente o legado pré-Fase 2.
            # fused_into/delete_status são SEMPRE preservados: nenhuma
            # gravação de conteúdo desfaz uma fusão.
            db.execute(
                "INSERT OR REPLACE INTO post_estado"
                "(msg_id_dest,score,texto,plat,lider,"
                "janela_fim,edit_count,ts,midia_chat,score_versao,"
                "fused_into,delete_status)"
                " VALUES(?,?,?,?,?,?,?,?,"
                " COALESCE(?,(SELECT midia_chat FROM post_estado"
                "             WHERE msg_id_dest=?)),"
                " COALESCE(?,(SELECT score_versao FROM post_estado"
                "             WHERE msg_id_dest=?)),"
                " (SELECT fused_into FROM post_estado WHERE msg_id_dest=?),"
                " (SELECT delete_status FROM post_estado"
                "   WHERE msg_id_dest=?))",
                (msg_id_dest, score, texto, plat, lider,
                 janela_fim, edit_count, agora,
                 midia_chat, msg_id_dest,
                 score_versao, msg_id_dest,
                 msg_id_dest, msg_id_dest))
            preservadas = []
            for oferta in ofertas:
                if not eh_chave_forte(oferta):
                    db.execute(
                        "INSERT OR REPLACE INTO oferta_index"
                        "(identity,msg_id_dest,ts) VALUES(?,?,?)",
                        (oferta, msg_id_dest, agora))
                    continue
                cur = db.execute(
                    "INSERT INTO oferta_index(identity,msg_id_dest,ts)"
                    " VALUES(?,?,?)"
                    " ON CONFLICT(identity) DO UPDATE SET"
                    "  msg_id_dest=excluded.msg_id_dest, ts=excluded.ts"
                    " WHERE oferta_index.msg_id_dest = excluded.msg_id_dest"
                    "    OR oferta_index.msg_id_dest = ?"
                    "    OR NOT EXISTS (SELECT 1 FROM post_estado pe"
                    "      WHERE pe.msg_id_dest = oferta_index.msg_id_dest"
                    f"       AND {_VIVO_PE})",
                    (oferta, msg_id_dest, agora,
                     superar if superar is not None else -1, agora))
                if cur.rowcount == 0:
                    preservadas.append(oferta)
            if superar is not None:
                db.execute("DELETE FROM post_exibida WHERE msg_id_dest=?",
                           (superar,))
            if exibidas is not None:
                db.execute("DELETE FROM post_exibida WHERE msg_id_dest=?",
                           (msg_id_dest,))
                db.executemany(
                    "INSERT OR IGNORE INTO post_exibida"
                    "(msg_id_dest,identity,ts) VALUES(?,?,?)",
                    [(msg_id_dest, e, agora) for e in exibidas])
            if preservadas:
                log_db.info(
                    f"🧷 [ANCORA_PRESERVADA] post:{msg_id_dest} não toma "
                    f"{sorted(preservadas)} — já têm dono vivo (só a fusão "
                    f"transfere posse)")
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
            db.execute("DELETE FROM post_exibida WHERE msg_id_dest=?",
                       (msg_id_dest,))
    except Exception as e:
        log_db.error(f"❌ db_remover_post: {e}")


# ─────────────────────────────────────────────────────────────────
# [Frente 8] COMPOSIÇÃO EXIBIDA, FUSÃO E TOMBSTONE
# ─────────────────────────────────────────────────────────────────
def db_exibida(msg_id_dest: int) -> set:
    """Composição que o post EXIBE agora (vazio: legado ou sem conteúdo)."""
    try:
        with _db() as db:
            return {r[0] for r in db.execute(
                "SELECT identity FROM post_exibida WHERE msg_id_dest=?",
                (msg_id_dest,)).fetchall()}
    except Exception as e:
        log_db.error(f"❌ db_exibida: {e}")
        return set()


def db_composicoes(ids, agora: float) -> dict:
    """{msg_id: (chaves exibidas, score)} de cada post da lista que está
    VIVO e não fundido. Releitura sob os locks da consolidação."""
    ids = [i for i in ids if i is not None and i >= 0]
    if not ids:
        return {}
    try:
        ph = ",".join("?" * len(ids))
        with _db() as db:
            rows = db.execute(
                f"SELECT pe.msg_id_dest, pe.score, px.identity"
                f"  FROM post_estado pe"
                f"  LEFT JOIN post_exibida px ON px.msg_id_dest = pe.msg_id_dest"
                f" WHERE pe.msg_id_dest IN ({ph}) AND {_VIVO_PE}",
                (*ids, agora)).fetchall()
        comp: dict = {}
        for mid, score, ident in rows:
            chaves, _ = comp.get(mid, (set(), score))
            if ident is not None:
                chaves.add(ident)
            comp[mid] = (chaves, score or 0)
        return comp
    except Exception as e:
        log_db.error(f"❌ db_composicoes: {e}")
        return {}


def db_vizinhos(chaves, excluir, agora: float, limite: int = 50) -> dict:
    """{msg_id: (chaves exibidas, score)} dos posts VIVOS, não fundidos e
    fora de `excluir`, que EXIBEM ao menos uma das `chaves`. Uma consulta
    indexada (idx_px_identity) + a composição completa deles."""
    chaves = [k for k in (chaves or ()) if k]
    if not chaves:
        return {}
    excl = [x for x in (excluir or ()) if x is not None]
    try:
        ph = ",".join("?" * len(chaves))
        filtro = (f" AND a.msg_id_dest NOT IN ({','.join('?' * len(excl))})"
                  if excl else "")
        with _db() as db:
            ids = [r[0] for r in db.execute(
                f"SELECT DISTINCT a.msg_id_dest FROM post_exibida a"
                f"  JOIN post_estado pe ON pe.msg_id_dest = a.msg_id_dest"
                f" WHERE a.identity IN ({ph}){filtro} AND {_VIVO_PE}"
                f" ORDER BY a.msg_id_dest LIMIT ?",
                (*chaves, *excl, agora, limite)).fetchall()]
        return db_composicoes(ids, agora) if ids else {}
    except Exception as e:
        log_db.error(f"❌ db_vizinhos: {e}")
        return {}


def db_fundir_posts(sobrevivente: int, perdedores: list[int],
                    agora: float, donos: Optional[dict] = None) -> list[int]:
    """FUSÃO ATÔMICA — a única operação que transfere posse de âncora
    forte entre posts vivos. Numa transação só, para cada perdedor:
      1. reconfirma que ele e o sobrevivente estão vivos e não fundidos;
      2. o sobrevivente HERDA o maior fim de vida (a vida da identidade
         transferida não é encurtada pela fusão);
      3. transfere TODAS as âncoras dele (memória) ao sobrevivente e,
         para as fortes listadas em `donos`, ao post que as EXIBE;
      4. grava fused_into=sobrevivente, encerra a vida e marca a
         remoção física como 'pendente';
      5. reaponta quem já tinha sido fundido nele (cadeia de 1 salto);
      6. apaga a composição exibida dele (não exibe mais nada);
      7. redireciona as origens dele ao sobrevivente.
    Devolve os perdedores efetivamente fundidos. Qualquer erro desfaz
    a transação inteira: nunca há fusão pela metade."""
    if not perdedores:
        return []
    feitos: list[int] = []
    donos = donos or {}
    try:
        with _db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                ok = db.execute(
                    f"SELECT 1 FROM post_estado WHERE msg_id_dest=?"
                    f" AND {_VIVO_SQL}", (sobrevivente, agora)).fetchone()
                for p in ([] if not ok else perdedores):
                    if p == sobrevivente:
                        continue
                    vivo = db.execute(
                        f"SELECT janela_fim FROM post_estado WHERE msg_id_dest=?"
                        f" AND {_VIVO_SQL}", (p, agora)).fetchone()
                    if not vivo:
                        continue
                    db.execute(
                        "UPDATE post_estado SET janela_fim=MAX(janela_fim, ?)"
                        " WHERE msg_id_dest=?", (vivo[0], sobrevivente))
                    chaves_p = [r[0] for r in db.execute(
                        "SELECT identity FROM oferta_index WHERE msg_id_dest=?",
                        (p,)).fetchall()]
                    db.execute(
                        "UPDATE oferta_index SET msg_id_dest=?, ts=?"
                        " WHERE msg_id_dest=?", (sobrevivente, agora, p))
                    for k in chaves_p:
                        dono = donos.get(k)
                        if dono is not None and dono != sobrevivente:
                            db.execute(
                                "UPDATE oferta_index SET msg_id_dest=?"
                                " WHERE identity=?", (dono, k))
                    db.execute(
                        "UPDATE post_estado SET fused_into=?,"
                        " janela_fim=MIN(janela_fim, ?),"
                        " delete_status='pendente' WHERE msg_id_dest=?",
                        (sobrevivente, agora, p))
                    db.execute(
                        "UPDATE post_estado SET fused_into=?"
                        " WHERE fused_into=?", (sobrevivente, p))
                    db.execute("DELETE FROM post_exibida WHERE msg_id_dest=?",
                               (p,))
                    db.execute(
                        "UPDATE origem_post SET dest=?, ts=? WHERE dest=?",
                        (sobrevivente, agora, p))
                    feitos.append(p)
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return feitos
    except Exception as e:
        log_db.error(f"❌ db_fundir_posts: {e}")
        return []


def db_registrar_conflito(alvo: int, chat: str, msg_id: int, motivo: str,
                          duplicadas, exclusivas, pendentes) -> None:
    """Registro persistente de um conflito estrutural IRRESOLVÍVEL (a
    escrita foi bloqueada). Auditoria: o que duplicaria, o que é
    exclusivo de quem, o que ficou pendente."""
    try:
        with _db() as db:
            db.execute(
                "INSERT INTO conflito_estrutural(ts,alvo,chat,msg_id,motivo,"
                "duplicadas,exclusivas,pendentes) VALUES(?,?,?,?,?,?,?,?)",
                (time.time(), alvo, chat or "", msg_id or 0, motivo,
                 ",".join(sorted(duplicadas)), ",".join(sorted(exclusivas)),
                 ",".join(sorted(pendentes))))
    except Exception as e:
        log_db.error(f"❌ db_registrar_conflito: {e}")


def db_set_delete_status(msg_id_dest: int, status: str) -> None:
    """Estado da remoção física do post fundido: pendente|ok|falhou."""
    try:
        with _db() as db:
            db.execute(
                "UPDATE post_estado SET delete_status=? WHERE msg_id_dest=?",
                (status, msg_id_dest))
    except Exception as e:
        log_db.error(f"❌ db_set_delete_status: {e}")


def db_remocoes_pendentes(limite: int = 100) -> list[int]:
    """Posts fundidos cuja remoção física não se concluiu (boot)."""
    try:
        with _db() as db:
            rows = db.execute(
                "SELECT msg_id_dest FROM post_estado"
                " WHERE fused_into IS NOT NULL"
                "   AND delete_status IN ('pendente','falhou')"
                " ORDER BY msg_id_dest LIMIT ?", (limite,)).fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        log_db.error(f"❌ db_remocoes_pendentes: {e}")
        return []

