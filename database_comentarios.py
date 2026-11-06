"""
Camada 7 — Banco: estado do REPASSE DE COMENTÁRIOS (pipeline.comentarios).

Responsabilidade ÚNICA: a tabela `comentario_repasse`.

  comentario_repasse  o comentário (chat, msg_id) da fonte que foi ACEITO e
                      reservou a resposta ao nosso post `dest`. `estado`:
                        reservado  a vaga do post é dele; a resposta vai sair
                                   (ou saiu e o processo caiu antes de
                                   confirmar)
                        enviado    a resposta saiu: `resposta` é o id da
                                   nossa mensagem no canal (a prova)
                        falhou     o envio falhou; a vaga continua dele

IDEMPOTÊNCIA — as duas garantias moram no esquema, não em memória:
  PRIMARY KEY(chat, msg_id)  o mesmo comentário nunca reserva duas vezes
                             (evento repetido, edição, reentrega, reinício);
  UNIQUE(dest)               um post tem UMA resposta: o primeiro comentário
                             ELEGÍVEL que reserva ganha. Comentário recusado
                             pela política nunca chega aqui — não ocupa vaga.
No máximo uma vez: `reservado` e `falhou` seguram a vaga (um reinício no
meio do envio não reenvia — a resposta duplicada é pior que a perdida).
"""
from __future__ import annotations

from database_conexao import _db
from logger import log_db

RESERVADO, ENVIADO, FALHOU = "reservado", "enviado", "falhou"
# Respostas de db_comentario_reservar
OK_RESERVA, REPETIDO, VAGA_OCUPADA, ERRO = "reservado", "repetido", "vaga_ocupada", "erro"


def db_comentario_reservar(chat: str, msg_id: int, dest: int, texto: str,
                           agora: float) -> str:
    """Reserva a resposta ao post `dest` para o comentário (chat, msg_id),
    numa transação. OK_RESERVA | REPETIDO | VAGA_OCUPADA | ERRO (banco fora:
    o chamador NÃO envia)."""
    try:
        with _db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                if db.execute("SELECT 1 FROM comentario_repasse WHERE chat=? AND msg_id=?",
                              (str(chat), int(msg_id))).fetchone():
                    db.execute("COMMIT")
                    return REPETIDO
                if db.execute("SELECT 1 FROM comentario_repasse WHERE dest=?",
                              (int(dest),)).fetchone():
                    db.execute("COMMIT")
                    return VAGA_OCUPADA
                db.execute(
                    "INSERT INTO comentario_repasse(chat,msg_id,dest,estado,resposta,texto,"
                    "ts,ts_estado) VALUES(?,?,?,?,NULL,?,?,?)",
                    (str(chat), int(msg_id), int(dest), RESERVADO, texto, agora, agora))
                db.execute("COMMIT")
                return OK_RESERVA
            except Exception:
                db.execute("ROLLBACK")
                raise
    except Exception as e:
        log_db.error(f"❌ db_comentario_reservar: {type(e).__name__}: {e}")
        return ERRO


def db_comentario_desfecho(chat: str, msg_id: int, estado: str, resposta,
                           agora: float) -> None:
    """Fato DEPOIS do I/O: ENVIADO com o id da resposta (prova) ou FALHOU."""
    try:
        with _db() as db:
            db.execute("UPDATE comentario_repasse SET estado=?, resposta=?, ts_estado=?"
                       " WHERE chat=? AND msg_id=?",
                       (estado, resposta, agora, str(chat), int(msg_id)))
    except Exception as e:
        log_db.error(f"❌ db_comentario_desfecho: {type(e).__name__}: {e}")


def db_comentario_de_post(dest: int):
    """(chat, msg_id, estado, resposta) da resposta do post, ou None."""
    try:
        with _db() as db:
            return db.execute("SELECT chat,msg_id,estado,resposta FROM comentario_repasse"
                              " WHERE dest=?", (int(dest),)).fetchone()
    except Exception as e:
        log_db.error(f"❌ db_comentario_de_post: {type(e).__name__}: {e}")
        return None
