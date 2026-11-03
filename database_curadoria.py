"""
Camada 7 — Banco: estado da CURADORIA (pipeline.curadoria).

Responsabilidade ÚNICA: as tabelas `curadoria_aval` e `curadoria_retida`.

  curadoria_aval    um canal curador (Fada Promoções, Nerd Ofertas) postou
                    o produto `chave` no instante `ts` (data da mensagem
                    dele no Telegram). Uma linha por (chave, fonte, msg).
  curadoria_retida  a mensagem (chat, msg_id) de uma fonte comum, de
                    categoria vigiada, ficou SEGURADA esperando aval. `ts` é
                    a data da mensagem na fonte; `estado` é o desfecho:
                      retida     esperando aval
                      liberando  o aval chegou; a mensagem está sendo
                                 reinjetada no ponto de entrada
                      liberada   passou pelo portão (aval ou reinjeção)
                      expirada   a janela fechou sem aval
                      ausente    sumiu da fonte antes de ser reinjetada
                      falhou     a reinjeção não aconteceu

SEM CORRIDA: as duas perguntas que se cruzam — "há aval para esta
mensagem?" (portão) e "há mensagem segurada para este aval?" (aval) —
rodam cada uma numa transação só (BEGIN IMMEDIATE, sob o mutex da conexão
única do processo). Ou o portão grava a retida antes e o aval a encontra,
ou o aval grava antes e o portão o encontra. Não existe a mensagem
segurada que o aval não vê.

Não participa de identidade, família, decisão nem publicação. Escritor
único: pipeline.curadoria, no loop do worker.
"""
from __future__ import annotations

import json
from typing import Iterable, List, Optional, Sequence, Tuple

from database_conexao import _db
from logger import log_db

RETIDA, LIBERANDO, LIBERADA = "retida", "liberando", "liberada"
EXPIRADA, AUSENTE, FALHOU = "expirada", "ausente", "falhou"

# Respostas de db_curadoria_avaliar
LIBERACAO = "LIBERACAO"   # é a reinjeção de uma liberada pelo aval: passa
JA_LIBERADA = "JA_LIBERADA"
AVAL = "AVAL"             # há aval dentro da janela: passa
SEGURADA = "SEGURADA"     # sem aval: fica segurada
ERRO = "ERRO"             # banco indisponível (o portão decide o que fazer)


def _chaves(valores: Iterable[str]) -> List[str]:
    return sorted({str(v) for v in valores if v})


def _marcas(n: int) -> str:
    return ",".join("?" * n)


def db_curadoria_avaliar(chat: str, msg_id: int, chaves: Sequence[str],
                         categoria: str, ts_msg: float, janela: float,
                         agora: float) -> Tuple[str, bool]:
    """O portão, numa transação: (resposta, nova).

    `nova` diz se a linha de retida nasceu agora (o log de SEGURADA sai
    uma vez por mensagem, não a cada edição dela). A janela é sempre
    contada da data da mensagem na fonte — a primeira vez que ela foi
    vista — e nunca recomeça numa edição."""
    chaves = _chaves(chaves)
    if not chaves:
        return AVAL, False
    try:
        with _db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT estado, ts FROM curadoria_retida WHERE chat=? AND msg_id=?",
                    (chat, msg_id)).fetchone()
                if row is not None and row[0] == LIBERANDO:
                    db.execute("UPDATE curadoria_retida SET estado=?, ts_estado=?"
                               " WHERE chat=? AND msg_id=?",
                               (LIBERADA, agora, chat, msg_id))
                    resposta, nova = LIBERACAO, False
                elif row is not None and row[0] == LIBERADA:
                    resposta, nova = JA_LIBERADA, False
                else:
                    ref = float(row[1]) if row is not None else float(ts_msg)
                    achou = db.execute(
                        f"SELECT 1 FROM curadoria_aval WHERE chave IN ({_marcas(len(chaves))})"
                        " AND ts BETWEEN ? AND ? LIMIT 1",
                        (*chaves, ref - janela, ref + janela)).fetchone()
                    if achou is not None:
                        if row is not None:
                            db.execute("UPDATE curadoria_retida SET estado=?, ts_estado=?"
                                       " WHERE chat=? AND msg_id=?",
                                       (LIBERADA, agora, chat, msg_id))
                        resposta, nova = AVAL, False
                    else:
                        if row is None:
                            db.execute(
                                "INSERT INTO curadoria_retida(chat,msg_id,chaves,categoria,"
                                "ts,estado,ts_estado) VALUES(?,?,?,?,?,?,?)",
                                (chat, msg_id, json.dumps(chaves), categoria,
                                 float(ts_msg), RETIDA, agora))
                        else:
                            # A edição trocou o link (produto novo): o aval de
                            # qualquer um dos produtos dela solta a mensagem.
                            antes = db.execute(
                                "SELECT chaves FROM curadoria_retida WHERE chat=? AND msg_id=?",
                                (chat, msg_id)).fetchone()[0]
                            try:
                                uniao = sorted(set(json.loads(antes)) | set(chaves))
                            except Exception:
                                uniao = chaves
                            db.execute("UPDATE curadoria_retida SET chaves=?"
                                       " WHERE chat=? AND msg_id=?",
                                       (json.dumps(uniao), chat, msg_id))
                        resposta, nova = SEGURADA, row is None
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return resposta, nova
    except Exception as e:
        log_db.error(f"❌ db_curadoria_avaliar: {type(e).__name__}: {e}")
        return ERRO, False


def db_curadoria_registrar_aval(chaves: Sequence[str], fonte: str, msg_id: int,
                                ts_aval: float, janela: float, piso: float,
                                agora: float) -> Optional[List[Tuple[str, int, str]]]:
    """Grava o aval e, na MESMA transação, separa as mensagens seguradas do
    mesmo produto que ele libera: estado `retida`, data a até `janela` do
    aval e não mais velha que `piso` (aval atrasado nunca solta mensagem
    antiga). As separadas passam a `liberando` e voltam como
    [(chat, msg_id, categoria)]. None = banco indisponível.

    Aval repetido (a mesma mensagem do curador vista de novo: edição,
    varredura) não muda a data do primeiro."""
    chaves = _chaves(chaves)
    if not chaves:
        return []
    try:
        with _db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                db.executemany(
                    "INSERT OR IGNORE INTO curadoria_aval(chave,fonte,msg_id,ts)"
                    " VALUES(?,?,?,?)",
                    [(c, fonte, int(msg_id), float(ts_aval)) for c in chaves])
                rows = db.execute(
                    "SELECT chat, msg_id, chaves, categoria FROM curadoria_retida"
                    " WHERE estado=? AND ts BETWEEN ? AND ? AND ts>=?",
                    (RETIDA, ts_aval - janela, ts_aval + janela, piso)).fetchall()
                alvo = set(chaves)
                soltas = []
                for chat, mid, cj, cat in rows:
                    try:
                        delas = set(json.loads(cj))
                    except Exception:
                        continue
                    if delas & alvo:
                        soltas.append((chat, int(mid), cat))
                for chat, mid, _ in soltas:
                    db.execute("UPDATE curadoria_retida SET estado=?, ts_estado=?"
                               " WHERE chat=? AND msg_id=? AND estado=?",
                               (LIBERANDO, agora, chat, mid, RETIDA))
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return soltas
    except Exception as e:
        log_db.error(f"❌ db_curadoria_registrar_aval: {type(e).__name__}: {e}")
        return None


def db_curadoria_em_liberacao(chat: str, msg_id: int, desde: float) -> bool:
    """A mensagem está sendo reinjetada pelo aval (estado `liberando`
    marcado a partir de `desde`)? É a PROVA que a admissão confere antes de
    deixar a reinjeção passar pela trava de frescor e pela idempotência."""
    try:
        with _db() as db:
            row = db.execute(
                "SELECT 1 FROM curadoria_retida WHERE chat=? AND msg_id=? AND estado=?"
                " AND ts_estado>=?", (chat, int(msg_id), LIBERANDO, desde)).fetchone()
        return row is not None
    except Exception:
        return False


def db_curadoria_desfecho(chat: str, msg_id: int, estado: str, agora: float) -> bool:
    """Desfecho de uma reinjeção que não chegou ao portão (`ausente`,
    `falhou`). Só sai de `liberando`. True = gravado."""
    try:
        with _db() as db:
            cur = db.execute(
                "UPDATE curadoria_retida SET estado=?, ts_estado=?"
                " WHERE chat=? AND msg_id=? AND estado=?",
                (estado, agora, chat, int(msg_id), LIBERANDO))
        return cur.rowcount > 0
    except Exception:
        return False


def db_curadoria_passou(chat: str, msg_id: int, agora: float) -> bool:
    """A mensagem que estava segurada (ou expirada, ou sendo reinjetada)
    seguiu o pipeline por outro motivo — a edição dela trouxe uma exceção
    ("Ainda ativo!", lista, retorno): passa a `liberada`, para nenhum aval
    posterior a reinjetar de novo e nenhum log dizer que expirou. True =
    havia linha para fechar."""
    try:
        with _db() as db:
            cur = db.execute(
                "UPDATE curadoria_retida SET estado=?, ts_estado=?"
                " WHERE chat=? AND msg_id=? AND estado IN (?,?,?)",
                (LIBERADA, agora, chat, int(msg_id), RETIDA, EXPIRADA, LIBERANDO))
        return cur.rowcount > 0
    except Exception:
        return False


def db_curadoria_expirar(limite: float, agora: float) -> List[Tuple[str, int, str]]:
    """As seguradas com data anterior a `limite` (janela fechada sem aval)
    passam a `expirada`. Devolve [(chat, msg_id, categoria)] para o log."""
    try:
        with _db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                rows = db.execute(
                    "SELECT chat, msg_id, categoria FROM curadoria_retida"
                    " WHERE estado=? AND ts<?", (RETIDA, limite)).fetchall()
                db.execute("UPDATE curadoria_retida SET estado=?, ts_estado=?"
                           " WHERE estado=? AND ts<?", (EXPIRADA, agora, RETIDA, limite))
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return [(c, int(m), cat) for c, m, cat in rows]
    except Exception as e:
        log_db.error(f"❌ db_curadoria_expirar: {type(e).__name__}: {e}")
        return []


def db_curadoria_retomar(desde: float, agora: float) -> List[Tuple[str, int, str]]:
    """Liberações interrompidas por um reinício: as `liberando` cuja
    mensagem tem data a partir de `desde` têm a prova renovada (ts_estado =
    agora) e voltam como [(chat, msg_id, categoria)] para nova entrega. As
    mais velhas ficam como estão: não vão mais ao canal."""
    try:
        with _db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                rows = db.execute(
                    "SELECT chat, msg_id, categoria FROM curadoria_retida"
                    " WHERE estado=? AND ts>=?", (LIBERANDO, desde)).fetchall()
                db.execute("UPDATE curadoria_retida SET ts_estado=?"
                           " WHERE estado=? AND ts>=?", (agora, LIBERANDO, desde))
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return [(c, int(m), cat) for c, m, cat in rows]
    except Exception as e:
        log_db.error(f"❌ db_curadoria_retomar: {type(e).__name__}: {e}")
        return []
