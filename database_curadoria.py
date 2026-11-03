"""
Camada 7 — Banco: estado da CURADORIA (pipeline.curadoria).

Responsabilidade ÚNICA: as tabelas `curadoria_aval`, `curadoria_retida` e
`curadoria_chave`.

  curadoria_aval    um canal avalista (Fada Promoções, Nerd Ofertas) postou
                    o produto `chave` no instante `ts` (data em que o
                    produto entrou na mensagem dele no Telegram). Uma linha
                    por (chave, fonte, msg); aval repetido não muda a data.
  curadoria_retida  a mensagem (chat, msg_id) de uma fonte retentora, de
                    categoria vigiada, que a curadoria avaliou. `tipo`:
                      nova    mensagem que ainda não foi ao canal
                      edicao  edição de mensagem JÁ publicada que traz
                              produto novo (alteração pendente: o post
                              segue com o produto aprovado)
                    `ts` é a data original da mensagem; `ts_ref` a
                    referência da janela da VERSÃO ATUAL (a entrada mais
                    recente entre as chaves atuais; sem chave, `ts`).
                    `estado`:
                      retida       esperando aval
                      liberando    o aval chegou; a versão está sendo
                                   reinjetada no ponto de entrada
                      reavaliando  a versão não pôde ser gravada (banco
                                   fora) e volta ao ponto de entrada para
                                   ser avaliada de novo — prova de
                                   admissão, NUNCA aprovação
                      liberada     passou pelo portão (aval, exceção ou
                                   reinjeção)
                      superada     a alteração pendente foi substituída por
                                   uma versão que passou sem ela
                      expirada     a janela fechou sem aval
                      ausente      sumiu da fonte antes de ser reinjetada
                      falhou       a reinjeção não aconteceu
  curadoria_chave   as identidades de produto da mensagem: `atual`=1 nas
                    da VERSÃO ATUAL, 0 nas que uma edição tirou;
                    `ts_entrada` = quando a chave entrou na mensagem (data
                    da mensagem, ou edit_date da edição que a trouxe). Uma
                    chave que sai e volta mantém a PRIMEIRA entrada: editar
                    não recomeça janela de produto que já estava lá.

A REGRA, em uma frase: a versão atual passa se ALGUMA chave atual tem aval
(mesma chave = mesma loja + mesmo id) com data a até `janela` da entrada
dessa chave. Lista vazia NUNCA é aprovação.

SEM CORRIDA: as perguntas que se cruzam — "há aval para esta versão?"
(portão, que grava o conjunto atual na MESMA transação) e "há versão
segurada para este aval?" (aval) — rodam cada uma numa transação só
(BEGIN IMMEDIATE, sob o mutex da conexão única do processo). Ou o portão
grava antes e o aval a encontra, ou o aval grava antes e o portão o
encontra.

FALHA: toda função devolve um valor que o chamador reconhece como falha
(ERRO, None) — nunca uma aprovação.

Não participa de identidade, família, decisão nem publicação. Escritor
único: pipeline.curadoria, no loop do worker.
"""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from database_conexao import _db
from logger import log_db

RETIDA, LIBERANDO, REAVALIANDO = "retida", "liberando", "reavaliando"
LIBERADA, SUPERADA = "liberada", "superada"
EXPIRADA, AUSENTE, FALHOU = "expirada", "ausente", "falhou"
NOVA, EDICAO = "nova", "edicao"
# Estados que uma versão que SEGUIU (exceção, identidade preservada) fecha.
_ABERTOS = (RETIDA, LIBERANDO, REAVALIANDO, EXPIRADA, FALHOU)

# Respostas de db_curadoria_avaliar
LIBERACAO = "LIBERACAO"   # é a reinjeção da versão que o aval soltou: passa
AVAL = "AVAL"             # há aval para a versão atual: passa
SEGURADA = "SEGURADA"     # sem aval (ou sem identidade): não passa
OBSOLETA = "OBSOLETA"     # reinjeção que não é mais a versão aprovada: não passa
ERRO = "ERRO"             # banco indisponível (o portão NÃO libera)


def _chaves(valores: Iterable[str]) -> List[str]:
    return sorted({str(v) for v in valores if v})


def _gravar_versao(db, chat: str, msg_id: int, chaves: Sequence[str],
                   ts_versao: float, ja_estavam: Iterable[str] = (),
                   ts_original: Optional[float] = None) -> Dict[str, float]:
    """Troca o conjunto ATUAL da mensagem pelo da versão (mesma transação
    do chamador). Chave nova entra com `ts_versao`; chave que já esteve na
    mensagem mantém a primeira entrada; chave que saiu fica atual=0. Chave
    sem histórico aqui mas que `ja_estavam` na mensagem (o post publicado já
    a tem) entra com `ts_original`: a data em que ela entrou, não a da
    edição. Devolve {chave: ts_entrada} do conjunto atual."""
    ja_estavam = set(ja_estavam or ())
    hist = dict(db.execute(
        "SELECT chave, ts_entrada FROM curadoria_chave WHERE chat=? AND msg_id=?",
        (chat, msg_id)).fetchall())
    db.execute("UPDATE curadoria_chave SET atual=0 WHERE chat=? AND msg_id=?",
               (chat, msg_id))
    atual: Dict[str, float] = {}
    for c in chaves:
        if c in hist:
            db.execute("UPDATE curadoria_chave SET atual=1"
                       " WHERE chat=? AND msg_id=? AND chave=?", (chat, msg_id, c))
            atual[c] = float(hist[c])
        else:
            ts = ts_original if (c in ja_estavam and ts_original is not None) else ts_versao
            db.execute("INSERT INTO curadoria_chave(chat,msg_id,chave,ts_entrada,atual)"
                       " VALUES(?,?,?,?,1)", (chat, msg_id, c, float(ts)))
            atual[c] = float(ts)
    return atual


def _atuais(db, chat: str, msg_id: int) -> set:
    return {c for (c,) in db.execute(
        "SELECT chave FROM curadoria_chave WHERE chat=? AND msg_id=? AND atual=1",
        (chat, msg_id)).fetchall()}


def _tem_aval(db, atual: Dict[str, float], janela: float) -> bool:
    for c, t in atual.items():
        if db.execute("SELECT 1 FROM curadoria_aval WHERE chave=? AND ts BETWEEN ? AND ?"
                      " LIMIT 1", (c, t - janela, t + janela)).fetchone() is not None:
            return True
    return False


def db_curadoria_avaliar(chat: str, msg_id: int, tipo: str, chaves: Sequence[str],
                         categoria: str, ts_msg: float, ts_versao: float,
                         janela: float, folga: float, agora: float,
                         reinjetada: bool = False,
                         ja_publicadas: Iterable[str] = ()) -> Tuple[str, bool]:
    """O portão, numa transação: grava o conjunto ATUAL da versão e confere
    o aval. Devolve (resposta, nova); `nova` diz se a mensagem passou a
    esperar aval agora (o log de SEGURADA sai uma vez, não a cada edição).

    `reinjetada`: a versão chega pela via da curadoria. Ela só passa direto
    (LIBERACAO) se a linha está `liberando` E o conjunto dela é exatamente
    o que o aval soltou; com `reavaliando` (banco que voltou) ou conjunto
    diferente, é avaliada como versão nova; qualquer outro estado é
    OBSOLETA (outra versão já passou ou a liberação acabou).

    `ja_publicadas`: produtos que o post de destino já tem (edição de
    mensagem publicada) — sem histórico aqui, entram pela data original da
    mensagem, não pela da edição.

    Versão sem chave (identidade ausente ou incerta) nunca tem aval: fica
    segurada até a janela da mensagem fechar."""
    chaves = _chaves(chaves)
    try:
        with _db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT estado, ts FROM curadoria_retida WHERE chat=? AND msg_id=?",
                    (chat, msg_id)).fetchone()
                estado = row[0] if row is not None else None
                if reinjetada and estado == LIBERANDO and _atuais(db, chat, msg_id) == set(chaves):
                    db.execute("UPDATE curadoria_retida SET estado=?, ts_estado=?"
                               " WHERE chat=? AND msg_id=?", (LIBERADA, agora, chat, msg_id))
                    db.execute("COMMIT")
                    return LIBERACAO, False
                if reinjetada and estado not in (LIBERANDO, REAVALIANDO):
                    db.execute("COMMIT")
                    return OBSOLETA, False
                ts0 = float(row[1]) if row is not None else float(ts_msg)
                if row is None:
                    db.execute(
                        "INSERT INTO curadoria_retida(chat,msg_id,tipo,categoria,ts,ts_ref,"
                        "estado,ts_estado) VALUES(?,?,?,?,?,?,?,?)",
                        (chat, msg_id, tipo, categoria, ts0, ts0, RETIDA, agora))
                atual = _gravar_versao(db, chat, msg_id, chaves, ts_versao,
                                       ja_publicadas, ts0)
                ts_ref = max(atual.values()) if atual else ts0
                if _tem_aval(db, atual, janela):
                    novo, resposta, nova = LIBERADA, AVAL, False
                else:
                    aberta = ts_ref + janela + folga >= agora
                    novo = RETIDA if aberta else EXPIRADA
                    resposta = SEGURADA
                    nova = novo == RETIDA and estado != RETIDA
                db.execute(
                    "UPDATE curadoria_retida SET tipo=?, categoria=?, ts_ref=?, estado=?,"
                    " ts_estado=CASE WHEN estado=? THEN ts_estado ELSE ? END"
                    " WHERE chat=? AND msg_id=?",
                    (tipo, categoria, ts_ref, novo, novo, agora, chat, msg_id))
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
                                agora: float) -> Optional[List[Tuple[str, int, str, str]]]:
    """Grava o aval e, na MESMA transação, separa as mensagens seguradas que
    ele solta: estado `retida`, com uma chave ATUAL igual cuja entrada está
    a até `janela` da data do aval e não é mais velha que `piso` (aval
    atrasado nunca solta produto antigo). As separadas passam a `liberando`
    e voltam como [(chat, msg_id, categoria, tipo)]. None = banco
    indisponível.

    Aval repetido (a mesma mensagem do avalista vista de novo: edição,
    varredura) não muda a data do primeiro: a soltura usa a data GRAVADA."""
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
                marcas = ",".join("?" * len(chaves))
                gravadas = db.execute(
                    f"SELECT chave, ts FROM curadoria_aval WHERE fonte=? AND msg_id=?"
                    f" AND chave IN ({marcas})", (fonte, int(msg_id), *chaves)).fetchall()
                soltas: Dict[Tuple[str, int], Tuple[str, str]] = {}
                for chave, ts in gravadas:
                    for chat, mid, cat, tipo in db.execute(
                            "SELECT r.chat, r.msg_id, r.categoria, r.tipo"
                            "  FROM curadoria_chave k JOIN curadoria_retida r"
                            "    ON r.chat=k.chat AND r.msg_id=k.msg_id"
                            " WHERE k.chave=? AND k.atual=1 AND r.estado=?"
                            "   AND k.ts_entrada BETWEEN ? AND ? AND k.ts_entrada>=?",
                            (chave, RETIDA, ts - janela, ts + janela, piso)).fetchall():
                        soltas[(chat, int(mid))] = (cat, tipo)
                for (chat, mid) in soltas:
                    db.execute("UPDATE curadoria_retida SET estado=?, ts_estado=?"
                               " WHERE chat=? AND msg_id=? AND estado=?",
                               (LIBERANDO, agora, chat, mid, RETIDA))
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return [(c, m, cat, tipo) for (c, m), (cat, tipo) in sorted(soltas.items())]
    except Exception as e:
        log_db.error(f"❌ db_curadoria_registrar_aval: {type(e).__name__}: {e}")
        return None


def db_curadoria_em_liberacao(chat: str, msg_id: int, desde: float) -> bool:
    """A mensagem está sendo reinjetada (estado `liberando` ou
    `reavaliando` marcado a partir de `desde`)? É a PROVA que a admissão
    confere antes de deixar a reinjeção passar pela trava de frescor e pela
    idempotência. Falha do banco = sem prova."""
    try:
        with _db() as db:
            row = db.execute(
                "SELECT 1 FROM curadoria_retida WHERE chat=? AND msg_id=? AND estado IN (?,?)"
                " AND ts_estado>=?",
                (chat, int(msg_id), LIBERANDO, REAVALIANDO, desde)).fetchone()
        return row is not None
    except Exception:
        return False


def db_curadoria_desfecho(chat: str, msg_id: int, estado: str, agora: float) -> bool:
    """Desfecho de uma reinjeção que não chegou ao portão (`ausente`,
    `falhou`). Só sai de `liberando`/`reavaliando`. True = gravado."""
    try:
        with _db() as db:
            cur = db.execute(
                "UPDATE curadoria_retida SET estado=?, ts_estado=?"
                " WHERE chat=? AND msg_id=? AND estado IN (?,?)",
                (estado, agora, chat, int(msg_id), LIBERANDO, REAVALIANDO))
        return cur.rowcount > 0
    except Exception:
        return False


def db_curadoria_seguiu(chat: str, msg_id: int, chaves: Sequence[str],
                        ts_versao: float, estado: str, agora: float) -> Optional[bool]:
    """A versão atual da mensagem SEGUIU o pipeline sem precisar de aval
    (exceção; edição que preserva a identidade publicada; versão que deixou
    de ser hardware vigiado). Se há linha: o conjunto atual passa a ser o
    desta versão e, se a linha estava aberta, ela fecha com `estado`
    (liberada/superada) — nenhum aval posterior reinjeta uma versão que
    não é mais a da fonte. True = fechou; False = nada aberto; None = banco
    indisponível."""
    try:
        with _db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT estado FROM curadoria_retida WHERE chat=? AND msg_id=?",
                    (chat, int(msg_id))).fetchone()
                fechou = False
                if row is not None:
                    _gravar_versao(db, chat, int(msg_id), _chaves(chaves), ts_versao)
                    if row[0] in _ABERTOS:
                        db.execute("UPDATE curadoria_retida SET estado=?, ts_estado=?"
                                   " WHERE chat=? AND msg_id=?",
                                   (estado, agora, chat, int(msg_id)))
                        fechou = True
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return fechou
    except Exception as e:
        log_db.error(f"❌ db_curadoria_seguiu: {type(e).__name__}: {e}")
        return None


def db_curadoria_reavaliar(chat: str, msg_id: int, tipo: str, categoria: str,
                           ts_msg: float, agora: float) -> Optional[bool]:
    """O banco voltou: a versão que ficou segurada só na memória volta ao
    ponto de entrada com a prova `reavaliando` (admissão), para o portão a
    avaliar de novo — `reavaliando` não aprova nada. True = prova gravada;
    False = nada a fazer (já passou, está em liberação ou sumiu da fonte);
    None = banco ainda indisponível."""
    try:
        with _db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT estado FROM curadoria_retida WHERE chat=? AND msg_id=?",
                    (chat, int(msg_id))).fetchone()
                if row is None:
                    db.execute(
                        "INSERT INTO curadoria_retida(chat,msg_id,tipo,categoria,ts,ts_ref,"
                        "estado,ts_estado) VALUES(?,?,?,?,?,?,?,?)",
                        (chat, int(msg_id), tipo, categoria, float(ts_msg), float(ts_msg),
                         REAVALIANDO, agora))
                    ok = True
                elif row[0] in (LIBERADA, SUPERADA, LIBERANDO, AUSENTE):
                    ok = False
                else:
                    db.execute("UPDATE curadoria_retida SET estado=?, ts_estado=?, tipo=?"
                               " WHERE chat=? AND msg_id=?",
                               (REAVALIANDO, agora, tipo, chat, int(msg_id)))
                    ok = True
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return ok
    except Exception as e:
        log_db.error(f"❌ db_curadoria_reavaliar: {type(e).__name__}: {e}")
        return None


def db_curadoria_expirar(limite: float, agora: float) -> List[Tuple[str, int, str, str]]:
    """As seguradas cuja versão atual tem referência anterior a `limite`
    (janela fechada sem aval) passam a `expirada`. Devolve
    [(chat, msg_id, categoria, tipo)] para o log."""
    try:
        with _db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                rows = db.execute(
                    "SELECT chat, msg_id, categoria, tipo FROM curadoria_retida"
                    " WHERE estado=? AND ts_ref<?", (RETIDA, limite)).fetchall()
                db.execute("UPDATE curadoria_retida SET estado=?, ts_estado=?"
                           " WHERE estado=? AND ts_ref<?", (EXPIRADA, agora, RETIDA, limite))
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return [(c, int(m), cat, tipo) for c, m, cat, tipo in rows]
    except Exception as e:
        log_db.error(f"❌ db_curadoria_expirar: {type(e).__name__}: {e}")
        return []


def db_curadoria_retomar(desde: float, agora: float,
                         presas_desde: Optional[float] = None
                         ) -> List[Tuple[str, int, str, str]]:
    """Reinjeções que não chegaram ao portão: as `liberando`/`reavaliando`
    com referência a partir de `desde` (e, com `presas_desde`, marcadas
    antes dele: paradas) têm a prova renovada (ts_estado = agora) e voltam
    como [(chat, msg_id, categoria, tipo)] para nova entrega. No boot,
    todas; no laço, só as paradas. As mais velhas ficam como estão."""
    try:
        cond = "estado IN (?,?) AND ts_ref>=?"
        args: list = [LIBERANDO, REAVALIANDO, desde]
        if presas_desde is not None:
            cond += " AND ts_estado<?"
            args.append(presas_desde)
        with _db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                rows = db.execute(
                    f"SELECT chat, msg_id, categoria, tipo FROM curadoria_retida WHERE {cond}",
                    args).fetchall()
                db.execute(f"UPDATE curadoria_retida SET ts_estado=? WHERE {cond}",
                           [agora, *args])
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return [(c, int(m), cat, tipo) for c, m, cat, tipo in rows]
    except Exception as e:
        log_db.error(f"❌ db_curadoria_retomar: {type(e).__name__}: {e}")
        return []
