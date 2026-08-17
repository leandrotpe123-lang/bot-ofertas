"""Camada 7 — Banco / Memória de códigos de cupom.

Responsabilidade ÚNICA: a tabela `cupom_idx` — o índice
(plat + código → identity) com janela deslizante, que permite casar
posts de cupom por código compartilhado.

NÃO conhece link, post nem oferta.

Usa _db de database_conexao — contrato interno da camada.

Extraído de database sem qualquer alteração de comportamento.
"""
from __future__ import annotations
import time
from typing import Optional

from logger import log_db

# Contrato INTERNO da camada — ver cabeçalho de database_conexao.
from database_conexao import _db

def db_cupom_idx_buscar(plat: str, codigos: list, janela_s: float) -> Optional[str]:
    """Retorna a identity de um post de cupom se QUALQUER código da lista
    já estiver indexado dentro da janela (plat + código). Casamento por
    código compartilhado — o link nunca entra. Sem código → None."""
    cods = [c.upper() for c in (codigos or []) if c]
    if not cods:
        return None
    limite = time.time() - janela_s
    ph = ",".join("?" * len(cods))
    try:
        with _db() as cx:
            r = cx.execute(
                f"SELECT identity FROM cupom_idx"
                f" WHERE plat=? AND codigo IN ({ph}) AND ts>=?"
                f" ORDER BY ts DESC LIMIT 1",
                (plat, *cods, limite)).fetchone()
            return r[0] if r else None
    except Exception as e:
        log_db.error(f"❌ db_cupom_idx_buscar: {e}")
        return None


def db_cupom_idx_registrar(plat: str, codigos: list, identity: str,
                           janela_s: float) -> int:
    """Registra TODOS os códigos do post sob a MESMA identity
    (plat + código → identity), com ts atual (janela deslizante).
    Preserva todos os códigos — não só um. Retorna QUANTOS códigos
    eram NOVOS dentro da janela (Frente 0 §2: a novidade é medida
    contra o ciclo) — insumo do ramo CUPOM_ENRIQUECIDO da evolução."""
    cods = [c.upper() for c in (codigos or []) if c]
    if not cods or not identity:
        return 0
    agora = time.time()
    limite = agora - janela_s
    try:
        with _db() as cx:
            ph = ",".join("?" * len(cods))
            ja = {r[0] for r in cx.execute(
                f"SELECT codigo FROM cupom_idx"
                f" WHERE plat=? AND codigo IN ({ph}) AND ts>=?",
                (plat, *cods, limite)).fetchall()}
            cx.executemany(
                "INSERT INTO cupom_idx(plat,codigo,identity,ts)"
                " VALUES(?,?,?,?)"
                " ON CONFLICT(plat,codigo) DO UPDATE SET"
                " identity=excluded.identity, ts=excluded.ts",
                [(plat, c, identity, agora) for c in cods])
            return sum(1 for c in cods if c not in ja)
    except Exception as e:
        log_db.error(f"❌ db_cupom_idx_registrar: {e}")
        return 0


def db_cupom_idx_registrar_inedito(plat: str, codigos: list, identity: str,
                                   janela_s: float) -> tuple:
    """Registra APENAS o que ainda não está vivo no índice.

    Irmã de db_cupom_idx_registrar, para o caminho de EDIÇÃO. A
    diferença é deliberada e não é otimização: aquela é a porta da
    PUBLICAÇÃO NOVA, onde reivindicar a identidade e estampar o ts é o
    comportamento correto; esta é a porta do APRENDIZADO, onde o índice
    só pode CRESCER.

    Invariantes:
      I1  código vivo NUNCA tem o ts renovado — a memória expira junto
          com o ciclo que a criou (memoria_cupom, C4.3). Renovar aqui
          faria uma sequência de edições manter um código eterno, e o
          histórico passaria a governar ciclos futuros.
      I2  código vivo NUNCA é repontuado para outra identity — isso
          sequestraria a corrente de outro post.
      I3  código EXPIRADO é reclamável: a janela morreu, a identidade
          está livre, e o registro é um registro novo.
      I4  idempotente: reexecutar com o mesmo conjunto devolve 0.
      I5  conflito (código vivo apontando para identity DIFERENTE) não
          é silencioso: sai na lista devolvida para o chamador logar.
          Ignora e segue — nunca sobrescreve.

    Devolve (n_ineditos, conflitos), onde conflitos é uma lista de
    (codigo, identity_vigente).
    """
    cods = sorted({c.upper() for c in (codigos or []) if c})
    if not cods or not identity:
        return 0, []
    agora = time.time()
    limite = agora - janela_s
    try:
        with _db() as cx:
            ph = ",".join("?" * len(cods))
            vivos = {r[0]: r[1] for r in cx.execute(
                f"SELECT codigo,identity FROM cupom_idx"
                f" WHERE plat=? AND codigo IN ({ph}) AND ts>=?",
                (plat, *cods, limite)).fetchall()}
            conflitos = [(c, vivos[c]) for c in cods
                         if c in vivos and vivos[c] != identity]
            novos = [c for c in cods if c not in vivos]
            if novos:
                cx.executemany(
                    "INSERT INTO cupom_idx(plat,codigo,identity,ts)"
                    " VALUES(?,?,?,?)"
                    " ON CONFLICT(plat,codigo) DO UPDATE SET"
                    " identity=excluded.identity, ts=excluded.ts",
                    [(plat, c, identity, agora) for c in novos])
            return len(novos), conflitos
    except Exception as e:
        log_db.error(f"❌ db_cupom_idx_registrar_inedito: {e}")
        return 0, []


