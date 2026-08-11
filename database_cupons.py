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


