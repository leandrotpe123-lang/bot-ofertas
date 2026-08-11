"""Camada 7 — Banco / Manutenção e diagnóstico.

Responsabilidade ÚNICA: retenção (varredura por TTL sobre TODAS as
tabelas) e contagem para diagnóstico.

É o único módulo do banco que atravessa domínios de propósito: a
limpeza é transversal por natureza. Por isso NÃO mora na conexão —
a camada de conexão não deve conhecer as tabelas do sistema.

Usa _db de database_conexao — contrato interno da camada.

Extraído de database sem qualquer alteração de comportamento.
"""
from __future__ import annotations
import time

from config import TTL_LINK_INATIVO
from logger import log_db

# Contrato INTERNO da camada — ver cabeçalho de database_conexao.
from database_conexao import _db

# ── limpeza ───────────────────────────────────────────────────────
def db_limpar():
    from globals import _raw_cache, _final_cache
    try:
        agora = time.time()
        with _db() as db:
            db.execute("DELETE FROM links_cache  WHERE ts<?", (agora - TTL_LINK_INATIVO,))
            db.execute("DELETE FROM post_estado  WHERE ts<?", (agora - 30 * 86400,))
            db.execute("DELETE FROM oferta_index WHERE ts<?", (agora - 30 * 86400,))
            db.execute("DELETE FROM cupom_idx    WHERE ts<?", (agora - 30 * 86400,))
            db.execute("DELETE FROM origem_post  WHERE ts<?", (agora - 30 * 86400,))
        if len(_raw_cache) > 3000:
            for k in list(_raw_cache.keys())[:1000]:
                del _raw_cache[k]
        if len(_final_cache) > 3000:
            for k in list(_final_cache.keys())[:1000]:
                del _final_cache[k]
        log_db.debug("🗑 Limpeza temp OK")
    except Exception as e:
        log_db.error(f"❌ db_limpar: {e}")

def _db_count_links() -> int:
    try:
        with _db() as db:
            row = db.execute("SELECT COUNT(*) FROM links_cache").fetchone()
            return row[0] if row else 0
    except Exception:
        return 0


