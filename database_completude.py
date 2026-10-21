"""
Camada 7 — Banco: estado da completude da entrada.

Responsabilidade ÚNICA: a tabela `completude_canal` — por canal-fonte, o
maior id de mensagem já observado e os ids de buraco ainda sem desfecho
(texto que só pipeline.completude escreve e interpreta). É o que faz a
observação continuar do mesmo ponto depois de um reinício. Não participa
de identidade, família, decisão nem publicação.

O escritor único é pipeline.completude, no loop do worker. As funções
nunca levantam: a completude jamais pode impedir a entrada de mensagem.
"""
from __future__ import annotations

import time
from typing import Dict, Iterable, Optional, Tuple

from database_conexao import _db


def db_completude_carregar(chats: Iterable[int]) -> Optional[Dict[int, Tuple[int, str]]]:
    """{chat: (maior, pendentes)} dos canais pedidos que têm estado salvo.
    None = banco indisponível (diferente de "nenhum estado salvo": {})."""
    try:
        pedidos = [int(c) for c in chats]
        if not pedidos:
            return {}
        marcas = ",".join("?" * len(pedidos))
        with _db() as db:
            rows = db.execute(
                f"SELECT chat, maior, pendentes FROM completude_canal WHERE chat IN ({marcas})",
                pedidos).fetchall()
        return {row[0]: (row[1], row[2]) for row in rows}
    except Exception:
        return None


def db_completude_gravar(chat: int, maior: int, pendentes: str) -> bool:
    """Grava o estado do canal (uma linha por canal). True = gravado."""
    try:
        with _db() as db:
            db.execute(
                "INSERT OR REPLACE INTO completude_canal(chat,maior,pendentes,ts)"
                " VALUES(?,?,?,?)", (int(chat), int(maior), pendentes, time.time()))
        return True
    except Exception:
        return False
