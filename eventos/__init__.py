"""
Proveniência [F1.1] — fachada do emissor de eventos do worker.

`emitir()` é a porta de entrada de todo evento estruturado do worker.
Até `instalar()` ela é um no-op: sem a API privada configurada, nada é
guardado e nada muda no processo. Instalada, delega ao Registro
(eventos.anel), que numera por época (boot_id) e sequência (seq).

GARANTIAS (ver eventos.anel e tests/test_eventos_anel.py):
  - síncrona: sem await, sem rede, sem disco, sem SQLite;
  - nunca levanta: falha do emissor jamais chega ao pipeline — nem a
    de instalar(), que no pior caso deixa a proveniência desligada;
  - limitada: evento, anel e custo por evento têm teto.

Quem lê é a API privada (eventos.api_privada), a pedido do Brain. O
worker nunca envia nada a ninguém.

[F1.2-A1] O pipeline não chama emitir(): usa emitir_de(), a coleta
preguiçosa e blindada de eventos.coleta (o coletor só roda com o registro
instalado e nenhuma falha dele chega a quem chamou). emitir() direto fica
para main.py (processo.*). O catálogo de tipos e enums é eventos.catalogo.

Fronteira: este pacote não importa telethon, globals, database nem
sqlite3 — garantido por tests/test_eventos_main.py.
"""
from __future__ import annotations

from typing import Optional

from eventos.anel import (ENVELOPE_V, MAX_EVENTO_BYTES, MAX_TEXTO,
                          Registro, ler_cursor)

__all__ = ["instalar", "desligar", "emitir", "registro_atual",
           "Registro", "ler_cursor", "ENVELOPE_V", "MAX_EVENTO_BYTES",
           "MAX_TEXTO", "emitir_de", "execucao", "transferir_execucao",
           "inicio_na_fila", "marcar_desfecho", "adiar", "saude_coleta",
           "resumo_catalogo", "coleta_disponivel"]

_registro: Optional[Registro] = None


def instalar(capacidade: int, capacidade_bytes: int) -> Optional[Registro]:
    """Liga o registro do processo. Idempotente: UMA época por processo.
    Nunca levanta: se não der para criar, devolve None e tudo segue
    desligado (emitir() no-op) — o boot do worker não depende disto."""
    global _registro
    if _registro is None:
        try:
            _registro = Registro(capacidade, capacidade_bytes)
        except Exception:
            return None
    return _registro


def desligar() -> None:
    """Volta ao no-op (ex.: a API privada não pôde abrir). O registro
    sai junto: sem leitor, nada se acumula."""
    global _registro
    _registro = None


def registro_atual() -> Optional[Registro]:
    return _registro


def emitir(tipo: str, dados: Optional[dict] = None,
           corr: Optional[dict] = None) -> int:
    """Registra um evento; devolve o seq (0 = desligado ou recusado).
    Nunca levanta."""
    reg = _registro
    if reg is None:
        return 0
    try:
        return reg.emitir(tipo, dados, corr)
    except Exception:
        return 0


# ── [F1.2-A1] Coleta segura e catálogo ────────────────────────────
# Import BLINDADO: se a infraestrutura nova não carregar, a F1.1 segue
# igual e a coleta vira no-op — o boot do worker não depende disto.
# (Fica no fim: eventos.coleta lê _registro desta fachada.)
try:
    from eventos import catalogo
except Exception:                                      # noqa: BLE001
    catalogo = None
try:
    from eventos.coleta import (adiar, emitir_de, execucao, inicio_na_fila,
                                marcar_desfecho, saude_coleta,
                                transferir_execucao)
    coleta_disponivel = True
except Exception:                                      # noqa: BLE001
    coleta_disponivel = False

    def emitir_de(*_a, **_k) -> None:
        return None

    def transferir_execucao(*_a, **_k) -> None:
        return None

    def inicio_na_fila(*_a, **_k) -> None:
        return None

    def marcar_desfecho(*_a, **_k) -> None:
        return None

    def saude_coleta() -> dict:
        return {"disponivel": False}

    class _Nulo:
        def __init__(self, *_a, **_k) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_exc) -> bool:
            return False

    execucao = adiar = _Nulo


def resumo_catalogo() -> dict:
    """Versão e hash do catálogo (processo.iniciado). Nunca levanta."""
    try:
        return catalogo.resumo()
    except Exception:                                  # noqa: BLE001
        return {"versao": None, "hash": None, "tipos": None}
