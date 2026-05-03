"""Helpers genéricos: JSON, mapa de mensagens."""
from __future__ import annotations
import json
import os
from threading import Lock
from typing import Optional

from config import ARQUIVO_MAPEAMENTO
from logger import log_sys

_MAP_LOCK = Lock()


def _ler_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_sys.error(f"❌ ler {path}: {e}")
        return {}


def _gravar_json(path: str, data: dict, lock: Lock):
    with lock:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log_sys.error(f"❌ gravar {path}: {e}")


def ler_mapa() -> dict:
    return _ler_json(ARQUIVO_MAPEAMENTO)


def salvar_mapa(m: dict):
    _gravar_json(ARQUIVO_MAPEAMENTO, m, _MAP_LOCK)
  
