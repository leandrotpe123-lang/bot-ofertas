"""
Singleton do TelegramClient.
Módulo sem dependências internas — pode ser importado por qualquer camada.
Resolve o ciclo main ↔ pipeline.montagem / pipeline.publicacao.
"""
from __future__ import annotations

from telethon import TelegramClient
from telethon.sessions import StringSession

from config import API_ID, API_HASH, SESSION_STRING

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

