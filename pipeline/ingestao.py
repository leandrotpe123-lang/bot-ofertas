"""Camada 1 — Ingestão. Extrai dados crus da mensagem. Zero lógica de negócio."""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import List

from telethon.tl.types import MessageMediaWebPage

from logger import log_ing

_RE_URL = re.compile(r'https?://[^\s\)\]>,"\'<\u200b\u200c\u200d\u2060]+')


@dataclass
class MensagemBruta:
    msg_id:    int
    chat:      str
    texto:     str
    links:     List[str]
    tem_midia: bool
    media_obj: object
    is_reply:  bool = False
    reply_to:  int  = 0


def ingerir(event) -> MensagemBruta:
    """Extrai dados crus da mensagem. Zero lógica de negócio."""
    texto = event.message.text or getattr(event.message, "message", "") or ""
    links = [u.strip().rstrip('.,;)>]}!?') for u in _RE_URL.findall(texto)]
    tem_midia = (
        event.message.media is not None
        and not isinstance(event.message.media, MessageMediaWebPage)
    )
    try:
        chat_obj = getattr(event, "_chat", None)
        username = getattr(chat_obj, "username", None)
        chat = (username or str(event.chat_id)).lower()
    except Exception:
        chat = str(event.chat_id)

    is_reply = bool(getattr(event.message, "reply_to", None))
    reply_to = 0
    if is_reply:
        try:
            reply_to = event.message.reply_to.reply_to_msg_id or 0
        except Exception:
            pass

    log_ing.debug(
        f"📩 id={event.message.id} chat={chat} "
        f"links={len(links)} midia={tem_midia} reply={is_reply}"
    )
    return MensagemBruta(
        msg_id=event.message.id, chat=chat, texto=texto,
        links=links, tem_midia=tem_midia, media_obj=event.message,
        is_reply=is_reply, reply_to=reply_to,
  )
  
