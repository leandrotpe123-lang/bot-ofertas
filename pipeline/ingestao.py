"""Camada 1 — Ingestão. Extrai dados crus da mensagem. Zero lógica de negócio."""
from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import List

from telethon.tl.types import MessageMediaWebPage, MessageEntityCode, MessageEntityPre

from logger import log_ing, _ts_str, _idade_str
from pipeline.identidade import chat_canonico, username_para_log

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
    # Trechos do texto que vieram entre crases simples (`code`) ou
    # crases triplas (```pre```). São CANDIDATOS DE ALTA CONFIANÇA
    # para cupom-code, porque divulgadores profissionais sempre formatam
    # códigos de cupom assim no Telegram pra ficarem clicáveis/copiáveis.
    code_entities: List[str] = field(default_factory=list)


def _extrair_code_entities(message) -> List[str]:
    """
    Extrai trechos formatados como `código` ou ```bloco``` no Telegram.
    Retorna lista de strings (sem deduplicar — preserva ordem de aparição).
    """
    try:
        texto = (getattr(message, "raw_text", None)
                 or getattr(message, "message", "") or "")
        if not texto:
            return []
        entidades = getattr(message, "entities", None) or []
        resultados: List[str] = []
        for ent in entidades:
            if not isinstance(ent, (MessageEntityCode, MessageEntityPre)):
                continue
            try:
                offset = int(ent.offset)
                length = int(ent.length)
                # Telegram envia offsets em UTF-16. Converte para UTF-16 e
                # extrai o trecho correto. Se falhar, usa offset direto
                # (suficiente pra ASCII, que é o caso de cupons).
                try:
                    utf16 = texto.encode("utf-16-le")
                    trecho_bytes = utf16[offset * 2:(offset + length) * 2]
                    trecho = trecho_bytes.decode("utf-16-le", errors="ignore")
                except Exception:
                    trecho = texto[offset:offset + length]
                trecho = trecho.strip()
                if trecho:
                    resultados.append(trecho)
            except Exception:
                continue
        return resultados
    except Exception:
        return []


async def ingerir(event) -> MensagemBruta:
    """Extrai dados crus da mensagem. Zero lógica de negócio."""
    texto = event.message.text or getattr(event.message, "message", "") or ""
    links = [u.strip().rstrip('.,;)>]}!?') for u in _RE_URL.findall(texto)]
    tem_midia = (
        event.message.media is not None
        and not isinstance(event.message.media, MessageMediaWebPage)
    )
    code_entities = _extrair_code_entities(event.message)

    # Identidade vem do Módulo 1 (id numérico canônico). Username só p/ log.
    chat = chat_canonico(event)
    chat_user = await username_para_log(event)

    is_reply = bool(getattr(event.message, "reply_to", None))
    reply_to = 0
    if is_reply:
        try:
            reply_to = event.message.reply_to.reply_to_msg_id or 0
        except Exception:
            pass

    log_ing.info(
        f"🧭 TL | id={event.message.id} chat={chat} | INGERIDO | "
        f"ts_orig={_ts_str(event.message.date)} "
        f"editado_em={_ts_str(getattr(event.message, 'edit_date', None))}")
    log_ing.debug(
        f"🧭 TL | id={event.message.id} chat={chat} | "
        f"idade_ingestao={_idade_str(event.message.date)}")

    log_ing.debug(
        f"📩 id={event.message.id} "
        f"chat={('@' + chat_user) if chat_user else chat} (cid={chat}) "
        f"links={len(links)} midia={tem_midia} reply={is_reply} "
        f"codes={len(code_entities)}"
    )
    return MensagemBruta(
        msg_id=event.message.id, chat=chat, texto=texto,
        links=links, tem_midia=tem_midia, media_obj=event.message,
        is_reply=is_reply, reply_to=reply_to,
        code_entities=code_entities,
                    )
            
