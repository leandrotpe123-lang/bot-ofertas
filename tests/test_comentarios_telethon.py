"""COMENTÁRIOS contra o Telethon REAL — leitura da resposta e mecanismo de envio.

O que o Telethon falso do harness não prova:
  01  o handler lê o cabeçalho REAL de resposta (types.MessageReplyHeader):
      resposta do canal ao próprio post → agendado; resposta a mensagem de
      OUTRO chat (reply_to_peer_id) → recusada; mensagem que não é post do
      canal (usuário, from_id) → recusada
  02  MECANISMO DE RESPOSTA: saida.responder monta a SendMessageRequest para
      o canal principal (GRUPO_DESTINO), com reply_to = InputReplyToMessage
      do NOSSO post, SEM tópico (top_msg_id vazio: canal não tem tópico),
      SEM silent (toca para os inscritos), sem prévia de link e com o texto
      puro (parse_mode=None: nenhuma entidade). Não é o grupo de discussão
      vinculado: é o próprio canal, como as fontes fazem.
  03  instalar(): o handler nasce só sobre as fontes autorizadas (Nerd e
      Oferta Convertida fora), e main._preparar_processo o liga UMA vez,
      depois dos handlers da casa

Execução (standalone, stdlib + Telethon):
    python3 tests/test_comentarios_telethon.py
"""
from __future__ import annotations

import ast
import asyncio
import datetime as dt
import os
import sys
import time
import types as _pytypes

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import telethon  # noqa: E402  — REAL, antes do harness
from telethon import TelegramClient, events, functions, utils  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import types  # noqa: E402

assert hasattr(telethon.TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from _harness_e5 import RAIZ, preparar, rodar  # noqa: E402

preparar()

from database import _init_db  # noqa: E402

_init_db()

import config  # noqa: E402
from database_conexao import _db  # noqa: E402
from globals import _init_globals  # noqa: E402
from pipeline import comentarios, exclusao, saida  # noqa: E402

AGORA = dt.datetime.now(dt.timezone.utc)
TC = TelegramClient(StringSession(), 1, "0" * 32)
TC._mb_entity_cache.set_self_user(424242, False, 0)


def canal(cid_marcado: int, nome: str):
    real = int(str(cid_marcado)[4:])
    return types.Channel(id=real, title=nome, photo=types.ChatPhotoEmpty(), date=AGORA,
                         access_hash=77, username=nome, broadcast=True)


PROMOTOM = canal(config.GRUPO_ID["promotom"], "promotom")
FADA = canal(config.GRUPO_ID["fadapromos"], "fadapromos")


def mensagem(ch, mid, texto, reply_to, post=True, from_id=None):
    m = types.Message(id=mid, peer_id=types.PeerChannel(ch.id), date=AGORA, message=texto,
                      post=post, reply_to=reply_to, from_id=from_id)
    m._finish_init(TC, {utils.get_peer_id(ch): ch}, None)
    return m


class Ev:
    def __init__(self, m):
        self.message, self.chat_id = m, m.chat_id


def test_01_leitura_da_resposta_real(r):
    dest = 61001
    with _db() as db:
        db.execute("INSERT OR REPLACE INTO post_estado(msg_id_dest,texto,ts) VALUES(?,?,?)",
                   (dest, "🔥 Fritadeira Wap\n✅ R$ 235", time.time()))
        db.execute("INSERT OR REPLACE INTO origem_post(chat,msg_id,dest,ts) VALUES(?,?,?,?)",
                   (str(utils.get_peer_id(PROMOTOM)), 113150, dest, time.time()))
    enviados = []

    async def espiao(d, texto):
        enviados.append((d, texto))
        return _pytypes.SimpleNamespace(id=1)

    ok = mensagem(PROMOTOM, 113151, "Isso é preção de vdd",
                  types.MessageReplyHeader(reply_to_msg_id=113150))
    cruzada = mensagem(PROMOTOM, 113152, "Precinho", types.MessageReplyHeader(
        reply_to_msg_id=14472, reply_to_peer_id=types.PeerChannel(FADA.id)))
    usuario = mensagem(PROMOTOM, 113153, "Precinho",
                       types.MessageReplyHeader(reply_to_msg_id=113150), post=False,
                       from_id=types.PeerUser(555))

    async def corpo():
        _init_globals()
        real, espera = saida.responder, comentarios.ESPERA_ANCORA_S
        saida.responder, comentarios.ESPERA_ANCORA_S = espiao, (0.01,)
        exclusao._POST_LOCKS.clear()
        try:
            res = [comentarios.ao_receber(Ev(m)) for m in (ok, cruzada, usuario)]
            await asyncio.gather(*list(comentarios._TAREFAS))
            return res
        finally:
            saida.responder, comentarios.ESPERA_ANCORA_S = real, espera
    res = asyncio.run(corpo())
    r.check(res == ["agendado", "resposta_de_outro_chat", "nao_e_post_do_canal"],
            "01.desfechos", str(res))
    r.check(enviados == [(dest, "Isso é preção de vdd")], "01.um_envio", str(enviados))


def test_02_mecanismo_de_resposta(r):
    from client import client
    pedidos, entidades = [], []
    destino = types.InputPeerChannel(channel_id=1999999999, access_hash=42)

    async def get_input_entity(alvo):
        entidades.append(alvo)
        return destino

    async def chamada(sender, request, ordered=False, flood_sleep_threshold=None):
        pedidos.append(request)
        return types.UpdateShortSentMessage(out=True, id=88001, pts=1, pts_count=1,
                                            date=AGORA)

    async def corpo():
        _init_globals()
        orig = (client.get_input_entity, client._call)
        client.get_input_entity, client._call = get_input_entity, chamada
        try:
            return await saida.responder(34670, "Precinho **forte** __x__")
        finally:
            client.get_input_entity, client._call = orig
    resp = asyncio.run(corpo())
    req = pedidos[0] if pedidos else None
    r.check(isinstance(req, functions.messages.SendMessageRequest), "02.send_message",
            repr(type(req)))
    r.check(entidades and entidades[0] == config.GRUPO_DESTINO and req.peer == destino,
            "02.destino_canal_principal", f"{entidades} {getattr(req, 'peer', None)}")
    rt = getattr(req, "reply_to", None)
    r.check(isinstance(rt, types.InputReplyToMessage) and rt.reply_to_msg_id == 34670
            and not getattr(rt, "top_msg_id", None), "02.marca_o_nosso_post_sem_topico",
            repr(rt))
    r.check(not req.silent and req.no_webpage and req.message == "Precinho **forte** __x__"
            and not req.entities, "02.nao_silencioso_texto_puro",
            f"silent={req.silent} no_webpage={req.no_webpage} msg={req.message!r} "
            f"ent={req.entities}")
    r.check(getattr(resp, "id", None) == 88001, "02.id_da_resposta_e_a_prova", repr(resp))


def test_03_instalacao(r):
    class Cliente:
        def __init__(self):
            self.builders = []

        def on(self, builder):
            self.builders.append(builder)
            return lambda f: f

    nerd = canal(-1001465877129, "nerdofertas")
    convertida = canal(config.GRUPO_ID["ofertasconvertidas"], "ofertasconvertidas")
    outras = [canal(config.GRUPO_ID[n], n.lower())
              for n in ("fumotom", "SamuelF3lipePromo", "fadadoscupons")]
    cli = Cliente()
    n = comentarios.instalar(cli, [PROMOTOM, FADA, nerd, convertida, *outras])
    b = cli.builders[0] if cli.builders else None
    chats = {utils.get_peer_id(c) for c in (getattr(b, "chats", None) or [])}
    r.check(n == 5 and isinstance(b, events.NewMessage)
            and chats == set(comentarios.FONTES), "03.so_fontes_autorizadas",
            f"n={n} chats={chats}")
    arvore = ast.parse(open(os.path.join(RAIZ, "main.py"), encoding="utf-8").read())
    prep = next(f for f in ast.walk(arvore)
                if isinstance(f, ast.AsyncFunctionDef) and f.name == "_preparar_processo")

    def linhas(attr_obj, attr):
        return [c.lineno for c in ast.walk(prep) if isinstance(c, ast.Call)
                and isinstance(c.func, ast.Attribute) and c.func.attr == attr
                and isinstance(c.func.value, ast.Name) and c.func.value.id == attr_obj]
    inst = linhas("comentarios", "instalar")
    reg = [c.lineno for c in ast.walk(prep) if isinstance(c, ast.Call)
           and isinstance(c.func, ast.Name) and c.func.id == "_registrar_handlers"]
    r.check(len(inst) == 1 and reg and inst[0] > reg[0], "03.main_liga_uma_vez_depois",
            f"instalar={inst} registrar={reg}")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "COMENTÁRIOS · Telethon real · resposta e mecanismo"))
