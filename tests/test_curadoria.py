"""
CURADORIA — hardware só vai ao canal com aval do Fada Promoções ou do Nerd
Ofertas. Pelo caminho REAL: Telethon real, `processar` real (travas de
admissão), `_pipeline` real, enriquecimento, portão, deduplicação, montagem e
publicação reais, banco real (temporário). Só a normalização, que faz rede
(afiliação), é trocada por um dicionário com a identidade de cada mensagem —
a MESMA forma (MensagemNormalizada.idents) que a normalização real entrega.

As regras do Léo (03/11/2026), uma por teste:
  01  Fumotom manda a placa de vídeo primeiro → TRAVA; o Fada solta a mesma
      placa 2 min depois → o Fada sai normal e a do Fumotom DESTRAVA e casa
      no mesmo post (nenhuma duplicata)
  02  passou de 5 min sem aval → a segurada expira e não vai ao canal; se o
      Fada mandar depois, o post do Fada sai normal
  03  o Nerd soltou ANTES → a do Promotom passa direto; nada do Nerd vai ao
      canal
  04  o Promotom manda primeiro, o Nerd 3 min depois → a do Promotom destrava
      (a reinjeção atravessa a trava de 120 s e a idempotência, com prova)
  05  o Nerd fora da janela (6 min) → não solta; "não tem problema"
  06  categoria certa: notebook gamer, PC gamer, monitor comum, air fryer…
      passam direto; monitor gamer, fonte, cooler… travam
  07  Samuel Felipe, o grupo de teste e o Fada dos Cupons também travam;
      o post do Fada (curador) sai normal e vira aval
  08  produto DIFERENTE no Nerd (título parecido, outro ASIN) não solta
  09  edição da segurada continua segurada (a janela não recomeça); edição
      da já publicada sincroniza normal
  10  a fonte apagou a segurada antes do aval → nada vai ao canal
  11  três fontes com a mesma placa, depois o Fada → as três destravam e o
      canal tem UM post só
  12  a exceção de admissão é estreita: só via CURADORIA + prova no banco +
      dentro da janela; nada mais passa pela trava de 120 s
  13  sem corrida: "segura" e "aval" no mesmo instante (threads) — nenhuma
      segurada fica sem o aval que chegou
  14  reinício: segurada e aval no banco; liberação interrompida é retomada
  15  SOMBRA publica como antes e só registra; DESLIGADO não toca em nada
  16  o canal de aval é LIDO (varredura): base no 1º ciclo, depois só o
      novo; handler e varredura não contam a mesma mensagem duas vezes;
      aval velho não solta nada
  17  falha do banco no portão deixa passar (nunca trava o canal por
      defeito interno); só produto passa pelo portão
  18  eventos: curadoria.retida encerra a execução como DESCARTADA; a
      reinjeção chega com via CURADORIA; o aval sai como curadoria.aval
  19  main.py: o canal de aval nunca entra nos handlers das fontes; a
      curadoria liga depois dos handlers e encerra antes de HTTP/Telegram/
      banco
  20  catálogo: o delta da curadoria é exatamente o declarado

Execução (standalone, stdlib + Telethon):  python3 tests/test_curadoria.py
"""
from __future__ import annotations

import ast
import asyncio
import datetime as dt
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _telethon_fakes import afetadas                        # noqa: E402

import telethon  # noqa: E402  — REAL, antes do harness
from telethon import TelegramClient, utils  # noqa: E402
from telethon import events as tl_events  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import types  # noqa: E402

assert hasattr(telethon.TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from _harness_e5 import preparar, rodar, ler_fonte  # noqa: E402

preparar()

import config                                                  # noqa: E402
import eventos                                                 # noqa: E402
import globals as g                                            # noqa: E402
import client as modulo_client                                 # noqa: E402
import plataformas                                             # noqa: E402
from database import _init_db, db_get_post                     # noqa: E402
from database_conexao import _db                               # noqa: E402
import database_curadoria as dbc                               # noqa: E402
from eventos import catalogo                                   # noqa: E402
from pipeline import (convergencia, curadoria, exclusao, normalizacao,  # noqa: E402
                      orchestrator, origem)
from pipeline import orchestrator_pipeline as op               # noqa: E402
from pipeline.completude import EventoRecuperado               # noqa: E402
from pipeline.identidade import _CHAT_USERNAME                 # noqa: E402
from pipeline.normalizacao import MensagemNormalizada          # noqa: E402

plataformas.inicializar()
_init_db()

# ─────────────────────────────────────────────────────────────────
# Canais (ids reais onde se sabe; os outros, fixos de teste)
# ─────────────────────────────────────────────────────────────────
_IDS = {"promotom": 1825680721, "fumotom": 3775401737, "SamuelF3lipePromo": 1768101197,
        "fadapromos": 1500000001, "ofertasconvertidas": 1500000002,
        "fadadoscupons": 1500000003, "nerdofertas": 1500000004}
TC = TelegramClient(StringSession(), 1, "0" * 32)
TC._mb_entity_cache.set_self_user(424242, False, 0)


def _canal(cid, user):
    return types.Channel(id=cid, title=user, photo=types.ChatPhotoEmpty(),
                         date=dt.datetime.now(dt.timezone.utc), access_hash=77,
                         username=user, broadcast=True)


ENT = {n: _canal(i, n) for n, i in _IDS.items()}
PEER = {n: utils.get_peer_id(e) for n, e in ENT.items()}
CHAT = {n: str(p) for n, p in PEER.items()}
for _n, _p in PEER.items():
    _CHAT_USERNAME[_p] = _n.lower()
FONTES = [ENT[n] for n in config.GRUPOS_ORIGEM]
AVAIS = [ENT["nerdofertas"]]


def _agora():
    return dt.datetime.now(dt.timezone.utc)


def mensagem(canal, mid, texto, idade_s=5.0, editada=False):
    m = types.Message(id=mid, peer_id=types.PeerChannel(_IDS[canal]),
                      date=_agora() - dt.timedelta(seconds=idade_s), message=texto,
                      post=True, edit_date=_agora() if editada else None)
    m._finish_init(TC, {PEER[canal]: ENT[canal]}, None)
    return m


# ─────────────────────────────────────────────────────────────────
# Identidade por mensagem — a forma que a normalização real entrega
# ─────────────────────────────────────────────────────────────────
NORMS: dict = {}
IDENTS_AVAL: dict = {}


def registrar_norm(canal, mid, texto, idents):
    ids = [i for _, i, _ in idents]
    NORMS[(CHAT[canal], mid)] = MensagemNormalizada(
        msg_id=mid, chat=CHAT[canal], texto_limpo=texto, texto_analise=texto,
        mapa={"http://o": "http://a"}, preservar=[], plat=idents[0][0] if idents else "",
        sku=ids[0] if ids else "", tem_midia=False, media_obj=None,
        ids_globais=ids, idents=list(idents))


async def _normalizar_falso(bruta):
    return NORMS.get((bruta.chat, bruta.msg_id))


async def _identificar_falso(links, msg_id=0):
    return list(IDENTS_AVAL.get(msg_id, []))


def amz(asin):
    return [("amazon", asin, "produto")]


# ─────────────────────────────────────────────────────────────────
# Telegram falso: o canal de destino e a busca por id nas fontes
# ─────────────────────────────────────────────────────────────────
class _Msg:
    def __init__(self, i):
        self.id, self.media, self.photo = i, None, None


_POST = {"n": 90000}


class Mundo:
    def __init__(self):
        self.criados, self.edits, self.deletes = [], [], []
        self.servidor, self.historico, self.handlers = {}, {}, []
        self.buscas, self.varreduras = [], []

    # canal de destino (pipeline.saida)
    async def send_message(self, dest, texto, parse_mode=None, link_preview=None):
        _POST["n"] += 1
        self.criados.append((_POST["n"], texto))
        return _Msg(_POST["n"])

    async def send_file(self, dest, img, caption=None, parse_mode=None,
                        force_document=False):
        return await self.send_message(dest, caption)

    async def edit_message(self, dest, msg_id, texto, parse_mode=None, file=None):
        self.edits.append((msg_id, texto))
        return _Msg(msg_id)

    async def delete_messages(self, dest, msg_id):
        self.deletes.append(msg_id)
        return afetadas(msg_id)

    # fontes e canal de aval (pipeline.curadoria)
    def add_event_handler(self, cb, ev):
        self.handlers.append((cb, ev))

    async def get_messages(self, ent, ids=None, limit=None, min_id=None, max_id=None):
        peer = utils.get_peer_id(ent)
        if ids is not None:
            self.buscas.append((peer, list(ids)))
            return [self.servidor.get((peer, i)) for i in ids]
        self.varreduras.append((peer, limit, min_id, max_id))
        todas = sorted(self.historico.get(peer, []), key=lambda m: -m.id)
        if min_id is not None:
            todas = [m for m in todas if m.id > min_id]
        if max_id is not None:
            todas = [m for m in todas if m.id < max_id]
        return todas[:limit]


async def drenar():
    for _ in range(60):
        pend = [t for t in list(g._buf) if isinstance(t, asyncio.Task) and not t.done()]
        pend += [t for t in list(curadoria._TAREFAS) if not t.done()]
        if not pend:
            return
        await asyncio.gather(*pend, return_exceptions=True)


def _limpar_curadoria():
    with _db() as db:
        db.execute("DELETE FROM curadoria_aval")
        db.execute("DELETE FROM curadoria_retida")


def cenario(corpo, modo="ATIVO"):
    async def _run():
        g._init_globals()
        g._encerrando = False
        for pool in (exclusao._IDENTITY_LOCKS, exclusao._IDENTITY_LOCKS_TS,
                     exclusao._POST_LOCKS, exclusao._POST_LOCKS_TS,
                     origem._LOCKS, origem._LOCKS_TS):
            pool.clear()
        origem._LOCKS_LCK = asyncio.Lock()
        convergencia._EM_CURSO.clear()
        _limpar_curadoria()
        mundo = Mundo()
        modulo_client.client = mundo
        orig = (op.normalizar, normalizacao.identificar_produtos, config.CURADORIA_MODO)
        op.normalizar = _normalizar_falso
        normalizacao.identificar_produtos = _identificar_falso
        config.CURADORIA_MODO = modo
        try:
            curadoria.instalar(mundo, FONTES, AVAIS, orchestrator.processar)
            await corpo(mundo)
            await drenar()
        finally:
            await curadoria.encerrar()
            op.normalizar, normalizacao.identificar_produtos, config.CURADORIA_MODO = orig
        return mundo
    return asyncio.run(_run())


_MID = {"n": 300000}


def novo_id():
    _MID["n"] += 1
    return _MID["n"]


async def chega(mundo, canal, texto, idents, *, mid=None, idade_s=5.0, editada=False):
    """Uma mensagem da fonte chega pelo handler (NewMessage/MessageEdited)."""
    mid = mid or novo_id()
    registrar_norm(canal, mid, texto, idents)
    m = mensagem(canal, mid, texto, idade_s=idade_s, editada=editada)
    mundo.servidor[(PEER[canal], mid)] = m
    await orchestrator.processar(tl_events.NewMessage.Event(m), is_edit=editada)
    await drenar()
    return mid


async def nerd(mundo, texto, idents, *, idade_s=5.0, pelo_handler=False):
    """O Nerd Ofertas posta (só aval: nunca entra no pipeline)."""
    mid = novo_id()
    IDENTS_AVAL[mid] = idents
    m = mensagem("nerdofertas", mid, texto, idade_s=idade_s)
    mundo.historico.setdefault(PEER["nerdofertas"], []).append(m)
    if pelo_handler:
        cb = mundo.handlers[0][0]
        await cb(tl_events.NewMessage.Event(m))
    else:
        await curadoria._observar_aval(PEER["nerdofertas"], m)
    await drenar()
    return mid


def retida(canal, mid):
    with _db() as db:
        row = db.execute("SELECT estado, ts FROM curadoria_retida WHERE chat=? AND msg_id=?",
                         (CHAT[canal], mid)).fetchone()
    return row[0] if row else None


def envelhecer(mundo, canal, mid, segundos):
    """O tempo passou: a mensagem da fonte e a linha dela têm `segundos`."""
    with _db() as db:
        db.execute("UPDATE curadoria_retida SET ts=? WHERE chat=? AND msg_id=?",
                   (time.time() - segundos, CHAT[canal], mid))
    m = mundo.servidor.get((PEER[canal], mid))
    if m is not None:
        m.date = _agora() - dt.timedelta(seconds=segundos)


def posts(mundo):
    return [p for p, _ in mundo.criados]


def vinculo(canal, mid):
    return origem.consultar(CHAT[canal], mid)


GPU = "Placa de Vídeo Gigabyte RTX 5060 Ti Eagle OC 8GB GDDR7\n\nR$ 2299"
GPU_FADA = "🔥 Placa de Video Gigabyte GeForce RTX 5060 Ti Eagle OC 8G\n\n✅ R$ 2299"
GPU_NERD = "➡️ Placa de Vídeo RTX 5060 Ti EAGLE OC 8G Gigabyte\n\n✅ R$ 2.299 😱😱"


# ══════════════════════════════════════════════════════════════════
def test_01_fumotom_primeiro_fada_destrava(r):
    s = {}

    async def corpo(mundo):
        a = "B0GPU00001"
        f = await chega(mundo, "fumotom", GPU, amz(a))
        s["antes"] = list(posts(mundo))
        s["estado_antes"] = retida("fumotom", f)
        fada = await chega(mundo, "fadapromos", GPU_FADA, amz(a), idade_s=3)
        s["depois"] = list(posts(mundo))
        s["estado"] = retida("fumotom", f)
        s["v_fumo"], s["v_fada"] = vinculo("fumotom", f), vinculo("fadapromos", fada)
        s["buscas"] = list(mundo.buscas)
    cenario(corpo)
    r.check(s["antes"] == [] and s["estado_antes"] == "retida", "01.fumotom_trava",
            str((s["antes"], s["estado_antes"])))
    r.check(len(s["depois"]) == 1, "01.fada_sai_normal_e_um_post_so", str(s["depois"]))
    r.check(s["estado"] == "liberada", "01.fumotom_destravou", str(s["estado"]))
    r.check(s["v_fumo"] == s["v_fada"] == s["depois"][0], "01.mesmo_post",
            str((s["v_fumo"], s["v_fada"], s["depois"])))
    r.check(s["buscas"] == [(PEER["fumotom"], [s["buscas"][0][1][0]])],
            "01.buscou_a_segurada_na_fonte_uma_vez", str(s["buscas"]))


def test_02_passou_de_5_min_expira_e_o_fada_sai_normal(r):
    s = {}

    async def corpo(mundo):
        a = "B0GPU00002"
        f = await chega(mundo, "fumotom", GPU, amz(a))
        envelhecer(mundo, "fumotom", f, 7 * 60 + 30)
        curadoria._expirar()
        s["estado"] = retida("fumotom", f)
        await chega(mundo, "fadapromos", GPU_FADA, amz(a))
        s["posts"] = list(mundo.criados)
        s["estado_final"] = retida("fumotom", f)
        s["buscas"] = list(mundo.buscas)
    cenario(corpo)
    r.check(s["estado"] == "expirada", "02.expirou", str(s["estado"]))
    r.check(len(s["posts"]) == 1 and "🔥" in s["posts"][0][1], "02.so_o_post_do_fada",
            str(s["posts"]))
    r.check(s["estado_final"] == "expirada" and s["buscas"] == [],
            "02.a_segurada_nao_volta", str((s["estado_final"], s["buscas"])))


def test_03_nerd_antes_promotom_passa_direto(r):
    s = {}

    async def corpo(mundo):
        a = "B0GPU00003"
        await nerd(mundo, GPU_NERD, amz(a), idade_s=60)
        s["apos_nerd"] = list(posts(mundo))
        p = await chega(mundo, "promotom", GPU, amz(a))
        s["posts"] = list(posts(mundo))
        s["estado"] = retida("promotom", p)
    cenario(corpo)
    r.check(s["apos_nerd"] == [], "03.nada_do_nerd_vai_ao_canal", str(s["apos_nerd"]))
    r.check(len(s["posts"]) == 1 and s["estado"] is None, "03.promotom_passa_direto",
            str((s["posts"], s["estado"])))


def test_04_promotom_primeiro_nerd_3min_depois_destrava(r):
    s = {}

    async def corpo(mundo):
        a = "B0GPU00004"
        p = await chega(mundo, "promotom", GPU, amz(a))
        s["antes"] = list(posts(mundo))
        envelhecer(mundo, "promotom", p, 3 * 60)
        await nerd(mundo, GPU_NERD, amz(a))
        s["posts"] = list(mundo.criados)
        s["estado"] = retida("promotom", p)
        s["v"] = vinculo("promotom", p)
    cenario(corpo)
    r.check(s["antes"] == [], "04.trava", str(s["antes"]))
    r.check(len(s["posts"]) == 1 and "Gigabyte RTX 5060" in s["posts"][0][1]
            and "😱" not in s["posts"][0][1], "04.sai_o_texto_do_promotom",
            str(s["posts"])[:200])
    r.check(s["estado"] == "liberada" and s["v"] == s["posts"][0][0],
            "04.reinjecao_atravessou_trava_e_idempotencia", str((s["estado"], s["v"])))


def test_05_nerd_fora_da_janela_nao_solta(r):
    s = {}

    async def corpo(mundo):
        a = "B0GPU00005"
        p = await chega(mundo, "promotom", GPU, amz(a))
        envelhecer(mundo, "promotom", p, 6 * 60)
        await nerd(mundo, GPU_NERD, amz(a))
        s["posts"], s["estado"], s["buscas"] = list(posts(mundo)), retida("promotom", p), \
            list(mundo.buscas)
        await nerd(mundo, GPU_NERD, amz("B0GPU00005"), idade_s=20 * 60)
        s["nerd_velho"] = list(posts(mundo))
    cenario(corpo)
    r.check(s["posts"] == [] and s["estado"] == "retida" and s["buscas"] == [],
            "05.nao_solta", str((s["posts"], s["estado"], s["buscas"])))
    r.check(s["nerd_velho"] == [], "05.aval_velho_ignorado")


def test_06_categoria_certa(r):
    s = {}
    livres = [
        ("Notebook Gamer Acer Nitro V15 RTX 4050 i5 8GB", "B0LIVRE001"),
        ("PC Gamer Completo Ryzen 5 5600 RX 7600 16GB", "B0LIVRE002"),
        ("Monitor LG 24MR400 24 IPS Full HD 100Hz", "B0LIVRE003"),
        ("Fritadeira Air Fryer Mondial 4L", "B0LIVRE004"),
        ("Fonte de Água para Gatos Amicus 1,2L", "B0LIVRE005"),
        ("Caixa Térmica Cooler 32 Litros", "B0LIVRE006"),
        ("SSD Externo Portátil Samsung T7 1TB", "B0LIVRE007"),
        ("Suporte Articulado para Monitor 17 a 32", "B0LIVRE008"),
    ]
    presos = [
        ("Monitor Gamer AOC Agon G42 27 200Hz", "B0PRESO001"),
        ("Fonte Mach1 Steady 750W 80 Plus Bronze", "B0PRESO002"),
        ("Water Cooler Kalkan Aura 360mm ARGB", "B0PRESO003"),
        ("Kit 3 Fans ARGB 120mm Jumpeak", "B0PRESO004"),
        ("Processador AMD Ryzen 7 5700X", "B0PRESO005"),
        ("Placa-Mãe Asus TUF Gaming B550M-Plus", "B0PRESO006"),
        ("Memória RAM Kingston Fury 16GB DDR4 3200MHz", "B0PRESO007"),
        ("SSD Kingston NV3 1TB NVMe M.2", "B0PRESO008"),
        ("Gabinete Gamer Mancer CV450 Mini-Tower", "B0PRESO009"),
    ]

    async def corpo(mundo):
        for titulo, a in livres:
            await chega(mundo, "fumotom", f"{titulo}\n\nR$ 199", amz(a))
        s["livres"] = len(mundo.criados)
        for titulo, a in presos:
            await chega(mundo, "fumotom", f"{titulo}\n\nR$ 199", amz(a))
        s["total"] = len(mundo.criados)
        with _db() as db:
            s["cats"] = sorted(c for (c,) in db.execute(
                "SELECT categoria FROM curadoria_retida WHERE estado='retida'"))
    cenario(corpo)
    r.check(s["livres"] == len(livres), "06.parecidos_passam_direto", str(s["livres"]))
    r.check(s["total"] == len(livres), "06.vigiados_travam", str(s["total"]))
    r.check(s["cats"] == sorted(["monitor_gamer", "fonte", "cooler", "cooler", "processador",
                                 "placa_mae", "memoria", "ssd", "gabinete"]),
            "06.categoria_registrada", str(s["cats"]))


def test_07_quem_trava_e_quem_e_curador(r):
    s = {}

    async def corpo(mundo):
        ids = {}
        for canal in ("SamuelF3lipePromo", "ofertasconvertidas", "fadadoscupons",
                      "promotom", "fumotom"):
            ids[canal] = await chega(mundo, canal, GPU, amz(f"B0QUEM{canal[:4].upper()}"))
        s["travados"] = {c: retida(c, m) for c, m in ids.items()}
        s["antes_fada"] = list(posts(mundo))
        fada = await chega(mundo, "fadapromos", GPU_FADA, amz("B0SOFADA01"))
        s["fada"] = list(posts(mundo))
        s["fada_retida"] = retida("fadapromos", fada)
        with _db() as db:
            s["aval"] = db.execute("SELECT chave, fonte FROM curadoria_aval").fetchall()
    cenario(corpo)
    r.check(set(s["travados"].values()) == {"retida"} and s["antes_fada"] == [],
            "07.todas_as_fontes_comuns_travam", str(s["travados"]))
    r.check(len(s["fada"]) == 1 and s["fada_retida"] is None, "07.fada_sai_normal",
            str((s["fada"], s["fada_retida"])))
    r.check(s["aval"] == [("amazon|B0SOFADA01", CHAT["fadapromos"])], "07.fada_vira_aval",
            str(s["aval"]))
    r.check(config.CURADORES == {"fadapromos", "nerdofertas"}
            and config.GRUPOS_AVAL == ["nerdofertas"]
            and "nerdofertas" not in config.GRUPOS_ORIGEM, "07.config")


def test_08_produto_diferente_nao_solta(r):
    s = {}

    async def corpo(mundo):
        p = await chega(mundo, "promotom", GPU, amz("B0DIFER001"))
        await nerd(mundo, GPU_NERD, amz("B0DIFER002"))
        s["posts"], s["estado"] = list(posts(mundo)), retida("promotom", p)
    cenario(corpo)
    r.check(s["posts"] == [] and s["estado"] == "retida", "08.outro_asin_nao_solta",
            str((s["posts"], s["estado"])))


def test_09_edicoes(r):
    s = {}

    async def corpo(mundo):
        a = "B0EDICAO01"
        p = await chega(mundo, "promotom", GPU, amz(a))
        with _db() as db:
            ts0 = db.execute("SELECT ts FROM curadoria_retida WHERE msg_id=?", (p,)).fetchone()[0]
        await chega(mundo, "promotom", GPU + "\n-CUPOM: PLACA10", amz(a), mid=p,
                    idade_s=40, editada=True)
        with _db() as db:
            s["ts_igual"] = db.execute("SELECT ts FROM curadoria_retida WHERE msg_id=?",
                                       (p,)).fetchone()[0] == ts0
        s["edit_segurada"] = (list(posts(mundo)), retida("promotom", p))
        await nerd(mundo, GPU_NERD, amz(a))
        s["publicado"] = list(posts(mundo))
        await chega(mundo, "promotom", GPU + "\n-CUPOM: PLACA15", amz(a), mid=p,
                    idade_s=60, editada=True)
        s["edits"] = list(mundo.edits)
    cenario(corpo)
    r.check(s["edit_segurada"] == ([], "retida") and s["ts_igual"],
            "09.edicao_da_segurada_continua_segurada", str(s["edit_segurada"]))
    r.check(len(s["publicado"]) == 1, "09.aval_solta", str(s["publicado"]))
    r.check(len(s["edits"]) == 1 and "PLACA15" in s["edits"][0][1]
            and s["edits"][0][0] == s["publicado"][0], "09.edicao_da_publicada_sincroniza",
            str(s["edits"])[:200])


def test_10_fonte_apagou_antes_do_aval(r):
    s = {}

    async def corpo(mundo):
        a = "B0APAGOU01"
        p = await chega(mundo, "fumotom", GPU, amz(a))
        del mundo.servidor[(PEER["fumotom"], p)]
        await chega(mundo, "fadapromos", GPU_FADA, amz(a))
        s["posts"], s["estado"] = list(mundo.criados), retida("fumotom", p)
    cenario(corpo)
    r.check(len(s["posts"]) == 1 and "🔥" in s["posts"][0][1] and s["estado"] == "ausente",
            "10.so_o_fada_e_a_segurada_ausente", str((s["posts"], s["estado"])))


def test_11_tres_fontes_e_o_fada_um_post_so(r):
    s = {}

    async def corpo(mundo):
        a = "B0TRES0001"
        ids = [await chega(mundo, c, GPU, amz(a))
               for c in ("promotom", "fumotom", "SamuelF3lipePromo")]
        s["antes"] = list(posts(mundo))
        await chega(mundo, "fadapromos", GPU_FADA, amz(a))
        s["posts"] = list(posts(mundo))
        s["estados"] = [retida(c, m) for c, m in
                        zip(("promotom", "fumotom", "SamuelF3lipePromo"), ids)]
        s["vinculos"] = {vinculo(c, m) for c, m in
                         zip(("promotom", "fumotom", "SamuelF3lipePromo"), ids)}
    cenario(corpo)
    r.check(s["antes"] == [], "11.as_tres_travam", str(s["antes"]))
    r.check(len(s["posts"]) == 1 and s["estados"] == ["liberada"] * 3,
            "11.destravam_e_um_post_so", str((s["posts"], s["estados"])))
    r.check(s["vinculos"] == {s["posts"][0]}, "11.as_tres_no_mesmo_post", str(s["vinculos"]))


def test_12_a_excecao_de_admissao_e_estreita(r):
    admitidos = []

    async def espiao(ev, is_edit):
        admitidos.append(ev.message.id)

    def liberando(mid, ha_s=0.0, canal="promotom"):
        with _db() as db:
            db.execute("INSERT OR REPLACE INTO curadoria_retida(chat,msg_id,chaves,categoria,"
                       "ts,estado,ts_estado) VALUES(?,?,?,?,?,?,?)",
                       (CHAT[canal], mid, "[]", "placa_de_video", time.time() - 200,
                        "liberando", time.time() - ha_s))

    real = orchestrator._enfileirar
    orchestrator._enfileirar = espiao
    _limpar_curadoria()
    try:
        async def corpo():
            g._encerrando = False
            m1 = mensagem("promotom", 910001, GPU, idade_s=200)
            await orchestrator.processar(tl_events.NewMessage.Event(m1))         # telegram
            m2 = mensagem("promotom", 910002, GPU, idade_s=200)
            await orchestrator.processar(EventoRecuperado(m2, via="CURADORIA"))  # sem prova
            liberando(910003)
            m3 = mensagem("promotom", 910003, GPU, idade_s=200)
            await orchestrator.processar(EventoRecuperado(m3, via="CURADORIA"))  # ADMITE
            liberando(910004)
            m4 = mensagem("promotom", 910004, GPU, idade_s=200)
            await orchestrator.processar(EventoRecuperado(m4, via="RECUPERACAO"))
            await orchestrator.processar(tl_events.NewMessage.Event(m4))
            liberando(910005)
            m5 = mensagem("promotom", 910005, GPU, idade_s=10 * 60)            # velha demais
            await orchestrator.processar(EventoRecuperado(m5, via="CURADORIA"))
            liberando(910006, ha_s=300)                                         # prova vencida
            m6 = mensagem("promotom", 910006, GPU, idade_s=200)
            await orchestrator.processar(EventoRecuperado(m6, via="CURADORIA"))
            liberando(910007, canal="fumotom")                                  # outro chat
            m7 = mensagem("promotom", 910007, GPU, idade_s=200)
            await orchestrator.processar(EventoRecuperado(m7, via="CURADORIA"))
        asyncio.run(corpo())
    finally:
        orchestrator._enfileirar = real
    r.check(admitidos == [910003], "12.so_com_as_tres_provas", str(admitidos))
    fonte = ler_fonte("pipeline/orchestrator_pipeline.py")
    r.check("not curadoria.reinjecao(event)" in fonte
            and fonte.count("curadoria.reinjecao(") == 1, "12.idempotencia_mesma_prova")


def test_13_sem_corrida_entre_segurar_e_aval(r):
    _limpar_curadoria()
    perdidos, base = [], time.time()
    for i in range(150):
        chave = f"amazon|B0CORRIDA{i:03d}"
        res = {}
        barreira = threading.Barrier(2)

        def segura():
            barreira.wait()
            res["portao"] = dbc.db_curadoria_avaliar("-1001", 50000 + i, [chave], "ssd",
                                                     base, 300, time.time())[0]

        def aval():
            barreira.wait()
            res["aval"] = dbc.db_curadoria_registrar_aval([chave], "-1002", 60000 + i, base,
                                                          300, base - 420, time.time())
        ts = [threading.Thread(target=segura), threading.Thread(target=aval)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        solta = any(m == 50000 + i for _, m, _ in (res["aval"] or []))
        if not (res["portao"] == dbc.AVAL or solta):
            perdidos.append((i, res))
    r.check(perdidos == [], "13.nenhuma_segurada_perde_o_aval", str(perdidos[:3]))
    # as duas ordens, deterministicas
    _limpar_curadoria()
    r.check(dbc.db_curadoria_avaliar("-1001", 1, ["k|1"], "ssd", base, 300, base)
            == (dbc.SEGURADA, True)
            and dbc.db_curadoria_registrar_aval(["k|1"], "-1002", 2, base + 10, 300,
                                                base - 420, base) == [("-1001", 1, "ssd")]
            and dbc.db_curadoria_avaliar("-1001", 1, ["k|1"], "ssd", base, 300, base)
            == (dbc.LIBERACAO, False), "13.segura_depois_aval")
    r.check(dbc.db_curadoria_registrar_aval(["k|2"], "-1002", 3, base, 300, base - 420, base)
            == [] and dbc.db_curadoria_avaliar("-1001", 4, ["k|2"], "ssd", base + 10, 300, base)
            == (dbc.AVAL, False), "13.aval_depois_segura")


def test_14_reinicio(r):
    s = {}

    async def corpo(mundo):
        a = "B0REINIC01"
        p = await chega(mundo, "fumotom", GPU, amz(a))
        await curadoria.encerrar()                              # o processo cai…
        curadoria.instalar(mundo, FONTES, AVAIS, orchestrator.processar)   # …e volta
        await chega(mundo, "fadapromos", GPU_FADA, amz(a))
        s["a"] = (list(posts(mundo)), retida("fumotom", p))
        # liberação interrompida no meio: fica `liberando`; o boot retoma
        b = "B0REINIC02"
        q = await chega(mundo, "promotom", GPU, amz(b))
        with _db() as db:
            db.execute("UPDATE curadoria_retida SET estado='liberando', ts_estado=? "
                       "WHERE msg_id=?", (time.time() - 3600, q))
        await curadoria.encerrar()
        curadoria.instalar(mundo, FONTES, AVAIS, orchestrator.processar)
        await drenar()
        s["b"] = (len(posts(mundo)), retida("promotom", q), vinculo("promotom", q))
    cenario(corpo)
    r.check(len(s["a"][0]) == 1 and s["a"][1] == "liberada", "14.aval_depois_do_reinicio",
            str(s["a"]))
    r.check(s["b"][0] == 2 and s["b"][1] == "liberada" and s["b"][2],
            "14.liberacao_interrompida_retomada", str(s["b"]))


def test_15_sombra_e_desligado(r):
    s = {}

    async def corpo(mundo):
        a = "B0SOMBRA01"
        p = await chega(mundo, "fumotom", GPU, amz(a))
        s["publicou"] = len(posts(mundo))
        s["registrou"] = retida("fumotom", p)
        await chega(mundo, "fadapromos", GPU_FADA, amz(a))
        s["final"] = (len(posts(mundo)), retida("fumotom", p), list(mundo.buscas))
    cenario(corpo, modo="SOMBRA")
    r.check(s["publicou"] == 1 and s["registrou"] == "retida", "15.sombra_publica_e_registra",
            str(s))
    r.check(s["final"][0] == 1 and s["final"][1] == "liberada" and s["final"][2] == [],
            "15.sombra_nao_reinjeta", str(s["final"]))

    d = {}

    async def corpo2(mundo):
        p = await chega(mundo, "fumotom", GPU, amz("B0DESLIG01"))
        d["x"] = (len(posts(mundo)), retida("fumotom", p), mundo.handlers)
    cenario(corpo2, modo="DESLIGADO")
    r.check(d["x"] == (1, None, []), "15.desligado_nao_toca", str(d["x"]))


def test_16_varredura_do_canal_de_aval(r):
    s = {}

    async def corpo(mundo):
        peer, ent = PEER["nerdofertas"], ENT["nerdofertas"]
        velho = mensagem("nerdofertas", 700001, GPU_NERD, idade_s=3600)
        recente = mensagem("nerdofertas", 700002, GPU_NERD, idade_s=30)
        IDENTS_AVAL[700001], IDENTS_AVAL[700002] = amz("B0VARRE001"), amz("B0VARRE002")
        mundo.historico[peer] = [velho, recente]
        chamadas = []
        real = curadoria._observar_aval

        async def conta(chat_id, m):
            chamadas.append(m.id)
            await real(chat_id, m)
        curadoria._observar_aval = conta
        try:
            await curadoria._varrer_canal(peer, ent)
            novo = mensagem("nerdofertas", 700003, GPU_NERD, idade_s=5)
            IDENTS_AVAL[700003] = amz("B0VARRE003")
            mundo.historico[peer].append(novo)
            await mundo.handlers[0][0](tl_events.NewMessage.Event(novo))   # handler…
            await drenar()
            await curadoria._varrer_canal(peer, ent)                      # …e varredura
            await drenar()
        finally:
            curadoria._observar_aval = real
        with _db() as db:
            s["avais"] = sorted(c for (c,) in db.execute("SELECT chave FROM curadoria_aval"))
        s["varreduras"] = list(mundo.varreduras)
        s["chamadas"] = chamadas
        s["posts"] = list(posts(mundo))
    cenario(corpo)
    r.check(s["avais"] == ["amazon|B0VARRE002", "amazon|B0VARRE003"],
            "16.so_o_recente_e_o_novo_viram_aval", str(s["avais"]))
    r.check(s["varreduras"][0][2] is None and s["varreduras"][1][2] == 700002,
            "16.base_e_depois_so_o_novo", str(s["varreduras"]))
    r.check(s["posts"] == [], "16.nada_do_nerd_publicado")
    r.check(s["chamadas"].count(700003) == 2, "16.handler_e_varredura_trazem_a_mesma",
            str(s["chamadas"]))


def test_17_falha_de_banco_e_natureza(r):
    s = {}

    async def corpo(mundo):
        real = curadoria.db_curadoria_avaliar
        curadoria.db_curadoria_avaliar = lambda *a, **k: (dbc.ERRO, False)
        try:
            await chega(mundo, "fumotom", GPU, amz("B0FALHA001"))
        finally:
            curadoria.db_curadoria_avaliar = real
        s["posts"] = len(posts(mundo))
    cenario(corpo)
    r.check(s["posts"] == 1, "17.banco_fora_deixa_passar", str(s["posts"]))

    class Enr:
        tipo = "cupom"
    n = MensagemNormalizada(msg_id=1, chat=CHAT["fumotom"],
                            texto_limpo="Cupom KaBuM em Placas de Vídeo 10% OFF",
                            texto_analise="Cupom KaBuM em Placas de Vídeo 10% OFF",
                            mapa={}, preservar=[], plat="", sku="", tem_midia=False,
                            media_obj=None)
    r.check(curadoria.portao(n, Enr(), False) is True
            and curadoria.portao(n, "ENR:1", False) is True, "17.so_produto")


def test_18_eventos(r):
    eventos.desligar()
    reg = eventos.instalar(200000, 512 << 20)
    seq0 = reg.saude()["seq_ultimo"]
    s = {}

    async def corpo(mundo):
        a = "B0EVENTO01"
        s["p"] = await chega(mundo, "promotom", GPU, amz(a))
        envelhecer(mundo, "promotom", s["p"], 60)
        await chega(mundo, "fadapromos", GPU_FADA, amz(a))
    try:
        cenario(corpo)
        evs = [json.loads(c) for c in reg.ler((reg.boot_id, seq0), 1000000, 1 << 30)["cargas"]]
    finally:
        eventos.desligar()
    da = [e for e in evs if e["corr"].get("msg") == s["p"]]
    tipos = [e["tipo"] for e in da]
    ex_retida = next((e["corr"]["exec"] for e in da if e["tipo"] == "curadoria.retida"), None)
    fim = [e["dados"].get("resultado") for e in da
           if e["tipo"] == "execucao.fim" and e["corr"].get("exec") == ex_retida]
    r.check(ex_retida is not None and fim == ["DESCARTADA"], "18.retida_encerra_descartada",
            str(tipos))
    vias = [e["dados"].get("via") for e in da if e["tipo"] == "origem.recebida"]
    r.check(vias == ["TELEGRAM", "CURADORIA"], "18.reinjecao_com_via_curadoria", str(vias))
    aval = [e for e in evs if e["tipo"] == "curadoria.aval"]
    r.check(len(aval) == 1 and aval[0]["dados"].get("solta_n") == 1
            and aval[0]["dados"].get("categoria") == "placa_de_video", "18.aval",
            str([e["dados"] for e in aval]))


def test_19_main(r):
    arv = ast.parse(ler_fonte("main.py"))
    prep = next(n for n in ast.walk(arv)
                if isinstance(n, ast.AsyncFunctionDef) and n.name == "_preparar_processo")
    corpo = [ast.unparse(s) for s in prep.body]
    i_av = next(i for i, c in enumerate(corpo) if "config.GRUPOS_AVAL" in c)
    i_hd = corpo.index("_registrar_handlers(fontes)")
    i_cur = next(i for i, c in enumerate(corpo) if "curadoria.instalar(client, fontes, avais, "
                 "processar)" in c)
    i_pa = next(i for i, c in enumerate(corpo) if "post_apagado.instalar" in c)
    r.check(i_av < i_hd < i_pa < i_cur, "19.ordem_no_boot", str((i_av, i_hd, i_pa, i_cur)))
    r.check(not any("avais" in c for c in corpo if "completude.instalar" in c
                    or "_registrar_handlers" in c or "origem_apagada.instalar" in c
                    or "sucessao.instalar" in c), "19.aval_fora_dos_handlers_das_fontes")
    enc = next(n for n in ast.walk(arv)
               if isinstance(n, ast.AsyncFunctionDef) and n.name == "_encerrar")
    txt = [ast.unparse(s) for s in enc.body]

    def idx(trecho):
        return next(i for i, c in enumerate(txt) if trecho in c)
    r.check(idx("post_apagado.encerrar") < idx("curadoria.encerrar")
            < idx("espelho_cupons.encerrar") < idx("g._http_session.close")
            < idx("client.disconnect") < idx("_fechar_db"), "19.encerra_antes_dos_recursos")


def test_21_edicao_que_troca_o_produto(r):
    """A fonte corrigiu o link da segurada (produto novo): o aval do produto
    NOVO também a solta."""
    s = {}

    async def corpo(mundo):
        p = await chega(mundo, "promotom", GPU, amz("B0TROCA001"))
        await chega(mundo, "promotom", GPU, amz("B0TROCA002"), mid=p, idade_s=30,
                    editada=True)
        await nerd(mundo, GPU_NERD, amz("B0TROCA002"))
        s["x"] = (len(posts(mundo)), retida("promotom", p))
    cenario(corpo)
    r.check(s["x"] == (1, "liberada"), "21.aval_do_produto_novo_solta", str(s["x"]))


def test_22_floodwait_na_varredura(r):
    """FloodWait acima do limite do Telethon: a varredura não pergunta de
    novo antes do prazo pedido (a sessão é do worker inteiro)."""
    s = {}

    class Flood(Exception):
        seconds = 120

    async def corpo(mundo):
        peer, ent = PEER["nerdofertas"], ENT["nerdofertas"]
        chamadas = []

        async def lotado(*a, **k):
            chamadas.append(1)
            raise Flood()
        mundo.get_messages = lotado
        await curadoria._varrer_canal(peer, ent)
        await curadoria._varrer_canal(peer, ent)
        s["chamadas"] = len(chamadas)
        s["pausa"] = curadoria._pausa_ate.get(peer, 0) - time.monotonic()
    cenario(corpo)
    r.check(s["chamadas"] == 1 and 100 < s["pausa"] <= 120, "22.respeita_o_prazo", str(s))


def test_23_custo_do_classificador(r):
    """O portão roda em todo post de produto: a categoria tem de ser barata."""
    from pipeline.curadoria_categoria import categoria
    titulos = [GPU, GPU_FADA, GPU_NERD, "WD-40 Fórmula Original 300ml\n\nR$ 29",
               "Fritadeira Air Fryer Mondial 4L\n\nR$ 199",
               "Smart TV LG 55 4K Processador α7\n\nR$ 2.999"] * 500
    t0 = time.perf_counter()
    for t in titulos:
        categoria(t)
    media_ms = (time.perf_counter() - t0) / len(titulos) * 1000
    r.check(media_ms < 2.0, "23.menos_de_2ms_por_post", f"{media_ms:.3f} ms")


def test_24_lista_de_varios_produtos_passa(r):
    """Lista com vários produtos (cada um com preço) passa sem aval, seja
    qual for a categoria. Um produto só com dois links NÃO é lista."""
    s = {}
    lista = ("🔥 Placas Mães B550M AM4 em Oferta\n\n🎟 Resgate cupom do anúncio\n\n"
             "🔹Gigabyte Aorus Elite - R$ 556\nhttps://meli.la/1BRngkh\n\n"
             "🔹 Asus Tuf Gaming Plus - R$ 588\nhttps://meli.la/1KN1RGJ")      # Samuel 124289
    dois_anuncios = ("Water Cooler Kalkan Aura Com Tela Lcd 360mm Argb Intel Amd\n\nR$ 299\n"
                     "-CUPOM: MELIPROMO3111\n\n1 https://meli.la/1GFJVtN\n"
                     "2 https://meli.la/1YWGhpw")                                  # Promotom 112558
    shopee = ("Amd Ryzen 5 5500x3D Processador, 4.0GHz Max Turbo, Am4\n\nR$ 1250\n"
              "-Resgate todos os cupons desta página:\nhttps://s.shopee.com.br/A\n\n"
              "-Link produto:\nhttps://s.shopee.com.br/B")                        # Fumotom 44636

    async def corpo(mundo):
        await chega(mundo, "SamuelF3lipePromo", lista,
                    [("mercadolivre", "MLB1001", "produto"), ("mercadolivre", "MLB1002", "produto")])
        s["lista"] = len(posts(mundo))
        k = await chega(mundo, "promotom", dois_anuncios,
                        [("mercadolivre", "MLB2001", "produto"),
                         ("mercadolivre", "MLB2002", "produto")])
        h = await chega(mundo, "fumotom", shopee, [("shopee", "77.88", "produto")])
        s["presos"] = (len(posts(mundo)), retida("promotom", k), retida("fumotom", h))
    cenario(corpo)
    r.check(s["lista"] == 1, "24.lista_passa", str(s["lista"]))
    r.check(s["presos"] == (1, "retida", "retida"), "24.um_produto_com_dois_links_trava",
            str(s["presos"]))


def test_25_frase_de_destaque_no_titulo_passa(r):
    s = {}

    async def corpo(mundo):
        await chega(mundo, "promotom", "Ainda ativo! Monitor Gamer Philips Evnia 23.8\" 144Hz "
                    "0, 5Ms 24M2n3200nf\n\nR$ 449", amz("B0DEST0001"))          # Promotom 112445
        await chega(mundo, "SamuelF3lipePromo", "-Ainda ativo, bom preço em 12x sem juros\n"
                    "🔥 Monitor Gamer AOC AGON G42 27\" 200Hz 0,3ms\n\n💵 R$ 599",
                    amz("B0DEST0002"))                                           # Samuel 124721
        await chega(mundo, "fumotom", "Preção! Placa de Vídeo RTX 5060 8GB\n\nR$ 1899",
                    amz("B0DEST0003"))
        s["passaram"] = len(posts(mundo))
        a = await chega(mundo, "fumotom", "Placa de Vídeo RTX 5060 8GB\n\nR$ 1899\n-Ainda ativo",
                        amz("B0DEST0004"))                                       # só no corpo
        b = await chega(mundo, "promotom", "Esgotou! Placa de Vídeo RTX 5070\n\nR$ 3299",
                        amz("B0DEST0005"))
        s["presos"] = (len(posts(mundo)), retida("fumotom", a), retida("promotom", b))
    cenario(corpo)
    r.check(s["passaram"] == 3, "25.frase_no_titulo_ou_cabecalho_passa", str(s["passaram"]))
    r.check(s["presos"] == (3, "retida", "retida"), "25.frase_fora_do_titulo_e_esgotou_travam",
            str(s["presos"]))


def test_26_voltando_passa(r):
    s = {}

    async def corpo(mundo):
        for i, t in enumerate(("Voltando! Placa de Vídeo Gigabyte RTX 5060 Ti 8GB",
                               "Voltou! Redragon Water Cooler GamerStorm Le240 Rd 240mm Rgb",
                               "Está de volta: SSD Kingston NV3 1TB NVMe",
                               "Estoque voltou! Fonte Mach1 Steady 750W 80 Plus Bronze")):
            await chega(mundo, "promotom", t + "\n\nR$ 999", amz(f"B0VOLTA00{i}"))
        s["n"] = len(posts(mundo))
    cenario(corpo)
    r.check(s["n"] == 4, "26.retorno_passa", str(s["n"]))


def test_27_isencao_nao_vira_aval(r):
    """Quem passa por isenção NÃO é curador: a segurada de outra fonte, do
    mesmo produto, continua segurada (o canal tem um post só)."""
    s = {}

    async def corpo(mundo):
        a = "B0ISENC001"
        f = await chega(mundo, "fumotom", GPU, amz(a))
        await chega(mundo, "promotom", "Ainda ativo! " + GPU, amz(a))
        s["x"] = (len(posts(mundo)), retida("fumotom", f), list(mundo.buscas))
        with _db() as db:
            s["avais"] = db.execute("SELECT COUNT(*) FROM curadoria_aval").fetchone()[0]
    cenario(corpo)
    r.check(s["x"] == (1, "retida", []) and s["avais"] == 0, "27.nao_e_aval",
            str((s["x"], s["avais"])))


def test_20_catalogo(r):
    r.check(catalogo.TIPOS.get("curadoria.retida") == "DESCARTADA"
            and "curadoria.aval" in catalogo.TIPOS
            and catalogo.TIPOS["curadoria.aval"] is None, "20.tipos")
    r.check("CURADORIA" in catalogo.ENUMS["via"]
            and {"curadoria.portao.retida", "curadoria.aval.registrado"} <= catalogo.LOCAIS,
            "20.via_e_locais")
    tipos = {k: v for k, v in catalogo.TIPOS.items() if not k.startswith("curadoria.")}
    enums = {k: (v - {"CURADORIA"} if k == "via" else v) for k, v in catalogo.ENUMS.items()}
    locais = {x for x in catalogo.LOCAIS if not x.startswith("curadoria.")}
    r.check(catalogo._hash(tipos, enums, locais) == "cc6d521b1ee65714",
            "20.sem_o_delta_e_o_catalogo_anterior")
    r.check(catalogo.VERSAO == 1, "20.versao")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CURADORIA · hardware só com aval · árvore real"))
