"""
VIDA DA OFERTA — 25 min → 1 hora (decisão do Léo, 02/10).

A janela é UMA só (pipeline.vida_oferta) e governa, ao mesmo tempo:
  · a admissão da EDIÇÃO (orchestrator.processar: EDIT_ANTIGO);
  · a vida do post, estampada no nascimento (janela_fim): família,
    sincronização do líder, evolução, posse de âncora;
  · a memória de códigos de cupom (cupom_idx).
Este arquivo prova as três na fronteira nova, pelo caminho REAL, e o
caso de produção que motivou a mudança:

  @promotom 110392 (Cupom Mercado Livre) — enviada 12:00:00 UTC; edições
  às 13:00:54 e 13:32:57 descartadas como EDIT_ANTIGO. Com 1 h exata, a
  de 13:00:54 (idade 1h00m54s) AINDA fica fora — por 54 s.

    python tests/test_vida_uma_hora.py
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import telethon  # noqa: E402  — REAL, antes do harness
from telethon import TelegramClient, utils  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import types  # noqa: E402

assert hasattr(telethon.TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from _harness_e5 import preparar, rodar  # noqa: E402

preparar()
os.environ.setdefault("ML_TAG", "leoofertas8270")

import globals as g                                            # noqa: E402
import client as modulo_client                                 # noqa: E402
import plataformas                                             # noqa: E402
from database import _init_db, db_get_post                     # noqa: E402
from database_conexao import _db                               # noqa: E402
from pipeline import exclusao, origem, orchestrator, publicacao  # noqa: E402
from pipeline import convergencia, memoria_cupom               # noqa: E402
from pipeline.completude import EventoRecuperado               # noqa: E402
from pipeline.enriquecimento import enriquecer, enriquecer_edicao  # noqa: E402
from pipeline.montagem import montar                            # noqa: E402
from pipeline.normalizacao import MensagemNormalizada           # noqa: E402
from pipeline.vida_oferta import VIDA_OFERTA_S, estampar, viva  # noqa: E402

plataformas.inicializar()
_init_db()

PROMOTOM_ID, FUMOTOM_ID = 1825680721, 3775401737


def _canal(cid, user):
    return types.Channel(id=cid, title=user, photo=types.ChatPhotoEmpty(),
                         date=dt.datetime.now(dt.timezone.utc), access_hash=77,
                         username=user, broadcast=True)


PROMOTOM_ENT, FUMOTOM_ENT = _canal(PROMOTOM_ID, "promotom"), _canal(FUMOTOM_ID, "fumotom")
PROMOTOM = str(utils.get_peer_id(PROMOTOM_ENT))
FUMOTOM = str(utils.get_peer_id(FUMOTOM_ENT))


class _Msg:
    def __init__(self, i):
        self.id, self.media, self.photo = i, None, None


_ID = {"n": 80000}


class Cliente:
    def __init__(self):
        self.criados, self.edits, self.deletes = [], [], []

    async def send_message(self, dest, texto, parse_mode=None, link_preview=None):
        _ID["n"] += 1
        self.criados.append(_ID["n"])
        return _Msg(_ID["n"])

    async def send_file(self, dest, img, caption=None, parse_mode=None,
                        force_document=False):
        return await self.send_message(dest, caption)

    async def edit_message(self, dest, msg_id, texto, parse_mode=None, file=None):
        self.edits.append((msg_id, texto))
        return _Msg(msg_id)

    async def delete_messages(self, dest, msg_id):
        self.deletes.append(msg_id)
        return True


def cenario(corpo):
    async def _run():
        g._init_globals()
        g._encerrando = False
        for pool in (exclusao._IDENTITY_LOCKS, exclusao._IDENTITY_LOCKS_TS,
                     exclusao._POST_LOCKS, exclusao._POST_LOCKS_TS,
                     origem._LOCKS, origem._LOCKS_TS):
            pool.clear()
        origem._LOCKS_LCK = asyncio.Lock()
        convergencia._EM_CURSO.clear()
        c = Cliente()
        modulo_client.client = c
        await corpo(c)
        return c
    return asyncio.run(_run())


_SEQ = {"n": 700000}


def produto(chat, texto, pid, msg_id=None, cupons=()):
    if msg_id is None:
        _SEQ["n"] += 1
        msg_id = _SEQ["n"]
    return MensagemNormalizada(
        msg_id=msg_id, chat=str(chat), texto_limpo=texto, texto_analise=texto,
        mapa={"http://o": "http://a"}, preservar=[], plat="shopee", sku=pid,
        tem_midia=False, media_obj=None, ids_globais=[pid],
        idents=[("shopee", pid, "produto")], cupons=list(cupons))


async def publicar(n):
    await publicacao.enviar(await montar(n), n, enr=enriquecer(n), is_edit=False)


async def editar(n):
    await publicacao.enviar(await montar(n), n, enr=enriquecer_edicao(n), is_edit=True)


def nasceu_ha(post, segundos):
    """Reescreve o nascimento do post como se tivesse sido `segundos`
    atrás — com a MESMA estampa da produção (vida_oferta.estampar)."""
    nasc = time.time() - segundos
    with _db() as db:
        db.execute("UPDATE post_estado SET janela_fim=?, ts=? WHERE msg_id_dest=?",
                   (estampar(nasc), nasc, post))


# ══════════════════════════════════════════════════════════════════
def test_01_constante_e_estampa_no_nascimento(r):
    s = {}

    async def corpo(c):
        t0 = time.time()
        await publicar(produto(FUMOTOM, "Produto V1 R$ 10", "11.1"))
        s["e"], s["t0"] = db_get_post(c.criados[0]), t0
    cenario(corpo)
    r.check(VIDA_OFERTA_S == 3600, "01.uma_hora", str(VIDA_OFERTA_S))
    dur = s["e"]["janela_fim"] - s["t0"]
    r.check(3599 <= dur <= 3601, "01.post_nasce_com_1h", f"{dur:.1f}")
    r.check(viva(estampar(0.0), 3599.999) and not viva(estampar(0.0), 3600.0),
            "01.fronteira_exclusiva")


def test_02_admissao_da_edicao(r):
    """EDIT_ANTIGO pela idade da MENSAGEM de origem: 30 min e 59m50s
    entram (com 25 min, a de 30 caía); 61 min cai. Nova continua com a
    trava própria de 120 s."""
    admitidos = []

    async def espiao(ev, is_edit):
        admitidos.append((ev.message.id, is_edit))

    def evento(mid, idade_s):
        tc = TelegramClient(StringSession(), 1, "0" * 32)
        m = types.Message(id=mid, peer_id=types.PeerChannel(PROMOTOM_ID),
                          date=dt.datetime.now(dt.timezone.utc)
                          - dt.timedelta(seconds=idade_s),
                          message="x", post=True)
        m._finish_init(tc, {int(PROMOTOM): PROMOTOM_ENT}, None)
        return EventoRecuperado(m)

    real = orchestrator._enfileirar
    orchestrator._enfileirar = espiao
    try:
        async def corpo():
            g._encerrando = False
            await orchestrator.processar(evento(1, 30 * 60), is_edit=True)
            await orchestrator.processar(evento(2, 3590), is_edit=True)
            await orchestrator.processar(evento(3, 61 * 60), is_edit=True)
            await orchestrator.processar(evento(4, 3654), is_edit=True)   # 110392 13:00:54
            await orchestrator.processar(evento(5, 5577), is_edit=True)   # 110392 13:32:57
            await orchestrator.processar(evento(6, 100), is_edit=False)
            await orchestrator.processar(evento(7, 130), is_edit=False)
        asyncio.run(corpo())
    finally:
        orchestrator._enfileirar = real
    r.check(admitidos == [(1, True), (2, True), (6, False)], "02.fronteiras",
            str(admitidos))


def test_03_familia_converge_ate_1h(r):
    """Outra fonte com a MESMA oferta 40 min depois do nascimento casa o
    post existente (com 25 min nascia post novo); 61 min depois, ciclo
    novo."""
    s = {}

    async def corpo(c):
        await publicar(produto(FUMOTOM, "Produto V3 R$ 30", "33.3"))
        p = c.criados[0]
        nasceu_ha(p, 40 * 60)
        await publicar(produto(PROMOTOM, "Produto V3 R$ 30", "33.3"))
        s["aos_40"] = list(c.criados)
        nasceu_ha(p, 61 * 60)
        await publicar(produto(PROMOTOM, "Produto V3 R$ 30", "33.3"))
        s["aos_61"] = list(c.criados)
        s["p"] = p
    cenario(corpo)
    r.check(s["aos_40"] == [s["p"]], "03.40min_mesmo_post", str(s["aos_40"]))
    r.check(len(s["aos_61"]) == 2, "03.61min_post_novo", str(s["aos_61"]))


def test_04_lider_edita_aos_40min_e_sincroniza(r):
    """O caso dos cupons: a fonte que publicou edita a lista 40 min depois
    acrescentando um cupom — o post espelha (SINCRONIZAR). Aos 61 min a
    edição não muda nada (ciclo encerrado)."""
    s = {}
    base = ("Cupom Shopee\n\nR$ 15 OFF em R$ 89: VIDA40A\n\n"
            "-Resgate aqui: \nhttps://s.shopee.com.br/70GSVaUJxC")

    async def corpo(c):
        n = produto(PROMOTOM, base, "44.4", cupons=["VIDA40A"])
        await publicar(n)
        p = c.criados[0]
        nasceu_ha(p, 40 * 60)
        texto2 = base + "\nR$ 25 OFF em R$ 199: VIDA40B"
        n2 = produto(PROMOTOM, texto2, "44.4", msg_id=n.msg_id,
                     cupons=["VIDA40A", "VIDA40B"])
        await editar(n2)
        s["edits_40"] = list(c.edits)
        # Aos 61 min a ADMISSÃO já corta esta edição (EDIT_ANTIGO, teste
        # 02); chamada direto aqui, prova só que o post encerrado não é
        # mais editado (o texto ficou congelado).
        nasceu_ha(p, 61 * 60)
        n3 = produto(PROMOTOM, texto2 + "\nR$ 5 OFF: VIDA40C", "44.4",
                     msg_id=n.msg_id, cupons=["VIDA40A", "VIDA40B", "VIDA40C"])
        await editar(n3)
        s["edits_61"] = list(c.edits)
        s["p"] = p
    cenario(corpo)
    r.check(len(s["edits_40"]) == 1 and s["edits_40"][0][0] == s["p"]
            and "VIDA40B" in s["edits_40"][0][1], "04.aos_40_espelha",
            str(s["edits_40"])[:200])
    r.check(s["edits_61"] == s["edits_40"], "04.aos_61_congelado")


def test_05_memoria_de_cupom_acompanha_o_ciclo(r):
    """O código visto há 40 min ainda gruda a identidade; há 61 min não."""
    n = produto(PROMOTOM, "Cupom X", "55.5", cupons=["MEMO55"])
    memoria_cupom.registrar_uso(n, "shopee", "shopee|cup|MEMO55-id")
    with _db() as db:
        db.execute("UPDATE cupom_idx SET ts=? WHERE codigo=?",
                   (time.time() - 40 * 60, "MEMO55"))
    r.check(memoria_cupom.buscar_identidade(n, "shopee", "fb") == "shopee|cup|MEMO55-id",
            "05.40min_gruda")
    with _db() as db:
        db.execute("UPDATE cupom_idx SET ts=? WHERE codigo=?",
                   (time.time() - 61 * 60, "MEMO55"))
    r.check(memoria_cupom.buscar_identidade(n, "shopee", "fb") == "fb",
            "05.61min_solta")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "VIDA DA OFERTA · 1 hora · árvore real"))
