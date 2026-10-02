"""
SUCESSÃO DA CHEFE e ESQUECIMENTO — depois da exclusão na fonte.

O que o Léo pediu (02/10):
  · a fonte publica, o meu converte; ela apaga → o meu apaga;
  · outra fonte que segura o mesmo post "já vira o chefe" (o post
    espelha a mensagem dela e as edições dela); se ELA apagar, o meu
    apaga também;
  · apagado o post, o bot ESQUECE a identidade: "tem hora que posta com
    link errado" — se a fonte postar de novo, precisa reconhecer.

Caminho REAL: enriquecer → montar → publicacao.enviar → decidir →
aplicadores; exclusão por origem_apagada; sucessão por sucessao (busca
da mensagem na fonte e reentrada como EDIÇÃO da líder → SINCRONIZAR);
anti-flood de reativação real (deduplicacao). Banco SQLite REAL; só a
rede do Telegram é falsa.

    python tests/test_sucessao_esquecimento.py
"""
from __future__ import annotations

import ast
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

from _harness_e5 import preparar, rodar, RAIZ  # noqa: E402

preparar()
os.environ.setdefault("ML_TAG", "leoofertas8270")

import globals as g                                            # noqa: E402
import client as modulo_client                                 # noqa: E402
import plataformas                                             # noqa: E402
from database import _init_db, db_get_post                     # noqa: E402
from database_conexao import _db                               # noqa: E402
from database_posts import db_transferir_lideranca             # noqa: E402
from pipeline import (convergencia, esquecimento, exclusao,    # noqa: E402
                      origem, origem_apagada, publicacao, sucessao)
from pipeline.deduplicacao import deve_enviar_async            # noqa: E402
from pipeline.enriquecimento import enriquecer, enriquecer_edicao  # noqa: E402
from pipeline.montagem import montar                            # noqa: E402
from pipeline.normalizacao import MensagemNormalizada           # noqa: E402

plataformas.inicializar()
_init_db()
convergencia._ESPERA_BASE_S = 0.0

IDS = {"fumotom": 3775401737, "promotom": 1825680721, "fadadoscupons": 2050488946}


def _canal(cid, user):
    return types.Channel(id=cid, title=user, photo=types.ChatPhotoEmpty(),
                         date=dt.datetime.now(dt.timezone.utc), access_hash=77,
                         username=user, broadcast=True)


ENTS = {u: _canal(i, u) for u, i in IDS.items()}
FUMOTOM, PROMOTOM, FADA = (str(utils.get_peer_id(ENTS[u]))
                           for u in ("fumotom", "promotom", "fadadoscupons"))
_TC = TelegramClient(StringSession(), 1, "0" * 32)


# ─────────────────────────────────────────────────────────────────
# Telegram falso: destino (envio/edição/remoção) e FONTES (busca)
# ─────────────────────────────────────────────────────────────────
class _Msg:
    def __init__(self, i):
        self.id, self.media, self.photo = i, None, None


_ID = {"n": 90000}


class Destino:
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

    async def download_media(self, media, file=None):
        return None


class Fontes:
    """A busca de mensagens nas fontes (a única rede da sucessão)."""

    def __init__(self):
        self.no_ar = {}           # (chat, msg_id) -> Message real do Telethon
        self.buscas = []
        self.erro = None
        self.antes_de_devolver = None

    def publicar(self, n):
        cid = int(n.chat)
        m = types.Message(id=n.msg_id, peer_id=types.PeerChannel(
            utils.resolve_id(cid)[0]), date=dt.datetime.now(dt.timezone.utc),
            message=n.texto_limpo, post=True)
        ent = next(e for e in ENTS.values() if utils.get_peer_id(e) == cid)
        m._finish_init(_TC, {cid: ent}, None)
        self.no_ar[(n.chat, n.msg_id)] = m

    def apagar(self, n):
        self.no_ar.pop((n.chat, n.msg_id), None)

    async def get_messages(self, ent, ids):
        chat = str(utils.get_peer_id(ent))
        self.buscas.append((chat, list(ids)))
        await asyncio.sleep(0)            # rede: a busca sempre cede o loop
        if self.erro:
            raise self.erro
        if self.antes_de_devolver:
            self.antes_de_devolver(chat, ids)
        return [self.no_ar.get((chat, i)) for i in ids]


class Entrada:
    """O ponto de entrada (papel de `processar`): a mensagem buscada entra
    como EDIÇÃO pelo caminho real da publicação."""

    def __init__(self):
        self.recebidos = []
        self.normas = {}

    def conhece(self, n):
        self.normas[(n.chat, n.msg_id)] = n

    async def __call__(self, evento, is_edit=False):
        chave = (str(evento.chat_id), evento.message.id)
        self.recebidos.append((chave, is_edit))
        n = self.normas.get(chave)
        if n is not None:
            await editar(n) if is_edit else await publicar(n)


def _isolar():
    g._init_globals()
    g._encerrando = False
    for pool in (exclusao._IDENTITY_LOCKS, exclusao._IDENTITY_LOCKS_TS,
                 exclusao._POST_LOCKS, exclusao._POST_LOCKS_TS,
                 origem._LOCKS, origem._LOCKS_TS):
        pool.clear()
    origem._LOCKS_LCK = asyncio.Lock()
    convergencia._EM_CURSO.clear()
    origem_apagada._APAGADAS.clear()
    origem_apagada._TAREFAS.clear()
    sucessao._EM_CURSO.clear()
    sucessao._DE_NOVO.clear()
    esquecimento._DA_ORIGEM.clear()
    esquecimento._DO_POST.clear()


async def drenar():
    for _ in range(200):
        ts = [t for t in (list(origem_apagada._TAREFAS)
                          + list(convergencia._EM_CURSO.values())
                          + list(sucessao._EM_CURSO.values())) if not t.done()]
        if not ts:
            return
        await asyncio.gather(*ts, return_exceptions=True)


def cenario(corpo):
    async def _run():
        _isolar()
        d, f, e = Destino(), Fontes(), Entrada()
        modulo_client.client = d
        sucessao.instalar(f, list(ENTS.values()), e, origem_apagada.apagadas)
        try:
            await corpo(d, f, e)
            await drenar()
        finally:
            sucessao._estado["cliente"] = None
        return d, f, e
    return asyncio.run(_run())


_SEQ = {"n": 800000}


def msg(chat, texto, pid=None, cupons=(), msg_id=None):
    if msg_id is None:
        _SEQ["n"] += 1
        msg_id = _SEQ["n"]
    ids = [pid] if pid else []
    return MensagemNormalizada(
        msg_id=msg_id, chat=str(chat), texto_limpo=texto, texto_analise=texto,
        mapa={"http://o": "http://a"}, preservar=[], plat="shopee",
        sku=pid or "", tem_midia=False, media_obj=None, ids_globais=ids,
        idents=[("shopee", pid, "produto")] if pid else [], cupons=list(cupons))


async def publicar(n):
    await publicacao.enviar(await montar(n), n, enr=enriquecer(n), is_edit=False)


async def editar(n):
    await publicacao.enviar(await montar(n), n, enr=enriquecer_edicao(n), is_edit=True)


async def na_fonte(f, e, n):
    """A fonte posta `n`: fica no ar na fonte e entra pelo caminho real."""
    f.publicar(n)
    e.conhece(n)
    await publicar(n)


async def apaga_na_fonte(f, n):
    f.apagar(n)
    r = await origem_apagada.apagadas(n.chat, [n.msg_id])
    await drenar()
    return r


def lider(post):
    e = db_get_post(post)
    return e["lider"], e["lider_msg"]


def ancoras_de(post):
    with _db() as db:
        return sorted(r[0] for r in db.execute(
            "SELECT identity FROM oferta_index WHERE msg_id_dest=?", (post,)))


ERRADO = ("🔥 Fone Bluetooth XY R$ 79\n\nhttps://s.shopee.com.br/LINKERRADO")
CERTO = ("🔥 Fone Bluetooth XY R$ 79\n\nhttps://s.shopee.com.br/LINKCERTO")


# ══════════════════════════════════════════════════════════════════
# PARTE 1 — Ligação
# ══════════════════════════════════════════════════════════════════
def test_01_main_liga_sucessao_com_o_ponto_de_entrada(r):
    arv = ast.parse(open(os.path.join(RAIZ, "main.py"), encoding="utf-8").read())
    prep = next(n for n in ast.walk(arv)
                if isinstance(n, ast.AsyncFunctionDef) and n.name == "_preparar_processo")
    corpo = [ast.unparse(n) for n in prep.body]
    i = corpo.index("origem_apagada.instalar(client, fontes)")
    r.check(corpo[i + 1] == "sucessao.instalar(client, fontes, processar, "
                           "origem_apagada.apagadas)", "01.logo_depois", corpo[i + 1])
    r.check(sum(1 for n in ast.walk(arv) if isinstance(n, ast.Call)
                and ast.unparse(n.func) == "sucessao.instalar") == 1, "01.uma_vez")


# ══════════════════════════════════════════════════════════════════
# PARTE 2 — Sucessão da chefe
# ══════════════════════════════════════════════════════════════════
def test_02_link_errado_repostado_assume(r):
    """A fonte posta com LINK ERRADO, reposta o certo (casa o post e é
    ignorada como duplicata) e apaga a errada: a repostagem vira a chefe
    e o post passa a mostrar o link CERTO — sem post novo, sem remover."""
    s = {}

    async def corpo(d, f, e):
        a1 = msg(PROMOTOM, ERRADO, "21.1")
        a2 = msg(PROMOTOM, CERTO, "21.1")
        await na_fonte(f, e, a1)
        await na_fonte(f, e, a2)
        p = d.criados[0]
        s.update(p=p, a2=a2, criados=list(d.criados), edits_antes=list(d.edits))
        s["r"] = await apaga_na_fonte(f, a1)
    d, f, e = cenario(corpo)
    p = s["p"]
    r.check(s["criados"] == [p] and s["edits_antes"] == [], "02.repostagem_casou_e_foi_ignorada")
    r.check(s["r"] == [] and d.deletes == [], "02.post_fica")
    r.check(f.buscas == [(PROMOTOM, [s["a2"].msg_id])], "02.buscou_a_sucessora", str(f.buscas))
    r.check(lider(p) == (PROMOTOM, s["a2"].msg_id), "02.sucessora_lider", str(lider(p)))
    r.check(len(d.edits) == 1 and d.edits[0][0] == p and "LINKCERTO" in d.edits[0][1]
            and "LINKERRADO" not in d.edits[0][1], "02.post_com_link_certo",
            str(d.edits)[:160])
    r.check(e.recebidos == [((PROMOTOM, s["a2"].msg_id), True)], "02.entrou_como_edicao")


def test_03_outra_fonte_vira_chefe_edita_e_apaga(r):
    """fumotom publica; Promotom casa o mesmo post. fumotom apaga →
    Promotom vira a chefe (post espelha a mensagem dele); Promotom EDITA
    → o post espelha a edição (sincronização da líder); Promotom apaga →
    o post sai."""
    s = {}
    T_A = "🔥 Air Fryer Mondial 4L R$ 199\n\nhttps://link.amazon/AFRYER"
    T_B = "🔥 Air Fryer Mondial 4L R$ 199 (Promotom)\n\nhttps://link.amazon/AFRYER"

    async def corpo(d, f, e):
        a = msg(FUMOTOM, T_A, "31.1")
        b = msg(PROMOTOM, T_B, "31.1")
        await na_fonte(f, e, a)
        await na_fonte(f, e, b)
        p = d.criados[0]
        s["p"] = p
        await apaga_na_fonte(f, a)
        s["lider"] = lider(p)
        s["edits_1"] = list(d.edits)
        b2 = msg(PROMOTOM, T_B.replace("R$ 199", "R$ 179"), "31.1", msg_id=b.msg_id)
        await editar(b2)
        s["edits_2"] = list(d.edits)
        s["r"] = await apaga_na_fonte(f, b)
    d, f, e = cenario(corpo)
    p = s["p"]
    r.check(s["lider"][0] == PROMOTOM, "03.promotom_chefe", str(s["lider"]))
    r.check(len(s["edits_1"]) == 1 and "(Promotom)" in s["edits_1"][0][1],
            "03.espelha_a_chefe_nova", str(s["edits_1"])[:120])
    r.check(len(s["edits_2"]) == 2 and "R$ 179" in s["edits_2"][-1][1],
            "03.edicao_da_chefe_espelha", str(s["edits_2"])[-1:])
    r.check(s["r"] == [p] and d.deletes == [p], "03.chefe_apaga_post_sai", str(d.deletes))
    r.check(len(d.criados) == 1, "03.nenhum_post_novo")


def test_04_apagar_quem_nao_e_chefe_nao_mexe(r):
    """A apagada não era a chefe: nenhuma busca, nenhuma edição."""
    s = {}

    async def corpo(d, f, e):
        a = msg(FUMOTOM, "Produto Q R$ 10", "41.1")
        b = msg(PROMOTOM, "Produto Q R$ 10", "41.1")
        await na_fonte(f, e, a)
        await na_fonte(f, e, b)
        s["p"] = d.criados[0]
        await apaga_na_fonte(f, b)
    d, f, e = cenario(corpo)
    r.check(f.buscas == [] and d.edits == [] and d.deletes == [], "04.nada",
            f"{f.buscas} {d.edits}")
    r.check(lider(s["p"])[0] == FUMOTOM, "04.chefe_mantida")


def test_05_sucessora_sumida_na_fonte_e_tratada_como_apagada(r):
    """A mais recente (Fada) também já foi apagada na fonte, mas o
    Telegram não avisou: a busca não a acha → desligada como apagada → a
    próxima (Promotom) assume."""
    s = {}

    async def corpo(d, f, e):
        a = msg(FUMOTOM, "Produto W R$ 10", "51.1")
        b = msg(PROMOTOM, "Produto W R$ 10", "51.1")
        c = msg(FADA, "Produto W R$ 10", "51.1")
        for n in (a, b, c):
            await na_fonte(f, e, n)
        s.update(p=d.criados[0], b=b, c=c)
        f.apagar(c)                                  # sem aviso
        await apaga_na_fonte(f, a)
    d, f, e = cenario(corpo)
    r.check(f.buscas == [(FADA, [s["c"].msg_id]), (PROMOTOM, [s["b"].msg_id])],
            "05.ordem_mais_recente_primeiro", str(f.buscas))
    r.check(origem.consultar(FADA, s["c"].msg_id) is None, "05.sumida_desligada")
    r.check(lider(s["p"]) == (PROMOTOM, s["b"].msg_id) and d.deletes == [],
            "05.proxima_assume", str(lider(s["p"])))


def test_06_todas_sumidas_post_sai(r):
    """Nenhuma sucessora existe mais na fonte: o post sai (auto-cura da
    exclusão não avisada)."""
    s = {}

    async def corpo(d, f, e):
        a = msg(FUMOTOM, "Produto Z R$ 10", "61.1")
        b = msg(PROMOTOM, "Produto Z R$ 10", "61.1")
        await na_fonte(f, e, a)
        await na_fonte(f, e, b)
        s["p"] = d.criados[0]
        f.apagar(b)                                  # sem aviso
        await apaga_na_fonte(f, a)
    d, f, e = cenario(corpo)
    r.check(d.deletes == [s["p"]] and e.recebidos == [], "06.post_sai", str(d.deletes))


def test_07_ciclo_fechado_fica_congelado(r):
    """Post com o ciclo de vida fechado: sem sucessão (estado final,
    congelado) — sem busca, sem edição; sai quando a última fonte apagar."""
    s = {}

    async def corpo(d, f, e):
        a = msg(FUMOTOM, "Produto K R$ 10", "71.1")
        b = msg(PROMOTOM, "Produto K R$ 10", "71.1")
        await na_fonte(f, e, a)
        await na_fonte(f, e, b)
        p = d.criados[0]
        s["p"] = p
        with _db() as db:
            db.execute("UPDATE post_estado SET janela_fim=? WHERE msg_id_dest=?",
                       (time.time() - 60, p))
        await apaga_na_fonte(f, a)
        s["meio"] = (list(f.buscas), list(d.edits), list(d.deletes))
        await apaga_na_fonte(f, b)
    d, f, e = cenario(corpo)
    r.check(s["meio"] == ([], [], []), "07.congelado", str(s["meio"]))
    r.check(d.deletes == [s["p"]], "07.ultima_apaga_sai")


def test_08_busca_falha_nada_muda(r):
    """Erro de rede na busca: nada é desligado nem transferido."""
    s = {}

    async def corpo(d, f, e):
        a = msg(FUMOTOM, "Produto R R$ 10", "81.1")
        b = msg(PROMOTOM, "Produto R R$ 10", "81.1")
        await na_fonte(f, e, a)
        await na_fonte(f, e, b)
        s.update(p=d.criados[0], b=b)
        f.erro = ConnectionError("rede")
        await apaga_na_fonte(f, a)
    d, f, e = cenario(corpo)
    r.check(origem.consultar(PROMOTOM, s["b"].msg_id) == s["p"], "08.b_segue_ligada")
    r.check(lider(s["p"])[0] == FUMOTOM and d.edits == [] and d.deletes == [],
            "08.nada_muda", str(lider(s["p"])))


def test_09_sucessora_apagada_durante_a_busca(r):
    """A sucessora é apagada enquanto a busca está no ar: a transferência
    (sob o lock do post) recusa — nada entra pela pipeline; a exclusão
    dela segue o caminho normal e o post sai."""
    s = {}

    async def corpo(d, f, e):
        a = msg(FUMOTOM, "Produto J R$ 10", "91.1")
        b = msg(PROMOTOM, "Produto J R$ 10", "91.1")
        await na_fonte(f, e, a)
        await na_fonte(f, e, b)
        s["p"] = d.criados[0]

        def apaga_b(chat, ids):
            with _db() as db:                      # a exclusão de B já correu
                db.execute("DELETE FROM origem_post WHERE chat=? AND msg_id=?",
                           (b.chat, b.msg_id))
        f.antes_de_devolver = apaga_b
        await apaga_na_fonte(f, a)
        s["recebidos"] = list(e.recebidos)
    d, f, e = cenario(corpo)
    r.check(s["recebidos"] == [] and lider(s["p"])[0] == FUMOTOM,
            "09.transferencia_recusada", str(s["recebidos"]))


def test_10_uma_rodada_por_post(r):
    """Dois pedidos seguidos para o mesmo post: a sucessora entra UMA vez."""
    s = {}

    async def corpo(d, f, e):
        a = msg(FUMOTOM, "Produto H R$ 10", "101.1")
        b = msg(PROMOTOM, "Produto H R$ 10", "101.1")
        await na_fonte(f, e, a)
        await na_fonte(f, e, b)
        p = d.criados[0]
        f.apagar(a)
        with _db() as db:
            db.execute("DELETE FROM origem_post WHERE chat=? AND msg_id=?",
                       (a.chat, a.msg_id))
        sucessao.agendar(p)
        sucessao.agendar(p)
        sucessao.agendar(p)
        await drenar()
        s["recebidos"] = list(e.recebidos)
        s["buscas"] = list(f.buscas)
    cenario(corpo)
    r.check(len(s["recebidos"]) == 1, "10.uma_vez", str(s["recebidos"]))
    r.check(len(s["buscas"]) == 1, "10.uma_busca", str(s["buscas"]))


def test_11_transferencia_no_banco(r):
    """db_transferir_lideranca só vale com tudo valendo."""
    async def corpo(d, f, e):
        a = msg(FUMOTOM, "Produto G R$ 10", "111.1")
        b = msg(PROMOTOM, "Produto G R$ 10", "111.1")
        await na_fonte(f, e, a)
        await na_fonte(f, e, b)
        p = d.criados[0]
        agora = time.time()
        r.check(not db_transferir_lideranca(p, b.chat, b.msg_id, agora),
                "11.lider_ainda_ligada")
        with _db() as db:
            db.execute("DELETE FROM origem_post WHERE chat=? AND msg_id=?",
                       (a.chat, a.msg_id))
        r.check(not db_transferir_lideranca(p, FADA, 1, agora), "11.nao_ligada")
        r.check(not db_transferir_lideranca(p, b.chat, b.msg_id, agora + 7200),
                "11.ciclo_fechado")
        r.check(db_transferir_lideranca(p, b.chat, b.msg_id, agora), "11.ok")
        r.check(lider(p) == (b.chat, b.msg_id), "11.gravou")
    cenario(corpo)


# ══════════════════════════════════════════════════════════════════
# PARTE 3 — Esquecimento: a repostagem é reconhecida
# ══════════════════════════════════════════════════════════════════
def test_12_post_apagado_esquece_as_ancoras(r):
    s = {}

    async def corpo(d, f, e):
        a = msg(FUMOTOM, "Produto E R$ 10", "121.1")
        await na_fonte(f, e, a)
        p = d.criados[0]
        s["antes"] = ancoras_de(p)
        await apaga_na_fonte(f, a)
        s["depois"] = ancoras_de(p)
        s["p"] = p
    cenario(corpo)
    r.check(s["antes"] == ["shopee|121.1"] and s["depois"] == [], "12.esqueceu",
            f"{s['antes']} {s['depois']}")


def test_13_apaga_e_reposta_cupom_reconhecido(r):
    """Cupom: a fonte apaga e reposta o MESMO cupom (link corrigido) — a
    repostagem é publicada como post novo; a memória de códigos não a
    segura."""
    async def corpo(d, f, e):
        t1 = "Cupom Shopee R$ 15 OFF em R$ 89: REPOSTA15\n\nhttps://s.shopee.com.br/ERRADO"
        t2 = "Cupom Shopee R$ 15 OFF em R$ 89: REPOSTA15\n\nhttps://s.shopee.com.br/CERTO"
        a1 = msg(FADA, t1, cupons=["REPOSTA15"])
        await na_fonte(f, e, a1)
        await apaga_na_fonte(f, a1)
        await na_fonte(f, e, msg(FADA, t2, cupons=["REPOSTA15"]))
    d, f, e = cenario(corpo)
    r.check(len(d.criados) == 2 and d.deletes == [d.criados[0]], "13.repostagem_publicada",
            f"{d.criados} {d.deletes}")


def test_14_voltou_apagado_e_repostado_passa_no_antiflood(r):
    """"VOLTOU" (reativação): o anti-flood segura a identidade por 10 min.
    A fonte apaga o "VOLTOU" (post sai) e reposta: a reserva foi solta, a
    repostagem PASSA. Controle: sem a exclusão, o segundo "VOLTOU" é
    barrado como flood."""
    s = {}
    T = "🔥 VOLTOU! Cupom Shopee R$ 20 OFF em R$ 99: VOLTA20{}\n\nhttps://s.shopee.com.br/x"

    async def corpo(d, f, e):
        a = msg(FADA, T.format("A"), cupons=["VOLTA20A"])
        s["1a"] = await deve_enviar_async(enriquecer(a))
        f.publicar(a)
        await publicar(a)
        s["controle"] = await deve_enviar_async(enriquecer(msg(FADA, T.format("A"),
                                                               cupons=["VOLTA20A"])))
        b = msg(FADA, T.format("B"), cupons=["VOLTA20B"])
        s["1b"] = await deve_enviar_async(enriquecer(b))
        f.publicar(b)
        await publicar(b)
        await apaga_na_fonte(f, b)
        b2 = msg(FADA, T.format("B"), cupons=["VOLTA20B"])
        s["repost"] = await deve_enviar_async(enriquecer(b2))
        await publicar(b2)
    d, f, e = cenario(corpo)
    r.check(s["1a"] and s["1b"], "14.primeiros_passam")
    r.check(s["controle"] is False, "14.controle_flood_barrado")
    r.check(s["repost"] is True and len(d.criados) == 3, "14.repostagem_passa",
            f"{s['repost']} {d.criados}")


def test_15_reserva_fica_enquanto_o_post_vive(r):
    """O "VOLTOU" que tomou a reserva é apagado, mas outra fonte segura o
    post: a reserva NÃO é solta (outro "VOLTOU" renasceria um post que
    está no ar). Só quando o post sai."""
    s = {}
    T = "🔥 VOLTOU! Cupom Shopee R$ 30 OFF em R$ 199: RESERVA30\n\nhttps://s.shopee.com.br/y"
    T_B = "Cupom Shopee R$ 30 OFF em R$ 199: RESERVA30\n\nhttps://s.shopee.com.br/z"

    async def corpo(d, f, e):
        a = msg(FADA, T, cupons=["RESERVA30"])
        await deve_enviar_async(enriquecer(a))
        await na_fonte(f, e, a)
        b = msg(PROMOTOM, T_B, cupons=["RESERVA30"])
        await na_fonte(f, e, b)
        s["p"] = d.criados[0]
        await apaga_na_fonte(f, a)
        s["vivo"] = await deve_enviar_async(enriquecer(msg(FADA, T, cupons=["RESERVA30"])))
        await apaga_na_fonte(f, b)
        s["morto"] = await deve_enviar_async(enriquecer(msg(FADA, T, cupons=["RESERVA30"])))
    d, f, e = cenario(corpo)
    r.check(s["vivo"] is False, "15.post_vivo_reserva_fica")
    r.check(s["morto"] is True and d.deletes == [s["p"]], "15.post_saiu_reserva_solta")


def test_16_memoria_limitada(r):
    teto = esquecimento._TETO
    try:
        esquecimento._TETO = 30
        esquecimento._DA_ORIGEM.clear()
        for i in range(100):
            esquecimento.reserva(FADA, i, f"fp{i}")
        r.check(len(esquecimento._DA_ORIGEM) == 30, "16.teto",
                str(len(esquecimento._DA_ORIGEM)))
        r.check(f"{FADA}|99" in esquecimento._DA_ORIGEM, "16.mais_nova_fica")
    finally:
        esquecimento._TETO = teto
        esquecimento._DA_ORIGEM.clear()


if __name__ == "__main__":
    sys.exit(rodar(globals(), "SUCESSÃO DA CHEFE e ESQUECIMENTO · árvore real"))
