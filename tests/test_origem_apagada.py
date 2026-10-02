"""
ORIGEM APAGADA — a fonte apaga a mensagem → o post sai do canal também.

Caminho REAL: enriquecer → montar → publicacao.enviar → família →
decidir → aplicador (vínculo de Origem) e, na exclusão, o despacho REAL
do Telethon 1.45 → origem_apagada → banco → convergencia._remover →
saida.apagar_post. Banco SQLite REAL em diretório temporário (harness);
só a rede do Telegram é falsa.

Os casos de produção (02/10, horário de Brasília):
  post 24539  nasceu do @fumotom 35483 e casou o @promotom 110388 — as
              duas fontes apagaram; o post ficou no ar.
  post 24548  nasceu do @fumotom 35488 (única origem) — apagada; o post
              ficou no ar.
(As mensagens de origem não existem mais; os textos aqui são os dos
nossos posts. A regra não depende da plataforma.)

REGRA (decisão do Léo): sai quando TODAS as origens ligadas ao post
foram apagadas; no mesmo instante; sem limite de idade além da
retenção do vínculo (30 dias); também do canal de cupons.

    python tests/test_origem_apagada.py
"""
from __future__ import annotations

import ast
import asyncio
import datetime as dt
import io
import logging
import os
import sys
import time
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import telethon  # noqa: E402  — REAL, antes do harness
from telethon import TelegramClient, errors, utils  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import types  # noqa: E402

assert hasattr(telethon.TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from _harness_e5 import preparar, rodar, RAIZ  # noqa: E402

preparar()   # telethon real preservado; só aiohttp (ausente) é simulado
os.environ.setdefault("ML_TAG", "leoofertas8270")

import globals as g                                            # noqa: E402
import client as modulo_client                                 # noqa: E402
import plataformas                                             # noqa: E402
from database import _init_db, db_get_post                     # noqa: E402
from database_conexao import _db                               # noqa: E402
from database_posts import db_desvincular_origem               # noqa: E402
from pipeline import (convergencia, espelho_cupons, exclusao,  # noqa: E402
                      origem, origem_apagada, publicacao)
from pipeline import orchestrator_pipeline                      # noqa: E402
from pipeline.completude import EventoRecuperado               # noqa: E402
from pipeline.decisao import Decisao, EVOLUIR                   # noqa: E402
from pipeline.enriquecimento import enriquecer, enriquecer_edicao  # noqa: E402
from pipeline.montagem import montar                            # noqa: E402
from pipeline.normalizacao import MensagemNormalizada           # noqa: E402
from pipeline.publicacao_aplicadores import _aplicar_evolucao  # noqa: E402

plataformas.inicializar()
_init_db()

convergencia._ESPERA_BASE_S = 0.0

FUMOTOM_ID, PROMOTOM_ID, FADA_ID, OUTRO_ID = (
    3775401737, 1825680721, 2050488946, 1111111111)


def _canal(cid, user):
    return types.Channel(id=cid, title=user, photo=types.ChatPhotoEmpty(),
                         date=dt.datetime.now(dt.timezone.utc), access_hash=77,
                         username=user, broadcast=True)


FUMOTOM_ENT = _canal(FUMOTOM_ID, "fumotom")
PROMOTOM_ENT = _canal(PROMOTOM_ID, "promotom")
OUTRO_ENT = _canal(OUTRO_ID, "outro")
FUMOTOM = str(utils.get_peer_id(FUMOTOM_ENT))     # "-1003775401737"
PROMOTOM = str(utils.get_peer_id(PROMOTOM_ENT))   # "-1001825680721"
FADA = str(utils.get_peer_id(_canal(FADA_ID, "fadadoscupons")))
OUTRO = str(utils.get_peer_id(OUTRO_ENT))

T_24539 = ("🔥 Pokémon TCG, Celebração de 30 anos – Blister Triplo com Adesivo "
           "Pokémon Exeggutor\n\n✅ R$ 165\n\nhttps://link.amazon/B0bIr4FRL")
T_24548 = ("🔥 Widicare Encrespando A Juba Creme De Pentear - Widi Care Widi "
           "Care Branco Médio\n\n✅ R$ 32\n\nhttps://link.amazon/B0aPAorqE")


# ─────────────────────────────────────────────────────────────────
# Telegram falso (rede) — conta envios, edições e remoções
# ─────────────────────────────────────────────────────────────────
class _Msg:
    def __init__(self, i, com_midia=False):
        self.id = i
        self.media = object() if com_midia else None
        self.photo = None


_ID_DESTINO = {"n": 70000}


def _proximo_id():
    _ID_DESTINO["n"] += 1
    return _ID_DESTINO["n"]


class Cliente:
    def __init__(self):
        self.criados, self.edits, self.deletes = [], [], []
        self.falhas_delete = 0            # quantas remoções falham
        self.flood_delete = 0             # quantas remoções levam FloodWait
        self.falha_edit_com_file = False
        self.tentativas_delete = 0
        self.portao_envio = None          # asyncio.Event: segura o envio
        self.no_delete = []               # locks travados DURANTE o delete

    async def _envio(self):
        if self.portao_envio is not None:
            await self.portao_envio.wait()

    async def send_message(self, dest, texto, parse_mode=None, link_preview=None):
        await self._envio()
        i = _proximo_id()
        self.criados.append(i)
        return _Msg(i)

    async def send_file(self, dest, img, caption=None, parse_mode=None,
                        force_document=False):
        await self._envio()
        i = _proximo_id()
        self.criados.append(i)
        return _Msg(i, com_midia=True)

    async def edit_message(self, dest, msg_id, texto, parse_mode=None, file=None):
        if file is not None and self.falha_edit_com_file:
            raise RuntimeError("edit com file recusado")
        self.edits.append((msg_id, texto))
        return _Msg(msg_id, com_midia=file is not None)

    async def delete_messages(self, dest, msg_id):
        self.tentativas_delete += 1
        self.no_delete.append(
            [k for k, lk in list(exclusao._POST_LOCKS.items()) if lk.locked()]
            + [k for k, lk in list(origem._LOCKS.items()) if lk.locked()])
        if self.flood_delete:
            self.flood_delete -= 1
            raise errors.FloodWaitError(request=None, capture=0)
        if self.falhas_delete:
            self.falhas_delete -= 1
            raise RuntimeError("delete recusado")
        self.deletes.append(msg_id)
        return True

    async def download_media(self, media, file=None):
        if file is not None:
            file.write(b"x" * 4096)
        return "arquivo"


class EspiaoCupons:
    """Conta espelho_cupons.removido (o canal de cupons) sem alterá-lo."""

    def __init__(self):
        self.removidos = []
        self._real = espelho_cupons.removido

    def __call__(self, mid):
        self.removidos.append(mid)
        return self._real(mid)

    def __enter__(self):
        espelho_cupons.removido = self
        return self

    def __exit__(self, *a):
        espelho_cupons.removido = self._real


def _isolar_loop():
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


async def drenar():
    """Espera as tasks de exclusão e de remoção física."""
    for _ in range(200):
        ts = [t for t in list(origem_apagada._TAREFAS)
              + list(convergencia._EM_CURSO.values()) if not t.done()]
        if not ts:
            return
        await asyncio.gather(*ts, return_exceptions=True)


def cenario(corpo, cliente=None):
    async def _run():
        _isolar_loop()
        c = cliente or Cliente()
        modulo_client.client = c
        await corpo(c)
        await drenar()
        return c
    return asyncio.run(_run())


# ─────────────────────────────────────────────────────────────────
# Mensagens (normalizadas) e caminho real de publicação
# ─────────────────────────────────────────────────────────────────
_SEQ = {"n": 600000}


def _mid():
    _SEQ["n"] += 1
    return _SEQ["n"]


def produto(chat, texto, pid, msg_id=None, midia=False, cupons=()):
    """Mensagem de produto: identidade pelo id EXATO (adaptador Shopee —
    a regra de exclusão não depende da plataforma)."""
    return MensagemNormalizada(
        msg_id=msg_id or _mid(), chat=str(chat), texto_limpo=texto,
        texto_analise=texto, mapa={"http://o": "http://a"}, preservar=[],
        plat="shopee", sku=pid, tem_midia=midia,
        media_obj=(object() if midia else None), ids_globais=[pid],
        idents=[("shopee", pid, "produto")], cupons=list(cupons))


async def publicar(n):
    await publicacao.enviar(await montar(n), n, enr=enriquecer(n), is_edit=False)


async def editar(n):
    await publicacao.enviar(await montar(n), n, enr=enriquecer_edicao(n),
                            is_edit=True)


def _pid(tag):
    return f"{abs(hash(tag)) % 10**9}.{abs(hash(tag + 'x')) % 10**9}"


def vinculo(chat, msg_id):
    return origem.consultar(str(chat), msg_id)


def origens_de(dest):
    with _db() as db:
        return sorted((r[0], r[1]) for r in db.execute(
            "SELECT chat,msg_id FROM origem_post WHERE dest=?", (dest,)).fetchall())


def exibida(mid):
    with _db() as db:
        return {r[0] for r in db.execute(
            "SELECT identity FROM post_exibida WHERE msg_id_dest=?",
            (mid,)).fetchall()}


def encerrado(mid):
    e = db_get_post(mid)
    return bool(e) and e["janela_fim"] <= time.time() and bool(e.get("delete_status"))


class _Log(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas = []

    def emit(self, rec):
        self.linhas.append(rec.getMessage())


LOG = _Log()
from logger import log_out as _lo, log_sys as _ls  # noqa: E402
_lo.addHandler(LOG)
_ls.addHandler(LOG)


def _logs(tag):
    return [l for l in LOG.linhas if tag in l]


def _tc():
    tc = TelegramClient(StringSession(), 1, "0" * 32)
    tc._mb_entity_cache.set_self_user(424242, False, 0)
    return tc


def _upd_canal(canal_id, ids, pts=1):
    u = types.UpdateDeleteChannelMessages(channel_id=canal_id, messages=list(ids),
                                          pts=pts, pts_count=len(ids))
    u._entities = {}
    return u


# ══════════════════════════════════════════════════════════════════
# PARTE 1 — Ligação no processo (main.py) e despacho REAL do Telethon
# ══════════════════════════════════════════════════════════════════
def test_01_main_liga_uma_vez_apos_os_handlers_da_casa(r):
    arv = ast.parse(open(os.path.join(RAIZ, "main.py"), encoding="utf-8").read())
    prep = next(n for n in ast.walk(arv)
                if isinstance(n, ast.AsyncFunctionDef) and n.name == "_preparar_processo")
    corpo = [ast.unparse(n) for n in prep.body]
    i_reg = corpo.index("_registrar_handlers(fontes)")
    r.check(corpo[i_reg + 1] == "origem_apagada.instalar(client, fontes)",
            "01.instalar_logo_apos_registrar", corpo[i_reg + 1])
    r.check(sum(1 for n in ast.walk(arv) if isinstance(n, ast.Call)
                and ast.unparse(n.func) == "origem_apagada.instalar") == 1,
            "01.uma_vez_no_processo")
    reg = next(n for n in ast.walk(arv)
               if isinstance(n, ast.FunctionDef) and n.name == "_registrar_handlers")
    r.check({n.name for n in ast.walk(reg) if isinstance(n, ast.AsyncFunctionDef)}
            == {"on_new", "on_edit"}, "01.handlers_da_casa_intocados")


def test_02_despacho_real_so_fontes_e_so_canal(r):
    """UpdateDeleteChannelMessages da FONTE chega ao tratamento com o id
    marcado do canal; canal não monitorado e exclusão sem canal
    (UpdateDeleteMessages) não chegam."""
    chamadas = []
    real = origem_apagada.agendar

    def espiao(chat_id, ids):
        chamadas.append((chat_id, list(ids)))

    async def corpo():
        tc = _tc()
        origem_apagada.instalar(tc, [FUMOTOM_ENT, PROMOTOM_ENT])
        await tc._dispatch_update(_upd_canal(FUMOTOM_ID, [35483, 35484]))
        await tc._dispatch_update(_upd_canal(OUTRO_ID, [1]))
        sem_canal = types.UpdateDeleteMessages(messages=[7], pts=1, pts_count=1)
        sem_canal._entities = {}
        await tc._dispatch_update(sem_canal)
        await tc._dispatch_update(_upd_canal(PROMOTOM_ID, [110388]))
    origem_apagada.agendar = espiao
    try:
        asyncio.run(corpo())
    finally:
        origem_apagada.agendar = real
    r.check(chamadas == [(int(FUMOTOM), [35483, 35484]), (int(PROMOTOM), [110388])],
            "02.so_fontes", str(chamadas))


def test_03_ponta_a_ponta_pelo_despacho_real(r):
    """24548: fumotom 35488 publica (única origem); a exclusão chega pelo
    despacho REAL → o post sai do canal e do de cupons, no banco fica
    encerrado com remoção 'ok'."""
    s = {}

    async def corpo(c):
        n = produto(FUMOTOM, T_24548, _pid("t03"), msg_id=35488)
        await publicar(n)
        s["post"] = c.criados[0]
        s["vinc"] = vinculo(FUMOTOM, 35488)
        tc = _tc()
        origem_apagada.instalar(tc, [FUMOTOM_ENT])
        with EspiaoCupons() as esp:
            await tc._dispatch_update(_upd_canal(FUMOTOM_ID, [35488]))
            await drenar()
        s["cupons"] = esp.removidos
    c = cenario(corpo)
    post = s["post"]
    r.check(s["vinc"] == post, "03.vinculo_no_nascimento", str(s))
    r.check(c.deletes == [post], "03.apagado_no_canal", str(c.deletes))
    r.check(s["cupons"] == [post], "03.apagado_no_canal_de_cupons", str(s["cupons"]))
    e = db_get_post(post)
    r.check(e["delete_status"] == "ok" and e["janela_fim"] <= time.time()
            and e["fused_into"] is None, "03.encerrado_no_banco", str(e))
    r.check(exibida(post) == set() and vinculo(FUMOTOM, 35488) is None,
            "03.sem_exibicao_e_sem_vinculo")


def test_04_despacho_nao_espera_lock(r):
    """O handler só agenda: com a origem travada (publicação em curso), o
    despacho do Telethon volta na hora; a exclusão acontece quando o lock
    solta."""
    s = {}

    async def corpo(c):
        n = produto(FUMOTOM, T_24548, _pid("t04"))
        await publicar(n)
        s["post"] = c.criados[0]
        tc = _tc()
        origem_apagada.instalar(tc, [FUMOTOM_ENT])
        lk = await origem.lock_origem(FUMOTOM, n.msg_id)
        await lk.acquire()
        t0 = time.monotonic()
        await asyncio.wait_for(
            tc._dispatch_update(_upd_canal(FUMOTOM_ID, [n.msg_id])), 1.0)
        s["dt"] = time.monotonic() - t0
        await asyncio.sleep(0.05)
        s["antes"] = list(c.deletes)
        lk.release()
    c = cenario(corpo)
    r.check(s["dt"] < 0.5, "04.despacho_imediato", f"{s['dt']:.3f}s")
    r.check(s["antes"] == [], "04.esperou_o_lock", str(s["antes"]))
    r.check(c.deletes == [s["post"]], "04.apagou_depois", str(c.deletes))


# ══════════════════════════════════════════════════════════════════
# PARTE 2 — A regra: "todas apagaram"
# ══════════════════════════════════════════════════════════════════
def test_05_producao_24539_duas_fontes(r):
    """24539: nasceu do fumotom 35483 e casou o Promotom 110388. Apagar
    só o fumotom MANTÉM o post; apagar o Promotom também o REMOVE."""
    pid = _pid("t05")
    s = {}

    async def corpo(c):
        await publicar(produto(FUMOTOM, T_24539, pid, msg_id=35483))
        await publicar(produto(PROMOTOM, T_24539, pid, msg_id=110388))
        s["criados"] = list(c.criados)
        post = c.criados[0]
        s["origens"] = origens_de(post)
        s["e1"] = await origem_apagada.apagadas(FUMOTOM, [35483])
        await drenar()
        s["apos_1"] = list(c.deletes)
        s["vivo_1"] = not encerrado(post)
        s["e2"] = await origem_apagada.apagadas(PROMOTOM, [110388])
    c = cenario(corpo)
    post = s["criados"][0]
    r.check(len(s["criados"]) == 1, "05.um_post_so", str(s["criados"]))
    r.check(s["origens"] == sorted([(FUMOTOM, 35483), (PROMOTOM, 110388)]),
            "05.duas_origens_ligadas", str(s["origens"]))
    r.check(s["e1"] == [] and s["apos_1"] == [] and s["vivo_1"],
            "05.uma_apagada_post_fica", str(s))
    r.check(s["e2"] == [post] and c.deletes == [post], "05.todas_apagadas_sai",
            str(c.deletes))


def test_06_ordem_inversa(r):
    """A ordem das exclusões não importa: primeiro a que casou, depois a
    que publicou."""
    pid = _pid("t06")
    s = {}

    async def corpo(c):
        a = produto(FUMOTOM, T_24539, pid)
        b = produto(PROMOTOM, T_24539, pid)
        await publicar(a)
        await publicar(b)
        s["post"] = c.criados[0]
        await origem_apagada.apagadas(PROMOTOM, [b.msg_id])
        await drenar()
        s["apos_1"] = list(c.deletes)
        await origem_apagada.apagadas(FUMOTOM, [a.msg_id])
    c = cenario(corpo)
    r.check(s["apos_1"] == [] and c.deletes == [s["post"]], "06.so_no_fim",
            f"{s['apos_1']} {c.deletes}")


def test_07_tres_fontes_e_mensagem_alheia(r):
    """Três origens; exclusões de mensagens que nunca viraram post (e de
    outro canal) não mexem em nada."""
    pid = _pid("t07")
    s = {}

    async def corpo(c):
        ns = [produto(ch, T_24539, pid) for ch in (FUMOTOM, PROMOTOM, FADA)]
        for n in ns:
            await publicar(n)
        s["post"] = c.criados[0]
        s["r0"] = await origem_apagada.apagadas(FUMOTOM, [999001, 999002])
        s["r1"] = await origem_apagada.apagadas(OUTRO, [ns[0].msg_id])
        s["r2"] = await origem_apagada.apagadas(FUMOTOM, [ns[0].msg_id])
        s["r3"] = await origem_apagada.apagadas(FADA, [ns[2].msg_id])
        await drenar()
        s["antes"] = list(c.deletes)
        s["r4"] = await origem_apagada.apagadas(PROMOTOM, [ns[1].msg_id])
    c = cenario(corpo)
    r.check(s["r0"] == s["r1"] == s["r2"] == s["r3"] == [] and s["antes"] == [],
            "07.fica_enquanto_uma_existir", str(s))
    r.check(s["r4"] == [s["post"]] and c.deletes == [s["post"]], "07.sai_na_ultima",
            str(c.deletes))


def test_08_post_antigo_sem_limite_de_idade(r):
    """Sem limite de idade: post com a vida já encerrada (texto congelado)
    também sai quando as origens somem."""
    s = {}

    async def corpo(c):
        n = produto(FUMOTOM, T_24548, _pid("t08"))
        await publicar(n)
        s["post"] = c.criados[0]
        with _db() as db:
            db.execute("UPDATE post_estado SET janela_fim=? WHERE msg_id_dest=?",
                       (time.time() - 6 * 3600, s["post"]))
        s["r"] = await origem_apagada.apagadas(FUMOTOM, [n.msg_id])
    c = cenario(corpo)
    r.check(s["r"] == [s["post"]] and c.deletes == [s["post"]], "08.antigo_sai",
            str(c.deletes))


def test_09_varios_ids_num_evento(r):
    """Um evento com vários ids: só os que têm post contam; cada post é
    removido uma vez."""
    s = {}

    async def corpo(c):
        a = produto(FUMOTOM, "Produto A R$ 10", _pid("t09a"))
        b = produto(FUMOTOM, "Produto B R$ 20", _pid("t09b"))
        await publicar(a)
        await publicar(b)
        s["posts"] = list(c.criados)
        s["r"] = await origem_apagada.apagadas(
            FUMOTOM, [888001, a.msg_id, 888002, b.msg_id, a.msg_id])
    c = cenario(corpo)
    r.check(s["r"] == s["posts"] and sorted(c.deletes) == sorted(s["posts"]),
            "09.cada_post_uma_vez", f"{s['r']} {c.deletes}")


# ══════════════════════════════════════════════════════════════════
# PARTE 3 — Republicação pela fonte e outras fontes depois da exclusão
# ══════════════════════════════════════════════════════════════════
def test_10_apaga_e_reposta_corrigido(r):
    """A fonte apaga e reposta (mensagem nova): o post antigo sai e a
    nova vira post novo — nunca edita o post encerrado."""
    pid = _pid("t10")
    s = {}

    async def corpo(c):
        a = produto(FUMOTOM, "Produto X R$ 99", pid)
        await publicar(a)
        s["velho"] = c.criados[0]
        await origem_apagada.apagadas(FUMOTOM, [a.msg_id])
        await drenar()
        a2 = produto(FUMOTOM, "Produto X R$ 89 (corrigido)", pid)
        await publicar(a2)
        s["a2"] = a2
    c = cenario(corpo)
    r.check(c.deletes == [s["velho"]], "10.velho_saiu", str(c.deletes))
    r.check(len(c.criados) == 2 and not any(e[0] == s["velho"] for e in c.edits),
            "10.novo_post_e_velho_intocado", f"{c.criados} {c.edits}")
    r.check(vinculo(FUMOTOM, s["a2"].msg_id) == c.criados[1], "10.vinculo_no_novo")


def test_11_reposta_antes_de_apagar(r):
    """A fonte reposta ANTES de apagar a original: a nova casa o mesmo
    post (vínculo); apagar a original MANTÉM o post; apagar a nova o
    REMOVE."""
    pid = _pid("t11")
    s = {}

    async def corpo(c):
        a = produto(FUMOTOM, "Produto Y R$ 50", pid)
        await publicar(a)
        a2 = produto(FUMOTOM, "Produto Y R$ 50", pid)
        await publicar(a2)
        s["post"] = c.criados[0]
        s["criados"] = list(c.criados)
        s["r1"] = await origem_apagada.apagadas(FUMOTOM, [a.msg_id])
        await drenar()
        s["antes"] = list(c.deletes)
        s["r2"] = await origem_apagada.apagadas(FUMOTOM, [a2.msg_id])
    c = cenario(corpo)
    r.check(len(s["criados"]) == 1, "11.um_post", str(s["criados"]))
    r.check(s["r1"] == [] and s["antes"] == [], "11.original_apagada_fica")
    r.check(c.deletes == [s["post"]], "11.nova_apagada_sai", str(c.deletes))


def test_12_outra_fonte_depois_da_exclusao_vira_post_novo(r):
    """Encerrado o post, a mesma oferta vinda de outra fonte (viva) é
    publicada de novo — o post encerrado não é família de ninguém."""
    pid = _pid("t12")
    s = {}

    async def corpo(c):
        a = produto(FUMOTOM, T_24539, pid)
        await publicar(a)
        s["velho"] = c.criados[0]
        await origem_apagada.apagadas(FUMOTOM, [a.msg_id])
        await drenar()
        await publicar(produto(PROMOTOM, T_24539, pid))
    c = cenario(corpo)
    r.check(len(c.criados) == 2 and c.deletes == [s["velho"]],
            "12.novo_post_da_fonte_viva", f"{c.criados} {c.deletes}")
    r.check(not encerrado(c.criados[1]), "12.novo_vivo")


# ══════════════════════════════════════════════════════════════════
# PARTE 4 — Corridas
# ══════════════════════════════════════════════════════════════════
def test_13_exclusao_durante_a_publicacao(r):
    """A exclusão chega com a publicação da MESMA mensagem em curso (o
    envio ao Telegram ainda não voltou): espera o lock de ORIGEM, acha o
    vínculo recém-gravado e remove — nenhum post órfão."""
    s = {}

    async def corpo(c):
        c.portao_envio = asyncio.Event()
        n = produto(FUMOTOM, T_24548, _pid("t13"))
        pub = asyncio.create_task(publicar(n))
        await asyncio.sleep(0.05)
        exc = asyncio.create_task(origem_apagada.apagadas(FUMOTOM, [n.msg_id]))
        await asyncio.sleep(0.05)
        s["criados_antes"] = list(c.criados)
        s["exc_esperando"] = not exc.done()
        c.portao_envio.set()
        await pub
        s["r"] = await exc
    c = cenario(corpo)
    r.check(s["criados_antes"] == [] and s["exc_esperando"], "13.esperou_a_publicacao")
    r.check(len(c.criados) == 1 and c.deletes == c.criados and s["r"] == c.criados,
            "13.publicou_e_removeu", f"{c.criados} {c.deletes}")


def test_14_exclusao_antes_da_publicacao_nova_e_edicao(r):
    """A exclusão chega ANTES de a mensagem (ainda na fila) publicar: nada
    é publicado — nem a nova, nem uma edição dela."""
    s = {}

    async def corpo(c):
        n = produto(FUMOTOM, T_24548, _pid("t14"))
        s["r"] = await origem_apagada.apagadas(FUMOTOM, [n.msg_id])
        await publicar(n)
        await editar(replace(n, texto_limpo=n.texto_limpo + " (editado)",
                             texto_analise=n.texto_analise + " (editado)"))
        s["vinc"] = vinculo(FUMOTOM, n.msg_id)
    c = cenario(corpo)
    r.check(s["r"] == [] and c.criados == [] and c.edits == [] and c.deletes == [],
            "14.nada_publicado", f"{c.criados} {c.edits}")
    r.check(s["vinc"] is None, "14.sem_vinculo")
    r.check(any("apagada na fonte antes de publicar" in l
                for l in _logs("ORIGEM_APAGADA")), "14.log")


def test_15_orquestracao_descarta_antes_de_converter(r):
    """Na orquestração, a mensagem apagada nem chega à normalização (sem
    conversão de link, sem memória de cupom)."""
    chamadas = []
    real = orchestrator_pipeline.normalizar

    async def espiao(bruta):
        chamadas.append(bruta.msg_id)
        return None

    async def corpo(c):
        tc = _tc()
        m = types.Message(id=35490, peer_id=types.PeerChannel(FUMOTOM_ID),
                          date=dt.datetime.now(dt.timezone.utc), message="x", post=True)
        m._finish_init(tc, {int(FUMOTOM): FUMOTOM_ENT}, None)
        m2 = types.Message(id=35491, peer_id=types.PeerChannel(FUMOTOM_ID),
                           date=dt.datetime.now(dt.timezone.utc), message="y", post=True)
        m2._finish_init(tc, {int(FUMOTOM): FUMOTOM_ENT}, None)
        await origem_apagada.apagadas(FUMOTOM, [35490])
        await orchestrator_pipeline._pipeline(EventoRecuperado(m), is_edit=False)
        await orchestrator_pipeline._pipeline(EventoRecuperado(m), is_edit=True)
        await orchestrator_pipeline._pipeline(EventoRecuperado(m2), is_edit=False)
    orchestrator_pipeline.normalizar = espiao
    try:
        cenario(corpo)
    finally:
        orchestrator_pipeline.normalizar = real
    r.check(chamadas == [35491], "15.so_a_nao_apagada_normaliza", str(chamadas))


def test_16_outra_fonte_casa_sob_o_lock_do_post(r):
    """Outra fonte registrando o vínculo no MESMO post enquanto a exclusão
    espera o lock do post: o post fica (a outra fonte segura)."""
    pid = _pid("t16")
    s = {}

    async def corpo(c):
        a = produto(FUMOTOM, T_24539, pid)
        await publicar(a)
        post = c.criados[0]
        s["post"] = post
        lk = await exclusao.lock_post(post)
        await lk.acquire()
        exc = asyncio.create_task(origem_apagada.apagadas(FUMOTOM, [a.msg_id]))
        await asyncio.sleep(0.05)
        origem.registrar(PROMOTOM, 110999, post)   # o encontro, sob o lock
        lk.release()
        s["r"] = await exc
    c = cenario(corpo)
    r.check(s["r"] == [] and c.deletes == [] and not encerrado(s["post"]),
            "16.post_fica", f"{s['r']} {c.deletes}")
    r.check(origens_de(s["post"]) == [(PROMOTOM, 110999)], "16.so_a_viva")


def test_17_vinculo_muda_sob_o_lock(r):
    """Fusão/substituição troca o post do vínculo enquanto a exclusão
    espera o lock: relido sob o lock, refaz no post novo."""
    s = {}

    async def corpo(c):
        a = produto(FUMOTOM, "Produto P1 R$ 10", _pid("t17a"))
        b = produto(PROMOTOM, "Produto P2 R$ 20", _pid("t17b"))
        await publicar(a)
        await publicar(b)
        p1, p2 = c.criados
        s["p1"], s["p2"] = p1, p2
        origem.registrar(PROMOTOM, b.msg_id, p1)    # P2 só com a origem de A depois
        lk = await exclusao.lock_post(p1)
        await lk.acquire()
        exc = asyncio.create_task(origem_apagada.apagadas(FUMOTOM, [a.msg_id]))
        await asyncio.sleep(0.05)
        with _db() as db:                           # como a fusão faz, sob o lock
            db.execute("UPDATE origem_post SET dest=? WHERE chat=? AND msg_id=?",
                       (p2, FUMOTOM, a.msg_id))
        lk.release()
        s["r"] = await exc
    c = cenario(corpo)
    r.check(s["r"] == [s["p2"]] and c.deletes == [s["p2"]], "17.refez_no_novo",
            f"{s['r']} {c.deletes}")
    r.check(not encerrado(s["p1"]), "17.antigo_intocado")


def test_18_concorrencia_duas_exclusoes_do_mesmo_post(r):
    """As duas fontes apagam ao mesmo tempo: o post sai UMA vez."""
    pid = _pid("t18")
    s = {}

    async def corpo(c):
        a = produto(FUMOTOM, T_24539, pid)
        b = produto(PROMOTOM, T_24539, pid)
        await publicar(a)
        await publicar(b)
        s["post"] = c.criados[0]
        await asyncio.gather(origem_apagada.apagadas(FUMOTOM, [a.msg_id]),
                             origem_apagada.apagadas(PROMOTOM, [b.msg_id]))
    c = cenario(corpo)
    r.check(c.deletes == [s["post"]] and c.tentativas_delete == 1,
            "18.uma_remocao", str(c.deletes))


def test_19_remocao_fisica_fora_de_todo_lock(r):
    """Nenhum I/O de Telegram com lock de ORIGEM ou de POST seguro."""
    async def corpo(c):
        n = produto(FUMOTOM, T_24548, _pid("t19"))
        await publicar(n)
        await origem_apagada.apagadas(FUMOTOM, [n.msg_id])
    c = cenario(corpo)
    r.check(c.no_delete == [[]], "19.sem_lock_no_delete", str(c.no_delete))


# ══════════════════════════════════════════════════════════════════
# PARTE 5 — Substituição, fusão, falha, FloodWait, boot, memória
# ══════════════════════════════════════════════════════════════════
def test_20_substituicao_leva_todas_as_origens(r):
    """Substituição (apagar+reenviar com mídia) move TODAS as origens para
    o corpo novo (I5) — antes só a que disparou a troca. Sem isso, apagar
    a mensagem que disparou removeria o post com outras fontes vivas."""
    pid = _pid("t20")
    s = {}

    async def corpo(c):
        a = produto(FUMOTOM, "Produto Z R$ 10", pid)
        b = produto(PROMOTOM, "Produto Z R$ 10", pid)
        await publicar(a)
        await publicar(b)
        velho = c.criados[0]
        c.falha_edit_com_file = True
        n2 = produto(FADA, "Produto Z R$ 10 com foto", pid, midia=True)
        montada = replace(await montar(n2), imagem=io.BytesIO(b"x" * 4096))
        d = Decisao(EVOLUIR, "EVOLUI", novo_score=20, exigir_imagem=True,
                    permite_substituir=True, trocar_midia=True, na_janela=True)
        await _aplicar_evolucao(montada, n2, d, db_get_post(velho), velho, 0,
                                [f"shopee|{pid}"], "t20",
                                exibidas=[f"shopee|{pid}"])
        novo = c.criados[-1]
        s.update(velho=velho, novo=novo, a=a, b=b, n2=n2,
                 origens=origens_de(novo), no_velho=origens_de(velho))
        s["r1"] = await origem_apagada.apagadas(FADA, [n2.msg_id])
        await drenar()
        s["apos_1"] = [x for x in c.deletes if x != velho]
        s["r2"] = await origem_apagada.apagadas(FUMOTOM, [a.msg_id])
        s["r3"] = await origem_apagada.apagadas(PROMOTOM, [b.msg_id])
    c = cenario(corpo)
    r.check(s["novo"] != s["velho"] and s["velho"] in c.deletes, "20.substituiu",
            f"{s['velho']} {s['novo']} {c.deletes}")
    r.check(s["origens"] == sorted([(FUMOTOM, s["a"].msg_id),
                                    (PROMOTOM, s["b"].msg_id),
                                    (FADA, s["n2"].msg_id)]) and s["no_velho"] == [],
            "20.todas_as_origens_no_novo", f"{s['origens']} {s['no_velho']}")
    r.check(s["r1"] == [] and s["apos_1"] == [], "20.a_que_trocou_apagada_fica")
    r.check(s["r2"] == [] and s["r3"] == [s["novo"]], "20.sai_na_ultima",
            f"{s['r2']} {s['r3']}")


def test_21_fusao_redireciona_e_a_regra_segue(r):
    """Depois de uma FUSÃO as origens do fundido seguram o sobrevivente."""
    s = {}

    async def corpo(c):
        a = produto(FUMOTOM, "Produto F1 R$ 10", _pid("t21a"))
        b = produto(PROMOTOM, "Produto F2 R$ 20", _pid("t21b"))
        await publicar(a)
        await publicar(b)
        p1, p2 = c.criados
        from database_posts import db_fundir_posts
        s["fundiu"] = db_fundir_posts(p2, [p1], time.time())
        convergencia.agendar_remocao(p1)            # como consolidar() faz
        await drenar()
        s["p1"], s["p2"] = p1, p2
        s["r1"] = await origem_apagada.apagadas(FUMOTOM, [a.msg_id])
        await drenar()
        s["antes"] = list(c.deletes)
        s["r2"] = await origem_apagada.apagadas(PROMOTOM, [b.msg_id])
    c = cenario(corpo)
    r.check(s["fundiu"] == [s["p1"]], "21.fundiu", str(s["fundiu"]))
    r.check(s["r1"] == [] and s["antes"] == [s["p1"]], "21.sobrevivente_fica",
            str(s["antes"]))
    r.check(s["r2"] == [s["p2"]] and c.deletes == [s["p1"], s["p2"]],
            "21.sai_na_ultima", str(c.deletes))


def test_22_falha_fica_para_o_boot(r):
    """Remoção que falha nas 3 tentativas: 'falhou' no banco; o boot
    (retomar_remocoes) reagenda com o rótulo certo e remove."""
    s = {}
    LOG.linhas.clear()

    async def corpo(c):
        n = produto(FUMOTOM, T_24548, _pid("t22"))
        await publicar(n)
        s["post"] = c.criados[0]
        c.falhas_delete = 3
        await origem_apagada.apagadas(FUMOTOM, [n.msg_id])
        await drenar()
        s["status"] = db_get_post(s["post"])["delete_status"]
        s["fisico"] = list(c.deletes)
        s["agendadas"] = convergencia.retomar_remocoes()
        await drenar()
    c = cenario(corpo)
    r.check(s["status"] == "falhou" and s["fisico"] == [], "22.falhou",
            str(s["status"]))
    r.check(s["agendadas"] >= 1 and c.deletes == [s["post"]]
            and db_get_post(s["post"])["delete_status"] == "ok", "22.boot_remove",
            f"{s['agendadas']} {c.deletes}")
    r.check(any(f"[ORIGEM_APAGADA_REMOVIDO] post:{s['post']}" in l
                for l in _logs("REMOVIDO")), "22.rotulo_no_boot")


def test_23_floodwait_honrado(r):
    """FloodWait na remoção: espera e tenta de novo (mesmo caminho da fusão)."""
    async def corpo(c):
        n = produto(FUMOTOM, T_24548, _pid("t23"))
        await publicar(n)
        c.flood_delete = 1
        await origem_apagada.apagadas(FUMOTOM, [n.msg_id])
    c = cenario(corpo)
    r.check(c.tentativas_delete == 2 and len(c.deletes) == 1, "23.repetiu",
            f"{c.tentativas_delete} {c.deletes}")


def test_24_encerramento_do_processo(r):
    """Com o processo encerrando: o handler não agenda nada; uma remoção
    já encerrada no banco fica 'pendente' para o próximo boot."""
    s = {}

    async def corpo(c):
        n = produto(FUMOTOM, T_24548, _pid("t24"))
        await publicar(n)
        s["post"] = c.criados[0]
        g._encerrando = True
        origem_apagada.agendar(int(FUMOTOM), [n.msg_id])
        s["tarefas"] = len(origem_apagada._TAREFAS)
        await origem_apagada.apagadas(FUMOTOM, [n.msg_id])
        await drenar()
        s["status"] = db_get_post(s["post"])["delete_status"]
        g._encerrando = False
    c = cenario(corpo)
    r.check(s["tarefas"] == 0, "24.handler_nao_agenda")
    r.check(c.deletes == [] and s["status"] == "pendente", "24.pendente_para_o_boot",
            str(s["status"]))


def test_25_post_ja_encerrado_nao_repete(r):
    """Vínculo gravado num post já encerrado (corrida rara): apagar não
    remove de novo."""
    s = {}

    async def corpo(c):
        n = produto(FUMOTOM, T_24548, _pid("t25"))
        await publicar(n)
        s["post"] = c.criados[0]
        await origem_apagada.apagadas(FUMOTOM, [n.msg_id])
        await drenar()
        origem.registrar(PROMOTOM, 120001, s["post"])
        s["r"] = await origem_apagada.apagadas(PROMOTOM, [120001])
    c = cenario(corpo)
    r.check(s["r"] == [] and c.deletes == [s["post"]] and c.tentativas_delete == 1,
            "25.uma_vez_so", str(c.deletes))


def test_26_banco_transacao_e_situacoes(r):
    """db_desvincular_origem: cada situação, e nada muda quando o vínculo
    aponta para outro post."""
    async def corpo(c):
        a = produto(FUMOTOM, "Produto D R$ 1", _pid("t26"))
        await publicar(a)
        post = c.criados[0]
        agora = time.time()
        r.check(db_desvincular_origem(FUMOTOM, 1, post, agora) == ("sem_vinculo", 0),
                "26.sem_vinculo")
        r.check(db_desvincular_origem(FUMOTOM, a.msg_id, post + 1, agora)
                == ("mudou", post), "26.mudou")
        r.check(vinculo(FUMOTOM, a.msg_id) == post, "26.mudou_nao_apaga")
        origem.registrar(PROMOTOM, 130001, post)
        r.check(db_desvincular_origem(FUMOTOM, a.msg_id, post, agora)
                == ("mantido", 1), "26.mantido")
        r.check(db_desvincular_origem(PROMOTOM, 130001, post, agora)
                == ("morto", 0), "26.morto")
        origem.registrar(PROMOTOM, 130002, 99999999)
        r.check(db_desvincular_origem(PROMOTOM, 130002, 99999999, agora)
                == ("sem_post", 0), "26.sem_post")
    cenario(corpo)


def test_27_memoria_limitada(r):
    """A lembrança das apagadas tem teto e expira."""
    teto, lembrar = origem_apagada._TETO, origem_apagada._LEMBRAR_S
    try:
        origem_apagada._TETO = 50
        origem_apagada._APAGADAS.clear()
        for i in range(200):
            origem_apagada._lembrar(FUMOTOM, i)
        r.check(len(origem_apagada._APAGADAS) == 50, "27.teto",
                str(len(origem_apagada._APAGADAS)))
        r.check(origem_apagada.apagada(FUMOTOM, 199)
                and not origem_apagada.apagada(FUMOTOM, 0), "27.mais_novas_ficam")
        origem_apagada._LEMBRAR_S = 0
        r.check(not origem_apagada.apagada(FUMOTOM, 199), "27.expira")
    finally:
        origem_apagada._TETO, origem_apagada._LEMBRAR_S = teto, lembrar
        origem_apagada._APAGADAS.clear()


def test_28_post_vivo_nunca_vira_pendente_no_boot(r):
    """O boot só retoma posts encerrados (fusão ou exclusão): um post vivo
    não entra em db_remocoes_pendentes."""
    from database_posts import db_remocoes_pendentes
    s = {}

    async def corpo(c):
        n = produto(FUMOTOM, T_24548, _pid("t28"))
        await publicar(n)
        s["post"] = c.criados[0]
        s["pend"] = db_remocoes_pendentes(10000)
    cenario(corpo)
    r.check(s["post"] not in s["pend"], "28.vivo_fora", str(s["pend"][-5:]))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "ORIGEM APAGADA · a fonte apaga → o post sai · árvore real"))
