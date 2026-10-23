"""
CORRIDA DE MÍDIA no post de cupom — contra o Telethon REAL 1.45.0.

NÃO usa o telethon falso do harness: o defeito está DENTRO da biblioteca
(espera automática de FloodWait e o adiamento de toda requisição do mesmo
tipo), então send_file, upload_file e TelegramClient._call aqui são os
verdadeiros. Só a REDE é falsa: um servidor que responde às requisições
MTProto com o comportamento medido em produção.

CASO REAL (logs da Railway + prévia pública dos dois canais, 02–23/10/2026):
  · 80 de 343 cupons com foto: um dos dois envios simultâneos (principal ou
    canal de cupons, mesmo arquivo enviado) saiu 10–11 s depois do outro —
    37 vezes o principal, 43 a cópia. 0 de 43 cupons só texto.
  · 19/10 03:00:03 e 23/10 03:00:03: um post de PRODUTO (sem cópia, outro
    arquivo) iniciado dentro da janela só saiu no segundo exato em que a
    cópia perdedora saiu (40395→03:00:14; 41835→03:00:13).
  · Nenhuma linha de log: o Telethon dorme sozinho em FloodWait <= 60 s
    (flood_sleep_threshold, nunca passado em client.py) e só registra em
    INFO no logger telethon.client.users, que não tem handler.

CASO REAL DA EDIÇÃO (depois do deploy da correção da publicação, 23/10/2026):
  · 14:46:14 e 15:00:19 UTC: cupom só texto (Promotom 111845 → 31194;
    111847 → 31196) e, segundos depois, a mesma oferta COM foto (Samuel
    122893; …) → [UPGRADE_MIDIA] TROCA/post_sem_midia. As duas edições
    (principal + canal de cupons) saíram juntas com o MESMO arquivo enviado
    e a linha nova mostrou: ⏳ [ESPERA_TELETHON] req=EditMessageRequest
    seg=10 — FLOOD_WAIT_10 do servidor, dormido pelo Telethon, que ainda
    adia toda EditMessage da conta até o prazo. 2 de 2 trocas de mídia.

MODELO DO SERVIDOR (o fato medido, nada além): dois pedidos de mídia
(messages.sendMedia ou messages.editMessage com mídia) com a MESMA mídia em
voo ao mesmo tempo → um deles recebe FLOOD_WAIT_10 (qual perde varia: os
dois lados aparecem em produção). Relógio virtual SÓ dentro de
telethon.client.users (time.time e asyncio.sleep), para os 10 s do
FloodWait custarem milissegundos sem tocar a lógica.

  01  biblioteca: FLOOD_WAIT_10 num SendMedia não vira exceção nem WARNING;
      o SendMedia seguinte, de OUTRO arquivo, fica adiado até o prazo
  02  REPRODUÇÃO (principal perde): o principal sai ~10 s depois — antes da
      correção. Depois: o principal sai sozinho e logo
  03  REPRODUÇÃO (cópia perde): um produto iniciado 1 s depois fica preso
      até a cópia sair (caso 41835) — antes. Depois: ninguém espera
  04  o principal não espera a cópia (cópia lenta 10 s ou FloodWait nela)
  05  cópia fiel por referência: texto cru, entidades, a MESMA foto, sem
      novo upload, vínculo no banco, uma cópia só
  06  falha da cópia: pontual → uma cópia (2ª tentativa); permanente →
      nenhuma, principal intacto, nada duplicado
  07  principal falhou → nenhuma requisição ao canal de cupons
  08  cupom só texto → os dois envios continuam começando juntos
  09  texto longo (> legenda) com foto → foto + texto nos dois canais, a
      cópia depois do principal, sem baixar a imagem de novo
  10  principal degradou para texto → a cópia nunca sai com foto
  11  ordem: nenhum envio de mídia do canal de cupons começa antes de o do
      principal terminar
  12  shutdown com a cópia em curso → drena, sem duplicar
  13  medição: cada espera automática do Telethon vira UMA linha
      (⏳ [ESPERA_TELETHON] classe, segundos, antecipada) e nada mais muda
      — WARNING do Telethon sai igual pelo lastResort, INFO continua mudo
  14  medição: ⏱ [ENVIO_MS] separa o envio ao principal e a cópia (ms,
      operação, resultado), sem texto nem link; a espera aparece na cópia
  15  REPRODUÇÃO DA EDIÇÃO (troca de mídia, caso 31194/31196): antes, as duas
      edições com o mesmo arquivo → FLOOD_WAIT_10 numa delas. Depois: a do
      canal de cupons começa quando a do principal termina, com a foto do
      principal por referência — sem espera, um upload só, os dois iguais
  16  a troca de mídia não segura a edição de OUTRO post (antes: toda
      EditMessage da conta ficava adiada até o fim do FloodWait)

    python tests/test_cupons_corrida_midia.py
"""
from __future__ import annotations

import asyncio
import io
import itertools
import logging
import os
import random
import sys
import time
import types as pytypes
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import aiohttp  # noqa: E402,F401  (o real: o harness só falsifica o que faltar)
import telethon  # noqa: E402
from telethon import TelegramClient, errors, functions  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import types  # noqa: E402
from telethon.tl.patched import Message as _MsgTL  # noqa: E402
import telethon.client.users as _tusers  # noqa: E402

assert hasattr(TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from test_cupom_sem_codigo import (  # noqa: E402  (preparar() já rodou lá)
    FADA, PROMOTOM, SAMUEL, _isolar_loop, _zerar_banco, msg)
from _harness_e5 import rodar  # noqa: E402
import client as modulo_client  # noqa: E402
from config import GRUPO_DESTINO  # noqa: E402
from database import db_espelho_get  # noqa: E402
from pipeline import convergencia, espelho_cupons, publicacao  # noqa: E402
from pipeline.enriquecimento import enriquecer  # noqa: E402
from pipeline.montagem import montar  # noqa: E402

convergencia._ESPERA_BASE_S = 0.0
CANAL = "FullPromotionCupons"
ID_PRINCIPAL, ID_CUPONS = 1700000001, 1700000002
FLOOD_S = 10
RAPIDO = FLOOD_S / 2          # "saiu logo": bem longe dos 10 s do FloodWait

# ── relógio virtual só dentro de telethon.client.users ─────────────────
_ESCALA = 0.1                  # 1 s virtual = 100 ms reais (folga para CI lento)
_T0_REAL = time.monotonic()
_BASE = 1_800_000_000.0


def agora() -> float:
    return _BASE + (time.monotonic() - _T0_REAL) / _ESCALA


async def vdormir(seg: float) -> None:
    await asyncio.sleep(max(0.0, seg) * _ESCALA)


class _TempoVirtual:
    def time(self):
        return agora()

    def __getattr__(self, nome):
        return getattr(time, nome)


class _AsyncioVirtual:
    async def sleep(self, seg, *a, **k):
        await vdormir(seg)

    def __getattr__(self, nome):
        return getattr(asyncio, nome)


_tusers.time = _TempoVirtual()
_tusers.asyncio = _AsyncioVirtual()


# ── imagem de verdade (o Telethon abre com o PIL antes do upload) ──────
def _jpeg(semente: int) -> bytes:
    from PIL import Image
    rng = random.Random(semente)
    img = Image.new("RGB", (48, 48))
    img.putdata([(rng.randrange(256), rng.randrange(256), rng.randrange(256))
                 for _ in range(48 * 48)])
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=90)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════
# Servidor falso (a rede); cliente = TelegramClient REAL
# ══════════════════════════════════════════════════════════════════════
class Servidor:
    """`send(request)` do MTProtoSender. Responde como o Telegram, com:
      · `perde`: quando dois pedidos de mídia (SendMedia ou EditMessage com
        mídia) da MESMA mídia ficam em voo juntos,
        quem recebe FLOOD_WAIT_10 — "segundo" (o que chegou depois) ou
        "primeiro" (o que já estava em voo);
      · `regra(req, destino)`: (latência, exceção|None) por requisição."""

    def __init__(self, perde="segundo", regra=None, lat=0.3):
        self.perde, self.regra, self.lat = perde, regra, lat
        self.reg = []                # (t, evento, classe, destino, chave)
        self.publicados = []         # dicts das mensagens criadas
        self.em_voo = []
        self.fotos = {}
        self._ids = {ID_PRINCIPAL: 50000, ID_CUPONS: 900}
        self._ids_foto = itertools.count(7_000_000)
        self.downloads = 0

    @staticmethod
    def destino(req):
        alvo = getattr(req, "peer", None) or getattr(req, "channel", None)
        cid = getattr(alvo, "channel_id", None)
        return {ID_PRINCIPAL: "principal", ID_CUPONS: "cupons"}.get(cid, "?")

    @staticmethod
    def chave(media):
        if isinstance(media, types.InputMediaUploadedPhoto):
            return ("arquivo", media.file.id)
        if isinstance(media, types.InputMediaPhoto):
            return ("foto", media.id.id)
        return None

    def send(self, request, ordered=False):
        if isinstance(request, list):                 # o Telethon manda lista em lote
            return [self.send(r) for r in request]
        fut = asyncio.get_running_loop().create_future()
        asyncio.get_running_loop().create_task(self._tratar(request, fut))
        return fut

    async def _tratar(self, req, fut):
        try:
            await self._tratar_ou_falhar(req, fut)
        except BaseException as e:                    # nunca deixa o cliente pendurado
            if not fut.done():
                fut.set_exception(RuntimeError(f"servidor falso: {type(e).__name__}: {e}"))
            if not isinstance(e, Exception):
                raise

    async def _tratar_ou_falhar(self, req, fut):
        nome, dest = type(req).__name__, self.destino(req)
        if isinstance(req, functions.upload.SaveFilePartRequest):
            await vdormir(0.05)
            self.reg.append((agora(), "fim", nome, "-", req.file_id))
            fut.set_result(True)
            return
        if not isinstance(req, (functions.messages.SendMediaRequest,
                                functions.messages.SendMessageRequest,
                                functions.messages.EditMessageRequest,
                                functions.channels.DeleteMessagesRequest,
                                functions.channels.GetMessagesRequest)):
            raise NotImplementedError(nome)
        chave = self.chave(getattr(req, "media", None))
        self.reg.append((agora(), "chega", nome, dest, chave))
        lat, falha = (self.regra(req, dest) if self.regra else (None, None))
        lat = self.lat if lat is None else lat
        voo = {"req": req, "chave": chave, "perde": False}
        if isinstance(req, (functions.messages.SendMediaRequest,
                            functions.messages.EditMessageRequest)) and chave is not None:
            rival = next((v for v in self.em_voo if v["chave"] == chave), None)
            if rival is not None:
                if self.perde == "segundo":
                    voo["perde"] = True
                else:
                    rival["perde"] = True
        self.em_voo.append(voo)
        try:
            await vdormir(0.05 if voo["perde"] else lat)
            if voo["perde"]:
                self.reg.append((agora(), "flood", nome, dest, chave))
                fut.set_exception(errors.FloodWaitError(request=req, capture=FLOOD_S))
                return
            if falha is not None:
                self.reg.append((agora(), "erro", nome, dest, chave))
                fut.set_exception(falha)
                return
            fut.set_result(self._responder(req, dest, chave))
        finally:
            self.em_voo.remove(voo)

    def _foto(self, chave, agora_dt):
        if chave is None:
            return None
        if chave[0] == "arquivo":
            foto = types.Photo(id=next(self._ids_foto), access_hash=11, file_reference=b"fr",
                               date=agora_dt, sizes=[types.PhotoSize("x", 48, 48, 999)], dc_id=1)
            self.fotos[foto.id] = foto
            return foto
        return self.fotos[chave[1]]

    def _tl(self, p):
        cid = ID_PRINCIPAL if p["canal"] == "principal" else ID_CUPONS
        foto = self.fotos.get(p["foto"]) if p["foto"] else None
        return _MsgTL(id=p["id"], peer_id=types.PeerChannel(cid), date=_dt(), message=p["texto"],
                      out=True, post=True, entities=p["entidades"] or None,
                      media=types.MessageMediaPhoto(photo=foto) if foto else None)

    def _responder(self, req, dest, chave):
        cid = ID_PRINCIPAL if dest == "principal" else ID_CUPONS
        agora_dt = _dt()
        self.reg.append((agora(), "fim", type(req).__name__, dest, chave))
        if isinstance(req, functions.channels.DeleteMessagesRequest):
            n = 0
            for p in self.publicados:
                if p["canal"] == dest and p["id"] in req.id and not p["apagada"]:
                    p["apagada"], n = True, n + 1
            return types.messages.AffectedMessages(pts=1, pts_count=n)
        if isinstance(req, functions.channels.GetMessagesRequest):
            ids = [getattr(i, "id", i) for i in req.id]
            msgs = [self._tl(p) for p in self.publicados
                    if p["canal"] == dest and p["id"] in ids and not p["apagada"]]
            return types.messages.ChannelMessages(pts=1, count=len(msgs), messages=msgs,
                                                  topics=[], chats=[_canal(cid)], users=[])
        if isinstance(req, functions.messages.EditMessageRequest):
            p = next(p for p in self.publicados
                     if p["canal"] == dest and p["id"] == req.id and not p["apagada"])
            if req.message is not None:
                p["texto"], p["entidades"] = req.message, list(req.entities or [])
            if req.media is not None:
                p["foto"] = self._foto(chave, agora_dt).id
            p["edicoes"] += 1
            return types.Updates(
                updates=[types.UpdateEditChannelMessage(message=self._tl(p), pts=1, pts_count=1)],
                users=[], chats=[_canal(cid)], date=agora_dt, seq=0)
        self._ids[cid] += 1
        mid = self._ids[cid]
        foto = self._foto(chave, agora_dt) if isinstance(
            req, functions.messages.SendMediaRequest) else None
        p = dict(t=agora(), canal=dest, id=mid, texto=req.message,
                 entidades=list(req.entities or []), foto=foto.id if foto else None,
                 por_referencia=bool(chave and chave[0] == "foto"), apagada=False, edicoes=0)
        self.publicados.append(p)
        if isinstance(req, functions.messages.SendMessageRequest):
            return types.UpdateShortSentMessage(id=mid, pts=mid, pts_count=1, date=agora_dt,
                                                out=True, entities=req.entities)
        return types.Updates(
            updates=[types.UpdateMessageID(id=mid, random_id=req.random_id),
                     types.UpdateNewChannelMessage(message=self._tl(p), pts=mid, pts_count=1)],
            users=[], chats=[_canal(cid)], date=agora_dt, seq=0)

    # leitura
    def em(self, canal):
        """Mensagens VIVAS no canal, na ordem de criação."""
        return [p for p in self.publicados if p["canal"] == canal and not p["apagada"]]

    def tentativas(self, canal):
        return [(e, c) for _t, e, c, d, _k in self.reg if d == canal]

    def eventos(self, evento, canal=None, classe="SendMediaRequest"):
        return [t for t, e, c, d, _k in self.reg
                if e == evento and c == classe and (canal is None or d == canal)]


def _dt():
    import datetime as _d
    return _d.datetime.fromtimestamp(agora(), _d.timezone.utc)


def _canal(cid):
    nome = GRUPO_DESTINO if cid == ID_PRINCIPAL else CANAL
    return types.Channel(id=cid, title=nome, photo=types.ChatPhotoEmpty(), date=None,
                         broadcast=True, access_hash=cid * 3, username=nome)


def cliente(servidor) -> TelegramClient:
    c = TelegramClient(StringSession(), 1, "0" * 32)
    c._sender = servidor
    for cid in (ID_PRINCIPAL, ID_CUPONS):
        c.session.process_entities(types.contacts.ResolvedPeer(
            peer=types.PeerChannel(cid), chats=[_canal(cid)], users=[]))

    async def baixar(media, file=None, **_k):        # a ORIGEM; fora do que se testa
        servidor.downloads += 1
        file.write(_jpeg(getattr(media, "semente", 1)))
        return file
    c.download_media = baixar
    return c


def cenario(corpo, servidor):
    async def _run():
        _isolar_loop()
        c = cliente(servidor)
        modulo_client.client = c
        antes = os.environ.get("CANAL_CUPONS")
        os.environ["CANAL_CUPONS"] = CANAL
        espelho_cupons.iniciar()
        try:
            return await asyncio.wait_for(corpo(c, servidor), 120)
        finally:
            await drenar()
            await espelho_cupons.encerrar()
            if antes is None:
                os.environ.pop("CANAL_CUPONS", None)
            else:
                os.environ["CANAL_CUPONS"] = antes
    _zerar_banco()
    return asyncio.run(_run())


async def drenar(limite=30.0):
    fim = time.monotonic() + limite
    while espelho_cupons._tarefas:
        restante = fim - time.monotonic()
        assert restante > 0, "operações do canal de cupons não terminaram"
        await asyncio.wait(set(espelho_cupons._tarefas), timeout=restante)


# ── corpus (textos reais do canal; URLs longas sintéticas) ─────────────
RICO_17525 = ("🚨 Novos Cupons Shopee\n\n🎟 R$30 OFF em R$299\n🎟 R$90 OFF em R$899\n\n"
              "✅ Resgate aqui: \nhttps://s.shopee.com.br/AAGNE4pfS9")
SECO_110370 = ("Cupons Shopee\n\n-Resgate aqui:\n1 https://s.shopee.com.br/2B5snGi9NX\n"
               "2 https://s.shopee.com.br/qaVCprDoA")
PRODUTO = "🔥 Fone de Ouvido Bluetooth Polyvox com Cancelamento\n\n💵 R$ 89\n"
PRODUTO_URL = "https://shopee.com.br/product/338371559/22593482107"


def foto(n, semente=None):
    return replace(n, tem_midia=True,
                   media_obj=pytypes.SimpleNamespace(semente=semente or n.msg_id),
                   midia_key=f"k-{n.msg_id}")


async def pub(n, score=10):
    enr = replace(enriquecer(n), score=score)
    await publicacao.enviar(await montar(n), n, enr=enr, is_edit=False)


def cupom_com_foto(chat=FADA, texto=RICO_17525):
    return foto(msg(chat, texto))


ML_17529 = ("🔥 Cupom Mercado Livre\n\n🎟 10% OFF acima de R$79, limite R$100: PROMOML\n\n"
            "✅ Resgate aqui:\nhttps://meli.la/2XoB7wc")
SHOPEE_17860 = ("🚨 CUPOM SHOPEE 🚨\n\n🎟 R$20 OFF em R$60: 4DMD4CUP0M\n\n"
                "✅ Resgate aqui: https://s.shopee.com.br/4DMD")
CUPONS_DISTINTOS = (
    lambda: foto(msg(FADA, RICO_17525)),
    lambda: foto(msg(PROMOTOM, ML_17529, cupons=["PROMOML"], plat="mercadolivre")),
    lambda: foto(msg(SAMUEL, SHOPEE_17860, cupons=["4DMD4CUP0M"])),
)


def produto_com_foto():
    return foto(msg(SAMUEL, PRODUTO + PRODUTO_URL, urls_longas=[PRODUTO_URL]))


# ══════════════════════════════════════════════════════════════════════
def test_01_biblioteca_dorme_em_silencio_e_adia_o_tipo(r):
    """O comportamento do Telethon que explica o silêncio e o produto preso.
    Sem código do FOGUETÃO: só a biblioteca contra o servidor."""
    srv = Servidor(perde="segundo", lat=0.3)
    avisos = []

    class _Pega(logging.Handler):
        def emit(self, rec):
            avisos.append((rec.levelno, rec.getMessage()))

    pega = _Pega()
    lg = logging.getLogger("telethon.client.users")
    lg.addHandler(pega)
    nivel_antes = lg.level
    lg.setLevel(logging.DEBUG)

    async def corpo(c, s):
        r.check(c.flood_sleep_threshold == 60, "01.flood_sleep_threshold_padrao_60",
                str(c.flood_sleep_threshold))
        h = await c.upload_file(_bytesio(_jpeg(1)))
        t0 = agora()
        a = asyncio.ensure_future(c.send_file(GRUPO_DESTINO, h, caption="a"))
        await vdormir(0.01)
        b = asyncio.ensure_future(c.send_file(CANAL, h, caption="b"))
        await vdormir(1.0)
        t_terc = agora()
        terc = asyncio.ensure_future(c.send_file(GRUPO_DESTINO, _bytesio(_jpeg(2)), caption="c"))
        await asyncio.gather(a, b, terc)
        fins = {p["texto"]: p["t"] - t0 for p in s.publicados}
        return fins, t_terc - t0
    try:
        fins, t_terc = cenario(corpo, srv)
    finally:
        lg.removeHandler(pega)
        lg.setLevel(nivel_antes)
    r.check(fins["a"] < RAPIDO, "01.primeiro_sai_logo", str(fins))
    r.check(FLOOD_S <= fins["b"] < FLOOD_S + RAPIDO, "01.perdedor_sai_depois_do_floodwait",
            str(fins))
    r.check(fins["c"] >= FLOOD_S - 0.5, "01.outro_arquivo_adiado_ate_o_prazo",
            f"iniciado em {t_terc:.1f}s, saiu em {fins['c']:.1f}s")
    dormiu = [m for lv, m in avisos if "flood wait" in m]
    r.check(len(dormiu) == 2 and all(lv == logging.INFO for lv, m in avisos if "flood wait" in m)
            and any("early" in m for m in dormiu) and all("SendMediaRequest" in m for m in dormiu),
            "01.so_INFO_sem_excecao_e_adiamento_antecipado", str(avisos))


def _bytesio(b):
    f = io.BytesIO(b)
    f.name = "imagem.jpg"
    return f


def test_02_reproducao_principal_perde(r):
    srv = Servidor(perde="primeiro")

    async def corpo(c, s):
        t0 = agora()
        await pub(cupom_com_foto())
        await drenar()
        return t0
    t0 = cenario(corpo, srv)
    pr, cp = srv.em("principal"), srv.em("cupons")
    r.check(len(pr) == 1 and len(cp) == 1, "02.um_post_em_cada_canal", f"{pr} {cp}")
    if pr and cp:
        r.check(pr[0]["t"] - t0 < RAPIDO, "02.principal_sai_logo",
                f"principal {pr[0]['t'] - t0:.1f}s, cópia {cp[0]['t'] - t0:.1f}s")
    r.check(not srv.eventos("flood", "principal"), "02.principal_nunca_recebe_floodwait",
            str(srv.eventos("flood")))


def test_03_reproducao_copia_perde_e_segura_o_produto(r):
    srv = Servidor(perde="segundo")

    async def corpo(c, s):
        t0 = agora()
        cup = asyncio.ensure_future(pub(cupom_com_foto()))
        await vdormir(1.0)                       # produto de outra fonte, 1 s depois
        t_prod = agora()
        await pub(produto_com_foto(), score=8)
        await cup
        await drenar()
        return t0, t_prod
    t0, t_prod = cenario(corpo, srv)
    prod = [p for p in srv.em("principal") if p["texto"].startswith("🔥 Fone")]
    r.check(len(prod) == 1, "03.produto_publicado", str(srv.publicados))
    if prod:
        r.check(prod[0]["t"] - t_prod < RAPIDO, "03.produto_nao_fica_preso",
                f"produto iniciado {t_prod - t0:.1f}s, saiu {prod[0]['t'] - t0:.1f}s")
    r.check(not srv.eventos("flood"), "03.nenhum_floodwait", str(srv.eventos("flood")))
    r.check(len(srv.em("cupons")) == 1, "03.copia_publicada")


def test_04_principal_nao_espera_a_copia(r):
    def lenta(req, d):
        return (10.0, None) if d == "cupons" else (None, None)

    def flood_uma_vez(req, d, visto=[]):
        if d == "cupons" and not visto:
            visto.append(1)
            return None, errors.FloodWaitError(request=req, capture=FLOOD_S)
        return None, None
    for rot, regra in (("lenta_10s", lenta), ("floodwait_na_copia", flood_uma_vez)):
        srv = Servidor(regra=regra)

        async def corpo(c, s):
            t0 = agora()
            n = cupom_com_foto()
            await pub(n)
            fim_pub = agora() - t0
            vinculo_antes = db_espelho_get(_principal(s))
            await drenar()
            return fim_pub, vinculo_antes, agora() - t0
        fim_pub, vinculo_antes, fim_total = cenario(corpo, srv)
        r.check(fim_pub < RAPIDO, f"04.{rot}.publicacao_nao_espera", f"{fim_pub:.2f}s")
        cp = srv.em("cupons")
        r.check(len(cp) == 1 and fim_total >= 9.5, f"04.{rot}.copia_chega_depois",
                f"{len(cp)} cópias, total {fim_total:.1f}s")
        r.check(vinculo_antes is None, f"04.{rot}.vinculo_so_quando_a_copia_existe",
                str(vinculo_antes))


def _principal(s):
    pr = s.em("principal")
    return pr[0]["id"] if pr else None


def test_05_copia_fiel_por_referencia(r):
    srv = Servidor()

    async def corpo(c, s):
        await pub(cupom_com_foto())
        await drenar()
        return db_espelho_get(_principal(s))
    vinculo = cenario(corpo, srv)
    pr, cp = srv.em("principal"), srv.em("cupons")
    r.check(len(pr) == 1 and len(cp) == 1, "05.uma_mensagem_em_cada", f"{pr} {cp}")
    if not (pr and cp):
        return
    r.check(cp[0]["texto"] == pr[0]["texto"], "05.texto_cru_igual")
    r.check([e.to_dict() for e in cp[0]["entidades"]] == [e.to_dict() for e in pr[0]["entidades"]]
            and pr[0]["entidades"], "05.entidades_iguais",
            f"{[type(e).__name__ for e in pr[0]['entidades']]}")
    r.check(cp[0]["foto"] == pr[0]["foto"] and cp[0]["por_referencia"], "05.mesma_foto_por_referencia",
            f"{cp[0]['foto']} {pr[0]['foto']}")
    partes = [k for _t, e, c, _d, k in srv.reg if c == "SaveFilePartRequest"]
    r.check(len(set(partes)) == 1, "05.um_upload_so", str(partes))
    r.check(vinculo == cp[0]["id"], "05.vinculo_no_banco", f"{vinculo} {cp[0]['id']}")
    # Diferença esperada entre os canais: UM SendMedia (latência do servidor
    # 0,3 s aqui) mais o tempo de aplicação — nunca antes, nunca ~10 s depois.
    delta = cp[0]["t"] - pr[0]["t"]
    r.check(0.0 < delta < RAPIDO, "05.copia_logo_depois_do_principal",
            f"Δ={delta:.2f}s (latência do servidor {srv.lat}s)")


def test_06_falha_da_copia(r):
    for rot, n_falhas, copias in (("pontual", 1, 1), ("permanente", 99, 0)):
        cont = {"n": 0}

        def regra(req, d, cont=cont, n_falhas=n_falhas):
            if d == "cupons":
                cont["n"] += 1
                if cont["n"] <= n_falhas:
                    return None, errors.ChatWriteForbiddenError(request=req)
            return None, None
        srv = Servidor(regra=regra)

        async def corpo(c, s):
            await pub(cupom_com_foto())
            await drenar()
            return db_espelho_get(_principal(s))
        vinculo = cenario(corpo, srv)
        pr, cp = srv.em("principal"), srv.em("cupons")
        r.check(len(pr) == 1, f"06.{rot}.principal_intacto", str(pr))
        r.check(len(cp) == copias, f"06.{rot}.copias={copias}", str(cp))
        r.check((vinculo == cp[0]["id"]) if copias else vinculo is None, f"06.{rot}.vinculo",
                str(vinculo))


def test_07_principal_falhou(r):
    srv = Servidor(regra=lambda req, d: (None, errors.ChatWriteForbiddenError(request=req))
                   if d == "principal" else (None, None))

    async def corpo(c, s):
        await pub(cupom_com_foto())
        await drenar()
    cenario(corpo, srv)
    r.check(not srv.em("principal") and not srv.tentativas("cupons"),
            "07.nenhuma_requisicao_ao_canal_de_cupons", str(srv.tentativas("cupons")))


def test_08_so_texto_continua_junto(r):
    srv = Servidor(lat=2.0)                   # em sequência, a cópia chegaria >= 2 s depois

    async def corpo(c, s):
        await pub(msg(PROMOTOM, SECO_110370), score=3)
        await drenar()
    cenario(corpo, srv)
    ch_pr = srv.eventos("chega", "principal", "SendMessageRequest")
    ch_cp = srv.eventos("chega", "cupons", "SendMessageRequest")
    r.check(len(ch_pr) == 1 and len(ch_cp) == 1 and abs(ch_pr[0] - ch_cp[0]) < 1.0,
            "08.comecam_juntos", f"{ch_pr} {ch_cp}")
    r.check(len(srv.em("cupons")) == 1, "08.copia_publicada")


def test_09_texto_longo_com_foto(r):
    srv = Servidor()
    longo = RICO_17525 + "\n\n" + "\n".join(f"🎟 R${i} OFF em R${i * 10}" for i in range(10, 90))

    async def corpo(c, s):
        await pub(cupom_com_foto(texto=longo))
        await drenar()
    cenario(corpo, srv)
    pr, cp = srv.em("principal"), srv.em("cupons")
    r.check([bool(p["foto"]) for p in pr] == [True, False] and [bool(p["foto"]) for p in cp]
            == [True, False], "09.foto_e_texto_nos_dois", f"{pr} {cp}")
    r.check(srv.downloads == 1, "09.sem_baixar_de_novo", str(srv.downloads))
    fim_pr = max(p["t"] for p in pr) if pr else 0
    ini_cp = srv.eventos("chega", "cupons") + srv.eventos("chega", "cupons", "SendMessageRequest")
    r.check(ini_cp and min(ini_cp) >= fim_pr - 0.05, "09.copia_depois_do_principal",
            f"{ini_cp} {fim_pr}")
    r.check(len(cp) == 2 and cp[1]["texto"] == pr[1]["texto"], "09.mesmo_texto")


def test_10_principal_degradou_para_texto(r):
    def regra(req, d):
        if d == "principal" and isinstance(req, functions.messages.SendMediaRequest):
            return None, errors.PhotoInvalidDimensionsError(request=req)
        return None, None
    srv = Servidor(regra=regra)

    async def corpo(c, s):
        await pub(cupom_com_foto())
        await drenar()
    cenario(corpo, srv)
    pr, cp = srv.em("principal"), srv.em("cupons")
    r.check(len(pr) == 1 and pr[0]["foto"] is None, "10.principal_so_texto", str(pr))
    r.check(len(cp) == 1 and cp[0]["foto"] is None and cp[0]["texto"] == pr[0]["texto"],
            "10.copia_so_texto", str(cp))
    r.check(not srv.eventos("chega", "cupons"), "10.copia_nunca_tentou_foto",
            str(srv.eventos("chega", "cupons")))


def test_11_ordem_nenhuma_midia_simultanea(r):
    violacoes = []
    for semente in range(20):
        rng = random.Random(semente)
        srv = Servidor(perde=rng.choice(["primeiro", "segundo"]),
                       regra=lambda req, d, rng=rng: (rng.uniform(0.05, 1.5), None))

        async def corpo(c, s, rng=rng):
            tarefas = [asyncio.ensure_future(pub(fab()))
                       for fab in rng.sample(CUPONS_DISTINTOS, 2)]
            await asyncio.gather(*tarefas)
            await drenar()
        cenario(corpo, srv)
        principais = [q for q in srv.em("principal") if q["foto"]]
        if len(principais) != 2 or len(srv.em("cupons")) != 2:
            violacoes.append((semente, "contagem", len(principais), len(srv.em("cupons"))))
        for p in srv.em("cupons"):
            irmao = next((q for q in srv.em("principal") if q["foto"] == p["foto"]), None)
            ini = [t for t, e, c, d, k in srv.reg if e == "chega" and d == "cupons"
                   and k == ("foto", p["foto"])]
            if irmao is None or not ini or ini[0] < irmao["t"]:
                violacoes.append((semente, p, irmao))
        if srv.eventos("flood"):
            violacoes.append((semente, "flood", srv.eventos("flood")))
    r.check(not violacoes, "11.uma_copia_por_post_so_depois_do_principal_sem_floodwait",
            str(violacoes[:3]))


def test_12_shutdown_com_copia_em_curso(r):
    srv = Servidor(regra=lambda req, d: (3.0, None) if d == "cupons" else (None, None))

    async def corpo(c, s):
        await pub(cupom_com_foto())
        await espelho_cupons.encerrar()           # sem drenar antes
        return db_espelho_get(_principal(s))
    vinculo = cenario(corpo, srv)
    cp = srv.em("cupons")
    r.check(len(srv.em("principal")) == 1 and len(cp) == 1 and vinculo == cp[0]["id"],
            "12.drenou_sem_duplicar", f"{cp} {vinculo}")


class _Coleta(logging.Handler):
    def __init__(self, nome):
        super().__init__(logging.DEBUG)
        self.linhas, self.lg = [], logging.getLogger(nome)

    def emit(self, rec):
        self.linhas.append((rec.levelno, rec.getMessage()))

    def __enter__(self):
        self.lg.addHandler(self)
        return self

    def __exit__(self, *exc):
        self.lg.removeHandler(self)
        return False


def test_13_espera_do_telethon_ganha_uma_linha_e_nada_mais_muda(r):
    import espera_telethon
    lg = logging.getLogger("telethon.client.users")
    nivel_antes, ultimo_antes = lg.level, logging.lastResort
    saida_padrao = io.StringIO()
    logging.lastResort = logging.StreamHandler(saida_padrao)
    logging.lastResort.setLevel(logging.WARNING)
    srv = Servidor(perde="segundo")
    try:
        espera_telethon.instalar()
        espera_telethon.instalar()                         # idempotente
        r.check(lg.filters.count(espera_telethon._FILTRO) == 1, "13.um_filtro_so")

        async def corpo(c, s):
            h = await c.upload_file(_bytesio(_jpeg(3)))
            a = asyncio.ensure_future(c.send_file(GRUPO_DESTINO, h, caption="segredo-a"))
            await vdormir(0.01)
            b = asyncio.ensure_future(c.send_file(CANAL, h, caption="segredo-b"))
            await vdormir(1.0)
            terc = asyncio.ensure_future(c.send_file(GRUPO_DESTINO, _bytesio(_jpeg(4)),
                                                     caption="segredo-c"))
            await asyncio.gather(a, b, terc)
            lg.warning("aviso real do Telethon %s", "x")    # WARNING: caminho de sempre
        with _Coleta("SISTEMA") as sis:
            cenario(corpo, srv)
    finally:
        lg.removeFilter(espera_telethon._FILTRO)
        lg.setLevel(nivel_antes)
        logging.lastResort = ultimo_antes
    esperas = [m for _lv, m in sis.linhas if "[ESPERA_TELETHON]" in m]
    nao = [m for m in esperas
           if m == "⏳ [ESPERA_TELETHON] req=SendMediaRequest seg=10 antecipada=nao"]
    sim = [m for m in esperas if m.startswith("⏳ [ESPERA_TELETHON] req=SendMediaRequest seg=")
           and m.endswith(" antecipada=sim")]
    r.check(len(esperas) == 2 and len(nao) == 1 and len(sim) == 1,
            "13.uma_linha_por_espera_perdedor_e_antecipada", str(esperas))
    r.check(all(lv == logging.WARNING for lv, m in sis.linhas if "[ESPERA_TELETHON]" in m),
            "13.nivel_warning")
    r.check(not any("segredo" in m for _lv, m in sis.linhas), "13.nada_de_texto")
    impresso = saida_padrao.getvalue()
    r.check(impresso.count("aviso real do Telethon x") == 1 and "flood wait" not in impresso,
            "13.lastresort_igual_WARNING_sai_INFO_nao", repr(impresso))


def test_14_medicao_separa_principal_e_copia(r):
    def flood_na_copia(req, d, visto=[]):
        if d == "cupons" and not visto:
            visto.append(1)
            return None, errors.FloodWaitError(request=req, capture=FLOOD_S)
        return None, None
    medidas = {}
    for rot, regra in (("normal", None), ("floodwait_na_copia", flood_na_copia)):
        srv = Servidor(regra=regra)

        async def corpo(c, s):
            await pub(cupom_com_foto())
            await drenar()
        with _Coleta("SISTEMA") as sis:
            cenario(corpo, srv)
        linhas = [m for _lv, m in sis.linhas if "[ENVIO_MS]" in m]
        medidas[rot] = linhas
        pr = [m for m in linhas if "destino=principal op=publicar" in m]
        cp = [m for m in linhas if "destino=cupons op=copia" in m]
        r.check(len(pr) == 1 and "resultado=ok tentativas=1 midia=sim" in pr[0],
                f"14.{rot}.uma_linha_do_principal", str(linhas))
        r.check(len(cp) == 1 and cp[0].endswith("resultado=ok"), f"14.{rot}.uma_linha_da_copia",
                str(linhas))
        r.check(not any(("Shopee" in m or "http" in m or "R$" in m) for m in linhas),
                f"14.{rot}.nada_de_texto_ou_link", str(linhas))
    ms = {rot: int(next(m for m in ls if "cupons" in m).split("ms=")[1].split()[0])
          for rot, ls in medidas.items()}
    real_do_flood = FLOOD_S * _ESCALA * 1000
    r.check(ms["floodwait_na_copia"] >= 0.8 * real_do_flood > ms["normal"],
            "14.a_espera_aparece_no_tempo_da_copia", f"{ms} (FloodWait = {real_do_flood:.0f} ms reais)")


# ── edição com troca de mídia (UPGRADE_MIDIA TROCA/post_sem_midia) ─────
def _so_texto(chat=PROMOTOM):
    return msg(chat, SHOPEE_17860, cupons=["4DMD4CUP0M"])


def _com_foto(chat=SAMUEL):
    return foto(msg(chat, SHOPEE_17860, cupons=["4DMD4CUP0M"]))


def _edicoes(s, canal, com_midia=True):
    """(chega, fim, chave) de cada EditMessageRequest no canal."""
    out, aberto = [], {}
    for t, e, c, d, k in s.reg:
        if c != "EditMessageRequest" or d != canal or (com_midia and k is None):
            continue
        if e == "chega":
            aberto.setdefault(k, []).append(t)
        elif e in ("fim", "flood", "erro") and aberto.get(k):
            out.append((aberto[k].pop(0), t, k, e))
    return out


def test_15_reproducao_edicao_com_troca_de_midia(r):
    for perde in ("segundo", "primeiro"):
        srv = Servidor(perde=perde)

        async def corpo(c, s):
            await pub(_so_texto(), score=9)
            await drenar()
            partes_antes = len({k for _t, e, cl, _d, k in s.reg if cl == "SaveFilePartRequest"})
            t0 = agora()
            await pub(_com_foto(), score=9)                 # IGUAL → troca de mídia
            fim_pub = agora() - t0
            await drenar()
            partes = {k for _t, e, cl, _d, k in s.reg if cl == "SaveFilePartRequest"}
            return t0, fim_pub, len(partes) - partes_antes
        t0, fim_pub, uploads = cenario(corpo, srv)
        rot = f"15.{perde}"
        pr, cp = srv.em("principal"), srv.em("cupons")
        r.check(len(pr) == 1 and len(cp) == 1 and pr[0]["foto"] is not None,
                f"{rot}.troca_de_midia_no_principal", f"{pr} {cp}")
        if not (pr and cp):
            continue
        r.check(cp[0]["foto"] == pr[0]["foto"] and cp[0]["texto"] == pr[0]["texto"],
                f"{rot}.canal_de_cupons_igual_mesma_foto", f"{cp[0]['foto']} {pr[0]['foto']}")
        r.check(not srv.eventos("flood", classe="EditMessageRequest"),
                f"{rot}.nenhum_floodwait_na_edicao",
                str(srv.eventos("flood", classe="EditMessageRequest")))
        ep = _edicoes(srv, "principal")
        ec = _edicoes(srv, "cupons", com_midia=False)        # TODA edição lá
        r.check(len(ep) == 1 and len(ec) == 1 and ec[0][0] >= ep[0][1],
                f"{rot}.uma_edicao_no_canal_de_cupons_depois_do_principal", f"{ep} {ec}")
        r.check(len(ec) == 1 and ec[0][2] == ("foto", pr[0]["foto"]),
                f"{rot}.copia_por_referencia", str(ec))
        r.check(uploads == 1, f"{rot}.um_upload_so", str(uploads))
        r.check(len(ep) == 1 and ep[0][1] - t0 < RAPIDO and fim_pub < RAPIDO,
                f"{rot}.principal_nao_espera",
                f"principal editou em {ep[0][1] - t0:.1f}s; aplicador {fim_pub:.1f}s" if ep else "")


def test_16_troca_de_midia_nao_segura_edicao_de_outro_post(r):
    srv = Servidor(perde="segundo")

    async def corpo(c, s):
        seco = msg(FADA, SECO_110370)
        await pub(seco, score=3)                       # post B (cupom sem código)
        await pub(_so_texto(), score=9)                # post A (só texto)
        await drenar()
        id_b = next(p["id"] for p in s.em("principal") if "Cupons Shopee" in p["texto"])
        up = asyncio.ensure_future(pub(_com_foto(), score=9))     # A: troca de mídia
        await vdormir(1.0)
        t_b = agora()
        await pub(msg(SAMUEL, RICO_17525), score=10)              # B: evolução (texto)
        await up
        await drenar()
        return id_b, t_b
    id_b, t_b = cenario(corpo, srv)
    edic_b = [(t, e) for t, e, c, d, _k in srv.reg if c == "EditMessageRequest"
              and d == "principal" and e == "fim"]
    b_final = next((p for p in srv.em("principal") if p["id"] == id_b), None)
    r.check(b_final is not None and "R$30 OFF em R$299" in b_final["texto"],
            "16.post_b_evoluiu", str(b_final and b_final["texto"][:40]))
    fins_b = [t for t, _e in edic_b if t >= t_b]
    r.check(fins_b and min(fins_b) - t_b < RAPIDO, "16.edicao_de_b_nao_fica_presa",
            f"B iniciou em {t_b:.1f}; edições terminadas: {[round(t - t_b, 1) for t in fins_b]}")
    r.check(not srv.eventos("flood", classe="EditMessageRequest"), "16.nenhum_floodwait",
            str(srv.eventos("flood", classe="EditMessageRequest")))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CUPOM · corrida de mídia nos dois canais (Telethon REAL "
                              + telethon.__version__ + ")"))
