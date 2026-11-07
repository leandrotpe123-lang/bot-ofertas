"""
CURADORIA — hardware só vai ao canal com aval do Fada Promoções ou do Nerd
Ofertas. Pelo caminho REAL: Telethon real, `processar` real (travas de
admissão), `_pipeline` real, enriquecimento, portão, deduplicação, montagem e
publicação reais, banco real (temporário). Só a normalização, que faz rede
(afiliação), é trocada por um dicionário com a identidade de cada mensagem —
a MESMA forma (MensagemNormalizada.idents) que a normalização real entrega —,
e a identificação do Nerd (rede) por um mapa link → identidade; o teste 32
roda a identificação REAL, com a rede trocada só na expansão de link.

As regras do Léo (03/11/2026), uma por teste:
  01  Fumotom manda a placa de vídeo primeiro → TRAVA; o Fada solta a mesma
      placa → o Fada sai normal e a do Fumotom DESTRAVA na hora (sem nova
      consulta ao Telegram) e casa no mesmo post (nenhuma duplicata)
  02  passou de 5 min sem aval → a segurada expira e não vai ao canal; se o
      Fada mandar depois, o post do Fada sai normal
  03  o Nerd soltou ANTES → a do Promotom passa direto; nada do Nerd vai ao
      canal
  04  o Promotom manda primeiro, o Nerd 3 min depois → a do Promotom destrava
      (a reinjeção atravessa a trava de 120 s e a idempotência, com prova)
  05  o Nerd fora da janela (6 min) → não solta; "não tem problema"
  06  categoria certa: notebook gamer, PC gamer, monitor comum, air fryer…
      passam direto; monitor gamer, fonte, cooler… travam
  07  Promotom, Fumotom e Samuel travam; Fada dos Cupons, Oferta Convertida
      e Fada Promoções publicam; só o Fada Promoções dá aval
  08  produto DIFERENTE no Nerd (título parecido, outro ASIN) não solta
  09  edição da segurada continua segurada (a janela não recomeça); edição
      da já publicada sincroniza normal
  10  a fonte apagou a segurada antes do aval → nada vai ao canal (pelo
      aviso de exclusão e, depois de reinício, pela busca na fonte)
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
  17  banco fora no portão BLOQUEIA o hardware (e só ele: outra categoria
      segue); cupom não é curado
  18  eventos: curadoria.retida encerra a execução como DESCARTADA; a
      reinjeção chega com via CURADORIA; o aval sai como curadoria.aval
  19  main.py: o canal de aval sai das fontes antes dos handlers; a
      curadoria liga depois dos handlers e encerra antes de HTTP/Telegram/
      banco
  20  catálogo: o delta da curadoria é exatamente o declarado
  21  edição que troca o produto: o aval do produto novo solta
  22  FloodWait na leitura do canal de aval é respeitado
  23  o classificador custa menos de 2 ms por post
  24  lista de produtos distintos passa; um produto com dois links trava
  25  frase de destaque em qualquer ponto do texto da PRÓPRIA mensagem
      passa; comentário (outra mensagem) e "Esgotou" não soltam
  26  retorno ("Voltando!") passa, pela pipeline.reativacao
  27  quem passa por exceção não vira aval
Bateria da auditoria final (item 8 do fechamento):
  28  papel de cada fonte pelo ID REAL; impostor com o @ do Fada e fonte
      nova travam; instalar confere ID × @
  29  só Fada Promoções e Nerd gravam aval (isentas, retentoras e exceções
      nunca)
  30  o Nerd nunca publica: sai das fontes, handler só observa, o portão o
      barra se cair no funil, a soltura é sempre a mensagem da fonte
  31  mesma identidade E mesma loja; loja sem identidade exata, link sem
      produto e link de campanha não são aval
  32  identificação REAL do Nerd sem afiliar: nenhum afilia/SiteStripe/
      createLink, nenhum cache de afiliação, links_cache/short_links intactos
  33  300 s nos dois sentidos pelas datas originais: limite exato no banco;
      299 passa e 301 não, no caminho real (Nerd e Fada)
  34  aval válido visto atrasado (varredura) ainda solta; a rodada lê o aval
      antes de expirar
  35  reentrega, recuperação, dois avais e reentrega depois de solta: um
      post e uma soltura
  36  edição da segurada com "Ainda ativo!" sai uma vez e fecha a linha
  37  falhas de leitura: busca na fonte e identificação do Nerd tentam de
      novo, com limite; exceção contida
  38  relógio local atrás do Telegram não perde aval nem reinjeção
  39  a soltura usa a mensagem da memória, na última edição
  40  sem bypass: a curadoria não envia, não escreve SQL, não afilia; uma
      única entrega ao ponto de entrada
  41  lista do Nerd com cabeçalho dá aval a cada produto
Auditoria forense (itens 1 a 4):
  42  a mensagem guarda TODOS os produtos da versão atual; aval de A OU de
      B (mesma loja) solta a mensagem inteira
  43  A→A+B guarda os dois (B pelo edit_date); A→B guarda só B (aval de A
      não solta); A+B→B; edições repetidas não devolvem o tirado nem
      duplicam a publicação
  44  troca do conjunto e aval na mesma transação (threads); reinjeção só
      passa com o conjunto exato que o aval soltou
  45  hardware SEM identidade (vazia, sem código, campanha, encurtador,
      busca) fica retido, expira normal; edição com link identificável
      passa a esperar aval; lista vazia nunca aprova; exceção dispensa aval
  46  banco fora: bloqueia só o hardware, só na memória (logs dizem que não
      é durável); banco volta: aval gravado, reavaliação, soltura sem duplicar
  47  exceção interna nunca autoriza; fonte isenta e outra categoria não
      bloqueiam; limite de repetição; SOMBRA nunca bloqueia
  48  memória limitada; fora da janela sai da memória; reinjeção perdida é
      reentregue (com limite); sem ler a prova não há reinjeção
  49  edição publicada com o mesmo produto (preço, cupom) sincroniza normal
  50  edição publicada com produto novo vigiado sem aval fica PENDENTE (o
      post segue com o aprovado); o aval aplica no MESMO post
  51  o produto novo da edição entra pelo edit_date (4 min antes vale; 6 min
      não; perto da data original não)
  52  pendente substituída (volta ao aprovado, troca para outra categoria)
      fecha; aval tardio não reaplica
  53  A→A+B publicada (aval de A cobre; publicada por exceção espera aval);
      edição que tira a identidade fica pendente
  54  edição de mensagem não líder: pendente; com aval, o mesmo post
  55  SOMBRA aplica e registra; edição do Fada dá aval pelo edit_date
  56  segurada antiga editada com produto novo: espera e sai pela data da
      versão
  57  o aprovado do post inclui a família; produto já publicado vale pela
      data original
Última auditoria (edição no Nerd, reentrada, EDIT_ANTIGO):
  58  o Nerd posta sem link e edita acrescentando o link: aval na hora pela
      edição (também em mensagem antiga do Nerd editada agora)
  59  o Nerd acrescenta B: A conserva a data, B entra pelo edit_date; rever
      a versão não duplica aval
  60  handler e varredura não duplicam a edição; a varredura acha a edição
      que o handler perdeu; o Nerd nunca entra no funil
  61  produto que sai e volta na fonte é ocorrência nova: aval antigo não a
      solta; aval novo solta; o que permanece conserva a data
  62  no Nerd, o produto que sai e volta ganha aval novo
  63  EDIT_ANTIGO: a edição pendente aplicada depois de 1 h só com via
      CURADORIA + prova no banco + edit_date recente; o resto continua
      barrado
  64  leitura parcial do Nerd não tira o produto nem dá data nova a ele
  65  update atrasado de versão velha do Nerd é ignorado
Versão do Nerd = edit_date + assinatura do conteúdo:
  66  duas edições diferentes no mesmo segundo são duas versões; a mesma
      versão não é relida; datas por produto; Nerd fora do funil
  67  atrasada no mesmo segundo: a varredura (conteúdo atual do servidor)
      reaplica a certa sem duplicar; a de 1 s antes é ignorada
  68  a varredura acha a edição do mesmo segundo que o handler perdeu
  69  nova tentativa de versão superada no mesmo segundo é ignorada
  70  reavaliação de robustez: memória sem vazamento, canal de aval não
      resolvido é erro, boot cobre a janela na rajada
Pendências finais:
  71  o aval do Fada é lido do post original sem afiliar: a afiliação do
      post do Fada falhar não tira o aval; Fada dos Cupons não dá aval
  72  toda rodada da varredura cobre a janela inteira (150 no boot, 130
      depois); fora da janela para; teto de segurança tem log
Pós-deploy (04/11):
  73  nomes públicos ativos: username + usernames[*] com active=True,
      minúsculas, sem "@", sem repetição
  74  boot confere GRUPOS_AVAL e GRUPO_ID pelo nome público ativo (o Nerd de
      produção, sem `username`, não dá mais ERRO falso); ID é a autoridade
  75  diagnóstico das releituras do aval: origem, edit_date, assinatura,
      comprimentos e entidades — sem conteúdo nem URL; repetida não loga

Execução (standalone, stdlib + Telethon):  python3 tests/test_curadoria.py
"""
from __future__ import annotations

import ast
import asyncio
import dataclasses
import datetime as dt
import json
import os
import re
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
from database import _init_db, db_exibida, db_get_post, db_ofertas_de_post  # noqa: E402
from database_conexao import _db                               # noqa: E402
import database_curadoria as dbc                               # noqa: E402
from eventos import catalogo                                   # noqa: E402
from pipeline import (convergencia, curadoria, exclusao,  # noqa: E402
                      orchestrator, origem, origem_apagada)
from pipeline import orchestrator_pipeline as op               # noqa: E402
from pipeline.completude import EventoRecuperado               # noqa: E402
from pipeline.identidade import _CHAT_USERNAME                 # noqa: E402
from pipeline.normalizacao import MensagemNormalizada          # noqa: E402

plataformas.inicializar()
_init_db()

# ─────────────────────────────────────────────────────────────────
# Canais: os IDs REAIS das fontes (config.GRUPO_ID, conferidos nos logs de
# boot da Railway). O Nerd ainda não é monitorado: id fixo de teste.
# ─────────────────────────────────────────────────────────────────
_IDS = {n: -(c + 10 ** 12) for n, c in config.GRUPO_ID.items()}
_IDS["nerdofertas"] = 1500000004
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


def mensagem(canal, mid, texto, idade_s=5.0, editada=False, edit_idade_s=0.0):
    m = types.Message(id=mid, peer_id=types.PeerChannel(_IDS[canal]),
                      date=_agora() - dt.timedelta(seconds=idade_s), message=texto,
                      post=True,
                      edit_date=(_agora() - dt.timedelta(seconds=edit_idade_s)
                                 if editada else None))
    m._finish_init(TC, {PEER[canal]: ENT[canal]}, None)
    return m


# ─────────────────────────────────────────────────────────────────
# Identidade por mensagem — a forma que a normalização real entrega
# ─────────────────────────────────────────────────────────────────
NORMS: dict = {}
IDENTS_AVAL: dict = {}


def registrar_norm(canal, mid, texto, idents):
    ids = [i for _, i, _ in idents if i]        # id ausente não é identidade (real)
    NORMS[(CHAT[canal], mid)] = MensagemNormalizada(
        msg_id=mid, chat=CHAT[canal], texto_limpo=texto, texto_analise=texto,
        mapa={"http://o": "http://a"}, preservar=[], plat=idents[0][0] if idents else "",
        sku=ids[0] if ids else "", tem_midia=False, media_obj=None,
        ids_globais=ids, idents=list(idents))


async def _normalizar_falso(bruta):
    n = NORMS.get((bruta.chat, bruta.msg_id))
    # media_obj é a própria mensagem, como na normalização real: é o que a
    # curadoria guarda para soltar na hora.
    return dataclasses.replace(n, media_obj=bruta.media_obj) if n else None


async def _identificar_falso(links):
    """O canal de aval identifica pelos LINKS da mensagem (sem afiliar):
    cada link de teste do Nerd leva às chaves registradas para ele."""
    return sorted({c for u in links for c in IDENTS_AVAL.get(u, [])}), False


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
        db.execute("DELETE FROM curadoria_chave")


def cenario(corpo, modo="ATIVO", identificar_real=False):
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
        orig = (op.normalizar, curadoria._identificar, config.CURADORIA_MODO)
        op.normalizar = _normalizar_falso
        if not identificar_real:
            curadoria._identificar = _identificar_falso
        config.CURADORIA_MODO = modo
        try:
            curadoria.instalar(mundo, FONTES, AVAIS, orchestrator.processar)
            await corpo(mundo)
            await drenar()
        finally:
            await curadoria.encerrar()
            op.normalizar, curadoria._identificar, config.CURADORIA_MODO = orig
        return mundo
    return asyncio.run(_run())


_MID = {"n": 300000}


def novo_id():
    _MID["n"] += 1
    return _MID["n"]


_VERSAO_FADA = {"n": 0}


async def chega(mundo, canal, texto, idents, *, mid=None, idade_s=5.0, editada=False,
                edit_idade_s=0.0, fada_normaliza=True):
    """Uma mensagem da fonte chega como o Telegram a entrega: ao handler de
    FONTE (processar) e, se o canal é avalista lido (Fada Promoções), também
    ao handler de AVAL — que lê o post original sem afiliar. No Fada, o post
    traz um link de teste que leva aos produtos dele (a identificação real
    fica no teste 32). `fada_normaliza=False`: a afiliação/normalização do
    post falha (o post não sai), mas o aval é lido do mesmo jeito."""
    mid = mid or novo_id()
    lido = PEER[canal] in curadoria._estado["lidos"]
    if lido:
        _VERSAO_FADA["n"] += 1
        link = f"https://fada.teste/p/{mid}/{_VERSAO_FADA['n']}"
        IDENTS_AVAL[link] = [f"{p}|{i}" for p, i, t in idents if i and t == "produto"]
        texto = f"{texto}\n\n🛒 {link}"
    if fada_normaliza or not lido:
        registrar_norm(canal, mid, texto, idents)
    else:
        NORMS.pop((CHAT[canal], mid), None)
    m = mensagem(canal, mid, texto, idade_s=idade_s, editada=editada,
                 edit_idade_s=edit_idade_s)
    mundo.servidor[(PEER[canal], mid)] = m
    ev = (tl_events.MessageEdited.Event(m) if editada else tl_events.NewMessage.Event(m))
    await orchestrator.processar(ev, is_edit=editada)
    if lido:
        for cb, b in mundo.handlers:
            if (cb is curadoria._ao_aval and isinstance(b, tl_events.MessageEdited) == editada
                    and PEER[canal] in {utils.get_peer_id(c) for c in b.chats}):
                await cb(ev)
    await drenar()
    return mid


def msg_nerd(mid, texto, idents, idade_s=5.0, link=None):
    """Mensagem do Nerd com um link de teste que leva à identidade dada."""
    link = link or f"https://nerd.teste/p/{mid}"
    IDENTS_AVAL[link] = [f"{p}|{i}" for p, i, _ in idents]
    return mensagem("nerdofertas", mid, f"{texto}\n\n{link}", idade_s=idade_s)


async def nerd(mundo, texto, idents, *, idade_s=5.0, pelo_handler=False, link=None):
    """O Nerd Ofertas posta (só aval: nunca entra no pipeline). A identidade
    sai do LINK da mensagem, como na leitura real."""
    mid = novo_id()
    m = msg_nerd(mid, texto, idents, idade_s=idade_s, link=link)
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
    """O tempo passou: a mensagem da fonte, a linha dela e a entrada de cada
    produto dela têm `segundos`."""
    t = time.time() - segundos
    with _db() as db:
        db.execute("UPDATE curadoria_retida SET ts=?, ts_ref=? WHERE chat=? AND msg_id=?",
                   (t, t, CHAT[canal], mid))
        db.execute("UPDATE curadoria_chave SET ts_entrada=? WHERE chat=? AND msg_id=?",
                   (t, CHAT[canal], mid))
    m = mundo.servidor.get((PEER[canal], mid))
    if m is not None:
        m.date = _agora() - dt.timedelta(seconds=segundos)


def posts(mundo):
    return [p for p, _ in mundo.criados]


def atuais(canal, mid):
    """O conjunto ATUAL de produtos da mensagem no banco da curadoria."""
    with _db() as db:
        return sorted(c for (c,) in db.execute(
            "SELECT chave FROM curadoria_chave WHERE chat=? AND msg_id=? AND atual=1",
            (CHAT[canal], mid)))


def linha(canal, mid):
    with _db() as db:
        return db.execute("SELECT tipo, estado FROM curadoria_retida WHERE chat=? AND msg_id=?",
                          (CHAT[canal], mid)).fetchone()


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
    r.check(s["buscas"] == [], "01.solta_na_hora_sem_consultar_o_telegram",
            str(s["buscas"]))


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
    r.check(len(s["posts"]) == 1 and s["estado"] == "liberada", "03.promotom_passa_direto",
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


def test_07_quem_trava_quem_e_isento_quem_da_aval(r):
    """TODA fonte trava, menos Fada Promoções, Fada dos Cupons e Oferta
    Convertida (pelo ID real). Só o Fada Promoções, entre as fontes, dá
    aval."""
    s = {}

    async def corpo(mundo):
        ids = {}
        for canal in ("SamuelF3lipePromo", "promotom", "fumotom"):
            ids[canal] = await chega(mundo, canal, GPU, amz(f"B0QUEM{canal[:4].upper()}"))
        s["travados"] = {c: retida(c, m) for c, m in ids.items()}
        s["antes"] = list(posts(mundo))
        isentos = {}
        for canal, asin in (("fadadoscupons", "B0ISENCUPO"), ("ofertasconvertidas", "B0ISENCONV"),
                            ("fadapromos", "B0ISENPROM")):
            isentos[canal] = await chega(mundo, canal, GPU, amz(asin))
        s["isentos"] = (len(posts(mundo)), {c: retida(c, m) for c, m in isentos.items()})
        with _db() as db:
            s["aval"] = db.execute("SELECT chave, fonte FROM curadoria_aval").fetchall()
    cenario(corpo)
    r.check(set(s["travados"].values()) == {"retida"} and s["antes"] == [],
            "07.promotom_fumotom_samuel_travam", str(s["travados"]))
    r.check(s["isentos"] == (3, {"fadadoscupons": None, "ofertasconvertidas": None,
                                 "fadapromos": None}), "07.isentos_publicam", str(s["isentos"]))
    r.check(s["aval"] == [("amazon|B0ISENPROM", CHAT["fadapromos"])],
            "07.so_o_fada_promocoes_da_aval", str(s["aval"]))
    r.check(config.CURADORIA_ISENTOS == {config.GRUPO_ID["fadapromos"],
                                         config.GRUPO_ID["fadadoscupons"],
                                         config.GRUPO_ID["ofertasconvertidas"]}
            and config.CURADORIA_AVALISTAS_FONTE == {config.GRUPO_ID["fadapromos"]}
            and config.GRUPOS_AVAL == ["nerdofertas"]
            and "nerdofertas" not in config.GRUPOS_ORIGEM
            and set(config.GRUPO_ID) == set(config.GRUPOS_ORIGEM), "07.config")


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
    """A fonte apagou a segurada: nada vai ao canal — pelo aviso de exclusão
    (origem_apagada, caminho real) e, sem a cópia na memória (reinício),
    pela busca na fonte."""
    s = {}

    async def corpo(mundo):
        a = "B0APAGOU01"
        p = await chega(mundo, "fumotom", GPU, amz(a))
        await origem_apagada.apagadas(CHAT["fumotom"], [p])          # o aviso do Telegram
        await chega(mundo, "fadapromos", GPU_FADA, amz(a))
        s["aviso"] = (len(mundo.criados), retida("fumotom", p))
        b = "B0APAGOU02"
        q = await chega(mundo, "promotom", GPU, amz(b))
        curadoria._SEGURADAS.clear()                                 # reinício: memória vazia
        del mundo.servidor[(PEER["promotom"], q)]                    # e a fonte não tem mais
        await chega(mundo, "fadapromos", GPU_FADA, amz(b))
        s["busca"] = (len(mundo.criados), retida("promotom", q), list(mundo.buscas))
    cenario(corpo)
    r.check(s["aviso"] == (1, "ausente"), "10.apagada_pelo_aviso", str(s["aviso"]))
    r.check(s["busca"][:2] == (2, "ausente") and len(s["busca"][2]) == 1,
            "10.apagada_pela_busca_na_fonte", str(s["busca"]))


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
            db.execute("INSERT OR REPLACE INTO curadoria_retida(chat,msg_id,tipo,categoria,"
                       "ts,ts_ref,estado,ts_estado) VALUES(?,?,?,?,?,?,?,?)",
                       (CHAT[canal], mid, "nova", "placa_de_video", time.time() - 200,
                        time.time() - 200, "liberando", time.time() - ha_s))

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
            res["portao"] = dbc.db_curadoria_avaliar("-1001", 50000 + i, dbc.NOVA, [chave],
                                                     "ssd", base, base, 300, 120,
                                                     time.time())[0]

        def aval():
            barreira.wait()
            res["aval"] = dbc.db_curadoria_registrar_aval([chave], "-1002", 60000 + i, base,
                                                          300, base - 420, time.time())
        ts = [threading.Thread(target=segura), threading.Thread(target=aval)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        solta = any(m == 50000 + i for _, m, _, _ in (res["aval"] or []))
        if not (res["portao"] == dbc.AVAL or solta):
            perdidos.append((i, res))
    r.check(perdidos == [], "13.nenhuma_segurada_perde_o_aval", str(perdidos[:3]))
    # as duas ordens, deterministicas
    _limpar_curadoria()
    av = lambda mid, ch, ts, **k: dbc.db_curadoria_avaliar(  # noqa: E731
        "-1001", mid, dbc.NOVA, ch, "ssd", ts, ts, 300, 120, base, **k)
    r.check(av(1, ["k|1"], base) == (dbc.SEGURADA, True)
            and dbc.db_curadoria_registrar_aval(["k|1"], "-1002", 2, base + 10, 300,
                                                base - 420, base) == [("-1001", 1, "ssd", "nova")]
            and av(1, ["k|1"], base, reinjetada=True) == (dbc.LIBERACAO, False),
            "13.segura_depois_aval")
    r.check(dbc.db_curadoria_registrar_aval(["k|2"], "-1002", 3, base, 300, base - 420, base)
            == [] and av(4, ["k|2"], base + 10) == (dbc.AVAL, False), "13.aval_depois_segura")


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
        velho = msg_nerd(700001, GPU_NERD, amz("B0VARRE001"), idade_s=3600)
        recente = msg_nerd(700002, GPU_NERD, amz("B0VARRE002"), idade_s=30)
        mundo.historico[peer] = [velho, recente]
        chamadas = []
        real = curadoria._observar_aval

        async def conta(chat_id, m, **kw):
            chamadas.append(m.id)
            await real(chat_id, m, **kw)
        curadoria._observar_aval = conta
        try:
            await curadoria._varrer_canal(peer, ent)
            novo = msg_nerd(700003, GPU_NERD, amz("B0VARRE003"), idade_s=5)
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
    r.check([v[1:] for v in s["varreduras"]] == [(curadoria._LOTE, None, None)] * 2,
            "16.sempre_a_pagina_mais_recente", str(s["varreduras"]))
    r.check(s["posts"] == [], "16.nada_do_nerd_publicado")
    r.check(s["chamadas"].count(700003) == 2, "16.handler_e_varredura_trazem_a_mesma",
            str(s["chamadas"]))


def test_17_falha_de_banco_e_natureza(r):
    s = {}

    async def corpo(mundo):
        real, repor = curadoria.db_curadoria_avaliar, curadoria._marcar_repor
        curadoria.db_curadoria_avaliar = lambda *a, **k: (dbc.ERRO, False)
        curadoria._marcar_repor = lambda: None        # a volta do banco é o teste 45
        try:
            p = await chega(mundo, "fumotom", GPU, amz("B0FALHA001"))
            await chega(mundo, "fumotom", "Fritadeira Air Fryer Mondial 4L\n\nR$ 199",
                        amz("B0FALHA002"))                     # outra categoria
            s["posts"] = (len(posts(mundo)), (CHAT["fumotom"], p) in curadoria._PENDENTES)
        finally:
            curadoria.db_curadoria_avaliar, curadoria._marcar_repor = real, repor
    cenario(corpo)
    r.check(s["posts"] == (1, True), "17.banco_fora_bloqueia_so_o_hardware", str(s["posts"]))

    class Enr:
        tipo = "cupom"
    n = MensagemNormalizada(msg_id=1, chat=CHAT["fumotom"],
                            texto_limpo="Cupom KaBuM em Placas de Vídeo 10% OFF",
                            texto_analise="Cupom KaBuM em Placas de Vídeo 10% OFF",
                            mapa={}, preservar=[], plat="", sku="", tem_midia=False,
                            media_obj=None)
    r.check(curadoria.portao(n, Enr(), False) is True, "17.cupom_nao_e_curado")


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
    i_sa = corpo.index("fontes = curadoria.fontes_sem_aval(fontes, avais)")
    r.check(i_av < i_sa < i_hd, "19.aval_sai_das_fontes_antes_dos_handlers", str((i_av, i_sa, i_hd)))
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


def test_25_frase_de_destaque_no_texto_da_mensagem_passa(r):
    """A frase de destaque vale em QUALQUER ponto do texto da PRÓPRIA
    mensagem. Comentário/resposta (outra mensagem) não entra na conta;
    "Esgotou" não é destaque."""
    s = {}

    async def corpo(mundo):
        await chega(mundo, "promotom", "Ainda ativo! Monitor Gamer Philips Evnia 23.8\" 144Hz "
                    "0, 5Ms 24M2n3200nf\n\nR$ 449", amz("B0DEST0001"))          # Promotom 112445
        await chega(mundo, "SamuelF3lipePromo", "-Ainda ativo, bom preço em 12x sem juros\n"
                    "🔥 Monitor Gamer AOC AGON G42 27\" 200Hz 0,3ms\n\n💵 R$ 599",
                    amz("B0DEST0002"))                                           # Samuel 124721
        await chega(mundo, "fumotom", "Preção! Placa de Vídeo RTX 5060 8GB\n\nR$ 1899",
                    amz("B0DEST0003"))
        await chega(mundo, "fumotom", "Placa de Vídeo RTX 5060 8GB\n\nR$ 1899\n-Vai esgotar!",
                    amz("B0DEST0004"))                                           # no corpo
        s["passaram"] = len(posts(mundo))
        b = await chega(mundo, "promotom", "Esgotou! Placa de Vídeo RTX 5070\n\nR$ 3299",
                        amz("B0DEST0005"))
        c = await chega(mundo, "promotom", GPU, amz("B0DEST0006"))
        # o comentário "Precinho!" é OUTRA mensagem (sem link): não solta nada
        coment = novo_id()
        m = mensagem("promotom", coment, "Precinho!")
        mundo.servidor[(PEER["promotom"], coment)] = m
        await orchestrator.processar(tl_events.NewMessage.Event(m))
        await drenar()
        s["presos"] = (len(posts(mundo)), retida("promotom", b), retida("promotom", c))
    cenario(corpo)
    r.check(s["passaram"] == 4, "25.frase_no_titulo_cabecalho_ou_corpo_passa", str(s["passaram"]))
    r.check(s["presos"] == (4, "retida", "retida"), "25.esgotou_e_comentario_nao_soltam",
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


# ══════════════════════════════════════════════════════════════════
# BATERIA DA AUDITORIA FINAL (item 8 do fechamento, 03/11/2026)
# ══════════════════════════════════════════════════════════════════
def extra(chave, cid, user):
    """Um canal a mais (fonte nova, impostor) com id e @ escolhidos."""
    _IDS[chave] = cid
    ENT[chave] = _canal(cid, user)
    PEER[chave] = utils.get_peer_id(ENT[chave])
    CHAT[chave] = str(PEER[chave])
    _CHAT_USERNAME[PEER[chave]] = user.lower()


class _Logs:
    """Captura o que a curadoria escreve no log (forense)."""

    def __init__(self):
        import logging
        from logger import log_ded
        self.linhas, self._log = [], log_ded
        self._h = logging.Handler()
        self._h.emit = lambda rec: self.linhas.append(rec.getMessage())

    def __enter__(self):
        self._log.addHandler(self._h)
        return self

    def __exit__(self, *a):
        self._log.removeHandler(self._h)

    def tem(self, trecho):
        return [x for x in self.linhas if trecho in x]


def test_28_fontes_pelo_id_real(r):
    """Cada papel sai do ID real (config.GRUPO_ID), nunca do @: um canal com
    o @ do Fada e outro ID TRAVA; fonte nova (fora da lista) TRAVA."""
    papeis = {n: curadoria._papel(c) for n, c in config.GRUPO_ID.items()}
    r.check(papeis == {"promotom": "RETÉM", "fumotom": "RETÉM", "SamuelF3lipePromo": "RETÉM",
                       "fadapromos": "ISENTA+AVAL", "fadadoscupons": "ISENTA",
                       "ofertasconvertidas": "ISENTA"}, "28.papel_de_cada_fonte", str(papeis))
    r.check(config.GRUPO_ID == {"promotom": -1001825680721, "fumotom": -1003775401737,
                                "SamuelF3lipePromo": -1001768101197,
                                "fadapromos": -1002389711530, "fadadoscupons": -1002050488946,
                                "ofertasconvertidas": -1003817694320},
            "28.ids_reais_dos_logs_de_boot")
    extra("impostor", 1999000001, "fadapromos")
    extra("novafonte", 1999000002, "novafonte")
    s = {}

    async def corpo(mundo):
        i = await chega(mundo, "impostor", GPU, amz("B0IMPOST01"))
        n = await chega(mundo, "novafonte", GPU, amz("B0NOVAFT01"))
        s["x"] = (len(posts(mundo)), retida("impostor", i), retida("novafonte", n))
        with _db() as db:
            s["aval"] = db.execute("SELECT COUNT(*) FROM curadoria_aval").fetchone()[0]
    cenario(corpo)
    r.check(s["x"] == (0, "retida", "retida") and s["aval"] == 0,
            "28.impostor_e_fonte_nova_travam_e_nao_dao_aval", str((s["x"], s["aval"])))

    # instalar confere ID × @ e avisa quando não batem (vale o ID)
    async def confere():
        g._init_globals()
        trocado = _canal(_IDS["fadapromos"], "outronome")
        with _Logs() as logs:
            curadoria.instalar(Mundo(), [trocado], [], orchestrator.processar)
            await curadoria.encerrar()
        return logs
    logs = asyncio.run(confere())
    r.check(logs.tem("responde como @outronome — vale o ID")
            and logs.tem("promotom (-1001825680721) não está entre as fontes"),
            "28.instalar_confere_id_e_arroba", str(logs.linhas)[:300])


def test_29_so_fada_promocoes_e_nerd_dao_aval(r):
    """Aval só nasce do Fada Promoções (pelo portão) e do Nerd (canal de
    aval). Fada dos Cupons, Oferta Convertida, retentoras e quem passou por
    exceção (destaque, lista, retorno) nunca gravam aval."""
    s = {}

    async def corpo(mundo):
        a = "B0QUEMAVAL"
        await chega(mundo, "fadadoscupons", GPU, amz(a))
        await chega(mundo, "ofertasconvertidas", GPU, amz(a))
        await chega(mundo, "promotom", "Ainda ativo! " + GPU, amz(a))
        await chega(mundo, "fumotom", "Voltando! " + GPU, amz(a))
        await chega(mundo, "SamuelF3lipePromo",
                    "🔥 Placas de vídeo\n\n🔹 RTX 5060 Ti Gigabyte - R$ 2299\nhttps://x/1\n\n"
                    "🔹 RX 9060 XT Sapphire - R$ 2499\nhttps://x/2",
                    amz(a) + amz("B0QUEMAVA2"))
        await chega(mundo, "promotom", GPU, amz(a))                  # retentora: trava
        with _db() as db:
            s["antes"] = db.execute("SELECT COUNT(*) FROM curadoria_aval").fetchone()[0]
        await chega(mundo, "fadapromos", GPU_FADA, amz(a))
        await nerd(mundo, GPU_NERD, amz(a))
        with _db() as db:
            s["depois"] = sorted(f for (f,) in db.execute("SELECT fonte FROM curadoria_aval"))
    cenario(corpo)
    r.check(s["antes"] == 0, "29.ninguem_mais_da_aval", str(s["antes"]))
    r.check(s["depois"] == sorted([CHAT["fadapromos"], str(PEER["nerdofertas"])]),
            "29.so_fada_promocoes_e_nerd", str(s["depois"]))


def test_30_nerd_nunca_publica(r):
    """O Nerd é só sinal: sai da lista de fontes antes dos handlers; tem um
    handler próprio que só observa; se ainda assim uma mensagem dele cair
    no funil, o portão a barra; o que o aval solta é SEMPRE a mensagem da
    fonte — nunca a do Nerd."""
    r.check(curadoria.fontes_sem_aval(FONTES + AVAIS, AVAIS) == FONTES,
            "30.sai_das_fontes")
    s = {}

    async def corpo(mundo):
        despachados = []
        real = curadoria._estado["despachar"]

        async def espiao(ev, is_edit=False):
            despachados.append((str(ev.chat_id), ev.message.id))
            await real(ev, is_edit=is_edit)
        curadoria._estado["despachar"] = espiao
        s["handlers"] = [(cb.__name__, type(ev).__name__,
                          sorted(utils.get_peer_id(c) for c in ev.chats))
                         for cb, ev in mundo.handlers]
        await nerd(mundo, GPU_NERD, amz("B0NERDSO01"), pelo_handler=True)   # nada segurado
        s["sem_segurada"] = (list(despachados), list(posts(mundo)))
        p = await chega(mundo, "promotom", GPU, amz("B0NERDSO02"))
        await nerd(mundo, GPU_NERD, amz("B0NERDSO02"), pelo_handler=True)
        s["solta"] = (list(despachados), len(posts(mundo)), p)
        s["texto"] = mundo.criados[-1][1] if mundo.criados else ""
        # configuração errada: mensagem do Nerd no funil de publicação
        mid = novo_id()
        registrar_norm("nerdofertas", mid, GPU_NERD, amz("B0NERDSO03"))
        m = mensagem("nerdofertas", mid, GPU_NERD)
        await orchestrator.processar(tl_events.NewMessage.Event(m))
        await drenar()
        s["funil"] = (len(posts(mundo)), vinculo("nerdofertas", mid))
    cenario(corpo)
    lidos = sorted([PEER["nerdofertas"], PEER["fadapromos"]])
    r.check(s["handlers"] == [("_ao_aval", "NewMessage", lidos),
                              ("_ao_aval", "MessageEdited", lidos)],
            "30.handlers_so_observam_nova_e_edicao", str(s["handlers"]))
    r.check(s["sem_segurada"] == ([], []), "30.nerd_sozinho_nao_despacha_nada",
            str(s["sem_segurada"]))
    r.check(s["solta"][0] == [(CHAT["promotom"], s["solta"][2])] and s["solta"][1] == 1
            and "😱" not in s["texto"], "30.solta_a_da_fonte_nunca_a_do_nerd", str(s["solta"]))
    r.check(s["funil"] == (1, None), "30.portao_barra_o_nerd_no_funil", str(s["funil"]))


def test_31_mesma_identidade_e_mesma_loja(r):
    """Mesmo número em outra loja não é o mesmo produto; loja fora de
    ASIN/MLB/Shopee não dá aval; link sem produto não dá aval; link de
    campanha/busca não é produto (não trava nem vira aval)."""
    s = {}

    async def corpo(mundo):
        p = await chega(mundo, "promotom", GPU, [("amazon", "B0LOJA0001", "produto")])
        await nerd(mundo, GPU_NERD, [("shopee", "B0LOJA0001", "produto")])
        await chega(mundo, "fadapromos", GPU_FADA, [("mercadolivre", "B0LOJA0001", "produto")])
        s["outra_loja"] = (retida("promotom", p), len(posts(mundo)))
        m = await chega(mundo, "fumotom", GPU, [("magalu", "abc123def", "produto")])
        with _Logs() as logs:
            await nerd(mundo, GPU_NERD, [("magalu", "abc123def", "produto")])
            await chega(mundo, "fadapromos", GPU_FADA, [("magalu", "abc123def", "produto")])
        s["magalu"] = (retida("fumotom", m), len(logs.tem("[CURADORIA_AVAL_SEM_LOJA]")))
        q = await chega(mundo, "SamuelF3lipePromo", GPU, amz("B0LOJA0002"))
        with _Logs() as logs:
            await nerd(mundo, GPU_NERD, [])                          # link sem produto
        s["sem_produto"] = (retida("SamuelF3lipePromo", q),
                            len(logs.tem("[CURADORIA_AVAL_SEM_PRODUTO]")))
        n0 = len(posts(mundo))
        c = await chega(mundo, "promotom", GPU, [("amazon", "PROMO123", "campanha")])
        s["campanha"] = (len(posts(mundo)) - n0, retida("promotom", c))
        with _db() as db:
            s["aval"] = sorted(k for (k,) in db.execute("SELECT chave FROM curadoria_aval"))
    cenario(corpo)
    r.check(s["outra_loja"] == ("retida", 1), "31.mesmo_id_outra_loja_nao_solta",
            str(s["outra_loja"]))
    r.check(s["magalu"] == ("retida", 2), "31.loja_sem_identidade_exata_nao_da_aval",
            str(s["magalu"]))
    r.check(s["sem_produto"] == ("retida", 1), "31.link_sem_produto_nao_da_aval",
            str(s["sem_produto"]))
    r.check(s["campanha"] == (0, "retida"), "31.campanha_nao_e_identidade_fica_retida",
            str(s["campanha"]))
    r.check(s["aval"] == ["mercadolivre|B0LOJA0001", "shopee|B0LOJA0001"], "31.avais_gravados",
            str(s["aval"]))


def test_32_nerd_identifica_sem_afiliar(r):
    """A identificação REAL do Nerd (link.amazon, s.shopee, meli.la e
    encurtador genérico): só expansão + extração de identidade. Nenhum
    `afilia` (SiteStripe/createLink), nenhum cache de afiliação, nenhuma
    linha nova em links_cache/short_links; o rodapé nerdofertas.com nem é
    aberto. A busca única do ML (P2-A) é fronteira de rede como o
    expansor: vê o MESMO mundo falso e só toca o link do ML."""
    from plataformas import registry
    from plataformas.mercadolivre import afiliacao as ml_afil, descoberta as ml_desc
    import utils.cache_links as cache_links
    destinos = {
        "https://link.amazon/B0nerd1": "https://www.amazon.com.br/dp/B0REALAMZ1?ref_=x",
        "https://s.shopee.com.br/AbCdE": "https://shopee.com.br/Placa-de-Video-i.111.222",
        "https://meli.la/1XyZw": "https://produto.mercadolivre.com.br/MLB-3000000001-placa-_JM",
        "https://cutt.ly/rtx5060": "https://link.amazon/B0nerd2",
        "https://link.amazon/B0nerd2": "https://www.amazon.com.br/dp/B0REALAMZ2",
    }
    abertos, proibidos, buscados = [], [], []

    async def desencurtar_falso(url, sessao, depth=0):
        abertos.append(url)
        return destinos.get(url, url)

    async def buscar_falso(url, sessao):
        buscados.append(url)
        return ml_desc.Busca(final=destinos.get(url, url))

    def proibido(nome):
        async def _a(*a, **k):
            proibidos.append(nome)
            raise AssertionError(nome)

        def _s(*a, **k):
            proibidos.append(nome)
            raise AssertionError(nome)
        return _a, _s
    s = {}
    texto = ("🚨 Produtos Tech para usar o cupom meia noite\n\n"
             "➡️ Placa de Vídeo RTX 5060 Ti 8GB\n✅ R$ 2.299\n🛒 https://link.amazon/B0nerd1\n\n"
             "➡️ Ryzen 7 5700x OEM\n✅ R$ 767\n🛒 https://s.shopee.com.br/AbCdE\n\n"
             "➡️ SSD 2TB PNY CS900\n✅ R$ 803\n🛒 https://meli.la/1XyZw\n\n"
             "➡️ RX 7600 Challenger ASRock 8GB\n✅ R$ 1.734\n🛒 https://cutt.ly/rtx5060\n\n"
             "🚀 Mais grupos de ofertas e cupons: https://nerdofertas.com/t")

    def linhas_cache():
        with _db() as db:
            return tuple(db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                         for t in ("links_cache", "short_links"))

    async def corpo(mundo):
        ids = {"promotom": await chega(mundo, "promotom", GPU, amz("B0REALAMZ1")),
               "fumotom": await chega(mundo, "fumotom", "Processador AMD Ryzen 7 5700X\n\nR$ 767",
                                      [("shopee", "111.222", "produto")]),
               "SamuelF3lipePromo": await chega(mundo, "SamuelF3lipePromo",
                                                "SSD PNY CS900 2TB SATA\n\nR$ 803",
                                                [("mercadolivre", "MLB3000000001", "produto")])}
        q = await chega(mundo, "promotom", "Placa de Vídeo RX 7600 Challenger 8GB\n\nR$ 1734",
                        amz("B0REALAMZ2"))
        antes = linhas_cache()
        orig_cat = dict(registry._catalogo)
        orig = (curadoria.desencurtar, ml_afil.desencurtar, g._set_final,
                ml_afil.registrar_link, ml_afil.consultar_link, cache_links.registrar_link,
                ml_desc.buscar)
        try:
            for nome, plat in orig_cat.items():
                registry._catalogo[nome] = dataclasses.replace(
                    plat, afilia=proibido(f"afilia:{nome}")[0])
            curadoria.desencurtar = ml_afil.desencurtar = desencurtar_falso
            ml_desc.buscar = buscar_falso
            g._set_final = proibido("_set_final")[1]
            ml_afil.registrar_link = proibido("registrar_link")[0]
            ml_afil.consultar_link = proibido("consultar_link")[0]
            cache_links.registrar_link = proibido("cache_links.registrar_link")[0]
            mid = novo_id()
            m = mensagem("nerdofertas", mid, texto)
            await curadoria._observar_aval(PEER["nerdofertas"], m)
            await drenar()
        finally:
            registry._catalogo.clear()
            registry._catalogo.update(orig_cat)
            (curadoria.desencurtar, ml_afil.desencurtar, g._set_final,
             ml_afil.registrar_link, ml_afil.consultar_link, cache_links.registrar_link,
             ml_desc.buscar) = orig
        s["estados"] = {c: retida(c, i) for c, i in ids.items()} | {"rx": retida("promotom", q)}
        s["cache"] = (antes, linhas_cache())
        with _db() as db:
            s["aval"] = sorted(k for (k,) in db.execute("SELECT chave FROM curadoria_aval"))
    cenario(corpo, identificar_real=True)
    r.check(proibidos == [], "32.nenhuma_afiliacao_nem_cache", str(proibidos))
    r.check(s["aval"] == ["amazon|B0REALAMZ1", "amazon|B0REALAMZ2",
                          "mercadolivre|MLB3000000001", "shopee|111.222"],
            "32.identidade_exata_de_cada_link", str(s["aval"]))
    r.check(set(s["estados"].values()) == {"liberada"}, "32.as_quatro_soltam",
            str(s["estados"]))
    r.check(s["cache"][0] == s["cache"][1], "32.links_cache_e_short_links_intactos",
            str(s["cache"]))
    r.check("https://nerdofertas.com/t" not in abertos and len(abertos) == 5,
            "32.so_abre_os_links_de_produto", str(abertos))
    r.check(buscados == ["https://meli.la/1XyZw"], "32.busca_unica_so_no_link_do_ml",
            str(buscados))


def test_33_janela_de_300s_nos_dois_sentidos(r):
    """300 s pelas DATAS ORIGINAIS das mensagens, nos dois sentidos — no
    banco (limite exato) e no caminho real (299 passa, 301 não)."""
    _limpar_curadoria()
    b = time.time()
    av = lambda mid, ch, ts: dbc.db_curadoria_avaliar(  # noqa: E731
        "-1001", mid, dbc.NOVA, ch, "ssd", ts, ts, 300, 120, b)[0]
    r.check(dbc.db_curadoria_registrar_aval(["k|A"], "-1002", 1, b, 300, b - 420, b) == []
            and av(2, ["k|A"], b + 300) == dbc.AVAL and av(3, ["k|A"], b - 300) == dbc.AVAL
            and av(4, ["k|A"], b + 300.5) == dbc.SEGURADA, "33.banco_aval_antes_ate_300_exatos")
    _limpar_curadoria()
    av(5, ["k|B"], b)
    av(6, ["k|B"], b - 1)
    r.check(dbc.db_curadoria_registrar_aval(["k|B"], "-1002", 7, b + 300, 300, b - 420, b)
            == [("-1001", 5, "ssd", "nova")], "33.banco_aval_depois_ate_300_exatos")
    s = {}

    async def corpo(mundo):
        # aval ANTES: o Nerd de 304 s e a fonte de 5 s (Δ 299) → passa
        await nerd(mundo, GPU_NERD, amz("B0JAN00001"), idade_s=304)
        a = await chega(mundo, "promotom", GPU, amz("B0JAN00001"))
        await nerd(mundo, GPU_NERD, amz("B0JAN00002"), idade_s=306)
        b2 = await chega(mundo, "promotom", GPU, amz("B0JAN00002"))
        s["antes"] = (retida("promotom", a), retida("promotom", b2))
        # aval DEPOIS: a fonte com 304 s e o Nerd de agora (Δ 299) → solta
        c = await chega(mundo, "fumotom", GPU, amz("B0JAN00003"))
        envelhecer(mundo, "fumotom", c, 304)
        await nerd(mundo, GPU_NERD, amz("B0JAN00003"))
        d = await chega(mundo, "fumotom", GPU, amz("B0JAN00004"))
        envelhecer(mundo, "fumotom", d, 306)
        await nerd(mundo, GPU_NERD, amz("B0JAN00004"))
        s["depois"] = (retida("fumotom", c), retida("fumotom", d))
        # Fada ANTES (a data do aval é a da mensagem do Fada)
        await chega(mundo, "fadapromos", GPU_FADA, amz("B0JAN00005"))
        await chega(mundo, "fadapromos", GPU_FADA, amz("B0JAN00006"))
        with _db() as db:
            db.execute("UPDATE curadoria_aval SET ts=? WHERE chave=?",
                       (time.time() - 304, "amazon|B0JAN00005"))
            db.execute("UPDATE curadoria_aval SET ts=? WHERE chave=?",
                       (time.time() - 306, "amazon|B0JAN00006"))
        e = await chega(mundo, "SamuelF3lipePromo", GPU, amz("B0JAN00005"))
        f = await chega(mundo, "SamuelF3lipePromo", GPU, amz("B0JAN00006"))
        s["fada"] = (retida("SamuelF3lipePromo", e), retida("SamuelF3lipePromo", f))
    cenario(corpo)
    r.check(s["antes"] == ("liberada", "retida"), "33.aval_antes_299_passa_301_trava",
            str(s["antes"]))
    r.check(s["depois"] == ("liberada", "retida"), "33.aval_depois_299_solta_301_nao",
            str(s["depois"]))
    r.check(s["fada"] == ("liberada", "retida"), "33.fada_antes_299_passa_301_trava",
            str(s["fada"]))
    r.check(config.CURADORIA_JANELA_S == 300, "33.janela_e_300_s")


def test_34_aval_visto_atrasado_nao_expira_antes(r):
    """O aval válido pela DATA (Δ <= 300 s) que só é VISTO depois — update
    perdido, varredura de 15 s, reinício curto — ainda solta (folga
    _ATRASO_MAX_S); a expiração nunca roda na frente da leitura do aval."""
    s = {}

    async def corpo(mundo):
        a = await chega(mundo, "promotom", GPU, amz("B0ATRASO01"))
        envelhecer(mundo, "promotom", a, 380)     # a fonte tem 380 s
        curadoria._expirar()                       # a varredura expira antes de ler…
        s["antes"] = retida("promotom", a)
        await nerd(mundo, GPU_NERD, amz("B0ATRASO01"), idade_s=180)   # Nerd de 200 s depois
        s["solta"] = (retida("promotom", a), len(posts(mundo)))
        # passou de 300 + folga: aí expira (o aval tardio não solta mais)
        b = await chega(mundo, "promotom", GPU, amz("B0ATRASO02"))
        envelhecer(mundo, "promotom", b, 425)
        curadoria._expirar()
        await nerd(mundo, GPU_NERD, amz("B0ATRASO02"), idade_s=130)
        s["tarde"] = (retida("promotom", b), len(posts(mundo)))
        # a ordem da rodada: LÊ o canal de aval, DEPOIS expira
        ordem = []
        reais = (curadoria._varrer_canal, curadoria._expirar, curadoria._VARREDURA_S)

        async def ler(chat_id, ent):
            ordem.append("ler")

        def expirar():
            ordem.append("expirar")
            curadoria._aberto = False
        curadoria._varrer_canal, curadoria._expirar, curadoria._VARREDURA_S = ler, expirar, 0
        try:
            await curadoria._varrer()
        finally:
            curadoria._varrer_canal, curadoria._expirar, curadoria._VARREDURA_S = reais
            curadoria._aberto = True
        s["ordem"] = ordem
    cenario(corpo)
    r.check(s["antes"] == "retida" and s["solta"] == ("liberada", 1),
            "34.aval_visto_180s_depois_ainda_solta", str((s["antes"], s["solta"])))
    r.check(s["tarde"] == ("expirada", 1), "34.depois_da_folga_expira", str(s["tarde"]))
    r.check(s["ordem"] == ["ler", "ler", "expirar"], "34.le_os_avalistas_antes_de_expirar",
            str(s["ordem"]))
    r.check(curadoria._ATRASO_MAX_S == 120 and curadoria._VARREDURA_S == 15,
            "34.folga_120s_cobre_a_varredura_de_15s")


def test_35_reprocessamento_nao_duplica(r):
    """O Telegram reentrega a mensagem segurada; a recuperação a reinjeta;
    dois avais chegam para ela; a fonte reentrega depois de solta — o canal
    tem UM post e o ponto de entrada recebe a soltura UMA vez."""
    s = {}

    async def corpo(mundo):
        despachados = []
        real = curadoria._estado["despachar"]

        async def espiao(ev, is_edit=False):
            despachados.append(ev.message.id)
            await real(ev, is_edit=is_edit)
        curadoria._estado["despachar"] = espiao
        a = "B0REPROC01"
        p = await chega(mundo, "fumotom", GPU, amz(a))
        m = mundo.servidor[(PEER["fumotom"], p)]
        await orchestrator.processar(tl_events.NewMessage.Event(m))           # reentrega
        await orchestrator.processar(EventoRecuperado(m, via="RECUPERACAO"))  # recuperação
        await drenar()
        with _db() as db:
            s["linhas"] = db.execute("SELECT COUNT(*) FROM curadoria_retida WHERE msg_id=?",
                                     (p,)).fetchone()[0]
        s["presa"] = (retida("fumotom", p), len(posts(mundo)))
        await nerd(mundo, GPU_NERD, amz(a))
        await chega(mundo, "fadapromos", GPU_FADA, amz(a))                    # 2º aval
        await orchestrator.processar(tl_events.NewMessage.Event(m))           # reentrega
        await drenar()
        s["final"] = (retida("fumotom", p), len(posts(mundo)), despachados.count(p))
    cenario(corpo)
    r.check(s["linhas"] == 1 and s["presa"] == ("retida", 0), "35.reentrega_continua_segurada",
            str((s["linhas"], s["presa"])))
    r.check(s["final"] == ("liberada", 1, 1), "35.um_post_e_uma_soltura", str(s["final"]))


def test_36_edicao_com_excecao_fecha_a_segurada(r):
    """A fonte edita a segurada e põe "Ainda ativo!": passa pela exceção,
    sai UMA vez, a linha fecha (liberada) e um aval posterior não a solta
    de novo."""
    s = {}

    async def corpo(mundo):
        a = "B0EDEXC001"
        p = await chega(mundo, "promotom", GPU, amz(a))
        await chega(mundo, "promotom", "Ainda ativo! " + GPU, amz(a), mid=p, idade_s=40,
                    editada=True)
        s["edit"] = (len(posts(mundo)), retida("promotom", p))
        await nerd(mundo, GPU_NERD, amz(a))
        s["aval"] = (len(posts(mundo)), retida("promotom", p), (p,) not in
                     [(k[1],) for k in curadoria._SEGURADAS])
    cenario(corpo)
    r.check(s["edit"] == (1, "liberada"), "36.edicao_com_destaque_sai_e_fecha", str(s["edit"]))
    r.check(s["aval"] == (1, "liberada", True), "36.aval_depois_nao_duplica", str(s["aval"]))


def test_37_falhas_de_leitura(r):
    """Busca na fonte (depois de reinício) e identificação do Nerd com
    falha transitória: novas tentativas; falha persistente fecha sem
    inventar nada."""
    s = {}

    async def corpo(mundo):
        reais = (curadoria._BUSCA_REPETIR_S, curadoria._REPETIR_S)
        curadoria._BUSCA_REPETIR_S, curadoria._REPETIR_S = (0.0, 0.0), (0.01, 0.01)
        get_real = mundo.get_messages
        try:
            # busca: falha 1x, depois funciona → solta
            falhas = {"n": 1}

            async def instavel(ent, ids=None, **k):
                if ids is not None and falhas["n"] > 0:
                    falhas["n"] -= 1
                    raise ConnectionError("rede")
                return await get_real(ent, ids=ids, **k)
            mundo.get_messages = instavel
            p = await chega(mundo, "promotom", GPU, amz("B0LEITU001"))
            curadoria._SEGURADAS.clear()                       # reinício
            await nerd(mundo, GPU_NERD, amz("B0LEITU001"))
            s["busca_volta"] = (retida("promotom", p), len(posts(mundo)))
            # busca: falha sempre → falhou, nada sai
            falhas["n"] = 99
            q = await chega(mundo, "promotom", GPU, amz("B0LEITU002"))
            curadoria._SEGURADAS.clear()
            await nerd(mundo, GPU_NERD, amz("B0LEITU002"))
            s["busca_falha"] = (retida("promotom", q), len(posts(mundo)))
            mundo.get_messages = get_real
            # identificação: incerta 1x, depois acha → solta
            tentativas = {"n": 0}

            async def ident(links):
                tentativas["n"] += 1
                if tentativas["n"] == 1:
                    return [], True
                return ["amazon|B0LEITU003"], False
            curadoria._identificar = ident
            t = await chega(mundo, "fumotom", GPU, amz("B0LEITU003"))
            await nerd(mundo, GPU_NERD, [])
            await asyncio.sleep(0.05)
            await drenar()
            s["ident_volta"] = (retida("fumotom", t), tentativas["n"])
            # identificação: incerta sempre → 1 + 2 tentativas e fim
            async def sempre_incerta(links):
                tentativas["n"] += 1
                return [], True
            curadoria._identificar = sempre_incerta
            tentativas["n"] = 0
            u = await chega(mundo, "fumotom", GPU, amz("B0LEITU004"))
            with _Logs() as logs:
                await nerd(mundo, GPU_NERD, [])
                for _ in range(5):
                    await asyncio.sleep(0.03)
                    await drenar()
            s["ident_falha"] = (retida("fumotom", u), tentativas["n"],
                                len(logs.tem("[CURADORIA_AVAL_REPETE]")),
                                len(logs.tem("identificação falhou); não é aval")))
            # exceção no meio da identificação: não derruba nada
            async def explode(links):
                raise RuntimeError("x")
            curadoria._identificar = explode
            await nerd(mundo, GPU_NERD, [])
            s["explode"] = True
        finally:
            curadoria._BUSCA_REPETIR_S, curadoria._REPETIR_S = reais
    cenario(corpo)
    r.check(s["busca_volta"] == ("liberada", 1), "37.busca_tenta_de_novo", str(s["busca_volta"]))
    r.check(s["busca_falha"] == ("falhou", 1), "37.busca_que_nao_volta_fecha_falhou",
            str(s["busca_falha"]))
    r.check(s["ident_volta"] == ("liberada", 2), "37.identificacao_tenta_de_novo",
            str(s["ident_volta"]))
    r.check(s["ident_falha"] == ("retida", 3, 2, 1), "37.identificacao_tem_limite",
            str(s["ident_falha"]))
    r.check(s.get("explode") is True, "37.excecao_contida")


def test_38_relogio_local_atras_do_telegram(r):
    """Relógio do servidor alguns segundos atrás do Telegram (data da
    mensagem "no futuro"): o aval do Nerd vale e a reinjeção passa."""
    s = {}

    async def corpo(mundo):
        await nerd(mundo, GPU_NERD, amz("B0RELOG001"), idade_s=-3)
        a = await chega(mundo, "promotom", GPU, amz("B0RELOG001"))
        b = await chega(mundo, "fumotom", GPU, amz("B0RELOG002"), idade_s=-3)
        await chega(mundo, "fadapromos", GPU_FADA, amz("B0RELOG002"), idade_s=-2)
        s["x"] = (retida("promotom", a), retida("fumotom", b), len(posts(mundo)))
    cenario(corpo)
    r.check(s["x"] == ("liberada", "liberada", 2), "38.data_no_futuro_nao_perde_aval",
            str(s["x"]))


def test_39_soltura_na_hora_com_a_ultima_edicao(r):
    """A soltura usa a mensagem guardada na memória — sem nova consulta ao
    Telegram — e na ÚLTIMA versão (a fonte editou enquanto estava segurada)."""
    s = {}

    async def corpo(mundo):
        a = "B0MEMOR001"
        p = await chega(mundo, "promotom", GPU, amz(a))
        editada = GPU + "\n-CUPOM: PLACA20"
        await chega(mundo, "promotom", editada, amz(a), mid=p, idade_s=30, editada=True)
        mundo.servidor.pop((PEER["promotom"], p))          # a fonte não seria consultada
        await nerd(mundo, GPU_NERD, amz(a))
        s["x"] = (retida("promotom", p), list(mundo.buscas),
                  mundo.criados[-1][1] if mundo.criados else "")
    cenario(corpo)
    r.check(s["x"][0] == "liberada" and s["x"][1] == [], "39.sem_consultar_o_telegram",
            str(s["x"][:2]))
    r.check("PLACA20" in s["x"][2], "39.sai_a_ultima_edicao", s["x"][2][:200])


def test_40_sem_bypass_do_pipeline(r):
    """A curadoria não escreve no canal nem no banco do pipeline: só lê o
    Telegram (get_messages), registra um handler e entrega ao ponto de
    entrada injetado. Banco só pelas funções de database_curadoria."""
    arv = ast.parse(ler_fonte("pipeline/curadoria.py"))
    mods = set()
    for n in ast.walk(arv):
        if isinstance(n, ast.ImportFrom):
            mods.add(n.module or "")
        elif isinstance(n, ast.Import):
            mods |= {a.name for a in n.names}
    proibidos = {m for m in mods if m.split(".")[-1] in {
        "saida", "publicacao", "aplicadores", "montagem", "montagem_imagem", "client",
        "orchestrator", "orchestrator_pipeline", "decisao", "convergencia", "familia",
        "database_conexao", "database_posts", "database_links", "afiliacao"}}
    r.check(proibidos == set(), "40.nao_importa_escrita_do_pipeline", str(proibidos))
    chamadas = {n.func.attr for n in ast.walk(arv)
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    r.check(not chamadas & {"send_message", "send_file", "edit_message", "delete_messages",
                            "forward_messages", "execute", "afilia"},
            "40.sem_envio_sem_sql_sem_afiliar", str(chamadas & {"send_message", "execute"}))
    fonte = ler_fonte("pipeline/curadoria.py")
    r.check("EventoRecuperado(m, via=VIA)" in fonte and fonte.count('_estado["despachar"](') == 1,
            "40.uma_unica_entrega_ao_ponto_de_entrada")


def test_41_lista_do_nerd_com_cabecalho_da_aval_a_cada_produto(r):
    """Post do Nerd com cabeçalho ("🚨 Produtos Tech…") e vários produtos:
    cada produto identificado é aval (Nerd 147990)."""
    s = {}
    texto = ("🚨 Produtos Tech para usar o cupom meia noite\n\n➡️ Ryzen 7 5700\n✅ R$ 646\n\n"
             "➡️  RX 7600 Challenger ASRock 8GB\n✅ R$ 1.734")

    async def corpo(mundo):
        a = await chega(mundo, "promotom", "Processador AMD Ryzen 7 5700\n\nR$ 646",
                        [("shopee", "1.1", "produto")])
        b = await chega(mundo, "fumotom", "Placa de Vídeo RX 7600 Challenger\n\nR$ 1734",
                        [("shopee", "2.2", "produto")])
        await nerd(mundo, texto, [("shopee", "1.1", "produto"), ("shopee", "2.2", "produto")])
        s["x"] = (retida("promotom", a), retida("fumotom", b))
    cenario(corpo)
    r.check(s["x"] == ("liberada", "liberada"), "41.cada_produto_da_lista", str(s["x"]))


# ══════════════════════════════════════════════════════════════════
# AUDITORIA FORENSE (03/11/2026) — itens 1 a 4
# ══════════════════════════════════════════════════════════════════
DOIS = ("Placa de Vídeo Gigabyte RTX 5060 Ti Eagle OC 8GB\n\nR$ 2299\n\n"
        "1 https://loja.teste/1\n2 https://loja.teste/2")             # 2 anúncios, 1 preço
RX = "Placa de Vídeo RX 9060 XT Sapphire Pulse 16GB GDDR6\n\nR$ 2499"
MOUSE = "Mouse Gamer Logitech G203 Lightsync RGB\n\nR$ 99"


def aval_direto(chave, ts, fonte="-1009"):
    """Um aval visto no passado (gravado como o portão/varredura gravaria)."""
    with _db() as db:
        db.execute("INSERT OR IGNORE INTO curadoria_aval(chave,fonte,msg_id,ts) VALUES(?,?,?,?)",
                   (chave, fonte, novo_id(), ts))


async def publicada(mundo, canal, texto, asin):
    """Mensagem da fonte retentora que já está no canal (aval ANTES)."""
    await nerd(mundo, GPU_NERD, amz(asin), idade_s=30)
    mid = await chega(mundo, canal, texto, amz(asin))
    return mid, vinculo(canal, mid)


def test_42_a_ou_b_solta_a_mensagem_inteira(r):
    """Item 1: a mensagem guarda TODOS os produtos da versão atual; aval de
    A ou de B (mesma loja) solta a mensagem inteira, com os dois."""
    s = {}

    async def corpo(mundo):
        p = await chega(mundo, "promotom", DOIS, amz("B0CONJA001") + amz("B0CONJB001"))
        s["conj"] = atuais("promotom", p)
        await nerd(mundo, GPU_NERD, amz("B0CONJA001"))                    # só A
        s["a"] = (retida("promotom", p), len(posts(mundo)),
                  mundo.criados[-1][1] if mundo.criados else "")
        q = await chega(mundo, "fumotom", DOIS, amz("B0CONJA002") + amz("B0CONJB002"))
        await chega(mundo, "fadapromos", GPU_FADA, amz("B0CONJB002"))     # só B (Fada)
        s["b"] = (retida("fumotom", q), vinculo("fumotom", q) is not None)
        await nerd(mundo, GPU_NERD, amz("B0CONJB003"))                    # B ANTES
        t = await chega(mundo, "SamuelF3lipePromo", DOIS,
                        amz("B0CONJA003") + amz("B0CONJB003"))
        s["antes"] = retida("SamuelF3lipePromo", t)
        u = await chega(mundo, "promotom", DOIS, amz("B0CONJA004") + amz("B0CONJB004"))
        await nerd(mundo, GPU_NERD, [("shopee", "B0CONJA004", "produto")])  # outra loja
        s["outra_loja"] = retida("promotom", u)
    cenario(corpo)
    r.check(s["conj"] == ["amazon|B0CONJA001", "amazon|B0CONJB001"], "42.guarda_a_e_b",
            str(s["conj"]))
    r.check(s["a"][:2] == ("liberada", 1) and "loja.teste/2" in s["a"][2],
            "42.aval_de_a_solta_a_mensagem_inteira", str(s["a"])[:200])
    r.check(s["b"] == ("liberada", True), "42.aval_de_b_solta", str(s["b"]))
    r.check(s["antes"] == "liberada", "42.aval_de_b_antes_tambem", str(s["antes"]))
    r.check(s["outra_loja"] == "retida", "42.mesmo_id_em_outra_loja_nao", str(s["outra_loja"]))


def test_43_edicoes_trocam_o_conjunto_atual(r):
    """Item 1: A→A+B guarda os dois (B entra pelo edit_date); A→B guarda só
    B (aval de A não solta mais); A+B→B guarda só B; edições repetidas não
    devolvem o produto tirado nem duplicam a publicação."""
    s = {}

    async def corpo(mundo):
        p = await chega(mundo, "promotom", GPU, amz("B0EDA00001"))
        with _db() as db:
            ta = db.execute("SELECT ts_entrada FROM curadoria_chave WHERE msg_id=?",
                            (p,)).fetchone()[0]
        await chega(mundo, "promotom", DOIS, amz("B0EDA00001") + amz("B0EDB00001"), mid=p,
                    idade_s=40, editada=True)
        with _db() as db:
            ent = dict(db.execute("SELECT chave, ts_entrada FROM curadoria_chave WHERE msg_id=?",
                                  (p,)).fetchall())
        s["mais"] = (atuais("promotom", p), ent["amazon|B0EDA00001"] == ta,
                     abs(ent["amazon|B0EDB00001"] - time.time()) < 5)
        await nerd(mundo, GPU_NERD, amz("B0EDB00001"))
        s["mais_solta"] = retida("promotom", p)

        q = await chega(mundo, "fumotom", GPU, amz("B0EDA00002"))
        await chega(mundo, "fumotom", RX, amz("B0EDB00002"), mid=q, idade_s=40, editada=True)
        s["troca"] = atuais("fumotom", q)
        await nerd(mundo, GPU_NERD, amz("B0EDA00002"))                   # A saiu
        s["troca_a"] = retida("fumotom", q)
        await nerd(mundo, GPU_NERD, amz("B0EDB00002"))
        s["troca_b"] = retida("fumotom", q)

        t = await chega(mundo, "SamuelF3lipePromo", DOIS, amz("B0EDA00003") + amz("B0EDB00003"))
        await chega(mundo, "SamuelF3lipePromo", RX, amz("B0EDB00003"), mid=t, idade_s=30,
                    editada=True)
        for i, extra in enumerate(("\n-CUPOM: X10", "\n-CUPOM: X15")):
            await chega(mundo, "SamuelF3lipePromo", RX + extra, amz("B0EDB00003"), mid=t,
                        idade_s=40 + i, editada=True)
        s["repete"] = atuais("SamuelF3lipePromo", t)
        await chega(mundo, "fadapromos", GPU_FADA, amz("B0EDA00003"))   # aval de A
        s["repete_a"] = retida("SamuelF3lipePromo", t)
        n0 = len(posts(mundo))
        await nerd(mundo, GPU_NERD, amz("B0EDB00003"))
        s["repete_b"] = (retida("SamuelF3lipePromo", t), len(posts(mundo)) - n0)
        for i in range(3):
            await chega(mundo, "SamuelF3lipePromo", RX + f"\n-CUPOM: Y{i}", amz("B0EDB00003"),
                        mid=t, idade_s=60 + i, editada=True)
        s["sem_dup"] = (len(posts(mundo)) - n0, atuais("SamuelF3lipePromo", t))
    cenario(corpo)
    r.check(s["mais"] == (["amazon|B0EDA00001", "amazon|B0EDB00001"], True, True),
            "43.a_mais_b_guarda_os_dois_b_pelo_edit_date", str(s["mais"]))
    r.check(s["mais_solta"] == "liberada", "43.aval_do_b_acrescentado_solta")
    r.check(s["troca"] == ["amazon|B0EDB00002"], "43.a_por_b_guarda_so_b", str(s["troca"]))
    r.check(s["troca_a"] == "retida" and s["troca_b"] == "liberada",
            "43.aval_de_a_nao_solta_b_e_o_de_b_solta", str((s["troca_a"], s["troca_b"])))
    r.check(s["repete"] == ["amazon|B0EDB00003"] and s["repete_a"] == "retida",
            "43.a_mais_b_para_b_e_repeticoes_nao_devolvem_a",
            str((s["repete"], s["repete_a"])))
    r.check(s["repete_b"] == ("liberada", 1) and s["sem_dup"] == (1, ["amazon|B0EDB00003"]),
            "43.uma_publicacao_so", str((s["repete_b"], s["sem_dup"])))


def test_44_troca_do_conjunto_e_aval_na_mesma_transacao(r):
    """Item 1: a troca do conjunto e a conferência do aval são uma
    transação. Edição A→B × aval de A ao mesmo tempo: nunca solta; edição
    A→A+B × aval de B: sempre solta. Reinjeção só passa com o conjunto
    exato que o aval soltou."""
    _limpar_curadoria()
    base = time.time()
    av = lambda mid, ch, ts, **k: dbc.db_curadoria_avaliar(  # noqa: E731
        "-1001", mid, dbc.NOVA, ch, "ssd", base, ts, 300, 120, time.time(), **k)
    ruins = []
    for i in range(120):
        for caso in ("troca", "soma"):
            mid = 71000 + 2 * i + (caso == "soma")
            A, B = f"amazon|A{caso}{i}", f"amazon|B{caso}{i}"
            av(mid, [A], base)
            barreira, res = threading.Barrier(2), {}

            def edita():
                barreira.wait()
                res["e"] = av(mid, [B] if caso == "troca" else [A, B], base + 5)[0]

            def aval():
                barreira.wait()
                res["a"] = dbc.db_curadoria_registrar_aval(
                    [A] if caso == "troca" else [B], "-1002", 90000 + mid, base + 10, 300,
                    base - 420, time.time())
            ts = [threading.Thread(target=edita), threading.Thread(target=aval)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            with _db() as db:
                est = db.execute("SELECT estado FROM curadoria_retida WHERE msg_id=?",
                                 (mid,)).fetchone()[0]
            if caso == "troca":
                ok = (est == "retida" and res["e"] == dbc.SEGURADA
                      and av(mid, [A], base, reinjetada=True)[0] == dbc.OBSOLETA)
            else:
                ok = est in ("liberada", "liberando")
            if not ok:
                ruins.append((caso, i, est, res))
    r.check(ruins == [], "44.sem_corrida_na_troca_do_conjunto", str(ruins[:3]))
    _limpar_curadoria()
    av(1, ["k|A"], base)
    dbc.db_curadoria_registrar_aval(["k|A"], "-1002", 2, base, 300, base - 420, time.time())
    r.check(av(1, ["k|A", "k|C"], base, reinjetada=True)[0] == dbc.AVAL,
            "44.reinjecao_com_conjunto_diferente_e_reavaliada")
    av(3, ["k|X"], base)
    dbc.db_curadoria_registrar_aval(["k|X"], "-1002", 4, base, 300, base - 420, time.time())
    r.check(av(3, ["k|Y"], base, reinjetada=True)[0] == dbc.SEGURADA
            and av(3, ["k|X"], base, reinjetada=True)[0] == dbc.OBSOLETA,
            "44.reinjecao_de_outro_produto_nao_passa")


def test_45_hardware_sem_identidade_fica_retido(r):
    """Item 2: identidade vazia, produto sem código, campanha, encurtador não
    resolvido e busca não são identidade: o hardware vigiado fica retido
    (nenhum aval o solta) e expira normal; a edição que traz link
    identificável passa a esperar aval por ele. Lista vazia nunca é
    aprovação no banco. Exceção (destaque) dispensa o aval também aqui."""
    _limpar_curadoria()
    b = time.time()
    r.check(dbc.db_curadoria_avaliar("-1001", 1, dbc.NOVA, [], "ssd", b, b, 300, 120, b)
            == (dbc.SEGURADA, True)
            and dbc.db_curadoria_registrar_aval([], "-1002", 2, b, 300, b - 420, b) == [],
            "45.banco_lista_vazia_nao_aprova")
    s = {}
    casos = {"vazia": [], "sem_codigo": [("amazon", "", "produto")],
             "campanha": [("amazon", "PROMO77", "campanha")],
             "encurtado": [("mercadolivre", "MLBX1", "encurtado")],
             "busca": [("mercadolivre", "placa-de-video", "busca")]}

    async def corpo(mundo):
        ids = {n: await chega(mundo, "promotom", GPU, idents) for n, idents in casos.items()}
        s["presas"] = ({n: retida("promotom", m) for n, m in ids.items()}, len(posts(mundo)),
                       {n: atuais("promotom", m) for n, m in ids.items()})
        await nerd(mundo, GPU_NERD, amz("B0QUALQ001"))
        await chega(mundo, "fadapromos", GPU_FADA, amz("B0QUALQ002"))
        s["aval"] = {n: retida("promotom", m) for n, m in ids.items()}
        p = ids["vazia"]
        await chega(mundo, "promotom", GPU, amz("B0SEMID001"), mid=p, idade_s=60, editada=True)
        s["edit"] = (atuais("promotom", p), retida("promotom", p))
        n0 = len(posts(mundo))
        await nerd(mundo, GPU_NERD, amz("B0SEMID001"))
        s["edit_solta"] = (retida("promotom", p), len(posts(mundo)) - n0)
        c = ids["campanha"]
        envelhecer(mundo, "promotom", c, 430)
        curadoria._expirar()
        s["expira"] = retida("promotom", c)
        await chega(mundo, "promotom", GPU, amz("B0SEMID002"), mid=c, idade_s=430, editada=True)
        s["reabre"] = (retida("promotom", c), atuais("promotom", c))
        n1 = len(posts(mundo))
        x = await chega(mundo, "promotom", "Ainda ativo! " + GPU, [])
        s["excecao"] = (retida("promotom", x), len(posts(mundo)) - n1)
    cenario(corpo)
    r.check(set(s["presas"][0].values()) == {"retida"} and s["presas"][1] == 0
            and all(v == [] for v in s["presas"][2].values()),
            "45.sem_identidade_fica_retida_sem_chave", str(s["presas"]))
    r.check(set(s["aval"].values()) == {"retida"}, "45.nenhum_aval_solta", str(s["aval"]))
    r.check(s["edit"] == (["amazon|B0SEMID001"], "retida") and s["edit_solta"] == ("liberada", 1),
            "45.edicao_com_link_identificavel_espera_e_solta",
            str((s["edit"], s["edit_solta"])))
    r.check(s["expira"] == "expirada", "45.expira_normal", str(s["expira"]))
    r.check(s["reabre"] == ("retida", ["amazon|B0SEMID002"]),
            "45.produto_novo_depois_de_expirar_tem_janela_propria", str(s["reabre"]))
    r.check(s["excecao"] == (None, 1), "45.excecao_dispensa_o_aval", str(s["excecao"]))


def test_46_banco_fora_bloqueia_e_recupera(r):
    """Item 3: banco fora → hardware vigiado BLOQUEADO só na memória (nunca
    autorizado); outra categoria e fonte isenta seguem; o aval do Fada fica
    na memória; com o banco de volta: o aval é gravado, a bloqueada volta ao
    portão (prova `reavaliando`) e o aval a solta, sem duplicar; a sem aval
    passa a esperar no banco e o aval seguinte a solta."""
    s = {}

    async def corpo(mundo):
        nomes = ("db_curadoria_avaliar", "db_curadoria_registrar_aval", "db_curadoria_reavaliar",
                 "_marcar_repor")
        reais = {n: getattr(curadoria, n) for n in nomes}
        fora = {"v": True}

        def falha(nome, valor):
            real = reais[nome]
            return lambda *a, **k: valor if fora["v"] else real(*a, **k)
        curadoria.db_curadoria_avaliar = falha("db_curadoria_avaliar", (dbc.ERRO, False))
        curadoria.db_curadoria_registrar_aval = falha("db_curadoria_registrar_aval", None)
        curadoria.db_curadoria_reavaliar = falha("db_curadoria_reavaliar", None)
        curadoria._marcar_repor = lambda: None
        try:
            with _Logs() as logs:
                a = await chega(mundo, "promotom", GPU, amz("B0BANCO001"))
                await chega(mundo, "promotom", "Fritadeira Air Fryer Mondial 4L\n\nR$ 199",
                            amz("B0BANCO0X1"))
                f = await chega(mundo, "fadapromos", GPU_FADA, amz("B0BANCO001"))
                b = await chega(mundo, "fumotom", GPU, amz("B0BANCO002"))
                s["fora"] = (len(posts(mundo)), sorted(m for _, m in curadoria._PENDENTES),
                             sorted(m for _, m in curadoria._AVAIS_PENDENTES),
                             retida("promotom", a), retida("fumotom", b))
                s["logs"] = (len(logs.tem("BLOQUEADA, guardada SÓ NA MEMÓRIA")),
                             len(logs.tem("[CURADORIA_AVAL_SEM_BANCO]")))
                curadoria._repor_pendentes()                    # banco ainda fora
                s["ainda"] = (len(curadoria._PENDENTES), len(curadoria._AVAIS_PENDENTES))
                fora["v"] = False                               # o banco voltou
                curadoria._repor_pendentes()
                await drenar()
                s["volta"] = (retida("promotom", a), retida("fumotom", b), len(posts(mundo)),
                              len(curadoria._PENDENTES), len(curadoria._AVAIS_PENDENTES),
                              vinculo("promotom", a) == vinculo("fadapromos", f))
                await nerd(mundo, GPU_NERD, amz("B0BANCO002"))
                s["depois"] = (retida("fumotom", b), len(posts(mundo)))
        finally:
            for n, v in reais.items():
                setattr(curadoria, n, v)
    cenario(corpo)
    r.check(s["fora"][0] == 2 and len(s["fora"][1]) == 2 and len(s["fora"][2]) == 1
            and s["fora"][3:] == (None, None), "46.bloqueia_so_o_hardware_na_memoria",
            str(s["fora"]))
    r.check(s["logs"] == (2, 1), "46.logs_dizem_que_nao_e_duravel", str(s["logs"]))
    r.check(s["ainda"] == (2, 1), "46.banco_ainda_fora_nada_muda", str(s["ainda"]))
    r.check(s["volta"] == ("liberada", "retida", 2, 0, 0, True),
            "46.banco_volta_grava_aval_e_reavalia_sem_duplicar", str(s["volta"]))
    r.check(s["depois"] == ("liberada", 3), "46.a_que_esperava_solta_com_o_aval", str(s["depois"]))


def test_47_excecao_interna_nunca_autoriza(r):
    """Item 3: exceção interna no caminho do hardware de fonte retentora
    BLOQUEIA; não bloqueia fonte isenta nem outra categoria; repetida, tem
    limite; SOMBRA nunca bloqueia; fonte não numérica não é curada."""
    s = {}

    async def corpo(mundo):
        reais = (curadoria.destino_vivo_de_origem, curadoria._registrar_aval,
                 curadoria._marcar_repor)

        def explode(*a, **k):
            raise RuntimeError("defeito")
        curadoria.destino_vivo_de_origem = explode
        curadoria._registrar_aval = explode
        curadoria._marcar_repor = lambda: None
        try:
            with _Logs() as logs:
                p = await chega(mundo, "promotom", GPU, amz("B0EXC00001"))
                await chega(mundo, "promotom", MOUSE, amz("B0EXC00002"))
                await chega(mundo, "fadapromos", GPU_FADA, amz("B0EXC00003"))
                s["x"] = (len(posts(mundo)), (CHAT["promotom"], p) in curadoria._PENDENTES,
                          len(logs.tem("[CURADORIA_FALHA_INTERNA]")),
                          len(logs.tem("não lido (RuntimeError)")))
                k = (CHAT["promotom"], p)
                curadoria._falhas[k] = curadoria._MAX_FALHAS
                await chega(mundo, "promotom", GPU + "\n-CUPOM: X", amz("B0EXC00001"), mid=p,
                            idade_s=20, editada=True)
                s["desiste"] = (k in curadoria._PENDENTES, len(logs.tem("desiste")),
                                len(posts(mundo)))
        finally:
            (curadoria.destino_vivo_de_origem, curadoria._registrar_aval,
             curadoria._marcar_repor) = reais
    cenario(corpo)
    r.check(s["x"] == (2, True, 1, 1), "47.bloqueia_so_o_hardware_da_retentora", str(s["x"]))
    r.check(s["desiste"] == (False, 1, 2), "47.excecao_repetida_tem_limite", str(s["desiste"]))
    d = {}

    async def corpo2(mundo):
        real = curadoria.destino_vivo_de_origem
        curadoria.destino_vivo_de_origem = lambda *a, **k: (_ for _ in ()).throw(RuntimeError())
        try:
            await chega(mundo, "promotom", GPU, amz("B0EXC00010"))
        finally:
            curadoria.destino_vivo_de_origem = real
        d["n"] = len(posts(mundo))
    cenario(corpo2, modo="SOMBRA")
    r.check(d["n"] == 1, "47.sombra_nunca_bloqueia", str(d))
    n = MensagemNormalizada(msg_id=1, chat="abc", texto_limpo=GPU, texto_analise=GPU, mapa={},
                            preservar=[], plat="", sku="", tem_midia=False, media_obj=None)

    class Enr:
        tipo = "produto"
    r.check(curadoria.portao(n, Enr(), False) is True, "47.fonte_nao_numerica_nao_e_curada")


def test_48_memoria_limitada_e_reentrega(r):
    """Item 3: a fila da memória tem limite (o mais velho sai, com log, e NÃO
    vai ao canal) e a que passou da janela sai sem ir ao canal; a reinjeção
    marcada que não chegou ao portão é entregue de novo (limite, depois
    `falhou`); sem ler a prova no banco, nenhuma reinjeção passa."""
    s = {}

    async def corpo(mundo):
        reais = (curadoria.db_curadoria_avaliar, curadoria._marcar_repor,
                 curadoria._MAX_PENDENTES, curadoria.db_curadoria_reavaliar)
        curadoria.db_curadoria_avaliar = lambda *a, **k: (dbc.ERRO, False)
        curadoria.db_curadoria_reavaliar = lambda *a, **k: None      # banco fora
        curadoria._marcar_repor = lambda: None
        curadoria._MAX_PENDENTES = 3
        try:
            with _Logs() as logs:
                ids = [await chega(mundo, "fumotom", GPU, amz(f"B0LIMIT00{i}")) for i in range(5)]
                s["limite"] = (sorted(m for _, m in curadoria._PENDENTES), ids[2:],
                               len(logs.tem("[CURADORIA_PENDENTE_DESCARTADA]")), len(posts(mundo)))
                k = (CHAT["fumotom"], ids[4])
                curadoria._PENDENTES[k]["limite"] = time.time() - 1
                curadoria._repor_pendentes()
                s["expira"] = (k in curadoria._PENDENTES, len(logs.tem("[CURADORIA_PENDENTE_EXPIROU]")))
        finally:
            (curadoria.db_curadoria_avaliar, curadoria._marcar_repor, curadoria._MAX_PENDENTES,
             curadoria.db_curadoria_reavaliar) = reais
            curadoria._PENDENTES.clear()
        await drenar()
        # reinjeção perdida no caminho → reentrega
        perdidas = {"n": 1}
        real_desp = curadoria._estado["despachar"]

        async def perde(ev, is_edit=False):
            if perdidas["n"] > 0:
                perdidas["n"] -= 1
                return
            await real_desp(ev, is_edit=is_edit)
        curadoria._estado["despachar"] = perde
        reais2 = curadoria._PRESA_S
        curadoria._PRESA_S = -1
        try:
            p = await chega(mundo, "promotom", GPU, amz("B0PRESA001"))
            await nerd(mundo, GPU_NERD, amz("B0PRESA001"))
            s["perdida"] = (retida("promotom", p), len(posts(mundo)))
            curadoria._reentregar_presas()
            await drenar()
            s["reentregue"] = (retida("promotom", p), len(posts(mundo)))
            perdidas["n"] = 99
            q = await chega(mundo, "promotom", GPU, amz("B0PRESA002"))
            await nerd(mundo, GPU_NERD, amz("B0PRESA002"))
            for _ in range(curadoria._MAX_REENTREGAS + 1):
                curadoria._reentregar_presas()
                await drenar()
            s["desiste"] = (retida("promotom", q), len(posts(mundo)))
        finally:
            curadoria._estado["despachar"], curadoria._PRESA_S = real_desp, reais2
        real_lib = curadoria.db_curadoria_em_liberacao

        def quebra(*a, **k):
            raise RuntimeError("banco")
        curadoria.db_curadoria_em_liberacao = quebra
        try:
            s["sem_prova"] = curadoria.reinjecao(
                EventoRecuperado(mensagem("promotom", 999001, GPU, idade_s=200), via="CURADORIA"))
        finally:
            curadoria.db_curadoria_em_liberacao = real_lib
    cenario(corpo)
    r.check(s["limite"] == (s["limite"][1], s["limite"][1], 2, 0), "48.fila_limitada",
            str(s["limite"]))
    r.check(s["expira"] == (False, 1), "48.fora_da_janela_sai_da_memoria", str(s["expira"]))
    r.check(s["perdida"] == ("liberando", 0) and s["reentregue"] == ("liberada", 1),
            "48.reinjecao_perdida_e_reentregue", str((s["perdida"], s["reentregue"])))
    r.check(s["desiste"] == ("falhou", 1), "48.reentrega_tem_limite", str(s["desiste"]))
    r.check(s["sem_prova"] is False, "48.sem_ler_a_prova_nao_passa")


def test_49_edicao_publicada_que_preserva_identidade_segue(r):
    """Item 4: preço, cupom e texto de mensagem JÁ publicada, com o mesmo
    produto: o post sincroniza normal, sem curadoria."""
    s = {}

    async def corpo(mundo):
        p, post = await publicada(mundo, "promotom", GPU, "B0PUBL0001")
        e0 = len(mundo.edits)
        await chega(mundo, "promotom", GPU.replace("2299", "2199"), amz("B0PUBL0001"), mid=p,
                    idade_s=20 * 60, editada=True)
        await chega(mundo, "promotom", GPU.replace("2299", "2199") + "\n-CUPOM: PLACA5",
                    amz("B0PUBL0001"), mid=p, idade_s=20 * 60, editada=True)
        s["x"] = (len(posts(mundo)), [m for m, _ in mundo.edits[e0:]],
                  "PLACA5" in (mundo.edits[-1][1] if mundo.edits else ""), linha("promotom", p))
        s["post"] = post
    cenario(corpo)
    r.check(s["x"][0] == 1 and s["x"][1] == [s["post"], s["post"]] and s["x"][2],
            "49.preco_e_cupom_sincronizam", str(s["x"])[:200])
    r.check(s["x"][3] == ("nova", "liberada"), "49.sem_pendencia", str(s["x"][3]))


def test_50_edicao_publicada_com_produto_novo_fica_pendente(r):
    """Item 4: a edição troca o produto por outro vigiado SEM aval → o post
    continua com o aprovado (nenhuma edição no canal), a alteração fica
    pendente; o aval do produto novo aplica a edição no MESMO post (caminho
    normal de edição, sem segundo post)."""
    s = {}

    async def corpo(mundo):
        p, post = await publicada(mundo, "promotom", GPU, "B0PUBL0002")
        e0, c0 = len(mundo.edits), len(mundo.criados)
        with _Logs() as logs:
            await chega(mundo, "promotom", RX, amz("B0PUBL0003"), mid=p, idade_s=60, editada=True)
        s["pendente"] = (len(mundo.edits) - e0, linha("promotom", p), atuais("promotom", p),
                         len(logs.tem("[CURADORIA_EDICAO_PENDENTE]")),
                         "Gigabyte" in db_get_post(post)["texto"])
        await nerd(mundo, GPU_NERD, amz("B0PUBL0003"))
        s["aplicada"] = (len(mundo.criados) - c0, [m for m, _ in mundo.edits[e0:]],
                         "RX 9060" in (mundo.edits[-1][1] if mundo.edits else ""),
                         linha("promotom", p), vinculo("promotom", p))
        s["post"] = post
    cenario(corpo)
    r.check(s["pendente"] == (0, ("edicao", "retida"), ["amazon|B0PUBL0003"], 1, True),
            "50.sem_aval_o_post_segue_com_o_aprovado", str(s["pendente"]))
    r.check(s["aplicada"] == (0, [s["post"]], True, ("edicao", "liberada"), s["post"]),
            "50.aval_aplica_no_mesmo_post", str(s["aplicada"]))


def test_51_edit_date_e_a_entrada_do_produto_novo(r):
    """Item 4: o produto que a edição traz entra pelo edit_date: aval 4 min
    antes da edição vale; 6 min antes não; aval perto da data ORIGINAL da
    mensagem (20 min antes) não vale para ele."""
    s = {}

    async def corpo(mundo):
        p, _ = await publicada(mundo, "promotom", GPU, "B0EDDT0001")
        e0 = len(mundo.edits)
        await nerd(mundo, GPU_NERD, amz("B0EDDT0002"), idade_s=240)
        await chega(mundo, "promotom", RX, amz("B0EDDT0002"), mid=p, idade_s=20 * 60, editada=True)
        s["vale"] = (len(mundo.edits) - e0, linha("promotom", p))
        q, _ = await publicada(mundo, "fumotom", GPU, "B0EDDT0003")
        await nerd(mundo, GPU_NERD, amz("B0EDDT0004"), idade_s=360)
        await chega(mundo, "fumotom", RX, amz("B0EDDT0004"), mid=q, idade_s=20 * 60, editada=True)
        s["nao_vale"] = linha("fumotom", q)
        t, _ = await publicada(mundo, "SamuelF3lipePromo", GPU, "B0EDDT0005")
        envelhecer(mundo, "SamuelF3lipePromo", t, 20 * 60)        # publicada há 20 min
        aval_direto("amazon|B0EDDT0006", time.time() - 20 * 60 + 10)
        await chega(mundo, "SamuelF3lipePromo", RX, amz("B0EDDT0006"), mid=t, idade_s=20 * 60,
                    editada=True)
        s["original"] = linha("SamuelF3lipePromo", t)
    cenario(corpo)
    r.check(s["vale"] == (1, ("edicao", "liberada")), "51.aval_4min_antes_da_edicao_vale",
            str(s["vale"]))
    r.check(s["nao_vale"] == ("edicao", "retida"), "51.aval_6min_antes_nao_vale",
            str(s["nao_vale"]))
    r.check(s["original"] == ("edicao", "retida"), "51.data_original_nao_vale_para_o_novo",
            str(s["original"]))


def test_52_pendente_substituida_nao_volta(r):
    """Item 4: a fonte volta ao produto aprovado (ou troca para um produto
    não vigiado): a edição segue normal e a pendência FECHA — o aval tardio
    do produto que saiu não reaplica nada."""
    s = {}

    async def corpo(mundo):
        p, post = await publicada(mundo, "promotom", GPU, "B0SUPER001")
        await chega(mundo, "promotom", RX, amz("B0SUPER002"), mid=p, idade_s=60, editada=True)
        e0 = len(mundo.edits)
        await chega(mundo, "promotom", GPU.replace("2299", "2099"), amz("B0SUPER001"), mid=p,
                    idade_s=70, editada=True)
        s["volta"] = (len(mundo.edits) - e0, linha("promotom", p))
        e1 = len(mundo.edits)
        await nerd(mundo, GPU_NERD, amz("B0SUPER002"))
        s["aval_tardio"] = (len(mundo.edits) - e1, len(posts(mundo)), "2099" in db_get_post(post)["texto"])
        q, _ = await publicada(mundo, "fumotom", GPU, "B0SUPER003")
        await chega(mundo, "fumotom", RX, amz("B0SUPER004"), mid=q, idade_s=60, editada=True)
        e2 = len(mundo.edits)
        await chega(mundo, "fumotom", MOUSE, amz("B0SUPER005"), mid=q, idade_s=70, editada=True)
        s["outra_cat"] = (len(mundo.edits) - e2, linha("fumotom", q))
        e3 = len(mundo.edits)
        await nerd(mundo, GPU_NERD, amz("B0SUPER004"))
        s["aval_tardio2"] = len(mundo.edits) - e3
    cenario(corpo)
    r.check(s["volta"] == (1, ("edicao", "superada")), "52.volta_ao_aprovado_segue_e_fecha",
            str(s["volta"]))
    r.check(s["aval_tardio"] == (0, 1, True), "52.aval_tardio_nao_reaplica", str(s["aval_tardio"]))
    r.check(s["outra_cat"] == (1, ("edicao", "superada")) and s["aval_tardio2"] == 0,
            "52.troca_para_outra_categoria_segue_e_fecha",
            str((s["outra_cat"], s["aval_tardio2"])))


def test_53_publicada_a_mais_b_e_sem_identidade(r):
    """Item 4: A→A+B numa publicada — com aval de A (aval de A OU B solta a
    mensagem inteira) a edição passa; publicada por exceção, sem aval, a
    edição espera aval de A ou de B (A pela data original). Edição que
    tira a identidade de produto fica pendente."""
    s = {}

    async def corpo(mundo):
        p, _ = await publicada(mundo, "promotom", GPU, "B0AMB00001")
        e0 = len(mundo.edits)
        await chega(mundo, "promotom", DOIS, amz("B0AMB00001") + amz("B0AMB00002"), mid=p,
                    idade_s=60, editada=True)
        s["com_aval"] = (len(mundo.edits) - e0, linha("promotom", p))
        q = await chega(mundo, "fumotom", "Ainda ativo! " + GPU, amz("B0AMB00003"))
        e1 = len(mundo.edits)
        await chega(mundo, "fumotom", DOIS, amz("B0AMB00003") + amz("B0AMB00004"), mid=q,
                    idade_s=60, editada=True)
        s["sem_aval"] = (len(mundo.edits) - e1, linha("fumotom", q))
        await nerd(mundo, GPU_NERD, amz("B0AMB00003"))
        s["aval_de_a"] = (len(mundo.edits) - e1, linha("fumotom", q))
        t, post = await publicada(mundo, "SamuelF3lipePromo", GPU, "B0AMB00005")
        e2 = len(mundo.edits)
        await chega(mundo, "SamuelF3lipePromo", GPU, [], mid=t, idade_s=60, editada=True)
        s["sem_id"] = (len(mundo.edits) - e2, linha("SamuelF3lipePromo", t),
                       "Gigabyte" in db_get_post(post)["texto"])
    cenario(corpo)
    r.check(s["com_aval"] == (1, ("edicao", "liberada")), "53.a_mais_b_com_aval_de_a_passa",
            str(s["com_aval"]))
    r.check(s["sem_aval"] == (0, ("edicao", "retida")) and s["aval_de_a"] == (1, ("edicao", "liberada")),
            "53.publicada_por_excecao_espera_aval", str((s["sem_aval"], s["aval_de_a"])))
    r.check(s["sem_id"] == (0, ("edicao", "retida"), True), "53.edicao_sem_identidade_pendente",
            str(s["sem_id"]))


def test_54_edicao_de_mensagem_nao_lider(r):
    """Item 4: a mensagem da fonte retentora que casou no post do Fada (não
    é a líder) troca o produto: pendente sem aval; com aval, a edição vai à
    decisão do MESMO post (nunca um segundo post)."""
    s = {}

    async def corpo(mundo):
        await chega(mundo, "fadapromos", GPU_FADA, amz("B0NLID0001"))
        post = posts(mundo)[-1]
        p = await chega(mundo, "promotom", GPU, amz("B0NLID0001"))
        s["vinc"] = vinculo("promotom", p) == post
        c0, e0 = len(mundo.criados), len(mundo.edits)
        await chega(mundo, "promotom", RX, amz("B0NLID0002"), mid=p, idade_s=60, editada=True)
        s["pendente"] = (len(mundo.criados) - c0, len(mundo.edits) - e0, linha("promotom", p))
        await nerd(mundo, GPU_NERD, amz("B0NLID0002"))
        s["aval"] = (len(mundo.criados) - c0, linha("promotom", p), vinculo("promotom", p) == post,
                     all(m == post for m, _ in mundo.edits[e0:]))
    cenario(corpo)
    r.check(s["vinc"], "54.casou_no_post_do_fada")
    r.check(s["pendente"] == (0, 0, ("edicao", "retida")), "54.sem_aval_pendente",
            str(s["pendente"]))
    r.check(s["aval"] == (0, ("edicao", "liberada"), True, True), "54.com_aval_o_mesmo_post",
            str(s["aval"]))


def test_55_edicao_publicada_em_sombra_e_fada(r):
    """Item 4: em SOMBRA a edição publicada vai ao ar e só fica registrada;
    a edição do Fada que traz produto novo é aval pelo edit_date."""
    s = {}

    async def corpo(mundo):
        p, post = await publicada(mundo, "promotom", GPU, "B0SOMB0001")
        e0 = len(mundo.edits)
        await chega(mundo, "promotom", RX, amz("B0SOMB0002"), mid=p, idade_s=60, editada=True)
        s["x"] = (len(mundo.edits) - e0, linha("promotom", p))
    cenario(corpo, modo="SOMBRA")
    r.check(s["x"] == (1, ("edicao", "retida")), "55.sombra_aplica_e_registra", str(s["x"]))
    d = {}

    async def corpo2(mundo):
        f = await chega(mundo, "fadapromos", GPU_FADA, amz("B0FADE0001"))
        await chega(mundo, "fadapromos", GPU_FADA, amz("B0FADE0001") + amz("B0FADE0002"),
                    mid=f, idade_s=20 * 60, editada=True)
        with _db() as db:
            d["ts"] = dict(db.execute("SELECT chave, ts FROM curadoria_aval").fetchall())
    cenario(corpo2)
    r.check(abs(d["ts"]["amazon|B0FADE0002"] - time.time()) < 10
            and d["ts"]["amazon|B0FADE0001"] < d["ts"]["amazon|B0FADE0002"],
            "55.fada_edicao_aval_pelo_edit_date", str(d))


def test_56_segurada_antiga_editada_com_produto_novo(r):
    """Item 1: mensagem retida (nunca publicada) há 15 min que a fonte edita
    trazendo B: B espera aval pelo edit_date e, com o aval, a mensagem sai
    — a reinjeção vale pela data da versão, não pela da mensagem."""
    s = {}

    async def corpo(mundo):
        p = await chega(mundo, "promotom", GPU, amz("B0VELHA001"))
        envelhecer(mundo, "promotom", p, 15 * 60)
        curadoria._expirar()
        s["expirou"] = retida("promotom", p)
        await chega(mundo, "promotom", RX, amz("B0VELHA002"), mid=p, idade_s=15 * 60,
                    editada=True)
        s["espera"] = (retida("promotom", p), atuais("promotom", p))
        await nerd(mundo, GPU_NERD, amz("B0VELHA002"))
        s["sai"] = (retida("promotom", p), len(posts(mundo)),
                    "RX 9060" in (mundo.criados[-1][1] if mundo.criados else ""))
    cenario(corpo)
    r.check(s["expirou"] == "expirada", "56.expirou_sem_aval")
    r.check(s["espera"] == ("retida", ["amazon|B0VELHA002"]), "56.produto_novo_reabre_a_espera",
            str(s["espera"]))
    r.check(s["sai"] == ("liberada", 1, True), "56.aval_do_novo_solta_a_versao_atual",
            str(s["sai"]))


def test_57_aprovado_do_post_inclui_a_familia_e_a_data_original(r):
    """Item 4: (a) o que o post já tem inclui a FAMÍLIA dele: a mensagem que
    casou no post do Fada trazendo outro anúncio (A2, aprendido pela
    família, não exibido) edita o preço só com A2 → segue normal; (b) o
    produto que já estava na mensagem publicada (sem histórico na
    curadoria) vale pela data ORIGINAL: aval de A perto dela cobre a edição
    A→A+B."""
    s = {}

    async def corpo(mundo):
        await chega(mundo, "fadapromos", "🔥 Placas de vídeo\n\n🔹 RTX 5060 Ti Gigabyte - R$ 2299"
                    "\n\n🔹 RX 9060 XT Sapphire - R$ 2499",
                    amz("B0FAMI0001") + amz("B0FAMI0009"))              # lista {A, C}
        post = posts(mundo)[-1]
        p = await chega(mundo, "promotom", DOIS, amz("B0FAMI0001") + amz("B0FAMI0002"))
        s["casou"] = (vinculo("promotom", p) == post,
                      "amazon|B0FAMI0002" in db_ofertas_de_post(post),
                      "amazon|B0FAMI0002" in db_exibida(post))
        await chega(mundo, "promotom", GPU.replace("2299", "2149"), amz("B0FAMI0002"), mid=p,
                    idade_s=60, editada=True)
        s["familia"] = (linha("promotom", p), len(curadoria._SEGURADAS))
        q = await chega(mundo, "fumotom", "Ainda ativo! " + GPU, amz("B0FAMI0003"))
        envelhecer(mundo, "fumotom", q, 20 * 60)
        aval_direto("amazon|B0FAMI0003", time.time() - 20 * 60 + 30)    # perto da original
        e1 = len(mundo.edits)
        await chega(mundo, "fumotom", DOIS, amz("B0FAMI0003") + amz("B0FAMI0004"), mid=q,
                    idade_s=20 * 60, editada=True)
        s["original"] = (len(mundo.edits) - e1, linha("fumotom", q))
        with _db() as db:
            s["entradas"] = dict(db.execute(
                "SELECT chave, ts_entrada FROM curadoria_chave WHERE msg_id=?", (q,)).fetchall())
    cenario(corpo)
    r.check(s["casou"] == (True, True, False), "57.casou_e_a_familia_aprendeu_sem_exibir",
            str(s["casou"]))
    r.check(s["familia"][0] == ("nova", "liberada"), "57.anuncio_da_familia_e_aprovado",
            str(s["familia"]))
    r.check(s["original"] == (1, ("edicao", "liberada")), "57.ja_publicado_vale_pela_data_original",
            str(s["original"]))
    e = s["entradas"]
    r.check(e["amazon|B0FAMI0004"] - e["amazon|B0FAMI0003"] > 15 * 60,
            "57.o_novo_entra_pelo_edit_date_o_antigo_pela_original", str(e))


# ══════════════════════════════════════════════════════════════════
# ÚLTIMA AUDITORIA (03/11/2026) — edição no Nerd, reentrada, EDIT_ANTIGO
# ══════════════════════════════════════════════════════════════════
def versao_nerd(mid, texto, links, idade_s=5.0, edit_idade_s=None, edit_em=None):
    """Uma VERSÃO da mensagem do Nerd: `links` = {url: idents}; cada link de
    teste leva às chaves dele (a identificação real fica no teste 32).
    `edit_em`: edit_date exato (o Telegram dá segundos — duas edições podem
    ter o MESMO)."""
    for u, idents in links.items():
        IDENTS_AVAL[u] = [f"{p}|{i}" for p, i, _ in idents]
    corpo = texto + "".join(f"\n\n🛒 {u}" for u in links)
    m = mensagem("nerdofertas", mid, corpo, idade_s=idade_s,
                 editada=edit_idade_s is not None or edit_em is not None,
                 edit_idade_s=edit_idade_s or 0.0)
    if edit_em is not None:
        m.edit_date = edit_em
    return m


async def pelo_handler(mundo, m, editada=False):
    """O update chega pelo handler REAL registrado no canal de aval."""
    cbs = {type(ev).__name__: cb for cb, ev in mundo.handlers}
    if editada:
        await cbs["MessageEdited"](tl_events.MessageEdited.Event(m))
    else:
        await cbs["NewMessage"](tl_events.NewMessage.Event(m))
    await drenar()


def avais_de(mid):
    with _db() as db:
        return sorted(db.execute("SELECT chave, ts FROM curadoria_aval WHERE fonte=? AND msg_id=?",
                                 (str(PEER["nerdofertas"]), mid)).fetchall())


def test_58_nerd_edita_e_acrescenta_link(r):
    """Item 1: o Nerd posta sem link (não é aval) e edita acrescentando o
    link identificável → aval reconhecido NA HORA pela edição; vale também
    para mensagem antiga do Nerd editada agora (a versão vale pelo
    edit_date). Nada do Nerd vai ao canal."""
    s = {}

    async def corpo(mundo):
        p = await chega(mundo, "promotom", GPU, amz("B0NEDIT001"))
        mid = novo_id()
        m0 = versao_nerd(mid, GPU_NERD, {}, idade_s=120)
        with _Logs() as logs:
            await pelo_handler(mundo, m0)
        s["sem_link"] = (retida("promotom", p), len(logs.tem("[CURADORIA_AVAL_SEM_PRODUTO]")),
                         avais_de(mid))
        m1 = versao_nerd(mid, GPU_NERD, {"https://nerd.teste/e/1": amz("B0NEDIT001")},
                         idade_s=120, edit_idade_s=0)
        await pelo_handler(mundo, m1, editada=True)
        s["editou"] = (retida("promotom", p), len(posts(mundo)),
                       all("😱" not in t for _, t in mundo.criados))
        q = await chega(mundo, "fumotom", GPU, amz("B0NEDIT002"))
        mid2 = novo_id()
        await pelo_handler(mundo, versao_nerd(mid2, GPU_NERD, {}, idade_s=30 * 60))
        await pelo_handler(mundo, versao_nerd(mid2, GPU_NERD,
                                              {"https://nerd.teste/e/2": amz("B0NEDIT002")},
                                              idade_s=30 * 60, edit_idade_s=0), editada=True)
        s["antiga"] = (retida("fumotom", q), len(posts(mundo)))
    cenario(corpo)
    r.check(s["sem_link"] == ("retida", 1, []), "58.sem_link_nao_e_aval", str(s["sem_link"]))
    r.check(s["editou"] == ("liberada", 1, True), "58.edicao_com_link_e_aval_e_nerd_nao_publica",
            str(s["editou"]))
    r.check(s["antiga"] == ("liberada", 2), "58.mensagem_antiga_editada_agora_vale",
            str(s["antiga"]))


def test_59_nerd_acrescenta_b_a_conserva_data(r):
    """Item 1: o Nerd posta A e depois edita acrescentando B: A conserva a
    data original, B entra pelo edit_date; rever a mesma versão (handler de
    novo, varredura) não duplica aval. A segurada de B que só cabe na janela
    pela data ORIGINAL do Nerd não é solta (B vale pela edição)."""
    s = {}

    async def corpo(mundo):
        q = await chega(mundo, "fumotom", RX, amz("B0NERDB001"))
        envelhecer(mundo, "fumotom", q, 400)          # B entrou na fonte há 400 s
        mid = novo_id()
        la, lb = "https://nerd.teste/a59", "https://nerd.teste/b59"
        m0 = versao_nerd(mid, GPU_NERD, {la: amz("B0NERDA001")}, idade_s=200)
        await pelo_handler(mundo, m0)
        m1 = versao_nerd(mid, GPU_NERD, {la: amz("B0NERDA001"), lb: amz("B0NERDB001")},
                         idade_s=200, edit_idade_s=0)
        await pelo_handler(mundo, m1, editada=True)
        await pelo_handler(mundo, m1, editada=True)                     # o mesmo update de novo
        mundo.historico[PEER["nerdofertas"]] = [m1]
        await curadoria._varrer_canal(PEER["nerdofertas"], ENT["nerdofertas"])   # e a varredura
        await drenar()
        s["avais"] = avais_de(mid)
        s["q"] = retida("fumotom", q)
    cenario(corpo)
    a = dict(s["avais"])
    agora = time.time()
    r.check(len(s["avais"]) == 2, "59.um_aval_por_produto_sem_duplicar", str(s["avais"]))
    r.check(abs(a["amazon|B0NERDA001"] - (agora - 200)) < 5
            and abs(a["amazon|B0NERDB001"] - agora) < 5,
            "59.a_conserva_data_b_pelo_edit_date", str(s["avais"]))
    r.check(s["q"] == "retida", "59.b_nao_vale_pela_data_original_do_nerd", str(s["q"]))


def test_60_handler_e_varredura_nao_duplicam_edicao(r):
    """Item 1: a versão editada chega pelo handler e pela varredura — é lida
    UMA vez; a edição que o handler perdeu é achada pela varredura (que lê
    sempre a página mais recente). O Nerd nunca entra no funil."""
    s = {}

    async def corpo(mundo):
        leituras, despachos = [], []
        real_reg, real_desp = curadoria._registrar_aval, curadoria._estado["despachar"]

        def conta(*a, **k):
            leituras.append(a[1])
            return real_reg(*a, **k)

        async def espia(ev, is_edit=False):
            despachos.append(str(ev.chat_id))
            await real_desp(ev, is_edit=is_edit)
        curadoria._registrar_aval, curadoria._estado["despachar"] = conta, espia
        try:
            peer, ent = PEER["nerdofertas"], ENT["nerdofertas"]
            mid = novo_id()
            m0 = versao_nerd(mid, GPU_NERD, {"https://nerd.teste/a60": amz("B0DUPL0001")})
            mundo.historico[peer] = [m0]
            await pelo_handler(mundo, m0)
            await curadoria._varrer_canal(peer, ent)
            await drenar()
            s["original"] = len(leituras)
            m1 = versao_nerd(mid, GPU_NERD, {"https://nerd.teste/a60": amz("B0DUPL0001"),
                                             "https://nerd.teste/b60": amz("B0DUPL0002")},
                             edit_idade_s=0)
            mundo.historico[peer] = [m1]
            await curadoria._varrer_canal(peer, ent)           # o handler perdeu a edição
            await drenar()
            s["achada"] = len(avais_de(mid))                   # a varredura achou B
            await pelo_handler(mundo, m1, editada=True)        # chega atrasado
            await curadoria._varrer_canal(peer, ent)
            await drenar()
            s["edicao"] = (len(leituras), len(avais_de(mid)))
            s["funil"] = (len(posts(mundo)), [d for d in despachos if d == CHAT["nerdofertas"]])
        finally:
            curadoria._registrar_aval, curadoria._estado["despachar"] = real_reg, real_desp
    cenario(corpo)
    r.check(s["original"] == 1, "60.original_lida_uma_vez", str(s))
    r.check(s["achada"] == 2 and s["edicao"] == (2, 2),
            "60.edicao_achada_pela_varredura_e_lida_uma_vez", str(s))
    r.check(s["funil"] == (0, []), "60.nerd_nunca_no_funil", str(s["funil"]))


def test_61_produto_removido_e_reintroduzido(r):
    """Item 2: A entra; o Nerd avalia A (dentro de 300 s da entrada) quando A
    já tinha sido trocada por B; passados mais de 5 min, A volta — é uma
    ocorrência NOVA (data da edição): o aval antigo de A não a solta; ela
    espera nova aprovação; o aval novo, dentro dos 300 s, solta. No banco:
    o que permanece conserva a data (A→A+B), o que sai e volta ganha a
    data da reintrodução."""
    _limpar_curadoria()
    b = time.time()
    av = lambda ch, ts: dbc.db_curadoria_avaliar(  # noqa: E731
        "-1001", 1, dbc.NOVA, ch, "ssd", b, ts, 300, 120, time.time())

    def entradas():
        with _db() as db:
            return {c: (round(t - b), a) for c, t, a in db.execute(
                "SELECT chave, ts_entrada, atual FROM curadoria_chave WHERE msg_id=1")}
    av(["k|A"], b)
    av(["k|A", "k|B"], b + 10)
    e1 = entradas()
    av(["k|B"], b + 20)
    av(["k|A", "k|B"], b + 400)
    e2 = entradas()
    r.check(e1 == {"k|A": (0, 1), "k|B": (10, 1)}, "61.a_mais_b_a_conserva_b_pela_edicao", str(e1))
    r.check(e2 == {"k|A": (400, 1), "k|B": (10, 1)}, "61.a_que_volta_ganha_a_data_da_volta", str(e2))
    s = {}

    async def corpo(mundo):
        p = await chega(mundo, "promotom", GPU, amz("B0REINT001"))          # A entra
        await chega(mundo, "promotom", RX, amz("B0REINT002"), mid=p, idade_s=20,
                    editada=True)                                          # A → B
        await nerd(mundo, GPU_NERD, amz("B0REINT001"))                      # aval de A
        s["a_fora"] = retida("promotom", p)
        envelhecer(mundo, "promotom", p, 330)                               # passam 5,5 min
        with _db() as db:
            db.execute("UPDATE curadoria_aval SET ts=ts-330 WHERE chave=?", ("amazon|B0REINT001",))
        await chega(mundo, "promotom", GPU, amz("B0REINT001"), mid=p, idade_s=350,
                    editada=True)                                          # A volta
        s["volta"] = (retida("promotom", p), atuais("promotom", p), len(posts(mundo)))
        await nerd(mundo, GPU_NERD, amz("B0REINT001"))                      # aval novo
        s["novo_aval"] = (retida("promotom", p), len(posts(mundo)))
    cenario(corpo)
    r.check(s["a_fora"] == "retida", "61.aval_de_a_fora_do_conjunto_nao_solta", str(s["a_fora"]))
    r.check(s["volta"] == ("retida", ["amazon|B0REINT001"], 0),
            "61.aval_antigo_nao_solta_a_reintroduzida", str(s["volta"]))
    r.check(s["novo_aval"] == ("liberada", 1), "61.aval_novo_solta", str(s["novo_aval"]))


def test_62_nerd_produto_que_sai_e_volta(r):
    """Item 2 (lado do avalista): no Nerd, o produto que uma edição tirou e
    outra trouxe de volta ganha um aval NOVO com a data da volta; o aval
    antigo continua (é fato); o que permaneceu não ganha linha."""
    s = {}

    async def corpo(mundo):
        mid = novo_id()
        la, lb = "https://nerd.teste/a62", "https://nerd.teste/b62"
        await pelo_handler(mundo, versao_nerd(mid, GPU_NERD, {la: amz("B0VOLT0001")}, idade_s=300))
        await pelo_handler(mundo, versao_nerd(mid, GPU_NERD, {lb: amz("B0VOLT0002")}, idade_s=300,
                                              edit_idade_s=200), editada=True)
        await pelo_handler(mundo, versao_nerd(mid, GPU_NERD, {la: amz("B0VOLT0001"),
                                                              lb: amz("B0VOLT0002")},
                                              idade_s=300, edit_idade_s=0), editada=True)
        s["avais"] = [(c, round(time.time() - t)) for c, t in avais_de(mid)]
    cenario(corpo)
    r.check(sorted(s["avais"]) == sorted([("amazon|B0VOLT0001", 300), ("amazon|B0VOLT0001", 0),
                                          ("amazon|B0VOLT0002", 200)]),
            "62.volta_ganha_aval_novo_o_que_ficou_nao", str(s["avais"]))


def test_63_edit_antigo_e_a_edicao_pendente(r):
    """Item 3: a edição recente de mensagem com quase 1 h, vinculada a post
    VIVO, fica pendente; o aval chega quando a mensagem já passou de 1 h — a
    reinjeção (via CURADORIA + prova no banco + edit_date recente) passa a
    trava EDIT_ANTIGO e a edição é aplicada no mesmo post. Sem prova, por
    outra via, ou com a versão velha: a trava continua."""
    s = {}

    async def corpo(mundo):
        p, post = await publicada(mundo, "promotom", GPU, "B0VIDA0001")
        await chega(mundo, "promotom", RX, amz("B0VIDA0002"), mid=p, idade_s=3590,
                    editada=True)                                   # 59min50s: entra
        s["pendente"] = linha("promotom", p)
        mundo.servidor[(PEER["promotom"], p)].date = _agora() - dt.timedelta(seconds=3700)
        e0, c0 = len(mundo.edits), len(mundo.criados)
        await nerd(mundo, GPU_NERD, amz("B0VIDA0002"))
        s["aplicada"] = ([m for m, _ in mundo.edits[e0:]], len(mundo.criados) - c0,
                         linha("promotom", p))
        s["post"] = post
        e1 = len(mundo.edits)
        await chega(mundo, "promotom", RX + "\n-CUPOM: X", amz("B0VIDA0002"), mid=p,
                    idade_s=3700, editada=True)                     # edição normal > 1 h
        s["normal_velha"] = len(mundo.edits) - e1
    cenario(corpo)
    r.check(s["pendente"] == ("edicao", "retida"), "63.edicao_recente_fica_pendente",
            str(s["pendente"]))
    r.check(s["aplicada"] == ([s["post"]], 0, ("edicao", "liberada")),
            "63.aval_aplica_depois_de_1h_com_prova", str(s["aplicada"]))
    r.check(s["normal_velha"] == 0, "63.edicao_normal_velha_continua_descartada")

    admitidas = []

    async def espiao(ev, is_edit):
        admitidas.append((ev.message.id, getattr(ev, "via", "TELEGRAM")))

    def prova(mid, estado="liberando"):
        with _db() as db:
            db.execute("INSERT OR REPLACE INTO curadoria_retida(chat,msg_id,tipo,categoria,ts,"
                       "ts_ref,estado,ts_estado) VALUES(?,?,?,?,?,?,?,?)",
                       (CHAT["fumotom"], mid, "edicao", "placa_de_video", time.time() - 3700,
                        time.time(), estado, time.time()))
    real = orchestrator._enfileirar
    orchestrator._enfileirar = espiao
    _limpar_curadoria()
    try:
        async def corpo2():
            g._encerrando = False
            ev = lambda mid, via, ed=0.0: EventoRecuperado(  # noqa: E731
                mensagem("fumotom", mid, RX, idade_s=3700, editada=True, edit_idade_s=ed), via=via)
            await orchestrator.processar(tl_events.MessageEdited.Event(
                mensagem("fumotom", 920001, RX, idade_s=3700, editada=True)), is_edit=True)
            await orchestrator.processar(ev(920002, "CURADORIA"), is_edit=True)       # sem prova
            prova(920003)
            await orchestrator.processar(ev(920003, "RECUPERACAO"), is_edit=True)     # outra via
            prova(920004)
            await orchestrator.processar(ev(920004, "CURADORIA", ed=600), is_edit=True)  # velha
            prova(920005, estado="retida")
            await orchestrator.processar(ev(920005, "CURADORIA"), is_edit=True)       # sem soltura
            prova(920006)
            await orchestrator.processar(ev(920006, "CURADORIA"), is_edit=True)       # ADMITE
            await orchestrator.processar(tl_events.MessageEdited.Event(
                mensagem("fumotom", 920007, RX, idade_s=3500, editada=True)), is_edit=True)
        asyncio.run(corpo2())
    finally:
        orchestrator._enfileirar = real
    r.check(admitidas == [(920006, "CURADORIA"), (920007, "TELEGRAM")],
            "63.trava_so_abre_com_as_tres_provas", str(admitidas))


def test_64_leitura_parcial_do_nerd_nao_remove_produto(r):
    """Item 1: a edição do Nerd em que um link falha na leitura (rede) não
    tira esse produto da mensagem dele: a nova tentativa o acha de novo e
    ele CONSERVA a data — não vira uma 'volta' com data nova."""
    s = {}

    async def corpo(mundo):
        respostas = iter([(["amazon|B0PARC0001", "amazon|B0PARC0002"], False),
                          (["amazon|B0PARC0001"], True),
                          (["amazon|B0PARC0001", "amazon|B0PARC0002"], False)])

        async def ident(links):
            return next(respostas)
        real_rep = curadoria._REPETIR_S
        curadoria._identificar, curadoria._REPETIR_S = ident, (0.01, 0.01)
        try:
            mid = novo_id()
            await pelo_handler(mundo, versao_nerd(mid, GPU_NERD, {}, idade_s=100))
            await pelo_handler(mundo, versao_nerd(mid, GPU_NERD + " (editado)", {}, idade_s=100,
                                                  edit_idade_s=0), editada=True)
            await asyncio.sleep(0.05)
            await drenar()
            s["avais"] = [(c, round(time.time() - t)) for c, t in avais_de(mid)]
        finally:
            curadoria._REPETIR_S = real_rep
    cenario(corpo)
    r.check(sorted(s["avais"]) == [("amazon|B0PARC0001", 100), ("amazon|B0PARC0002", 100)],
            "64.produto_nao_lido_conserva_a_data", str(s["avais"]))


def test_65_update_atrasado_de_versao_velha_do_nerd(r):
    """Item 1: a edição do Nerd já foi lida (pela varredura) e o update da
    versão ORIGINAL chega atrasado pelo handler: é ignorado — não tira do
    conjunto o produto da edição nem cria aval novo."""
    s = {}

    async def corpo(mundo):
        mid = novo_id()
        la, lb = "https://nerd.teste/a65", "https://nerd.teste/b65"
        m0 = versao_nerd(mid, GPU_NERD, {la: amz("B0ATRAS001")}, idade_s=100)
        m1 = versao_nerd(mid, GPU_NERD, {lb: amz("B0ATRAS002")}, idade_s=100, edit_idade_s=10)
        m2 = versao_nerd(mid, GPU_NERD, {la: amz("B0ATRAS001"), lb: amz("B0ATRAS002")},
                         idade_s=100, edit_idade_s=0)
        await pelo_handler(mundo, m2, editada=True)      # a mais nova primeiro
        await pelo_handler(mundo, m1, editada=True)      # atrasadas
        await pelo_handler(mundo, m0)
        with _db() as db:
            s["conj"] = sorted(c for (c,) in db.execute(
                "SELECT chave FROM curadoria_chave WHERE chat=? AND msg_id=? AND atual=1",
                (str(PEER["nerdofertas"]), mid)))
        s["avais"] = len(avais_de(mid))
    cenario(corpo)
    r.check(s["conj"] == ["amazon|B0ATRAS001", "amazon|B0ATRAS002"] and s["avais"] == 2,
            "65.versao_velha_atrasada_ignorada", str(s))


def _segundo():
    """Um edit_date com a precisão do Telegram (segundos)."""
    return _agora().replace(microsecond=0)


def _espiar_leituras():
    """Conta cada leitura (versão processada) do canal de aval e cada
    entrega ao ponto de entrada — para provar 'sem duplicar' e 'Nerd fora
    do funil'."""
    leituras, despachos = [], []
    real_reg, real_desp = curadoria._registrar_aval, curadoria._estado["despachar"]

    def conta(*a, **k):
        leituras.append((a[1], tuple(a[2])))
        return real_reg(*a, **k)

    async def espia(ev, is_edit=False):
        despachos.append(str(ev.chat_id))
        await real_desp(ev, is_edit=is_edit)
    curadoria._registrar_aval, curadoria._estado["despachar"] = conta, espia

    def desfaz():
        curadoria._registrar_aval, curadoria._estado["despachar"] = real_reg, real_desp
    return leituras, despachos, desfaz


def atuais_nerd(mid):
    with _db() as db:
        return sorted(c for (c,) in db.execute(
            "SELECT chave FROM curadoria_chave WHERE chat=? AND msg_id=? AND atual=1",
            (str(PEER["nerdofertas"]), mid)))


def test_66_duas_edicoes_no_mesmo_segundo(r):
    """Versão = edit_date + assinatura do conteúdo: duas edições DIFERENTES
    no mesmo segundo são duas versões (a segunda é lida e dá aval); a mesma
    edição de novo (handler repetido, varredura) não é relida; o produto
    que permanece conserva a data, o acrescentado entra pelo edit_date.
    Vale também quando só o TEXTO muda no mesmo segundo (o mesmo link passa
    a ter categoria vigiada). O Nerd nunca entra no funil."""
    s = {}

    async def corpo(mundo):
        leituras, despachos, desfaz = _espiar_leituras()
        try:
            peer, ent = PEER["nerdofertas"], ENT["nerdofertas"]
            q = await chega(mundo, "fumotom", RX, amz("B0SEG00002"))       # espera B
            mid = novo_id()
            la, lb = "https://nerd.teste/a66", "https://nerd.teste/b66"
            await pelo_handler(mundo, versao_nerd(mid, GPU_NERD, {la: amz("B0SEG00001")},
                                                  idade_s=200))
            t = _segundo()
            e1 = versao_nerd(mid, GPU_NERD + " ✏️", {la: amz("B0SEG00001")}, idade_s=200,
                             edit_em=t)
            e2 = versao_nerd(mid, GPU_NERD, {la: amz("B0SEG00001"), lb: amz("B0SEG00002")},
                             idade_s=200, edit_em=t)                         # MESMO segundo
            await pelo_handler(mundo, e1, editada=True)
            await pelo_handler(mundo, e2, editada=True)
            s["dois"] = (len(leituras), retida("fumotom", q))
            await pelo_handler(mundo, e2, editada=True)                     # repetido
            mundo.historico[peer] = [e2]
            await curadoria._varrer_canal(peer, ent)                        # e na varredura
            await drenar()
            s["sem_dup"] = (len(leituras), avais_de(mid), atuais_nerd(mid))
            # só o texto muda no mesmo segundo: o mesmo link passa a ser aval
            p = await chega(mundo, "promotom", GPU, amz("B0SEG00003"))
            mid2 = novo_id()
            t2 = _segundo()
            lc = "https://nerd.teste/c66"
            await pelo_handler(mundo, versao_nerd(mid2, "Promoção relâmpago!",
                                                  {lc: amz("B0SEG00003")}, edit_em=t2),
                               editada=True)
            await pelo_handler(mundo, versao_nerd(mid2, GPU_NERD, {lc: amz("B0SEG00003")},
                                                  edit_em=t2), editada=True)
            s["texto"] = retida("promotom", p)
            s["funil"] = (len([x for x in mundo.criados if "😱" in x[1]]),
                          [d for d in despachos if d == CHAT["nerdofertas"]])
        finally:
            desfaz()
    cenario(corpo)
    r.check(s["dois"] == (3, "liberada"), "66.duas_edicoes_no_mesmo_segundo_sao_duas_versoes",
            str(s["dois"]))
    a = dict(s["sem_dup"][1])
    r.check(s["sem_dup"][0] == 3 and len(s["sem_dup"][1]) == 2
            and s["sem_dup"][2] == ["amazon|B0SEG00001", "amazon|B0SEG00002"],
            "66.mesma_versao_nao_relida_nem_duplicada", str(s["sem_dup"]))
    r.check(abs(a["amazon|B0SEG00001"] - (time.time() - 200)) < 5
            and abs(a["amazon|B0SEG00002"] - time.time()) < 5,
            "66.permanece_conserva_acrescentado_pelo_edit_date", str(s["sem_dup"][1]))
    r.check(s["texto"] == "liberada", "66.so_o_texto_muda_no_mesmo_segundo", str(s["texto"]))
    r.check(s["funil"] == (0, []), "66.nerd_nunca_no_funil", str(s["funil"]))


def test_67_atrasada_no_mesmo_segundo_a_varredura_corrige(r):
    """O update de uma edição MAIS VELHA chega atrasado no MESMO segundo da
    mais nova (a data não desempata): a varredura, que traz o conteúdo
    ATUAL do servidor, reaplica a versão certa — o conjunto atual volta a
    ser o da mais nova e nenhum aval é duplicado. A atrasada de um segundo
    antes é ignorada na hora."""
    s = {}

    async def corpo(mundo):
        leituras, _, desfaz = _espiar_leituras()
        try:
            peer, ent = PEER["nerdofertas"], ENT["nerdofertas"]
            mid = novo_id()
            la, lb = "https://nerd.teste/a67", "https://nerd.teste/b67"
            t = _segundo()
            velha = versao_nerd(mid, GPU_NERD, {la: amz("B0ATR00001")}, idade_s=100, edit_em=t)
            nova = versao_nerd(mid, GPU_NERD, {lb: amz("B0ATR00002")}, idade_s=100, edit_em=t)
            await pelo_handler(mundo, nova, editada=True)
            await pelo_handler(mundo, velha, editada=True)                   # atrasada
            s["errado"] = atuais_nerd(mid)
            mundo.historico[peer] = [nova]                                   # o servidor
            await curadoria._varrer_canal(peer, ent)
            await drenar()
            s["corrigido"] = (atuais_nerd(mid), sorted(c for c, _ in avais_de(mid)))
            await curadoria._varrer_canal(peer, ent)                         # estável
            await drenar()
            n = len(leituras)
            antes = versao_nerd(mid, GPU_NERD, {la: amz("B0ATR00001")}, idade_s=100,
                                edit_em=t - dt.timedelta(seconds=1))
            await pelo_handler(mundo, antes, editada=True)                   # 1 s antes
            s["um_seg_antes"] = (len(leituras) - n, atuais_nerd(mid))
        finally:
            desfaz()
    cenario(corpo)
    r.check(s["errado"] == ["amazon|B0ATR00001"], "67.sem_varredura_o_empate_fica_na_ordem",
            str(s["errado"]))
    r.check(s["corrigido"] == (["amazon|B0ATR00002"], ["amazon|B0ATR00001", "amazon|B0ATR00002"]),
            "67.varredura_reaplica_a_atual_sem_duplicar", str(s["corrigido"]))
    r.check(s["um_seg_antes"] == (0, ["amazon|B0ATR00002"]),
            "67.edicao_de_segundo_anterior_ignorada", str(s["um_seg_antes"]))


def test_68_varredura_recupera_edicao_do_mesmo_segundo(r):
    """A edição do mesmo segundo que o handler PERDEU é achada pela
    varredura (outra assinatura) e solta a segurada; varredura repetida não
    relê."""
    s = {}

    async def corpo(mundo):
        leituras, _, desfaz = _espiar_leituras()
        try:
            peer, ent = PEER["nerdofertas"], ENT["nerdofertas"]
            q = await chega(mundo, "promotom", RX, amz("B0REC00002"))
            mid = novo_id()
            la, lb = "https://nerd.teste/a68", "https://nerd.teste/b68"
            t = _segundo()
            e1 = versao_nerd(mid, GPU_NERD, {la: amz("B0REC00001")}, edit_em=t)
            e2 = versao_nerd(mid, GPU_NERD, {la: amz("B0REC00001"), lb: amz("B0REC00002")},
                             edit_em=t)
            await pelo_handler(mundo, e1, editada=True)
            s["antes"] = retida("promotom", q)
            mundo.historico[peer] = [e2]                     # o handler não trouxe a e2
            await curadoria._varrer_canal(peer, ent)
            await drenar()
            s["achou"] = (retida("promotom", q), len(leituras))
            await curadoria._varrer_canal(peer, ent)
            await drenar()
            s["estavel"] = len(leituras)
        finally:
            desfaz()
    cenario(corpo)
    r.check(s["antes"] == "retida" and s["achou"] == ("liberada", 2) and s["estavel"] == 2,
            "68.varredura_acha_a_edicao_perdida_do_mesmo_segundo", str(s))


def test_69_nova_tentativa_de_versao_superada_no_mesmo_segundo(r):
    """A leitura da edição e1 falha (rede) e fica para nova tentativa; antes
    dela, chega a e2 do MESMO segundo: a nova tentativa da e1 é ignorada —
    não desfaz o conjunto da e2."""
    s = {}

    async def corpo(mundo):
        respostas = {"https://nerd.teste/a69": iter([([], True), (["amazon|B0TENT0001"], False)])}

        async def ident(links):
            if "https://nerd.teste/b69" in links:
                return ["amazon|B0TENT0002"], False
            return next(respostas["https://nerd.teste/a69"])
        real_rep = curadoria._REPETIR_S
        curadoria._identificar, curadoria._REPETIR_S = ident, (0.05, 0.05)
        try:
            mid = novo_id()
            t = _segundo()
            e1 = versao_nerd(mid, GPU_NERD, {"https://nerd.teste/a69": []}, edit_em=t)
            e2 = versao_nerd(mid, GPU_NERD, {"https://nerd.teste/b69": []}, edit_em=t)
            cbs = {type(ev).__name__: cb for cb, ev in mundo.handlers}
            await cbs["MessageEdited"](tl_events.MessageEdited.Event(e1))
            await asyncio.sleep(0)
            await cbs["MessageEdited"](tl_events.MessageEdited.Event(e2))
            await asyncio.sleep(0.15)
            await drenar()
            s["x"] = (atuais_nerd(mid), sorted(c for c, _ in avais_de(mid)))
        finally:
            curadoria._REPETIR_S = real_rep
    cenario(corpo)
    r.check(s["x"] == (["amazon|B0TENT0002"], ["amazon|B0TENT0002"]),
            "69.nova_tentativa_da_superada_ignorada", str(s["x"]))


def test_70_reavaliacao_de_robustez(r):
    """Achados da reavaliação geral: (a) a mensagem que expira só na memória
    não deixa contador de falha para trás; (b) canal de aval que não resolve
    no boot é ERRO explícito; (c) a 1ª varredura (boot) cobre a janela
    inteira mesmo com mais de _LOTE mensagens novas no Nerd."""
    s = {}

    async def corpo(mundo):
        real = (curadoria.db_curadoria_avaliar, curadoria._marcar_repor)
        curadoria.db_curadoria_avaliar = lambda *a, **k: (dbc.ERRO, False)
        curadoria._marcar_repor = lambda: None
        try:
            p = await chega(mundo, "fumotom", GPU, amz("B0ROBU0001"))
            k = (CHAT["fumotom"], p)
            s["antes"] = (k in curadoria._falhas, k in curadoria._PENDENTES)
            curadoria._PENDENTES[k]["limite"] = time.time() - 1
            curadoria._repor_pendentes()
            s["depois"] = (k in curadoria._falhas, k in curadoria._PENDENTES,
                           k in curadoria._SEGURADAS)
        finally:
            curadoria.db_curadoria_avaliar, curadoria._marcar_repor = real
        # (c) 45 mensagens novas no Nerd durante o reinício (todas na janela)
        peer, ent = PEER["nerdofertas"], ENT["nerdofertas"]
        alvo = await chega(mundo, "promotom", GPU, amz("B0ROBU0002"))
        hist = [msg_nerd(800000 + i, GPU_NERD, amz(f"B0RAJ{i:05d}"), idade_s=200 - i)
                for i in range(44)]
        hist.insert(0, msg_nerd(799999, GPU_NERD, amz("B0ROBU0002"), idade_s=250))  # a mais velha
        mundo.historico[peer] = hist
        await curadoria._varrer_canal(peer, ent)
        await drenar()
        s["rajada"] = (retida("promotom", alvo), len(mundo.varreduras))

    cenario(corpo)
    r.check(s["antes"] == (True, True) and s["depois"] == (False, False, False),
            "70a.expira_na_memoria_sem_deixar_contador", str(s))
    r.check(s["rajada"][0] == "liberada" and s["rajada"][1] >= 3,
            "70c.boot_cobre_a_janela_na_rajada", str(s["rajada"]))

    async def boot():
        g._init_globals()
        with _Logs() as logs:
            curadoria.instalar(Mundo(), FONTES, [], orchestrator.processar)
            await curadoria.encerrar()
        return logs
    logs = asyncio.run(boot())
    r.check(len(logs.tem("canal de aval ['@nerdofertas'] NÃO resolvido")) == 1,
            "70b.canal_de_aval_nao_resolvido_e_erro", str(logs.linhas)[-300:])


# ══════════════════════════════════════════════════════════════════
# PENDÊNCIAS FINAIS — aval do Fada sem afiliar; varredura cobre a janela
# ══════════════════════════════════════════════════════════════════
def test_71_aval_do_fada_nao_depende_da_afiliacao(r):
    """O aval do Fada é lido do post ORIGINAL, sem afiliar: se a afiliação
    (normalização) do post do Fada falha e ele não sai, a segurada do
    Promotom com o mesmo produto é solta assim mesmo. O portão não grava
    mais aval do Fada; o post dele segue normal quando a afiliação dá
    certo. Fada dos Cupons continua sem dar aval."""
    s = {}

    async def corpo(mundo):
        quem = []
        real = curadoria.db_curadoria_registrar_aval

        def espia(*a, **k):
            import inspect
            quem.append(inspect.stack()[2].function)
            return real(*a, **k)
        curadoria.db_curadoria_registrar_aval = espia
        try:
            p = await chega(mundo, "promotom", GPU, amz("B0FAFI0001"))
            f = await chega(mundo, "fadapromos", GPU_FADA, amz("B0FAFI0001"),
                            fada_normaliza=False)
            s["afiliacao_falhou"] = (retida("promotom", p), len(posts(mundo)),
                                     vinculo("promotom", p) == posts(mundo)[-1],
                                     vinculo("fadapromos", f))
            q = await chega(mundo, "fumotom", GPU, amz("B0FAFI0002"))
            g2 = await chega(mundo, "fadapromos", GPU_FADA, amz("B0FAFI0002"))
            s["normal"] = (retida("fumotom", q), vinculo("fadapromos", g2) is not None,
                           vinculo("fadapromos", g2) == vinculo("fumotom", q))
            t = await chega(mundo, "SamuelF3lipePromo", GPU, amz("B0FAFI0003"))
            await chega(mundo, "fadadoscupons", GPU, amz("B0FAFI0003"))
            s["cupons"] = retida("SamuelF3lipePromo", t)
            s["quem"] = sorted(set(quem))
        finally:
            curadoria.db_curadoria_registrar_aval = real
    cenario(corpo)
    r.check(s["afiliacao_falhou"] == ("liberada", 1, True, None),
            "71.afiliacao_do_fada_falhou_e_o_aval_vale", str(s["afiliacao_falhou"]))
    r.check(s["normal"] == ("liberada", True, True), "71.fada_publica_normal_e_da_aval",
            str(s["normal"]))
    r.check(s["cupons"] == "retida", "71.fada_dos_cupons_nao_da_aval", str(s["cupons"]))
    r.check(s["quem"] == ["_observar_aval"], "71.aval_so_pela_leitura_sem_afiliar",
            str(s["quem"]))


def test_72_varredura_cobre_a_janela_inteira(r):
    """Toda rodada lê TODA a janela (no boot e depois): 150 mensagens do Nerd
    dentro da janela — acima das 100 do teto antigo — e o aval mais velho é
    achado; numa rodada seguinte, outra rajada de 130 também; mensagem fora
    da janela para a leitura. Acima do teto de segurança, ERRO no log."""
    s = {}

    async def corpo(mundo):
        peer, ent = PEER["nerdofertas"], ENT["nerdofertas"]
        alvo = await chega(mundo, "promotom", GPU, amz("B0JAN150AA"))
        hist = [msg_nerd(810000 + i, GPU_NERD, amz(f"B0R1{i:06d}"), idade_s=280 - i * 1.5)
                for i in range(149)]
        hist.insert(0, msg_nerd(809999, GPU_NERD, amz("B0JAN150AA"), idade_s=290))  # a mais velha
        hist.insert(0, msg_nerd(809000, GPU_NERD, amz("B0FORAJAN1"), idade_s=900))  # fora
        hist = [msg_nerd(700000 + i, GPU_NERD, [], idade_s=5000 - i)                  # antigas
                for i in range(400)] + hist
        mundo.historico[peer] = hist
        mundo.varreduras.clear()
        await curadoria._varrer_canal(peer, ent)
        await drenar()
        with _db() as db:
            fora = db.execute("SELECT COUNT(*) FROM curadoria_aval WHERE chave=?",
                              ("amazon|B0FORAJAN1",)).fetchone()[0]
        s["boot"] = (retida("promotom", alvo), len(mundo.varreduras), fora)
        alvo2 = await chega(mundo, "fumotom", GPU, amz("B0JAN130BB"))
        rajada = [msg_nerd(820000 + i, GPU_NERD, amz(f"B0R2{i:06d}"), idade_s=60 - i * 0.3)
                  for i in range(129)]
        rajada.insert(0, msg_nerd(819999, GPU_NERD, amz("B0JAN130BB"), idade_s=70))
        mundo.historico[peer] = hist + rajada
        await curadoria._varrer_canal(peer, ent)
        await drenar()
        s["depois"] = retida("fumotom", alvo2)
        real = curadoria._PAGINAS_MAX
        curadoria._PAGINAS_MAX = 2
        try:
            with _Logs() as logs:
                await curadoria._varrer_canal(peer, ent)
            s["teto"] = len(logs.tem("[CURADORIA_VARREDURA_LIMITE]"))
        finally:
            curadoria._PAGINAS_MAX = real
    cenario(corpo)
    r.check(s["boot"][0] == "liberada" and 8 <= s["boot"][1] <= 9 and s["boot"][2] == 0,
            "72.boot_le_a_janela_inteira_e_para_nela", str(s["boot"]))
    r.check(s["depois"] == "liberada", "72.rodada_seguinte_tambem_cobre_rajada", str(s["depois"]))
    r.check(s["teto"] == 1, "72.teto_de_seguranca_tem_log", str(s.get("teto")))


# ══════════════════════════════════════════════════════════════════
# PÓS-DEPLOY (04/11/2026) — nomes públicos ativos; diagnóstico de releituras
# ══════════════════════════════════════════════════════════════════
def _canal_aliases(cid, principal, aliases):
    """Canal com vários @ (como o Nerd em produção: `username` vazio e os @
    em `usernames`, cada um com active)."""
    return types.Channel(id=cid, title="canal", photo=types.ChatPhotoEmpty(),
                         date=dt.datetime.now(dt.timezone.utc), access_hash=77,
                         username=principal, broadcast=True,
                         usernames=[types.Username(username=u, editable=False, active=a)
                                    for u, a in aliases])


def test_73_nomes_publicos_ativos(r):
    """username principal + usernames[*] só com active=True; minúsculas, sem
    "@", sem repetição, o principal primeiro."""
    np = curadoria.nomes_publicos
    r.check(np(_canal_aliases(1, None, [("NerdOfertas", True)])) == ["nerdofertas"],
            "73.username_ausente_alias_ativo")
    r.check(np(_canal_aliases(1, None, [("nerd_antigo", False)])) == [],
            "73.alias_inativo_nao_conta")
    r.check(np(_canal_aliases(1, "Promotom", [])) == ["promotom"], "73.username_principal")
    r.check(np(_canal_aliases(1, None, [("A1", True), ("b2", True), ("c3", False),
                                        ("a1", True)])) == ["a1", "b2"],
            "73.varios_aliases_sem_repeticao")
    r.check(np(_canal_aliases(1, "@Fumotom", [("FUMOTOM", True), ("Fumo2", True)]))
            == ["fumotom", "fumo2"], "73.caixa_e_arroba")
    r.check(np(object()) == [] and np(ENT["promotom"]) == ["promotom"],
            "73.entidade_sem_atributos_e_canal_comum")


def test_74_boot_confere_pelo_nome_publico_ativo(r):
    """O Nerd de produção (username vazio, @nerdofertas só em `usernames`)
    não gera mais o ERRO falso de 'NÃO resolvido'; o boot e os logs mostram
    o @; alias INATIVO não conta; nas fontes, o ID continua a autoridade e o
    @ é conferido também pelos aliases ativos."""
    nerd = _canal_aliases(_IDS["nerdofertas"], None, [("NerdOfertas", True)])
    fontes_alias = list(FONTES)
    i = next(k for k, e in enumerate(fontes_alias) if e.id == _IDS["promotom"])
    fontes_alias[i] = _canal_aliases(_IDS["promotom"], None, [("PROMOTOM", True)])
    fontes_outro = list(FONTES)
    fontes_outro[i] = _canal_aliases(_IDS["promotom"], None, [("outro_nome", True)])

    def boot(fontes, avais):
        async def _b():
            g._init_globals()
            # Como em produção: o cache de identidade do pipeline só guarda o
            # `username` principal — o Nerd (sem ele) não está lá.
            fora = _CHAT_USERNAME.pop(PEER["nerdofertas"], None)
            try:
                with _Logs() as logs:
                    curadoria.instalar(Mundo(), fontes, avais, orchestrator.processar)
                    nome = curadoria._nome(utils.get_peer_id(avais[0])) if avais else ""
                    papel_promotom = curadoria._papel(utils.get_peer_id(fontes[i]))
                    await curadoria.encerrar()
            finally:
                if fora is not None:
                    _CHAT_USERNAME[PEER["nerdofertas"]] = fora
            return logs, nome, papel_promotom
        return asyncio.run(_b())
    logs, nome, _ = boot(fontes_alias, [nerd])
    boot_linha = (logs.tem("🎯 curadoria ATIVO") or [""])[0]
    r.check(logs.tem("NÃO resolvido") == [] and logs.tem("responde como") == [],
            "74.alias_ativo_sem_erro_falso", str(logs.linhas)[-400:])
    r.check(f"@nerdofertas={PEER['nerdofertas']}" in boot_linha and "@promotom=" in boot_linha
            and nome == "@nerdofertas", "74.boot_e_logs_mostram_o_arroba",
            boot_linha[:300] + " | " + nome)
    logs2, _, _ = boot(FONTES, [_canal_aliases(_IDS["nerdofertas"], None,
                                               [("nerdofertas", False)])])
    r.check(len(logs2.tem("canal de aval ['@nerdofertas'] NÃO resolvido")) == 1,
            "74.alias_inativo_nao_confirma", str(logs2.linhas)[-300:])
    logs3, _, papel = boot(fontes_outro, [nerd])
    r.check(len(logs3.tem("configurado como @promotom responde como @outro_nome — vale o ID"))
            == 1 and papel == "RETÉM", "74.fonte_com_outro_arroba_vale_o_id",
            str(logs3.linhas)[-300:])


def test_75_diagnostico_de_releitura_sem_conteudo(r):
    """Instrumentação LIMITADA das releituras do aval: só quando a mesma
    mensagem é processada de novo sai [CURADORIA_AVAL_RELEITURA], com a
    origem (nova / edição / varredura / nova tentativa), o edit_date, a
    assinatura, os comprimentos do texto cru e do formatado, o resumo das
    entidades e a mídia — sem texto e sem URL. A versão repetida que a
    deduplicação descarta não loga. Assinatura e regras de versão: as
    mesmas (os avais não mudam)."""
    s = {}

    async def corpo(mundo):
        peer, ent = PEER["nerdofertas"], ENT["nerdofertas"]
        mid = novo_id()
        link = "https://nerd.teste/r75"
        cru = f"Placa de Vídeo RTX 5060 Ti 8GB\n\n🛒 {link}"
        IDENTS_AVAL[link] = ["amazon|B0DIAG0001"]

        def versao(bold=False):
            m = mensagem("nerdofertas", mid, cru, idade_s=30)
            if bold:                      # edição silenciosa: só a formatação
                m.entities = [types.MessageEntityBold(offset=0, length=13)]
                m._text = None
            return m
        with _Logs() as logs:
            m0 = versao()
            mundo.historico[peer] = [m0]
            await pelo_handler(mundo, m0)                       # nova
            await pelo_handler(mundo, m0)                       # mesma versão: sem log
            await curadoria._varrer_canal(peer, ent)            # varredura, mesma versão
            await drenar()
            s["sem_releitura"] = len(logs.tem("[CURADORIA_AVAL_RELEITURA]"))
            m1 = versao(bold=True)
            await pelo_handler(mundo, m1, editada=True)         # edição só de formato
            s["releituras"] = logs.tem("[CURADORIA_AVAL_RELEITURA]")
        s["avais"] = avais_de(mid)
    cenario(corpo)
    linha = s["releituras"][0] if s["releituras"] else ""
    r.check(s["sem_releitura"] == 0, "75.versao_repetida_nao_loga", str(s["sem_releitura"]))
    r.check(len(s["releituras"]) == 1 and "antes: origem=nova" in linha
            and "agora: origem=edicao" in linha and "ent=-" in linha and "ent=Bold:1" in linha,
            "75.releitura_mostra_origem_e_entidades", linha)
    cru = re.findall(r"cru=(\d+)", linha)
    fmt = re.findall(r"fmt=(\d+)", linha)
    r.check(len(cru) == 2 and cru[0] == cru[1] and fmt[0] != fmt[1],
            "75.cru_igual_formatado_diferente", linha)
    r.check("http" not in linha and "RTX" not in linha and "nerd.teste" not in linha,
            "75.sem_conteudo_nem_url", linha)
    r.check([c for c, _ in s["avais"]] == ["amazon|B0DIAG0001"],
            "75.aval_nao_duplica", str(s["avais"]))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CURADORIA · hardware só com aval · árvore real"))
