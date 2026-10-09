"""
F1.2-A2-R — RECUPERAÇÃO e REINJEÇÃO sobre a espinha de execução da A2-E:
a completude da entrada (buraco, busca e entrega da recuperada) e a
sucessão da chefe (transferência da liderança e reentrada como edição).

O contrato desta frente:
  · completude e sucessão NÃO abrem execução: os eventos delas saem fora
    de qualquer execução, correlacionados por chat, msg e post;
  · a mensagem reinjetada entra pelo MESMO `processar` (via RECUPERACAO ou
    SUCESSAO) e ganha nele a sua ÚNICA execução, raiz (sem exec_pai);
  · só fato real do catálogo vira evento — log sem fato não vira evento;
  · a exceção do pipeline vai só pela classe, em `excecao`.

Pelo caminho REAL: completude.observar → _agendar → _recuperar →
_buscar_e_entregar e sucessao.agendar → _rodar → _suceder, com o
`processar` real e as camadas do _pipeline trocadas pelas falsas da A2-E
(test_eventos_entrada), mensagens do Telethon REAL, SQLite REAL (o banco
temporário do harness) e lock_post real na sucessão. Só a rede do Telegram
(get_messages) é falsa.

  Contrato
    C01  os 11 locais ↔ (tipo, motivo) um a um, só em completude.py e
         sucessao.py, no catálogo e disjuntos dos da A1 e da A2-E; VERSAO 1
         com o hash recalculado (o catálogo global e "nenhum outro emissor"
         ficam com a frente mais nova: tests/test_eventos_exclusao.py)
    C02  forma exata de cada evento; sem exec nem exec_pai; exceção só pela
         classe; erro_tipo nunca vem do coletor; os 11 pontos exercitados
    C03  estrutura (AST): emissão logo depois do log; nada emitido sob o
         lock_post; liderança depois do COMMIT confirmado e antes do
         despachar; origem.buraco só nos ramos de buraco; o caminho quente
         (observar, a cada mensagem) sem emissão e sem célula de closure;
         await, create_task, lock, banco e despachar iguais à base
         (79ed70d); sentinela de `faltam` antes do try
    C04  coletores: lambdas, lista fechada de chamadas, auxiliar puro
  Completude
    F01  sucesso: buraco → recuperada → recebida(RECUPERACAO) → fim; a
         recuperada entra UMA vez pelo processar
    F02  buraco detectado (até 20 ids) e buraco grande (não recupera); sem
         salto de id, nenhum buraco
    F03  buraco que fecha sozinho: nenhuma busca, nenhum evento de recuperação
    F04  ausente e serviço: um evento por id, nenhuma execução
    F05  falha de busca: ERRO_BUSCA, sem nova tentativa
    F06  recuperada velha: NOVA_ANTIGA dentro da própria execução
    F07  exceção: ENTREGA_FALHOU (depois do fim ERRO) e ABORTADA
    F08  cancelamento na espera e na busca: nada sai, nada vaza
    F09  laço com vários ids: uma execução raiz por recuperada; o contexto
         volta ao normal entre elas
  Sucessão
    F10  sucesso: COMMIT real → liderança (prova confirmado) →
         recebida(SUCESSAO, edição) → fim
    F11  ciclo fechado
    F12  busca falhando (rede e fonte não monitorada)
    F13  nova rodada pedida durante a rodada: uma task, duas rodadas
    F14  sem fato de catálogo, sem evento (não assumiu, líder presente, sem
         origens, sucessora sumida)
    F15  cancelamento A) na busca; B) depois do COMMIT e da emissão, no
         despachar (B1 dentro do processar, B2 na fila) — sem rollback,
         sem segunda transferência; e exceção no despachar
    F16  contexto no NASCIMENTO das tasks _recuperar e _rodar: nenhum exec
         herdado, também pela cadeia real do aviso de exclusão; controle
         positivo: dentro de uma execução, a sonda vê a herança
  Regressão
    R01  diferencial desligado × ligado × sabotado em todos os cenários; a
         sabotagem acerta cada emissão e é detectada
    R02  dormente: nenhum coletor roda; mesmos pontos alcançados; caminho
         quente (observar sem buraco) sem emissão; custo com folga
    R03  interfaces preservadas; observar e agendar continuam síncronos
  Segurança
    S01  canários (texto, URL, segredo na exceção) em todos os cenários
    S02  encaminhada de usuário: nenhum id de usuário
    S03  tamanho: 1000 ids cabem no teto, cortados; buraco de 20 inteiro
    S04  correlação global: execução não duplicada nem órfã

    python tests/test_eventos_recuperacao.py
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import json
import os
import re
import sys
import time
import timeit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import telethon  # noqa: E402  — REAL, antes do harness
from telethon import utils  # noqa: E402
from telethon.tl import types  # noqa: E402

assert hasattr(telethon.TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from _harness_e5 import preparar, rodar  # noqa: E402

preparar()

import test_eventos_entrada as a2e                               # noqa: E402 — bancada da A2-E
import globals as g                                              # noqa: E402
import eventos                                                   # noqa: E402
from eventos import anel, catalogo, coleta                       # noqa: E402
from database import _init_db, db_get_post                       # noqa: E402
from database_conexao import _db                                 # noqa: E402
from pipeline import completude, orchestrator, origem_apagada, sucessao  # noqa: E402
from pipeline.completude import EventoRecuperado                 # noqa: E402
from utils.uma_por_vez import em_execucao                        # noqa: E402

regua = a2e.regua
_rodar, _ate = a2e._rodar, a2e._ate
_init_db()
g._init_globals()        # o boot do processo: locks dos pools (lock_post) e caches


# ─────────────────────────────────────────────────────────────────
# Fontes, ponto de entrada e banco
# ─────────────────────────────────────────────────────────────────
CANAL_A, CANAL_B, CANAL_C, CANAL_X = a2e.CANAL, a2e.CANAL + 1, a2e.CANAL + 2, a2e.CANAL + 9
ENT_A, ENT_B, ENT_C, ENT_X = (a2e._entidade(c) for c in (CANAL_A, CANAL_B, CANAL_C, CANAL_X))
CHAT_A, CHAT_B, CHAT_C, CHAT_X = (str(utils.get_peer_id(e)) for e in (ENT_A, ENT_B, ENT_C, ENT_X))

TEXTO = "Oferta recuperada R$ 10 https://amzn.to/rec1"
_URL_EXC = "https://x.co/CANARIOexc?tag=SEGREDO_TXT"


def _servico(mid, canal=CANAL_A):
    """Mensagem de SERVIÇO do Telethon real (fixar mensagem)."""
    m = types.MessageService(id=mid, peer_id=types.PeerChannel(canal), date=a2e._agora(),
                             action=types.MessageActionPinMessage())
    ent = a2e._entidade(canal)
    m._finish_init(a2e.TC, {int(utils.get_peer_id(ent)): ent}, None)
    return m


class _Quebra:
    """Resposta ilegível da busca: ler o id levanta (vira ABORTADA)."""

    @property
    def id(self):
        raise RuntimeError(f"resposta ilegível {a2e.SEGREDO_EXC} {_URL_EXC}")


class Fonte:
    """Telegram falso das fontes: SÓ get_messages, a única rede da
    completude e da sucessão. Devolve mensagens do Telethon REAL."""

    def __init__(self, msgs=(), *, erros=(), devolve=None, segura=False, antes=None):
        self.msgs = {(str(utils.get_peer_id(m.peer_id)), m.id): m for m in msgs}
        self.erros = list(erros)              # uma exceção por chamada, na ordem
        self.devolve = devolve                # resposta no lugar da lista
        self.segura = asyncio.Event() if segura else None
        self.antes = antes                    # gancho antes de devolver
        self.chamadas = []

    async def get_messages(self, ent, ids):
        chat = str(utils.get_peer_id(ent))
        self.chamadas.append((chat, list(ids)))
        if self.segura is not None:
            await self.segura.wait()
        await asyncio.sleep(0)                # rede: a busca sempre cede o laço
        if self.erros:
            raise self.erros.pop(0)
        if self.antes is not None:
            self.antes(chat, ids)
        if self.devolve is not None:
            return self.devolve
        return [self.msgs.get((chat, i)) for i in ids]


class Entrada:
    """O ponto de entrada: registra e repassa ao `processar` REAL."""

    def __init__(self):
        self.chamadas = []

    async def __call__(self, evento, is_edit=False):
        self.chamadas.append((str(evento.chat_id), evento.message.id, is_edit,
                              getattr(evento, "via", None)))
        await orchestrator.processar(evento, is_edit=is_edit)


async def _nada(*a, **k):
    return None


async def _enfileirar_quebra(ev, ie):
    """A mesma costura da F08 da A2-E: a admissão quebra dentro do processar."""
    raise a2e._EXC_ENTRADA


class _EnfileirarSuspenso:
    """Costura da F08 da A2-E: o `processar` real fica suspenso no
    `_enfileirar` — o cancelamento chega DENTRO do despachar. Só a primeira
    chamada suspende: uma reentrada a mais (segunda transferência) passa
    direto e aparece nas asserções, em vez de travar."""

    def __init__(self):
        self.entrou = asyncio.Event()
        self._nunca = asyncio.Event()
        self.chamadas = 0

    async def __call__(self, ev, ie):
        self.chamadas += 1
        if self.chamadas == 1:
            self.entrou.set()
            await self._nunca.wait()


def _gravar_post(dest, origens, lider, *, vivo=True):
    """Post e vínculos de origem gravados direto no banco de TESTE (o
    temporário do harness): a sucessão lê e transfere pelo banco real.
    `origens` em ordem de ligação, da mais velha para a mais nova."""
    agora = time.time()
    with _db() as db:
        db.execute("DELETE FROM origem_post WHERE dest=?", (dest,))
        db.execute("DELETE FROM post_estado WHERE msg_id_dest=?", (dest,))
        db.execute(
            "INSERT INTO post_estado(msg_id_dest, score, texto, plat, lider, janela_fim,"
            " edit_count, ts, lider_msg) VALUES(?,?,?,?,?,?,?,?,?)",
            (dest, 10, "post de teste", "shopee", lider[0],
             agora + (3600 if vivo else -60), 0, agora, lider[1]))
        for i, (chat, mid) in enumerate(origens):
            db.execute("INSERT OR REPLACE INTO origem_post(chat, msg_id, dest, ts)"
                       " VALUES(?,?,?,?)", (chat, mid, dest, agora - 100 + i))


def _desligar_origem(chat, mid):
    with _db() as db:
        db.execute("DELETE FROM origem_post WHERE chat=? AND msg_id=?", (chat, mid))


def _lider(dest):
    e = db_get_post(dest) or {}
    return e.get("lider"), e.get("lider_msg")


# Teto de espera por task: um defeito que trave vira falha de asserção, não
# trava a suíte.
_LIMITE_S = 10.0


async def _esperar(tarefa):
    """O desfecho da task, com teto: None, o nome da exceção, CANCELADA ou
    TEMPO_ESGOTADO (a task presa é cancelada)."""
    if tarefa is None:
        return None
    try:
        res = (await asyncio.wait_for(asyncio.gather(tarefa, return_exceptions=True),
                                      _LIMITE_S))[0]
    except asyncio.TimeoutError:
        return "TEMPO_ESGOTADO"
    if tarefa.cancelled():
        return "CANCELADA"
    return type(res).__name__ if isinstance(res, BaseException) else None


async def _drenar():
    """Espera a fila esvaziar, com teto. False: algo ficou preso (e foi
    cancelado)."""
    for _ in range(50):
        ts = [t for t in g._buf if isinstance(t, asyncio.Task) and not t.done()]
        if not ts:
            return True
        try:
            await asyncio.wait_for(asyncio.gather(*ts, return_exceptions=True), _LIMITE_S)
        except asyncio.TimeoutError:
            await asyncio.gather(*ts, return_exceptions=True)
            return False
    return False


# ─────────────────────────────────────────────────────────────────
# Cenários (o mesmo código roda desligado, ligado e sabotado)
# ─────────────────────────────────────────────────────────────────
class Cena:
    """Um cenário num modo: o que o código FEZ (a régua do diferencial) e os
    eventos que saíram."""

    _FUNCIONAL = ("fonte", "entrada", "sumidas", "camadas", "log", "estado_canal",
                  "resultado", "lider_no_desfecho", "repeticao", "lider", "em_curso",
                  "de_novo", "drenou", "buf", "workers", "lanes", "exec_fora")

    def __init__(self, **kw):
        self.fonte = self.entrada = self.sumidas = self.camadas = self.log = ()
        self.estado_canal = self.resultado = self.lider = self.exec_fora = None
        self.lider_no_desfecho = self.repeticao = None
        self.drenou = True
        self.em_curso = self.buf = self.workers = self.lanes = self.processar = 0
        self.de_novo, self.tarefas, self.eventos, self.delta = (), [], [], {}
        self.__dict__.update(kw)

    def funcional(self):
        return tuple(getattr(self, k) for k in self._FUNCIONAL)


def _fechar(cena, fonte, entrada, cam, log, reg, seq0, antes, n_handler):
    depois = coleta.saude_coleta()
    cena.fonte, cena.entrada = tuple(fonte.chamadas), tuple(entrada.chamadas)
    cena.camadas, cena.log = tuple(cam.chamadas), tuple(log.linhas)
    cena.buf, cena.workers, cena.lanes = len(g._buf), g._w_ativos, em_execucao()
    cena.eventos = a2e._eventos_desde(reg, seq0)
    cena.delta = {k: depois[k] - antes.get(k, 0) for k in depois}
    cena.processar = n_handler + len(entrada.chamadas)
    return cena


_COMPLETUDE = ("RECUPERADA", "FECHADO_SOZINHO", "GRANDE", "AUSENTE", "SERVICO", "ERRO_BUSCA",
               "ENTREGA_FALHOU", "ABORTADA", "VELHA", "VARIOS", "CANCELA_ESPERA",
               "CANCELA_BUSCA")
_BASE_C = {c: 710000 + 1000 * i for i, c in enumerate(_COMPLETUDE)}


async def _completude(cen, texto=TEXTO, **extra):
    """Um cenário da completude pelo caminho REAL: o handler (observar e
    depois processar, como em main.on_new), o buraco, a busca e a entrega
    ao MESMO processar."""
    m0 = _BASE_C[cen]
    salto = {"GRANDE": completude._MAX_BURACO + 2, "VARIOS": 4}.get(cen, 2)

    def msg(mid, **kw):
        return a2e.mensagem(mid, texto, **{**extra, **kw})

    na_fonte = {"RECUPERADA": [msg(m0 + 1)], "ENTREGA_FALHOU": [msg(m0 + 1)],
                "CANCELA_ESPERA": [msg(m0 + 1)], "CANCELA_BUSCA": [msg(m0 + 1)],
                "VELHA": [msg(m0 + 1, idade_s=600)], "SERVICO": [_servico(m0 + 1)],
                "VARIOS": [msg(m0 + 1), msg(m0 + 3)]}.get(cen, [])
    kw = {}
    if cen == "ERRO_BUSCA":
        kw["erros"] = [ConnectionError(f"rede caiu {a2e.SEGREDO_EXC} {_URL_EXC}")]
    if cen == "ABORTADA":
        kw["devolve"] = [_Quebra()]
    if cen == "CANCELA_BUSCA":
        kw["segura"] = True
    fonte, entrada = Fonte(na_fonte, **kw), Entrada()
    cam, log = a2e.Camadas("CONCLUSAO"), a2e.Log()
    reg, seq0 = a2e._seq_atual()
    antes = coleta.saude_coleta()
    salvo = (dict(completude._estado), dict(completude._canais), completude.log_ing,
             completude._ESPERA_S)
    completude.log_ing = log
    completude._ESPERA_S = 5.0 if cen == "CANCELA_ESPERA" else 0.05
    cena, pelo_handler = Cena(nome=cen, m0=m0, salto=salto), []
    try:
        completude.instalar(fonte, [ENT_A], entrada)
        with a2e.Bancada(cam, log):

            async def handler(m):
                completude.observar(m.chat_id, m.id)
                pelo_handler.append(m.id)
                try:
                    await orchestrator.processar(a2e.evento_telegram(m), is_edit=False)
                except Exception as e:                   # noqa: BLE001 — como main.on_new
                    log.error(f"❌ on_new: {e}")

            await handler(msg(m0))
            await handler(msg(m0 + salto))
            if cen == "FECHADO_SOZINHO":
                await asyncio.sleep(0)                   # a completude já espera…
                await handler(msg(m0 + 1))               # …e o update atrasado chega sozinho
            if cen == "ENTREGA_FALHOU":
                orchestrator._enfileirar = _enfileirar_quebra
            drenou = await _drenar()                     # a entrada termina antes da busca
            canal = completude._canais.get(int(CHAT_A))
            tarefa = canal.tarefa if canal is not None else None
            if tarefa is not None:
                if cen == "CANCELA_ESPERA":
                    await asyncio.sleep(0)
                    tarefa.cancel()
                if cen == "CANCELA_BUSCA":
                    await _ate(lambda: bool(fonte.chamadas))
                    tarefa.cancel()
                cena.resultado = await _esperar(tarefa)
            cena.drenou = await _drenar() and drenou
            cena.exec_fora = coleta.exec_atual()
            if canal is not None:
                cena.estado_canal = (canal.maior, tuple(sorted(canal.vistos)),
                                     tuple(sorted(canal.pendentes)))
    finally:
        completude._estado.clear()
        completude._estado.update(salvo[0])
        completude._canais.clear()
        completude._canais.update(salvo[1])
        completude.log_ing, completude._ESPERA_S = salvo[2], salvo[3]
    return _fechar(cena, fonte, entrada, cam, log, reg, seq0, antes, len(pelo_handler))


_SUCESSAO = ("SUCESSO", "CICLO_FECHADO", "BUSCA_FALHOU", "NAO_MONITORADA", "SUMIU",
             "NAO_ASSUMIU", "LIDER_PRESENTE", "SEM_ORIGENS", "NOVA_RODADA",
             "NOVA_RODADA_FALHA", "DESPACHAR_LEVANTA", "CANCELA_BUSCA",
             "CANCELA_DESPACHAR", "CANCELA_FILA")
_BASE_S = {c: 73000 + 10 * i for i, c in enumerate(_SUCESSAO)}
# Depois do desfecho, outro pedido para o mesmo post: prova que não há
# segunda transferência (a rodada nova encontra a líder presente).
_REPETE = ("SUCESSO", "SUMIU", "DESPACHAR_LEVANTA", "CANCELA_DESPACHAR", "CANCELA_FILA")


async def _sucessao(cen, texto=TEXTO, **extra):
    """Um cenário da sucessão pelo caminho REAL: agendar → _rodar →
    _suceder, banco e lock_post reais, a reentrada pelo MESMO processar.
    A líder (A, a1) já não está entre as origens: a fonte apagou."""
    dest = _BASE_S[cen]
    a1, b1, c1 = dest * 10 + 1, dest * 10 + 2, dest * 10 + 3

    def msg(mid, canal):
        return a2e.mensagem(mid, texto, canal=canal, **extra)

    origens, na_fonte, kw = [(CHAT_B, b1)], [msg(b1, CANAL_B)], {}
    if cen == "NAO_MONITORADA":
        origens = [(CHAT_X, b1)]
    if cen == "SUMIU":                       # a mais nova sumiu da fonte; a outra assume
        origens, na_fonte = [(CHAT_C, c1), (CHAT_B, b1)], [msg(c1, CANAL_C)]
    if cen == "LIDER_PRESENTE":
        origens = [(CHAT_A, a1), (CHAT_B, b1)]
    if cen == "SEM_ORIGENS":
        origens = []
    if cen in ("BUSCA_FALHOU", "NOVA_RODADA_FALHA"):
        kw["erros"] = [ConnectionError(f"rede caiu {a2e.SEGREDO_EXC} {_URL_EXC}")]
    if cen in ("NOVA_RODADA", "NOVA_RODADA_FALHA", "CANCELA_BUSCA"):
        kw["segura"] = True
    if cen == "NAO_ASSUMIU":                 # a sucessora se desliga durante a busca
        kw["antes"] = lambda chat, ids: _desligar_origem(CHAT_B, b1)
    _gravar_post(dest, origens, (CHAT_A, a1), vivo=cen != "CICLO_FECHADO")
    fonte, entrada, sumidas = Fonte(na_fonte, **kw), Entrada(), []

    async def ao_sumir(chat, ids):           # espião: origem.apagada está fora da A2-R
        sumidas.append((chat, list(ids)))

    cam = a2e.Camadas("BLOQUEIA" if cen == "CANCELA_FILA" else "CONCLUSAO")
    log, suspenso = a2e.Log(), _EnfileirarSuspenso()
    reg, seq0 = a2e._seq_atual()
    antes = coleta.saude_coleta()
    salvo = (dict(sucessao._estado), sucessao.log_out)
    sucessao.log_out = log
    cena = Cena(nome=cen, dest=dest, a1=a1, b1=b1, c1=c1)
    try:
        sucessao.instalar(fonte, [ENT_A, ENT_B, ENT_C], entrada, ao_sumir)
        with a2e.Bancada(cam, log):
            if cen == "DESPACHAR_LEVANTA":
                orchestrator._enfileirar = _enfileirar_quebra
            if cen == "CANCELA_DESPACHAR":
                orchestrator._enfileirar = suspenso
            sucessao.agendar(dest)
            tarefa = sucessao._EM_CURSO.get(dest)
            cena.tarefas = [tarefa]
            if cen in ("NOVA_RODADA", "NOVA_RODADA_FALHA"):
                await _ate(lambda: bool(fonte.chamadas))
                sucessao.agendar(dest)                   # pedido DURANTE a rodada
                cena.tarefas.append(sucessao._EM_CURSO.get(dest))
                fonte.segura.set()
            if cen == "CANCELA_BUSCA":
                await _ate(lambda: bool(fonte.chamadas))
                tarefa.cancel()
            if cen == "CANCELA_DESPACHAR":
                await _ate(suspenso.entrou.is_set)
                tarefa.cancel()
            cena.resultado = await _esperar(tarefa)
            if cen == "CANCELA_FILA":
                await _ate(lambda: any(c[0] == "enviar" for c in cam.chamadas))
                for t in [t for t in g._buf if isinstance(t, asyncio.Task)]:
                    t.cancel()
            drenou = await _drenar()
            cena.lider_no_desfecho = _lider(dest)       # antes de qualquer outra rodada
            if cen in _REPETE:
                sucessao.agendar(dest)
                cena.repeticao = await _esperar(sucessao._EM_CURSO.get(dest))
                drenou = await _drenar() and drenou
            cena.drenou = drenou
            cena.exec_fora = coleta.exec_atual()
        cena.em_curso, cena.de_novo = len(sucessao._EM_CURSO), tuple(sorted(sucessao._DE_NOVO))
        cena.lider = _lider(dest)
    finally:
        sucessao._estado.update(salvo[0])
        sucessao.log_out = salvo[1]
        sucessao._EM_CURSO.clear()
        sucessao._DE_NOVO.clear()
    cena.sumidas = tuple(sumidas)
    return _fechar(cena, fonte, entrada, cam, log, reg, seq0, antes, 0)


async def _pela_exclusao():
    """A cadeia REAL de produção até a sucessão: o aviso de exclusão
    (origem_apagada.agendar, o que o on_delete faz) → apagadas → post
    mantido → sucessao.agendar, com o ao_sumir de main.py."""
    dest = 73990
    a1, b1 = dest * 10 + 1, dest * 10 + 2
    _gravar_post(dest, [(CHAT_A, a1), (CHAT_B, b1)], (CHAT_A, a1))
    fonte, entrada = Fonte([a2e.mensagem(b1, TEXTO, canal=CANAL_B)]), Entrada()
    cam, log = a2e.Camadas("CONCLUSAO"), a2e.Log()
    reg, seq0 = a2e._seq_atual()
    salvo = (dict(sucessao._estado), sucessao.log_out, origem_apagada.log_out)
    sucessao.log_out = origem_apagada.log_out = log
    try:
        sucessao.instalar(fonte, [ENT_A, ENT_B, ENT_C], entrada, origem_apagada.apagadas)
        with a2e.Bancada(cam, log):
            origem_apagada.agendar(int(CHAT_A), [a1])
            for _ in range(50):
                ts = [t for t in list(origem_apagada._TAREFAS)
                      + list(sucessao._EM_CURSO.values()) if not t.done()]
                if not ts:
                    break
                for t in ts:
                    await _esperar(t)
            await _drenar()
        lider = _lider(dest)
    finally:
        sucessao._estado.update(salvo[0])
        sucessao.log_out, origem_apagada.log_out = salvo[1], salvo[2]
        sucessao._EM_CURSO.clear()
        sucessao._DE_NOVO.clear()
        origem_apagada._APAGADAS.clear()
    return a2e._eventos_desde(reg, seq0), lider, tuple(entrada.chamadas), b1


async def _dentro_de_uma_execucao():
    """Controle positivo da F16: as MESMAS chamadas, de dentro de uma
    execução aberta — a task nasce com o exec dela. As tasks são canceladas
    antes de rodar."""
    salvo = (dict(completude._estado), dict(completude._canais), completude.log_ing,
             dict(sucessao._estado))
    completude.log_ing = a2e.Log()
    try:
        completude.instalar(Fonte(), [ENT_A], Entrada())
        sucessao.instalar(Fonte(), [ENT_A], Entrada(), _nada)
        with eventos.execucao(lambda: {"chat": "controle", "msg": 1}):
            ex = coleta.exec_atual()
            completude.observar(int(CHAT_A), 1)
            completude.observar(int(CHAT_A), 3)          # buraco: _recuperar nasce aqui
            sucessao.agendar(73999)                       # _rodar nasce aqui
        tarefas = [completude._canais[int(CHAT_A)].tarefa, sucessao._EM_CURSO.get(73999)]
        for t in tarefas:
            t.cancel()
        await asyncio.gather(*tarefas, return_exceptions=True)
        return ex
    finally:
        completude._estado.clear()
        completude._estado.update(salvo[0])
        completude._canais.clear()
        completude._canais.update(salvo[1])
        completude.log_ing = salvo[2]
        sucessao._estado.update(salvo[3])
        sucessao._EM_CURSO.clear()
        sucessao._DE_NOVO.clear()


class SabotagemA2R(a2e.Sabotagem):
    """A da A2-E (os auxiliares dos coletores e o próprio anel levantam)
    mais o auxiliar que o coletor da A2-R usa: acerta a COLETA e a EMISSÃO
    dos pontos novos. O caminho funcional não pode notar."""

    def __enter__(self):
        super().__enter__()

        def quebra(*a, **k):
            raise RuntimeError("sabotagem")
        self.idade = completude.__dict__.get("_idade_seg")
        if self.idade is not None:
            completude._idade_seg = quebra
        return self

    def __exit__(self, *exc):
        if self.idade is not None:
            completude._idade_seg = self.idade
        return super().__exit__(*exc)


def _matriz(modo):
    """Todos os cenários num modo: {"C.<cenário>" | "S.<cenário>": Cena}."""
    a2e._desligar() if modo == "desligado" else a2e._ligar()
    res = {}
    for prefixo, f, cenas in (("C", _completude, _COMPLETUDE), ("S", _sucessao, _SUCESSAO)):
        for c in cenas:
            if modo == "sabotado":
                with SabotagemA2R():
                    res[f"{prefixo}.{c}"] = _rodar(f(c))
            else:
                res[f"{prefixo}.{c}"] = _rodar(f(c))
    return res


_CACHE = {}


def _ligada():
    if "ligado" not in _CACHE:
        _CACHE["ligado"] = _matriz("ligado")
    return _CACHE["ligado"]


# ─────────────────────────────────────────────────────────────────
# Contrato
# ─────────────────────────────────────────────────────────────────
_ARQ_A2R = ("pipeline/completude.py", "pipeline/sucessao.py")
LOCAL_A1 = "eventos.execucao.fim"

LOCAIS_A2R = {
    "completude.observar.buraco": ("origem.buraco", None),
    "completude.observar.buraco_grande": ("origem.buraco", None),
    "completude.buscar_e_entregar.erro_busca": ("origem.recuperacao_falhou", "ERRO_BUSCA"),
    "completude.buscar_e_entregar.ausente": ("origem.recuperacao_falhou", "AUSENTE"),
    "completude.buscar_e_entregar.servico": ("origem.recuperacao_falhou", "SERVICO"),
    "completude.buscar_e_entregar.recuperada": ("origem.recuperada", None),
    "completude.buscar_e_entregar.entrega_falhou": ("origem.recuperacao_falhou",
                                                    "ENTREGA_FALHOU"),
    "completude.recuperar.abortada": ("origem.recuperacao_falhou", "ABORTADA"),
    "sucessao.suceder.ciclo_fechado": ("post.sucessao_pendente", "CICLO_FECHADO"),
    "sucessao.suceder.busca_falhou": ("post.sucessao_pendente", "BUSCA_FALHOU"),
    "sucessao.suceder.lideranca_transferida": ("post.lideranca_transferida", None),
}
_ENUM_MOTIVO = {"origem.recuperacao_falhou": "motivo_recuperacao",
                "post.sucessao_pendente": "motivo_sucessao_pendente"}


TIPOS_A2R = {t for t, _ in LOCAIS_A2R.values()}


def _a2r(evs):
    """Os eventos da A2-R — pelo TIPO, nunca só pelo local: um ponto novo
    com local inventado também aparece (e as asserções o pegam)."""
    return [e for e in evs if e["tipo"] in TIPOS_A2R]


def _locais(evs):
    return [e["dados"]["local"] for e in _a2r(evs)]


def _enviar(cena):
    return [x for x in cena.camadas if x[0] == "enviar"]


def test_c01_onze_locais_e_catalogo_global(r):
    achados = [(rel, t, loc, m) for rel in _ARQ_A2R for t, loc, m in a2e._emissoes(rel)]
    locais = [loc for _, _, loc, _ in achados]
    r.check(len(locais) == len(set(locais)) == 11, "C01.onze_pontos_unicos", str(locais))
    r.check(set(locais) == set(LOCAIS_A2R), "C01.locais_do_contrato",
            str(sorted(set(locais) ^ set(LOCAIS_A2R))))
    errados = [(t, loc, m) for _, t, loc, m in achados if LOCAIS_A2R.get(loc) != (t, m)]
    r.check(errados == [], "C01.local_tipo_motivo_um_a_um", str(errados))
    r.check(all(loc.split(".")[0] == os.path.basename(rel)[:-3] for rel, _, loc, _ in achados),
            "C01.local_nomeia_o_proprio_modulo", str([(rel, loc) for rel, _, loc, _ in achados]))
    r.check(set(LOCAIS_A2R) <= catalogo.LOCAIS, "C01.onze_no_catalogo",
            str(sorted(set(LOCAIS_A2R) - catalogo.LOCAIS)))
    r.check(all(t in catalogo.TIPOS and catalogo.terminal(t) is None
                for t, _ in LOCAIS_A2R.values()), "C01.tipos_do_catalogo_nao_terminais")
    motivos = {}
    for t, m in LOCAIS_A2R.values():
        if m is not None:
            motivos.setdefault(_ENUM_MOTIVO[t], set()).add(m)
    r.check(all(motivos.get(enum) == catalogo.ENUMS[enum] for enum in _ENUM_MOTIVO.values()),
            "C01.motivos_do_catalogo_todos_ligados", str(motivos))
    # Só o escopo da A2-R, nunca uma lista global que cresce a cada frente:
    # os 11 são disjuntos dos da A1 e da A2-E e nenhum é emitido fora de
    # completude.py e sucessao.py. O catálogo global (a composição das
    # frentes) e "nenhum outro emissor" ficam com a frente mais nova
    # (tests/test_eventos_exclusao.py, C01).
    r.check(set(LOCAIS_A2R).isdisjoint(set(a2e.LOCAIS_A2E) | {LOCAL_A1}),
            "C01.frentes_disjuntas")
    fora = []
    for rel in regua._arquivos():
        if rel.split(os.sep)[0] == "eventos" or rel in _ARQ_A2R:
            continue
        if {loc for _, loc, _ in a2e._emissoes(rel)} & set(LOCAIS_A2R):
            fora.append(rel)
    r.check(fora == [], "C01.onze_so_em_completude_e_sucessao", str(fora))
    rc = catalogo.resumo()
    r.check(catalogo.VERSAO == 1 and rc["versao"] == 1
            and rc["hash"] == catalogo._hash(catalogo.TIPOS, catalogo.ENUMS, catalogo.LOCAIS)
            and re.fullmatch(r"[0-9a-f]{16}", rc["hash"] or "") is not None,
            "C01.versao_1_hash_recalculado", str(rc))


_FORMA = {
    "completude.observar.buraco": ({"chat"}, {"de", "ate", "tamanho", "faltam", "recupera"}),
    "completude.observar.buraco_grande": ({"chat"},
                                          {"de", "ate", "tamanho", "faltam", "recupera"}),
    "completude.buscar_e_entregar.erro_busca": ({"chat"}, {"motivo", "ids", "excecao"}),
    "completude.buscar_e_entregar.ausente": ({"chat", "msg"}, {"motivo", "ids"}),
    "completude.buscar_e_entregar.servico": ({"chat", "msg"}, {"motivo", "ids"}),
    "completude.buscar_e_entregar.recuperada": ({"chat", "msg"}, {"idade_s"}),
    "completude.buscar_e_entregar.entrega_falhou": ({"chat", "msg"},
                                                    {"motivo", "ids", "excecao"}),
    "completude.recuperar.abortada": ({"chat"}, {"motivo", "ids", "excecao"}),
    "sucessao.suceder.ciclo_fechado": ({"post"}, {"motivo"}),
    "sucessao.suceder.busca_falhou": ({"post", "chat", "msg"}, {"motivo", "excecao"}),
    "sucessao.suceder.lideranca_transferida": ({"post", "chat", "msg"},
                                               {"lider_anterior", "prova"}),
}
_CHAT_RE = re.compile(r"-100\d+")
_CLASSE_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _problemas_de_forma(e):
    loc, k, d = e["dados"].get("local"), e["corr"], e["dados"]
    if loc not in _FORMA:
        return [f"local fora do contrato {loc!r}"]
    corr_esp, dados_esp = _FORMA[loc]
    erros = []
    if set(k) != corr_esp:
        erros.append(f"corr {sorted(k)}")
    if set(d) != dados_esp | {"local", "ts_fato"}:
        erros.append(f"dados {sorted(d)}")
    if (e["tipo"], d.get("motivo")) != LOCAIS_A2R[loc]:
        erros.append(f"tipo/motivo {e['tipo']}/{d.get('motivo')}")
    if "chat" in k and not (isinstance(k["chat"], str) and _CHAT_RE.fullmatch(k["chat"])):
        erros.append(f"chat {k['chat']!r}")
    for chave in ("msg", "post"):
        if chave in k and type(k[chave]) is not int:
            erros.append(f"{chave} {k[chave]!r}")
    if "ids" in d and not (d["ids"] and all(type(i) is int for i in d["ids"])):
        erros.append(f"ids {d['ids']!r}")
    if "excecao" in d and not (isinstance(d["excecao"], str)
                               and _CLASSE_RE.fullmatch(d["excecao"])):
        erros.append(f"excecao {d['excecao']!r}")
    if e["tipo"] == "origem.buraco":
        ok = (all(type(d[c]) is int for c in ("de", "ate", "tamanho"))
              and d["tamanho"] == d["ate"] - d["de"] - 1 and type(d["recupera"]) is bool
              and (d["faltam"] is None) == (not d["recupera"]))
        if not ok:
            erros.append(f"buraco {d}")
    if e["tipo"] == "origem.recuperada" and not (isinstance(d["idade_s"], (int, float))
                                                 and d["idade_s"] >= 0):
        erros.append(f"idade_s {d['idade_s']!r}")
    if e["tipo"] == "post.lideranca_transferida":
        la, pv = d["lider_anterior"], d["prova"]
        if not (isinstance(la, dict) and set(la) == {"chat", "msg"}
                and isinstance(la["chat"], str)):
            erros.append(f"lider_anterior {la!r}")
        if pv != {"telegram": "nao_tocado", "banco": "confirmado"} or not (
                catalogo.valido("prova_telegram", pv.get("telegram"))
                and catalogo.valido("prova_banco", pv.get("banco"))):
            erros.append(f"prova {pv!r}")
    return erros


def test_c02_forma_dos_eventos(r):
    vistos, erros = set(), []
    for nome, c in _ligada().items():
        for e in _a2r(c.eventos):
            vistos.add(e["dados"]["local"])
            for p in _problemas_de_forma(e):
                erros.append(f"{nome}:{e['dados']['local']}: {p}")
    r.check(vistos == set(LOCAIS_A2R), "C02.os_onze_pontos_exercitados",
            str(sorted(set(LOCAIS_A2R) - vistos)))
    r.check(erros == [], "C02.forma_exata", str(erros[:6]))


# Base 79ed70d (ast.unparse), por função: o que a A2-R NÃO pode mudar.
_BASE_AWAIT = {
    ("pipeline/completude.py", "get_chat"): ["self.message.get_chat()"],
    ("pipeline/completude.py", "_recuperar"): ["_buscar_e_entregar(chat_id, canal, faltam)",
                                                "asyncio.sleep(_ESPERA_S)"],
    ("pipeline/completude.py", "_buscar_e_entregar"): [
        "_estado['cliente'].get_messages(_estado['entidades'][chat_id], ids=faltam)",
        "_estado['despachar'](EventoRecuperado(m), is_edit=False)"],
    ("pipeline/sucessao.py", "_rodar"): ["_suceder(dest)"],
    ("pipeline/sucessao.py", "_buscar"): ["_estado['cliente'].get_messages(ent, ids=[msg_id])"],
    ("pipeline/sucessao.py", "_suceder"): [
        "_buscar(chat, msg_id)", "_estado['ao_sumir'](chat, [msg_id])",
        "_estado['despachar'](EventoRecuperado(m, via='SUCESSAO'), is_edit=True)",
        "exclusao.lock_post(dest)"],
}
_BASE_CREATE_TASK = {
    ("pipeline/completude.py", "_agendar"): [
        "asyncio.get_running_loop().create_task(_recuperar(chat_id))"],
    ("pipeline/sucessao.py", "agendar"): ["asyncio.get_running_loop().create_task(_rodar(dest))"],
}
_BASE_DB = {("pipeline/sucessao.py", "_suceder"): ["db_get_post", "db_origens_do_post",
                                                   "db_transferir_lideranca"]}
_BASE_LOCK = {("pipeline/sucessao.py", "_suceder"): ["exclusao.lock_post(dest)"]}
_BASE_DESPACHAR = {
    ("pipeline/completude.py", "_buscar_e_entregar"): [
        "_estado['despachar'](EventoRecuperado(m), is_edit=False)"],
    ("pipeline/sucessao.py", "_suceder"): [
        "_estado['despachar'](EventoRecuperado(m, via='SUCESSAO'), is_edit=True)"],
}


def _por_funcao(filtro):
    res = {}
    for rel in _ARQ_A2R:
        for fn in ast.walk(regua._arvore(rel)):
            if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                achados = sorted(x for x in (filtro(n) for n in ast.walk(fn)) if x is not None)
                if achados:
                    res[(rel, fn.name)] = achados
    return res


def _e_emissao(s):
    return (isinstance(s, ast.Expr) and isinstance(s.value, ast.Call)
            and regua._nome_final(s.value.func) == "emitir_de")


def _local_de(call):
    return next((k.value.value for k in call.keywords if k.arg == "local"
                 and isinstance(k.value, ast.Constant)), None)


def _blocos(arv):
    for n in ast.walk(arv):
        for campo in ("body", "orelse", "finalbody"):
            bloco = getattr(n, campo, None)
            if isinstance(bloco, list) and bloco and isinstance(bloco[0], ast.stmt):
                yield bloco


def test_c03_estrutura(r):
    def chamada(pred):
        return lambda n: (ast.unparse(n) if isinstance(n, ast.Call) and pred(n) else None)
    atual = _por_funcao(lambda n: ast.unparse(n.value) if isinstance(n, ast.Await) else None)
    r.check(atual == {k: sorted(v) for k, v in _BASE_AWAIT.items()},
            "C03.nenhum_await_novo", str(atual))
    r.check(_por_funcao(chamada(lambda n: regua._nome_final(n.func) == "create_task"))
            == _BASE_CREATE_TASK, "C03.nenhuma_task_nova")
    r.check(_por_funcao(lambda n: ast.unparse(n.func) if isinstance(n, ast.Call)
                        and regua._nome_final(n.func).startswith("db_") else None) == _BASE_DB,
            "C03.nenhum_acesso_novo_a_banco")
    r.check(_por_funcao(chamada(lambda n: "lock" in regua._nome_final(n.func).lower()))
            == _BASE_LOCK, "C03.nenhum_lock_novo")
    r.check(_por_funcao(chamada(lambda n: "despachar" in ast.unparse(n.func)))
            == _BASE_DESPACHAR, "C03.mesmo_despachar_mesmo_processar")

    # Toda emissão sai LOGO DEPOIS do log de sempre.
    sem_log = []
    for rel in _ARQ_A2R:
        for bloco in _blocos(regua._arvore(rel)):
            for i, s in enumerate(bloco):
                if not _e_emissao(s):
                    continue
                ant = bloco[i - 1] if i > 0 else None
                if not (isinstance(ant, ast.Expr) and isinstance(ant.value, ast.Call)
                        and ast.unparse(ant.value.func).startswith(("log_ing.", "log_out."))):
                    sem_log.append(f"{rel}:{s.lineno}")
    r.check(sem_log == [], "C03.emissao_logo_depois_do_log", str(sem_log))

    # Nada emitido sob o lock_post (A.8): a sucessão emite depois do async with.
    suc = regua._arvore("pipeline/sucessao.py")
    sob_lock = [w.lineno for w in ast.walk(suc) if isinstance(w, ast.AsyncWith)
                for n in ast.walk(w) if _e_emissao(n)]
    r.check(sob_lock == [], "C03.nada_emitido_sob_o_lock_post", str(sob_lock))

    # A liderança: depois do COMMIT (async with) e do `if not assumiu`, e
    # imediatamente antes do despachar.
    suceder = a2e._funcao("pipeline/sucessao.py", "_suceder")
    laco = next((n for n in ast.walk(suceder) if isinstance(n, ast.For)), None)
    corpo = laco.body if laco is not None else []

    def indice(pred):
        return next((i for i, s in enumerate(corpo) if pred(s)), None)
    i_lock = indice(lambda s: isinstance(s, ast.AsyncWith))
    i_if = indice(lambda s: isinstance(s, ast.If) and ast.unparse(s.test) == "not assumiu")
    i_em = indice(lambda s: _e_emissao(s) and _local_de(s.value)
                  == "sucessao.suceder.lideranca_transferida")
    i_desp = indice(lambda s: isinstance(s, ast.Expr) and isinstance(s.value, ast.Await)
                    and "despachar" in ast.unparse(s.value))
    r.check(None not in (i_lock, i_if, i_em, i_desp) and i_lock < i_if < i_em
            and i_em + 1 == i_desp, "C03.lideranca_depois_do_commit_antes_do_despachar",
            str((i_lock, i_if, i_em, i_desp)))

    # origem.buraco SÓ nos ramos de buraco de _observar.
    obs = a2e._funcao("pipeline/completude.py", "_observar")
    pais = {f: n for n in ast.walk(obs) for f in ast.iter_child_nodes(n)}
    ramos = []
    for s in ast.walk(obs):
        if _e_emissao(s):
            se = pais.get(s)
            ramos.append(ast.unparse(se.test) if isinstance(se, ast.If) and s in se.body
                         else "FORA")
    r.check(sorted(ramos) == ["buraco > _MAX_BURACO", "faltam"],
            "C03.buraco_so_nos_ramos_de_buraco", str(ramos))
    outras = [f for f in ("observar", "_podar", "_agendar", "instalar")
              if any(_e_emissao(n) for n in ast.walk(a2e._funcao("pipeline/completude.py", f)))]
    r.check(outras == [], "C03.caminho_quente_sem_emissao", str(outras))
    # E sem custo escondido: o caminho quente (a cada mensagem) não ganha
    # célula de closure — um lambda que capturasse os locais de _observar
    # os tornaria células em TODA chamada, com ou sem buraco.
    celulas = {f: getattr(completude, f).__code__.co_cellvars
               + getattr(completude, f).__code__.co_freevars
               for f in ("observar", "_observar", "_podar", "_agendar")}
    r.check(all(v == () for v in celulas.values()), "C03.caminho_quente_sem_celula_de_closure",
            str(celulas))

    # Sentinela de `faltam` antes do try (A.3.6): o coletor da ABORTADA lê.
    rec = a2e._corpo_sem_docstring(a2e._funcao("pipeline/completude.py", "_recuperar"))
    i_try = next((i for i, s in enumerate(rec) if isinstance(s, ast.Try)), None)
    sent = rec[i_try - 1] if i_try else None
    r.check(isinstance(sent, (ast.Assign, ast.AnnAssign))
            and ast.unparse(sent.targets[0] if isinstance(sent, ast.Assign) else sent.target)
            == "faltam" and ast.unparse(sent.value) == "[]",
            "C03.sentinela_antes_do_try", ast.unparse(sent) if sent is not None else "")


_HELPERS_A2R = {"_idade_seg"}
_BUILTINS_A2R = {"str", "type", "round"}
_METODOS_A2R = {"get"}


def test_c04_coletores_lambdas_puras(r):
    estranhos, nao_lambda, corpos, minimos = [], [], 0, []
    for rel in _ARQ_A2R:
        arv = regua._arvore(rel)
        importados = a2e._importados(arv)
        for n in ast.walk(arv):
            if not (isinstance(n, ast.Call) and regua._nome_final(n.func) == "emitir_de"):
                continue
            alvos = [n.args[1]] if len(n.args) > 1 else []
            for k in n.keywords:
                if k.arg == "minimo":
                    alvos.append(k.value)
                    minimos.append(_local_de(n))
            for no in alvos:
                corpos += 1
                if not isinstance(no, ast.Lambda):
                    nao_lambda.append(f"{rel}:{n.lineno}")
                    continue
                for c in ast.walk(no.body):
                    if not isinstance(c, ast.Call):
                        continue
                    txt = ast.unparse(c.func)
                    if txt in _HELPERS_A2R:
                        continue
                    if isinstance(c.func, ast.Name):
                        if c.func.id in importados or c.func.id not in _BUILTINS_A2R:
                            estranhos.append(f"{rel}:{c.lineno} {txt}")
                    elif isinstance(c.func, ast.Attribute):
                        if (regua._raiz_de(c.func) in importados
                                or c.func.attr not in _METODOS_A2R):
                            estranhos.append(f"{rel}:{c.lineno} {txt}")
                    else:
                        estranhos.append(f"{rel}:{c.lineno} {txt}")
    r.check(corpos == 12, "C04.coletores_encontrados", str(corpos))
    r.check(nao_lambda == [], "C04.todo_coletor_e_lambda", str(nao_lambda))
    r.check(minimos == ["completude.buscar_e_entregar.recuperada"],
            "C04.minimo_so_onde_o_coletor_chama_auxiliar", str(minimos))
    r.check(estranhos == [], "C04.lista_fechada_de_chamadas", str(estranhos))
    arv_log = regua._arvore("logger.py")
    alvo = regua._funcoes(arv_log).get("_idade_seg")
    motivos = (regua._impureza(alvo, regua._funcoes(arv_log), {"_idade_seg"})
               if alvo is not None else ["ausente"])
    r.check(motivos == [], "C04.auxiliar_puro", str(motivos))


# ─────────────────────────────────────────────────────────────────
# Completude
# ─────────────────────────────────────────────────────────────────
def test_f01_recuperacao_com_sucesso(r):
    c = _ligada()["C.RECUPERADA"]
    m0, m1, m2 = c.m0, c.m0 + 1, c.m0 + 2
    a2r = _a2r(c.eventos)
    r.check(_locais(c.eventos) == ["completude.observar.buraco",
                                   "completude.buscar_e_entregar.recuperada"],
            "F01.eventos", str(_locais(c.eventos)))
    if len(a2r) != 2:
        return
    bur, rec = a2r
    r.check(bur["corr"] == {"chat": CHAT_A} and bur["dados"]["faltam"] == [m1]
            and (bur["dados"]["de"], bur["dados"]["ate"], bur["dados"]["tamanho"]) == (m0, m2, 1)
            and bur["dados"]["recupera"] is True, "F01.buraco", str(bur))
    r.check(rec["corr"] == {"chat": CHAT_A, "msg": m1} and 0 <= rec["dados"]["idade_s"] < 60,
            "F01.recuperada", str(rec))
    ex, evs = a2e._da_execucao(c.eventos, m1)
    r.check([e["tipo"] for e in evs] == ["origem.recebida", "execucao.fim"]
            and evs[0]["dados"]["via"] == "RECUPERACAO" and evs[0]["dados"]["is_edit"] is False
            and evs[0]["corr"]["chat"] == CHAT_A and "exec_pai" not in evs[0]["corr"]
            and evs[-1]["dados"]["resultado"] == "SEM_DESFECHO",
            "F01.entra_pelo_processar_numa_execucao_raiz", str(evs))
    rec0 = a2e._da_execucao(c.eventos, m0)[1]
    rec2 = a2e._da_execucao(c.eventos, m2)[1]
    r.check(bool(rec0 and rec2 and evs) and rec0[0]["seq"] < bur["seq"] < rec2[0]["seq"]
            < rec["seq"] < evs[0]["seq"] < evs[-1]["seq"], "F01.ordem")
    r.check([x for x in c.entrada if x[1] == m1] == [(CHAT_A, m1, False, "RECUPERACAO")],
            "F01.um_despachar", str(c.entrada))
    r.check(_enviar(c) == [("enviar", f"MONTADA:{m}", f"ENR:{m}", False) for m in (m0, m2, m1)],
            "F01.uma_publicacao_por_mensagem", str(_enviar(c)))
    r.check(c.delta.get("execucoes") == c.delta.get("fins") == 3 and c.exec_fora is None,
            "F01.tres_execucoes_tres_fins", str(c.delta))


def test_f02_buraco_detectado_e_buraco_grande(r):
    on = _ligada()
    v = on["C.VARIOS"]
    bur = (_a2r(v.eventos) or [{}])[0]
    r.check(bur.get("tipo") == "origem.buraco" and bur["corr"] == {"chat": CHAT_A}
            and bur["dados"]["faltam"] == [v.m0 + 1, v.m0 + 2, v.m0 + 3]
            and (bur["dados"]["de"], bur["dados"]["ate"], bur["dados"]["tamanho"])
            == (v.m0, v.m0 + 4, 3) and bur["dados"]["recupera"] is True,
            "F02.buraco_de_tres", str(bur))
    gr = on["C.GRANDE"]
    tam = completude._MAX_BURACO + 1
    evs = _a2r(gr.eventos)
    r.check(len(evs) == 1 and evs[0]["dados"]["local"] == "completude.observar.buraco_grande"
            and evs[0]["dados"]["recupera"] is False and evs[0]["dados"]["faltam"] is None
            and (evs[0]["dados"]["de"], evs[0]["dados"]["ate"], evs[0]["dados"]["tamanho"])
            == (gr.m0, gr.m0 + tam + 1, tam), "F02.buraco_grande_nao_recupera", str(evs))
    r.check(gr.fonte == () and gr.resultado is None and gr.estado_canal[2] == ()
            and gr.entrada == (), "F02.grande_sem_busca_nem_pendencia")

    # Sem salto de id não há buraco: primeiro id, sequência, edição antiga,
    # repetição — nenhum origem.buraco.
    a2e._ligar()
    reg, seq0 = a2e._seq_atual()
    salvo = (dict(completude._estado), dict(completude._canais), completude.log_ing)
    completude.log_ing = a2e.Log()
    try:
        completude.instalar(Fonte(), [ENT_A], Entrada())
        for i in (800000, 800001, 800002, 799990, 800002, 800003):
            completude.observar(int(CHAT_A), i)
        tarefa = completude._canais[int(CHAT_A)].tarefa
    finally:
        completude._estado.clear()
        completude._estado.update(salvo[0])
        completude._canais.clear()
        completude._canais.update(salvo[1])
        completude.log_ing = salvo[2]
    evs = a2e._eventos_desde(reg, seq0)
    r.check(evs == [] and tarefa is None, "F02.sem_salto_nenhum_buraco",
            str([(e["tipo"], e["dados"].get("local")) for e in evs]))


def test_f03_buraco_que_fecha_sozinho(r):
    c = _ligada()["C.FECHADO_SOZINHO"]
    m1 = c.m0 + 1
    r.check(_locais(c.eventos) == ["completude.observar.buraco"], "F03.so_o_buraco",
            str(_locais(c.eventos)))
    r.check(c.fonte == () and c.entrada == () and c.resultado is None,
            "F03.nenhuma_busca_nenhuma_reinjecao")
    ex, evs = a2e._da_execucao(c.eventos, m1)
    r.check([e["tipo"] for e in evs] == ["origem.recebida", "execucao.fim"]
            and evs[0]["dados"]["via"] == "TELEGRAM", "F03.atrasado_na_propria_execucao",
            str(evs))
    r.check(any(n == "debug" and "[BURACO_FECHADO]" in l for n, l in c.log),
            "F03.log_de_sempre", str(c.log))


def test_f04_ausente_e_servico(r):
    on = _ligada()
    for cen in ("AUSENTE", "SERVICO"):
        c = on[f"C.{cen}"]
        m1 = c.m0 + 1
        evs = _a2r(c.eventos)
        r.check(_locais(c.eventos) == ["completude.observar.buraco",
                                       f"completude.buscar_e_entregar.{cen.lower()}"],
                f"F04.{cen}.eventos", str(_locais(c.eventos)))
        f = evs[-1] if evs else {"corr": {}, "dados": {}}
        r.check(f["corr"] == {"chat": CHAT_A, "msg": m1} and f["dados"].get("motivo") == cen
                and f["dados"].get("ids") == [m1], f"F04.{cen}.forma", str(f))
        r.check(a2e._da_execucao(c.eventos, m1) == (None, []) and c.entrada == ()
                and c.fonte == ((CHAT_A, [m1]),), f"F04.{cen}.nenhuma_execucao")


def test_f05_falha_de_busca(r):
    c = _ligada()["C.ERRO_BUSCA"]
    m1 = c.m0 + 1
    evs = _a2r(c.eventos)
    r.check(_locais(c.eventos) == ["completude.observar.buraco",
                                   "completude.buscar_e_entregar.erro_busca"],
            "F05.eventos", str(_locais(c.eventos)))
    f = evs[-1] if evs else {"corr": {}, "dados": {}}
    r.check(f["corr"] == {"chat": CHAT_A} and f["dados"].get("ids") == [m1]
            and f["dados"].get("excecao") == "ConnectionError", "F05.forma", str(f))
    r.check(c.fonte == ((CHAT_A, [m1]),) and m1 in c.estado_canal[1]
            and c.estado_canal[2] == () and c.entrada == (), "F05.sem_nova_tentativa")
    r.check(any("[RECUPERACAO_FALHOU]" in l and "erro=ConnectionError" in l for _, l in c.log),
            "F05.log_de_sempre")


def test_f06_recuperada_velha(r):
    c = _ligada()["C.VELHA"]
    m1 = c.m0 + 1
    r.check(_locais(c.eventos) == ["completude.observar.buraco",
                                   "completude.buscar_e_entregar.recuperada"],
            "F06.eventos", str(_locais(c.eventos)))
    ex, evs = a2e._da_execucao(c.eventos, m1)
    r.check([e["tipo"] for e in evs] == ["origem.recebida", "origem.descartada", "execucao.fim"]
            and evs[0]["dados"]["via"] == "RECUPERACAO"
            and evs[1]["dados"]["motivo"] == "NOVA_ANTIGA"
            and evs[2]["dados"]["resultado"] == "DESCARTADA",
            "F06.nova_antiga_na_propria_execucao", str([e["tipo"] for e in evs]))
    r.check(c.entrada == ((CHAT_A, m1, False, "RECUPERACAO"),) and _enviar(c) == [
        ("enviar", f"MONTADA:{m}", f"ENR:{m}", False) for m in (c.m0, c.m0 + 2)],
        "F06.nada_publicado_da_velha")


def test_f07_excecao_entrega_e_abortada(r):
    on = _ligada()
    c = on["C.ENTREGA_FALHOU"]
    m1 = c.m0 + 1
    a2r = _a2r(c.eventos)
    r.check(_locais(c.eventos) == ["completude.observar.buraco",
                                   "completude.buscar_e_entregar.recuperada",
                                   "completude.buscar_e_entregar.entrega_falhou"],
            "F07.entrega.eventos", str(_locais(c.eventos)))
    f = a2r[-1] if a2r else {"corr": {}, "dados": {}, "seq": 0}
    r.check(f["corr"] == {"chat": CHAT_A, "msg": m1} and f["dados"].get("ids") == [m1]
            and f["dados"].get("excecao") == "RuntimeError", "F07.entrega.forma", str(f))
    ex, evs = a2e._da_execucao(c.eventos, m1)
    r.check([e["tipo"] for e in evs] == ["origem.recebida", "origem.descartada", "execucao.fim"]
            and evs[1]["dados"]["motivo"] == "ERRO_ENTRADA"
            and evs[2]["dados"]["resultado"] == "ERRO", "F07.entrega.execucao_da_recuperada",
            str([e["tipo"] for e in evs]))
    r.check(len(a2r) == 3 and len(evs) == 3
            and a2r[1]["seq"] < evs[0]["seq"] < evs[-1]["seq"] < f["seq"], "F07.entrega.ordem")
    r.check(("error", "🧩 entrega da recuperada falhou: RuntimeError") in c.log
            and c.resultado is None, "F07.entrega.contida_log_de_sempre")
    a = on["C.ABORTADA"]
    m1 = a.m0 + 1
    f = (_a2r(a.eventos) or [{"corr": {}, "dados": {}}])[-1]
    r.check(_locais(a.eventos) == ["completude.observar.buraco", "completude.recuperar.abortada"]
            and f["corr"] == {"chat": CHAT_A} and f["dados"].get("ids") == [m1]
            and f["dados"].get("excecao") == "RuntimeError", "F07.abortada", str(f))
    r.check(a.resultado is None and a.entrada == () and len(a.fonte) == 1
            and ("error", "🧩 completude: recuperação abortada (RuntimeError)") in a.log,
            "F07.abortada_contida")


def test_f08_cancelamento_da_completude(r):
    on = _ligada()
    for cen, buscas in (("CANCELA_ESPERA", 0), ("CANCELA_BUSCA", 1)):
        c = on[f"C.{cen}"]
        r.check(_locais(c.eventos) == ["completude.observar.buraco"], f"F08.{cen}.so_o_buraco",
                str(_locais(c.eventos)))
        r.check(c.resultado == "CANCELADA" and len(c.fonte) == buscas and c.entrada == (),
                f"F08.{cen}.cancelamento_propaga", f"{c.resultado} {c.fonte}")
        r.check(c.delta.get("execucoes") == c.delta.get("fins") == 2 and c.exec_fora is None
                and c.buf == c.workers == c.lanes == 0, f"F08.{cen}.nada_vaza", str(c.delta))


def test_f09_laco_com_varios_ids(r):
    c = _ligada()["C.VARIOS"]
    m0 = c.m0
    a2r = _a2r(c.eventos)
    r.check(_locais(c.eventos) == ["completude.observar.buraco",
                                   "completude.buscar_e_entregar.recuperada",
                                   "completude.buscar_e_entregar.ausente",
                                   "completude.buscar_e_entregar.recuperada"],
            "F09.eventos", str(_locais(c.eventos)))
    r.check([e["corr"].get("msg") for e in a2r[1:]] == [m0 + 1, m0 + 2, m0 + 3],
            "F09.um_por_id_na_ordem")
    execs = [a2e._da_execucao(c.eventos, m) for m in (m0 + 1, m0 + 3)]
    r.check(all(ex is not None and [e["tipo"] for e in evs] == ["origem.recebida", "execucao.fim"]
                and "exec_pai" not in evs[0]["corr"] for ex, evs in execs)
            and execs[0][0] != execs[1][0], "F09.uma_execucao_raiz_por_recuperada", str(execs))
    # A recuperada de m0+3 sai DEPOIS do processar de m0+1, no mesmo laço da
    # mesma task, e sem exec: o contexto voltou ao normal entre as entregas.
    r.check(len(a2r) == 4 and bool(execs[0][1])
            and execs[0][1][0]["seq"] < a2r[3]["seq"] and "exec" not in a2r[3]["corr"],
            "F09.contexto_restaurado_entre_entregas")
    r.check(c.entrada == ((CHAT_A, m0 + 1, False, "RECUPERACAO"),
                          (CHAT_A, m0 + 3, False, "RECUPERACAO")), "F09.duas_entregas",
            str(c.entrada))


# ─────────────────────────────────────────────────────────────────
# Sucessão
# ─────────────────────────────────────────────────────────────────
def test_f10_sucessao_com_sucesso(r):
    c = _ligada()["S.SUCESSO"]
    a2r = _a2r(c.eventos)
    r.check(_locais(c.eventos) == ["sucessao.suceder.lideranca_transferida"], "F10.eventos",
            str(_locais(c.eventos)))
    lid = a2r[0] if a2r else {"corr": {}, "dados": {}, "seq": 0}
    r.check(lid["corr"] == {"post": c.dest, "chat": CHAT_B, "msg": c.b1}, "F10.correlacao",
            str(lid["corr"]))
    r.check(lid["dados"].get("lider_anterior") == {"chat": CHAT_A, "msg": c.a1}
            and lid["dados"].get("prova") == {"telegram": "nao_tocado", "banco": "confirmado"},
            "F10.lider_anterior_e_prova", str(lid["dados"]))
    r.check(c.lider_no_desfecho == c.lider == (CHAT_B, c.b1), "F10.commit_real_no_banco",
            f"{c.lider_no_desfecho} {c.lider}")
    ex, evs = a2e._da_execucao(c.eventos, c.b1)
    r.check([e["tipo"] for e in evs] == ["origem.recebida", "execucao.fim"]
            and evs[0]["dados"]["via"] == "SUCESSAO" and evs[0]["dados"]["is_edit"] is True
            and evs[0]["corr"]["chat"] == CHAT_B and "exec_pai" not in evs[0]["corr"],
            "F10.reentra_pelo_processar_como_edicao", str(evs))
    r.check(bool(evs) and lid["seq"] < evs[0]["seq"] < evs[-1]["seq"], "F10.ordem")
    r.check(c.entrada == ((CHAT_B, c.b1, True, "SUCESSAO"),)
            and _enviar(c) == [("enviar", f"MONTADA:{c.b1}", f"ENR_EDICAO:{c.b1}", True)],
            "F10.uma_reentrada_uma_publicacao", str(c.entrada))
    r.check(c.delta.get("execucoes") == c.delta.get("fins") == 1 and c.exec_fora is None,
            "F10.uma_execucao", str(c.delta))


def test_f11_sucessao_ciclo_fechado(r):
    c = _ligada()["S.CICLO_FECHADO"]
    a2r = _a2r(c.eventos)
    r.check(len(a2r) == 1 and a2r[0]["dados"]["local"] == "sucessao.suceder.ciclo_fechado"
            and a2r[0]["corr"] == {"post": c.dest}
            and a2r[0]["dados"]["motivo"] == "CICLO_FECHADO", "F11.pendente_ciclo_fechado",
            str(a2r))
    r.check(c.fonte == () and c.entrada == () and c.lider == (CHAT_A, c.a1),
            "F11.nada_buscado_nada_muda")


def test_f12_sucessao_com_busca_falhando(r):
    on = _ligada()
    for cen, chat, exc, buscas in (("BUSCA_FALHOU", CHAT_B, "ConnectionError", 1),
                                   ("NAO_MONITORADA", CHAT_X, "LookupError", 0)):
        c = on[f"S.{cen}"]
        a2r = _a2r(c.eventos)
        r.check(len(a2r) == 1 and a2r[0]["dados"]["local"] == "sucessao.suceder.busca_falhou"
                and a2r[0]["corr"] == {"post": c.dest, "chat": chat, "msg": c.b1}
                and a2r[0]["dados"]["motivo"] == "BUSCA_FALHOU"
                and a2r[0]["dados"]["excecao"] == exc, f"F12.{cen}.pendente", str(a2r))
        r.check(len(c.fonte) == buscas and c.entrada == () and c.lider == (CHAT_A, c.a1),
                f"F12.{cen}.nada_muda")


def test_f13_nova_rodada_durante_a_rodada(r):
    on = _ligada()
    c = on["S.NOVA_RODADA"]
    r.check(len(c.tarefas) == 2 and c.tarefas[0] is not None and c.tarefas[0] is c.tarefas[1],
            "F13.uma_task_so")
    r.check(_locais(c.eventos) == ["sucessao.suceder.lideranca_transferida"]
            and len(c.fonte) == 1, "F13.segunda_rodada_silenciosa", str(_locais(c.eventos)))
    r.check(c.entrada == ((CHAT_B, c.b1, True, "SUCESSAO"),) and len(_enviar(c)) == 1
            and c.delta.get("execucoes") == c.delta.get("fins") == 1,
            "F13.sem_execucao_nem_publicacao_duplicada", str(c.delta))
    r.check(c.de_novo == () and c.em_curso == 0 and c.lider == (CHAT_B, c.b1),
            "F13.estado_limpo")
    f = on["S.NOVA_RODADA_FALHA"]
    r.check(_locais(f.eventos) == ["sucessao.suceder.busca_falhou",
                                   "sucessao.suceder.lideranca_transferida"]
            and len(f.fonte) == 2 and f.lider == (CHAT_B, f.b1), "F13.falha_depois_sucesso",
            str(_locais(f.eventos)))
    r.check(f.entrada == ((CHAT_B, f.b1, True, "SUCESSAO"),)
            and f.delta.get("execucoes") == f.delta.get("fins") == 1, "F13.falha_sucesso_uma_execucao")


def test_f14_sem_fato_de_catalogo_sem_evento(r):
    on = _ligada()
    for cen, buscas in (("NAO_ASSUMIU", 1), ("LIDER_PRESENTE", 0), ("SEM_ORIGENS", 0)):
        c = on[f"S.{cen}"]
        r.check(_a2r(c.eventos) == [] and c.eventos == [] and c.entrada == ()
                and c.lider == (CHAT_A, c.a1) and len(c.fonte) == buscas,
                f"F14.{cen}.nenhum_evento", str(_locais(c.eventos)))
    s = on["S.SUMIU"]
    a2r = _a2r(s.eventos)
    r.check(s.sumidas == ((CHAT_B, [s.b1]),), "F14.sumida_vai_para_o_ao_sumir", str(s.sumidas))
    r.check(_locais(s.eventos) == ["sucessao.suceder.lideranca_transferida"]
            and a2r[0]["corr"] == {"post": s.dest, "chat": CHAT_C, "msg": s.c1}
            and s.lider == (CHAT_C, s.c1) and s.fonte == ((CHAT_B, [s.b1]), (CHAT_C, [s.c1])),
            "F14.sumida_sem_evento_a_seguinte_assume", str(a2r))


def test_f15_cancelamento_e_excecao_da_sucessao(r):
    on = _ligada()
    # A) durante a busca
    a = on["S.CANCELA_BUSCA"]
    r.check(a.eventos == [], "F15.A.nenhuma_lideranca_nenhum_evento", str(_locais(a.eventos)))
    r.check(a.entrada == () and a.resultado == "CANCELADA" and len(a.fonte) == 1,
            "F15.A.nenhuma_reinjecao")
    r.check(a.delta.get("execucoes") == 0 and a.delta.get("fins") == 0,
            "F15.A.nenhuma_execucao_nova", str(a.delta))
    r.check(a.exec_fora is None and a.em_curso == 0 and a.de_novo == (),
            "F15.A.contexto_restaurado")
    r.check(a.lider_no_desfecho == a.lider == (CHAT_A, a.a1), "F15.A.lider_intacta",
            f"{a.lider_no_desfecho} {a.lider}")
    # B) depois do COMMIT e da emissão da liderança, durante o despachar
    for cen, tarefa in (("CANCELA_DESPACHAR", "CANCELADA"), ("CANCELA_FILA", None)):
        b = on[f"S.{cen}"]
        lids = [e for e in _a2r(b.eventos) if e["tipo"] == "post.lideranca_transferida"]
        r.check(_locais(b.eventos) == ["sucessao.suceder.lideranca_transferida"]
                and len(lids) == 1
                and lids[0]["corr"] == {"post": b.dest, "chat": CHAT_B, "msg": b.b1},
                f"F15.B.{cen}.uma_lideranca_nenhuma_segunda_transferencia",
                str(_locais(b.eventos)))
        # logo no desfecho do cancelamento (antes de qualquer outra rodada) e
        # depois da rodada de repetição: a liderança nova continua gravada
        r.check(b.lider_no_desfecho == b.lider == (CHAT_B, b.b1),
                f"F15.B.{cen}.lideranca_gravada_sem_rollback",
                f"{b.lider_no_desfecho} {b.lider}")
        ex, evs = a2e._da_execucao(b.eventos, b.b1)
        r.check([e["tipo"] for e in evs] == ["origem.recebida", "execucao.fim"]
                and evs[0]["dados"]["via"] == "SUCESSAO"
                and evs[-1]["dados"]["resultado"] == "CANCELADA",
                f"F15.B.{cen}.processar_trata_o_cancelamento", str(evs))
        r.check(bool(lids and evs) and lids[0]["seq"] < evs[0]["seq"], f"F15.B.{cen}.ordem")
        r.check(b.delta.get("execucoes") == b.delta.get("fins") == 1,
                f"F15.B.{cen}.uma_execucao", str(b.delta))
        r.check(b.entrada == ((CHAT_B, b.b1, True, "SUCESSAO"),) and b.resultado == tarefa
                and b.exec_fora is None and b.em_curso == 0, f"F15.B.{cen}.uma_reentrada",
                f"{b.entrada} {b.resultado}")
    b1 = on["S.CANCELA_DESPACHAR"]
    r.check(not any(e["tipo"] == "origem.descartada" for e in b1.eventos),
            "F15.B1.cancelamento_nao_e_erro_de_entrada")
    b2 = on["S.CANCELA_FILA"]
    r.check(_enviar(b2) == [("enviar", f"MONTADA:{b2.b1}", f"ENR_EDICAO:{b2.b1}", True)],
            "F15.B2.uma_publicacao_interrompida", str(_enviar(b2)))
    # exceção no despachar
    x = on["S.DESPACHAR_LEVANTA"]
    r.check(_locais(x.eventos) == ["sucessao.suceder.lideranca_transferida"],
            "F15.excecao.so_a_lideranca", str(_locais(x.eventos)))
    ex, evs = a2e._da_execucao(x.eventos, x.b1)
    r.check([e["tipo"] for e in evs] == ["origem.recebida", "origem.descartada", "execucao.fim"]
            and evs[1]["dados"]["motivo"] == "ERRO_ENTRADA"
            and evs[2]["dados"]["resultado"] == "ERRO", "F15.excecao.erro_de_entrada",
            str([e["tipo"] for e in evs]))
    r.check(x.lider == (CHAT_B, x.b1) and x.resultado is None
            and any(n == "error" and l.startswith(f"❌ sucessão post:{x.dest}:")
                    for n, l in x.log), "F15.excecao.contida_no_rodar")


def test_f16_contexto_no_nascimento_das_tasks(r):
    """N1/N2: asyncio.create_task copia os contextvars. A sonda é a fábrica
    de tasks do laço: no INSTANTE do create_task registra o exec de quem
    cria e o exec que a task levou no contexto dela, antes de rodar."""
    a2e._ligar()
    nascimentos = []

    def fabrica(loop, coro, **kw):
        de_quem_cria = coleta._EXEC.get()
        t = asyncio.Task(coro, loop=loop, **kw)
        herdado = t.get_context().get(coleta._EXEC)
        nascimentos.append((getattr(getattr(coro, "cr_code", None), "co_name", "?"),
                            de_quem_cria, herdado))
        return t

    def de(nome):
        return [n for n in nascimentos if n[0] == nome]

    a2e.LOOP.set_task_factory(fabrica)
    try:
        c = _rodar(_completude("RECUPERADA"))
        rec, fila = de("_recuperar"), de("uma_por_vez")
        r.check(len(rec) == 1 and rec[0][1:] == (None, None), "F16.N1.recuperar_nasce_sem_exec",
                str(rec))
        r.check(len(fila) == 3 and all(n[2] is not None and n[1] is n[2] for n in fila),
                "F16.N1.controle_a_fila_nasce_com_o_exec_do_processar",
                str([(n[0], n[2] is not None) for n in fila]))
        r.check(len(_a2r(c.eventos)) == 2, "F16.N1.cenario_rodou")
        nascimentos.clear()
        _rodar(_sucessao("SUCESSO"))
        rod = de("_rodar")
        r.check(len(rod) == 2 and all(n[1:] == (None, None) for n in rod),
                "F16.N2.rodar_nasce_sem_exec", str(rod))
        nascimentos.clear()
        evs, lider, entradas, b1 = _rodar(_pela_exclusao())
        ap, rod = de("apagadas"), de("_rodar")
        r.check(len(ap) == 1 and len(rod) == 1 and all(n[1:] == (None, None) for n in ap + rod),
                "F16.N2.cadeia_real_do_aviso_de_exclusao", str(ap + rod))
        r.check(lider == (CHAT_B, b1) and _locais(evs) == ["sucessao.suceder.lideranca_transferida"]
                and entradas == ((CHAT_B, b1, True, "SUCESSAO"),),
                "F16.N2.cadeia_real_transfere_e_reinjeta", f"{lider} {_locais(evs)} {entradas}")
        nascimentos.clear()
        ex = _rodar(_dentro_de_uma_execucao())
        rec, rod = de("_recuperar"), de("_rodar")
        r.check(ex is not None and len(rec) == 1 and len(rod) == 1
                and all(n[1] is n[2] and getattr(n[2], "id", None) == ex for n in rec + rod),
                "F16.controle_positivo_a_sonda_ve_a_heranca",
                str([(n[0], getattr(n[2], "id", None)) for n in rec + rod]) + f" exec={ex}")
    finally:
        a2e.LOOP.set_task_factory(None)


# ─────────────────────────────────────────────────────────────────
# Regressão (T2 diferencial)
# ─────────────────────────────────────────────────────────────────
def test_r01_desligado_ligado_sabotado(r):
    off = _matriz("desligado")
    on = _ligada()
    sab = _matriz("sabotado")
    for c in on:
        r.check(off[c].funcional() == on[c].funcional(), f"R01.{c}.ligado_igual",
                f"{off[c].funcional()} vs {on[c].funcional()}")
        r.check(off[c].funcional() == sab[c].funcional(), f"R01.{c}.sabotado_igual",
                f"{off[c].funcional()} vs {sab[c].funcional()}")
        r.check(off[c].eventos == [] and sab[c].eventos == [],
                f"R01.{c}.desligado_e_sabotado_sem_evento")
        d_on, d_sab = on[c].delta, sab[c].delta
        r.check(d_sab.get("chamadas") == d_on.get("chamadas")
                and d_sab.get("fins") == d_on.get("fins")
                and d_sab.get("falhas_internas") == d_sab.get("chamadas", 0) + d_sab.get("fins", 0),
                f"R01.{c}.sabotagem_acertou_cada_emissao",
                f"on={d_on} sab={d_sab}")
        n_rec = sum(1 for e in on[c].eventos if e["tipo"] == "origem.recuperada")
        r.check(d_sab.get("falhas_coleta", 0) >= n_rec, f"R01.{c}.sabotagem_acertou_o_coletor",
                f"{d_sab.get('falhas_coleta')} < {n_rec}")
    a2r_total = sum(len(_a2r(on[c].eventos)) for c in on)
    r.check(a2r_total >= 25 and sum(sab[c].delta.get("falhas_coleta", 0) for c in sab) > 0,
            "R01.sabotagem_detectada", str(a2r_total))


def test_r02_dormente_nada_roda(r):
    on = _ligada()
    a2e._desligar()
    vez = {}
    real_emitir, real_idade = eventos.emitir_de, completude.__dict__.get("_idade_seg")

    def conta(tipo, coletor, *, local, minimo=None):
        vez[local] = vez.get(local, 0) + 1
        return real_emitir(tipo, coletor, local=local, minimo=minimo)

    def idade(*a, **k):
        vez["_idade_seg"] = vez.get("_idade_seg", 0) + 1
        return real_idade(*a, **k)

    por_cena = {}
    eventos.emitir_de = conta
    if real_idade is not None:
        completude._idade_seg = idade
    try:
        for prefixo, f, cenas in (("C", _completude, _COMPLETUDE), ("S", _sucessao, _SUCESSAO)):
            for c in cenas:
                vez.clear()
                _rodar(f(c))
                por_cena[f"{prefixo}.{c}"] = dict(vez)
        # Caminho quente, LIGADO: observar sem buraco não chega a emitir_de.
        a2e._ligar()
        vez.clear()
        salvo = (dict(completude._estado), dict(completude._canais), completude.log_ing)
        completude.log_ing = a2e.Log()
        try:
            completude.instalar(Fonte(), [ENT_A], Entrada())
            for i in range(900000, 900500):
                completude.observar(int(CHAT_A), i)
            completude.observar(int(CHAT_A), 900100)          # edição de mensagem antiga
            quente = dict(vez)
        finally:
            completude._estado.clear()
            completude._estado.update(salvo[0])
            completude._canais.clear()
            completude._canais.update(salvo[1])
            completude.log_ing = salvo[2]
    finally:
        eventos.emitir_de = real_emitir
        if real_idade is not None:
            completude._idade_seg = real_idade
    for c, v in por_cena.items():
        pontos = sum(n for loc, n in v.items() if loc in LOCAIS_A2R)
        r.check(pontos == len(_a2r(on[c].eventos)), f"R02.{c}.mesmos_pontos_alcancados",
                f"{pontos} vs {len(_a2r(on[c].eventos))}")
        r.check(v.get("_idade_seg", 0) == 0, f"R02.{c}.nenhum_coletor_roda", str(v))
    r.check(quente == {}, "R02.caminho_quente_sem_emissao_mesmo_ligado", str(quente))
    a2e._desligar()
    x = 900001
    medidos = min(timeit.repeat(
        lambda: eventos.emitir_de("origem.buraco", lambda: ({"chat": "c"}, {"ate": x}),
                                  local="completude.observar.buraco"),
        number=20000, repeat=3)) / 20000 * 1e6
    r.check(medidos <= 5.0, "R02.custo_desligado_com_folga", f"{medidos:.3f} us")


def test_r03_interfaces_preservadas(r):
    def params(f):
        return [(p.name, p.default) for p in inspect.signature(f).parameters.values()]
    vazio = inspect.Parameter.empty
    esperado = {
        "completude.instalar": (completude.instalar,
                                [("cliente", vazio), ("fontes", vazio), ("despachar", vazio)]),
        "completude.observar": (completude.observar, [("chat_id", vazio), ("msg_id", vazio)]),
        "completude._recuperar": (completude._recuperar, [("chat_id", vazio)]),
        "completude._buscar_e_entregar": (completude._buscar_e_entregar,
                                          [("chat_id", vazio), ("canal", vazio),
                                           ("faltam", vazio)]),
        "sucessao.instalar": (sucessao.instalar,
                              [("cliente", vazio), ("fontes", vazio), ("despachar", vazio),
                               ("ao_sumir", vazio)]),
        "sucessao.agendar": (sucessao.agendar, [("dest", vazio)]),
        "sucessao._rodar": (sucessao._rodar, [("dest", vazio)]),
        "sucessao._suceder": (sucessao._suceder, [("dest", vazio)]),
    }
    for nome, (f, p) in esperado.items():
        r.check(params(f) == p, f"R03.{nome}", str(params(f)))
    r.check(not any(inspect.iscoroutinefunction(f) for f in (
        completude.observar, completude._observar, completude._agendar, completude.instalar,
        sucessao.agendar, sucessao.instalar)), "R03.observar_e_agendar_continuam_sincronos")
    r.check(completude.__all__ == ["instalar", "observar", "EventoRecuperado"]
            and sucessao.__all__ == ["instalar", "agendar"], "R03.__all__")
    m = a2e.mensagem(990001)
    r.check(EventoRecuperado.__slots__ == ("message", "via")
            and EventoRecuperado(m).via == "RECUPERACAO"
            and EventoRecuperado(m, via="SUCESSAO").via == "SUCESSAO", "R03.evento_recuperado")


# ─────────────────────────────────────────────────────────────────
# Segurança
# ─────────────────────────────────────────────────────────────────
def test_s01_canarios(r):
    a2e._ligar()
    ent = [types.MessageEntityTextUrl(0, 6, url="https://amzn.to/CANARIOent?tag=x")]
    evs = []
    for c in _COMPLETUDE:
        evs += _rodar(_completude(c, texto=a2e.TEXTO_CANARIO, entities=ent)).eventos
    for c in _SUCESSAO:
        evs += _rodar(_sucessao(c, texto=a2e.TEXTO_CANARIO, entities=ent)).eventos
    n = sum(1 for e in evs if e["dados"].get("local") in LOCAIS_A2R)
    r.check(n >= 25 and any(e["dados"].get("excecao") for e in evs), "S01.cenarios_emitiram",
            str(n))
    a2e._oraculo(r, "S01", evs)


def test_s02_encaminhada_de_usuario(r):
    a2e._ligar()
    fwd = types.MessageFwdHeader(date=a2e._agora(), from_id=types.PeerUser(987654321),
                                 from_name="Fulano Oculto")
    evs = (_rodar(_completude("RECUPERADA", fwd_from=fwd)).eventos
           + _rodar(_sucessao("SUCESSO", fwd_from=fwd)).eventos)
    bruto = json.dumps(evs, ensure_ascii=False)
    r.check(bool(evs) and "987654321" not in bruto and "Fulano" not in bruto,
            "S02.nenhum_dado_do_usuario")
    rein = [e for e in evs if e["tipo"] == "origem.recebida"
            and e["dados"].get("via") in ("RECUPERACAO", "SUCESSAO")]
    r.check(len(rein) == 2 and all(e["dados"].get("encaminhada") == {"canal": None, "msg": None}
                                   for e in rein), "S02.reinjetadas_sem_usuario", str(rein))


def test_s03_tamanho(r):
    reg = a2e._ligar()
    seq0 = reg.saude()["seq_ultimo"]
    ids = list(range(10 ** 6, 10 ** 6 + 1000))

    async def corpo():
        salvo = (dict(completude._estado), dict(completude._canais), completude.log_ing,
                 completude._ESPERA_S)
        completude.log_ing = a2e.Log()
        try:
            completude.instalar(Fonte(erros=[ConnectionError("x")]), [ENT_A], Entrada())
            await completude._buscar_e_entregar(int(CHAT_A), completude._Canal(1), list(ids))
            completude.instalar(Fonte(devolve=[_Quebra()]), [ENT_A], Entrada())
            completude._ESPERA_S = 0.0
            canal = completude._Canal(1)
            canal.pendentes = set(ids)
            completude._canais[int(CHAT_A)] = canal
            await completude._recuperar(int(CHAT_A))
            completude.instalar(Fonte(), [ENT_A], Entrada())
            completude.observar(int(CHAT_A), 5 * 10 ** 6)
            completude.observar(int(CHAT_A), 5 * 10 ** 6 + completude._MAX_BURACO + 1)
            t = completude._canais[int(CHAT_A)].tarefa
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)
        finally:
            completude._estado.clear()
            completude._estado.update(salvo[0])
            completude._canais.clear()
            completude._canais.update(salvo[1])
            completude.log_ing, completude._ESPERA_S = salvo[2], salvo[3]
    _rodar(corpo())
    evs = a2e._eventos_desde(reg, seq0)
    grandes = [e for e in evs if e["dados"].get("local") in (
        "completude.buscar_e_entregar.erro_busca", "completude.recuperar.abortada")]
    r.check(len(grandes) == 2 and all(e["dados"].get("cortado") is True for e in grandes),
            "S03.mil_ids_cortados_no_teto", str([sorted(e["dados"]) for e in grandes]))
    bur = [e for e in evs if e["dados"].get("local") == "completude.observar.buraco"
           and e["dados"].get("tamanho") == completude._MAX_BURACO]
    r.check(len(bur) == 1 and len(bur[0]["dados"]["faltam"]) == completude._MAX_BURACO
            and "cortado" not in bur[0]["dados"], "S03.buraco_de_vinte_inteiro", str(bur))
    cargas = reg.ler((reg.boot_id, seq0), 1000000, 1 << 30)["cargas"]
    r.check(bool(cargas) and all(len(c) <= coleta.TETO_EVENTO
                                 and anel._MARCA_CORTE.encode() not in c for c in cargas)
            and reg.saude()["excedidos"] == 0, "S03.tudo_cabe_sem_corte_do_anel",
            str(max((len(c) for c in cargas), default=0)))


def test_s04_correlacao_global(r):
    for nome, c in _ligada().items():
        r.check(c.drenou is True and "TEMPO_ESGOTADO" not in (c.resultado, c.repeticao),
                f"S04.{nome}.nada_preso", f"{c.drenou} {c.resultado} {c.repeticao}")
        por_exec = {}
        for e in c.eventos:
            ex = e["corr"].get("exec")
            if ex is not None:
                por_exec.setdefault(ex, []).append(e)
        r.check(all(evs[0]["tipo"] == "origem.recebida" and evs[-1]["tipo"] == "execucao.fim"
                    and [x["tipo"] for x in evs].count("execucao.fim") == 1
                    and [x["tipo"] for x in evs].count("origem.recebida") == 1
                    for evs in por_exec.values()),
                f"S04.{nome}.execucao_nao_duplicada_nem_orfa")
        r.check(len(por_exec) == c.delta.get("execucoes") == c.delta.get("fins") == c.processar,
                f"S04.{nome}.uma_execucao_por_processar",
                f"{len(por_exec)} {c.delta.get('execucoes')} {c.delta.get('fins')} {c.processar}")
        r.check(all("exec" not in e["corr"] and "exec_pai" not in e["corr"]
                    for e in _a2r(c.eventos)), f"S04.{nome}.a2r_fora_de_execucao")
        r.check(all("exec_pai" not in e["corr"] for e in c.eventos),
                f"S04.{nome}.nenhuma_execucao_aninhada")
        ok = True
        for evs in por_exec.values():
            via = evs[0]["dados"].get("via")
            if via in ("RECUPERACAO", "SUCESSAO"):
                tipo = "origem.recuperada" if via == "RECUPERACAO" else "post.lideranca_transferida"
                antes = [e for e in _a2r(c.eventos) if e["tipo"] == tipo
                         and e["seq"] < evs[0]["seq"]
                         and (e["corr"].get("chat"), e["corr"].get("msg"))
                         == (evs[0]["corr"].get("chat"), evs[0]["corr"].get("msg"))]
                ok = ok and len(antes) == 1
        r.check(ok, f"S04.{nome}.reinjecao_correlacionada_ao_fato")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "EVENTOS · recuperação e reinjeção (completude e sucessão) · "
                              "F1.2-A2-R"))
