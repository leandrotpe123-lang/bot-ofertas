"""
F1.2 — FATIA DA EXCLUSÃO: origem.apagada, a raiz EXCLUSAO, o `via` da B5 e a
raiz MANUTENCAO (a fatia que a A2-R registrou para antes da A2-O).

O contrato desta frente (F1.2: B.1, B.2, A.6, A.8 e B5; decisões 1 a 4 do
dono, 08/10):
  · raiz EXCLUSAO: UMA execução por id apagado, aberta em `apagadas` antes de
    `_uma`, nas duas vias — TELEGRAM (o aviso de exclusão) e SUCESSAO (a
    origem que a sucessão achou sumida da fonte). `agendar` não abre
    execução. A remoção física (convergencia.agendar_remocao) herda o exec;
    `sucessao.agendar` fica FORA da execução: a task da sucessão nasce sem
    exec (a N2 da A2-R continua valendo);
  · origem.apagada: UM ponto, em `apagadas`, logo depois de `_uma` — fora de
    todo lock, depois do log de sempre e antes do esquecimento, da remoção e
    da sucessão. As 7 situações; prova telegram nao_tocado; banco confirmado
    (MORTO, MANTIDO, SEM_POST, JA_REMOVIDO), falhou_sem_escrita (ERRO) e
    nao_aplicavel (SEM_VINCULO, MUDOU); restam = contagem em MANTIDO, 0 em
    MORTO, nulo nas outras; post só quando há post;
  · exceção inesperada em `_uma`: nenhum origem.apagada; a execução fecha em
    ERRO (marcar_desfecho, como o _blindado); log e laço de sempre;
  · via SUCESSAO: main.py injeta partial(apagadas, via="SUCESSAO");
    sucessao.py não muda;
  · raiz MANUTENCAO: main.py, em volta de convergencia.retomar_remocoes; as
    remoções retomadas herdam o exec; no except de sempre, ERRO.

Pelo caminho REAL: origem_apagada.agendar (o que o on_delete faz) →
apagadas → _uma (lock_origem, lock_post e SQLite reais, o banco temporário
do harness) → esquecimento → convergencia.agendar_remocao → _remover →
saida.apagar_post, e sucessao.agendar → _rodar → _suceder → processar (as
camadas do _pipeline trocadas pelas falsas da A2-E). Só a rede do Telegram é
falsa: delete_messages do destino e get_messages da fonte. O bloco da
MANUTENCAO e o `ao_sumir` da sucessão são o código do PRÓPRIO main.py,
extraído pela AST. Toda espera tem teto de TEMPO: a bancada nunca trava.

  Contrato
    C01  1 local, só em origem_apagada.py; catálogo global = A1 + A2-E +
         A2-R + exclusão, sem sobra nem falta; nenhum outro emissor; VERSAO 1
         com o hash recalculado; tipos e enums da fatia no catálogo
    C02  forma exata de origem.apagada nas 7 situações e do execucao.fim das
         raízes EXCLUSAO e MANUTENCAO; as 7 situações exercitadas
    C03  estrutura de origem_apagada (AST): execução por id em volta de
         `_uma`; emissão logo depois de `_uma`, antes do esquecimento;
         sucessao.agendar fora da execução; remoção dentro; nada em
         `agendar` nem sob lock; ERRO no except; await, create_task, lock e
         banco iguais à base
    C04  estrutura de main.py (AST): a raiz MANUTENCAO no mesmo lugar; o
         partial com via SUCESSAO; nenhuma outra execução ou emissão
  Exclusão (via TELEGRAM)
    F01  MORTO: evento, remoção física herdando o exec, post encerrado
    F02  MANTIDO (não era a chefe) e MANTIDO da chefe (sucessão real)
    F03  SEM_POST, JA_REMOVIDO e ERRO (rollback real do banco)
    F04  SEM_VINCULO (sem vínculo e corrida no banco) e MUDOU (esgotado)
    F05  exceção inesperada em `_uma`: nada de origem.apagada, ERRO, o laço
         segue para o próximo id
    F06  vários ids num aviso: uma execução por id, na ordem
    F07  repetição e duplicata concorrente: um MORTO só, uma remoção
    F08  concorrência entre duas fontes do mesmo post
    F09  cancelamento A) esperando o lock; B) depois do fato — sem rollback,
         sem evento a mais, sem remoção; o resto do aviso não roda
  Sucessão e manutenção
    F10  via SUCESSAO pela cadeia real (aviso → sucessão → sumida →
         apagadas): raiz própria, sem exec_pai; liderança e reentrada fora
    F11  contexto no NASCIMENTO das tasks: apagadas e _rodar sem exec;
         _remover com o exec da raiz (o controle positivo da sonda)
    F12  MANUTENCAO: remoções retomadas com o exec; vazio; ERRO
  Regressão
    R01  diferencial desligado × ligado × sabotado em todos os cenários; a
         sabotagem acerta abertura e emissão e é detectada
    R02  dormente: nenhum contador da coleta se move; custo com folga
    R03  interfaces preservadas
  Segurança
    S01  canários (segredo e URL na exceção e no banco) em nenhum evento
    S02  volume: 60 ids num aviso, nada cortado nem degradado
    S03  correlação global: cada execução com exatamente um fim

    python tests/test_eventos_exclusao.py
"""
from __future__ import annotations

import ast
import asyncio
import contextlib
import functools
import inspect
import json
import os
import sqlite3
import sys
import time
import timeit
import types as _pytypes

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import telethon  # noqa: E402  — REAL, antes do harness

assert hasattr(telethon.TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from _harness_e5 import preparar, rodar  # noqa: E402

preparar()

import test_eventos_entrada as a2e                               # noqa: E402 — bancada da A2-E
import test_eventos_recuperacao as a2r                           # noqa: E402 — bancada da A2-R
import client as modulo_client                                   # noqa: E402
import database_posts                                            # noqa: E402
import globals as g                                              # noqa: E402
import eventos                                                   # noqa: E402
from eventos import anel, catalogo, coleta                       # noqa: E402
from database_conexao import _db                                 # noqa: E402
from pipeline import convergencia, esquecimento, origem, origem_apagada, sucessao  # noqa: E402

regua = a2e.regua
_rodar = a2e._rodar
convergencia._ESPERA_BASE_S = 0.0           # só pesa em falha de remoção

CHAT_A, CHAT_B, CHAT_C = a2r.CHAT_A, a2r.CHAT_B, a2r.CHAT_C
ENT_A, ENT_B, ENT_C = a2r.ENT_A, a2r.ENT_B, a2r.ENT_C
LOCAL_EXC = "origem_apagada.apagadas.apagada"
_ARQ_EXC = ("pipeline/origem_apagada.py",)
_URL_EXC = "https://x.co/CANARIOexc?tag=SEGREDO_TXT"
_SITUACOES = ("MORTO", "MANTIDO", "SEM_POST", "JA_REMOVIDO", "ERRO", "SEM_VINCULO", "MUDOU")
_PROVA_BANCO = {"MORTO": "confirmado", "MANTIDO": "confirmado", "SEM_POST": "confirmado",
                "JA_REMOVIDO": "confirmado", "ERRO": "falhou_sem_escrita",
                "SEM_VINCULO": "nao_aplicavel", "MUDOU": "nao_aplicavel"}
_ARV_MAIN = regua._arvore("main.py")

# Teto de TEMPO por espera (nunca por número de voltas: a completude e a
# sucessão têm relógio próprio, e um laço que só conta voltas pode acabar
# antes dele).
_LIMITE_S = 10.0


async def _ate(cond, limite_s=_LIMITE_S):
    fim = time.monotonic() + limite_s
    while not cond():
        if time.monotonic() > fim:
            return False
        await asyncio.sleep(0)
    return True


# ─────────────────────────────────────────────────────────────────
# Telegram falso do destino, banco e sonda de nascimento
# ─────────────────────────────────────────────────────────────────
class Destino:
    """O canal de destino: só delete_messages, a remoção física."""

    def __init__(self):
        self.deletes = []

    async def delete_messages(self, dest, msg_id):
        await asyncio.sleep(0)                # rede: cede o laço
        self.deletes.append(msg_id)
        return True


def _post(dest, origens, lider=("", None), *, vivo=True, status=None):
    a2r._gravar_post(dest, origens, lider, vivo=vivo)
    if status is not None:
        with _db() as db:
            db.execute("UPDATE post_estado SET delete_status=? WHERE msg_id_dest=?",
                       (status, dest))


def _vinculo(chat, mid, dest):
    with _db() as db:
        db.execute("INSERT OR REPLACE INTO origem_post(chat, msg_id, dest, ts) VALUES(?,?,?,?)",
                   (chat, mid, dest, time.time()))


def _limpar(dests, links):
    with _db() as db:
        for d in dests:
            db.execute("DELETE FROM origem_post WHERE dest=?", (d,))
            db.execute("DELETE FROM post_estado WHERE msg_id_dest=?", (d,))
        for chat, mid in links:
            db.execute("DELETE FROM origem_post WHERE chat=? AND msg_id=?", (chat, mid))


def _banco(dests, links):
    """O que a exclusão grava no banco (sem relógio)."""
    agora, out = time.time(), []
    with _db() as db:
        for d in dests:
            org = sorted(tuple(r) for r in db.execute(
                "SELECT chat, msg_id FROM origem_post WHERE dest=?", (d,)).fetchall())
            pe = db.execute("SELECT delete_status, fused_into, lider, lider_msg, janela_fim"
                            " FROM post_estado WHERE msg_id_dest=?", (d,)).fetchone()
            out.append((d, tuple(org), None if pe is None
                        else (pe[0], pe[1], pe[2], pe[3], pe[4] <= agora)))
        for chat, mid in links:
            row = db.execute("SELECT dest FROM origem_post WHERE chat=? AND msg_id=?",
                             (chat, mid)).fetchone()
            out.append((chat, mid, row[0] if row else None))
    return tuple(out)


class Nascimentos:
    """Fábrica de tasks do laço: no INSTANTE do create_task registra o exec
    de quem cria e o que a task levou no contexto dela, antes de rodar."""

    def __init__(self):
        self.lista = []

    def __call__(self, loop, coro, **kw):
        cria = coleta._EXEC.get()
        t = asyncio.Task(coro, loop=loop, **kw)
        leva = t.get_context().get(coleta._EXEC)
        self.lista.append((getattr(getattr(coro, "cr_code", None), "co_name", "?"), cria, leva))
        return t

    def de(self, nome):
        return [n for n in self.lista if n[0] == nome]


class _ConexaoQuebrada:
    """O banco recusa o DELETE do vínculo: o db_desvincular_origem REAL faz
    ROLLBACK e devolve ("erro", 0)."""

    def __init__(self, db):
        self._db = db

    def execute(self, sql, *a):
        if sql.startswith("DELETE FROM origem_post"):
            raise sqlite3.OperationalError(f"disco cheio {a2e.SEGREDO_EXC} {_URL_EXC}")
        return self._db.execute(sql, *a)


@contextlib.contextmanager
def _db_quebrado():
    with _db() as db:
        yield _ConexaoQuebrada(db)


def _ao_sumir_de_main():
    """O 4º argumento de sucessao.instalar no main.py, avaliado: o que o
    processo injeta na sucessão como tratamento de origem sumida."""
    chamada = next(n for n in ast.walk(_ARV_MAIN) if isinstance(n, ast.Call)
                   and ast.unparse(n.func) == "sucessao.instalar")
    return eval(compile(ast.Expression(chamada.args[3]), "main.py", "eval"),
                {"functools": functools, "origem_apagada": origem_apagada})


def _preparar_processo():
    return next(n for n in ast.walk(_ARV_MAIN)
                if isinstance(n, ast.AsyncFunctionDef) and n.name == "_preparar_processo")


def _bloco_manutencao():
    """O comando do _preparar_processo que retoma as remoções no boot."""
    return next(st for st in _preparar_processo().body
                if any(isinstance(n, ast.Call)
                       and ast.unparse(n.func) == "convergencia.retomar_remocoes"
                       for n in ast.walk(st)))


async def _drenar_tudo():
    """Exclusão, remoção, sucessão e fila até o fim, com teto. False: algo
    ficou preso (e foi cancelado)."""
    for _ in range(100):
        ts = [t for t in (list(origem_apagada._TAREFAS) + list(convergencia._EM_CURSO.values())
                          + list(sucessao._EM_CURSO.values())
                          + [t for t in g._buf if isinstance(t, asyncio.Task)])
              if not t.done()]
        if not ts:
            return True
        try:
            await asyncio.wait_for(asyncio.gather(*ts, return_exceptions=True), _LIMITE_S)
        except asyncio.TimeoutError:
            for t in ts:
                t.cancel()
            await asyncio.gather(*ts, return_exceptions=True)
            return False
    return False


async def _desfecho(t):
    """(desfecho da task, o que ela devolveu)."""
    res = await a2r._esperar(t)
    if res is None and t is not None and t.done() and not t.cancelled():
        return (None, tuple(t.result()))
    return (res, None)


# ─────────────────────────────────────────────────────────────────
# Cenários (o mesmo código roda desligado, ligado e sabotado)
# ─────────────────────────────────────────────────────────────────
class Cena:
    """Um cenário num modo: o que o código FEZ (a régua do diferencial) e os
    eventos que saíram."""

    _FUNCIONAL = ("resultados", "banco", "deletes", "log", "fonte", "entrada", "camadas",
                  "lembradas", "nascidas", "lider", "em_curso", "de_novo", "drenou",
                  "exec_fora")

    def __init__(self, **kw):
        self.resultados = self.banco = self.deletes = self.log = self.fonte = ()
        self.entrada = self.camadas = self.lembradas = self.nascidas = self.de_novo = ()
        self.lider = self.exec_fora = None
        self.em_curso, self.drenou = 0, True
        self.eventos, self.nasc, self.delta = [], [], {}
        self.__dict__.update(kw)

    def funcional(self):
        return tuple(getattr(self, k) for k in self._FUNCIONAL)


_EXCLUSAO = ("MORTO", "MANTIDO", "MANTIDO_CHEFE", "SEM_POST", "JA_REMOVIDO", "ERRO",
             "SEM_VINCULO", "SEM_VINCULO_BANCO", "MUDOU", "EXCECAO", "VARIOS", "REPETICAO",
             "DUPLICADO", "CONCORRENTE", "CANCELA_LOCK", "CANCELA_DEPOIS", "VIA_SUCESSAO",
             "MASSA")
_BASE_X = {c: 91000 + 100 * i for i, c in enumerate(_EXCLUSAO)}
_CONCORRENTES = ("DUPLICADO", "CONCORRENTE")
_N_MASSA = 60


async def _exclusao(cen):
    """Um cenário da exclusão pelo caminho REAL, do agendar (o on_delete)
    ao fim de toda task que ele gera."""
    dest = _BASE_X[cen]
    a1, b1, c1, a2, a3 = (dest * 10 + k for k in range(1, 6))
    outro, mudou = dest + 50, [dest + k for k in range(1, 6)]
    massa = [dest * 10 + k for k in range(1, _N_MASSA + 1)]
    links = [(CHAT_A, a1), (CHAT_B, b1), (CHAT_C, c1), (CHAT_A, a2), (CHAT_A, a3)]
    links += [(CHAT_A, m) for m in massa]
    _limpar([dest, outro] + mudou, links)
    avisos, na_fonte, patches = [(CHAT_A, [a1])], [], []

    if cen in ("MORTO", "ERRO", "SEM_VINCULO_BANCO", "MUDOU", "REPETICAO", "DUPLICADO",
               "CANCELA_DEPOIS"):
        _post(dest, [(CHAT_A, a1)])
    if cen == "MANTIDO":
        _post(dest, [(CHAT_A, a1), (CHAT_B, b1)], (CHAT_B, b1))
    if cen == "MANTIDO_CHEFE":
        _post(dest, [(CHAT_A, a1), (CHAT_B, b1)], (CHAT_A, a1))
        na_fonte = [a2e.mensagem(b1, a2r.TEXTO, canal=a2r.CANAL_B)]
    if cen == "SEM_POST":
        _vinculo(CHAT_A, a1, dest)
    if cen == "JA_REMOVIDO":
        _post(dest, [(CHAT_A, a1)], vivo=False, status="ok")
    if cen == "EXCECAO":
        _post(dest, [(CHAT_A, a2)])
        avisos = [(CHAT_A, [a1, a2])]
    if cen == "VARIOS":
        _post(dest, [(CHAT_A, a1), (CHAT_B, b1)], (CHAT_B, b1))
        _post(outro, [(CHAT_A, a3)])
        avisos = [(CHAT_A, [a1, a2, a3])]              # a2: sem vínculo
    if cen == "REPETICAO":
        avisos = [(CHAT_A, [a1]), (CHAT_A, [a1])]
    if cen == "DUPLICADO":
        avisos = [(CHAT_A, [a1]), (CHAT_A, [a1])]
    if cen == "CONCORRENTE":
        _post(dest, [(CHAT_A, a1), (CHAT_B, b1)])
        avisos = [(CHAT_A, [a1]), (CHAT_B, [b1])]
    if cen == "CANCELA_LOCK":
        _post(dest, [(CHAT_A, a1)])
        _post(outro, [(CHAT_A, a2)])
        avisos = [(CHAT_A, [a1, a2])]
    if cen == "VIA_SUCESSAO":                          # B, a mais nova, sumiu da fonte
        _post(dest, [(CHAT_A, a1), (CHAT_C, c1), (CHAT_B, b1)], (CHAT_A, a1))
        na_fonte = [a2e.mensagem(c1, a2r.TEXTO, canal=a2r.CANAL_C)]
    if cen == "MASSA":
        avisos = [(CHAT_A, massa)]

    if cen == "ERRO":
        patches.append((database_posts, "_db", _db_quebrado))
    if cen in ("SEM_VINCULO_BANCO", "MUDOU"):
        real = origem_apagada.db_desvincular_origem
        prox = iter(mudou)

        def corrida(chat, mid, d, agora, _real=real):
            if cen == "SEM_VINCULO_BANCO":            # o vínculo saiu entre a leitura e a transação
                a2r._desligar_origem(chat, mid)
            else:                                     # fusão repontou o vínculo sob outro lock
                _vinculo(chat, mid, next(prox))
            return _real(chat, mid, d, agora)
        patches.append((origem_apagada, "db_desvincular_origem", corrida))
    if cen == "EXCECAO":
        real_lock = origem.lock_origem

        async def lock_quebra(chat, mid, _real=real_lock):
            if mid == a1:
                raise RuntimeError(f"lock quebrou {a2e.SEGREDO_EXC} {_URL_EXC}")
            return await _real(chat, mid)
        patches.append((origem, "lock_origem", lock_quebra))
    entrou = asyncio.Event()
    if cen == "CANCELA_DEPOIS":
        async def post_saiu_preso(d):
            entrou.set()
            await asyncio.Event().wait()
        patches.append((esquecimento, "post_saiu", post_saiu_preso))

    destino, fonte, entrada = Destino(), a2r.Fonte(na_fonte), a2r.Entrada()
    cam, log, nasc = a2e.Camadas("CONCLUSAO"), a2e.Log(), Nascimentos()
    patches += [(modulo_client, "client", destino), (origem_apagada, "log_out", log),
                (convergencia, "log_out", log), (sucessao, "log_out", log),
                (database_posts, "log_db", log)]
    salvos = [(m, n, getattr(m, n)) for m, n, _ in patches]
    reg, seq0 = a2e._seq_atual()
    antes = coleta.saude_coleta()
    lembradas0 = set(origem_apagada._APAGADAS)
    salvo_suc = dict(sucessao._estado)
    cena = Cena(nome=cen, dest=dest, outro=outro, a1=a1, a2=a2, a3=a3, b1=b1, c1=c1,
                massa=massa)
    tarefas, resultados = [], []
    try:
        for m, n, v in patches:
            setattr(m, n, v)
        sucessao.instalar(fonte, [ENT_A, ENT_B, ENT_C], entrada, _ao_sumir_de_main())
        a2e.LOOP.set_task_factory(nasc)
        with a2e.Bancada(cam, log):
            lk = None
            if cen == "CANCELA_LOCK":
                lk = await origem.lock_origem(CHAT_A, a1)
                await lk.acquire()
            try:
                for chat, ids in avisos:
                    vistas = set(origem_apagada._TAREFAS)
                    origem_apagada.agendar(int(chat), ids)
                    tarefas += [t for t in origem_apagada._TAREFAS if t not in vistas]
                    if cen in _CONCORRENTES:
                        continue
                    t = tarefas[-1] if tarefas else None
                    if cen == "CANCELA_LOCK":
                        await _ate(lambda: bool(getattr(lk, "_waiters", None)))
                        t.cancel()
                    if cen == "CANCELA_DEPOIS":
                        await _ate(entrou.is_set)
                        t.cancel()
                    resultados.append(await _desfecho(t))
                for t in (tarefas if cen in _CONCORRENTES else ()):
                    resultados.append(await _desfecho(t))
            finally:
                if lk is not None:
                    lk.release()
            cena.drenou = await _drenar_tudo()
            cena.exec_fora = coleta.exec_atual()
    finally:
        a2e.LOOP.set_task_factory(None)
        for m, n, v in salvos:
            setattr(m, n, v)
        cena.em_curso, cena.de_novo = len(sucessao._EM_CURSO), tuple(sorted(sucessao._DE_NOVO))
        sucessao._estado.update(salvo_suc)
        sucessao._EM_CURSO.clear()
        sucessao._DE_NOVO.clear()
        novas = set(origem_apagada._APAGADAS) - lembradas0
        for k in novas:
            origem_apagada._APAGADAS.pop(k, None)
    cena.lembradas = tuple(sorted(novas))
    cena.resultados = tuple(resultados)
    cena.banco = _banco([dest, outro], links[:5])
    cena.deletes, cena.log = tuple(destino.deletes), tuple(log.linhas)
    cena.fonte, cena.entrada = tuple(fonte.chamadas), tuple(entrada.chamadas)
    cena.camadas = tuple(cam.chamadas)
    cena.lider = a2r._lider(dest)
    cena.nasc = list(nasc.lista)
    cena.nascidas = tuple(n for n, _, _ in nasc.lista)
    cena.eventos = a2e._eventos_desde(reg, seq0)
    depois = coleta.saude_coleta()
    cena.delta = {k: depois[k] - antes.get(k, 0) for k in depois}
    return cena


_MANUTENCAO = ("MANUT_OK", "MANUT_VAZIO", "MANUT_ERRO")
_BASE_M = {c: 98000 + 10 * i for i, c in enumerate(_MANUTENCAO)}


async def _manutencao(cen):
    """O bloco do main.py que retoma as remoções no boot, rodando como no
    _preparar_processo (síncrono, com o laço no ar)."""
    dests = [_BASE_M[cen], _BASE_M[cen] + 1]
    _limpar(dests, [])
    with _db() as db:                       # só as desta cena ficam pendentes
        db.execute("UPDATE post_estado SET delete_status='ok'"
                   " WHERE delete_status IN ('pendente','falhou')")
    if cen == "MANUT_OK":
        for d in dests:
            _post(d, [], vivo=False, status="pendente")

    def quebra():
        raise RuntimeError(f"boot quebrou {a2e.SEGREDO_EXC} {_URL_EXC}")
    conv = (_pytypes.SimpleNamespace(retomar_remocoes=quebra) if cen == "MANUT_ERRO"
            else convergencia)
    destino, log, nasc = Destino(), a2e.Log(), Nascimentos()
    ns = {"convergencia": conv, "eventos": eventos, "log_sys": log}
    codigo = compile(ast.Module(body=[_bloco_manutencao()], type_ignores=[]), "main.py", "exec")
    patches = [(modulo_client, "client", destino), (convergencia, "log_out", log)]
    salvos = [(m, n, getattr(m, n)) for m, n, _ in patches]
    reg, seq0 = a2e._seq_atual()
    antes = coleta.saude_coleta()
    cena = Cena(nome=cen, dests=dests)
    try:
        for m, n, v in patches:
            setattr(m, n, v)
        a2e.LOOP.set_task_factory(nasc)
        exec(codigo, ns)                     # noqa: S102 — o código do main.py, como está
        cena.drenou = await _drenar_tudo()
        cena.exec_fora = coleta.exec_atual()
    finally:
        a2e.LOOP.set_task_factory(None)
        for m, n, v in salvos:
            setattr(m, n, v)
    cena.banco = _banco(dests, [])
    cena.deletes, cena.log = tuple(sorted(destino.deletes)), tuple(sorted(log.linhas))
    cena.nasc = list(nasc.lista)
    cena.nascidas = tuple(n for n, _, _ in nasc.lista)
    cena.eventos = a2e._eventos_desde(reg, seq0)
    depois = coleta.saude_coleta()
    cena.delta = {k: depois[k] - antes.get(k, 0) for k in depois}
    return cena


class SabotagemExc(a2e.Sabotagem):
    """A da A2-E (os auxiliares dos coletores e o próprio anel levantam) e a
    validação do catálogo que a execução consulta ao abrir: acerta a
    ABERTURA da execução e a EMISSÃO. O caminho funcional não pode notar."""

    def __enter__(self):
        super().__enter__()

        def quebra(*a, **k):
            raise RuntimeError("sabotagem")
        self.valido = catalogo.valido
        catalogo.valido = quebra
        return self

    def __exit__(self, *exc):
        catalogo.valido = self.valido
        return super().__exit__(*exc)


def _matriz(modo):
    """Todos os cenários num modo: {"X.<cenário>" | "M.<cenário>": Cena}."""
    a2e._desligar() if modo == "desligado" else a2e._ligar()
    res = {}
    for prefixo, f, cenas in (("X", _exclusao, _EXCLUSAO), ("M", _manutencao, _MANUTENCAO)):
        for c in cenas:
            if modo == "sabotado":
                with SabotagemExc():
                    res[f"{prefixo}.{c}"] = _rodar(f(c))
            else:
                res[f"{prefixo}.{c}"] = _rodar(f(c))
    return res


_CACHE = {}


def _ligada():
    if "ligado" not in _CACHE:
        _CACHE["ligado"] = _matriz("ligado")
    return _CACHE["ligado"]


def _apagadas(evs):
    return [e for e in evs if e["tipo"] == "origem.apagada"]


def _fins(evs, tipo):
    return [e for e in evs if e["tipo"] == "execucao.fim" and e["dados"].get("tipo") == tipo]


def _resumo(e):
    """(situacao, restam, post, via) de um origem.apagada."""
    return (e["dados"].get("situacao"), e["dados"].get("restam"), e["corr"].get("post"),
            e["dados"].get("via"))


def _da_exec(evs, ex):
    return [e for e in evs if e["corr"].get("exec") == ex]


# ─────────────────────────────────────────────────────────────────
# Contrato
# ─────────────────────────────────────────────────────────────────
def test_c01_catalogo_e_fronteira(r):
    achados = [e for rel in _ARQ_EXC for e in a2e._emissoes(rel)]
    r.check(achados == [("origem.apagada", LOCAL_EXC, None)], "C01.um_ponto_um_local",
            str(achados))
    r.check(LOCAL_EXC in catalogo.LOCAIS, "C01.local_no_catalogo")
    r.check(catalogo.TIPOS.get("origem.apagada", "ausente") is None,
            "C01.origem_apagada_nao_terminal")
    r.check(set(catalogo.ENUMS["situacao_origem_apagada"]) == set(_SITUACOES),
            "C01.as_sete_situacoes", str(sorted(catalogo.ENUMS["situacao_origem_apagada"])))
    r.check({"TELEGRAM", "SUCESSAO"} <= catalogo.ENUMS["via"]
            and {"EXCLUSAO", "MANUTENCAO"} <= catalogo.ENUMS["tipo_execucao"]
            and set(_PROVA_BANCO.values()) <= catalogo.ENUMS["prova_banco"]
            and "nao_tocado" in catalogo.ENUMS["prova_telegram"]
            and {"SEM_DESFECHO", "ERRO", "CANCELADA"} <= catalogo.ENUMS["resultado_execucao"],
            "C01.enums_da_fatia")
    # O catálogo global é a composição das frentes — A1 + A2-E + A2-R +
    # exclusão —, sem sobra nem falta. Cada frente confere o próprio escopo
    # no próprio teste.
    frentes = {"A1": {a2r.LOCAL_A1}, "A2-E": set(a2e.LOCAIS_A2E), "A2-R": set(a2r.LOCAIS_A2R),
               "EXCLUSAO": {LOCAL_EXC}}
    uniao = set().union(*frentes.values())
    r.check(sum(len(v) for v in frentes.values()) == len(uniao) == 29, "C01.frentes_disjuntas",
            str(len(uniao)))
    r.check(catalogo.LOCAIS == uniao, "C01.catalogo_global_A1_A2E_A2R_exclusao",
            str(sorted(catalogo.LOCAIS ^ uniao)))
    fora = []
    for rel in regua._arquivos():
        if rel.split(os.sep)[0] == "eventos":
            continue
        locs = {loc for _, loc, _ in a2e._emissoes(rel)}
        if not locs:
            continue
        dono = (set(a2e.LOCAIS_A2E) if rel in a2e._ARQ_FUNIL
                else set(a2r.LOCAIS_A2R) if rel in a2r._ARQ_A2R
                else {LOCAL_EXC} if rel in _ARQ_EXC else set())
        if not locs <= dono:
            fora.append((rel, sorted(locs - dono)))
    r.check(fora == [], "C01.nenhum_outro_emissor", str(fora))
    r.check(regua.verificar(open(os.path.join(regua.RAIZ, _ARQ_EXC[0]), encoding="utf-8").read(),
                            _ARQ_EXC[0], catalogo.TIPOS, catalogo.LOCAIS, False, {}) == [],
            "C01.regua_do_contrato")
    rc = catalogo.resumo()
    r.check(catalogo.VERSAO == 1 and rc["versao"] == 1
            and rc["hash"] == catalogo._hash(catalogo.TIPOS, catalogo.ENUMS, catalogo.LOCAIS)
            and catalogo._hash(catalogo.TIPOS, catalogo.ENUMS, catalogo.LOCAIS - {LOCAL_EXC})
            == "5dad380f73bcc7ac",
            "C01.versao_1_hash_recalculado_e_a2r_sem_o_local", str(rc))


_CHAVES_APAGADA = {"situacao", "restam", "via", "prova", "local", "ts_fato"}
_CHAVES_FIM = {"resultado", "tipo", "duracao_ms", "eventos", "local", "ts_fato"}


def _problemas_apagada(e):
    p, d, c = [], e["dados"], e["corr"]
    if set(d) != _CHAVES_APAGADA:
        p.append(f"dados {sorted(d)}")
    s = d.get("situacao")
    if s not in _SITUACOES:
        p.append(f"situacao {s!r}")
    if d.get("prova") != {"telegram": "nao_tocado", "banco": _PROVA_BANCO.get(s)}:
        p.append(f"prova {d.get('prova')}")
    restam = d.get("restam")
    if s == "MANTIDO" and not (type(restam) is int and restam >= 1):
        p.append(f"restam {restam!r}")
    if s == "MORTO" and restam != 0:
        p.append(f"restam {restam!r}")
    if s not in ("MANTIDO", "MORTO") and restam is not None:
        p.append(f"restam {restam!r}")
    if d.get("via") not in ("TELEGRAM", "SUCESSAO"):
        p.append(f"via {d.get('via')!r}")
    if d.get("local") != LOCAL_EXC:
        p.append(f"local {d.get('local')!r}")
    chaves = {"exec", "chat", "msg"} | ({"post"} if "post" in c else set())
    if set(c) != chaves:
        p.append(f"corr {sorted(c)}")
    if type(c.get("chat")) is not str or type(c.get("msg")) is not int:
        p.append(f"tipos chat/msg {c.get('chat')!r} {c.get('msg')!r}")
    if "post" in c and type(c["post"]) is not int:
        p.append(f"post {c['post']!r}")
    return p


def _problemas_fim(e, tipo):
    p, d, c = [], e["dados"], e["corr"]
    extra = {"erro_tipo"} if d.get("resultado") == "ERRO" else set()
    if set(d) != _CHAVES_FIM | extra:
        p.append(f"dados {sorted(d)}")
    if d.get("tipo") != tipo or d.get("local") != "eventos.execucao.fim":
        p.append(f"tipo/local {d.get('tipo')} {d.get('local')}")
    chaves = {"exec", "chat", "msg"} if tipo == "EXCLUSAO" else {"exec"}
    if set(c) != chaves:
        p.append(f"corr {sorted(c)}")
    return p


def test_c02_forma_exata(r):
    vistas, erros = set(), []
    for nome, c in _ligada().items():
        for e in _apagadas(c.eventos):
            vistas.add(e["dados"].get("situacao"))
            erros += [f"{nome}: {x}" for x in _problemas_apagada(e)]
        for tipo in ("EXCLUSAO", "MANUTENCAO"):
            for e in _fins(c.eventos, tipo):
                erros += [f"{nome}: fim {x}" for x in _problemas_fim(e, tipo)]
    r.check(vistas == set(_SITUACOES), "C02.as_sete_situacoes_exercitadas",
            str(sorted(set(_SITUACOES) - vistas)))
    r.check(erros == [], "C02.forma_exata", str(erros[:6]))
    morto = _apagadas(_ligada()["X.MORTO"].eventos)
    r.check(len(morto) == 1 and morto[0]["corr"] == {
        "exec": morto[0]["corr"].get("exec"), "chat": CHAT_A,
        "msg": _ligada()["X.MORTO"].a1, "post": _ligada()["X.MORTO"].dest}
        and morto[0]["dados"]["situacao"] == "MORTO" and morto[0]["dados"]["restam"] == 0
        and morto[0]["dados"]["via"] == "TELEGRAM", "C02.morto_literal", str(morto))


# Base f6d18ae (ast.unparse), por função: o que a fatia NÃO pode mudar.
_BASE_AWAIT = {
    "apagadas": ["_uma(chat, int(msg_id))", "esquecimento.post_saiu(dest)"],
    "_uma": ["origem.lock_origem(chat, msg_id)", "exclusao.lock_post(dest)"],
}
_BASE_CREATE_TASK = {"agendar": ["asyncio.get_running_loop().create_task"]}
_BASE_DB = {"_uma": ["db_desvincular_origem"]}


def _funcoes_oa():
    return {n.name: n for n in regua._arvore(_ARQ_EXC[0]).body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _chamadas(no, filtro):
    return [ast.unparse(n.func) for n in ast.walk(no) if isinstance(n, ast.Call)
            and filtro(ast.unparse(n.func))]


def test_c03_estrutura_de_origem_apagada(r):
    fs = _funcoes_oa()
    ap = fs["apagadas"]
    laco = next((n for n in ap.body if isinstance(n, ast.For)), None)
    corpo = laco.body if laco is not None else []
    com = corpo[0] if corpo and isinstance(corpo[0], ast.With) else None
    r.check(com is not None and len(corpo) == 2, "C03.uma_execucao_por_id_no_laco",
            str([type(n).__name__ for n in corpo]))
    if com is None:
        return
    item = com.items[0].context_expr
    r.check(ast.unparse(item.func) == "eventos.execucao"
            and [(k.arg, ast.unparse(k.value)) for k in item.keywords] == [("tipo", "'EXCLUSAO'")]
            and len(item.args) == 1 and isinstance(item.args[0], ast.Lambda)
            and isinstance(item.args[0].body, ast.Dict)
            and [ast.unparse(k) for k in item.args[0].body.keys] == ["'chat'", "'msg'"],
            "C03.raiz_EXCLUSAO_com_chat_e_msg", ast.unparse(item))
    dentro = com.body
    r.check(len(dentro) >= 2 and isinstance(dentro[0], ast.Try)
            and ast.unparse(dentro[0].body[0]) == "situacao, dest, restam = await _uma(chat, int(msg_id))",
            "C03._uma_primeiro_dentro_da_execucao", ast.unparse(dentro[0].body[0]) if dentro else "")
    r.check(len(dentro) >= 2 and isinstance(dentro[1], ast.Expr)
            and ast.unparse(dentro[1].value.func) == "eventos.emitir_de",
            "C03.emissao_logo_depois_de__uma", ast.unparse(dentro[1])[:80] if len(dentro) > 1 else "")
    resto = "\n".join(ast.unparse(n) for n in dentro[2:])
    r.check(resto.startswith("if situacao in ('morto', 'mantido'):\n    esquecimento.origem_saiu")
            and "await esquecimento.post_saiu(dest)" in resto
            and "convergencia.agendar_remocao(dest, 'ORIGEM_APAGADA')" in resto
            and "sucessao.agendar" not in resto,
            "C03.esquecimento_e_remocao_dentro_depois_da_emissao", resto)
    trata = dentro[0].handlers if isinstance(dentro[0], ast.Try) else []
    r.check([ast.unparse(h.type) for h in trata] == ["asyncio.CancelledError", "Exception"]
            and [ast.unparse(s) for s in trata[0].body] == ["raise"]
            and [ast.unparse(s) for s in trata[1].body][1:] == [
                "eventos.marcar_desfecho('ERRO', e)", "continue"]
            and ast.unparse(trata[1].body[0]).startswith("log_out.error(f'❌ origem apagada"),
            "C03.except_de_sempre_com_ERRO", str([ast.unparse(h) for h in trata])[:300])
    depois = ast.unparse(corpo[1])
    r.check(depois == "if situacao == 'mantido':\n    sucessao.agendar(dest)",
            "C03.sucessao_agendar_fora_da_execucao", depois)
    r.check(not _chamadas(com, lambda f: f == "sucessao.agendar")
            and _chamadas(com, lambda f: f == "convergencia.agendar_remocao"),
            "C03.na_execucao_a_remocao_e_nunca_a_sucessao")
    sem_eventos = {nome: _chamadas(fs[nome], lambda f: f.startswith("eventos."))
                   for nome in ("agendar", "_uma", "instalar", "apagada", "_lembrar")}
    r.check(all(v == [] for v in sem_eventos.values()), "C03.nada_em_agendar_nem_sob_lock",
            str(sem_eventos))
    retornos = [n for n in ast.walk(fs["_uma"]) if isinstance(n, ast.Return)]
    r.check(retornos and all(isinstance(n.value, ast.Tuple) and len(n.value.elts) == 3
                             for n in retornos), "C03._uma_devolve_situacao_post_restam",
            str([ast.unparse(n) for n in retornos]))
    emissoes = _chamadas(regua._arvore(_ARQ_EXC[0]), lambda f: f.startswith("eventos."))
    r.check(sorted(emissoes) == ["eventos.emitir_de", "eventos.execucao",
                                 "eventos.marcar_desfecho"], "C03.so_tres_chamadas_de_eventos",
            str(emissoes))
    for base, filtro, nome in (
            (_BASE_AWAIT, None, "await"),
            (_BASE_CREATE_TASK, lambda f: f.endswith("create_task"), "create_task"),
            (_BASE_DB, lambda f: f.startswith("db_"), "banco")):
        for fn in fs:
            if nome == "await":
                achou = [ast.unparse(n.value) for n in ast.walk(fs[fn])
                         if isinstance(n, ast.Await) and isinstance(n.value, ast.Call)]
            else:
                achou = _chamadas(fs[fn], filtro)
            r.check(achou == base.get(fn, []), f"C03.{nome}_igual_a_base.{fn}", str(achou))
    asyncs = sum(isinstance(n, ast.AsyncWith) for n in ast.walk(fs["_uma"]))
    r.check(asyncs == 2 and sum(isinstance(n, ast.AsyncWith) for n in ast.walk(ap)) == 0,
            "C03.locks_iguais_a_base", str(asyncs))


_MANUT_ESPERADO = (
    "with eventos.execucao(tipo='MANUTENCAO'):\n"
    "    try:\n"
    "        convergencia.retomar_remocoes()\n"
    "    except Exception as e:\n"
    "        log_sys.error(f'❌ retomar_remocoes: {e}')\n"
    "        eventos.marcar_desfecho('ERRO', e)")


def test_c04_estrutura_de_main(r):
    corpo = [ast.unparse(n) for n in _preparar_processo().body]
    bloco = ast.unparse(_bloco_manutencao())
    r.check(bloco == _MANUT_ESPERADO, "C04.raiz_MANUTENCAO_em_volta_da_retomada", bloco)
    i = corpo.index(bloco)
    r.check(corpo[i - 1] == "espelho_cupons.iniciar()"
            and corpo[i + 1] == "completude.instalar(client, fontes, processar)",
            "C04.mesmo_lugar_no_boot", f"{corpo[i - 1]} | {corpo[i + 1]}")
    inst = [s for s in corpo if s.startswith("sucessao.instalar(")]
    r.check(inst == ["sucessao.instalar(client, fontes, processar, "
                     "functools.partial(origem_apagada.apagadas, via='SUCESSAO'))"],
            "C04.partial_com_via_SUCESSAO", str(inst))
    r.check(any(isinstance(n, ast.Import) and [a.name for a in n.names] == ["functools"]
                for n in _ARV_MAIN.body), "C04.import_functools")
    ev = [ast.unparse(n.func) for n in ast.walk(_ARV_MAIN) if isinstance(n, ast.Call)
          and regua._nome_final(n.func) in ("emitir_de", "execucao", "transferir_execucao",
                                            "inicio_na_fila", "marcar_desfecho", "adiar")]
    r.check(sorted(ev) == ["eventos.execucao", "eventos.marcar_desfecho"],
            "C04.nenhuma_outra_execucao_ou_emissao_em_main", str(ev))
    oa = _ao_sumir_de_main()
    r.check(isinstance(oa, functools.partial) and oa.func is origem_apagada.apagadas
            and oa.args == () and oa.keywords == {"via": "SUCESSAO"},
            "C04.o_ao_sumir_avaliado", repr(oa))


# ─────────────────────────────────────────────────────────────────
# Exclusão (via TELEGRAM)
# ─────────────────────────────────────────────────────────────────
def _uma_raiz(r, nome, c, chat, msg, resultado, n_eventos):
    """A execução EXCLUSAO de (chat, msg): exatamente um fim, raiz, com o
    resultado e o número de eventos; devolve o exec."""
    fins = [e for e in _fins(c.eventos, "EXCLUSAO")
            if (e["corr"].get("chat"), e["corr"].get("msg")) == (chat, msg)]
    ok = (len(fins) == 1 and "exec_pai" not in fins[0]["corr"]
          and fins[0]["dados"]["resultado"] == resultado
          and fins[0]["dados"]["eventos"] == n_eventos)
    r.check(ok, f"{nome}.uma_raiz_{resultado}", str([e["dados"] for e in fins]))
    return fins[0]["corr"]["exec"] if fins else None


def test_f01_morto(r):
    c = _ligada()["X.MORTO"]
    ex = _uma_raiz(r, "F01", c, CHAT_A, c.a1, "SEM_DESFECHO", 1)
    ap = _apagadas(c.eventos)
    r.check([_resumo(e) for e in ap] == [("MORTO", 0, c.dest, "TELEGRAM")]
            and ap[0]["corr"]["exec"] == ex, "F01.evento_na_raiz", str([_resumo(e) for e in ap]))
    evs = _da_exec(c.eventos, ex)
    r.check([e["tipo"] for e in evs] == ["origem.apagada", "execucao.fim"]
            and evs[0]["seq"] < evs[1]["seq"], "F01.ordem_fato_depois_fim")
    rem = c.nasc and [n for n in c.nasc if n[0] == "_remover"]
    r.check(rem and len(rem) == 1 and ex is not None and getattr(rem[0][1], "id", None) == ex
            and rem[0][1] is rem[0][2] and getattr(rem[0][2], "tipo", None) == "EXCLUSAO",
            "F01.remocao_herda_o_exec_da_raiz", str(rem))
    r.check(c.resultados == ((None, (c.dest,)),) and c.deletes == (c.dest,)
            and c.banco[0] == (c.dest, (), ("ok", None, "", None, True)),
            "F01.post_encerrado_e_removido", f"{c.resultados} {c.deletes} {c.banco[0]}")


def test_f02_mantido(r):
    c = _ligada()["X.MANTIDO"]
    ex = _uma_raiz(r, "F02.MANTIDO", c, CHAT_A, c.a1, "SEM_DESFECHO", 1)
    r.check([_resumo(e) for e in _apagadas(c.eventos)] == [("MANTIDO", 1, c.dest, "TELEGRAM")],
            "F02.MANTIDO.evento", str([_resumo(e) for e in _apagadas(c.eventos)]))
    r.check(c.deletes == () and c.entrada == () and [n[0] for n in c.nasc] == ["apagadas", "_rodar"]
            and ex is not None, "F02.MANTIDO.sucessao_confere_e_nada_muda", str(c.nascidas))
    c = _ligada()["X.MANTIDO_CHEFE"]
    ex = _uma_raiz(r, "F02.CHEFE", c, CHAT_A, c.a1, "SEM_DESFECHO", 1)
    lid = [e for e in c.eventos if e["tipo"] == "post.lideranca_transferida"]
    rec = [e for e in c.eventos if e["tipo"] == "origem.recebida"]
    r.check(len(lid) == 1 and "exec" not in lid[0]["corr"] and len(rec) == 1
            and rec[0]["dados"]["via"] == "SUCESSAO" and "exec_pai" not in rec[0]["corr"]
            and rec[0]["corr"]["exec"] != ex,
            "F02.CHEFE.sucessao_fora_da_raiz", str([(e["tipo"], e["corr"]) for e in lid + rec]))
    fim_x = _fins(c.eventos, "EXCLUSAO")
    r.check(fim_x and lid and fim_x[0]["seq"] < lid[0]["seq"] < rec[0]["seq"],
            "F02.CHEFE.ordem_exclusao_lideranca_reentrada")
    r.check(c.lider == (CHAT_B, c.b1) and c.entrada == ((CHAT_B, c.b1, True, "SUCESSAO"),),
            "F02.CHEFE.b_assume_e_reentra", f"{c.lider} {c.entrada}")


def test_f03_sem_post_ja_removido_erro(r):
    on = _ligada()
    for cen, sit in (("SEM_POST", "SEM_POST"), ("JA_REMOVIDO", "JA_REMOVIDO"), ("ERRO", "ERRO")):
        c = on[f"X.{cen}"]
        ex = _uma_raiz(r, f"F03.{cen}", c, CHAT_A, c.a1, "SEM_DESFECHO", 1)
        ap = _apagadas(c.eventos)
        r.check([_resumo(e) for e in ap] == [(sit, None, c.dest, "TELEGRAM")]
                and ap[0]["dados"]["prova"]["banco"] == _PROVA_BANCO[sit]
                and ap[0]["corr"]["exec"] == ex, f"F03.{cen}.evento",
                str([(_resumo(e), e["dados"]["prova"]) for e in ap]))
        r.check(c.deletes == () and c.resultados == ((None, ()),), f"F03.{cen}.nada_removido",
                f"{c.deletes} {c.resultados}")
    c = on["X.ERRO"]
    r.check(c.banco[0][1] == ((CHAT_A, c.a1),) and c.banco[0][2][0] is None
            and any("db_desvincular_origem" in l for _, l in c.log),
            "F03.ERRO.rollback_real_vinculo_intacto", str(c.banco[0]))
    r.check(on["X.SEM_POST"].banco[2:3] == ((CHAT_A, on["X.SEM_POST"].a1, None),),
            "F03.SEM_POST.vinculo_desfeito", str(on["X.SEM_POST"].banco))


def test_f04_sem_vinculo_e_mudou(r):
    on = _ligada()
    for cen, esperado in (("SEM_VINCULO", ("SEM_VINCULO", None, None, "TELEGRAM")),
                          ("SEM_VINCULO_BANCO", ("SEM_VINCULO", None, "post", "TELEGRAM")),
                          ("MUDOU", ("MUDOU", None, None, "TELEGRAM"))):
        c = on[f"X.{cen}"]
        esperado = tuple(c.dest if x == "post" else x for x in esperado)
        ex = _uma_raiz(r, f"F04.{cen}", c, CHAT_A, c.a1, "SEM_DESFECHO", 1)
        ap = _apagadas(c.eventos)
        r.check([_resumo(e) for e in ap] == [esperado]
                and ap[0]["dados"]["prova"] == {"telegram": "nao_tocado", "banco": "nao_aplicavel"}
                and ap[0]["corr"]["exec"] == ex, f"F04.{cen}.evento_nao_aplicavel",
                str([(_resumo(e), e["dados"]["prova"]) for e in ap]))
        r.check(c.deletes == () and c.resultados == ((None, ()),), f"F04.{cen}.nada_removido")
    c = on["X.MUDOU"]
    r.check(sum("vínculo mudou 4x" in l for _, l in c.log) == 1, "F04.MUDOU.esgotou_quatro_vezes",
            str(c.log))


def test_f05_excecao_em__uma(r):
    c = _ligada()["X.EXCECAO"]
    fins = _fins(c.eventos, "EXCLUSAO")
    r.check([(e["corr"]["msg"], e["dados"]["resultado"], e["dados"].get("erro_tipo"),
              e["dados"]["eventos"]) for e in fins]
            == [(c.a1, "ERRO", "RuntimeError", 0), (c.a2, "SEM_DESFECHO", None, 1)],
            "F05.ERRO_e_o_laco_segue", str([e["dados"] for e in fins]))
    ap = _apagadas(c.eventos)
    r.check([(e["corr"]["msg"],) + _resumo(e) for e in ap]
            == [(c.a2, "MORTO", 0, c.dest, "TELEGRAM")], "F05.nada_de_origem_apagada_na_excecao",
            str([_resumo(e) for e in ap]))
    r.check(any(l.startswith(f"❌ origem apagada") and str(c.a1) in l for _, l in c.log)
            and c.resultados == ((None, (c.dest,)),) and c.deletes == (c.dest,),
            "F05.log_de_sempre_e_o_segundo_id_removido", f"{c.resultados} {c.deletes}")


def test_f06_varios_ids(r):
    c = _ligada()["X.VARIOS"]
    fins = _fins(c.eventos, "EXCLUSAO")
    r.check([e["corr"]["msg"] for e in fins] == [c.a1, c.a2, c.a3]
            and len({e["corr"]["exec"] for e in fins}) == 3, "F06.uma_execucao_por_id_na_ordem",
            str([e["corr"] for e in fins]))
    ap = _apagadas(c.eventos)
    r.check([_resumo(e) for e in ap] == [("MANTIDO", 1, c.dest, "TELEGRAM"),
                                         ("SEM_VINCULO", None, None, "TELEGRAM"),
                                         ("MORTO", 0, c.outro, "TELEGRAM")],
            "F06.situacoes_na_ordem", str([_resumo(e) for e in ap]))
    ok = all(_da_exec(c.eventos, f["corr"]["exec"])[-1] is f
             and [e["tipo"] for e in _da_exec(c.eventos, f["corr"]["exec"])]
             == ["origem.apagada", "execucao.fim"] for f in fins)
    seqs = [e["seq"] for f in fins for e in _da_exec(c.eventos, f["corr"]["exec"])]
    r.check(ok and seqs == sorted(seqs), "F06.cada_execucao_fechada_antes_da_proxima", str(seqs))
    r.check(c.resultados == ((None, (c.outro,)),) and c.deletes == (c.outro,)
            and c.nascidas.count("_rodar") == 1 and c.nascidas.count("_remover") == 1,
            "F06.funcional", f"{c.resultados} {c.nascidas}")


def test_f07_repeticao_e_duplicata(r):
    for cen in ("REPETICAO", "DUPLICADO"):
        c = _ligada()[f"X.{cen}"]
        ap = _apagadas(c.eventos)
        r.check(sorted((_resumo(e) for e in ap), key=str)
                == sorted([("MORTO", 0, c.dest, "TELEGRAM"),
                           ("SEM_VINCULO", None, None, "TELEGRAM")], key=str),
                f"F07.{cen}.um_morto_so", str([_resumo(e) for e in ap]))
        fins = _fins(c.eventos, "EXCLUSAO")
        r.check(len(fins) == 2 and len({e["corr"]["exec"] for e in fins}) == 2
                and all(e["corr"]["exec"] in {f["corr"]["exec"] for f in fins} for e in ap),
                f"F07.{cen}.duas_execucoes_cada_uma_com_o_seu", str([e["corr"] for e in fins]))
        r.check(c.deletes == (c.dest,) and c.nascidas.count("_remover") == 1
                and sorted(c.resultados, key=str) == sorted(((None, (c.dest,)), (None, ())), key=str),
                f"F07.{cen}.uma_remocao", f"{c.deletes} {c.resultados}")


def test_f08_concorrencia_entre_fontes(r):
    c = _ligada()["X.CONCORRENTE"]
    ap = _apagadas(c.eventos)
    r.check(sorted((s, n, p) for s, n, p, _ in map(_resumo, ap))
            == [("MANTIDO", 1, c.dest), ("MORTO", 0, c.dest)], "F08.um_mantido_um_morto",
            str([_resumo(e) for e in ap]))
    fins = _fins(c.eventos, "EXCLUSAO")
    pares = {(e["corr"]["chat"], e["corr"]["msg"]): e["corr"]["exec"] for e in fins}
    r.check(len(fins) == 2 and set(pares) == {(CHAT_A, c.a1), (CHAT_B, c.b1)}
            and all(pares.get((e["corr"]["chat"], e["corr"]["msg"])) == e["corr"]["exec"]
                    for e in ap), "F08.cada_evento_na_raiz_da_sua_origem", str(pares))
    r.check(c.deletes == (c.dest,) and c.banco[0][1] == () and c.banco[0][2][0] == "ok",
            "F08.post_sai_uma_vez", f"{c.deletes} {c.banco[0]}")


def test_f09_cancelamento(r):
    c = _ligada()["X.CANCELA_LOCK"]
    fins = _fins(c.eventos, "EXCLUSAO")
    r.check([(e["corr"]["msg"], e["dados"]["resultado"], e["dados"]["eventos"]) for e in fins]
            == [(c.a1, "CANCELADA", 0)] and _apagadas(c.eventos) == [],
            "F09.A.na_espera_do_lock_nada_sai", str([e["dados"] for e in fins]))
    r.check(c.resultados == (("CANCELADA", None),) and c.deletes == ()
            and c.banco[0][1] == ((CHAT_A, c.a1),) and c.banco[1][1] == ((CHAT_A, c.a2),)
            and "_remover" not in c.nascidas and c.exec_fora is None,
            "F09.A.o_resto_do_aviso_nao_roda", f"{c.resultados} {c.banco[:2]}")
    c = _ligada()["X.CANCELA_DEPOIS"]
    fins = _fins(c.eventos, "EXCLUSAO")
    ap = _apagadas(c.eventos)
    r.check([(e["dados"]["resultado"], e["dados"]["eventos"]) for e in fins] == [("CANCELADA", 1)]
            and [_resumo(e) for e in ap] == [("MORTO", 0, c.dest, "TELEGRAM")]
            and ap[0]["corr"]["exec"] == fins[0]["corr"]["exec"] and ap[0]["seq"] < fins[0]["seq"],
            "F09.B.fato_ja_gravado_sai_uma_vez", str([e["dados"] for e in fins]))
    r.check(c.resultados == (("CANCELADA", None),) and c.deletes == ()
            and "_remover" not in c.nascidas
            and c.banco[0] == (c.dest, (), ("pendente", None, "", None, True)),
            "F09.B.sem_rollback_e_sem_remocao_fica_para_o_boot", f"{c.resultados} {c.banco[0]}")


# ─────────────────────────────────────────────────────────────────
# Sucessão e manutenção
# ─────────────────────────────────────────────────────────────────
def test_f10_via_sucessao_pela_cadeia_real(r):
    c = _ligada()["X.VIA_SUCESSAO"]
    ap = _apagadas(c.eventos)
    r.check([(e["corr"]["chat"], e["corr"]["msg"]) + _resumo(e) for e in ap]
            == [(CHAT_A, c.a1, "MANTIDO", 2, c.dest, "TELEGRAM"),
                (CHAT_B, c.b1, "MANTIDO", 1, c.dest, "SUCESSAO")],
            "F10.as_duas_vias", str([_resumo(e) for e in ap]))
    fins = _fins(c.eventos, "EXCLUSAO")
    r.check(len(fins) == 2 and all("exec_pai" not in e["corr"] for e in fins + ap)
            and [e["corr"]["exec"] for e in ap] == [e["corr"]["exec"] for e in fins],
            "F10.cada_via_uma_raiz_sem_exec_pai", str([e["corr"] for e in fins]))
    lid = [e for e in c.eventos if e["tipo"] == "post.lideranca_transferida"]
    rec = [e for e in c.eventos if e["tipo"] == "origem.recebida"]
    r.check(len(lid) == 1 and "exec" not in lid[0]["corr"] and len(rec) == 1
            and "exec_pai" not in rec[0]["corr"] and rec[0]["dados"]["via"] == "SUCESSAO",
            "F10.lideranca_e_reentrada_fora_da_raiz", str([e["corr"] for e in lid + rec]))
    r.check(fins and lid and rec and fins[0]["seq"] < fins[1]["seq"] < lid[0]["seq"] < rec[0]["seq"],
            "F10.ordem_aviso_sumida_lideranca_reentrada")
    r.check(c.fonte == ((CHAT_B, [c.b1]), (CHAT_C, [c.c1])) and c.lider == (CHAT_C, c.c1)
            and c.entrada == ((CHAT_C, c.c1, True, "SUCESSAO"),)
            and c.banco[0][1] == ((CHAT_C, c.c1),) and c.em_curso == 0,
            "F10.funcional_da_cadeia", f"{c.fonte} {c.lider} {c.entrada} {c.banco[0]}")


def test_f11_contexto_no_nascimento(r):
    on = _ligada()
    erros = []
    for nome, c in on.items():
        if not nome.startswith("X."):
            continue
        fins = {e["corr"]["exec"]: e for e in _fins(c.eventos, "EXCLUSAO")}
        for co, cria, leva in c.nasc:
            if co in ("apagadas", "_rodar") and (cria is not None or leva is not None):
                erros.append(f"{nome}:{co} nasceu com exec")
            if co == "_remover" and not (cria is not None and cria is leva
                                         and cria.tipo == "EXCLUSAO" and cria.id in fins):
                erros.append(f"{nome}:_remover sem o exec da raiz")
            if co == "uma_por_vez" and not (leva is not None and leva.tipo == "MENSAGEM"
                                            and leva.pai is None):
                erros.append(f"{nome}:fila com exec {getattr(leva, 'tipo', None)}")
    r.check(erros == [], "F11.apagadas_e_rodar_sem_exec_remocao_com_o_da_raiz", str(erros[:6]))
    nomes = [co for c in on.values() for co, _, _ in c.nasc]
    r.check({"apagadas", "_rodar", "_remover", "uma_por_vez"} <= set(nomes),
            "F11.a_sonda_viu_os_quatro_nascimentos", str(sorted(set(nomes))))
    rem = [n for n in on["M.MANUT_OK"].nasc if n[0] == "_remover"]
    r.check(len(rem) == 2 and all(n[1] is not None and n[1] is n[2] and n[1].tipo == "MANUTENCAO"
                                  for n in rem), "F11.controle_positivo_manutencao", str(rem))


def test_f12_manutencao(r):
    on = _ligada()
    c = on["M.MANUT_OK"]
    fins = _fins(c.eventos, "MANUTENCAO")
    r.check(len(fins) == 1 and fins[0]["dados"]["resultado"] == "SEM_DESFECHO"
            and fins[0]["dados"]["eventos"] == 0 and set(fins[0]["corr"]) == {"exec"},
            "F12.OK.uma_raiz", str([e["dados"] for e in fins]))
    r.check(c.deletes == tuple(c.dests) and all(b[2][0] == "ok" for b in c.banco)
            and [e["tipo"] for e in c.eventos] == ["execucao.fim"],
            "F12.OK.remocoes_retomadas", f"{c.deletes} {c.banco}")
    c = on["M.MANUT_VAZIO"]
    fins = _fins(c.eventos, "MANUTENCAO")
    r.check(len(fins) == 1 and fins[0]["dados"]["resultado"] == "SEM_DESFECHO"
            and c.nascidas == () and c.deletes == (), "F12.VAZIO", str([e["dados"] for e in fins]))
    c = on["M.MANUT_ERRO"]
    fins = _fins(c.eventos, "MANUTENCAO")
    r.check(len(fins) == 1 and fins[0]["dados"]["resultado"] == "ERRO"
            and fins[0]["dados"].get("erro_tipo") == "RuntimeError",
            "F12.ERRO.fim_em_erro", str([e["dados"] for e in fins]))
    r.check(any(l.startswith("❌ retomar_remocoes: boot quebrou") for _, l in c.log)
            and c.nascidas == () and c.exec_fora is None, "F12.ERRO.log_de_sempre", str(c.log))


# ─────────────────────────────────────────────────────────────────
# Regressão
# ─────────────────────────────────────────────────────────────────
def test_r01_desligado_ligado_sabotado(r):
    off = _matriz("desligado")
    on = _ligada()
    sab = _matriz("sabotado")
    for nome in on:
        r.check(off[nome].funcional() == on[nome].funcional() == sab[nome].funcional(),
                f"R01.{nome}.funcional_identico",
                str([(k, a, b) for k, a, b in zip(Cena._FUNCIONAL, off[nome].funcional(),
                                                  on[nome].funcional()) if a != b])[:300])
        r.check(off[nome].drenou and on[nome].drenou and sab[nome].drenou
                and off[nome].exec_fora is on[nome].exec_fora is sab[nome].exec_fora is None,
                f"R01.{nome}.nada_preso")
        r.check(sab[nome].delta.get("falhas_internas", 0) > 0 and sab[nome].eventos == [],
                f"R01.{nome}.sabotagem_acertou_e_foi_contada", str(sab[nome].delta))
    _CACHE["desligado"] = off


def test_r02_dormente(r):
    off = _CACHE.get("desligado") or _matriz("desligado")
    mexeu = {n: {k: v for k, v in c.delta.items() if v} for n, c in off.items()}
    r.check(all(not d for d in mexeu.values()) and all(c.eventos == [] for c in off.values()),
            "R02.nenhum_contador_da_coleta_se_move", str({n: d for n, d in mexeu.items() if d}))
    a2e._desligar()

    def ponto():
        with eventos.execucao(lambda c="-1001", m=1: {"chat": c, "msg": m}, tipo="EXCLUSAO"):
            eventos.emitir_de("origem.apagada", lambda c="-1001", m=1, d=2, s="morto", n=0,
                              v="TELEGRAM": ({"chat": c}, {"situacao": s}), local=LOCAL_EXC)
    n = 20000
    custo = min(timeit.repeat(ponto, number=n, repeat=5)) / n * 1e6
    r.check(custo < 3.0, "R02.custo_desligado_por_id_com_folga", f"{custo:.3f} µs")


def test_r03_interfaces(r):
    def params(f):
        return [(p.name, p.default) for p in inspect.signature(f).parameters.values()]
    vazio = inspect.Parameter.empty
    r.check(params(origem_apagada.apagadas) == [("chat", vazio), ("ids", vazio),
                                                ("via", "TELEGRAM")],
            "R03.apagadas_ganha_via_com_padrao", str(params(origem_apagada.apagadas)))
    r.check(params(origem_apagada.agendar) == [("chat_id", vazio), ("ids", vazio)]
            and not inspect.iscoroutinefunction(origem_apagada.agendar)
            and inspect.iscoroutinefunction(origem_apagada.apagadas)
            and params(origem_apagada.instalar) == [("cliente", vazio), ("fontes", vazio)]
            and params(origem_apagada.apagada) == [("chat", vazio), ("msg_id", vazio)]
            and origem_apagada.__all__ == ["instalar", "agendar", "apagadas", "apagada"],
            "R03.o_resto_intocado")
    r.check(params(sucessao.instalar) == [("cliente", vazio), ("fontes", vazio),
                                          ("despachar", vazio), ("ao_sumir", vazio)]
            and params(convergencia.retomar_remocoes) == [],
            "R03.sucessao_e_convergencia_intocadas")


# ─────────────────────────────────────────────────────────────────
# Segurança
# ─────────────────────────────────────────────────────────────────
_PROIBIDOS = (a2e.SEGREDO_EXC, "CANARIO", "SEGREDO_TXT", "x.co", "://", "disco cheio",
              "lock quebrou", "boot quebrou", a2r.TEXTO)


def test_s01_canarios(r):
    for nome, c in _ligada().items():
        bruto = json.dumps(c.eventos, ensure_ascii=False)
        achados = [p for p in _PROIBIDOS if p in bruto]
        r.check(achados == [], f"S01.{nome}.nenhum_canario", str(achados))
        r.check(all("excecao" not in e["dados"] for e in _apagadas(c.eventos))
                and all("erro_tipo" not in e["dados"] for e in c.eventos
                        if e["tipo"] != "execucao.fim"), f"S01.{nome}.excecao_so_pela_classe_no_fim")


def test_s02_volume(r):
    c = _ligada()["X.MASSA"]
    ap, fins = _apagadas(c.eventos), _fins(c.eventos, "EXCLUSAO")
    r.check(len(ap) == len(fins) == _N_MASSA
            and [e["corr"]["msg"] for e in ap] == c.massa
            and all(_resumo(e) == ("SEM_VINCULO", None, None, "TELEGRAM") for e in ap),
            "S02.um_evento_e_uma_raiz_por_id", f"{len(ap)} {len(fins)}")
    tam = max((len(json.dumps(e, ensure_ascii=False).encode()) for e in c.eventos), default=0)
    r.check(all(not e["dados"].get("cortado") and not e["dados"].get("degradado")
                for e in c.eventos) and tam < anel.MAX_EVENTO_BYTES,
            "S02.nada_cortado_nem_degradado", str(tam))


def test_s03_correlacao_global(r):
    for nome, c in _ligada().items():
        fins = [e for e in c.eventos if e["tipo"] == "execucao.fim"]
        por_exec = {}
        for e in fins:
            por_exec.setdefault(e["corr"]["exec"], []).append(e)
        unicos = all(len(v) == 1 for v in por_exec.values())
        orfaos = [e["tipo"] for e in c.eventos
                  if "exec" in e["corr"] and e["corr"]["exec"] not in por_exec]
        raizes = all("exec_pai" not in e["corr"] for e in fins
                     if e["dados"]["tipo"] in ("EXCLUSAO", "MANUTENCAO"))
        da_raiz = all(por_exec.get(e["corr"].get("exec"), [{}])[0].get("dados", {}).get("tipo")
                      == "EXCLUSAO"
                      and (por_exec[e["corr"]["exec"]][0]["corr"]["chat"],
                           por_exec[e["corr"]["exec"]][0]["corr"]["msg"])
                      == (e["corr"]["chat"], e["corr"]["msg"]) for e in _apagadas(c.eventos))
        r.check(unicos and not orfaos and raizes and da_raiz, f"S03.{nome}",
                f"unicos={unicos} orfaos={orfaos} raizes={raizes} da_raiz={da_raiz}")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "EVENTOS · fatia da exclusão (origem.apagada, raízes EXCLUSAO "
                              "e MANUTENCAO, via da B5) · F1.2"))
