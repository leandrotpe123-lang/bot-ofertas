"""
F1.2-A2-E — ESPINHA DE EXECUÇÃO do funil de entrada: processar → fila →
_pipeline, até a CHAMADA a publicacao.enviar (o que acontece dentro de
enviar é da C1).

Pelo caminho REAL do orquestrador, com o Telethon REAL (como em
test_vida_uma_hora) e as camadas do _pipeline trocadas por falsas
controláveis: o que se prova é o funil e a instrumentação, não as camadas.

  Contrato
    C01  cada LOCAL do funil ↔ um (tipo, motivo), um a um; os 16 estão no
         catálogo e só o funil os emite (escopo da A2-E: o catálogo
         inteiro não é conferido aqui)
    C02  origem.recebida: forma exata; sem texto integral; links como
         referências; encaminhada só de canal; grouped_id como texto
    C03  origem.descartada: forma, enums e efeitos_parciais dos 15 motivos;
         a exceção do pipeline vai em `excecao` (só a classe) — erro_tipo é
         chave reservada da coleta (A1) e nunca vem do coletor
    C04  execucao.fim por resultado; espera_ms só se a task começou
    C05  estrutura (AST): recebida antes das travas; processar só aguarda
         _enfileirar; _enfileirar sem await e com create_task antes de
         transferir_execucao; inicio_na_fila dentro do orçamento; ordem
         log → descarte → desfecho em _blindado; nada emitido sob lock
  Funcionais
    F01  matriz dos 17 caminhos: [recebida, descartada?, fim], mesmo exec
    F02  via TELEGRAM (evento real do Telethon) / RECUPERACAO / SUCESSAO
    F03  laço da completude: execuções distintas, contexto restaurado
    F04  rajada no mesmo chat: a ordem da lane se mantém nos eventos
    F05  orçamento saturado: espera_ms mede a espera
    F06  cancelamento: exatamente um CANCELADA (na lane, antes de rodar)
    F07  ERRO_WORKER: fim ERRO; log intacto
    F08  ERRO_ENTRADA: a MESMA exceção propaga; CancelledError não é erro
    F09  FILA_CHEIA (200) e ENCERRANDO: log como antes
    F10  proveniência desligada no meio do caminho
  Regressão (T2 diferencial)
    R01  desligado × ligado × sabotado nos 17 caminhos: comportamento
         idêntico (chamadas, logs, exceção, fila, workers, lanes)
    R02  ingestão: texto_de/links_de ≡ expressões originais; ingerir igual
    R03  links do evento ≡ links da ingestão (h12, ordem, total)
    R04  desligado: nenhum coletor roda; ≤ 6 chamadas da coleta por
         mensagem; custo por chamada com folga
    R05  interfaces preservadas (assinaturas, despachar, EventoRecuperado)
  Segurança
    S01  canários (URL no texto e na entidade; segredo na exceção e no fim
         do texto) nos 17 caminhos: nada disso em evento nenhum
    S02  encaminhada de usuário: nenhum id de usuário
    S03  mensagens malformadas: no máximo degradado; pipeline igual
    S04  100 mil caracteres e 1000 links: ≤ 12 KiB; 20 links + total
    S05  o que os coletores chamam: lista fechada e puro (mesma régua de
         tests/test_eventos_contrato.py)

    python tests/test_eventos_entrada.py
"""
from __future__ import annotations

import ast
import asyncio
import datetime as dt
import inspect
import itertools
import json
import os
import re
import sys
import time
import timeit
import types as _pytypes

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import telethon  # noqa: E402  — REAL, antes do harness
from telethon import TelegramClient, utils  # noqa: E402
from telethon import events as tl_events  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import types  # noqa: E402

assert hasattr(telethon.TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from _harness_e5 import preparar, rodar  # noqa: E402

preparar()

import globals as g                                              # noqa: E402
import eventos                                                   # noqa: E402
from eventos import anel, catalogo, coleta                       # noqa: E402
from pipeline import completude, identidade, ingestao            # noqa: E402
from pipeline import orchestrator, orchestrator_fila             # noqa: E402
from pipeline import orchestrator_pipeline as op                 # noqa: E402
from pipeline.completude import EventoRecuperado                 # noqa: E402
from utils.uma_por_vez import em_execucao                        # noqa: E402
import test_eventos_contrato as regua                            # noqa: E402

# UM laço para o arquivo inteiro: o orçamento (semáforo de módulo) se
# prende ao primeiro laço em que espera.
LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(LOOP)


def _rodar(coro):
    return LOOP.run_until_complete(coro)


async def _ate(cond, voltas=20000):
    for _ in range(voltas):
        if cond():
            return True
        await asyncio.sleep(0)
    return False


# ─────────────────────────────────────────────────────────────────
# Mensagens REAIS do Telethon
# ─────────────────────────────────────────────────────────────────
TC = TelegramClient(StringSession(), 1, "0" * 32)
_ENTIDADES = {}
_IDS = itertools.count(20000)


def _agora():
    return dt.datetime.now(dt.timezone.utc)


def _entidade(cid):
    ent = _ENTIDADES.get(cid)
    if ent is None:
        ent = types.Channel(id=cid, title=f"c{cid}", photo=types.ChatPhotoEmpty(),
                            date=_agora(), access_hash=77, username=f"canal{cid}",
                            broadcast=True)
        _ENTIDADES[cid] = ent
    return ent


CANAL = 1825680721
CHAT = str(utils.get_peer_id(_entidade(CANAL)))          # "-1001825680721"


def mensagem(mid, texto="Oferta boa R$ 10 https://amzn.to/abc1", *, idade_s=5.0,
             canal=CANAL, **extra):
    """Message REAL do Telethon, ligada ao cliente (texto em markdown)."""
    m = types.Message(id=mid, peer_id=types.PeerChannel(canal),
                      date=_agora() - dt.timedelta(seconds=idade_s),
                      message=texto, post=True, **extra)
    ent = _entidade(canal)
    m._finish_init(TC, {int(utils.get_peer_id(ent)): ent}, None)
    return m


def evento_telegram(m):
    """Evento REAL do Telethon (NewMessage.Event), o que o handler entrega.
    Repassa atributo desconhecido para a mensagem."""
    return tl_events.NewMessage.Event(m)


# ─────────────────────────────────────────────────────────────────
# Camadas do _pipeline trocadas por falsas controláveis
# ─────────────────────────────────────────────────────────────────
SEGREDO_EXC = "SEGREDO_EXC_7f3a"    # palavra comum: a barreira NÃO a mascara


class _Bruta:
    def __init__(self, chat, links, mid):
        self.chat, self.links, self.mid = chat, links, mid


class _Norm:
    def __init__(self, chat, mid):
        self.chat, self.mid = chat, mid


class Camadas:
    """Cada chamada fica registrada com o que importa: é a régua do T2."""
    LINKS = ["https://amzn.to/a1", "https://amzn.to/b2"]

    def __init__(self, caminho):
        self.caminho = caminho
        self.chamadas = []
        self.liberar = asyncio.Event()

    async def ingerir(self, ev):
        self.chamadas.append(("ingerir", ev.message.id))
        if self.caminho == "ERRO_INGESTAO":
            raise ConnectionError(f"ingestão {SEGREDO_EXC}")
        return _Bruta(str(ev.chat_id), list(self.LINKS), ev.message.id)

    def apagada(self, chat, mid):
        self.chamadas.append(("apagada", chat, mid))
        return self.caminho == "ORIGEM_APAGADA"

    async def checar_e_marcar(self, chave):
        self.chamadas.append(("checar_e_marcar", chave))
        return self.caminho == "JA_PROCESSADO"

    def destino_vivo(self, chat, mid):
        self.chamadas.append(("destino_vivo", chat, mid))
        return 4242 if self.caminho == "ORIGEM_JA_PUBLICADA" else None

    async def normalizar(self, bruta):
        self.chamadas.append(("normalizar", bruta.chat, bruta.mid))
        if self.caminho == "ERRO_NORMALIZAR":
            raise ValueError(f"normalização {SEGREDO_EXC}")
        if self.caminho == "NORMALIZACAO_VAZIA":
            return None
        return _Norm(bruta.chat, bruta.mid)

    def enriquecer(self, norm):
        self.chamadas.append(("enriquecer", norm.mid))
        return f"ENR:{norm.mid}"

    def enriquecer_edicao(self, norm):
        self.chamadas.append(("enriquecer_edicao", norm.mid))
        return f"ENR_EDICAO:{norm.mid}"

    async def deve_enviar(self, enr):
        self.chamadas.append(("deve_enviar", enr))
        if self.caminho == "ERRO_DEDUP":
            raise KeyError(f"dedup {SEGREDO_EXC}")
        return self.caminho != "DEDUP"

    async def montar(self, norm):
        self.chamadas.append(("montar", norm.mid))
        if self.caminho == "ERRO_MONTAR":
            raise TypeError(f"montagem {SEGREDO_EXC}")
        return f"MONTADA:{norm.mid}"

    async def enviar(self, montada, norm=None, enr=None, is_edit=False):
        self.chamadas.append(("enviar", montada, enr, is_edit))
        if self.caminho == "ERRO_WORKER":
            raise RuntimeError(f"enviar quebrou {SEGREDO_EXC}")
        if self.caminho in ("CANCELAMENTO", "BLOQUEIA"):
            await self.liberar.wait()


class Log:
    """Grava as linhas de log do funil (idade normalizada: o relógio anda
    entre um modo e outro)."""

    def __init__(self):
        self.linhas = []

    def _gravar(self, nivel, msg):
        self.linhas.append((nivel, re.sub(r"idade=\S+", "idade=*", str(msg))))

    def info(self, msg, *a, **k):
        self._gravar("info", msg)

    def warning(self, msg, *a, **k):
        self._gravar("warning", msg)

    def error(self, msg, *a, **k):
        self._gravar("error", msg)

    def debug(self, msg, *a, **k):
        self._gravar("debug", msg)


class Bancada:
    """Liga as camadas falsas e o log gravador no funil REAL e restaura
    tudo na saída."""

    def __init__(self, cam, log):
        self.cam, self.log = cam, log

    def __enter__(self):
        cam = self.cam
        self.orig = [(m, n, getattr(m, n)) for m, n in (
            (op, "ingerir"), (op, "origem_apagada"), (op, "checar_e_marcar"),
            (op, "destino_vivo_de_origem"), (op, "normalizar"), (op, "enriquecer"),
            (op, "enriquecer_edicao"), (op, "deve_enviar_async"), (op, "montar"),
            (op, "enviar"), (op, "log_sys"), (orchestrator, "log_sys"),
            (orchestrator_fila, "log_sys"), (orchestrator, "_enfileirar"))]
        op.ingerir = cam.ingerir
        op.origem_apagada = _pytypes.SimpleNamespace(apagada=cam.apagada)
        op.checar_e_marcar = cam.checar_e_marcar
        op.destino_vivo_de_origem = cam.destino_vivo
        op.normalizar = cam.normalizar
        op.enriquecer, op.enriquecer_edicao = cam.enriquecer, cam.enriquecer_edicao
        op.deve_enviar_async, op.montar, op.enviar = cam.deve_enviar, cam.montar, cam.enviar
        op.log_sys = orchestrator.log_sys = orchestrator_fila.log_sys = self.log
        return self

    def __exit__(self, *exc):
        for m, n, v in self.orig:
            setattr(m, n, v)
        g._encerrando = False
        return False


# ─────────────────────────────────────────────────────────────────
# Modos: desligado, ligado, sabotado
# ─────────────────────────────────────────────────────────────────
def _ligar():
    eventos.desligar()
    return eventos.instalar(200000, 512 << 20)


def _desligar():
    eventos.desligar()


class Sabotagem:
    """Quebra a instrumentação POR DENTRO: as funções que os coletores usam
    e o próprio anel levantam. O pipeline não pode notar (I13)."""

    def __enter__(self):
        def quebra(*a, **k):
            raise RuntimeError("sabotagem")
        self.orig = [(eventos, n, getattr(eventos, n))
                     for n in ("previa", "h12", "representar_url")]
        self.orig += [(ingestao, n, getattr(ingestao, n))
                      for n in ("texto_de", "links_de", "chave_midia") if hasattr(ingestao, n)]
        for m, n, _ in self.orig:
            setattr(m, n, quebra)
        self.reg = eventos.registro_atual()
        self.reg.emitir = quebra
        return self

    def __exit__(self, *exc):
        for m, n, v in self.orig:
            setattr(m, n, v)
        del self.reg.emitir
        return False


# ─────────────────────────────────────────────────────────────────
# Os 17 caminhos
# ─────────────────────────────────────────────────────────────────
CAMINHOS = ("EDIT_ANTIGO", "NOVA_ANTIGA", "ERRO_ENTRADA", "ENCERRANDO", "FILA_CHEIA",
            "ERRO_WORKER", "ERRO_INGESTAO", "ORIGEM_APAGADA", "JA_PROCESSADO",
            "ORIGEM_JA_PUBLICADA", "ERRO_NORMALIZAR", "NORMALIZACAO_VAZIA", "DEDUP",
            "ERRO_DEDUP", "ERRO_MONTAR", "CONCLUSAO", "CANCELAMENTO")
DESCARTES = CAMINHOS[:15]
_ETAPAS = ("ingerir", "apagada", "checar_e_marcar", "destino_vivo", "normalizar",
           "enriquecer", "deve_enviar", "montar", "enviar")
MID = {c: 5000 + i for i, c in enumerate(CAMINHOS)}


class Esperado:
    def __init__(self, motivo, etapa, efeitos, fim, comecou, ate, *, is_edit=False,
                 idade=5.0, erro=None):
        self.motivo, self.etapa, self.efeitos, self.fim = motivo, etapa, efeitos, fim
        self.comecou, self.ate, self.is_edit = comecou, ate, is_edit
        self.idade, self.erro = idade, erro


ESPERADO = {
    "EDIT_ANTIGO": Esperado("EDIT_ANTIGO", "ADMISSAO", "NENHUM", "DESCARTADA", False, 0,
                            is_edit=True, idade=2 * 3600),
    "NOVA_ANTIGA": Esperado("NOVA_ANTIGA", "ADMISSAO", "NENHUM", "DESCARTADA", False, 0,
                            idade=600),
    "ERRO_ENTRADA": Esperado("ERRO_ENTRADA", "ENTRADA", "NENHUM", "ERRO", False, 0,
                             erro="RuntimeError"),
    "ENCERRANDO": Esperado("ENCERRANDO", "FILA", "NENHUM", "DESCARTADA", False, 0),
    "FILA_CHEIA": Esperado("FILA_CHEIA", "FILA", "NENHUM", "DESCARTADA", False, 0),
    "ERRO_WORKER": Esperado("ERRO_WORKER", None, "POSSIVEIS", "ERRO", True, 9,
                            erro="RuntimeError"),
    "ERRO_INGESTAO": Esperado("ERRO_INGESTAO", "PIPELINE", "NENHUM", "DESCARTADA", True, 1,
                              erro="ConnectionError"),
    "ORIGEM_APAGADA": Esperado("ORIGEM_APAGADA", "PIPELINE", "NENHUM", "DESCARTADA", True, 2),
    "JA_PROCESSADO": Esperado("JA_PROCESSADO", "PIPELINE", "NENHUM", "DESCARTADA", True, 3),
    "ORIGEM_JA_PUBLICADA": Esperado("ORIGEM_JA_PUBLICADA", "PIPELINE", "POSSIVEIS",
                                    "DESCARTADA", True, 4),
    "ERRO_NORMALIZAR": Esperado("ERRO_NORMALIZAR", "PIPELINE", "POSSIVEIS", "DESCARTADA",
                                True, 5, erro="ValueError"),
    "NORMALIZACAO_VAZIA": Esperado("NORMALIZACAO_VAZIA", "PIPELINE", "POSSIVEIS",
                                   "DESCARTADA", True, 5),
    "DEDUP": Esperado("DEDUP", "PIPELINE", "POSSIVEIS", "DESCARTADA", True, 7),
    "ERRO_DEDUP": Esperado("ERRO_DEDUP", "PIPELINE", "POSSIVEIS", "DESCARTADA", True, 7,
                           erro="KeyError"),
    "ERRO_MONTAR": Esperado("ERRO_MONTAR", "PIPELINE", "POSSIVEIS", "DESCARTADA", True, 8,
                            erro="TypeError"),
    "CONCLUSAO": Esperado(None, None, None, "SEM_DESFECHO", True, 9),
    "CANCELAMENTO": Esperado(None, None, None, "CANCELADA", True, 9),
}
_EXC_ENTRADA = RuntimeError(f"admissão quebrou {SEGREDO_EXC}")


class Desfecho:
    """O que o pipeline FEZ (a régua do T2) e os eventos que saíram."""

    def __init__(self, cam, log, excecao, evs, mid):
        self.chamadas, self.log, self.excecao = cam.chamadas, log.linhas, excecao
        self.fila, self.workers, self.lanes = len(g._buf), g._w_ativos, em_execucao()
        self.eventos, self.mid = evs, mid

    def funcional(self):
        nome = type(self.excecao).__name__ if self.excecao is not None else None
        return (tuple(self.chamadas), tuple(self.log), nome, self.fila, self.workers,
                self.lanes)


def _eventos_desde(reg, seq0):
    if reg is None:
        return []
    return [json.loads(c) for c in reg.ler((reg.boot_id, seq0), 1000000, 1 << 30)["cargas"]]


def _seq_atual():
    reg = eventos.registro_atual()
    return reg, (reg.saude()["seq_ultimo"] if reg is not None else 0)


def _da_execucao(evs, mid):
    """(exec, eventos da execução cuja origem.recebida é a mensagem `mid`)."""
    rec = [e for e in evs if e["tipo"] == "origem.recebida" and e["corr"].get("msg") == mid]
    if not rec:
        return None, []
    ex = rec[0]["corr"].get("exec")
    return ex, [e for e in evs if ex is not None and e["corr"].get("exec") == ex]


async def _executar_caminho(caminho, mid=None, *, msg=None, evento=None, is_edit=None,
                            cam=None, log=None):
    """Roda UM caminho pelo processar REAL e espera a task da fila."""
    esp = ESPERADO[caminho]
    is_edit = esp.is_edit if is_edit is None else is_edit
    cam, log = cam or Camadas(caminho), log or Log()
    mid = MID[caminho] if mid is None else mid
    if evento is None:
        evento = evento_telegram(msg if msg is not None else mensagem(mid, idade_s=esp.idade))
    reg, seq0 = _seq_atual()
    sentinelas, excecao = [], None
    with Bancada(cam, log):
        if caminho == "ERRO_ENTRADA":
            async def _quebra(ev, ie):
                raise _EXC_ENTRADA
            orchestrator._enfileirar = _quebra
        if caminho == "ENCERRANDO":
            g._encerrando = True
        if caminho == "FILA_CHEIA":
            sentinelas = [object() for _ in range(orchestrator_fila._FILA_MAX)]
            g._buf.update(sentinelas)
        try:
            try:
                await orchestrator.processar(evento, is_edit=is_edit)
            except BaseException as e:                 # noqa: BLE001 — a régua compara
                excecao = e
            tarefas = [t for t in g._buf if isinstance(t, asyncio.Task)]
            if caminho == "CANCELAMENTO":
                await _ate(lambda: any(c[0] == "enviar" for c in cam.chamadas))
                for t in tarefas:
                    t.cancel()
            await asyncio.gather(*tarefas, return_exceptions=True)
        finally:
            g._encerrando = False
            g._buf.difference_update(sentinelas)
    return Desfecho(cam, log, excecao, _eventos_desde(reg, seq0), evento.message.id)


def _matriz(modo):
    """Os 17 caminhos num modo: ({caminho: Desfecho}, Δ contadores)."""
    _desligar() if modo == "desligado" else _ligar()
    antes = coleta.saude_coleta()
    res = {}
    for c in CAMINHOS:
        if modo == "sabotado":
            with Sabotagem():
                res[c] = _rodar(_executar_caminho(c))
        else:
            res[c] = _rodar(_executar_caminho(c))
    depois = coleta.saude_coleta()
    return res, {k: depois.get(k, 0) - antes.get(k, 0) for k in depois}


_CACHE = {}


def _matriz_ligada():
    if "ligado" not in _CACHE:
        _CACHE["ligado"] = _matriz("ligado")
    return _CACHE["ligado"]


def _dados(evs, tipo):
    return [e["dados"] for e in evs if e["tipo"] == tipo]


def _strings(x):
    """Toda string de um evento: chaves e valores, em qualquer profundidade."""
    if isinstance(x, str):
        yield x
    elif isinstance(x, dict):
        for k, v in x.items():
            yield str(k)
            yield from _strings(v)
    elif isinstance(x, list):
        for v in x:
            yield from _strings(v)


# ─────────────────────────────────────────────────────────────────
# Contrato no código (AST)
# ─────────────────────────────────────────────────────────────────
_ARQ_FUNIL = ("pipeline/orchestrator.py", "pipeline/orchestrator_fila.py",
              "pipeline/orchestrator_pipeline.py")

LOCAIS_A2E = {
    "orchestrator.processar.recebida": ("origem.recebida", None),
    "orchestrator.processar.edit_antigo": ("origem.descartada", "EDIT_ANTIGO"),
    "orchestrator.processar.nova_antiga": ("origem.descartada", "NOVA_ANTIGA"),
    "orchestrator.processar.erro_entrada": ("origem.descartada", "ERRO_ENTRADA"),
    "orchestrator_fila.enfileirar.encerrando": ("origem.descartada", "ENCERRANDO"),
    "orchestrator_fila.enfileirar.fila_cheia": ("origem.descartada", "FILA_CHEIA"),
    "orchestrator_fila.blindado.erro_worker": ("origem.descartada", "ERRO_WORKER"),
    "orchestrator_pipeline.pipeline.erro_ingestao": ("origem.descartada", "ERRO_INGESTAO"),
    "orchestrator_pipeline.pipeline.origem_apagada": ("origem.descartada", "ORIGEM_APAGADA"),
    "orchestrator_pipeline.pipeline.ja_processado": ("origem.descartada", "JA_PROCESSADO"),
    "orchestrator_pipeline.pipeline.origem_ja_publicada": ("origem.descartada",
                                                           "ORIGEM_JA_PUBLICADA"),
    "orchestrator_pipeline.pipeline.erro_normalizar": ("origem.descartada", "ERRO_NORMALIZAR"),
    "orchestrator_pipeline.pipeline.normalizacao_vazia": ("origem.descartada",
                                                          "NORMALIZACAO_VAZIA"),
    "orchestrator_pipeline.pipeline.dedup": ("origem.descartada", "DEDUP"),
    "orchestrator_pipeline.pipeline.erro_dedup": ("origem.descartada", "ERRO_DEDUP"),
    "orchestrator_pipeline.pipeline.erro_montar": ("origem.descartada", "ERRO_MONTAR"),
}


def _emissoes(rel):
    """(tipo, local, motivo) de cada emitir_de do arquivo, pelo AST."""
    saida = []
    for n in ast.walk(regua._arvore(rel)):
        if not (isinstance(n, ast.Call) and regua._nome_final(n.func) == "emitir_de"):
            continue
        tipo = (n.args[0].value if n.args and isinstance(n.args[0], ast.Constant) else None)
        local = next((k.value.value for k in n.keywords if k.arg == "local"
                      and isinstance(k.value, ast.Constant)), None)
        motivo = None
        for d in (ast.walk(n.args[1]) if len(n.args) > 1 else ()):
            if isinstance(d, ast.Dict):
                for k, v in zip(d.keys, d.values):
                    if (isinstance(k, ast.Constant) and k.value == "motivo"
                            and isinstance(v, ast.Constant)):
                        motivo = v.value
        saida.append((tipo, local, motivo))
    return saida


def _funcao(rel, nome):
    return next(n for n in ast.walk(regua._arvore(rel))
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == nome)


def _corpo_sem_docstring(fn):
    corpo = list(fn.body)
    if corpo and isinstance(corpo[0], ast.Expr) and isinstance(corpo[0].value, ast.Constant):
        corpo = corpo[1:]
    return corpo


def test_c01_locais_um_a_um_e_catalogo(r):
    achados = [e for rel in _ARQ_FUNIL for e in _emissoes(rel)]
    locais = [loc for _, loc, _ in achados]
    r.check(len(locais) == len(set(locais)) == 16, "C01.dezesseis_pontos_unicos", str(locais))
    r.check(set(locais) == set(LOCAIS_A2E), "C01.locais_do_contrato",
            str(sorted(set(locais) ^ set(LOCAIS_A2E))))
    errados = [(t, loc, m) for t, loc, m in achados if LOCAIS_A2E.get(loc) != (t, m)]
    r.check(errados == [], "C01.local_tipo_motivo_um_a_um", str(errados))
    # Só o escopo da A2-E, nunca uma lista global que cresce a cada frente:
    # os 16 estão no catálogo e nenhum é emitido fora do funil.
    r.check(set(LOCAIS_A2E) <= catalogo.LOCAIS, "C01.catalogo_tem_os_dezesseis",
            str(sorted(set(LOCAIS_A2E) - catalogo.LOCAIS)))
    motivos = {m for _, _, m in achados if m}
    r.check(motivos == catalogo.ENUMS["motivo_descarte"] - {"REDIRECIONAMENTO_ESGOTADO"},
            "C01.quinze_motivos_fora_do_nucleo", str(sorted(motivos)))
    r.check(catalogo.VERSAO == 1 and catalogo.resumo()["hash"],
            "C01.catalogo_versao_1_com_hash", str(catalogo.resumo()))
    fora = []
    for rel in regua._arquivos():
        if rel.split(os.sep)[0] == "eventos" or rel in _ARQ_FUNIL:
            continue
        if {loc for _, loc, _ in _emissoes(rel)} & set(LOCAIS_A2E):
            fora.append(rel)
    r.check(fora == [], "C01.dezesseis_so_no_funil", str(fora))


def test_c05_estrutura_do_funil(r):
    p = _funcao("pipeline/orchestrator.py", "processar")
    awaits = [ast.unparse(n.value) for n in ast.walk(p) if isinstance(n, ast.Await)]
    r.check(awaits == ["_enfileirar(event, is_edit)"], "C05.processar_so_aguarda_enfileirar",
            str(awaits))
    corpo = _corpo_sem_docstring(p)
    w = corpo[0] if len(corpo) == 1 else None
    ok_with = (isinstance(w, ast.With)
               and ast.unparse(w.items[0].context_expr).startswith("eventos.execucao("))
    r.check(ok_with, "C05.execucao_envolve_todo_o_processar",
            ast.unparse(corpo[0])[:80] if corpo else "")
    primeiro = w.body[0] if ok_with else None
    r.check(isinstance(primeiro, ast.Expr)
            and ast.unparse(primeiro).startswith("eventos.emitir_de('origem.recebida'"),
            "C05.recebida_e_o_primeiro_comando",
            ast.unparse(primeiro)[:80] if primeiro is not None else "")

    f = _funcao("pipeline/orchestrator_fila.py", "_enfileirar")
    r.check(not any(isinstance(n, ast.Await) for n in ast.walk(f)),
            "C05.enfileirar_sem_await")
    corpo = _corpo_sem_docstring(f)
    i = next((k for k, s in enumerate(corpo)
              if "asyncio.create_task(" in ast.unparse(s)), None)
    seguinte = ast.unparse(corpo[i + 1]) if i is not None and i + 1 < len(corpo) else ""
    r.check(seguinte == "eventos.transferir_execucao(tarefa)",
            "C05.create_task_e_logo_transferir", seguinte)

    e = _funcao("pipeline/orchestrator_fila.py", "_executar")
    orc = [n for n in ast.walk(e) if isinstance(n, ast.AsyncWith)
           and ast.unparse(n.items[0].context_expr) == "_orcamento"]
    dentro = sum(1 for w in orc for n in ast.walk(w) if isinstance(n, ast.Call)
                 and ast.unparse(n.func) == "eventos.inicio_na_fila")
    total = sum(1 for n in ast.walk(e) if isinstance(n, ast.Call)
                and ast.unparse(n.func) == "eventos.inicio_na_fila")
    r.check(len(orc) == 1 and dentro == total == 1, "C05.inicio_na_fila_dentro_do_orcamento",
            f"orc={len(orc)} dentro={dentro} total={total}")

    b = _funcao("pipeline/orchestrator_fila.py", "_blindado")
    trat = [h for h in ast.walk(b) if isinstance(h, ast.ExceptHandler)
            and isinstance(h.type, ast.Name) and h.type.id == "Exception"]
    linhas = [ast.unparse(s) for s in trat[0].body] if trat else []
    ok = (len(linhas) == 3 and linhas[0].startswith("log_sys.error(")
          and "emitir_de(" in linhas[1] and "ERRO_WORKER" in linhas[1]
          and linhas[2] == "eventos.marcar_desfecho('ERRO', e)")
    r.check(ok, "C05.blindado_log_descarte_desfecho", str([x[:40] for x in linhas]))

    sob_lock = []
    for rel in _ARQ_FUNIL:
        for n in ast.walk(regua._arvore(rel)):
            if isinstance(n, ast.AsyncWith):
                for c in ast.walk(n):
                    if isinstance(c, ast.Call) and regua._nome_final(c.func) == "emitir_de":
                        sob_lock.append(f"{rel}:{c.lineno}")
    r.check(sob_lock == [], "C05.nenhuma_emissao_sob_lock", str(sob_lock))


# ─────────────────────────────────────────────────────────────────
# Contrato em execução
# ─────────────────────────────────────────────────────────────────
_CHAVES_RECEBIDA = {"via", "is_edit", "date", "edit_date", "idade_s", "grupo", "previa",
                    "texto_h12", "texto_len", "links", "links_n", "midia", "grouped_id",
                    "reply_to", "encaminhada", "local", "ts_fato"}
_CHAVES_DESCARTE = {"motivo", "etapa", "ponto", "is_edit", "efeitos_parciais", "excecao",
                    "sub_motivo", "idade_s", "ocupacao", "dest", "n_links", "local", "ts_fato"}
_CHAVES_FIM = {"resultado", "tipo", "duracao_ms", "eventos", "espera_ms", "erro_tipo",
               "local", "ts_fato"}


def _mensagem_rica(mid, *, is_edit=False):
    agora = _agora()
    return mensagem(
        mid, "Clique aqui https://amzn.to/xyz9?tag=abc-20 e veja (https://shope.ee/q1).",
        entities=[types.MessageEntityTextUrl(0, 11, url="https://mercadolivre.com/sec/1a2b")],
        media=types.MessageMediaPhoto(photo=types.Photo(
            id=555, access_hash=1, file_reference=b"", date=agora, sizes=[], dc_id=1)),
        fwd_from=types.MessageFwdHeader(date=agora, from_id=types.PeerChannel(999),
                                        channel_post=31),
        reply_to=types.MessageReplyHeader(reply_to_msg_id=5),
        grouped_id=13900000000000001, edit_date=agora if is_edit else None)


def test_c02_forma_da_recebida(r):
    _ligar()
    identidade._CHAT_USERNAME[int(CHAT)] = "promotom"
    try:
        for is_edit in (False, True):
            m = _mensagem_rica(next(_IDS), is_edit=is_edit)
            d0 = _rodar(_executar_caminho("CONCLUSAO", m.id, msg=m, is_edit=is_edit))
            ex, evs = _da_execucao(d0.eventos, m.id)
            rec = _dados(evs, "origem.recebida")
            n = f"C02.{'edicao' if is_edit else 'nova'}"
            if not r.check(len(rec) == 1, f"{n}.uma_recebida", str(len(rec))):
                continue
            d, corr = rec[0], evs[0]["corr"]
            r.check(set(d) == _CHAVES_RECEBIDA, f"{n}.chaves_exatas",
                    str(sorted(set(d) ^ _CHAVES_RECEBIDA)))
            r.check("texto" not in d, f"{n}.sem_texto_integral")
            r.check(corr == {"exec": ex, "chat": CHAT, "msg": m.id} and isinstance(ex, int),
                    f"{n}.corr", str(corr))
            texto = m.text
            links = ingestao.links_de(texto)
            r.check(d.get("via") == "TELEGRAM" and d.get("is_edit") is is_edit,
                    f"{n}.via_e_is_edit", str((d.get("via"), d.get("is_edit"))))
            r.check(d.get("date") == m.date.isoformat()
                    and d.get("edit_date") == (m.edit_date.isoformat() if is_edit else None),
                    f"{n}.datas", str((d.get("date"), d.get("edit_date"))))
            r.check(isinstance(d.get("idade_s"), float) and 4.0 <= d["idade_s"] <= 60.0,
                    f"{n}.idade_s", str(d.get("idade_s")))
            r.check(d.get("grupo") == "promotom", f"{n}.grupo", str(d.get("grupo")))
            r.check(d.get("previa") == coleta.previa(texto) and len(d["previa"]) <= 200
                    and "://" not in d["previa"], f"{n}.previa", str(d.get("previa")))
            r.check(d.get("texto_h12") == coleta.h12(texto) and d.get("texto_len") == len(texto),
                    f"{n}.texto_h12_e_len", str((d.get("texto_h12"), d.get("texto_len"))))
            r.check(d.get("links") == [coleta.representar_url(u) for u in links]
                    and d.get("links_n") == len(links) == 3
                    and all(set(x) == {"host", "h12", "motivo", "plataforma"}
                            and x["plataforma"] is None for x in d["links"]),
                    f"{n}.links_referencias", str(d.get("links")))
            r.check(d.get("midia") == {"tipo": "MessageMediaPhoto", "key": "photo:555"},
                    f"{n}.midia", str(d.get("midia")))
            r.check(d.get("grouped_id") == "13900000000000001", f"{n}.grouped_id_texto",
                    str(d.get("grouped_id")))
            r.check(d.get("reply_to") == 5, f"{n}.reply_to", str(d.get("reply_to")))
            r.check(d.get("encaminhada") == {"canal": "-1000000000999", "msg": 31},
                    f"{n}.encaminhada_de_canal", str(d.get("encaminhada")))
        m = mensagem(next(_IDS), "sem link nenhum", canal=CANAL + 7)
        d0 = _rodar(_executar_caminho("CONCLUSAO", m.id, msg=m))
        rec = _dados(_da_execucao(d0.eventos, m.id)[1], "origem.recebida")
        r.check(bool(rec) and rec[0].get("grupo") == "" and rec[0].get("links") == []
                and rec[0].get("links_n") == 0 and rec[0].get("encaminhada") is None
                and rec[0].get("reply_to") is None and rec[0].get("grouped_id") is None
                and rec[0].get("midia") == {"tipo": None, "key": ""},
                "C02.mensagem_simples", str(rec[:1]))
    finally:
        identidade._CHAT_USERNAME.pop(int(CHAT), None)


_DETALHES = {
    "EDIT_ANTIGO": lambda d: d.get("idade_s", 0) > 3600,
    "NOVA_ANTIGA": lambda d: d.get("idade_s", 0) > 120,
    "FILA_CHEIA": lambda d: d.get("ocupacao") == orchestrator_fila._FILA_MAX,
    "ORIGEM_JA_PUBLICADA": lambda d: d.get("dest") == 4242,
    "NORMALIZACAO_VAZIA": lambda d: (d.get("n_links") == 2
                                     and d.get("sub_motivo") == "DESCONHECIDO"),
    "DEDUP": lambda d: d.get("sub_motivo") == "DESCONHECIDO",
}


def _confere_descarte(r, n, d, esp, is_edit):
    r.check(set(d) <= _CHAVES_DESCARTE, f"{n}.chaves", str(sorted(set(d) - _CHAVES_DESCARTE)))
    r.check(d.get("motivo") == esp.motivo and catalogo.valido("motivo_descarte", d["motivo"]),
            f"{n}.motivo", str(d.get("motivo")))
    r.check(d.get("ponto") == "PRE" and catalogo.valido("ponto_descarte", d["ponto"]),
            f"{n}.ponto", str(d.get("ponto")))
    r.check(d.get("is_edit") is is_edit, f"{n}.is_edit", str(d.get("is_edit")))
    if esp.etapa is None:
        r.check("etapa" not in d, f"{n}.etapa_ausente", str(d.get("etapa")))
    else:
        r.check(d.get("etapa") == esp.etapa and catalogo.valido("etapa", d["etapa"]),
                f"{n}.etapa", str(d.get("etapa")))
    r.check(d.get("efeitos_parciais") == esp.efeitos
            and catalogo.valido("efeitos_parciais", d["efeitos_parciais"]),
            f"{n}.efeitos_parciais", str(d.get("efeitos_parciais")))
    r.check(d.get("excecao") == esp.erro, f"{n}.excecao_so_a_classe",
            str(d.get("excecao")))
    r.check("erro_tipo" not in d, f"{n}.sem_erro_tipo_reservado", str(d.get("erro_tipo")))
    if "sub_motivo" in d:
        r.check(catalogo.valido("sub_motivo_descarte", d["sub_motivo"]), f"{n}.sub_motivo_enum",
                str(d["sub_motivo"]))
    if esp.motivo in _DETALHES:
        r.check(_DETALHES[esp.motivo](d), f"{n}.detalhes", str(d))


def test_c03_forma_dos_descartes(r):
    res, _ = _matriz_ligada()
    for c in DESCARTES:
        esp = ESPERADO[c]
        desc = _dados(_da_execucao(res[c].eventos, res[c].mid)[1], "origem.descartada")
        if r.check(len(desc) == 1, f"C03.{c}.um_descarte", str(len(desc))):
            _confere_descarte(r, f"C03.{c}", desc[0], esp, esp.is_edit)
    _ligar()
    m = mensagem(next(_IDS))
    d0 = _rodar(_executar_caminho("NORMALIZACAO_VAZIA", m.id, msg=m, is_edit=True))
    desc = _dados(_da_execucao(d0.eventos, m.id)[1], "origem.descartada")
    if r.check(len(desc) == 1, "C03.edicao.um_descarte", str(len(desc))):
        _confere_descarte(r, "C03.edicao", desc[0], ESPERADO["NORMALIZACAO_VAZIA"], True)
    r.check([x[0] for x in d0.chamadas] == ["ingerir", "apagada", "normalizar"],
            "C03.edicao.caminho_da_edicao", str(d0.chamadas))


def test_c04_fim_por_resultado(r):
    res, _ = _matriz_ligada()
    for c in ("DEDUP", "ERRO_WORKER", "ERRO_ENTRADA", "EDIT_ANTIGO", "CONCLUSAO",
              "CANCELAMENTO"):
        esp = ESPERADO[c]
        evs = _da_execucao(res[c].eventos, res[c].mid)[1]
        fins = _dados(evs, "execucao.fim")
        if not r.check(len(fins) == 1, f"C04.{c}.um_fim", str(len(fins))):
            continue
        d = fins[0]
        r.check(set(d) <= _CHAVES_FIM, f"C04.{c}.chaves", str(sorted(set(d) - _CHAVES_FIM)))
        r.check(d.get("resultado") == esp.fim
                and catalogo.valido("resultado_execucao", d["resultado"]),
                f"C04.{c}.resultado", str(d.get("resultado")))
        r.check(d.get("tipo") == "MENSAGEM" and d.get("local") == "eventos.execucao.fim",
                f"C04.{c}.tipo_e_local", str((d.get("tipo"), d.get("local"))))
        r.check(("espera_ms" in d) is esp.comecou, f"C04.{c}.espera_so_se_comecou",
                str(d.get("espera_ms")))
        r.check(d.get("erro_tipo") == (esp.erro if esp.fim == "ERRO" else None),
                f"C04.{c}.erro_tipo", str(d.get("erro_tipo")))
        r.check(d.get("eventos") == len(evs) - 1, f"C04.{c}.eventos_contados",
                f"{d.get('eventos')} vs {len(evs) - 1}")


# ─────────────────────────────────────────────────────────────────
# Funcionais
# ─────────────────────────────────────────────────────────────────
def test_f01_matriz_dos_17_caminhos(r):
    res, delta = _matriz_ligada()
    for c in CAMINHOS:
        esp, d = ESPERADO[c], res[c]
        r.check([x[0] for x in d.chamadas] == list(_ETAPAS[:esp.ate]),
                f"F01.{c}.camadas", str([x[0] for x in d.chamadas]))
        ex, evs = _da_execucao(d.eventos, d.mid)
        tipos = [e["tipo"] for e in evs]
        esperado = (["origem.recebida", "origem.descartada", "execucao.fim"] if esp.motivo
                    else ["origem.recebida", "execucao.fim"])
        r.check(tipos == esperado, f"F01.{c}.sequencia", str(tipos))
        seqs = [e["seq"] for e in evs]
        r.check(seqs == sorted(set(seqs)), f"F01.{c}.ordem", str(seqs))
        r.check(all(e["corr"].get("chat") == CHAT and e["corr"].get("msg") == d.mid
                    and e["corr"].get("exec") == ex and "exec_pai" not in e["corr"]
                    for e in evs), f"F01.{c}.correlacao", str([e["corr"] for e in evs]))
        desc = _dados(evs, "origem.descartada")
        r.check([x.get("motivo") for x in desc] == ([esp.motivo] if esp.motivo else []),
                f"F01.{c}.motivo", str(desc))
        r.check([x.get("resultado") for x in _dados(evs, "execucao.fim")] == [esp.fim],
                f"F01.{c}.resultado", str(_dados(evs, "execucao.fim")))
        r.check(d.fila == 0 and d.workers == 0 and d.lanes == 0, f"F01.{c}.estado_limpo",
                f"fila={d.fila} workers={d.workers} lanes={d.lanes}")
    r.check(delta.get("execucoes") == delta.get("fins") == len(CAMINHOS),
            "F01.uma_execucao_e_um_fim_por_caminho", str(delta))
    r.check(coleta.exec_atual() is None, "F01.nenhuma_execucao_vazada")


class _ClienteFalso:
    """Cliente do Telegram falso: só get_messages (a busca da completude)."""

    def __init__(self, msgs):
        self.msgs = {m.id: m for m in msgs}

    async def get_messages(self, ent, ids):
        return [self.msgs.get(i) for i in ids]


async def _pela_completude(msgs, cam):
    """Entrega `msgs` pelo caminho REAL da completude (o mesmo do buraco)."""
    estado, canais, log_ing = dict(completude._estado), dict(completude._canais), \
        completude.log_ing
    completude.log_ing = Log()
    try:
        completude.instalar(_ClienteFalso(msgs), [_entidade(CANAL)], orchestrator.processar)
        with Bancada(cam, Log()):
            await completude._buscar_e_entregar(int(CHAT), completude._Canal(1),
                                                [m.id for m in msgs])
            fora = coleta.exec_atual()
            await asyncio.gather(*[t for t in g._buf if isinstance(t, asyncio.Task)],
                                 return_exceptions=True)
        return fora
    finally:
        completude._estado.clear()
        completude._estado.update(estado)
        completude._canais.clear()
        completude._canais.update(canais)
        completude.log_ing = log_ing


def test_f02_via_das_tres_origens(r):
    _ligar()
    m = mensagem(next(_IDS))
    m.via = "SUCESSAO"      # armadilha: o evento do Telethon repassa atributo à mensagem
    d0 = _rodar(_executar_caminho("CONCLUSAO", m.id, msg=m))
    rec = _dados(_da_execucao(d0.eventos, m.id)[1], "origem.recebida")
    r.check([x.get("via") for x in rec] == ["TELEGRAM"], "F02.telegram", str(rec[:1]))

    reg, seq0 = _seq_atual()
    m = mensagem(next(_IDS))
    _rodar(_pela_completude([m], Camadas("CONCLUSAO")))
    rec = _dados(_da_execucao(_eventos_desde(reg, seq0), m.id)[1], "origem.recebida")
    r.check([x.get("via") for x in rec] == ["RECUPERACAO"], "F02.recuperacao", str(rec[:1]))

    try:
        m = mensagem(next(_IDS))
        ev = EventoRecuperado(m, via="SUCESSAO")
        d0 = _rodar(_executar_caminho("CONCLUSAO", m.id, evento=ev, is_edit=True))
        rec = _dados(_da_execucao(d0.eventos, m.id)[1], "origem.recebida")
        r.check([x.get("via") for x in rec] == ["SUCESSAO"], "F02.sucessao", str(rec[:1]))
    except TypeError as e:
        r.check(False, "F02.sucessao", f"EventoRecuperado sem via: {e}")

    def chamadas(rel):
        return [n for n in ast.walk(regua._arvore(rel)) if isinstance(n, ast.Call)
                and ast.unparse(n.func) == "EventoRecuperado"]
    suc = chamadas("pipeline/sucessao.py")
    r.check(len(suc) >= 1 and all(
        [(k.arg, getattr(k.value, "value", None)) for k in c.keywords] == [("via", "SUCESSAO")]
        for c in suc), "F02.sucessao_passa_via_no_codigo", str([ast.unparse(c) for c in suc]))
    comp = chamadas("pipeline/completude.py")
    r.check(len(comp) >= 1 and all(not c.keywords and len(c.args) == 1 for c in comp),
            "F02.completude_usa_o_padrao", str([ast.unparse(c) for c in comp]))


def test_f03_laco_da_completude(r):
    _ligar()
    reg, seq0 = _seq_atual()
    msgs = [mensagem(next(_IDS)) for _ in range(3)]
    fora = _rodar(_pela_completude(msgs, Camadas("CONCLUSAO")))
    evs = _eventos_desde(reg, seq0)
    execs = [_da_execucao(evs, m.id)[0] for m in msgs]
    r.check(None not in execs and len(set(execs)) == 3, "F03.tres_execucoes_distintas",
            str(execs))
    rec = [e for e in evs if e["tipo"] == "origem.recebida"]
    r.check([e["corr"].get("msg") for e in rec] == [m.id for m in msgs]
            and all(e["dados"].get("via") == "RECUPERACAO" and "exec_pai" not in e["corr"]
                    for e in rec), "F03.em_ordem_sem_pai", str([e["corr"] for e in rec]))
    r.check(fora is None and coleta.exec_atual() is None, "F03.contexto_restaurado", str(fora))
    fins = [e["corr"].get("exec") for e in evs if e["tipo"] == "execucao.fim"]
    r.check(None not in execs and sorted(fins, key=str) == sorted(execs, key=str),
            "F03.um_fim_por_execucao", str(fins))


def test_f04_rajada_no_mesmo_chat(r):
    _ligar()
    reg, seq0 = _seq_atual()
    cam = Camadas("CONCLUSAO")
    msgs = [mensagem(next(_IDS)) for _ in range(30)]

    async def corpo():
        with Bancada(cam, Log()):
            for m in msgs:
                await orchestrator.processar(evento_telegram(m), is_edit=False)
            await asyncio.gather(*[t for t in g._buf if isinstance(t, asyncio.Task)],
                                 return_exceptions=True)
    _rodar(corpo())
    ids = [m.id for m in msgs]
    evs = _eventos_desde(reg, seq0)
    r.check([e["corr"]["msg"] for e in evs if e["tipo"] == "origem.recebida"] == ids,
            "F04.recebidas_em_ordem")
    r.check([c[1] for c in cam.chamadas if c[0] == "enviar"] == [f"MONTADA:{i}" for i in ids],
            "F04.lane_em_ordem")
    r.check([e["corr"]["msg"] for e in evs if e["tipo"] == "execucao.fim"] == ids,
            "F04.fins_em_ordem")
    porexec = {}
    for e in evs:
        porexec.setdefault(e["corr"].get("exec"), []).append(e["tipo"])
    r.check(len(porexec) == 30 and all(v == ["origem.recebida", "execucao.fim"]
                                       for v in porexec.values()),
            "F04.cada_execucao_completa", str(list(porexec.values())[:3]))


def test_f05_espera_no_orcamento(r):
    _ligar()
    reg, seq0 = _seq_atual()
    cam = Camadas("BLOQUEIA")
    bloqueadas = [mensagem(next(_IDS), canal=CANAL + 1 + i) for i in range(16)]
    x = mensagem(next(_IDS), canal=CANAL + 100)

    async def corpo():
        with Bancada(cam, Log()):
            for m in bloqueadas:
                await orchestrator.processar(evento_telegram(m), is_edit=False)
            await _ate(lambda: sum(1 for c in cam.chamadas if c[0] == "enviar") == 16)
            await orchestrator.processar(evento_telegram(x), is_edit=False)
            await asyncio.sleep(0.08)
            cam.liberar.set()
            await asyncio.gather(*[t for t in g._buf if isinstance(t, asyncio.Task)],
                                 return_exceptions=True)
    _rodar(corpo())
    evs = _eventos_desde(reg, seq0)
    fx = _dados(_da_execucao(evs, x.id)[1], "execucao.fim")
    esp = fx[0].get("espera_ms", -1) if fx else -1
    r.check(esp >= 60, "F05.espera_mede_o_orcamento", f"{esp} ms")
    outras = [_dados(_da_execucao(evs, m.id)[1], "execucao.fim") for m in bloqueadas]
    r.check(all(f and f[0].get("espera_ms", 999) < 60 for f in outras),
            "F05.sem_espera_quando_ha_vaga", str([f[0].get("espera_ms") for f in outras if f]))


def test_f06_cancelamento(r):
    _ligar()
    reg, seq0 = _seq_atual()
    antes = coleta.saude_coleta()
    cam = Camadas("BLOQUEIA")
    a, b, c = (mensagem(next(_IDS)) for _ in range(3))

    async def corpo():
        with Bancada(cam, Log()):
            await orchestrator.processar(evento_telegram(a), is_edit=False)
            ta = next(t for t in g._buf if isinstance(t, asyncio.Task))
            await _ate(lambda: any(x[0] == "enviar" for x in cam.chamadas))
            await orchestrator.processar(evento_telegram(b), is_edit=False)
            tb = next(t for t in g._buf if isinstance(t, asyncio.Task) and t is not ta)
            for _ in range(5):
                await asyncio.sleep(0)
            tb.cancel()                              # b esperava a lane de a
            await asyncio.gather(tb, return_exceptions=True)
            await orchestrator.processar(evento_telegram(c), is_edit=False)
            tc = next(t for t in g._buf if isinstance(t, asyncio.Task)
                      and t is not ta and t is not tb)
            tc.cancel()                              # antes de rodar
            await asyncio.gather(tc, return_exceptions=True)
            cam.liberar.set()
            await asyncio.gather(ta, return_exceptions=True)
    _rodar(corpo())
    evs = _eventos_desde(reg, seq0)
    fa, fb, fc = (_dados(_da_execucao(evs, m.id)[1], "execucao.fim") for m in (a, b, c))
    r.check([f.get("resultado") for f in fb] == ["CANCELADA"] and "espera_ms" not in fb[0],
            "F06.cancelada_na_lane", str(fb))
    r.check([f.get("resultado") for f in fc] == ["CANCELADA"] and "espera_ms" not in fc[0],
            "F06.cancelada_antes_de_rodar", str(fc))
    r.check([f.get("resultado") for f in fa] == ["SEM_DESFECHO"], "F06.a_que_rodou_termina",
            str(fa))
    depois = coleta.saude_coleta()
    r.check(depois["fins"] - antes["fins"] == 3 and depois["execucoes"] - antes["execucoes"] == 3,
            "F06.exatamente_um_fim_cada",
            f"fins {depois['fins'] - antes['fins']} execs {depois['execucoes'] - antes['execucoes']}")
    r.check(em_execucao() == 0 and not g._buf, "F06.nada_pendurado")


def test_f07_erro_worker(r):
    res_on, _ = _matriz_ligada()
    _desligar()
    off = _rodar(_executar_caminho("ERRO_WORKER"))
    on = res_on["ERRO_WORKER"]
    r.check(("error", f"❌ Worker: enviar quebrou {SEGREDO_EXC}") in on.log
            and on.log == off.log, "F07.log_intacto", str(on.log))
    evs = _da_execucao(on.eventos, on.mid)[1]
    desc, fim = _dados(evs, "origem.descartada"), _dados(evs, "execucao.fim")
    r.check(bool(desc) and desc[0].get("excecao") == "RuntimeError" and "erro_tipo" not in desc[0]
            and desc[0].get("efeitos_parciais") == "POSSIVEIS" and "etapa" not in desc[0],
            "F07.descarte", str(desc))
    r.check(bool(fim) and fim[0].get("resultado") == "ERRO"
            and fim[0].get("erro_tipo") == "RuntimeError", "F07.fim_erro", str(fim))


def test_f08_erro_de_entrada(r):
    res_on, _ = _matriz_ligada()
    on = res_on["ERRO_ENTRADA"]
    r.check(on.excecao is _EXC_ENTRADA, "F08.mesma_excecao_propaga", repr(on.excecao))
    evs = _da_execucao(on.eventos, on.mid)[1]
    r.check([e["tipo"] for e in evs] == ["origem.recebida", "origem.descartada",
                                          "execucao.fim"], "F08.eventos",
            str([e["tipo"] for e in evs]))
    _ligar()
    cancel = asyncio.CancelledError()
    reg, seq0 = _seq_atual()
    m = mensagem(next(_IDS))
    capturada = []

    async def corpo():
        with Bancada(Camadas("CONCLUSAO"), Log()):
            async def _cancela(ev, ie):
                raise cancel
            orchestrator._enfileirar = _cancela
            try:
                await orchestrator.processar(evento_telegram(m), is_edit=False)
            except BaseException as e:               # noqa: BLE001
                capturada.append(e)
    _rodar(corpo())
    r.check(capturada and capturada[0] is cancel, "F08.cancelamento_propaga_igual",
            repr(capturada))
    evs = _da_execucao(_eventos_desde(reg, seq0), m.id)[1]
    r.check([e["tipo"] for e in evs] == ["origem.recebida", "execucao.fim"]
            and evs[-1]["dados"].get("resultado") == "CANCELADA",
            "F08.cancelamento_nao_e_erro_de_entrada", str([e["tipo"] for e in evs]))


def test_f09_fila_cheia_e_encerrando(r):
    res_on, _ = _matriz_ligada()
    _desligar()
    for c, linha in (("FILA_CHEIA", ("warning", f"⚠️ Fila cheia | id={MID['FILA_CHEIA']}")),
                     ("ENCERRANDO", None)):
        off = _rodar(_executar_caminho(c))
        on = res_on[c]
        r.check(on.log == off.log == ([linha] if linha else []), f"F09.{c}.log_como_antes",
                str(on.log))
        r.check(on.chamadas == off.chamadas == [], f"F09.{c}.nada_entra_no_pipeline")
        desc = _dados(_da_execucao(on.eventos, on.mid)[1], "origem.descartada")
        r.check([x.get("motivo") for x in desc] == [c], f"F09.{c}.descarte", str(desc))


def test_f10_desligada_no_meio(r):
    reg = _ligar()
    seq0 = reg.saude()["seq_ultimo"]
    cam = Camadas("BLOQUEIA")
    m = mensagem(next(_IDS))
    erros = []

    async def corpo():
        with Bancada(cam, Log()):
            await orchestrator.processar(evento_telegram(m), is_edit=False)
            await _ate(lambda: any(x[0] == "enviar" for x in cam.chamadas))
            eventos.desligar()
            cam.liberar.set()
            for res in await asyncio.gather(*[t for t in g._buf if isinstance(t, asyncio.Task)],
                                            return_exceptions=True):
                if isinstance(res, BaseException):
                    erros.append(res)
    _rodar(corpo())
    tipos = [e["tipo"] for e in _da_execucao(_eventos_desde(reg, seq0), m.id)[1]]
    r.check(tipos == ["origem.recebida"], "F10.depois_de_desligar_nada_sai", str(tipos))
    r.check(erros == [] and [x[0] for x in cam.chamadas] == list(_ETAPAS),
            "F10.pipeline_segue_igual", str(erros))
    r.check(eventos.registro_atual() is None, "F10.continua_desligada")
    _ligar()


# ─────────────────────────────────────────────────────────────────
# Regressão (T2 diferencial)
# ─────────────────────────────────────────────────────────────────
def test_r01_desligado_ligado_sabotado(r):
    off, _ = _matriz("desligado")
    on, _ = _matriz_ligada()
    sab, delta = _matriz("sabotado")
    for c in CAMINHOS:
        r.check(off[c].funcional() == on[c].funcional(), f"R01.{c}.ligado_igual",
                f"{off[c].funcional()} vs {on[c].funcional()}")
        r.check(off[c].funcional() == sab[c].funcional(), f"R01.{c}.sabotado_igual",
                f"{off[c].funcional()} vs {sab[c].funcional()}")
        r.check(off[c].eventos == [] and sab[c].eventos == [],
                f"R01.{c}.desligado_e_sabotado_sem_evento")
    r.check(delta.get("falhas_coleta", 0) > 0 and delta.get("falhas_internas", 0) > 0,
            "R01.sabotagem_atingiu_a_coleta", str(delta))
    r.check(off["ERRO_ENTRADA"].excecao is _EXC_ENTRADA
            and sab["ERRO_ENTRADA"].excecao is _EXC_ENTRADA, "R01.excecao_e_a_mesma")


# Cópia do padrão de pipeline/ingestao.py:14 em b4c0db2 — no fonte, os
# quatro invisíveis estão como escapes de texto dentro de raw string (o
# re os interpreta); aqui a barra vem de chr(92) para o texto ser igual.
_RE_URL_ORIGINAL = re.compile(r'https?://[^\s\)\]>,"\'<' + "".join(
    chr(92) + "u" + h for h in ("200b", "200c", "200d", "2060")) + "]+")


def _texto_original(m):          # pipeline/ingestao.py:111 em b4c0db2
    return m.text or getattr(m, "message", "") or ""


def _links_original(texto):      # pipeline/ingestao.py:112 em b4c0db2
    return [u.strip().rstrip('.,;)>]}!?') for u in _RE_URL_ORIGINAL.findall(texto)]


def _corpus():
    zw = chr(0x200B)
    return [
        mensagem(next(_IDS), "Veja https://amzn.to/abc). Corre!"),
        mensagem(next(_IDS), "Clique aqui",
                 entities=[types.MessageEntityTextUrl(0, 11,
                                                      url="https://mercadolivre.com/sec/xyz")]),
        mensagem(next(_IDS), "a https://amzn.to/a" + zw + "b fim"),
        mensagem(next(_IDS), ""),
        mensagem(next(_IDS), "🔥 https://shope.ee/abc?x=1#frag, e "
                             "https://s.click.aliexpress.com/e/_abc!"),
        mensagem(next(_IDS), "(https://amzn.to/xyz)"),
        mensagem(next(_IDS), "https://a.com/xhttps://b.com/y"),
        mensagem(next(_IDS), "**Negrito** https://magazineluiza.com.br/p/123/ __fim__"),
    ]


def _sem_cache_de_username(teste):
    """ingerir aquece o cache id→@username (identidade): o teste devolve
    o cache como estava."""
    def rodar_limpo(r):
        antes = dict(identidade._CHAT_USERNAME)
        try:
            teste(r)
        finally:
            identidade._CHAT_USERNAME.clear()
            identidade._CHAT_USERNAME.update(antes)
    rodar_limpo.__name__ = teste.__name__
    return rodar_limpo


@_sem_cache_de_username
def test_r02_ingestao_mesma_extracao(r):
    r.check(ingestao._RE_URL.pattern == _RE_URL_ORIGINAL.pattern, "R02.regex_intacta")
    texto_de = getattr(ingestao, "texto_de", None)
    links_de = getattr(ingestao, "links_de", None)
    if not r.check(callable(texto_de) and callable(links_de), "R02.funcoes_puras_existem"):
        return
    corpus = _corpus()
    vazio = _pytypes.SimpleNamespace(text=None, message=None)
    r.check(texto_de(vazio) == _texto_original(vazio) == "", "R02.texto_vazio")
    for i, m in enumerate(corpus):
        t = _texto_original(m)
        r.check(texto_de(m) == t, f"R02.{i}.texto_de", repr(t))
        r.check(links_de(t) == _links_original(t), f"R02.{i}.links_de",
                f"{links_de(t)} vs {_links_original(t)}")
        bruta = _rodar(ingestao.ingerir(EventoRecuperado(m)))
        r.check(bruta.texto == t and bruta.links == _links_original(t),
                f"R02.{i}.ingerir_igual", f"{bruta.links}")


@_sem_cache_de_username
def test_r03_links_do_evento_iguais_aos_da_ingestao(r):
    _ligar()
    for i, m in enumerate(_corpus()):
        bruta = _rodar(ingestao.ingerir(EventoRecuperado(m)))
        d0 = _rodar(_executar_caminho("CONCLUSAO", m.id, msg=m))
        rec = _dados(_da_execucao(d0.eventos, m.id)[1], "origem.recebida")
        if not r.check(len(rec) == 1, f"R03.{i}.recebida", str(len(rec))):
            continue
        r.check([x.get("h12") for x in rec[0].get("links", [])]
                == [coleta.h12(u) for u in bruta.links[:20]]
                and rec[0].get("links_n") == len(bruta.links)
                and rec[0].get("texto_h12") == coleta.h12(bruta.texto)
                and rec[0].get("texto_len") == len(bruta.texto),
                f"R03.{i}.mesmos_links_e_texto", str(rec[0].get("links")))


class _Contador:
    def __init__(self, f, conta, nome):
        self.f, self.conta, self.nome = f, conta, nome

    def __call__(self, *a, **k):
        self.conta[self.nome] = self.conta.get(self.nome, 0) + 1
        return self.f(*a, **k)


def test_r04_dormente_nada_roda(r):
    _desligar()
    puras, api = {}, {}
    alvos = [(eventos, n, puras) for n in ("previa", "h12", "representar_url")]
    alvos += [(ingestao, n, puras) for n in ("texto_de", "links_de", "chave_midia")
              if hasattr(ingestao, n)]
    alvos += [(eventos, n, api) for n in ("execucao", "emitir_de", "transferir_execucao",
                                          "inicio_na_fila", "marcar_desfecho")]
    orig = [(m, n, getattr(m, n)) for m, n, _ in alvos]
    por_caminho = {}
    try:
        for m, n, conta in alvos:
            setattr(m, n, _Contador(getattr(m, n), conta, n))
        for c in CAMINHOS:
            api.clear()
            _rodar(_executar_caminho(c))
            por_caminho[c] = sum(api.values())
    finally:
        for m, n, v in orig:
            setattr(m, n, v)
    r.check(puras == {}, "R04.nenhum_coletor_roda_desligado", str(puras))
    r.check(max(por_caminho.values()) <= 6, "R04.no_maximo_6_chamadas_por_mensagem",
            str(por_caminho))
    r.check(por_caminho.get("CONCLUSAO", 0) >= 4, "R04.funil_instrumentado",
            str(por_caminho))
    x = {"a": 1}
    custos = {
        "emitir_de": lambda: eventos.emitir_de("origem.recebida", lambda: ({}, x),
                                               local="eventos.execucao.fim"),
        "execucao": lambda: eventos.execucao(lambda: x).__enter__().__exit__(None, None, None),
        "transferir_execucao": lambda: eventos.transferir_execucao(None),
        "inicio_na_fila": eventos.inicio_na_fila,
        "marcar_desfecho": lambda: eventos.marcar_desfecho("ERRO"),
    }
    medidos = {k: min(timeit.repeat(f, number=20000, repeat=3)) / 20000 * 1e6
               for k, f in custos.items()}
    r.check(all(v <= 5.0 for v in medidos.values()), "R04.custo_por_chamada_com_folga",
            ", ".join(f"{k}={v:.3f}us" for k, v in medidos.items()))


def test_r05_interfaces_preservadas(r):
    def params(f):
        return [(p.name, p.default) for p in inspect.signature(f).parameters.values()]
    vazio = inspect.Parameter.empty
    r.check(params(orchestrator.processar) == [("event", vazio), ("is_edit", False)],
            "R05.processar", str(params(orchestrator.processar)))
    for f in (orchestrator_fila._enfileirar, orchestrator_fila._blindado,
              orchestrator_fila._executar):
        r.check(params(f) == [("event", vazio), ("is_edit", vazio)], f"R05.{f.__name__}",
                str(params(f)))
    r.check(params(op._pipeline) == [("event", vazio), ("is_edit", False)], "R05._pipeline",
            str(params(op._pipeline)))
    m = mensagem(next(_IDS))
    ev = EventoRecuperado(m)
    r.check(ev.message is m and ev.chat_id == m.chat_id
            and getattr(ev, "via", None) == "RECUPERACAO", "R05.evento_recuperado_compativel")
    r.check(EventoRecuperado.__slots__ == ("message", "via"), "R05.slots",
            str(EventoRecuperado.__slots__))
    for rel in ("pipeline/completude.py", "pipeline/sucessao.py"):
        chamadas = [n for n in ast.walk(regua._arvore(rel)) if isinstance(n, ast.Call)
                    and "despachar" in ast.unparse(n.func)]
        r.check(chamadas and all(len(c.args) == 1 and [k.arg for k in c.keywords] == ["is_edit"]
                                 for c in chamadas), f"R05.despachar_{rel}",
                str([ast.unparse(c) for c in chamadas]))
    fonte_main = regua._arvore("main.py")
    r.check(not any(isinstance(n, ast.Call) and regua._nome_final(n.func) in
                    ("emitir_de", "execucao", "transferir_execucao")
                    for n in ast.walk(fonte_main)), "R05.main_intocado_pela_a2e")


# ─────────────────────────────────────────────────────────────────
# Segurança
# ─────────────────────────────────────────────────────────────────
_FW, _DIV = chr(0xFF0F), chr(0x2215)
TEXTO_CANARIO = " ".join([
    "Oferta https://amzn.to/CANARIOa1?tag=CANARIOtag-20",
    "mercadolivre.com/sec/CANARIOml",
    "[link](https://shope.ee/CANARIOmd)",
    "https://a.co/CANARIO1https://b.co/CANARIO2",
    "texto(https://x.co/CANARIOpar)",
    "https://x.co/p#CANARIOfrag",
    "user@CANARIOmail.com",
    "x=CANARIOeq%20",
    "amzn.to" + _FW + "CANARIOfw",
    "amzn.to" + _DIV + "CANARIOdiv",
]) + " " + "palavras comuns " * 30 + "SEGREDO_TXT_9b"


def _oraculo(r, nome, evs, *proibidos):
    bruto = json.dumps(evs, ensure_ascii=False)
    for p in ("CANARIO", SEGREDO_EXC, "SEGREDO_TXT", "://") + proibidos:
        r.check(p not in bruto, f"{nome}.sem_{p}", p)
    ruins = [s for e in evs for s in _strings(e) if not coleta._sem_separador(s)]
    r.check(ruins == [], f"{nome}.nenhuma_string_com_separador", str(ruins[:5]))


def test_s01_canarios(r):
    _ligar()
    reg, seq0 = _seq_atual()
    m = mensagem(next(_IDS), TEXTO_CANARIO,
                 entities=[types.MessageEntityTextUrl(
                     0, 6, url="https://amzn.to/CANARIOent?tag=x")])
    r.check("CANARIOent" in m.text and "SEGREDO_TXT" in m.text, "S01.canarios_no_texto_lido")
    for c in CAMINHOS:                 # os 17 caminhos, com a mensagem de canários
        mm = mensagem(next(_IDS), TEXTO_CANARIO, idade_s=ESPERADO[c].idade,
                      entities=m.entities)
        _rodar(_executar_caminho(c, mm.id, msg=mm))
    evs = _eventos_desde(reg, seq0)
    tipos = [e["tipo"] for e in evs]
    r.check(tipos.count("origem.recebida") == tipos.count("execucao.fim") == len(CAMINHOS)
            and tipos.count("origem.descartada") == len(DESCARTES),
            "S01.os_17_caminhos_emitiram", str(len(evs)))
    _oraculo(r, "S01", evs)


def test_s02_encaminhada_de_usuario(r):
    _ligar()
    reg, seq0 = _seq_atual()
    m = mensagem(next(_IDS), fwd_from=types.MessageFwdHeader(
        date=_agora(), from_id=types.PeerUser(987654321), from_name="Fulano Oculto"))
    _rodar(_executar_caminho("CONCLUSAO", m.id, msg=m))
    evs = _eventos_desde(reg, seq0)
    rec = _dados(_da_execucao(evs, m.id)[1], "origem.recebida")
    r.check(bool(rec) and rec[0].get("encaminhada") == {"canal": None, "msg": None},
            "S02.sem_canal_nem_msg", str(rec[:1]))
    bruto = json.dumps(evs, ensure_ascii=False)
    r.check("987654321" not in bruto and "Fulano" not in bruto, "S02.nenhum_dado_do_usuario")


class _MsgFalsa:
    def __init__(self, mid, **attrs):
        self.id = mid
        self.date = _agora()
        self.text = self.message = "oi https://amzn.to/x"
        self.edit_date = self.media = self.fwd_from = self.reply_to = None
        self.grouped_id = self.entities = None
        for k, v in attrs.items():
            setattr(self, k, v)


class _MsgDataQuebra(_MsgFalsa):
    @property
    def date(self):
        raise RuntimeError(f"data ilegível {SEGREDO_EXC}")

    @date.setter
    def date(self, v):
        pass


class _EvFalso:
    def __init__(self, m, chat=int(CHAT)):
        self.message, self.chat_id = m, chat


def test_s03_mensagens_malformadas(r):
    casos = {
        "data_nula": _MsgFalsa(1, date=None),
        "midia_exotica": _MsgFalsa(2, media=object()),
        "encaminhada_estranha": _MsgFalsa(3, fwd_from=_pytypes.SimpleNamespace(from_id=42)),
        "resposta_estranha": _MsgFalsa(4, reply_to="x"),
        "sem_texto": _MsgFalsa(5, text=None, message=None),
        "album_estranho": _MsgFalsa(6, grouped_id=[1, 2]),
        "data_que_quebra": _MsgDataQuebra(7),
    }
    todos = []
    for nome, m in casos.items():
        _desligar()
        off = _rodar(_executar_caminho("CONCLUSAO", m.id, evento=_EvFalso(m)))
        _ligar()
        on = _rodar(_executar_caminho("CONCLUSAO", m.id, evento=_EvFalso(m)))
        todos += on.eventos
        r.check(off.funcional() == on.funcional(), f"S03.{nome}.pipeline_igual",
                f"{off.funcional()} vs {on.funcional()}")
        r.check(all(len(json.dumps(e, ensure_ascii=False).encode()) <= coleta.TETO_EVENTO
                    for e in on.eventos), f"S03.{nome}.cabe")
        tipos = [e["tipo"] for e in on.eventos]
        r.check(tipos.count("origem.recebida") == 1 and tipos.count("execucao.fim") == 1
                and tipos[0] == "origem.recebida" and tipos[-1] == "execucao.fim",
                f"S03.{nome}.recebida_e_fim", str(tipos))
    r.check(type(off.excecao).__name__ == "RuntimeError" and on.excecao is not None
            and str(on.excecao) == str(off.excecao), "S03.data_que_quebra.excecao_igual",
            repr(on.excecao))
    _oraculo(r, "S03", todos)


def test_s04_volume(r):
    _ligar()
    links = " ".join(f"https://amzn.to/x{i}?tag=t-20" for i in range(1000))
    texto = links + " " + "a" * (100_000 - len(links))
    m = _MsgFalsa(9, text=texto, message=texto)
    t0 = time.perf_counter()
    on = _rodar(_executar_caminho("CONCLUSAO", m.id, evento=_EvFalso(m)))
    dur = time.perf_counter() - t0
    rec = _dados(on.eventos, "origem.recebida")
    r.check(bool(rec) and len(rec[0].get("links", [])) == 20 and rec[0].get("links_n") == 1000
            and len(rec[0].get("previa", "x" * 999)) <= 200
            and rec[0].get("texto_len") == len(texto), "S04.vinte_links_e_total",
            str({k: (v if k != "links" else len(v)) for k, v in (rec[0] if rec else {}).items()
                 if k != "previa"}))
    reg = eventos.registro_atual()
    cargas = reg.ler((reg.boot_id, 0), 1000000, 1 << 30)["cargas"]
    r.check(bool(cargas) and all(len(c) <= coleta.TETO_EVENTO
                                 and anel._MARCA_CORTE.encode() not in c for c in cargas)
            and reg.saude()["excedidos"] == 0,
            "S04.tudo_cabe_sem_corte_do_anel", str(max((len(c) for c in cargas), default=0)))
    r.check(dur < 0.25, "S04.tempo_com_folga", f"{dur * 1000:.1f} ms")


_HELPERS_PERMITIDOS = {"ingestao.texto_de", "ingestao.links_de", "ingestao.chave_midia",
                       "username_de", "_idade_seg"}
_BUILTINS_PERMITIDOS = {"len", "str", "int", "round", "bool", "type", "getattr",
                        "isinstance"}
_METODOS_PERMITIDOS = {"isoformat"}
_DEFS = {"ingestao.texto_de": ("pipeline/ingestao.py", "texto_de"),
         "ingestao.links_de": ("pipeline/ingestao.py", "links_de"),
         "ingestao.chave_midia": ("pipeline/ingestao.py", "chave_midia"),
         "username_de": ("pipeline/identidade.py", "username_de"),
         "_idade_seg": ("logger.py", "_idade_seg")}


def _importados(arv):
    nomes = set()
    for n in ast.walk(arv):
        if isinstance(n, ast.Import):
            nomes |= {(a.asname or a.name).split(".")[0] for a in n.names}
        elif isinstance(n, ast.ImportFrom):
            nomes |= {a.asname or a.name for a in n.names}
    return nomes


def test_s05_coletores_chamam_so_o_que_e_puro(r):
    estranhos, corpos = [], 0
    for rel in _ARQ_FUNIL:
        arv = regua._arvore(rel)
        funcoes, importados = regua._funcoes(arv), _importados(arv)
        pilha = []
        for n in ast.walk(arv):
            if isinstance(n, ast.Call) and regua._nome_final(n.func) == "emitir_de":
                pilha.append(n.args[1])
            elif (isinstance(n, ast.Call) and ast.unparse(n.func) == "eventos.execucao"
                  and n.args):
                pilha.append(n.args[0])
        vistos = set()
        while pilha:
            no = pilha.pop()
            corpos += 1
            for c in ast.walk(no):
                if not isinstance(c, ast.Call):
                    continue
                txt = ast.unparse(c.func)
                if txt in regua._PURAS_PERMITIDAS or txt in _HELPERS_PERMITIDOS:
                    continue
                if isinstance(c.func, ast.Name):
                    if c.func.id in funcoes:
                        if c.func.id not in vistos:
                            vistos.add(c.func.id)
                            pilha.append(funcoes[c.func.id])
                    elif c.func.id in importados or c.func.id not in _BUILTINS_PERMITIDOS:
                        estranhos.append(f"{rel}:{c.lineno} {txt}")
                elif isinstance(c.func, ast.Attribute):
                    raiz = regua._raiz_de(c.func)
                    if raiz in importados or c.func.attr not in _METODOS_PERMITIDOS:
                        estranhos.append(f"{rel}:{c.lineno} {txt}")
                else:
                    estranhos.append(f"{rel}:{c.lineno} {txt}")
    r.check(corpos >= 17, "S05.coletores_encontrados", str(corpos))
    r.check(estranhos == [], "S05.lista_fechada_de_chamadas", str(estranhos))
    impuros = []
    for nome, (rel, fn) in _DEFS.items():
        arv = regua._arvore(rel)
        alvo = regua._funcoes(arv).get(fn)
        if alvo is None:
            impuros.append(f"{nome}: ausente")
            continue
        motivos = regua._impureza(alvo, regua._funcoes(arv), {fn})
        if motivos:
            impuros.append(f"{nome}: {motivos}")
    r.check(impuros == [], "S05.auxiliares_puros", str(impuros))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "EVENTOS · espinha de execução do funil de entrada · F1.2-A2-E"))
