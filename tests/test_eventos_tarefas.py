"""
F1.2-A2-O — LINHAGEM DAS TAREFAS DE FUNDO: a remoção física da FUSÃO e as
operações do canal de cupons.

O defeito (main a382cdb): a mensagem entra pela fila, cuja task é dona da
execução MENSAGEM (transferir_execucao); o execucao.fim sai quando ESSA task
termina. Dentro dela, a publicação cria tasks que ninguém aguarda e que vivem
mais que ela — a remoção do post perdedor de uma fusão
(convergencia.agendar_remocao, chamada por consolidar e pelo redirecionamento
_PostFundido) e cada operação do canal de cupons (espelho_cupons._tarefa:
publicar junto, vincular, editar junto, conferir, reconciliar, substituir,
abortar, e o removido que a própria remoção pede). create_task copia o
contexto: essas tasks levavam o exec da MENSAGEM e, depois do fim dela,
qualquer evento delas sairia com um exec já encerrado.

O contrato (sem mudar a coleta, o catálogo nem o envelope):
  · tarefa INDEPENDENTE — a que quem cria não aguarda e que pode viver mais
    que a execução corrente — nasce num contexto VAZIO: sem exec, sem
    exec_pai, sem adiamento. As duas fábricas: convergencia.agendar_remocao
    (_remover) e espelho_cupons._tarefa (_protegido). A causa (a mensagem
    que fundiu, o post) não vira exec_pai: quando instrumentada, a
    correlação é pelos ids do negócio;
  · a fila continua DONA da execução MENSAGEM: o que roda dentro da task da
    fila leva o exec dela (o controle positivo);
  · cada execução tem exatamente um execucao.fim, e nenhum evento com o exec
    (ou exec_pai) dela sai depois desse fim;
  · o adiamento é da task DONA: o que ela emite dentro de adiar() espera o
    fim do escopo; a independente nascida ali emite direto, sem exec;
  · comportamento operacional idêntico desligado/ligado/sabotado.

O contexto vazio descarta TODAS as ContextVar, não só o exec. Hoje o processo
só tem as duas da coleta (execução e adiamento) — correlação de uma execução
que já acabou, nada técnico de que as tasks precisem: o que elas usam chega
por argumento (o post, o motivo). Regra: ContextVar NOVA — no código (E03) ou
no contexto de quem cria a task independente, posta por biblioteca ou pelo
pipeline (L08) — quebra esta bancada e obriga a REAVALIAR o contexto vazio
das tarefas independentes (manter, ou passar a copiar um contexto de
referência do processo). Comparação por IDENTIDADE da ContextVar; mensagem
só com nomes, nunca valores nem o objeto do contexto.

Callbacks (add_done_callback): o contexto vazio da task NÃO vale para eles —
o callback roda numa cópia do contexto de quem o registrou (a MENSAGEM). Os
de hoje só limpam estrutura (E05); emissão futura num deles sai depois do
fim e cai em nada_depois_do_fim, que olha TODOS os eventos do anel.

Limites da prova: a bancada não roda a rede real do Telethon (sem sessão) —
para ela vale a prova estática (o Telethon não declara ContextVar, e o laço
de updates nasce no connect, fora de execução); E02/E03 olham o código do
repositório, não o de bibliotecas; L08 vê só o que o caminho exercitado
põe no contexto.

Pelo caminho REAL: processar (abre a MENSAGEM) → _enfileirar (a task da fila,
dona dela) → _pipeline (só as camadas puras trocadas: a mensagem normalizada
vem pronta) → publicacao.enviar REAL → família, decisão, aplicadores,
espelho_cupons, convergencia.consolidar / _PostFundido → agendar_remocao →
_remover → saida.apagar_post. SQLite real (o temporário do harness). Só o
Telegram é falso — e é nele a sonda: cada chamada lê o _EXEC do contexto REAL
da task que a fez, a corrotina dessa task e a marca da bancada, e emite um
evento de teste pela coleta. Com `segurar`, as tasks independentes esperam
um portão (asyncio.Event) que só abre depois do fim das execuções que as
criaram: a ordem é determinística, sem voltas de laço.

  Estrutura
    E01  as duas fábricas criam a task num contexto vazio (AST)
    E02  inventário FECHADO dos mecanismos de agendamento (tirados da própria
         stdlib: funções do asyncio, métodos do laço e do Future, threads e
         executores), por (arquivo, função, mecanismo), cada um com a
         linhagem dele; ponto novo quebra este teste
    E03  as únicas ContextVar do repositório são as duas da coleta (também
         por alias) e a lista de importadores de contextvars é fechada
    E04  fronteira: convergencia, espelho_cupons, saida e publicacao não
         importam eventos (o desacoplamento não passa pela coleta)
    E05  os callbacks de término só limpam estrutura (o da coleta é o fim da
         DONA, por desenho)
  Linhagem (ligado; tarefas independentes seguradas até depois do fim)
    L01  FUSAO: consolidar funde e remove; espelho publica, vincula, edita
         junto, confere e apaga junto (removido, pedido DENTRO da remoção)
    L02  REDIRECIONA: a remoção falha 3×; a edição tardia do perdedor
         redireciona (_PostFundido) e pede a remoção de novo, da publicação
    L03  ESPELHO_FALHA: a 1ª operação do canal de cupons falha; a rede
         refaz (vincular → criar)
    L04  ABORTA: o principal não sai; a cópia do canal de cupons é apagada
    L05  CONCORRENTE: duas fusões em lanes diferentes ao mesmo tempo
    L06  CANCELA: a task da fila (dona) é cancelada com uma operação
         independente já criada; a remoção segurada é cancelada — nada vaza
    L07  controle positivo: o que roda DENTRO da task da fila leva o exec
         dela, aberto, antes do fim
    L08  contexto, dinâmico, em todos os cenários e modos: o do CRIADOR no
         create_task, o ENTREGUE (task.get_context()) e o EM EXECUÇÃO (na
         sonda); controles positivos (o criador tem a marca e, ligado, a
         execução aberta); só tasks inventariadas no caminho real; a fábrica
         de tasks da bancada é neutra (com e sem ela, a mesma observação)
    L09  controle positivo da trava: uma ContextVar NÃO registrada, posta na
         task da fila, é acusada — e não sobrevive ao isolamento
    L10  ADIA: adiar() na task da fila em volta da publicação real — a dona
         captura, a independente emite direto e sem exec
  Regressão
    R01  diferencial desligado × ligado × sabotado, sem segurar
    R02  nada preso, nenhuma exceção não tratada, nenhum aviso do asyncio

    python tests/test_eventos_tarefas.py
"""
from __future__ import annotations

import ast
import asyncio
import contextvars
import inspect
import logging
import os
import sys
import time
import types as _pytypes

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _telethon_fakes import afetadas                        # noqa: E402

import telethon  # noqa: E402  — REAL, antes do harness
from telethon import utils  # noqa: E402

assert hasattr(telethon.TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

from _harness_e5 import preparar, rodar  # noqa: E402

preparar()

import test_eventos_entrada as a2e                               # noqa: E402 — bancada da A2-E
import client as modulo_client                                   # noqa: E402
import globals as g                                              # noqa: E402
import plataformas                                               # noqa: E402
import eventos                                                   # noqa: E402
from eventos import catalogo, coleta                             # noqa: E402
from config import GRUPO_DESTINO                                 # noqa: E402
from database import _init_db, db_espelho_get, db_get_post       # noqa: E402
from database_conexao import _db                                 # noqa: E402
from pipeline import (convergencia, espelho_cupons, exclusao,    # noqa: E402
                      orchestrator, origem, publicacao, publicacao_aplicadores)
from pipeline.enriquecimento import enriquecer, enriquecer_edicao  # noqa: E402
from pipeline.montagem import montar                             # noqa: E402
from pipeline.normalizacao import MensagemNormalizada            # noqa: E402
from pipeline.publicacao_estado import destino_vivo_de_origem    # noqa: E402
from utils.uma_por_vez import em_execucao                        # noqa: E402

plataformas.inicializar()
_init_db()
g._init_globals()
convergencia._ESPERA_BASE_S = 0.0           # só pesa nas tentativas de remoção

regua = a2e.regua
LOOP, _rodar = a2e.LOOP, a2e._rodar
_LIMITE_S = 10.0                            # teto de TEMPO de toda espera
CANAL_CUP = "CuponsLinhagemA2O"
_SONDA = "teste.sonda_tarefa"
_INDEPENDENTES = ("_remover", "_protegido")  # as corrotinas das duas fábricas
_FILA = "uma_por_vez"                        # a task da fila, dona da MENSAGEM

# Marca só da bancada, posta no contexto de quem dispara o cenário: a task que
# a carrega herdou ESSE contexto.
_MARCA = contextvars.ContextVar("bancada_a2o_marca", default=None)
# Variável NÃO registrada (L09): a trava tem de acusá-la.
_ESTRANHA = contextvars.ContextVar("bancada_a2o_estranha", default=None)
# As ÚNICAS ContextVar que o contexto vazio pode descartar — por IDENTIDADE:
# a execução e o adiamento da coleta, e a marca desta bancada.
_CONHECIDAS = frozenset({coleta._EXEC, coleta._ADIAMENTO, _MARCA})
_REAVALIAR = "reavaliar o contexto vazio das tarefas independentes (A2-O)"


def _nomes(vs):
    return sorted(v.name for v in vs)

# Exceção solta no laço e aviso do asyncio: R02 exige zero.
_NAO_TRATADAS, _AVISOS = [], []
LOOP.set_exception_handler(lambda loop, ctx: _NAO_TRATADAS.append(str(ctx.get("message"))))


class _AvisoAsyncio(logging.Handler):
    def emit(self, rec):
        _AVISOS.append(rec.getMessage()[:200])


logging.getLogger("asyncio").addHandler(_AvisoAsyncio())


# ─────────────────────────────────────────────────────────────────
# Fontes e mensagens (corpus da Frente 8: textos reais, códigos por cenário)
# ─────────────────────────────────────────────────────────────────
FADA, PROMOTOM, SAMUEL, FUMOTOM = 2050488946, 1825680721, 1768101197, 3775401737
NEUTRO = 9000000004                         # a 3ª fonte da corrida (Frente 8, CD)
T_FADA_1 = ("🚨 Cupons Shopee em Selecionados\n\n"
            "🎟 R$25 OFF em R$199: 1010SHO (Lojas Oficiais)\n\n"
            "✅ Resgate aqui:\nhttps://s.shopee.com.br/1qU7Zs67MB\n\n"
            "🛒 Carrinho: https://s.shopee.com.br/7plKiu5H62")
T_FADA_2 = ("🚨 Cupons Shopee em Selecionados\n\n"
            "🎟 R$25 OFF em R$199: 1010SHO (Lojas Oficiais)\n"
            "🎟 R$15 OFF em R$89: MODA15SHO (Moda)\n\n"
            "✅ Resgate aqui:\nhttps://s.shopee.com.br/1qU7Zs67MB\n\n"
            "🛒 Carrinho: https://s.shopee.com.br/7plKiu5H62")
T_FADA_3 = T_FADA_2 + "\n🎟 R$10 OFF em R$59: EXTRA10SHO (Extra)"
T_PROM_1 = ("Cupom Shopee\n\nR$ 15 OFF em R$ 89: MODA15SHO\n\n"
            "-Resgate aqui: \nhttps://s.shopee.com.br/70GSVaUJxC\n\n"
            "-Link carrinho:\nhttps://s.shopee.com.br/7fW9IpEnZG\n\n-Anúncio")
T_PROM_2 = ("Cupom Shopee\n\nR$ 15 OFF em R$ 89: MODA15SHO\n"
            "R$ 25 OFF em R$ 199: 1010SHO\n\n"
            "-Resgate aqui: \nhttps://s.shopee.com.br/70GSVaUJxC\n\n"
            "-Link carrinho:\nhttps://s.shopee.com.br/7fW9IpEnZG\n\n-Anúncio")


def _chat(canal):
    return str(utils.get_peer_id(a2e._entidade(canal)))


def _codigos(tag):
    return f"1010SHO{tag}", f"MODA15SHO{tag}", f"EXTRA10SHO{tag}"


def _t(texto, tag):
    return (texto.replace("1010SHO", f"1010SHO{tag}").replace("MODA15SHO", f"MODA15SHO{tag}")
            .replace("EXTRA10SHO", f"EXTRA10SHO{tag}"))


def _norm(canal, mid, texto, cupons):
    """Mensagem normalizada de cupom Shopee (sem produto): o que normalizar()
    devolveria — é ela que entra na publicação real."""
    return MensagemNormalizada(
        msg_id=mid, chat=_chat(canal), texto_limpo=texto, texto_analise=texto,
        mapa={"http://o": "http://a"}, preservar=[], plat="shopee", sku="",
        tem_midia=False, media_obj=None, ids_globais=[], idents=[],
        cupons=list(cupons), midia_key="")


def _fusao(tag, fonte_a, fonte_b, base):
    """O par da Frente 8: A (código X) e B (código Y) nascem separados; a
    EDIÇÃO de A passa a ter X e Y e funde B em A."""
    x, y, _z = _codigos(tag)
    a, b = base + 1, base + 2
    return [("NOVA", fonte_a, a, _t(T_FADA_1, tag), [x]),
            ("NOVA", fonte_b, b, _t(T_PROM_1, tag), [y]),
            ("EDICAO", fonte_a, a, _t(T_FADA_2, tag), [x, y])]


# ─────────────────────────────────────────────────────────────────
# Telegram falso dos dois canais — e a sonda da linhagem
# ─────────────────────────────────────────────────────────────────
class _Msg:
    def __init__(self, canal, i, texto, entities=None):
        self.canal, self.id, self.message, self.entities = canal, i, texto, entities
        self.media, self.photo = None, None


def _corrotina():
    t = asyncio.current_task()
    return getattr(getattr(t.get_coro(), "cr_code", None), "co_name", "?") if t else "?"


class Telegram:
    """Os dois canais (principal e de cupons). Em CADA chamada a sonda anota
    o que o contexto REAL da task que chamou tem — o _EXEC (id, tipo e se já
    estava encerrado), a corrotina da task, a marca e as ContextVar
    presentes (identidade) — e emite pela coleta, medindo se o evento foi
    direto ao anel (Δseq 1) ou ficou no buffer do adiamento (Δseq 0). Tasks
    independentes esperam o portão quando `segurar`. Só primitivos e
    ContextVar: nenhum objeto da execução nem do contexto fica guardado."""

    def __init__(self, segurar=False, falha_delete=0, falha_cupons=0, principal_falha=False,
                 prender_edicao=False):
        self.msgs, self.ops, self.sondas = {}, [], []
        self._seq = {GRUPO_DESTINO: 70000, CANAL_CUP: 0}
        self.segurar, self.portao = segurar, asyncio.Event()
        self.falha_delete, self.falha_cupons = falha_delete, falha_cupons
        self.principal_falha = principal_falha
        self.prender_edicao, self.presa = prender_edicao, asyncio.Event()
        self.janela, self.janelas = 0, 0          # escopo de adiar() aberto na dona (L10)

    def liberar(self):
        self.portao.set()

    async def _sonda(self, op, canal, ident):
        co, est = _corrotina(), coleta._EXEC.get()
        s = {"co": co, "op": op, "canal": canal, "id": ident, "n": len(self.sondas),
             "exec": None if est is None else est.id, "tipo": None if est is None else est.tipo,
             "encerrada": None if est is None else est.encerrada, "marca": _MARCA.get(),
             "ctx": frozenset(contextvars.copy_context().keys())}
        self.sondas.append(s)
        if co in _INDEPENDENTES and self.segurar:
            await self.portao.wait()
        if co == _FILA and op == "edit_message" and canal == GRUPO_DESTINO and self.prender_edicao:
            self.presa.set()
            await asyncio.Event().wait()          # só sai cancelada (L06)
        reg = eventos.registro_atual()
        antes = reg.saude()["seq_ultimo"] if reg is not None else None
        eventos.emitir_de(_SONDA, lambda: ({"post": ident} if ident else {},
                                           {"op": op, "papel": co, "n": s["n"]}),
                          local="teste.sonda.tarefa")
        s["dseq"] = None if reg is None else reg.saude()["seq_ultimo"] - antes
        s["janela"] = self.janela

    def _id(self, canal):
        self._seq[canal] += 1
        return self._seq[canal]

    def _falhar(self, canal, op):
        if canal == CANAL_CUP and self.falha_cupons:
            self.falha_cupons -= 1
            raise RuntimeError("canal de cupons fora do ar")
        if canal == GRUPO_DESTINO and self.principal_falha and op in ("send_message", "send_file"):
            raise RuntimeError("principal fora do ar")

    def _novo(self, canal, texto, entities=None):
        m = _Msg(canal, self._id(canal), texto, entities)
        self.msgs[(canal, m.id)] = m
        self.ops.append((canal, "novo", m.id))
        return m

    async def send_message(self, dest, texto, parse_mode="md", link_preview=None,
                           formatting_entities=None):
        await self._sonda("send_message", dest, None)
        self._falhar(dest, "send_message")
        await asyncio.sleep(0)
        return self._novo(dest, texto, formatting_entities)

    async def send_file(self, dest, img, caption=None, parse_mode="md", force_document=False,
                        formatting_entities=None):
        await self._sonda("send_file", dest, None)
        self._falhar(dest, "send_file")
        await asyncio.sleep(0)
        return self._novo(dest, caption or "", formatting_entities)

    async def edit_message(self, dest, msg_id, texto, parse_mode="md", file=None,
                           formatting_entities=None, link_preview=None):
        await self._sonda("edit_message", dest, msg_id)
        self._falhar(dest, "edit_message")
        await asyncio.sleep(0)
        m = self.msgs.get((dest, msg_id))
        if m is None:
            raise RuntimeError("MESSAGE_ID_INVALID")
        m.message = texto
        self.ops.append((dest, "edit", msg_id))
        return m

    async def delete_messages(self, dest, ids):
        lista = list(ids) if isinstance(ids, (list, tuple)) else [ids]
        await self._sonda("delete_messages", dest, lista[0] if lista else None)
        self._falhar(dest, "delete_messages")
        await asyncio.sleep(0)
        if dest == GRUPO_DESTINO and self.falha_delete:
            self.falha_delete -= 1
            self.ops.append((dest, "delete_falhou", lista[0]))
            raise RuntimeError("delete recusado")
        n = 0
        for i in lista:
            n += self.msgs.pop((dest, i), None) is not None
            self.ops.append((dest, "delete", i))
        return afetadas(lista, n)

    async def get_messages(self, dest, ids=None):
        await self._sonda("get_messages", dest, ids)
        await asyncio.sleep(0)
        return self.msgs.get((dest, ids))

    async def upload_file(self, file):
        raise AssertionError("os cenários não têm imagem")

    async def download_media(self, media, file=None):
        return None

    def vivos(self, canal):
        return tuple(sorted(i for (c, i) in self.msgs if c == canal))


class Nascimentos:
    """Fábrica de tasks do laço — NEUTRA: cria a task exatamente como o laço
    criaria (asyncio.Task com os MESMOS argumentos, `context` incluído; o
    nome o laço põe depois) e só anota, no INSTANTE do create_task:
      co ........ a corrotina da task nova;
      por ....... a corrotina da task que a criou;
      criador ... as ContextVar do contexto de quem criou (copy_context() ali,
                  no contexto do criador);
      entregue .. as ContextVar do contexto que a task REALMENTE recebeu
                  (task.get_context(), não o argumento);
      exec_criador / aberta_no_criador — o id do _EXEC do criador e se a
                  execução estava aberta;
      marca_criador — a marca da bancada no criador (o nome do cenário).
    Nunca o objeto da execução nem do contexto, nem valores do pipeline."""

    def __init__(self):
        self.lista = []

    def __call__(self, loop, coro, **kw):
        criador = contextvars.copy_context()
        t = asyncio.Task(coro, loop=loop, **kw)
        est = criador.get(coleta._EXEC)
        self.lista.append({"co": getattr(getattr(coro, "cr_code", None), "co_name", "?"),
                           "por": _corrotina(), "task": t,
                           "criador": frozenset(criador.keys()),
                           "entregue": frozenset(t.get_context().keys()),
                           "exec_criador": None if est is None else est.id,
                           "aberta_no_criador": None if est is None else not est.encerrada,
                           "marca_criador": criador.get(_MARCA)})
        return t


# ─────────────────────────────────────────────────────────────────
# As camadas puras do _pipeline: a mensagem normalizada vem pronta; da
# publicação para baixo tudo é REAL
# ─────────────────────────────────────────────────────────────────
class CamadasReais:
    def __init__(self, tg=None, nasc=None, adiar=False, estranha=False):
        self.norms, self.resultados, self.chamadas, self.corrida = {}, [], [], set()
        self.tg, self.nasc, self.adiar, self.estranha = tg, nasc, adiar, estranha

    async def ingerir(self, ev):
        return a2e._Bruta(str(ev.chat_id), [], ev.message.id)

    def apagada(self, chat, mid):
        return False

    async def checar_e_marcar(self, chave):
        return False

    def destino_vivo(self, chat, mid):
        return destino_vivo_de_origem(chat, mid)

    async def normalizar(self, bruta):
        return self.norms[(bruta.chat, bruta.mid)]

    def enriquecer(self, norm):
        return enriquecer(norm)

    def enriquecer_edicao(self, norm):
        return enriquecer_edicao(norm)

    async def deve_enviar(self, enr):
        return True

    async def montar(self, norm):
        return await montar(norm)

    async def enviar(self, montada, norm=None, enr=None, is_edit=False):
        """Na task REAL da fila. ESTRANHA (L09): põe uma ContextVar não
        registrada no contexto dela antes da publicação real. ADIA (L10): a
        publicação real dentro de eventos.adiar(), e — ainda no escopo — a
        dona espera as tasks independentes nascidas nela (com teto): elas
        emitem com o buffer da dona aberto."""
        if self.estranha:
            _ESTRANHA.set(norm.msg_id)
        if not self.adiar:
            return await self._enviar(montada, norm, enr, is_edit)
        n0 = len(self.nasc.lista)
        with eventos.adiar():
            self.tg.janelas += 1
            self.tg.janela = self.tg.janelas
            try:
                await self._enviar(montada, norm, enr, is_edit)
                novas = [n["task"] for n in self.nasc.lista[n0:] if n["co"] in _INDEPENDENTES]
                if novas:
                    await asyncio.wait(novas, timeout=_LIMITE_S)
            finally:
                self.tg.janela = 0

    async def _enviar(self, montada, norm, enr, is_edit):
        if (norm.chat, norm.msg_id) in self.corrida:
            with _db() as db:
                perdedor = db.execute("SELECT msg_id_dest FROM post_estado"
                                      " WHERE fused_into IS NOT NULL").fetchone()[0]
            self.resultados.append(await publicacao._enviar_resolvido(
                montada, norm, enr.ofertas, enr.score, True, perdedor))
            return
        self.resultados.append(await publicacao.enviar(montada, norm, enr=enr, is_edit=is_edit))


class _AsyncioSemEspera(_pytypes.ModuleType):
    """O asyncio dos aplicadores sem o backoff de envio (2**t s): só no
    cenário ABORTA, só neste processo."""

    def __init__(self):
        super().__init__("asyncio_sem_espera")
        self.__dict__.update(asyncio.__dict__)

        async def sleep(_s, *a, **k):
            await asyncio.sleep(0)
        self.sleep = sleep


# ─────────────────────────────────────────────────────────────────
# Cenários (o mesmo código roda desligado, ligado e sabotado)
# ─────────────────────────────────────────────────────────────────
_CENAS = ("FUSAO", "REDIRECIONA", "ESPELHO_FALHA", "ABORTA", "CONCORRENTE", "CANCELA", "ADIA")
_SEM_PORTAO = {"ADIA"}       # as independentes rodam DENTRO do escopo de adiar() da dona


def _roteiro(cen):
    """Fases [(passos, ação)]: os passos de uma fase entram JUNTOS; entre
    fases a fila esvazia (os fins saem) e só então o portão abre. Passo
    CORRIDA: a corrida da Frente 8 (test_fusao_familias 09) — o alvo já
    resolvido como o PERDEDOR antes do lock —, na task real da fila."""
    if cen == "FUSAO":
        return [([p], None) for p in _fusao("A2OF", FADA, PROMOTOM, 6100)]
    if cen == "REDIRECIONA":
        x, y, _z = _codigos("A2OR")
        fases = [([p], None) for p in _fusao("A2OR", FADA, PROMOTOM, 6200)]
        fases.append(([("CORRIDA", NEUTRO, 6204, _t(T_PROM_2, "A2OR"), [y, x])], None))
        return fases
    if cen == "ESPELHO_FALHA":
        x, _y, _z = _codigos("A2OE")
        return [([("NOVA", FADA, 6301, _t(T_FADA_1, "A2OE"), [x])], None)]
    if cen == "ABORTA":
        x, _y, _z = _codigos("A2OA")
        return [([("NOVA", FADA, 6401, _t(T_FADA_1, "A2OA"), [x])], None)]
    if cen == "CONCORRENTE":
        f1 = _fusao("A2OC1", FADA, PROMOTOM, 6500)
        f2 = _fusao("A2OC2", SAMUEL, FUMOTOM, 6600)
        return [([a, b], None) for a, b in zip(f1, f2)]
    if cen == "CANCELA":
        # 3ª fase: a remoção do perdedor, segurada, é cancelada antes do
        # portão (fica pendente). 4ª: A evolui de novo (3º código) e a dona
        # (a task da fila) é cancelada no meio da edição do principal — a
        # edição do canal de cupons, criada antes, segue independente.
        x, y, z = _codigos("A2OX")
        f = _fusao("A2OX", FADA, PROMOTOM, 6700)
        return [([f[0]], None), ([f[1]], None), ([f[2]], "CANCELA_REMOCAO"),
                ([("EDICAO", FADA, 6701, _t(T_FADA_3, "A2OX"), [x, y, z])], "CANCELA_DONA")]
    if cen == "ADIA":
        return [([p], None) for p in _fusao("A2OD", FADA, PROMOTOM, 6800)]
    if cen == "ESTRANHA":
        return [([p], None) for p in _fusao("A2OS", FADA, PROMOTOM, 6900)]
    raise KeyError(cen)


_AJUSTES = {"REDIRECIONA": {"falha_delete": 3}, "ESPELHO_FALHA": {"falha_cupons": 1},
            "ABORTA": {"principal_falha": True}}


class Cena:
    _FUNCIONAL = ("resultados", "ops", "principal", "cupons", "banco", "espelho", "nascidas",
                  "drenou", "exec_fora", "fila_cancelada")

    def __init__(self, **kw):
        self.resultados = self.ops = self.principal = self.cupons = self.banco = ()
        self.espelho = self.nascidas = ()
        self.drenou, self.exec_fora, self.fila_cancelada = True, None, None
        self.eventos, self.sondas, self.nasc, self.delta = [], [], [], {}
        self.__dict__.update(kw)

    def funcional(self):
        return tuple(getattr(self, k) for k in self._FUNCIONAL)


def _zerar():
    """Cada cenário começa sem posts, vínculos nem espelho: os códigos se
    repetem entre modos DE PROPÓSITO (o diferencial compara o mesmo
    cenário)."""
    with _db() as db:
        tabelas = [t for (t,) in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
        for t in tabelas:
            db.execute(f"DELETE FROM {t}")
    g._init_globals()
    for pool in (exclusao._IDENTITY_LOCKS, exclusao._IDENTITY_LOCKS_TS,
                 exclusao._POST_LOCKS, exclusao._POST_LOCKS_TS, origem._LOCKS, origem._LOCKS_TS):
        pool.clear()
    origem._LOCKS_LCK = asyncio.Lock()
    convergencia._EM_CURSO.clear()


async def _fila():
    """Até a fila esvaziar (com teto). False: algo ficou preso."""
    fim = time.monotonic() + _LIMITE_S
    while True:
        ts = [t for t in g._buf if isinstance(t, asyncio.Task) and not t.done()]
        if not ts:
            await asyncio.sleep(0)               # os callbacks de fim já rodaram
            return True
        restante = fim - time.monotonic()
        if restante <= 0:
            return False
        await asyncio.wait(ts, timeout=restante)


async def _drenar():
    """Fila, remoções e canal de cupons até o fim, com teto. False: algo
    ficou preso (e foi cancelado)."""
    fim = time.monotonic() + _LIMITE_S
    while True:
        ts = [t for t in (list(g._buf) + list(convergencia._EM_CURSO.values())
                          + list(espelho_cupons._tarefas))
              if isinstance(t, asyncio.Task) and not t.done()]
        if not ts:
            await asyncio.sleep(0)
            return True
        restante = fim - time.monotonic()
        if restante <= 0:
            for t in ts:
                t.cancel()
            await asyncio.wait(ts, timeout=_LIMITE_S)
            return False
        await asyncio.wait(ts, timeout=restante)


def _dona_na_fila():
    return next((t for t in g._buf if isinstance(t, asyncio.Task) and not t.done()), None)


async def _cenario(cen, segurar=False, fabrica=True):
    """Um cenário pelo caminho REAL: cada passo é um update do Telegram que
    entra por processar; entre fases, a fila esvazia e o portão abre.
    `fabrica`: instala a fábrica de tasks da bancada (neutra; restaurada no
    finally) — sem ela, nada de nascimentos, a sonda continua."""
    _zerar()
    tg = Telegram(segurar=segurar and cen not in _SEM_PORTAO, **_AJUSTES.get(cen, {}))
    log, nasc = a2e.Log(), Nascimentos()
    cam = CamadasReais(tg, nasc, adiar=cen == "ADIA", estranha=cen == "ESTRANHA")
    patches = [(modulo_client, "client", tg), (convergencia, "log_out", log)]
    if cen == "ABORTA":
        patches.append((publicacao_aplicadores, "asyncio", _AsyncioSemEspera()))
    salvos = [(m, n, getattr(m, n)) for m, n, _ in patches]
    reg, seq0 = a2e._seq_atual()
    antes = coleta.saude_coleta()
    env0 = os.environ.get("CANAL_CUPONS")
    cena = Cena(nome=cen)
    marca = _MARCA.set(cen)
    fabrica0 = LOOP.get_task_factory()
    try:
        for m, n, v in patches:
            setattr(m, n, v)
        os.environ["CANAL_CUPONS"] = CANAL_CUP
        espelho_cupons.iniciar()
        if fabrica:
            LOOP.set_task_factory(nasc)
        with a2e.Bancada(cam, log):
            for passos, acao in _roteiro(cen):
                tg.prender_edicao = acao == "CANCELA_DONA"
                for tipo, canal, mid, texto, cupons in passos:
                    cam.norms[(_chat(canal), mid)] = _norm(canal, mid, texto, cupons)
                    if tipo == "CORRIDA":
                        cam.corrida.add((_chat(canal), mid))
                    m = a2e.mensagem(mid, texto, canal=canal)
                    await orchestrator.processar(a2e.evento_telegram(m),
                                                 is_edit=(tipo != "NOVA"))
                if acao == "CANCELA_DONA":
                    await asyncio.wait_for(tg.presa.wait(), _LIMITE_S)
                    dona = _dona_na_fila()
                    dona.cancel()
                    await asyncio.wait({dona}, timeout=_LIMITE_S)
                    cena.fila_cancelada = dona.cancelled()
                cena.drenou = await _fila() and cena.drenou
                if acao == "CANCELA_REMOCAO":
                    for t in list(convergencia._EM_CURSO.values()):
                        t.cancel()
                tg.liberar()                     # os fins desta fase já saíram
                cena.drenou = await _drenar() and cena.drenou
                tg.portao = asyncio.Event()      # a próxima fase segura de novo
            tg.liberar()
            cena.drenou = await _drenar() and cena.drenou
            cena.exec_fora = coleta.exec_atual()
    finally:
        tg.liberar()                             # falhou no meio: nada fica segurado
        cena.drenou = await _drenar() and cena.drenou
        LOOP.set_task_factory(fabrica0)
        await espelho_cupons.encerrar()
        if env0 is None:
            os.environ.pop("CANAL_CUPONS", None)
        else:
            os.environ["CANAL_CUPONS"] = env0
        for m, n, v in salvos:
            setattr(m, n, v)
        _MARCA.reset(marca)
    cena.resultados = tuple(cam.resultados)
    cena.ops = tuple(tg.ops)
    cena.principal, cena.cupons = tg.vivos(GRUPO_DESTINO), tg.vivos(CANAL_CUP)
    principais = sorted({i for c, _op, i in tg.ops if c == GRUPO_DESTINO and i is not None})
    with _db() as db:
        cena.banco = tuple((r[0], r[1], r[2]) for r in db.execute(
            "SELECT msg_id_dest, delete_status, fused_into FROM post_estado ORDER BY msg_id_dest"))
    cena.espelho = tuple((p, db_espelho_get(p)) for p in principais)
    cena.nasc = [{k: v for k, v in n.items() if k != "task"} for n in nasc.lista]
    cena.nascidas = tuple(sorted(n["co"] for n in nasc.lista))
    cena.fabrica0 = fabrica0
    nasc.lista.clear()                           # nenhuma referência às tasks fica
    cena.sondas = list(tg.sondas)
    cena.eventos = a2e._eventos_desde(reg, seq0)
    depois = coleta.saude_coleta()
    cena.delta = {k: depois[k] - antes.get(k, 0) for k in depois}
    cena.vivas = [t for t in asyncio.all_tasks(LOOP) if not t.done() and t is not asyncio.current_task()]
    return cena


class SabotagemTarefas(a2e.Sabotagem):
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


def _matriz(modo, segurar=False):
    a2e._desligar() if modo == "desligado" else a2e._ligar()
    res = {}
    for c in _CENAS:
        if modo == "sabotado":
            with SabotagemTarefas():
                res[c] = _rodar(_cenario(c, segurar))
        else:
            res[c] = _rodar(_cenario(c, segurar))
    return res


_CACHE = {}


def _segurada():
    """Ligado, tarefas independentes seguradas até depois do fim (a ordem que
    expõe a linhagem)."""
    if "segurada" not in _CACHE:
        _CACHE["segurada"] = _matriz("ligado", segurar=True)
    return _CACHE["segurada"]


def _diferencial():
    """Os cenários desligado, ligado e sabotado, sem segurar (R01 e L08)."""
    if "diferencial" not in _CACHE:
        _CACHE["diferencial"] = {m: _matriz(m) for m in ("desligado", "ligado", "sabotado")}
    return _CACHE["diferencial"]


# ─────────────────────────────────────────────────────────────────
# Verificadores
# ─────────────────────────────────────────────────────────────────
def _linhagem(evs):
    """(fins por exec, execs sem exatamente um fim, eventos depois do fim de
    quem citam): todo exec citado (exec ou exec_pai) tem exatamente um
    execucao.fim, e nenhum evento dele sai DEPOIS desse fim."""
    fins = {}
    for e in evs:
        if e["tipo"] == "execucao.fim":
            fins.setdefault(e["corr"]["exec"], []).append(e["seq"])
    citados = {e["corr"][k] for e in evs for k in ("exec", "exec_pai") if k in e["corr"]}
    sem_um_fim = sorted(x for x in citados if len(fins.get(x, ())) != 1)
    depois = [(e["seq"], e["tipo"], k, e["corr"][k]) for e in evs for k in ("exec", "exec_pai")
              if k in e["corr"] and fins.get(e["corr"][k]) and e["seq"] > fins[e["corr"][k]][0]]
    return fins, sem_um_fim, depois


def _sondas_ev(c, papel):
    return [e for e in c.eventos if e["tipo"] == _SONDA and e["dados"].get("papel") in papel]


def _retrato(s):
    """A sonda em primitivos (para mensagem): nada do contexto além dos nomes."""
    return (s["co"], s["op"], s["canal"], s["id"], s["exec"], s["tipo"], s["encerrada"],
            s["marca"], _nomes(s["ctx"]))


def _independentes(c):
    return [n for n in c.nasc if n["co"] in _INDEPENDENTES]


def _estranhas(c):
    """A TRAVA: nomes das ContextVar no contexto de quem criou uma task
    independente que NÃO são as conhecidas (por identidade). Não é allowlist
    que esconde: qualquer outra variável aparece aqui e quebra o teste."""
    return sorted({v.name for n in _independentes(c) for v in n["criador"] - _CONHECIDAS})


def _criador_esperado(modo, cen):
    """O contexto EXATO de quem cria a task independente na task da fila:
    a marca; ligado, a execução; com adiar() aberto (ligado ou sabotado,
    em que o anel existe), o adiamento."""
    e = {_MARCA}
    if modo == "ligado":
        e.add(coleta._EXEC)
    if cen == "ADIA" and modo != "desligado":
        e.add(coleta._ADIAMENTO)
    return frozenset(e)


# ─────────────────────────────────────────────────────────────────
# Estrutura
# ─────────────────────────────────────────────────────────────────
def _funcao(rel, nome):
    return next(n for n in ast.walk(regua._arvore(rel))
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == nome)


def _create_tasks(no):
    return [n for n in ast.walk(no) if isinstance(n, ast.Call)
            and regua._nome_final(n.func) == "create_task"]


def test_e01_fabricas_criam_em_contexto_vazio(r):
    for rel, fn, co in (("pipeline/convergencia.py", "agendar_remocao", "_remover"),
                        ("pipeline/espelho_cupons.py", "_tarefa", "_protegido"),
                        ("pipeline/post_apagado.py", "agendar", "apagados"),
                        ("pipeline/post_apagado.py", "iniciar_conferencia", "_laco"),
                        ("pipeline/post_apagado.py", "suspeito", "_verificar")):
        cts = _create_tasks(_funcao(rel, fn))
        ok = (len(cts) == 1 and len(cts[0].args) == 1
              and regua._nome_final(cts[0].args[0].func) == co
              and [(k.arg, ast.unparse(k.value)) for k in cts[0].keywords]
              == [("context", "contextvars.Context()")])
        r.check(ok, f"E01.{fn}.contexto_vazio", str([ast.unparse(c) for c in cts]))


# Os mecanismos de agendamento, TIRADOS da própria stdlib (não de uma lista
# escrita à mão): as funções públicas do asyncio cujo 1º parâmetro é o que
# será agendado (corrotina, awaitable, função), os métodos do laço que
# agendam (call_*, add_*, create_task, run_in_executor, set_task_factory) e os
# add_* do Future (add_done_callback). Mais o que cria execução sem aparecer
# assim: Task e TaskGroup (asyncio), Thread e Timer (threading), submit
# (executores). Função do asyncio só conta chamada como asyncio.X ou importada
# de lá (Event.wait não é asyncio.wait); método do laço/Future conta em
# qualquer objeto.
_NAO_AGENDAM = {"iscoroutinefunction": "introspecção", "call_exception_handler": "só reporta"}


def _mecanismos():
    fun = {"Task", "TaskGroup"}
    for n in asyncio.__all__:
        f = getattr(asyncio, n, None)
        if inspect.isfunction(f):
            ps = list(inspect.signature(f).parameters)
            if ps and ps[0] in ("coro", "coro_or_future", "coros_or_futures", "aws", "fs",
                                "arg", "fut", "func", "main"):
                fun.add(n)
    met = {n for n in dir(asyncio.AbstractEventLoop)
           if n.startswith(("call_", "add_", "create_task", "run_in_executor", "set_task_factory"))}
    met |= {n for n in dir(asyncio.Future) if n.startswith("add_")} | {"submit"}
    return fun - set(_NAO_AGENDAM), met - set(_NAO_AGENDAM), {"Thread", "Timer"}


# O inventário FECHADO: todo (arquivo, função mais interna, mecanismo) de
# produção que agenda trabalho, e a linhagem dele. Ponto novo, ou que some,
# quebra o teste: a linhagem tem de ser decidida (dona, fora de execução,
# independente, aguardada ou callback).
_INVENTARIO = {
    ("main.py", "<módulo>", "run"): "boot: asyncio.run(main()), fora de execução",
    ("main.py", "_on_sinal", "create_task"): "shutdown: _encerrar, handler de sinal, fora de execução",
    ("main.py", "_preparar_processo", "create_task"): "boot: health, web e api_privada, fora de execução",
    ("main.py", "_preparar_processo", "add_signal_handler"): "boot: callback de sinal, fora de execução",
    ("main.py", "_encerrar", "wait"): "shutdown: drena tasks já existentes (aguardado)",
    ("main.py", "_encerrar", "gather"): "shutdown: tasks de fundo já existentes (aguardado)",
    ("heartbeat_sessao.py", "iniciar", "create_task"): "boot: laço do heartbeat, fora de execução",
    ("heartbeat_sessao.py", "_laco", "wait_for"): "AGUARDADA (timeout > 0: sem task nova no 3.13)",
    ("eventos/api_privada.py", "_esperar_novidade", "wait_for"): "AGUARDADA (Event.wait com prazo)",
    ("eventos/coleta.py", "transferir_execucao", "add_done_callback"): "CALLBACK: fim da DONA (por desenho)",
    ("ack_imediato.py", "_ao_update", "call_later"): "CALLBACK: contexto do laço de updates do Telethon (raiz), não emite",
    ("pipeline/orchestrator_fila.py", "_enfileirar", "create_task"): "DONA: transferir_execucao (a fila)",
    ("pipeline/orchestrator_fila.py", "_enfileirar", "add_done_callback"): "CALLBACK: g._buf.discard",
    ("pipeline/normalizacao_links.py", "resolver_e_afiliar", "gather"): "AGUARDADA dentro da execução",
    ("pipeline/montagem_imagem.py", "preparar_imagem_tg", "wait_for"): "AGUARDADA dentro da execução",
    ("pipeline/completude.py", "_agendar", "create_task"): "fora de execução: observar vem antes de processar",
    ("pipeline/sucessao.py", "agendar", "create_task"): "fora de execução: depois do with EXCLUSAO",
    ("pipeline/sucessao.py", "agendar", "add_done_callback"): "CALLBACK: _EM_CURSO.pop",
    ("pipeline/origem_apagada.py", "agendar", "create_task"): "fora de execução: handler de exclusão",
    ("pipeline/origem_apagada.py", "agendar", "add_done_callback"): "CALLBACK: _TAREFAS.discard",
    ("pipeline/convergencia.py", "agendar_remocao", "create_task"): "INDEPENDENTE: contexto vazio",
    ("pipeline/convergencia.py", "agendar_remocao", "add_done_callback"): "CALLBACK: _EM_CURSO.pop",
    ("pipeline/espelho_cupons.py", "_tarefa", "create_task"): "INDEPENDENTE: contexto vazio",
    ("pipeline/espelho_cupons.py", "_tarefa", "add_done_callback"): "CALLBACK: _tarefas.discard",
    ("pipeline/espelho_cupons.py", "_encadear", "add_done_callback"): "CALLBACK: _cadeia.pop",
    ("pipeline/espelho_cupons.py", "_aguardar", "wait"): "AGUARDADA: a operação anterior do post",
    ("pipeline/espelho_cupons.py", "encerrar", "wait"): "shutdown: o que já está em curso (aguardado)",
    ("pipeline/espelho_cupons.py", "encerrar", "gather"): "shutdown: o que já está em curso (aguardado)",
    # [Frente M] post apagado à mão pelo dono: handler do destino e conferência
    ("pipeline/post_apagado.py", "agendar", "create_task"): "INDEPENDENTE: contexto vazio (handler do destino)",
    ("pipeline/post_apagado.py", "agendar", "add_done_callback"): "CALLBACK: _TAREFAS.discard",
    ("pipeline/post_apagado.py", "iniciar_conferencia", "create_task"): "INDEPENDENTE: contexto vazio (conferência)",
    ("pipeline/post_apagado.py", "suspeito", "create_task"): "INDEPENDENTE: contexto vazio (verificação por suspeita do I/O)",
    ("pipeline/post_apagado.py", "suspeito", "add_done_callback"): "CALLBACK: _TAREFAS.discard",
}


def _mecanismo(func, fun, met, thr, de_asyncio, de_threading):
    if isinstance(func, ast.Attribute):
        base = func.value.id if isinstance(func.value, ast.Name) else None
        if base == "asyncio" and func.attr in fun:
            return func.attr
        if base == "threading" and func.attr in thr:
            return func.attr
        if func.attr in met:
            return func.attr
    elif isinstance(func, ast.Name):
        if func.id in de_asyncio and func.id in fun:
            return func.id
        if func.id in de_threading and func.id in thr:
            return func.id
    return None


def _pontos_de_agendamento(com_no=False):
    """{(arquivo, função mais interna, mecanismo)} — com_no: também os nós."""
    fun, met, thr = _mecanismos()
    pontos = {}
    for rel in regua._arquivos():
        if rel.split(os.sep)[0] == "testes_ml":
            continue                    # sondas manuais, fora do processo do worker
        arv = regua._arvore(rel)
        de_asyncio, de_threading = set(), set()
        for n in ast.walk(arv):
            if isinstance(n, ast.ImportFrom) and n.module in ("asyncio", "threading"):
                (de_asyncio if n.module == "asyncio" else de_threading).update(
                    a.asname or a.name for a in n.names)

        def visita(no, dono):
            for filho in ast.iter_child_nodes(no):
                if isinstance(filho, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    visita(filho, filho.name)
                    continue
                if isinstance(filho, ast.Call):
                    m = _mecanismo(filho.func, fun, met, thr, de_asyncio, de_threading)
                    if m:
                        pontos.setdefault((rel, dono, m), []).append(filho)
                visita(filho, dono)
        visita(arv, "<módulo>")
    return pontos if com_no else set(pontos)


def test_e02_inventario_fechado_dos_mecanismos_de_agendamento(r):
    fun, met, thr = _mecanismos()
    essenciais = {"create_task", "ensure_future", "gather", "shield", "wait", "wait_for",
                  "as_completed", "to_thread", "run_coroutine_threadsafe", "Task", "TaskGroup"}
    r.check(essenciais <= fun and {"call_soon", "call_later", "call_at", "call_soon_threadsafe",
                                   "run_in_executor", "add_done_callback", "create_task",
                                   "set_task_factory", "add_signal_handler"} <= met,
            "E02.mecanismos_tirados_da_stdlib", f"funcoes={sorted(fun)} metodos={sorted(met)}")
    pontos = _pontos_de_agendamento()
    r.check(pontos == set(_INVENTARIO), "E02.inventario_exato",
            f"novos={sorted(pontos - set(_INVENTARIO))} sumiram={sorted(set(_INVENTARIO) - pontos)}")


def _referencias_contextvar(arv):
    """Nós que citam ContextVar: atributo .ContextVar, o nome ContextVar ou
    um alias importado de contextvars (from contextvars import ContextVar as X)."""
    aliases = {"ContextVar"}
    for n in ast.walk(arv):
        if isinstance(n, ast.ImportFrom) and n.module == "contextvars":
            aliases |= {a.asname or a.name for a in n.names if a.name == "ContextVar"}
    return [n for n in ast.walk(arv)
            if (isinstance(n, ast.Attribute) and n.attr == "ContextVar")
            or (isinstance(n, ast.Name) and n.id in aliases)]


def test_e03_as_unicas_contextvar_sao_as_da_coleta(r):
    criadas, citacoes, importadores = [], set(), set()
    for rel in regua._arquivos():
        arv = regua._arvore(rel)
        if any((isinstance(n, ast.Import) and any(a.name.split(".")[0] == "contextvars"
                                                  for a in n.names))
               or (isinstance(n, ast.ImportFrom) and (n.module or "").split(".")[0] == "contextvars")
               for n in ast.walk(arv)):
            importadores.add(rel)
        refs = _referencias_contextvar(arv)
        if refs:
            citacoes.add(rel)
        ids = {id(x) for x in refs}
        for n in ast.walk(arv):
            if isinstance(n, ast.Call) and id(n.func) in ids:
                criadas.append((rel, ast.unparse(n.args[0]) if n.args else "?"))
    r.check(sorted(criadas) == [("eventos/coleta.py", "'eventos_adiamento'"),
                                ("eventos/coleta.py", "'eventos_execucao'")],
            "E03.so_execucao_e_adiamento", f"ContextVar nova: {_REAVALIAR} — {sorted(criadas)}")
    r.check(citacoes == {"eventos/coleta.py"}, "E03.contextvar_so_citada_na_coleta",
            f"ContextVar citada fora da coleta: {_REAVALIAR} — {sorted(citacoes)}")
    r.check(importadores == {"eventos/coleta.py", "main.py", "pipeline/convergencia.py",
                             "pipeline/espelho_cupons.py", "pipeline/post_apagado.py"},
            "E03.importadores_de_contextvars_lista_fechada",
            f"importador novo de contextvars: {_REAVALIAR} — {sorted(importadores)}")


def test_e04_fronteira_sem_eventos_nas_fabricas(r):
    importa = {}
    for rel in ("pipeline/convergencia.py", "pipeline/espelho_cupons.py", "pipeline/saida.py",
                "pipeline/publicacao.py", "pipeline/publicacao_aplicadores.py"):
        importa[rel] = sorted({a.name for n in ast.walk(regua._arvore(rel))
                               if isinstance(n, ast.Import) for a in n.names
                               if a.name.split(".")[0] == "eventos"}
                              | {n.module for n in ast.walk(regua._arvore(rel))
                                 if isinstance(n, ast.ImportFrom)
                                 and (n.module or "").split(".")[0] == "eventos"})
    r.check(all(v == [] for v in importa.values()), "E04.nenhuma_importa_eventos", str(importa))


def _so_limpa_estrutura(no):
    """O callback é método de limpeza de estrutura (discard/pop/remove de um
    dict/set do módulo) ou lambda que só chama pop/get/discard — nada de
    eventos, nada de I/O."""
    if isinstance(no, ast.Attribute):
        return no.attr in ("discard", "pop", "remove")
    if isinstance(no, ast.Lambda):
        chamadas = [c for c in ast.walk(no.body) if isinstance(c, ast.Call)]
        return bool(chamadas) and all(
            regua._nome_final(c.func) in ("pop", "get", "discard")
            and regua._raiz_de(c.func) not in ("eventos", "coleta") for c in chamadas)
    return False


def test_e05_callbacks_de_termino_so_limpam_estrutura(r):
    """add_done_callback roda o callback numa cópia do contexto de QUEM O
    REGISTROU — o contexto vazio da task não vale para ele. Os de hoje só
    limpam estrutura; o da coleta é o fim da DONA (por desenho)."""
    sitios = {k: v for k, v in _pontos_de_agendamento(com_no=True).items()
              if k[2] == "add_done_callback"}
    ruins = []
    for (rel, dono, _m), nos in sorted(sitios.items()):
        for c in nos:
            arg = c.args[0] if c.args else None
            if (rel, dono) == ("eventos/coleta.py", "transferir_execucao"):
                ok = (isinstance(arg, ast.Call) and regua._nome_final(arg.func) == "partial"
                      and arg.args and regua._nome_final(arg.args[0]) == "_fim_da_tarefa")
            else:
                ok = _so_limpa_estrutura(arg)
            if not ok:
                ruins.append((rel, dono, ast.unparse(arg)[:80] if arg is not None else "?"))
    r.check(len(sitios) == 9 and ruins == [], "E05.callbacks_so_limpam_estrutura",
            f"sitios={len(sitios)} ruins={ruins}")


# ─────────────────────────────────────────────────────────────────
# Linhagem
# ─────────────────────────────────────────────────────────────────
def _linhagem_da_cena(r, nome, c):
    fins, sem_um_fim, depois = _linhagem(c.eventos)
    r.check(fins and not sem_um_fim, f"{nome}.um_fim_por_execucao", str(sem_um_fim))
    r.check(depois == [], f"{nome}.nada_depois_do_fim_da_propria_execucao", str(depois[:4]))
    indep = [s for s in c.sondas if s["co"] in _INDEPENDENTES]
    r.check(all(s["exec"] is None and s["marca"] is None for s in indep)
            and all("exec" not in e["corr"] and "exec_pai" not in e["corr"]
                    for e in _sondas_ev(c, _INDEPENDENTES)),
            f"{nome}.independentes_sem_exec_no_contexto_real",
            str([_retrato(s) for s in indep if s["exec"] is not None or s["marca"] is not None][:4]))
    nasc = _independentes(c)
    r.check(all(not n["entregue"] for n in nasc),
            f"{nome}.independentes_nascem_em_contexto_vazio",
            str([(n["co"], _nomes(n["entregue"])) for n in nasc if n["entregue"]][:4]))
    fila = [s for s in c.sondas if s["co"] == _FILA]
    r.check(all(s["exec"] is not None and s["tipo"] == "MENSAGEM" and s["encerrada"] is False
                and s["marca"] == c.nome for s in fila),
            f"{nome}.controle_positivo_a_fila_leva_a_sua_execucao",
            str([_retrato(s) for s in fila][:4]))
    r.check(c.drenou and c.exec_fora is None and not c.vivas and not g._buf
            and not convergencia._EM_CURSO and not espelho_cupons._tarefas and em_execucao() == 0,
            f"{nome}.nada_preso", f"drenou={c.drenou} vivas={len(c.vivas)}")
    return fins


def _exercitado(r, nome, c, minimos):
    cont = {co: sum(1 for s in c.sondas if s["co"] == co) for co in (_FILA,) + _INDEPENDENTES}
    r.check(all(cont[co] >= n for co, n in minimos.items()), f"{nome}.exercitado", str(cont))
    fins = sorted(e["seq"] for e in c.eventos if e["tipo"] == "execucao.fim")
    ind = [e["seq"] for e in _sondas_ev(c, _INDEPENDENTES)]
    r.check(fins and any(s > fins[0] for s in ind), f"{nome}.independente_roda_depois_de_um_fim",
            f"fins={fins[:3]} indep={ind[:3]}")


def test_l01_fusao(r):
    c = _segurada()["FUSAO"]
    _linhagem_da_cena(r, "L01", c)
    _exercitado(r, "L01", c, {_FILA: 3, "_remover": 1, "_protegido": 4})
    dels = [i for canal, op, i in c.ops if op == "delete"]
    r.check(len(c.principal) == 1 and len(c.cupons) == 1 and len(dels) == 2,
            "L01.fusao_apagou_o_perdedor_nos_dois_canais", f"{c.principal} {c.cupons} {c.ops}")
    papeis = {(s["co"], s["canal"], s["op"]) for s in c.sondas}
    r.check(("_remover", GRUPO_DESTINO, "delete_messages") in papeis
            and ("_protegido", CANAL_CUP, "delete_messages") in papeis
            and ("_protegido", CANAL_CUP, "edit_message") in papeis,
            "L01.remocao_e_espelho_pelo_caminho_real", str(sorted(papeis)))


def test_l02_redireciona(r):
    c = _segurada()["REDIRECIONA"]
    _linhagem_da_cena(r, "L02", c)
    _exercitado(r, "L02", c, {_FILA: 3, "_remover": 4})   # a corrida não edita o principal
    falhas = [i for canal, op, i in c.ops if op == "delete_falhou"]
    nasc = [n for n in c.nasc if n["co"] == "_remover"]
    r.check(len(falhas) == 3 and len(nasc) == 2, "L02.remocao_falhou_e_redirecionamento_repediu",
            f"falhas={falhas} remover={len(nasc)}")


def test_l03_espelho_falha(r):
    c = _segurada()["ESPELHO_FALHA"]
    _linhagem_da_cena(r, "L03", c)
    _exercitado(r, "L03", c, {_FILA: 1, "_protegido": 2})
    r.check(len(c.cupons) == 1 and len(c.principal) == 1, "L03.a_rede_refez_o_canal_de_cupons",
            f"{c.principal} {c.cupons}")


def test_l04_aborta(r):
    c = _segurada()["ABORTA"]
    _linhagem_da_cena(r, "L04", c)
    _exercitado(r, "L04", c, {_FILA: 3, "_protegido": 2})
    r.check(c.principal == () and c.cupons == (), "L04.principal_nao_saiu_copia_apagada",
            f"{c.principal} {c.cupons} {c.ops}")


def test_l05_concorrente(r):
    c = _segurada()["CONCORRENTE"]
    _linhagem_da_cena(r, "L05", c)
    _exercitado(r, "L05", c, {_FILA: 6, "_remover": 2, "_protegido": 8})
    r.check(len(c.principal) == 2 and len(c.cupons) == 2
            and len([n for n in c.nasc if n["co"] == "_remover"]) == 2,
            "L05.duas_fusoes_duas_remocoes", f"{c.principal} {c.cupons}")


def test_l06_cancela(r):
    c = _segurada()["CANCELA"]
    _linhagem_da_cena(r, "L06", c)
    fins = [e for e in c.eventos if e["tipo"] == "execucao.fim"]
    r.check(c.fila_cancelada is True
            and [e["dados"]["resultado"] for e in fins].count("CANCELADA") == 1,
            "L06.a_dona_foi_cancelada_e_fechou_uma_vez", str([e["dados"]["resultado"] for e in fins]))
    junto = [s for s in c.sondas if s["co"] == "_protegido" and s["op"] == "edit_message"]
    r.check(junto and all(s["exec"] is None for s in junto),
            "L06.a_edicao_do_canal_de_cupons_seguiu_independente", str([_retrato(s) for s in junto]))


def test_l07_controle_positivo(r):
    on = _segurada()
    fila = [s for c in on.values() for s in c.sondas if s["co"] == _FILA]
    r.check(len(fila) >= 10, "L07.a_fila_foi_sondada", str(len(fila)))
    for nome, c in on.items():
        fins, _, _ = _linhagem(c.eventos)
        evs = _sondas_ev(c, (_FILA,))
        r.check(evs and all(e["corr"].get("exec") in fins and e["seq"] < fins[e["corr"]["exec"]][0]
                            for e in evs),
                f"L07.{nome}.evento_da_fila_no_exec_dela_antes_do_fim",
                str([(e["seq"], e["corr"].get("exec")) for e in evs][:4]))


def _observacoes(c):
    """O que a sonda viu DENTRO das corrotinas, sem os ids de execução (mudam
    entre rodadas): papel, operação, canal, alvo, se havia execução e se
    estava encerrada, a marca e os nomes do contexto em execução."""
    return [(s["co"], s["op"], s["canal"], s["id"], s["exec"] is not None, s["encerrada"],
             s["marca"], tuple(_nomes(s["ctx"]))) for s in c.sondas]


def _contexto_da_cena(r, rot, c, modo):
    ind = _independentes(c)
    sondas = [s for s in c.sondas if s["co"] in _INDEPENDENTES]
    r.check(ind and all(not n["entregue"] for n in ind), f"{rot}.entregue_vazio",
            str([(n["co"], _nomes(n["entregue"])) for n in ind if n["entregue"]][:4]))
    r.check(sondas and all(not s["ctx"] for s in sondas), f"{rot}.em_execucao_vazio",
            str([_retrato(s) for s in sondas if s["ctx"]][:4]))
    est = _estranhas(c)
    r.check(est == [], f"{rot}.descartadas_so_as_conhecidas",
            f"ContextVar nova no contexto de quem cria a task independente: {est} — {_REAVALIAR}")
    da_fila = [n for n in ind if n["por"] == _FILA]
    aberta = True if modo == "ligado" else None
    r.check(da_fila and all(n["criador"] == _criador_esperado(modo, c.nome)
                            and n["marca_criador"] == c.nome
                            and n["aberta_no_criador"] is aberta for n in da_fila),
            f"{rot}.controle_positivo_o_criador_tem_a_marca_e_a_execucao",
            str([(n["co"], _nomes(n["criador"]), n["marca_criador"], n["aberta_no_criador"])
                 for n in da_fila][:4]))
    r.check(set(c.nascidas) <= {_FILA, *_INDEPENDENTES}, f"{rot}.so_tasks_inventariadas_no_caminho_real",
            str(sorted(set(c.nascidas))))
    r.check(c.fabrica0 is None and LOOP.get_task_factory() is None, f"{rot}.fabrica_restaurada")


def test_l08_contexto_do_criador_entregue_e_em_execucao(r):
    for nome, c in _segurada().items():
        _contexto_da_cena(r, f"L08.segurada.{nome}", c, "ligado")
    for modo, res in _diferencial().items():
        for nome, c in res.items():
            _contexto_da_cena(r, f"L08.{modo}.{nome}", c, modo)
    # A fábrica da bancada não cria nem esconde nada: sem ela, a sonda — que
    # mora DENTRO das corrotinas reais — vê exatamente o mesmo.
    a2e._ligar()
    sem, com = _rodar(_cenario("FUSAO", segurar=True, fabrica=False)), _segurada()["FUSAO"]
    r.check(sem.nasc == [] and len(_observacoes(com)) > 0
            and _observacoes(sem) == _observacoes(com)
            and sem.funcional()[:6] == com.funcional()[:6],
            "L08.fabrica_neutra_a_mesma_observacao_sem_ela",
            str([(a, b) for a, b in zip(_observacoes(sem), _observacoes(com)) if a != b][:3]))


def test_l09_trava_acusa_contextvar_nao_registrada(r):
    a2e._ligar()
    c = _rodar(_cenario("ESTRANHA", segurar=True))
    r.check(_estranhas(c) == ["bancada_a2o_estranha"], "L09.trava_acusa_a_variavel_nao_registrada",
            str(_estranhas(c)))
    fila = [s for s in c.sondas if s["co"] == _FILA]
    r.check(fila and all(_ESTRANHA in s["ctx"] for s in fila),
            "L09.a_variavel_estava_no_contexto_real_da_fila", str([_retrato(s) for s in fila][:3]))
    ind = _independentes(c)
    sondas = [s for s in c.sondas if s["co"] in _INDEPENDENTES]
    r.check(ind and sondas and all(_ESTRANHA not in n["entregue"] for n in ind)
            and all(_ESTRANHA not in s["ctx"] for s in sondas),
            "L09.a_variavel_nao_sobrevive_ao_isolamento",
            str([(n["co"], _nomes(n["entregue"])) for n in ind if _ESTRANHA in n["entregue"]][:3]))
    r.check(c.drenou and not c.vivas, "L09.nada_preso", f"drenou={c.drenou} vivas={len(c.vivas)}")


def test_l10_adiamento_respeita_a_dona(r):
    c = _segurada()["ADIA"]
    _linhagem_da_cena(r, "L10", c)
    na_janela = [s for s in c.sondas if s.get("janela")]
    dona = [s for s in na_janela if s["co"] == _FILA]
    ind = [s for s in na_janela if s["co"] in _INDEPENDENTES]
    r.check(dona and ind and len({s["janela"] for s in ind}) >= 1, "L10.exercitado",
            f"dona={len(dona)} independentes={len(ind)}")
    r.check(all(s["dseq"] == 0 for s in dona), "L10.a_dona_captura_no_buffer",
            str([(s["n"], s["dseq"]) for s in dona if s["dseq"] != 0][:4]))
    r.check(all(s["dseq"] == 1 for s in ind), "L10.a_independente_emite_direto_fora_do_buffer",
            str([(s["n"], s["dseq"]) for s in ind if s["dseq"] != 1][:4]))
    por_n = {e["dados"]["n"]: e for e in c.eventos if e["tipo"] == _SONDA}
    r.check(all(s["n"] in por_n for s in na_janela), "L10.toda_emissao_chegou_ao_anel",
            str([s["n"] for s in na_janela if s["n"] not in por_n][:4]))
    r.check(all("exec" not in por_n[s["n"]]["corr"] and "exec_pai" not in por_n[s["n"]]["corr"]
                for s in ind if s["n"] in por_n),
            "L10.a_independente_sai_sem_exec",
            str([por_n[s["n"]]["corr"].get("exec") for s in ind if s["n"] in por_n][:4]))
    fins, _, _ = _linhagem(c.eventos)
    ordem = []
    for j in sorted({s["janela"] for s in na_janela}):
        d = [por_n[s["n"]]["seq"] for s in dona if s["janela"] == j and s["n"] in por_n]
        i = [por_n[s["n"]]["seq"] for s in ind if s["janela"] == j and s["n"] in por_n]
        ordem.append(d == sorted(d) and (not d or not i or min(d) > max(i)))
    r.check(ordem and all(ordem), "L10.despejo_em_ordem_depois_das_independentes", str(ordem))
    r.check(all(por_n[s["n"]]["corr"].get("exec") in fins
                and por_n[s["n"]]["seq"] < fins[por_n[s["n"]]["corr"]["exec"]][0]
                for s in dona if s["n"] in por_n),
            "L10.despejo_no_exec_da_dona_antes_do_fim")


# ─────────────────────────────────────────────────────────────────
# Regressão
# ─────────────────────────────────────────────────────────────────
def test_r01_desligado_ligado_sabotado(r):
    d = _diferencial()
    off, on, sab = d["desligado"], d["ligado"], d["sabotado"]
    for nome in _CENAS:
        r.check(off[nome].funcional() == on[nome].funcional() == sab[nome].funcional(),
                f"R01.{nome}.funcional_identico",
                str([(k, a, b) for k, a, b in zip(Cena._FUNCIONAL, off[nome].funcional(),
                                                   on[nome].funcional()) if a != b])[:300])
        r.check(off[nome].eventos == [] and sab[nome].eventos == []
                and not any(v for v in off[nome].delta.values())
                and sab[nome].delta.get("falhas_internas", 0) > 0,
                f"R01.{nome}.dormente_e_sabotagem_contada", str(sab[nome].delta))
        r.check(on[nome].drenou and off[nome].drenou and sab[nome].drenou,
                f"R01.{nome}.nada_preso")


def test_r02_nada_preso_nenhuma_excecao(r):
    _rodar(asyncio.sleep(0))
    vivas = [t for t in asyncio.all_tasks(LOOP) if not t.done()]
    r.check(vivas == [], "R02.nenhuma_task_viva",
            str([getattr(getattr(t.get_coro(), "cr_code", None), "co_name", "?") for t in vivas][:3]))
    r.check(_NAO_TRATADAS == [], "R02.nenhuma_excecao_nao_tratada", str(_NAO_TRATADAS[:3]))
    r.check(_AVISOS == [], "R02.nenhum_aviso_do_asyncio", str(_AVISOS[:3]))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "EVENTOS · linhagem das tarefas de fundo (fusão e canal de "
                              "cupons) · F1.2-A2-O"))
