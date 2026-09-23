"""S6.1 — testes da sonda de entrega, contra o Telethon REAL.

NÃO usa tests/_harness_e5.py: aquele harness instala um telethon FALSO
em sys.modules. Aqui o objetivo é provar a sonda contra as classes, os
loggers, os modelos de mensagem e o despacho verdadeiros do Telethon —
a sonda depende exatamente deles.

Execução (standalone, stdlib + Telethon instalado):
    python3 tests/test_sonda_entrega.py
Se o Telethon não estiver instalado, apontar PYTHONPATH para a fonte.
"""
from __future__ import annotations

import ast
import asyncio
import datetime as dt
import inspect
import io
import logging
import os
import pathlib
import sys
import traceback

RAIZ = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

import telethon  # noqa: E402
from telethon import TelegramClient, events, utils  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import functions, types  # noqa: E402

assert hasattr(TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

import diagnostico.sonda_entrega as sonda  # noqa: E402

CANAL_MON = 1825680721          # canal monitorado (id cru)
CANAL_OUTRO = 1111111111        # canal NÃO monitorado
MARCADO = utils.get_peer_id(types.PeerChannel(CANAL_MON))


# ── infraestrutura mínima ─────────────────────────────────────────
class _Captura(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas = []

    def emit(self, record):
        self.linhas.append(record.getMessage())


def _campos(linha: str) -> dict:
    partes = linha.split("|")
    d = {"_tipo": partes[1]}
    for p in partes[2:]:
        k, _, v = p.partition("=")
        d[k] = v
    return d


def _cliente() -> TelegramClient:
    return TelegramClient(StringSession(), 1, "0" * 32)


def _msg(canal, mid, date, texto="", edit_date=None):
    return types.Message(
        id=mid, peer_id=types.PeerChannel(canal), date=date,
        message=texto, edit_date=edit_date)


def _novo(canal, mid, date, texto=""):
    return types.UpdateNewChannelMessage(
        message=_msg(canal, mid, date, texto), pts=1, pts_count=1)


def _edit(canal, mid, date, edit_date, texto=""):
    return types.UpdateEditChannelMessage(
        message=_msg(canal, mid, date, texto, edit_date), pts=2, pts_count=1)


class Ctx:
    """Instala a sonda num cliente real, captura a saída e desfaz tudo."""

    def __enter__(self):
        self.cli = _cliente()
        self.cap = _Captura()
        logging.getLogger("SONDA").addHandler(self.cap)
        sonda.instalar(self.cli, [types.PeerChannel(CANAL_MON)])
        self.cap.linhas.clear()          # descarta a linha INICIO
        return self

    def linhas(self, tipo=None):
        out = [_campos(x) for x in self.cap.linhas if x.startswith("SONDA|")]
        return [c for c in out if tipo is None or c["_tipo"] == tipo]

    def __exit__(self, *exc):
        sonda.desinstalar(self.cli)
        logging.getLogger("SONDA").removeHandler(self.cap)
        return False


# ── testes ────────────────────────────────────────────────────────
def t01_gancho_main_so_com_variavel(r):
    """main.py só IMPORTA a sonda dentro de `if os.environ.get(
    "SONDA_ENTREGA") == "1"`, e antes de _registrar_handlers."""
    arv = ast.parse((RAIZ / "main.py").read_text(encoding="utf-8"))
    fn = next(n for n in ast.walk(arv)
              if isinstance(n, ast.AsyncFunctionDef)
              and n.name == "_preparar_processo")
    imports = [n for n in ast.walk(fn) if isinstance(n, ast.ImportFrom)
               and (n.module or "").startswith("diagnostico")]
    r.ok(len(imports) == 1, "exatamente 1 import da sonda em _preparar_processo")
    guardas = [n for n in ast.walk(fn) if isinstance(n, ast.If)
               and imports[0] in ast.walk(n)]
    teste = ast.unparse(guardas[0].test) if guardas else ""
    r.ok(teste == "os.environ.get('SONDA_ENTREGA') == '1'",
         f"import guardado pela variavel (achado: {teste!r})")
    linhas = [n.lineno for n in fn.body]
    idx_if = next(i for i, n in enumerate(fn.body) if n is guardas[0])
    idx_reg = next(i for i, n in enumerate(fn.body)
                   if "_registrar_handlers" in ast.unparse(n))
    r.ok(idx_if < idx_reg, "sonda registrada ANTES dos handlers da casa")
    topo = [n for n in arv.body if isinstance(n, (ast.Import, ast.ImportFrom))
            and "diagnostico" in ast.unparse(n)]
    r.ok(not topo, "nenhum import da sonda no topo de main.py")


def t02_nenhum_modulo_de_negocio_conhece_a_sonda(r):
    ofensores = []
    for p in RAIZ.rglob("*.py"):
        rel = p.relative_to(RAIZ).as_posix()
        if rel.startswith(("tests/", "testes_ml/", "diagnostico/")) or rel == "main.py":
            continue
        txt = p.read_text(encoding="utf-8", errors="ignore")
        if "diagnostico" in txt or "sonda_entrega" in txt:
            ofensores.append(rel)
    r.ok(not ofensores, f"pipeline/plataformas/banco intocados (ofensores={ofensores})")


def t03_desligada_nada_muda(r):
    lg = logging.getLogger("telethon")
    antes = (list(lg.handlers),
             {n: logging.getLogger(n).level for n in sonda._LOGGERS_TELETHON})
    r.ok(not sonda._instalada, "sem instalar() a sonda está inerte")
    r.ok(all(not isinstance(h, sonda._Grampo) for h in lg.handlers),
         "sem grampo no logger telethon")
    with Ctx():
        pass
    depois = (list(lg.handlers),
              {n: logging.getLogger(n).level for n in sonda._LOGGERS_TELETHON})
    r.ok(antes == depois, "desinstalar() restaura handlers e níveis exatamente")


def t04_upd_novo_idade_e_privacidade(r):
    with Ctx() as c:
        agora = sonda._agora_ms()
        date = dt.datetime.fromtimestamp((agora - 37_000) / 1000, dt.timezone.utc)
        texto = "SEGREDO https://amzn.to/abc?tag=leo-20 CUPOM123"
        sonda._registrar_update(_novo(CANAL_MON, 555, date, texto))
        u = c.linhas("UPD")
        r.ok(len(u) == 1, "1 linha UPD para canal monitorado")
        u = u[0]
        r.ok(u["chat"] == str(MARCADO) and u["id"] == "555" and u["tipo"] == "new",
             "chat marcado, id e tipo corretos")
        idade = int(u["idade_ms"])
        r.ok(36_000 <= idade <= 39_000, f"idade_ms ~37s (obtido {idade})")
        bruto = "\n".join(c.cap.linhas)
        r.ok(all(x not in bruto for x in ("SEGREDO", "amzn", "http", "CUPOM123", "tag=")),
             "nenhum texto/URL/cupom da mensagem vaza")


def t05_canal_nao_monitorado_ignorado(r):
    with Ctx() as c:
        d = dt.datetime.now(dt.timezone.utc)
        sonda._registrar_update(_novo(CANAL_OUTRO, 1, d, "x"))
        r.ok(not c.linhas("UPD"), "canal fora da lista não gera linha")


def t06_edicao_usa_edit_date(r):
    with Ctx() as c:
        agora = sonda._agora_ms()
        criada = dt.datetime.fromtimestamp((agora - 600_000) / 1000, dt.timezone.utc)
        editada = dt.datetime.fromtimestamp((agora - 4_000) / 1000, dt.timezone.utc)
        sonda._registrar_update(_edit(CANAL_MON, 9, criada, editada))
        u = c.linhas("UPD")[0]
        r.ok(u["tipo"] == "edit", "tipo=edit")
        r.ok(3_000 <= int(u["idade_ms"]) <= 6_000,
             f"idade da EDIÇÃO vem de edit_date (obtido {u['idade_ms']})")


def t07_lotes(r):
    with Ctx() as c:
        relogio = [1_000_000]
        original = sonda._agora_ms
        sonda._agora_ms = lambda: relogio[0]
        try:
            d = dt.datetime.fromtimestamp(999, dt.timezone.utc)
            # deltas ENTRE updates consecutivos (ms)
            for dt_ms in (0, 40, 90, 1_500, 100, 5_000):
                relogio[0] += dt_ms
                sonda._registrar_update(_novo(CANAL_MON, relogio[0], d))
        finally:
            sonda._agora_ms = original
        lotes = [int(u["lote"]) for u in c.linhas("UPD")]
        r.ok(lotes[0] == lotes[1] == lotes[2], "gaps <=250ms: mesmo lote")
        r.ok(lotes[3] == lotes[4] == lotes[2] + 1, "gap de 1.5s: lote novo")
        r.ok(lotes[5] == lotes[4] + 1, "gap de 5s: outro lote")


def t08_grampo_com_codigo_real_do_telethon(r):
    """Os registros são produzidos pelas FUNÇÕES/MODELOS reais do
    Telethon: _fmt_flood real e o modelo real do messagepacker."""
    from telethon.client.users import _fmt_flood
    with Ctx() as c:
        pk = logging.getLogger("telethon.extensions.messagepacker")
        pk.debug("Assigned msg_id = %d to %s (%x)", 1, "PingRequest", 0xAB)
        pk.debug("Assigned msg_id = %d to %s (%x)", 2, "SendMessageRequest", 0xCD)
        d = dt.datetime.now(dt.timezone.utc)
        sonda._registrar_update(_novo(CANAL_MON, 7, d))
        ms = logging.getLogger("telethon.network.mtprotosender")
        ms.debug("Handling pong for message %d", 1)
        ms.debug("Handling update %s", "Updates")
        ms.info("Closing current connection to begin reconnect...")
        req = functions.messages.SendMessageRequest(
            peer=types.InputPeerSelf(), message="NAO VAZAR", random_id=1)
        logging.getLogger("telethon.client.users").info(*_fmt_flood(27, req))
        up = logging.getLogger("telethon.client.updates")
        up.debug("Getting difference for channel %s updates", 123)
        tipos = [x["_tipo"] for x in c.linhas()]
        r.ok(tipos == ["PING", "REQ", "UPD", "PONG", "NET", "CONN", "FLOOD", "DIFF"],
             f"sequência de eventos traduzida (obtido {tipos})")
        u = c.linhas("UPD")[0]
        r.ok(u["req_ant"] == "SendMessageRequest" and int(u["desde_req_ms"]) >= 0,
             "UPD carrega a última requisição e o tempo desde ela")
        r.ok(int(u["desde_ping_ms"]) >= 0, "UPD carrega tempo desde o último ping")
        f = c.linhas("FLOOD")[0]
        r.ok(f["seg"] == "27" and f["req"] == "SendMessageRequest",
             "FloodWait silencioso ≤60s fica visível (segundos + classe)")
        r.ok("NAO VAZAR" not in "\n".join(c.cap.linhas),
             "argumentos da requisição nunca vazam")


def t09_modelos_existem_no_telethon_instalado(r):
    """A lista branca só funciona se os modelos forem IDÊNTICOS aos do
    Telethon instalado. Produção instala a versão mais recente: este
    teste acusa qualquer divergência futura."""
    base = pathlib.Path(inspect.getfile(telethon)).parent
    fonte = "\n".join(p.read_text(encoding="utf-8", errors="ignore")
                      for p in base.rglob("*.py"))
    modelos = ([sonda._MODELO_REQ, sonda._MODELO_NET, sonda._MODELO_PONG,
                "Sleeping%s for %ds (%s) on %s flood wait"]
               + list(sonda._CONEXAO) + list(sonda._DIFF))
    faltando = [m for m in modelos if m not in fonte]
    r.ok(not faltando, f"todos os {len(modelos)} modelos existem no Telethon {telethon.__version__} (faltando={faltando})")


def t10_warning_preservado_e_info_sensivel_silenciado(r):
    """Sem a sonda, WARNING+ do Telethon sai pelo lastResort e INFO não
    sai. Com a sonda, exatamente igual — e nada sensível vira SONDA."""
    def rodar(com_sonda):
        buf = io.StringIO()
        antigo = sys.stderr
        sys.stderr = buf
        try:
            if com_sonda:
                ctx = Ctx(); ctx.__enter__()
            ms = logging.getLogger("telethon.network.mtprotosender")
            ms.warning("Invalid buffer %s", "b'dados-crus-https://x'")
            ms.info("Received response without parent request: %s",
                    "corpo com https://segredo.example TOKEN=1")
            if com_sonda:
                sondas = [x for x in ctx.cap.linhas if x.startswith("SONDA|")]
                ctx.__exit__(None, None, None)
            else:
                sondas = []
        finally:
            sys.stderr = antigo
        return buf.getvalue(), sondas
    sem, _ = rodar(False)
    com, sondas = rodar(True)
    r.ok(sem == com, "saída de stderr IDÊNTICA com e sem sonda")
    r.ok("segredo" not in com, "INFO com corpo de resposta continua não impresso")
    r.ok(not sondas, "registros sensíveis nunca viram linha SONDA")


def t11_robustez_e_falha_nunca_calada(r):
    class _Venenosa:
        peer_id = types.PeerChannel(CANAL_MON)
        id = 1

        @property
        def date(self):
            raise RuntimeError("detalhe com https://segredo.example")

    with Ctx() as c:
        for lixo in (None, object(), types.UpdateNewChannelMessage(
                message=types.MessageEmpty(id=1, peer_id=None), pts=1, pts_count=1)):
            sonda._registrar_update(lixo)
        r.ok(not c.linhas("ERRO") and not c.linhas("UPD"),
             "updates fora do escopo são ignorados sem erro")
        logging.getLogger("telethon.extensions.messagepacker").debug(
            "Assigned msg_id = %d to %s (%x)")          # args ausentes
        r.ok(not c.linhas("ERRO"), "registro malformado não lança nem acusa")
        for _ in range(7):
            sonda._registrar_update(types.UpdateNewChannelMessage(
                message=_Venenosa(), pts=1, pts_count=1))
        erros = c.linhas("ERRO")
        r.ok(len(erros) == 5, f"falha interna vira ERRO, limitada a 5 (obtido {len(erros)})")
        r.ok(erros and erros[0]["exc"] == "RuntimeError" and erros[0]["onde"] == "upd",
             "ERRO traz só onde + classe da exceção")
        r.ok("segredo" not in "\n".join(c.cap.linhas),
             "mensagem da exceção nunca vaza")


def t12_despacho_real_casa_e_sonda(r):
    """Pelo _dispatch_update REAL: a sonda (registrada antes) carimba a
    chegada e o handler da casa recebe o MESMO evento, sem interferência."""
    async def cenario():
        with Ctx() as c:
            c.cli._mb_entity_cache.set_self_user(424242, False, 0)
            recebidos = []

            async def casa(ev):
                recebidos.append((ev.chat_id, ev.message.id, len(c.linhas("UPD"))))

            c.cli.add_event_handler(casa, events.NewMessage(chats=[MARCADO]))
            d = dt.datetime.now(dt.timezone.utc)
            upd = _novo(CANAL_MON, 31337, d, "texto")
            upd._entities = {}
            await c.cli._dispatch_update(upd)
            r.ok(recebidos == [(MARCADO, 31337, 1)],
                 f"casa recebeu o evento DEPOIS da sonda carimbar (obtido {recebidos})")
            r.ok(len(c.linhas("UPD")) == 1, "sonda registrou 1 UPD pelo despacho real")
    asyncio.run(cenario())


# ── runner ────────────────────────────────────────────────────────
class Resultado:
    def __init__(self):
        self.n = 0
        self.falhas = []

    def ok(self, cond, desc):
        self.n += 1
        if not cond:
            self.falhas.append(desc)


def main() -> int:
    testes = [(k, v) for k, v in sorted(globals().items())
              if k.startswith("t") and k[1:3].isdigit() and callable(v)]
    print(f"Telethon REAL {telethon.__version__} em {inspect.getfile(telethon)}")
    tot = 0; falhos = 0
    for nome, fn in testes:
        r = Resultado()
        try:
            fn(r)
        except Exception:
            r.falhas.append("EXCEÇÃO:\n" + traceback.format_exc())
        tot += r.n
        estado = "ok  " if not r.falhas else "FALHA"
        print(f"  {estado} {nome:48s} {r.n} asserts")
        for f in r.falhas:
            print(f"        -> {f}")
        falhos += bool(r.falhas)
    print(f"TESTES: {len(testes) - falhos}/{len(testes)} verdes | ASSERCOES: {tot} | FALHAS: {falhos}")
    return 1 if falhos else 0


if __name__ == "__main__":
    sys.exit(main())
