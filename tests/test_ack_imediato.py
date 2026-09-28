"""S6.4 — testes do ACK imediato, contra o Telethon REAL 1.45.0.

NÃO usa tests/_harness_e5.py (telethon FALSO em sys.modules): o
mecanismo depende exatamente do `_send_loop`, do `MessagePacker` e do
despacho verdadeiros do Telethon.

Execução (standalone, stdlib + Telethon instalado):
    python3 tests/test_ack_imediato.py
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import logging
import os
import pathlib
import sys
import time
import traceback
import types as pytypes

RAIZ = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

import telethon  # noqa: E402
from telethon import TelegramClient, events  # noqa: E402
from telethon.network.mtprotosender import MTProtoSender  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import types  # noqa: E402

assert hasattr(TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

import ack_imediato as ack  # noqa: E402

JANELA = ack._JANELA_S


# ── infraestrutura mínima ─────────────────────────────────────────
class _Captura(logging.Handler):
    def __init__(self, nome):
        super().__init__(logging.DEBUG)
        self.linhas = []
        self.lg = logging.getLogger(nome)

    def emit(self, record):
        self.linhas.append(record.getMessage())

    def __enter__(self):
        self.lg.addHandler(self)
        return self

    def __exit__(self, *exc):
        self.lg.removeHandler(self)
        return False


def _reset():
    ack._estado.update(cliente=None, prox=0.0, erros=0)


def _cliente() -> TelegramClient:
    return TelegramClient(StringSession(), 1, "0" * 32)


class _Evento:
    """Espião de `_ready`: conta os despertares do _send_loop."""
    def __init__(self, falha=None):
        self.n = 0
        self.falha = falha

    def set(self):
        if self.falha:
            raise self.falha
        self.n += 1


def _falso(sender_ok=True, pendentes=(1,), falha=None, **faltando):
    """Cliente falso com a MESMA superfície que o módulo lê."""
    registrados = []
    ev = _Evento(falha)
    fila = pytypes.SimpleNamespace(_ready=ev)
    sender = pytypes.SimpleNamespace(_pending_ack=set(pendentes), _send_queue=fila)
    for nome in faltando:
        if nome == "_ready":
            del fila._ready
        else:
            delattr(sender, nome)
    cli = pytypes.SimpleNamespace(
        add_event_handler=lambda cb, ev_: registrados.append((cb, ev_)))
    if sender_ok:
        cli._sender = sender
    return cli, sender, ev, registrados


def _handlers_ack(cli):
    return [ev for cb, ev in cli.list_event_handlers() if cb is ack._ao_update]


# ── testes ────────────────────────────────────────────────────────
def t01_gancho_main_antes_dos_handlers_da_casa(r):
    arv = ast.parse((RAIZ / "main.py").read_text(encoding="utf-8"))
    r.ok(any(isinstance(n, ast.Import) and any(a.name == "ack_imediato" for a in n.names)
             for n in arv.body), "main.py importa ack_imediato no topo")
    fn = next(n for n in ast.walk(arv)
              if isinstance(n, ast.AsyncFunctionDef) and n.name == "_preparar_processo")
    chamadas = [n for n in ast.walk(fn) if isinstance(n, ast.Call)
                and ast.unparse(n.func) == "ack_imediato.instalar"]
    r.ok(len(chamadas) == 1, "exatamente 1 chamada a ack_imediato.instalar")
    r.ok(chamadas and ast.unparse(chamadas[0]) == "ack_imediato.instalar(client)",
         "instalado no client PRINCIPAL")
    idx = {}
    for i, n in enumerate(fn.body):
        s = ast.unparse(n)
        for chave in ("ack_imediato.instalar", "completude.instalar", "_registrar_handlers"):
            if chave in s and chave not in idx:
                idx[chave] = i
    r.ok(isinstance(fn.body[idx["ack_imediato.instalar"]], ast.Expr),
         "chamada incondicional (fora de if)")
    r.ok(idx["ack_imediato.instalar"] < idx["completude.instalar"] < idx["_registrar_handlers"],
         f"ACK registrado ANTES dos handlers da casa (índices={idx})")


def t02_requirements_fixado(r):
    linhas = [x.strip() for x in (RAIZ / "requirements.txt").read_text(encoding="utf-8").splitlines()
              if x.strip() and not x.startswith("#")]
    r.ok(linhas == ["telethon==1.45.0", "requests", "beautifulsoup4", "aiohttp",
                    "Pillow", "playwright"],
         f"só telethon fixado; demais dependências intocadas (obtido {linhas})")


def t03_instala_no_1_45_0(r):
    _reset()
    cli = _cliente()
    with _Captura("SISTEMA") as cap:
        ok = ack.instalar(cli)
    evs = _handlers_ack(cli)
    r.ok(telethon.__version__ == "1.45.0", f"Telethon REAL {telethon.__version__}")
    r.ok(ok is True, "instalar() devolve True")
    r.ok(len(evs) == 1 and isinstance(evs[0], events.Raw),
         "exatamente 1 handler, via events.Raw (API pública)")
    r.ok(any("ACK imediato ativo" in x for x in cap.linhas), "loga ativação")
    r.ok(ack.instalar(cli) is True and len(_handlers_ack(cli)) == 1,
         "idempotente: 2ª chamada não duplica o handler")
    outro = _cliente()
    r.ok(ack.instalar(outro) is False and not _handlers_ack(outro),
         "um 2º cliente NÃO recebe o mecanismo (só o principal)")
    _reset()


def t04_versao_diferente_nao_instala(r):
    _reset()
    original = telethon.__version__
    telethon.__version__ = "1.46.0"
    try:
        cli = _cliente()
        with _Captura("SISTEMA") as cap:
            ok = ack.instalar(cli)
    finally:
        telethon.__version__ = original
    r.ok(ok is False and not _handlers_ack(cli), "versão ≠ 1.45.0: não instala")
    r.ok(ack._estado["cliente"] is None, "estado permanece desligado")
    r.ok(any("NÃO instalado" in x and "1.46.0" in x for x in cap.linhas),
         "loga o motivo com a versão encontrada")
    _reset()


def t05_atributos_ausentes_nao_instala(r):
    casos = [
        ({"sender_ok": False}, "client._sender ausente"),
        ({"_pending_ack": 1}, "sender._pending_ack ausente"),
        ({"_send_queue": 1}, "sender._send_queue ausente"),
        ({"_ready": 1}, "sender._send_queue._ready ausente"),
    ]
    for kw, motivo in casos:
        _reset()
        cli, _, _, registrados = _falso(**kw)
        with _Captura("SISTEMA") as cap:
            ok = ack.instalar(cli)
        r.ok(ok is False and not registrados, f"{motivo}: não instala")
        r.ok(any(motivo in x for x in cap.linhas), f"{motivo}: motivo logado")
    _reset()
    cli, _, _, registrados = _falso()

    def explode(*a):
        raise RuntimeError("x")
    cli.add_event_handler = explode
    with _Captura("SISTEMA") as cap:
        r.ok(ack.instalar(cli) is False, "falha no add_event_handler: não derruba o boot")
    r.ok(ack._estado["cliente"] is None and any("falha ao instalar" in x for x in cap.linhas),
         "falha ao registrar: estado desligado e motivo logado")
    _reset()


def t06_handler_nao_bloqueia_o_dispatcher(r):
    fonte = inspect.getsource(ack._ao_update)
    r.ok(not any(isinstance(n, ast.Await) for n in ast.walk(ast.parse(fonte))),
         "_ao_update não contém await")

    async def cenario():
        _reset()
        cli = _cliente()
        cli._mb_entity_cache.set_self_user(424242, False, 0)
        ack.instalar(cli)
        casa_liberada = asyncio.Event()
        estado_na_casa = []

        async def casa(ev):
            estado_na_casa.append(ack._estado["prox"] > 0)
            await casa_liberada.wait()          # pipeline lento

        cli.add_event_handler(casa, events.Raw())
        upd = types.UpdateUserStatus(user_id=1, status=types.UserStatusOnline(expires=0))
        upd._entities = {}
        t = asyncio.get_running_loop().create_task(cli._dispatch_update(upd))
        await asyncio.sleep(0.05)
        r.ok(estado_na_casa == [True],
             "pelo _dispatch_update REAL: ACK agendou ANTES da casa rodar")
        r.ok(not t.done(), "casa bloqueada não impede o ACK (já agendado)")
        casa_liberada.set()
        await t
        # O handler conclui sem nunca suspender: 1 passo da coroutine.
        coro = ack._ao_update(None)
        try:
            coro.send(None)
            suspendeu = True
        except StopIteration:
            suspendeu = False
        r.ok(not suspendeu, "_ao_update termina sem ceder o loop (nunca espera nada)")
        _reset()
    asyncio.run(cenario())


def t07_janela_coalesce_rajada(r):
    async def cenario():
        _reset()
        cli, sender, ev, _ = _falso(pendentes=(1, 2, 3))
        ack.instalar(cli)
        for _ in range(50):
            await ack._ao_update(None)
        await asyncio.sleep(JANELA / 2)
        r.ok(ev.n == 0, "dentro da janela: nenhum despertar ainda")
        await asyncio.sleep(JANELA)
        r.ok(ev.n == 1, f"rajada de 50 updates = 1 despertar (obtido {ev.n})")
        for _ in range(30):
            await ack._ao_update(None)
        await asyncio.sleep(JANELA * 1.5)
        r.ok(ev.n == 2, f"nova rajada após a janela = +1 despertar (obtido {ev.n})")
        sender._pending_ack.clear()
        for _ in range(10):
            await ack._ao_update(None)
        await asyncio.sleep(JANELA * 1.5)
        r.ok(ev.n == 2, "sem ack pendente: nenhum despertar espúrio")
        # Janela FIXA (não debounce): fluxo contínuo a cada 50 ms por 1 s.
        sender._pending_ack.add(9)
        ev.n = 0
        t0 = time.monotonic()
        primeiro = None
        while time.monotonic() - t0 < 1.0:
            await ack._ao_update(None)
            await asyncio.sleep(0.05)
            if ev.n and primeiro is None:
                primeiro = time.monotonic() - t0
        await asyncio.sleep(JANELA * 1.5)
        r.ok(4 <= ev.n <= 6, f"fluxo contínuo de 1 s: ~5 despertares (obtido {ev.n})")
        r.ok(primeiro is not None and primeiro < JANELA + 0.1,
             f"primeiro ack não é adiado pelo fluxo (em {primeiro:.3f}s)" if primeiro
             else "primeiro ack não é adiado pelo fluxo")
        _reset()
    asyncio.run(cenario())


def t08_sem_task_recorrente(r):
    async def cenario():
        _reset()
        loop = asyncio.get_running_loop()
        agendados = []
        original = loop.call_later

        def espiao(atraso, cb, *a, **k):
            if cb is ack._empurrar:            # asyncio.sleep também usa call_later
                agendados.append(cb)
            return original(atraso, cb, *a, **k)
        loop.call_later = espiao
        try:
            cli, _, ev, _ = _falso()
            antes = len(asyncio.all_tasks())
            ack.instalar(cli)
            for _ in range(20):
                await ack._ao_update(None)
            r.ok(len(asyncio.all_tasks()) == antes, "nenhuma task criada pelo handler")
            await asyncio.sleep(1.0)
            r.ok(len(agendados) == 1 and ev.n == 1,
                 f"1 rajada = 1 call_later; ocioso por 1 s: nada se reagenda "
                 f"(call_later={len(agendados)}, despertares={ev.n})")
            r.ok(len(asyncio.all_tasks()) == antes, "nenhuma task viva depois")
        finally:
            loop.call_later = original
            _reset()
    asyncio.run(cenario())


def t09_trava_nunca_fica_presa(r):
    """Sem TimerHandle guardado: se um empurrão se perder, o próximo
    update depois da janela agenda outro."""
    async def cenario():
        _reset()
        loop = asyncio.get_running_loop()
        original = loop.call_later
        perdidos = [0]

        def perde_o_primeiro(atraso, cb, *a, **k):
            if cb is ack._empurrar and perdidos[0] == 0:
                perdidos[0] = 1
                h = original(atraso, cb, *a, **k)
                h.cancel()                     # empurrão perdido
                return h
            return original(atraso, cb, *a, **k)
        loop.call_later = perde_o_primeiro
        try:
            cli, _, ev, _ = _falso()
            ack.instalar(cli)
            await ack._ao_update(None)
            await asyncio.sleep(JANELA * 1.5)
            r.ok(ev.n == 0, "1º empurrão perdido (simulado)")
            await ack._ao_update(None)
            await asyncio.sleep(JANELA * 1.5)
            r.ok(ev.n == 1, "próximo update reagenda: a trava expirou sozinha")
        finally:
            loop.call_later = original
            _reset()
    asyncio.run(cenario())


class _ConexaoFalsa:
    def __init__(self):
        self.enviados = []
        self._connected = True

    async def send(self, data):
        self.enviados.append(data)


def t10_send_loop_real_monta_o_ack(r):
    """O MsgsAck é montado, registrado, limpo e enviado pelo _send_loop
    REAL do Telethon. O módulo só acorda o laço."""
    async def cenario():
        _reset()
        cli = _cliente()
        sender = cli._sender
        sender.auth_key.key = os.urandom(256)
        sender._connection = _ConexaoFalsa()
        sender._user_connected = True
        ack.instalar(cli)
        pk = logging.getLogger("telethon.extensions.messagepacker")
        nivel = pk.level
        pk.setLevel(logging.DEBUG)
        with _Captura("telethon.extensions.messagepacker") as cap:
            laco = asyncio.get_running_loop().create_task(sender._send_loop())
            try:
                await asyncio.sleep(0.02)       # laço parado em _send_queue.get()
                r.ok(not sender._connection.enviados, "laço ocioso não envia nada")
                sender._pending_ack.update({111, 222})
                ack._empurrar()
                r.ok(len(sender._send_queue._deque) == 0,
                     "o empurrão NÃO enfileira nada (FIFO intocado)")
                await asyncio.sleep(0.02)
                r.ok(len(sender._connection.enviados) == 1,
                     "empurrão direto: o laço acordou e enviou 1 pacote")
                sender._connection.enviados.clear()
                cap.linhas.clear()
                # Caminho real: rajada de updates -> 1 ack após a janela.
                sender._pending_ack.update({333, 444, 555})
                for _ in range(40):
                    await ack._ao_update(None)
                await asyncio.sleep(JANELA / 2)
                r.ok(not sender._connection.enviados and sender._pending_ack == {333, 444, 555},
                     "antes da janela: nada enviado, pendentes intactos")
                await asyncio.sleep(JANELA)
                r.ok(len(sender._connection.enviados) == 1,
                     f"após a janela: exatamente 1 pacote (obtido {len(sender._connection.enviados)})")
                r.ok(sender._pending_ack == set(), "_pending_ack limpo pelo _send_loop")
                ultimo = sender._last_acks[-1].request if sender._last_acks else None
                r.ok(isinstance(ultimo, types.MsgsAck)
                     and sorted(ultimo.msg_ids) == [333, 444, 555],
                     "MsgsAck com os 3 ids registrado em _last_acks pelo Telethon")
                r.ok(not sender._pending_state, "ack não entra em _pending_state")
                atrib = [x for x in cap.linhas if x.startswith("Assigned msg_id")]
                r.ok(len(atrib) == 1 and " to MsgsAck " in atrib[0],
                     f"msg_id atribuído pelo MessagePacker a 1 MsgsAck (obtido {atrib})")
            finally:
                laco.cancel()
                try:
                    await laco
                except asyncio.CancelledError:
                    pass
                pk.setLevel(nivel)
                _reset()
    asyncio.run(cenario())


def t11_falha_nunca_propaga_e_nunca_calada(r):
    _reset()
    cli, _, _, _ = _falso(falha=RuntimeError("detalhe https://segredo.example"))
    ack.instalar(cli)
    with _Captura("SISTEMA") as cap:
        for _ in range(8):
            ack._empurrar()                     # não pode lançar
    avisos = [x for x in cap.linhas if "ACK imediato: falha" in x]
    r.ok(len(avisos) == 5, f"falha vira aviso, limitado a 5 (obtido {len(avisos)})")
    r.ok(avisos and "RuntimeError" in avisos[0], "aviso traz a classe da exceção")
    r.ok("segredo" not in "\n".join(cap.linhas), "mensagem da exceção nunca vaza")
    _reset()


def t12_telethon_intocado(r):
    """Nenhuma classe do Telethon é alterada: nem o sender, nem outros
    senders (DCs de mídia), que são instâncias da mesma classe."""
    antes = dict(MTProtoSender.__dict__)
    _reset()
    cli = _cliente()
    ack.instalar(cli)
    asyncio.run(ack._ao_update(None))
    r.ok(dict(MTProtoSender.__dict__) == antes, "MTProtoSender idêntico (sem monkeypatch)")
    fonte = (RAIZ / "ack_imediato.py").read_text(encoding="utf-8")
    arv = ast.parse(fonte)
    atrib = [ast.unparse(n) for n in ast.walk(arv) if isinstance(n, ast.Assign)
             for t in n.targets if isinstance(t, ast.Attribute)]
    r.ok(not atrib, f"o módulo não atribui atributo em objeto algum (obtido {atrib})")
    r.ok("MTProtoSender" not in fonte.split('"""', 2)[2], "o módulo não referencia MTProtoSender")
    _reset()


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
    print(f"Telethon REAL {telethon.__version__} em {inspect.getfile(telethon)} | "
          f"Python {sys.version.split()[0]}")
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
