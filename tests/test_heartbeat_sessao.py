"""S6.8-A — testes do heartbeat de sessão, contra o Telethon REAL 1.45.0.

NÃO usa tests/_harness_e5.py (telethon FALSO em sys.modules): a prova
central — o PING do heartbeat não mexe no keepalive nativo — passa pelo
`_send_loop` e pelo `_process_message` verdadeiros.

Execução (standalone, stdlib + Telethon instalado):
    python3 tests/test_heartbeat_sessao.py
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import logging
import os
import pathlib
import subprocess
import sys
import time
import traceback
import types as pytypes

RAIZ = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

import telethon  # noqa: E402
from telethon import TelegramClient, errors, functions  # noqa: E402
from telethon.network.mtprotosender import MTProtoSender  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402
from telethon.tl import types  # noqa: E402

assert hasattr(TelegramClient, "_dispatch_update"), "telethon FALSO — abortado"

import globals as g  # noqa: E402
import heartbeat_sessao as hb  # noqa: E402

VAR = "HEARTBEAT_PING_S"


# ── infraestrutura mínima ─────────────────────────────────────────
class _Captura(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.linhas = []
        self.lg = logging.getLogger("SISTEMA")

    def emit(self, record):
        self.linhas.append(record.getMessage())

    def __enter__(self):
        self.lg.addHandler(self)
        return self

    def __exit__(self, *exc):
        self.lg.removeHandler(self)
        return False


class _Env:
    def __init__(self, valor):
        self.valor = valor

    def __enter__(self):
        self.antes = os.environ.get(VAR)
        if self.valor is None:
            os.environ.pop(VAR, None)
        else:
            os.environ[VAR] = self.valor
        return self

    def __exit__(self, *exc):
        if self.antes is None:
            os.environ.pop(VAR, None)
        else:
            os.environ[VAR] = self.antes
        return False


class _ClienteFalso:
    """Superfície pública usada pelo heartbeat: __call__ e is_connected."""

    def __init__(self, latencia=0.0, falhas=None, conectado=True):
        self.latencia = latencia
        self.falhas = list(falhas or [])   # exceções/None consumidas por chamada
        self.conectado = conectado
        self.chamadas = []                 # (instante, request)
        self.em_voo = 0
        self.max_em_voo = 0

    def is_connected(self):
        return self.conectado

    async def __call__(self, request):
        self.chamadas.append((time.monotonic(), request))
        self.em_voo += 1
        self.max_em_voo = max(self.max_em_voo, self.em_voo)
        try:
            falha = self.falhas.pop(0) if self.falhas else None
            if falha == "trava":
                await asyncio.sleep(3600)
            if isinstance(falha, BaseException):
                raise falha
            await asyncio.sleep(self.latencia)
            return types.Pong(msg_id=1, ping_id=request.ping_id)
        finally:
            self.em_voo -= 1


def _rodar(coro, limite=10.0):
    async def _c():
        return await asyncio.wait_for(coro, limite)
    return asyncio.run(_c())


def _flood(seg=30):
    e = errors.FloodWaitError(request=None, capture=seg)
    return e


# ── testes: configuração ──────────────────────────────────────────
def t01_ausente_off(r):
    with _Env(None):
        r.ok(hb.intervalo_configurado() is None, "sem variável → OFF")


def t02_vazia_off(r):
    with _Env(""):
        r.ok(hb.intervalo_configurado() is None, "vazia → OFF")
    with _Env("   "):
        r.ok(hb.intervalo_configurado() is None, "só espaços → OFF")


def t03_zero_off(r):
    for v in ("0", "0.0", "-0"):
        with _Env(v):
            r.ok(hb.intervalo_configurado() is None, f"{v!r} → OFF")


def t04_invalida_off(r):
    for v in ("abc", "nan", "inf", "-inf", "-2", "1,5", "2s"):
        with _Env(v):
            r.ok(hb.intervalo_configurado() is None, f"{v!r} → OFF")


def t05_decimal(r):
    for v, esperado in (("2.5", 2.5), ("5", 5.0), ("1", 1.0), (" 3 ", 3.0)):
        with _Env(v):
            r.ok(hb.intervalo_configurado() == esperado, f"{v!r} → {esperado}")


def t06_piso_1s(r):
    for v in ("0.3", "0.999", "0.0001"):
        with _Env(v):
            r.ok(hb.intervalo_configurado() == 1.0, f"{v!r} → piso 1.0")


# ── testes: laço ──────────────────────────────────────────────────
def t07_cadencia_inicio_a_inicio(r):
    """Com latência de 100 ms e intervalo de 200 ms, os INÍCIOS ficam
    ~200 ms apart (o tempo do ping é descontado da espera). Cadência
    fim→início daria ~300 ms e é reprovada."""
    cli = _ClienteFalso(latencia=0.1)

    async def cenario():
        t = asyncio.get_running_loop().create_task(hb._laco(cli, 0.2))
        await asyncio.sleep(1.15)
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
    _rodar(cenario())
    inicios = [c[0] for c in cli.chamadas]
    deltas = [b - a for a, b in zip(inicios, inicios[1:])]
    r.ok(len(inicios) >= 4, f"~5 pings em 1,15 s (obtido {len(inicios)})")
    r.ok(deltas and all(0.17 <= d <= 0.25 for d in deltas),
         f"início→início ≈ 200 ms (obtido {[round(d, 3) for d in deltas]})")


def t08_nunca_dois_em_voo(r):
    """Ping mais lento que o intervalo: o próximo só sai depois."""
    cli = _ClienteFalso(latencia=0.3)

    async def cenario():
        t = asyncio.get_running_loop().create_task(hb._laco(cli, 0.05))
        await asyncio.sleep(1.2)
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
    _rodar(cenario())
    r.ok(cli.max_em_voo == 1, f"no máximo 1 ping em voo (obtido {cli.max_em_voo})")
    inicios = [c[0] for c in cli.chamadas]
    deltas = [b - a for a, b in zip(inicios, inicios[1:])]
    r.ok(deltas and all(d >= 0.29 for d in deltas),
         f"sem rajada de recuperação: cada início após o fim do anterior ({[round(d, 3) for d in deltas]})")


def t09_ping_request_publico(r):
    cli = _ClienteFalso()

    async def cenario():
        t = asyncio.get_running_loop().create_task(hb._laco(cli, 0.05))
        await asyncio.sleep(0.2)
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
    _rodar(cenario())
    reqs = [c[1] for c in cli.chamadas]
    r.ok(reqs and all(isinstance(x, functions.PingRequest) for x in reqs),
         "envia functions.PingRequest pelo client() público")
    ids = [x.ping_id for x in reqs]
    r.ok(len(set(ids)) == len(ids) and all(-2 ** 63 <= i < 2 ** 63 for i in ids),
         "ping_id aleatório de 64 bits, sem repetição")


def t10_timeout_e_reset(r):
    """timeout, sucesso, timeout, timeout, sucesso: nunca 3 SEGUIDOS."""
    cli = _ClienteFalso(falhas=["trava", None, "trava", "trava", None, None])

    async def cenario():
        t = asyncio.get_running_loop().create_task(hb._laco(cli, 0.01, timeout=0.05))
        await asyncio.sleep(0.8)
        vivo = not t.done()
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
        return vivo
    with _Captura() as cap:
        vivo = _rodar(cenario())
    r.ok(vivo, "sucesso intercalado zera o contador: segue ativo")
    r.ok(not any("DESATIVADO" in x for x in cap.linhas), "não desativou")


# Pausas curtas para teste (produção: 30 s → 300 s). Mesma lógica.
_PB, _PM = 0.1, 0.4


def _pausas(linhas):
    """[(motivo, pausa_s)] de cada HEARTBEAT|PAUSA logada."""
    out = []
    for x in linhas:
        if "HEARTBEAT|PAUSA|" in x:
            campos = dict(c.split("=", 1) for c in x.split("|") if "=" in c)
            out.append((campos.get("motivo"), float(campos.get("pausa_s", "nan"))))
    return out


def t11_um_timeout_nao_pausa(r):
    cli = _ClienteFalso(falhas=["trava", None, None])

    async def cenario():
        t = asyncio.get_running_loop().create_task(
            hb._laco(cli, 0.01, timeout=0.05, pausa_base=_PB, pausa_max=_PM))
        await asyncio.sleep(0.3)
        vivo = not t.done()
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
        return vivo
    with _Captura() as cap:
        vivo = _rodar(cenario())
    r.ok(vivo and len(cli.chamadas) >= 3, f"1 timeout: segue pingando (chamadas={len(cli.chamadas)})")
    r.ok(not any("PAUSA" in x or "DESATIVADO" in x for x in cap.linhas), "sem pausa, sem desativar")


def t12_tres_timeouts_pausam_e_retomam(r):
    cli = _ClienteFalso(falhas=["trava", "trava", "trava", None, None])

    async def cenario():
        t = asyncio.get_running_loop().create_task(
            hb._laco(cli, 0.01, timeout=0.05, pausa_base=_PB, pausa_max=_PM))
        await asyncio.sleep(0.6)
        vivo = not t.done()
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
        return vivo
    with _Captura() as cap:
        vivo = _rodar(cenario())
    r.ok(vivo, "3 timeouts NÃO matam a task")
    r.ok(_pausas(cap.linhas)[:1] == [("3_falhas_seguidas", _PB)], f"PAUSA logada ({_pausas(cap.linhas)})")
    inicios = [c[0] for c in cli.chamadas]
    r.ok(len(inicios) >= 5 and inicios[3] - inicios[2] >= 0.05 + _PB - 0.02,
         "a 4ª tentativa só sai depois da pausa")
    r.ok(any("HEARTBEAT|RETOMADO|apos=3_falhas_seguidas|pausas=1" in x for x in cap.linhas),
         "RETOMADO logado no 1º sucesso")
    r.ok(not any("DESATIVADO" in x for x in cap.linhas), "nunca desativa por timeout")


def t13_floodwait_respeita_o_prazo_e_volta(r):
    cli = _ClienteFalso(falhas=[_flood(1), None, None])

    async def cenario():
        t = asyncio.get_running_loop().create_task(hb._laco(cli, 0.01))
        await asyncio.sleep(1.4)
        vivo = not t.done()
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
        return vivo
    with _Captura() as cap:
        vivo = _rodar(cenario())
    inicios = [c[0] for c in cli.chamadas]
    r.ok(vivo and len(inicios) >= 2, f"FloodWait não mata a task (chamadas={len(inicios)})")
    r.ok(len(inicios) >= 2 and inicios[1] - inicios[0] >= 0.99,
         f"esperou o prazo pedido ({[round(b - a, 3) for a, b in zip(inicios, inicios[1:])]})")
    r.ok(any("HEARTBEAT|FLOODWAIT|s=1" in x for x in cap.linhas), "FLOODWAIT logado")
    r.ok(any("HEARTBEAT|RETOMADO|apos=FloodWaitError" in x for x in cap.linhas), "RETOMADO logado")
    r.ok(not any("DESATIVADO" in x for x in cap.linhas), "não desativa")


def t13b_rpc_transitorio_pausa_e_retoma(r):
    for nome, erro in (("ServerError", errors.ServerError(request=None, message="INTERNAL")),
                       ("TimedOutError", errors.TimedOutError(request=None, message="TIMEOUT")),
                       ("RPCError", errors.RPCError(request=None, message="X", code=599))):
        cli = _ClienteFalso(falhas=[erro, None, None])

        async def cenario(cli=cli):
            t = asyncio.get_running_loop().create_task(
                hb._laco(cli, 0.01, pausa_base=_PB, pausa_max=_PM))
            await asyncio.sleep(0.4)
            vivo = not t.done()
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)
            return vivo
        with _Captura() as cap:
            vivo = _rodar(cenario())
        inicios = [c[0] for c in cli.chamadas]
        r.ok(vivo and len(inicios) >= 2, f"{nome}: task viva e volta a pingar ({len(inicios)})")
        r.ok(_pausas(cap.linhas)[:1] == [(nome, _PB)], f"{nome}: PAUSA imediata ({_pausas(cap.linhas)})")
        r.ok(len(inicios) >= 2 and inicios[1] - inicios[0] >= _PB - 0.02, f"{nome}: respeitou a pausa")
        r.ok(any(f"RETOMADO|apos={nome}" in x for x in cap.linhas), f"{nome}: RETOMADO")


def t13c_permanente_desativa_sem_retry(r):
    for nome, erro in (("BadRequestError", errors.BadRequestError(request=None, message="X")),
                       ("UnauthorizedError", errors.UnauthorizedError(request=None, message="X")),
                       ("AuthKeyUnregisteredError", errors.AuthKeyUnregisteredError(request=None)),
                       ("ForbiddenError", errors.ForbiddenError(request=None, message="X"))):
        cli = _ClienteFalso(falhas=[erro, None])

        async def cenario(cli=cli):
            t = asyncio.get_running_loop().create_task(
                hb._laco(cli, 0.01, pausa_base=_PB, pausa_max=_PM))
            await asyncio.sleep(0.3)
            return t
        with _Captura() as cap:
            t = _rodar(cenario())
        r.ok(t.done() and t.exception() is None and len(cli.chamadas) == 1,
             f"{nome}: desativa na 1ª, sem nova tentativa (chamadas={len(cli.chamadas)})")
        r.ok(any(f"DESATIVADO|motivo={nome}" in x for x in cap.linhas), f"{nome}: motivo logado")
        r.ok(not _pausas(cap.linhas), f"{nome}: nenhuma pausa/retry")


def t13d_backoff_cresce_com_teto_e_zera_no_sucesso(r):
    srv = lambda: errors.ServerError(request=None, message="INTERNAL")  # noqa: E731
    cli = _ClienteFalso(falhas=[srv(), srv(), srv(), srv(), None, srv(), None])

    async def cenario():
        t = asyncio.get_running_loop().create_task(
            hb._laco(cli, 0.01, pausa_base=0.05, pausa_max=0.15))
        await asyncio.sleep(0.9)
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
    with _Captura() as cap:
        _rodar(cenario())
    seq = [p for _m, p in _pausas(cap.linhas)]
    r.ok(seq[:5] == [0.05, 0.1, 0.15, 0.15, 0.05], f"30→60→120…teto, zera no sucesso (proporcional: {seq})")
    r.ok(hb._PAUSA_BASE_S == 30.0 and hb._PAUSA_MAX_S == 300.0, "produção: 30 s → teto 300 s")
    r.ok(hb._TIMEOUT_S == 10.0 and hb._PISO_S == 1.0 and hb._MAX_TIMEOUTS == 3,
         "timeout, piso e limiar de falhas inalterados")


def t14_erro_conexao_segue(r):
    cli = _ClienteFalso(falhas=[ConnectionError("x"), OSError("y"), None, None])

    async def cenario():
        t = asyncio.get_running_loop().create_task(hb._laco(cli, 0.02))
        await asyncio.sleep(0.4)
        vivo = not t.done()
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
        return vivo
    with _Captura() as cap:
        vivo = _rodar(cenario())
    r.ok(vivo and len(cli.chamadas) >= 4, "erro de conexão não derruba nem desativa")
    r.ok(not any("DESATIVADO" in x for x in cap.linhas), "sem desativação")


def t15_desconectado_pula(r):
    cli = _ClienteFalso(conectado=False)

    async def cenario():
        t = asyncio.get_running_loop().create_task(hb._laco(cli, 0.02))
        await asyncio.sleep(0.2)
        n_desc = len(cli.chamadas)
        religou = time.monotonic()
        cli.conectado = True
        await asyncio.sleep(0.1)
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
        return n_desc, religou
    n_desc, religou = _rodar(cenario())
    r.ok(n_desc == 0, f"desconectado: nenhum ping enviado (obtido {n_desc})")
    r.ok(cli.chamadas and all(c[0] >= religou for c in cli.chamadas),
         "volta a pingar só depois de reconectar")


def t16_cancelamento_limpo(r):
    cli = _ClienteFalso(falhas=["trava"])

    async def cenario():
        t = asyncio.get_running_loop().create_task(hb._laco(cli, 0.01, timeout=5))
        await asyncio.sleep(0.1)          # cancelado DURANTE um ping em voo
        t.cancel()
        res = await asyncio.gather(t, return_exceptions=True)
        return t, res
    t, res = _rodar(cenario())
    r.ok(t.done() and not t.cancelled() and res == [None],
         f"cancelamento em voo termina limpo, sem exceção vazada (res={res})")


class _ClienteCancelaNoMesmoTick:
    """O ping termina no MESMO tick em que a task do heartbeat é
    cancelada — a janela em que wait_for (Python < 3.12) engole o
    cancelamento. `falha` None = ping ok; exceção = ping com erro."""

    def __init__(self, falha=None):
        self.falha, self.alvo, self.chamadas = falha, None, 0

    def is_connected(self):
        return True

    async def __call__(self, request):
        self.chamadas += 1
        if self.chamadas == 2 and self.alvo is not None:
            asyncio.get_running_loop().call_soon(self.alvo.cancel)
            if self.falha is not None:
                raise self.falha
        return types.Pong(msg_id=1, ping_id=request.ping_id)


def t16b_cancelamento_no_mesmo_tick_do_ping(r):
    for rot, falha in (("ping_ok", None),
                       ("ping_erro", errors.ServerError(request=None, message="X"))):
        cli = _ClienteCancelaNoMesmoTick(falha)

        async def cenario(cli=cli):
            t = asyncio.get_running_loop().create_task(
                hb._laco(cli, 0.01, pausa_base=30.0, pausa_max=300.0))
            cli.alvo = t
            t0 = time.monotonic()
            res = await asyncio.wait_for(asyncio.gather(t, return_exceptions=True), 2.0)
            return t, res, time.monotonic() - t0, cli.chamadas
        try:
            t, res, dt, n = _rodar(cenario())
            r.ok(t.done() and res == [None] and dt < 1.0 and n == 2,
                 f"{rot}: cancel honrado mesmo engolido pelo wait_for "
                 f"(res={res}, {dt:.3f}s, chamadas={n})")
        except Exception as e:                       # noqa: BLE001
            r.ok(False, f"{rot}: task não terminou após cancel ({type(e).__name__})")


def t17_encerrando_sai_e_shutdown(r):
    cli = _ClienteFalso()

    async def cenario():
        t = asyncio.get_running_loop().create_task(hb._laco(cli, 0.05))
        await asyncio.sleep(0.12)
        g._encerrando = True
        try:
            await asyncio.sleep(0.15)
            return t.done(), len(cli.chamadas)
        finally:
            g._encerrando = False
    fim, n = _rodar(cenario())
    r.ok(fim, "g._encerrando → a task sai sozinha")
    # integração com main._encerrar: cancel + await da task do heartbeat
    arv = ast.parse((RAIZ / "main.py").read_text(encoding="utf-8"))
    enc = next(n for n in ast.walk(arv)
               if isinstance(n, ast.AsyncFunctionDef) and n.name == "_encerrar")
    src = ast.unparse(enc)
    r.ok("_TASKS_FUNDO.get('heartbeat')" in src and "t.cancel()" in src
         and "asyncio.gather(t, return_exceptions=True)" in src,
         "_encerrar cancela E aguarda a task do heartbeat")


def t17b_conexao_seguida_pausa_cancelamento_e_shutdown_na_pausa(r):
    # 3 erros de conexão seguidos: mesma régua dos timeouts → pausa
    cli = _ClienteFalso(falhas=[ConnectionError("a"), OSError("b"), ConnectionError("c"), None])

    async def pausa():
        t = asyncio.get_running_loop().create_task(
            hb._laco(cli, 0.01, pausa_base=_PB, pausa_max=_PM))
        await asyncio.sleep(0.4)
        vivo = not t.done()
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
        return vivo
    with _Captura() as cap:
        vivo = _rodar(pausa())
    r.ok(vivo and _pausas(cap.linhas)[:1] == [("3_falhas_seguidas", _PB)],
         f"3 erros de conexão → pausa, task viva ({_pausas(cap.linhas)})")

    # cancelamento NO MEIO de uma pausa longa: sai limpo e na hora
    cli2 = _ClienteFalso(falhas=[errors.ServerError(request=None, message="X")])

    async def cancela():
        t = asyncio.get_running_loop().create_task(
            hb._laco(cli2, 0.01, pausa_base=30.0, pausa_max=300.0))
        await asyncio.sleep(0.1)
        t0 = time.monotonic()
        t.cancel()
        res = await asyncio.gather(t, return_exceptions=True)
        return t, res, time.monotonic() - t0
    t, res, dt = _rodar(cancela())
    r.ok(t.done() and not t.cancelled() and res == [None] and dt < 0.5,
         f"cancelamento durante a pausa termina limpo e imediato (res={res}, {dt:.3f}s)")

    # shutdown (g._encerrando) durante a pausa: ao acordar, sai sem pingar
    cli3 = _ClienteFalso(falhas=[errors.ServerError(request=None, message="X"), None])

    async def encerra():
        t = asyncio.get_running_loop().create_task(
            hb._laco(cli3, 0.01, pausa_base=0.2, pausa_max=0.2))
        await asyncio.sleep(0.05)
        g._encerrando = True
        try:
            await asyncio.sleep(0.35)
            return t.done(), len(cli3.chamadas)
        finally:
            g._encerrando = False
    fim, n = _rodar(encerra())
    r.ok(fim and n == 1, f"encerrando durante a pausa: sai sem nova tentativa (chamadas={n})")


def t18_off_nao_cria_task(r):
    async def cenario():
        antes = len(asyncio.all_tasks())
        with _Env(None), _Captura() as cap:
            t = hb.iniciar(_ClienteFalso())
        return t, len(asyncio.all_tasks()) - antes, cap.linhas
    t, novas, linhas = _rodar(cenario())
    r.ok(t is None and novas == 0, "OFF: nenhuma task criada")
    r.ok(any("HEARTBEAT|OFF" in x for x in linhas), "loga HEARTBEAT|OFF")

    async def ligado():
        with _Env("2.5"), _Captura() as cap:
            t = hb.iniciar(_ClienteFalso())
        vivo = t is not None and not t.done()
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
        return vivo, cap.linhas
    vivo, linhas = _rodar(ligado())
    r.ok(vivo, "ON: exatamente uma task criada")
    r.ok(any("HEARTBEAT|ATIVO|intervalo=2.5s" in x for x in linhas), "loga ATIVO com o intervalo")


class _ConexaoFalsa:
    def __init__(self):
        self.enviados = []
        self._connected = True

    async def send(self, data):
        self.enviados.append(data)


def t19_keepalive_nativo_intacto(r):
    """PING do heartbeat pelo _send_loop REAL + pong entregue pelo
    _process_message REAL: o estado do keepalive nativo não muda."""
    antes = dict(MTProtoSender.__dict__)

    async def cenario():
        cli = TelegramClient(StringSession(), 1, "0" * 32)
        sender = cli._sender
        sender.auth_key.key = os.urandom(256)
        sender._connection = _ConexaoFalsa()
        sender._user_connected = True
        sender._ping = 424242                    # ping NATIVO em voo
        laco = asyncio.get_running_loop().create_task(sender._send_loop())
        try:
            chamada = asyncio.get_running_loop().create_task(
                cli(functions.PingRequest(ping_id=777)))
            await asyncio.sleep(0.05)
            estado = next(s for s in sender._pending_state.values()
                          if isinstance(s.request, functions.PingRequest))
            msg = pytypes.SimpleNamespace(
                msg_id=99, obj=types.Pong(msg_id=estado.msg_id, ping_id=777))
            await sender._process_message(msg)
            pong = await asyncio.wait_for(chamada, 2)
            return pong, sender._ping, len(sender._connection.enviados)
        finally:
            laco.cancel()
            await asyncio.gather(laco, return_exceptions=True)
    pong, ping_nativo, enviados = _rodar(cenario())
    r.ok(isinstance(pong, types.Pong) and pong.ping_id == 777, "o PING do heartbeat recebe o próprio pong")
    r.ok(ping_nativo == 424242, "o _ping do keepalive nativo NÃO foi alterado")
    r.ok(enviados >= 1, "saiu pelo _send_loop real")
    r.ok(dict(MTProtoSender.__dict__) == antes, "MTProtoSender sem monkeypatch")
    fonte = (RAIZ / "heartbeat_sessao.py").read_text(encoding="utf-8")
    r.ok("_keepalive" not in fonte.split('"""', 2)[2], "o módulo não referencia o keepalive nativo")


def t20_s64_intacto(r):
    blob = subprocess.run(["git", "-C", str(RAIZ), "hash-object", "ack_imediato.py"],
                          capture_output=True, text=True).stdout.strip()
    r.ok(blob.startswith("d47783e"), f"ack_imediato.py inalterado (blob {blob[:12]})")
    req = (RAIZ / "requirements.txt").read_text(encoding="utf-8").splitlines()
    r.ok(req[0].strip() == "telethon==1.45.0" and telethon.__version__ == "1.45.0",
         "Telethon 1.45.0 fixado e instalado")


def t21_sem_internals_e_integracao(r):
    fonte = (RAIZ / "heartbeat_sessao.py").read_text(encoding="utf-8")
    arv = ast.parse(fonte)
    privados = sorted({ast.unparse(n) for n in ast.walk(arv)
                       if isinstance(n, ast.Attribute) and n.attr.startswith("_")
                       and not (n.attr.startswith("__") and n.attr.endswith("__"))
                       and ast.unparse(n.value) not in ("g", "self")})
    r.ok(not privados, f"nenhum atributo privado de Telethon/cliente (obtido {privados})")
    imps = [ast.unparse(n) for n in arv.body if isinstance(n, (ast.Import, ast.ImportFrom))]
    r.ok(not any(x for x in imps if "pipeline" in x or "plataformas" in x or "database" in x),
         f"não importa pipeline/plataformas/banco ({imps})")
    r.ok(not any(isinstance(n, ast.Call) and ast.unparse(n.func).endswith("create_task")
                 for n in ast.walk(next(f for f in arv.body
                                        if isinstance(f, ast.AsyncFunctionDef) and f.name == "_laco"))),
         "o laço não cria task por ciclo")
    m = ast.parse((RAIZ / "main.py").read_text(encoding="utf-8"))
    fn = next(n for n in ast.walk(m)
              if isinstance(n, ast.AsyncFunctionDef) and n.name == "_preparar_processo")
    corpo = [ast.unparse(n) for n in fn.body]
    i_ack = next(i for i, s in enumerate(corpo) if s == "ack_imediato.instalar(client)")
    r.ok(corpo[i_ack + 1] == "_TASKS_FUNDO['heartbeat'] = heartbeat_sessao.iniciar(client)",
         "instalado logo depois do ack_imediato")
    r.ok(sum("heartbeat_sessao.iniciar" in s for s in corpo) == 1, "uma única instalação")


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
