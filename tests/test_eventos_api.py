"""
F1.1 — API PRIVADA (eventos/api_privada.py): só leitura, só memória.

Servidor aiohttp REAL. A aplicação (criar_app) é servida num servidor
local de TESTE para as rotas; a escuta de produção (iniciar) é provada à
parte, de forma DETERMINÍSTICA: nada aqui depende de o host (ou o runner
do CI) ter IPv6 — a configuração do socket é provada com socket falso
injetado e o caminho de sucesso com uma fábrica de socket injetada.

  01  assinatura: sem, errada, vencida, futura, malformada → 401; válida → 200
  02  canonicalização: mudar cursor/limite/espera, acrescentar ou
      reordenar parâmetro, trocar caminho, método ou ts → 401
  03  /v1/saude: retrato da memória + processo; no-store
  04  /v1/eventos: inicio → cursor → continua
  05  reinício (boot A → B): cursor de A → nova_epoca, lê B do início
  06  lacuna no mesmo boot (anel girou)
  07  espera longa: acorda com evento novo; expira vazia; troca de época
      responde na hora; desligamento responde na hora
  08  parâmetros inválidos → 400
  09  só GET: POST/PUT/DELETE/PATCH/HEAD → 405; rota desconhecida → 404
  10  nenhuma conexão SQLite durante as requisições
  12  iniciar recusa configuração inválida — inclusive porta de proxy TCP
      público — sem abrir socket
  13  escuta privada DUAL-STACK (socket falso injetado): AF_INET6 em "::"
      com IPV6_V6ONLY=0 (IPv4 + IPv6); famílias lidas do socket; falha
      fecha o socket; nenhum AF_INET pedido; falha → API desligada
  14  IPv6 INDISPONÍVEL (AF_INET6 sabotado): API desligada, NENHUM socket
      AF_INET criado (nada de 0.0.0.0), o registro/worker segue
  15  taxa: rajada acima do balde → 429; recarrega; sem assinatura não
      gasta ficha (ninguém sem o segredo esgota o balde do Brain)
  16  esperas longas simultâneas limitadas: acima do teto responde já
  17  CAMINHO DE SUCESSO determinístico (fábrica de socket injetada):
      serve assinado, /v1/saude traz a rede, encerrar acorda a espera
      longa na hora, solta a porta, idempotente; cancelamento no meio do
      iniciar não deixa nada aberto
  18  ISOLAMENTO: requisição que passou pela borda pública (X-Railway-Edge,
      X-Real-IP, X-Forwarded-*, Forwarded…) ou com Host público
      (www.leoind.com.br, *.up.railway.app) → 404 ANTES da assinatura;
      worker.railway.internal, localhost, loopback e fd12:: → 200
  99  o segredo nunca aparece em NENHUMA resposta de todos os testes acima

    python tests/test_eventos_api.py
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import socket
import sqlite3
import sys
import time

import aiohttp                                                  # real, antes do harness
from aiohttp import web
from yarl import URL

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar                         # noqa: E402
preparar()

from eventos import api_privada                                 # noqa: E402
from eventos.anel import Registro                               # noqa: E402
from eventos.api_privada import (CAB_ASSINATURA, CAB_TS, assinar,  # noqa: E402
                                 criar_app)

SEGREDO = "segredo-de-teste-" + "x" * 24        # só teste; nunca produção
A, B = "a" * 16, "b" * 16
_CORPOS: list = []                               # tudo que a API devolveu


class Provedor:
    """Troca de registro = reinício do processo (época nova)."""
    def __init__(self, reg):
        self.reg = reg

    def __call__(self):
        return self.reg


async def _servir(app):
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    porta = site._server.sockets[0].getsockname()[1]
    return runner, f"http://127.0.0.1:{porta}"


async def _pedir(sessao, base, caminho, *, metodo="GET", ts=None,
                 assinatura=None, assinar_com=None, cabecalhos=True, extra=None):
    ts = str(int(time.time())) if ts is None else ts
    if assinatura is None:
        m, c, t = assinar_com or (metodo, caminho, ts)
        assinatura = assinar(SEGREDO, m, c, t)
    h = {CAB_TS: ts, CAB_ASSINATURA: assinatura} if cabecalhos else {}
    h.update(extra or {})
    async with sessao.request(metodo, URL(base + caminho, encoded=True),
                              headers=h) as resp:
        corpo = await resp.read()
        _CORPOS.append(corpo + str(dict(resp.headers)).encode())
        return resp.status, corpo, resp.headers


def cenario(corpo, provedor=None, saude_extra=None):
    async def _run():
        prov = provedor or Provedor(Registro(1000, 1 << 26, boot_id=A))
        runner, base = await _servir(criar_app(prov, SEGREDO, saude_extra=saude_extra))
        try:
            async with aiohttp.ClientSession() as s:
                return await corpo(s, base, prov)
        finally:
            await runner.cleanup()
    return asyncio.run(_run())


def _json(corpo):
    return json.loads(corpo.decode("utf-8"))


def _familia(a, k):
    """Família de um socket.socket(...) chamado com posição OU palavra
    (o asyncio usa family=...); sem família → AF_INET, como o Python."""
    return k.get("family", a[0] if a else socket.AF_INET)


# ══════════════════════════════════════════════════════════════════
def test_01_assinatura(r):
    async def corpo(s, base, prov):
        agora = int(time.time())
        cam = "/v1/saude"
        res = {
            "sem": (await _pedir(s, base, cam, cabecalhos=False))[0],
            "errada": (await _pedir(s, base, cam, assinatura="0" * 64))[0],
            "vencida": (await _pedir(s, base, cam, ts=str(agora - 61)))[0],
            "futura": (await _pedir(s, base, cam, ts=str(agora + 61)))[0],
            "ts_ruim": (await _pedir(s, base, cam, ts="12a", assinatura="0" * 64))[0],
            "outro_segredo": (await _pedir(
                s, base, cam,
                assinatura=hmac.new(b"outro" * 8, f"GET\n{cam}\n{agora}".encode(),
                                    hashlib.sha256).hexdigest(), ts=str(agora)))[0],
            "valida": (await _pedir(s, base, cam))[0],
            "maiusculas": (await _pedir(
                s, base, cam, ts=str(agora),
                assinatura=assinar(SEGREDO, "GET", cam, str(agora)).upper()))[0],
            "na_borda": (await _pedir(s, base, cam, ts=str(agora - 59)))[0],
        }
        return res
    res = cenario(corpo)
    for k in ("sem", "errada", "vencida", "futura", "ts_ruim", "outro_segredo"):
        r.check(res[k] == 401, f"01.{k}_401", str(res[k]))
    for k in ("valida", "maiusculas", "na_borda"):
        r.check(res[k] == 200, f"01.{k}_200", str(res[k]))


def test_02_canonicalizacao(r):
    ts = str(int(time.time()))
    original = f"/v1/eventos?cursor={A}:0&limite=10&espera=0"
    alteracoes = {
        "cursor": f"/v1/eventos?cursor={A}:1&limite=10&espera=0",
        "limite": f"/v1/eventos?cursor={A}:0&limite=11&espera=0",
        "espera": f"/v1/eventos?cursor={A}:0&limite=10&espera=1",
        "parametro_a_mais": f"/v1/eventos?cursor={A}:0&limite=10&espera=0&x=1",
        "reordenado": f"/v1/eventos?limite=10&cursor={A}:0&espera=0",
        "sem_query": "/v1/eventos",
        "caminho": "/v1/saude",
    }

    async def corpo(s, base, prov):
        out = {"original": (await _pedir(s, base, original, ts=ts))[0]}
        for nome, cam in alteracoes.items():
            out[nome] = (await _pedir(s, base, cam, ts=ts,
                                      assinar_com=("GET", original, ts)))[0]
        out["metodo"] = (await _pedir(s, base, original, metodo="POST", ts=ts,
                                      assinar_com=("GET", original, ts)))[0]
        out["ts"] = (await _pedir(s, base, original, ts=str(int(ts) - 1),
                                  assinar_com=("GET", original, ts)))[0]
        return out
    res = cenario(corpo)
    r.check(res["original"] == 200, "02.original_200", str(res["original"]))
    for nome in list(alteracoes) + ["metodo", "ts"]:
        r.check(res[nome] == 401, f"02.{nome}_invalida_a_assinatura", str(res[nome]))
    # a canônica é exatamente METODO \n caminho?query \n ts
    esperado = hmac.new(SEGREDO.encode(), f"GET\n{original}\n{ts}".encode(),
                        hashlib.sha256).hexdigest()
    r.check(assinar(SEGREDO, "GET", original, ts) == esperado, "02.forma_canonica")


def test_03_saude(r):
    async def corpo(s, base, prov):
        prov.reg.emitir("processo.iniciado")
        return await _pedir(s, base, "/v1/saude")
    st, c, h = cenario(corpo, saude_extra=lambda: {"fila": 3, "workers": 1})
    d = _json(c)
    r.check(st == 200 and d["boot_id"] == A and d["seq_ultimo"] == 1
            and d["max_eventos_anel"] == 1000 and d["max_bytes_anel"] == 1 << 26
            and d["eventos_anel"] == 1 and d["bytes_anel"] > 0
            and d["processo"] == {"fila": 3, "workers": 1}, "03.campos", str(d))
    r.check(h.get("Cache-Control") == "no-store", "03.no_store")
    for k in ("seq_primeiro", "descartados", "descartados_por_eventos",
              "descartados_por_bytes", "falhas_emissao", "excedidos",
              "boot_iniciado_em", "no_ar_s", "rede", "recusadas_rede",
              "recusadas_assinatura", "recusadas_excesso", "esperas_ativas"):
        r.check(k in d, f"03.tem_{k}")


def test_04_eventos_por_cursor(r):
    async def corpo(s, base, prov):
        for i in range(5):
            prov.reg.emitir("t", {"i": i})
        st1, c1, _ = await _pedir(s, base, "/v1/eventos")
        m1 = _json(c1)
        for i in range(5, 7):
            prov.reg.emitir("t", {"i": i})
        st2, c2, _ = await _pedir(s, base, f"/v1/eventos?cursor={m1['cursor']}")
        return st1, m1, st2, _json(c2)
    st1, m1, st2, m2 = cenario(corpo)
    r.check(st1 == 200 and m1["continuidade"] == "inicio"
            and [e["seq"] for e in m1["eventos"]] == [1, 2, 3, 4, 5]
            and m1["cursor"] == f"{A}:5", "04.inicio", str(m1["cursor"]))
    r.check(st2 == 200 and m2["continuidade"] == "continua" and m2["lacunas"] == []
            and [e["dados"]["i"] for e in m2["eventos"]] == [5, 6]
            and m2["cursor"] == f"{A}:7", "04.continua", str(m2["cursor"]))
    r.check(all(e["boot"] == A for e in m1["eventos"] + m2["eventos"]), "04.boot_em_cada_evento")


def test_05_reinicio_boot_A_para_B(r):
    reg_a = Registro(1000, 1 << 26, boot_id=A)
    prov = Provedor(reg_a)

    async def corpo(s, base, prov):
        for i in range(10):
            reg_a.emitir("t", {"i": i})
        m_a = _json((await _pedir(s, base, "/v1/eventos?limite=8"))[1])   # Brain: A:8
        reg_b = Registro(1000, 1 << 26, boot_id=B)                         # REINÍCIO
        reg_b.emitir("processo.iniciado")
        reg_b.emitir("processo.online")
        prov.reg = reg_b
        m_b = _json((await _pedir(s, base, f"/v1/eventos?cursor={m_a['cursor']}"))[1])
        reg_b.emitir("processo.vida")
        m_b2 = _json((await _pedir(s, base, f"/v1/eventos?cursor={m_b['cursor']}"))[1])
        return m_a, m_b, m_b2
    m_a, m_b, m_b2 = cenario(corpo, provedor=prov)
    r.check(m_a["cursor"] == f"{A}:8", "05.cursor_de_A", m_a["cursor"])
    r.check(m_b["continuidade"] == "nova_epoca" and m_b["boot_id"] == B,
            "05.detecta_novo_boot", str(m_b["continuidade"]))
    r.check(m_b["epoca_anterior"] == {"boot": A, "ultimo_seq_recebido": 8}
            and m_b["lacunas"] == [{"boot": A, "de": 9, "ate": None}],
            "05.declara_perda_do_fim_de_A", str(m_b["lacunas"]))
    r.check([(e["boot"], e["seq"]) for e in m_b["eventos"]] == [(B, 1), (B, 2)],
            "05.le_B_do_inicio", str([(e["boot"], e["seq"]) for e in m_b["eventos"]]))
    r.check(m_b2["continuidade"] == "continua" and [e["seq"] for e in m_b2["eventos"]] == [3],
            "05.segue_em_B")


def test_06_lacuna(r):
    prov = Provedor(Registro(5, 1 << 26, boot_id=A))

    async def corpo(s, base, prov):
        for i in range(12):
            prov.reg.emitir("t", {"i": i})
        return _json((await _pedir(s, base, f"/v1/eventos?cursor={A}:2"))[1])
    m = cenario(corpo, provedor=prov)
    r.check(m["continuidade"] == "lacuna"
            and m["lacunas"] == [{"boot": A, "de": 3, "ate": 7}]
            and [e["seq"] for e in m["eventos"]] == [8, 9, 10, 11, 12],
            "06.lacuna_declarada", str(m["lacunas"]))


def test_07_espera_longa(r):
    reg_a = Registro(1000, 1 << 26, boot_id=A)
    prov = Provedor(reg_a)

    async def corpo(s, base, prov):
        reg_a.emitir("t", {"i": 0})
        # (a) acorda com evento novo
        t0 = time.monotonic()
        tarefa = asyncio.create_task(_pedir(s, base, f"/v1/eventos?cursor={A}:1&espera=5"))
        await asyncio.sleep(0.2)
        reg_a.emitir("t", {"i": 1})
        st, c, _ = await tarefa
        acordou = (time.monotonic() - t0, _json(c))
        # (b) expira vazia
        t0 = time.monotonic()
        st, c, _ = await _pedir(s, base, f"/v1/eventos?cursor={A}:2&espera=0.3")
        expirou = (time.monotonic() - t0, _json(c))
        # (c) troca de época durante a espera responde na hora
        t0 = time.monotonic()
        tarefa = asyncio.create_task(_pedir(s, base, f"/v1/eventos?cursor={A}:2&espera=10"))
        await asyncio.sleep(0.2)
        reg_b = Registro(1000, 1 << 26, boot_id=B)
        reg_b.emitir("processo.iniciado")
        prov.reg = reg_b
        st, c, _ = await tarefa
        troca = (time.monotonic() - t0, _json(c))
        # (d) desligamento responde na hora
        prov.reg = reg_a
        tarefa = asyncio.create_task(_pedir(s, base, f"/v1/eventos?cursor={A}:2&espera=10"))
        await asyncio.sleep(0.2)
        t0 = time.monotonic()                  # instante do desligamento
        api_privada._parando = True            # o que encerrar() faz
        api_privada._acordar_esperas()
        try:
            st, c, _ = await tarefa
        finally:
            api_privada._parando = False
        parada = (time.monotonic() - t0, st)
        return acordou, expirou, troca, parada
    acordou, expirou, troca, parada = cenario(corpo, provedor=prov)
    r.check(acordou[0] < 1.5 and [e["seq"] for e in acordou[1]["eventos"]] == [2],
            "07.acorda_com_evento", f"{acordou[0]:.2f}s")
    r.check(0.3 <= expirou[0] < 2.0 and expirou[1]["eventos"] == []
            and expirou[1]["continuidade"] == "continua", "07.expira_vazia",
            f"{expirou[0]:.2f}s")
    r.check(troca[0] < 1.5 and troca[1]["continuidade"] == "nova_epoca",
            "07.troca_de_epoca_na_hora", f"{troca[0]:.2f}s")
    # sem o despertar, só no próximo passo de _PASSO_ESPERA_S (≥ ~0,3 s)
    r.check(parada[0] < 0.25 and parada[1] == 200, "07.desligamento_na_hora",
            f"{parada[0]:.3f}s depois do desligamento")
    r.check(not api_privada._esperas, "07.nenhuma_espera_pendurada",
            str(len(api_privada._esperas)))


def test_08_parametros_invalidos(r):
    async def corpo(s, base, prov):
        out = {}
        for nome, q in {"cursor": "cursor=nao-e-cursor", "cursor_vazio": "cursor=",
                        "limite_0": "limite=0", "limite_alto": "limite=1001",
                        "limite_texto": "limite=dez", "espera_alta": "espera=26",
                        "espera_texto": "espera=abc", "espera_negativa": "espera=-1"}.items():
            st, c, _ = await _pedir(s, base, f"/v1/eventos?{q}")
            out[nome] = (st, _json(c).get("erro"))
        return out
    for nome, (st, erro) in cenario(corpo).items():
        r.check(st == 400 and erro == "parametro_invalido", f"08.{nome}_400", str((st, erro)))


def test_09_so_get(r):
    async def corpo(s, base, prov):
        out = {}
        for m in ("POST", "PUT", "DELETE", "PATCH", "HEAD"):
            out[m] = (await _pedir(s, base, "/v1/eventos", metodo=m))[0]
        out["desconhecida"] = (await _pedir(s, base, "/v1/estado/post/1"))[0]
        out["desconhecida_sem_assinatura"] = (await _pedir(
            s, base, "/v1/estado/post/1", cabecalhos=False))[0]
        return out
    res = cenario(corpo)
    for m in ("POST", "PUT", "DELETE", "PATCH", "HEAD"):
        r.check(res[m] == 405, f"09.{m}_405", str(res[m]))
    r.check(res["desconhecida"] == 404, "09.rota_de_estado_nao_existe", str(res["desconhecida"]))
    r.check(res["desconhecida_sem_assinatura"] == 401, "09.autentica_antes_da_rota")


def test_10_sem_sqlite(r):
    chamadas = []
    original = sqlite3.connect

    def espiao(*a, **k):
        chamadas.append(a)
        return original(*a, **k)

    async def corpo(s, base, prov):
        for i in range(50):
            prov.reg.emitir("t", {"i": i})
        for cam in ("/v1/saude", "/v1/eventos", f"/v1/eventos?cursor={A}:10&limite=5",
                    f"/v1/eventos?cursor={B}:3", f"/v1/eventos?cursor={A}:50&espera=0.2"):
            await _pedir(s, base, cam)
    sqlite3.connect = espiao
    try:
        cenario(corpo)
    finally:
        sqlite3.connect = original
    r.check(chamadas == [], "10.zero_sqlite3_connect", str(chamadas[:3]))


def test_12_iniciar_recusa_configuracao(r):
    criados = []
    original = socket.socket

    def espiao(*a, **k):
        familia = _familia(a, k)
        if familia in (socket.AF_INET, socket.AF_INET6):    # o laço usa AF_UNIX
            criados.append(familia)
        return original(*a, **k)

    async def corpo():
        prov = Provedor(Registro(10, 1 << 20, boot_id=A))
        return {
            "segredo_curto": await api_privada.iniciar(9101, "curto", prov, porta_publica=8080),
            "segredo_vazio": await api_privada.iniciar(9101, "", prov, porta_publica=8080),
            "porta_publica": await api_privada.iniciar(8080, SEGREDO, prov, porta_publica=8080),
            "porta_baixa": await api_privada.iniciar(80, SEGREDO, prov, porta_publica=8080),
            "nao_configurada": await api_privada.iniciar(0, "", prov, porta_publica=8080),
            "proxy_tcp_publico": await api_privada.iniciar(
                9101, SEGREDO, prov, porta_publica=8080, porta_tcp_publica=9101),
        }
    socket.socket = espiao
    try:
        res = asyncio.run(corpo())
    finally:
        socket.socket = original
    r.check(all(v is False for v in res.values()), "12.todas_recusadas", str(res))
    r.check(criados == [], "12.nenhum_socket_aberto", str(criados))
    r.check(not api_privada.ativa(), "12.inativa")
    r.check(api_privada.motivo_desligada(0, "", 8080).startswith("não configurada")
            and api_privada.motivo_desligada(9101, SEGREDO, 8080) == ""
            and api_privada.motivo_desligada(9101, SEGREDO, 8080, 9102) == ""
            and "proxy TCP" in api_privada.motivo_desligada(9101, SEGREDO, 8080, 9101),
            "12.regua_unica")


class SocketFalso:
    """Socket de mentira: registra o que a escuta faz com ele. Não toca a
    rede — o teste prova a CONFIGURAÇÃO, não a pilha IP do host."""
    criados: list = []
    v6only_lido = None                       # None = devolve o que foi setado
    falhar_em = None                         # "bind" | "listen" | None

    def __init__(self, familia, tipo):
        self.family, self.type = familia, tipo
        self.opcoes, self.chamadas, self.fechado = {}, [], False
        SocketFalso.criados.append(self)

    def setsockopt(self, nivel, opcao, valor):
        self.opcoes[(nivel, opcao)] = valor

    def getsockopt(self, nivel, opcao):
        if (nivel, opcao) == (socket.IPPROTO_IPV6, socket.IPV6_V6ONLY) \
                and self.v6only_lido is not None:
            return self.v6only_lido
        return self.opcoes.get((nivel, opcao), 0)

    def bind(self, endereco):
        if self.falhar_em == "bind":
            raise OSError(98, "Address already in use")
        self.chamadas.append(("bind", endereco))

    def listen(self, n):
        if self.falhar_em == "listen":
            raise OSError(22, "Invalid argument")
        self.chamadas.append(("listen", n))

    def setblocking(self, b):
        self.chamadas.append(("setblocking", b))

    def close(self):
        self.fechado = True


def test_13_escuta_privada_dual_stack(r):
    SocketFalso.criados.clear()
    s = api_privada._socket_privado(9100, criar=SocketFalso)
    r.check(s.family == socket.AF_INET6 and s.type == socket.SOCK_STREAM,
            "13.af_inet6", str((s.family, s.type)))
    r.check(s.opcoes.get((socket.IPPROTO_IPV6, socket.IPV6_V6ONLY)) == 0,
            "13.ipv6_v6only_0_dual_stack", str(s.opcoes))
    r.check(s.opcoes.get((socket.SOL_SOCKET, socket.SO_REUSEADDR)) == 1, "13.reuseaddr")
    r.check(s.chamadas == [("bind", ("::", 9100)), ("listen", 128), ("setblocking", False)]
            and not s.fechado, "13.bind_em_dois_pontos_e_escuta", str(s.chamadas))
    r.check(api_privada._familias(s) == "ipv4+ipv6", "13.familias_ipv4_e_ipv6",
            api_privada._familias(s))

    class KernelSoIPv6(SocketFalso):
        v6only_lido = 1
    r.check(api_privada._familias(api_privada._socket_privado(9100, criar=KernelSoIPv6))
            == "ipv6", "13.familias_lidas_do_socket_nao_presumidas")

    for etapa in ("bind", "listen"):
        class Falha(SocketFalso):
            falhar_em = etapa
        try:
            api_privada._socket_privado(9100, criar=Falha)
            levantou = False
        except OSError:
            levantou = True
        r.check(levantou and SocketFalso.criados[-1].fechado,
                f"13.falha_no_{etapa}_fecha_e_levanta")
    r.check(all(x.family == socket.AF_INET6 for x in SocketFalso.criados),
            "13.nenhum_af_inet_pedido",
            str({x.family for x in SocketFalso.criados}))

    class FalhaBind(SocketFalso):
        falhar_em = "bind"

    async def corpo():
        prov = Provedor(Registro(10, 1 << 20, boot_id=A))
        ok = await api_privada.iniciar(
            9100, SEGREDO, prov, porta_publica=8080,
            fabrica_socket=lambda p: api_privada._socket_privado(p, criar=FalhaBind))
        return ok, api_privada.ativa(), prov.reg.emitir("depois")
    ok, ativa, seq = asyncio.run(corpo())
    r.check(ok is False and ativa is False and seq == 1,
            "13.sem_escuta_privada_fica_desligada_e_segue", str((ok, ativa, seq)))


def test_14_ipv6_sabotado(r):
    criados_v4 = []
    original = socket.socket

    def sem_ipv6(*a, **k):
        familia = _familia(a, k)
        if familia == socket.AF_INET6:
            raise OSError(97, "Address family not supported by protocol")
        if familia == socket.AF_INET:
            criados_v4.append(familia)
        return original(*a, **k)

    async def corpo():
        prov = Provedor(Registro(10, 1 << 20, boot_id=A))
        ok = await api_privada.iniciar(9103, SEGREDO, prov, porta_publica=8080)
        await api_privada.encerrar()                 # idempotente sem nada ligado
        # o worker segue: o laço continua e o registro emite normalmente
        await asyncio.sleep(0)
        return ok, api_privada.ativa(), prov.reg.emitir("depois")
    socket.socket = sem_ipv6
    try:
        ok, ativa, seq = asyncio.run(corpo())
    finally:
        socket.socket = original
    r.check(ok is False and ativa is False, "14.desligada", str((ok, ativa)))
    r.check(criados_v4 == [], "14.nenhum_af_inet_nada_de_0000", str(criados_v4))
    r.check(seq == 1, "14.worker_segue", str(seq))


def test_15_taxa(r):
    async def corpo(s, base, prov):
        estados = [(await _pedir(s, base, "/v1/saude"))[0] for _ in range(api_privada.RAJADA + 10)]
        st429, c429, h429 = await _pedir(s, base, "/v1/saude")
        await asyncio.sleep(0.35)                     # recarrega ~3 fichas
        depois = (await _pedir(s, base, "/v1/saude"))[0]
        return estados, (st429, _json(c429), h429.get("Retry-After")), depois

    estados, (st429, c429, retry), depois = cenario(corpo)
    ok = estados.count(200)
    r.check(api_privada.RAJADA <= ok <= api_privada.RAJADA + 8 and 429 in estados,
            "15.rajada_limitada", f"200={ok} 429={estados.count(429)}")
    r.check(st429 == 429 and c429 == {"erro": "excesso"} and retry == "1",
            "15.resposta_429", str((st429, c429, retry)))
    r.check(depois == 200, "15.recarrega", str(depois))

    async def sem_assinatura_nao_gasta(s, base, prov):
        for _ in range(api_privada.RAJADA * 3):
            await _pedir(s, base, "/v1/saude", cabecalhos=False)        # 401
        sts = [(await _pedir(s, base, "/v1/saude"))[0] for _ in range(api_privada.RAJADA)]
        return sts
    sts = cenario(sem_assinatura_nao_gasta)
    r.check(sts == [200] * api_privada.RAJADA, "15.sem_assinatura_nao_esgota_o_balde",
            str(sts.count(200)))


def test_16_esperas_limitadas(r):
    reg = Registro(1000, 1 << 26, boot_id=A)
    prov = Provedor(reg)

    async def corpo(s, base, prov):
        reg.emitir("t")
        t0 = time.monotonic()
        tarefas = [asyncio.create_task(_pedir(s, base, f"/v1/eventos?cursor={A}:1&espera=5"))
                   for _ in range(api_privada._MAX_ESPERAS + 2)]
        await asyncio.sleep(0.4)
        prontas = [t for t in tarefas if t.done()]
        saude = _json((await _pedir(s, base, "/v1/saude"))[1])
        reg.emitir("t")                                 # acorda quem espera
        resultados = await asyncio.gather(*tarefas)
        return len(prontas), saude, resultados, time.monotonic() - t0
    n_prontas, saude, resultados, dur = cenario(corpo, provedor=prov)
    r.check(n_prontas == 2, "16.acima_do_teto_responde_ja", str(n_prontas))
    r.check(saude["esperas_ativas"] == api_privada._MAX_ESPERAS, "16.teto_respeitado",
            str(saude["esperas_ativas"]))
    seqs = sorted(len(_json(c)["eventos"]) for st, c, _ in resultados)
    r.check(seqs == [0, 0] + [1] * api_privada._MAX_ESPERAS and dur < 2.0,
            "16.as_que_esperavam_acordam", f"{seqs} {dur:.2f}s")
    st, c, _ = cenario(lambda s, b, p: _pedir(s, b, "/v1/saude"), provedor=prov)
    r.check(_json(c)["esperas_ativas"] == 0, "16.contador_volta_a_zero")


def test_17_iniciar_encerrar_e_cancelamento(r):
    abertos = []

    def substituto(porta):                       # fábrica injetada (ver 13)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", porta))
        s.listen(16)
        s.setblocking(False)
        abertos.append(s)
        return s

    def porta_livre():
        t = socket.socket()
        t.bind(("127.0.0.1", 0))
        p = t.getsockname()[1]
        t.close()
        return p

    reg = Registro(100, 1 << 20, boot_id=A)
    reg.emitir("processo.iniciado")
    prov = Provedor(reg)

    async def ciclo():
        porta = porta_livre()
        info = {"ok": await api_privada.iniciar(porta, SEGREDO, prov, porta_publica=8080,
                                                fabrica_socket=substituto)}
        info["idempotente"] = await api_privada.iniciar(porta, SEGREDO, prov,
                                                        porta_publica=8080,
                                                        fabrica_socket=substituto)
        base = f"http://127.0.0.1:{porta}"
        async with aiohttp.ClientSession() as s:
            st, c, _ = await _pedir(s, base, "/v1/saude")
            info["saude"] = st
            info["rede"] = _json(c).get("rede")
            espera = asyncio.create_task(_pedir(s, base, f"/v1/eventos?cursor={A}:1&espera=20"))
            await asyncio.sleep(0.2)
            t0 = time.monotonic()                # instante do encerrar()
            await api_privada.encerrar()
            st, c, _ = await espera
            info["espera_no_encerrar"] = (st, time.monotonic() - t0)
        info["ativa"] = api_privada.ativa()
        await api_privada.encerrar()                 # idempotente
        # porta solta: a mesma API sobe de novo na mesma porta
        info["religa"] = await api_privada.iniciar(porta, SEGREDO, prov, porta_publica=8080,
                                                   fabrica_socket=substituto)
        await api_privada.encerrar()
        return info

    async def cancelado():
        porta = porta_livre()
        original_start = web.SockSite.start

        async def start_lento(self):
            await asyncio.sleep(30)

        web.SockSite.start = start_lento
        try:
            tarefa = asyncio.create_task(
                api_privada.iniciar(porta, SEGREDO, prov, porta_publica=8080,
                                    fabrica_socket=substituto))
            await asyncio.sleep(0.2)
            tarefa.cancel()
            res = await asyncio.gather(tarefa, return_exceptions=True)
        finally:
            web.SockSite.start = original_start
        return type(res[0]).__name__, abertos[-1].fileno(), api_privada.ativa()

    info = asyncio.run(ciclo())
    cancel = asyncio.run(cancelado())
    r.check(info["ok"] is True and info["idempotente"] is True and info["saude"] == 200,
            "17.serve", str(info))
    r.check(info["rede"] == {"porta": info["rede"]["porta"], "familias": "ipv4"}
            if info.get("rede") else False, "17.saude_traz_a_rede_lida_do_socket",
            str(info.get("rede")))
    r.check(info["espera_no_encerrar"][0] == 200 and info["espera_no_encerrar"][1] < 0.25,
            "17.encerrar_acorda_espera_na_hora",
            f"{info['espera_no_encerrar'][1]:.3f}s depois do encerrar()")
    r.check(info["ativa"] is False and info["religa"] is True and not api_privada.ativa(),
            "17.solta_a_porta", str(info))
    r.check(cancel == ("CancelledError", -1, False), "17.cancelamento_fecha_o_socket",
            str(cancel))


def test_18_isolamento_da_borda_publica(r):
    publicos = {
        "host_dominio_publico": {"Host": "www.leoind.com.br"},
        "host_dominio_publico_443": {"Host": "www.leoind.com.br:443"},
        "host_railway_publico": {"Host": "foguetao-production.up.railway.app"},
        "host_parecido": {"Host": "worker.railway.internal.evil.com"},
        "host_ip_publico": {"Host": "8.8.8.8"},
        "borda_x_railway_edge": {"X-Railway-Edge": "railway/us-east4"},
        "borda_x_railway_request_id": {"X-Railway-Request-Id": "abc"},
        "borda_x_real_ip": {"X-Real-IP": "200.1.2.3"},
        "borda_x_forwarded_for": {"X-Forwarded-For": "200.1.2.3"},
        "borda_x_forwarded_host": {"X-Forwarded-Host": "www.leoind.com.br"},
        "borda_x_forwarded_proto": {"X-Forwarded-Proto": "https"},
        "borda_x_request_start": {"X-Request-Start": "1700000000000"},
        "borda_forwarded": {"Forwarded": "for=200.1.2.3"},
        "privado_mas_pela_borda": {"Host": "worker.railway.internal:9100",
                                   "X-Real-IP": "200.1.2.3"},
    }
    privados = {
        "railway_internal": {"Host": "worker.railway.internal:9100"},
        "railway_internal_sem_porta": {"Host": "worker.railway.internal"},
        "localhost": {"Host": "localhost:9100"},
        "loopback_v4": {"Host": "127.0.0.1:9100"},
        "loopback_v6": {"Host": "[::1]:9100"},
        "privado_v6_railway": {"Host": "[fd12:3456::1]:9100"},
        "privado_v4": {"Host": "10.1.2.3:9100"},
    }

    async def corpo(s, base, prov):
        out = {}
        for nome, h in publicos.items():
            st, c, _ = await _pedir(s, base, "/v1/saude", extra=h)
            out[nome] = (st, _json(c).get("erro"))
        st, c, _ = await _pedir(s, base, "/v1/saude", cabecalhos=False,
                                extra={"Host": "www.leoind.com.br"})
        out["sem_assinatura_host_publico"] = (st, _json(c).get("erro"))
        for nome, h in privados.items():
            out[nome] = ((await _pedir(s, base, "/v1/saude", extra=h))[0], None)
        out["_saude"] = _json((await _pedir(s, base, "/v1/saude"))[1])
        return out
    res = cenario(corpo)
    for nome in publicos:
        r.check(res[nome] == (404, "nao_encontrado"), f"18.{nome}_404", str(res[nome]))
    r.check(res["sem_assinatura_host_publico"] == (404, "nao_encontrado"),
            "18.antes_da_assinatura", str(res["sem_assinatura_host_publico"]))
    for nome in privados:
        r.check(res[nome][0] == 200, f"18.{nome}_200", str(res[nome]))
    r.check(res["_saude"]["recusadas_rede"] == len(publicos) + 1
            and res["_saude"]["recusadas_assinatura"] == 0, "18.contadas_como_rede",
            str({k: res["_saude"][k] for k in ("recusadas_rede", "recusadas_assinatura")}))


def test_99_segredo_nunca_vaza(r):
    """Roda por último (ordem por nome): cobre as respostas de todos."""
    r.check(len(_CORPOS) > 100, "99.amostra", str(len(_CORPOS)))
    r.check(not any(SEGREDO.encode() in c for c in _CORPOS), "99.segredo_ausente")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "EVENTOS · API privada só leitura · F1.1"))
