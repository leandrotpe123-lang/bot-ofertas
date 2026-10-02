"""
SHOPEE — pool próprio com a conexão ociosa aberta 60 s (sessão comum:
15 s). Nenhuma requisição própria: sem toque, sem aquecimento.

Roda contra um servidor HTTP LOCAL de verdade (aiohttp): o servidor
registra a porta do cliente em cada requisição — mesma porta = mesma
conexão reaproveitada. Tempos em escala 1:100 quando preciso.

  01  portão: desligado, sem sessão do core ou fora do bot → sessão de
      sempre; ligado → pool próprio, com os parâmetros da sessão comum
      (exceto conexão aberta 60 s)
  02  o ganho: parado 30 s (escala), a sessão comum (15 s) abre conexão
      nova; o pool (60 s) reaproveita. Passado o prazo, abre de novo
  03  o pool não faz requisição nenhuma por conta própria
  04  encerramento EXPLÍCITO: o pool é registrado no core e fechado no
      passo 4 de main._encerrar — depois do drain, junto da sessão
      comum, antes do Telegram
  05  no encerramento não se cria pool
  06  pool que não pôde ser criado → sessão de sempre, sem insistir
  07  loop novo → pool novo
  08  afilia usa o pool na expansão e na API; desligado, a de sempre
  09  medição (SHP_PERF): conexões novas × reaproveitadas, api × loja
  10  fechar as sessões extras: falha numa não impede as outras

    python tests/test_shopee_conexao.py
"""
import ast
import asyncio
import os
import sys
import types

import aiohttp                                                  # real, antes do harness
from aiohttp import web
from yarl import URL

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_shopee_expansao import _limpar_memoria, PRODUTO       # noqa: E402
from test_cupom_sem_codigo import _zerar_banco                  # noqa: E402
from _harness_e5 import RAIZ, rodar                             # noqa: E402
import config                                                   # noqa: E402
import globals as g                                             # noqa: E402
from plataformas.contrato import Afiliacao                      # noqa: E402
from plataformas.shopee import afiliacao as shp                 # noqa: E402
from plataformas.shopee import conexao as cx                    # noqa: E402

REAIS = {k: getattr(cx, k) for k in ("_ABERTA_S", "_PERF")}


def _zerar(**escala):
    for k, v in REAIS.items():
        setattr(cx, k, v)
    for k, v in escala.items():
        setattr(cx, k, v)
    cx._estado.update(sessao=None, loop=None, indisponivel=False)
    cx._conta.clear()
    cx._tempo.clear()
    g._sessoes_extras.clear()
    g._encerrando = False
    os.environ["SHP_POOL_PROPRIO"] = "1"


class _Servidor:
    """Servidor local: registra (método, caminho, porta do cliente)."""

    def __init__(self):
        self.vistos = []

    async def __aenter__(self):
        async def tudo(request):
            peer = request.transport.get_extra_info("peername")
            self.vistos.append((request.method, request.path, peer[1] if peer else None))
            return web.Response(text="ok")
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", tudo)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        self.url = f"http://127.0.0.1:{self.site._server.sockets[0].getsockname()[1]}"
        return self

    async def __aexit__(self, *a):
        await self.runner.cleanup()


async def _get(sessao, url):
    async with sessao.get(url) as resp:
        await resp.read()


def _rodar(corpo):
    """Um processo: sessão comum do core + o corpo + o encerramento
    (passo 4: fecha a comum e as sessões próprias)."""
    async def _run():
        padrao = aiohttp.ClientSession()
        try:
            return await corpo(padrao)
        finally:
            await padrao.close()
            await g.fechar_sessoes_extras()
    return asyncio.run(_run())


# ══════════════════════════════════════════════════════════════════
def test_01_portao(r):
    _zerar()

    async def corpo(padrao):
        os.environ["SHP_POOL_PROPRIO"] = "0"
        desligado = await cx.sessao(padrao)
        os.environ["SHP_POOL_PROPRIO"] = "1"
        sem_core = await cx.sessao(None)
        outro = object()
        estranho = await cx.sessao(outro)
        pool = await cx.sessao(padrao)
        de_novo = await cx.sessao(padrao)
        con = pool.connector
        params = (con._keepalive_timeout, con.limit, con._ssl, pool.timeout.total,
                  pool.timeout.connect, pool.headers.get("User-Agent"))
        return desligado is padrao, sem_core, estranho is outro, pool, de_novo, padrao, params
    desligado_ok, sem_core, estranho_ok, pool, de_novo, padrao, params = _rodar(corpo)
    aberta, limite, ssl, total, connect, ua = params
    r.check(desligado_ok, "01.desligado_sessao_de_sempre")
    r.check(sem_core is None and estranho_ok, "01.fora_do_bot_intocado")
    r.check(pool is not padrao and pool is de_novo, "01.pool_proprio_unico")
    r.check(aberta == 60.0, "01.aberta_60s", str(aberta))
    r.check(limite == 50 and ssl is False, "01.mesmos_limites_da_comum")
    r.check(total == 40 and connect == 8, "01.mesmo_timeout")
    r.check(ua in config.USER_AGENTS, "01.mesmo_user_agent")
    r.check(pool.closed, "01.fechado_no_encerramento")


def test_02_ganho_parado_entre_15_e_60(r):
    # escala 1:100 — comum 15 s → 0,15 s; pool 60 s → 0,6 s; parado 30 s → 0,3 s
    _zerar(_ABERTA_S=0.6)

    async def corpo(padrao):
        comum = aiohttp.ClientSession(connector=aiohttp.TCPConnector(keepalive_timeout=0.15))
        try:
            async with _Servidor() as srv:
                pool = await cx.sessao(padrao)
                for s in (comum, pool):
                    await _get(s, srv.url + "/a")
                await asyncio.sleep(0.3)
                for s in (comum, pool):
                    await _get(s, srv.url + "/b")
                await asyncio.sleep(0.9)                   # passou do prazo do pool
                await _get(pool, srv.url + "/c")
                return srv.vistos
        finally:
            await comum.close()
    v = _rodar(corpo)
    comum_a, pool_a, comum_b, pool_b, pool_c = (x[2] for x in v)
    r.check(comum_a != comum_b, "02.comum_abre_de_novo_apos_30s", f"{comum_a} {comum_b}")
    r.check(pool_a == pool_b, "02.pool_reaproveita_apos_30s", f"{pool_a} {pool_b}")
    r.check(pool_c != pool_b, "02.pool_abre_de_novo_apos_60s", f"{pool_b} {pool_c}")


def test_03_sem_requisicao_propria(r):
    _zerar()

    async def corpo(padrao):
        async with _Servidor() as srv:
            await cx.sessao(padrao)
            await asyncio.sleep(0.5)
            antes = list(srv.vistos)
            return antes, [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    vistos, tarefas = _rodar(corpo)
    r.check(vistos == [], "03.nenhuma_requisicao", str(vistos))
    r.check(tarefas == [], "03.nenhuma_tarefa_de_fundo", str(tarefas))


def test_04_encerramento_explicito(r):
    _zerar()

    async def corpo(padrao):
        pool = await cx.sessao(padrao)
        registrado = pool in g._sessoes_extras
        aberto = not pool.closed
        n = await g.fechar_sessoes_extras()
        n2 = await g.fechar_sessoes_extras()
        return registrado, aberto, n, n2, pool.closed, list(g._sessoes_extras)
    registrado, aberto, n, n2, fechado, resto = _rodar(corpo)
    r.check(registrado and aberto, "04.registrado_no_core")
    r.check(n == 1 and fechado and resto == [] and n2 == 0, "04.fecha_uma_vez",
            f"{n} {fechado} {resto} {n2}")
    # main._encerrar: depois do drain, no passo 4 (junto da comum), antes do Telegram
    with open(os.path.join(RAIZ, "main.py"), encoding="utf-8") as f:
        arv = ast.parse(f.read())
    enc = next(n for n in ast.walk(arv)
               if isinstance(n, ast.AsyncFunctionDef) and n.name == "_encerrar")
    src = ast.unparse(enc)
    i_drain = src.find("if g._buf:")
    i_comum = src.find("await g._http_session.close()")
    i_extra = src.find("await g.fechar_sessoes_extras()")
    i_tg = src.find("await client.disconnect()")
    r.check(-1 not in (i_drain, i_comum, i_extra, i_tg)
            and i_drain < i_comum < i_extra < i_tg, "04.ordem_no_main",
            f"drain={i_drain} comum={i_comum} extra={i_extra} tg={i_tg}")


def test_05_nao_cria_no_encerramento(r):
    _zerar()

    async def corpo(padrao):
        g._encerrando = True
        try:
            return await cx.sessao(padrao) is padrao, list(g._sessoes_extras)
        finally:
            g._encerrando = False
    ok, extras = _rodar(corpo)
    r.check(ok and extras == [], "05.sessao_de_sempre_sem_pool", str(extras))


def test_06_criacao_falhou(r):
    _zerar()
    real = cx._criar
    tentativas = []

    def quebra():
        tentativas.append(1)
        raise OSError("sem pool")
    cx._criar = quebra
    try:
        async def corpo(padrao):
            a = await cx.sessao(padrao)
            b = await cx.sessao(padrao)
            return a is padrao and b is padrao
        ok = _rodar(corpo)
    finally:
        cx._criar = real
    r.check(ok and len(tentativas) == 1, "06.sessao_de_sempre_sem_insistir", str(tentativas))


def test_07_loop_novo_pool_novo(r):
    _zerar()

    async def corpo(padrao):
        return await cx.sessao(padrao)
    p1 = _rodar(corpo)
    p2 = _rodar(corpo)
    r.check(p1 is not p2 and p1.closed and p2.closed, "07.pool_por_loop")


def test_08_afilia_usa_o_pool(r):
    for ligado in ("1", "0"):
        _zerar()
        os.environ["SHP_POOL_PROPRIO"] = ligado
        recebidas = []
        reais = (shp.desencurtar, shp._chamar_servico_afiliados)

        async def desencurtar(url, sessao, depth=0):
            recebidas.append(("expansao", sessao))
            return PRODUTO

        async def servico(url, sessao):
            recebidas.append(("api", sessao))
            return "https://s.shopee.com.br/POOL1"

        async def corpo(padrao):
            g._init_globals()
            _limpar_memoria()
            _zerar_banco()
            config._SEM_HTTP = asyncio.Semaphore(20)
            shp.desencurtar, shp._chamar_servico_afiliados = desencurtar, servico
            try:
                res = await shp.afilia("https://s.shopee.com.br/Pool" + ligado + "x9", padrao)
            finally:
                shp.desencurtar, shp._chamar_servico_afiliados = reais
            return res, padrao, cx._estado["sessao"]
        res, padrao, pool = _rodar(corpo)
        usadas = {s for _, s in recebidas}
        r.check(isinstance(res, Afiliacao) and res.publicada == "https://s.shopee.com.br/POOL1",
                f"08.{ligado}.converteu", str(res))
        r.check([k for k, _ in recebidas] == ["expansao", "api"], f"08.{ligado}.passos",
                str(recebidas))
        if ligado == "1":
            r.check(usadas == {pool} and pool is not padrao, "08.expansao_e_api_no_pool",
                    str(recebidas))
        else:
            r.check(usadas == {padrao} and pool is None, "08.desligado_sessao_de_sempre",
                    str(recebidas))
    os.environ["SHP_POOL_PROPRIO"] = "1"


def test_09_medicao(r):
    _zerar(_PERF=True)

    async def corpo(padrao):
        async with _Servidor() as srv:
            pool = await cx.sessao(padrao)
            for _ in range(3):
                await _get(pool, srv.url + "/p")
        # host da API: classificado pelo nome (sem rede — chama os ganchos)
        tc = cx._rastreio()
        ctx = types.SimpleNamespace(trace_request_ctx=None)
        params = types.SimpleNamespace(url=URL("https://open-api.affiliate.shopee.com.br/graphql"))
        for f in tc.on_request_start:
            await f(None, ctx, params)
        for f in tc.on_connection_create_start:
            await f(None, ctx, None)
        for f in tc.on_connection_create_end:
            await f(None, ctx, None)
        return cx.resumo(), dict(cx._conta)
    texto, conta = _rodar(corpo)
    r.check(conta.get("loja_nova") == 1 and conta.get("loja_reuso") == 2,
            "09.loja_nova_e_reuso", str(conta))
    r.check(conta.get("api_nova") == 1, "09.api_pelo_host", str(conta))
    r.check("loja nova=1 reuso=2" in texto and "api nova=1 reuso=0" in texto, "09.resumo", texto)
    _zerar()
    r.check(cx.resumo() == "", "09.sem_dados_sem_texto")


def test_10_falha_ao_fechar_uma_nao_para_as_outras(r):
    _zerar()

    class _Quebrada:
        closed = False

        async def close(self):
            raise RuntimeError("x")

    async def corpo(padrao):
        pool = await cx.sessao(padrao)
        g.registrar_sessao_extra(_Quebrada())
        n = await g.fechar_sessoes_extras()
        return n, pool.closed, list(g._sessoes_extras)
    n, fechado, resto = _rodar(corpo)
    r.check(n == 1 and fechado and resto == [], "10.fecha_as_demais", f"{n} {fechado} {resto}")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "SHOPEE · pool próprio (conexão aberta 60 s)"))
