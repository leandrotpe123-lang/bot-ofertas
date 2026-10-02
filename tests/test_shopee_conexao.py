"""
SHOPEE — conexão sempre aberta: pool próprio; a conexão com a API de
afiliados fica viva (toque leve); a loja só ganha conexão aberta por
mais tempo, sem toque nenhum.

Roda contra um servidor HTTP LOCAL de verdade (aiohttp): o servidor
registra a porta do cliente em cada requisição — mesma porta = mesma
conexão reaproveitada. Tempos em escala (décimos de segundo).

  01  portão: desligado, sem sessão do core ou fora do bot → sessão de
      sempre; ligado → pool próprio, com os parâmetros da sessão comum
      (exceto conexão aberta 60 s)
  02  o toque mantém a MESMA conexão, e a chamada real reaproveita ela
  03  uso real recente → não toca
  04  o toque vai só à raiz da API: HEAD "/", nada de loja/encurtador;
      afilia pede o aquecimento da API de afiliados
  05  403/429 → pausa que dobra; resposta normal volta ao ritmo
  06  3 falhas de rede seguidas → pausa
  07  defeito no toque → o toque desliga, o pool continua servindo
  08  encerramento: para de tocar sem fechar o pool; fim do processo
      fecha o pool
  09  pool que não pôde ser criado → sessão de sempre, sem nova tentativa
  10  loop novo → pool novo (nada de pool de loop morto)
  11  afilia usa o pool na expansão e na API; desligado, a de sempre
  12  medição (SHP_PERF): conexões novas × reaproveitadas, sem toque

    python tests/test_shopee_conexao.py
"""
import asyncio
import os
import sys
import time

import aiohttp                                                  # real, antes do harness
from aiohttp import web

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_shopee_expansao import (_Rede, _com_rede, _limpar_memoria,  # noqa: E402
                                  PRODUTO)
from test_cupom_sem_codigo import _zerar_banco                  # noqa: E402
from _harness_e5 import rodar                                   # noqa: E402
import config                                                   # noqa: E402
import globals as g                                             # noqa: E402
from plataformas.contrato import Afiliacao                      # noqa: E402
from plataformas.shopee import afiliacao as shp                 # noqa: E402
from plataformas.shopee import conexao as cx                    # noqa: E402

REAIS = {k: getattr(cx, k) for k in ("_TOQUE_S", "_PAUSA_BASE_S", "_PAUSA_MAX_S",
                                     "_FALHAS_PARA_PAUSAR", "_TIMEOUT_TOQUE_S", "_PERF")}


def _zerar(**escala):
    for k, v in REAIS.items():
        setattr(cx, k, v)
    for k, v in escala.items():
        setattr(cx, k, v)
    cx._estado.update(sessao=None, loop=None, tarefa=None, ultimo_uso=0.0,
                      indisponivel=False)
    cx._conta.clear()
    cx._tempo.clear()
    g._encerrando = False
    os.environ["SHP_CONEXAO_QUENTE"] = "1"


class _Servidor:
    """Servidor local: registra (método, caminho, porta do cliente)."""

    def __init__(self, status=200):
        self.status = status
        self.vistos = []

    async def __aenter__(self):
        async def tudo(request):
            peer = request.transport.get_extra_info("peername")
            self.vistos.append((request.method, request.path, peer[1] if peer else None))
            if self.status == "derruba":
                request.transport.close()
                return web.Response(status=200)
            return web.Response(status=self.status, text="ok")
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", tudo)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        self.porta = self.site._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{self.porta}"
        return self

    async def __aexit__(self, *a):
        await self.runner.cleanup()

    def de(self, metodo):
        return [v for v in self.vistos if v[0] == metodo]


def _rodar(corpo):
    """asyncio.run com o fim de processo de verdade: tarefas pendentes
    são canceladas (é o que fecha o pool em produção)."""
    async def _run():
        padrao = aiohttp.ClientSession()
        try:
            return await corpo(padrao)
        finally:
            await padrao.close()
    return asyncio.run(_run())


# ══════════════════════════════════════════════════════════════════
def test_01_portao(r):
    _zerar()

    async def corpo(padrao):
        os.environ["SHP_CONEXAO_QUENTE"] = "0"
        desligado = await cx.sessao(padrao, aquecer="")
        os.environ["SHP_CONEXAO_QUENTE"] = "1"
        sem_core = await cx.sessao(None, aquecer="")
        outro = object()
        estranho = await cx.sessao(outro, aquecer="")
        pool = await cx.sessao(padrao, aquecer="")
        de_novo = await cx.sessao(padrao, aquecer="")
        con = pool.connector
        params = (con._keepalive_timeout, con.limit, con._ssl, pool.timeout.total,
                  pool.timeout.connect, pool.headers.get("User-Agent"))
        return desligado, sem_core, estranho is outro, pool, de_novo, padrao, params
    desligado, sem_core, estranho_ok, pool, de_novo, padrao, params = _rodar(corpo)
    aberta, limite, ssl, total, connect, ua = params
    r.check(desligado is padrao, "01.desligado_sessao_de_sempre")
    r.check(sem_core is None and estranho_ok, "01.fora_do_bot_intocado")
    r.check(pool is not padrao and pool is de_novo, "01.pool_proprio_unico")
    r.check(aberta == 60.0, "01.aberta_60s", str(aberta))
    r.check(limite == 50 and ssl is False, "01.mesmos_limites_da_comum")
    r.check(total == 40 and connect == 8, "01.mesmo_timeout")
    r.check(ua in config.USER_AGENTS, "01.mesmo_user_agent")
    r.check(pool.closed, "01.fechado_no_fim_do_processo")


def test_02_toque_mantem_a_mesma_conexao(r):
    _zerar(_TOQUE_S=0.1)

    async def corpo(padrao):
        async with _Servidor() as srv:
            pool = await cx.sessao(padrao, aquecer=srv.url + "/graphql")
            await asyncio.sleep(0.6)
            n = len(srv.vistos)
            limite = time.monotonic() + 2.0
            while len(srv.vistos) == n and time.monotonic() < limite:   # logo depois de um toque...
                await asyncio.sleep(0.005)
            await asyncio.sleep(0.02)                # ...com a resposta já de volta
            async with pool.post(srv.url + "/graphql", data="{}") as resp:
                await resp.read()
            return srv.vistos
    vistos = _rodar(corpo)
    heads = [v for v in vistos if v[0] == "HEAD"]
    posts = [v for v in vistos if v[0] == "POST"]
    portas = {v[2] for v in vistos}
    r.check(len(heads) >= 4, "02.tocou_no_ritmo", str(len(heads)))
    r.check(len(portas) == 1, "02.uma_conexao_so", str(portas))
    r.check(posts and posts[0][2] in {h[2] for h in heads}, "02.chamada_real_reaproveita",
            f"{posts} {heads[:1]}")


def test_03_uso_recente_nao_toca(r):
    _zerar(_TOQUE_S=0.1)

    async def corpo(padrao):
        async with _Servidor() as srv:
            cx.usada()
            await cx.sessao(padrao, aquecer=srv.url + "/graphql")
            for _ in range(8):
                cx.usada()
                await asyncio.sleep(0.06)
            return srv.de("HEAD")
    heads = _rodar(corpo)
    r.check(heads == [], "03.sem_toque_com_uso_real", str(heads))


def test_04_so_a_raiz_da_api(r):
    _zerar(_TOQUE_S=0.1)

    async def corpo(padrao):
        async with _Servidor() as srv:
            await cx.sessao(padrao, aquecer=srv.url + "/graphql")
            await asyncio.sleep(0.35)
            return srv.vistos
    vistos = _rodar(corpo)
    r.check(vistos and all(m == "HEAD" and p == "/" for m, p, _ in vistos),
            "04.so_HEAD_na_raiz", str(vistos))
    # afilia pede o aquecimento da API de afiliados — e de nada mais
    pedidos = []
    real = cx.sessao

    async def espia(padrao, aquecer=""):
        pedidos.append(aquecer)
        return padrao
    cx.sessao = espia
    try:
        rede = _Rede(lambda u: PRODUTO, api=lambda u: "https://s.shopee.com.br/X1")
        _com_rede(rede, lambda: shp.afilia("https://s.shopee.com.br/AAGNE4pfS9", None))
        _com_rede(_Rede(lambda u: u), lambda: shp.afilia("https://s.shopee.com.br/1qU7Zs67MB", None))
    finally:
        cx.sessao = real
    r.check(pedidos and set(pedidos) == {shp._ENDPOINT_AFILIADOS}, "04.aquece_so_a_api",
            str(pedidos))
    r.check(shp._ENDPOINT_AFILIADOS.startswith("https://open-api.affiliate.shopee.com.br/"),
            "04.host_da_api")


def test_05_pede_calma_pausa(r):
    for status in (429, 403):
        _zerar(_TOQUE_S=0.05, _PAUSA_BASE_S=0.4)

        async def corpo(padrao):
            async with _Servidor(status=status) as srv:
                await cx.sessao(padrao, aquecer=srv.url + "/graphql")
                await asyncio.sleep(0.3)
                durante = len(srv.de("HEAD"))
                srv.status = 200
                await asyncio.sleep(0.5)
                return durante, len(srv.de("HEAD")), dict(cx._conta)
        durante, depois, conta = _rodar(corpo)
        r.check(durante == 1 and conta.get("pausa") == 1, f"05.{status}.pausou",
                f"{durante} {conta}")
        r.check(depois >= 3, f"05.{status}.voltou_ao_ritmo", str(depois))
    # a pausa dobra e tem teto
    _zerar(_TOQUE_S=0.02, _PAUSA_BASE_S=0.05, _PAUSA_MAX_S=0.15)

    async def corpo2(padrao):
        async with _Servidor(status=429) as srv:
            await cx.sessao(padrao, aquecer=srv.url + "/graphql")
            await asyncio.sleep(1.0)
            ts = [t for t in srv.vistos]
            return len(ts)
    n = _rodar(corpo2)
    # toques em 0; ,05; ,15; ,30; ,45; ,60; ,75; ,90 → 8 em 1 s. Sem teto:
    # 0; ,05; ,15; ,35; ,75 → 5. Sem dobrar (0,05 fixo): ~20.
    r.check(7 <= n <= 9, "05.dobra_com_teto", str(n))


def test_06_falhas_de_rede_pausam(r):
    _zerar(_TOQUE_S=0.03, _PAUSA_BASE_S=5.0)

    async def corpo(padrao):
        async with _Servidor(status="derruba") as srv:
            pool = await cx.sessao(padrao, aquecer=srv.url + "/graphql")
            tentativas = []
            original = pool.head                     # o toque ainda não rodou

            def conta_head(*a, **k):
                tentativas.append(time.monotonic())
                return original(*a, **k)
            pool.head = conta_head
            await asyncio.sleep(0.6)
            return len(tentativas), dict(cx._conta)
    n, conta = _rodar(corpo)
    # (o servidor vê até 2 requisições por tentativa: o aiohttp repete
    # sozinho, uma vez, HEAD/GET em conexão derrubada pelo servidor)
    r.check(n == 3 and conta.get("pausa") == 1, "06.tres_falhas_e_pausa", f"{n} {conta}")


def test_07_defeito_desliga_so_o_toque(r):
    _zerar(_TOQUE_S=0.05)

    async def corpo(padrao):
        async with _Servidor() as srv:
            original = aiohttp.ClientSession.head
            chamadas = []

            def defeito(self, *a, **k):
                chamadas.append(1)
                raise RuntimeError("defeito")
            aiohttp.ClientSession.head = defeito
            try:
                pool = await cx.sessao(padrao, aquecer=srv.url + "/graphql")
                await asyncio.sleep(0.3)
            finally:
                aiohttp.ClientSession.head = original
            tarefa = cx._estado["tarefa"]
            async with pool.get(srv.url + "/x") as resp:
                ok = resp.status
            return (pool.closed, tarefa.done(), ok, await cx.sessao(padrao, aquecer="") is pool,
                    len(srv.de("HEAD")) + len(chamadas) - 1)
    fechado, terminou, ok, mesmo, heads = _rodar(corpo)
    r.check(not fechado and ok == 200 and mesmo, "07.pool_segue_servindo",
            f"{fechado} {ok} {mesmo}")
    # uma tentativa com defeito e nenhuma depois (defeito não vira "falha de rede")
    r.check(not terminou and heads == 0, "07.toque_parado_sem_derrubar", f"{terminou} {heads}")


def test_08_encerramento(r):
    _zerar(_TOQUE_S=0.05)

    async def corpo(padrao):
        async with _Servidor() as srv:
            pool = await cx.sessao(padrao, aquecer=srv.url + "/graphql")
            await asyncio.sleep(0.2)
            g._encerrando = True
            await asyncio.sleep(0.06)                 # toque em voo termina
            antes = len(srv.de("HEAD"))
            await asyncio.sleep(0.3)
            durante = len(srv.de("HEAD"))
            async with pool.get(srv.url + "/drain") as resp:    # o drain ainda converte
                drain = resp.status
            novo = await cx.sessao(padrao, aquecer="")
            tarefa = cx._estado["tarefa"]
            tarefa.cancel()                                     # fim do processo
            await asyncio.gather(tarefa, return_exceptions=True)
            return antes, durante, drain, pool.closed, novo is pool
    antes, durante, drain, fechado, mesmo = _rodar(corpo)
    g._encerrando = False
    r.check(antes >= 1 and durante == antes, "08.para_de_tocar", f"{antes} {durante}")
    r.check(drain == 200 and mesmo, "08.pool_aberto_no_drain")
    r.check(fechado, "08.fim_do_processo_fecha")
    # sem pool ainda e já encerrando: não cria
    _zerar()

    async def corpo2(padrao):
        g._encerrando = True
        return await cx.sessao(padrao, aquecer="http://127.0.0.1:9/") is padrao
    r.check(_rodar(corpo2), "08.nao_cria_no_encerramento")
    g._encerrando = False


def test_09_criacao_falhou(r):
    _zerar()
    real = cx._criar
    tentativas = []

    def quebra():
        tentativas.append(1)
        raise OSError("sem pool")
    cx._criar = quebra
    try:
        async def corpo(padrao):
            a = await cx.sessao(padrao, aquecer="")
            b = await cx.sessao(padrao, aquecer="")
            return a is padrao and b is padrao
        ok = _rodar(corpo)
    finally:
        cx._criar = real
    r.check(ok and len(tentativas) == 1, "09.sessao_de_sempre_sem_insistir", str(tentativas))


def test_10_loop_novo_pool_novo(r):
    _zerar()

    async def corpo(padrao):
        return await cx.sessao(padrao, aquecer="")
    p1 = _rodar(corpo)
    p2 = _rodar(corpo)
    r.check(p1 is not p2 and p1.closed and p2.closed, "10.pool_por_loop")


def test_11_afilia_usa_o_pool(r):
    for ligado in ("1", "0"):
        _zerar()
        os.environ["SHP_CONEXAO_QUENTE"] = ligado
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
            endpoint = shp._ENDPOINT_AFILIADOS
            async with _Servidor() as srv:            # o toque vai ao servidor local
                shp._ENDPOINT_AFILIADOS = srv.url + "/graphql"
                shp.desencurtar, shp._chamar_servico_afiliados = desencurtar, servico
                try:
                    res = await shp.afilia("https://s.shopee.com.br/Pool" + ligado + "x9", padrao)
                finally:
                    shp.desencurtar, shp._chamar_servico_afiliados = reais
                    shp._ENDPOINT_AFILIADOS = endpoint
            return res, padrao, cx._estado["sessao"]
        res, padrao, pool = _rodar(corpo)
        usadas = {s for _, s in recebidas}
        r.check(isinstance(res, Afiliacao) and res.publicada == "https://s.shopee.com.br/POOL1",
                f"11.{ligado}.converteu", str(res))
        if ligado == "1":
            r.check(usadas == {pool} and pool is not padrao, "11.expansao_e_api_no_pool",
                    str(recebidas))
        else:
            r.check(usadas == {padrao} and pool is None, "11.desligado_sessao_de_sempre",
                    str(recebidas))
    os.environ["SHP_CONEXAO_QUENTE"] = "1"


def test_12_medicao(r):
    _zerar(_TOQUE_S=10.0, _PERF=True)                   # só o 1º toque, imediato

    async def corpo(padrao):
        async with _Servidor() as srv:
            pool = await cx.sessao(padrao, aquecer=srv.url + "/graphql")
            await asyncio.sleep(0.2)
            for _ in range(3):
                async with pool.get(srv.url + "/p") as resp:
                    await resp.read()
            return cx.resumo(), dict(cx._conta)
    texto, conta = _rodar(corpo)
    r.check(conta.get("loja_reuso", 0) == 3 and conta.get("loja_nova", 0) == 0,
            "12.tres_reusos_da_conexao_quente", str(conta))
    r.check(conta.get("toque", 0) >= 1 and "toques=" in texto, "12.toques_contados", texto)
    r.check("loja nova=0 reuso=3" in texto, "12.resumo", texto)
    _zerar()
    r.check(cx.resumo() == "", "12.sem_dados_sem_texto")


def test_13_chamada_real_marca_uso(r):
    _zerar()
    reais = (shp._ENDPOINT_AFILIADOS, shp._TIMEOUTS_AFILIACAO, shp._ESPERA_ENTRE_TENTATIVAS)

    async def corpo(padrao):
        config._SEM_HTTP = asyncio.Semaphore(20)
        async with _Servidor() as srv:
            shp._ENDPOINT_AFILIADOS = srv.url + "/graphql"
            shp._TIMEOUTS_AFILIACAO, shp._ESPERA_ENTRE_TENTATIVAS = (0.5,), 0.01
            try:
                antes = cx._estado["ultimo_uso"]
                await shp._chamar_servico_afiliados("https://shopee.com.br/x", padrao)
                return antes, cx._estado["ultimo_uso"]
            finally:
                (shp._ENDPOINT_AFILIADOS, shp._TIMEOUTS_AFILIACAO,
                 shp._ESPERA_ENTRE_TENTATIVAS) = reais
    antes, depois = _rodar(corpo)
    r.check(antes == 0.0 and depois > time.monotonic() - 5, "13.api_marca_uso",
            f"{antes} {depois}")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "SHOPEE · conexão sempre aberta"))
