"""
F1.1 — FIAÇÃO da proveniência no worker (main.py, config.py) e fronteiras
do pacote eventos/.

Prova que a F1.1 não tem efeito funcional no pipeline e que, sem as duas
variáveis válidas, nada liga:
  01  config: variáveis ausentes, inválidas ou fora da faixa → padrão,
      sem derrubar o import (subprocesso com ambiente controlado)
  02  sem configuração (o estado da produção após o merge): motivo
      "não configurada" → nada é instalado; emitir() é no-op barato
  03  main._preparar_processo: instala SÓ quando a régua única deixa,
      depois do banco; processo.iniciado só se instalou; a task da API
      nasce UMA vez, logo depois do servidor web, e só com o registro
  04  main._encerrar: a task da API só é cancelada depois do drain; a API
      fecha depois do web e ANTES do banco
  05  main._on_sinal / _run: processo.encerrando e processo.online nos
      pontos certos; _log_lifecycle intocado; nenhum `await` em emitir
  06  _ligar_api_privada (real): API não abre → proveniência desligada;
      exceção contida; ligada → processo.vida periódico; falha no retrato
      não mata o laço
  07  fronteiras: eventos/ não importa banco, sqlite3, telethon, globals,
      client, pipeline nem config; só main.py importa eventos (nenhum
      módulo do pipeline foi tocado)
  08  a API só tem GET /v1/saude e GET /v1/eventos; o app PÚBLICO
      (web/redirect.py, porta PORT) não tem nada de /v1 nem importa eventos
  09  o segredo nunca entra em log: nenhuma chamada de log recebe o segredo

    python tests/test_eventos_main.py
"""
from __future__ import annotations

import ast
import asyncio
import json
import os
import subprocess
import sys
import time

# Hermético: as variáveis da F1.1 (e PORT) nunca vêm do shell de quem roda.
for _v in ("API_PRIVADA_PORTA", "API_PRIVADA_SEGREDO", "EVENTOS_ANEL_MAX",
           "EVENTOS_ANEL_MAX_BYTES", "PORT", "RAILWAY_TCP_APPLICATION_PORT"):
    os.environ.pop(_v, None)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import telethon  # noqa: E402  — REAL, antes do harness (main importa o client)
import aiohttp   # noqa: E402,F401  — REAL, antes do harness (rotas reais)

from _harness_e5 import RAIZ, preparar, rodar  # noqa: E402
preparar()

import config                                   # noqa: E402
import eventos                                  # noqa: E402
from eventos import api_privada                 # noqa: E402

_MAIN = None


def _main():
    global _MAIN
    if _MAIN is None:
        import main as _m
        _MAIN = _m
    return _MAIN


def _arvore(rel):
    with open(os.path.join(RAIZ, rel), encoding="utf-8") as f:
        return ast.parse(f.read())


ARV = _arvore("main.py")


def _funcao(nome):
    return next(n for n in ast.walk(ARV)
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                and n.name == nome)


def _indice(corpo, predicado):
    """Índice do PRIMEIRO comando de topo do corpo que contém um nó que
    satisfaz `predicado` (em qualquer profundidade); -1 se nenhum."""
    for i, stmt in enumerate(corpo):
        if any(predicado(n) for n in ast.walk(stmt)):
            return i
    return -1


def _chamada(texto):
    """Predicado: chamada cuja função é exatamente `texto` (ex.: 'x.y')."""
    return lambda n: isinstance(n, ast.Call) and ast.unparse(n.func) == texto


def _contem(no, predicado):
    return any(predicado(n) for n in ast.walk(no))


# ══════════════════════════════════════════════════════════════════
def test_01_config_nunca_derruba_e_cai_no_padrao(r):
    codigo = ("import json, config; print(json.dumps([config._PORTA_PUBLICA, "
              "config.API_PRIVADA_PORTA, config.API_PRIVADA_SEGREDO == '', "
              "config.EVENTOS_ANEL_MAX, config.EVENTOS_ANEL_MAX_BYTES, "
              "config._PORTA_TCP_PUBLICA]))")

    def rodar_com(**amb):
        env = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": RAIZ,
               "PYTHONIOENCODING": "utf-8", **amb}
        p = subprocess.run([sys.executable, "-c", codigo], cwd=RAIZ, env=env,
                           capture_output=True, text=True, timeout=60)
        return p.returncode, (json.loads(p.stdout.strip().splitlines()[-1])
                              if p.returncode == 0 else p.stderr[-300:])

    padrao = [8080, 0, True, 20_000, 32 << 20, 0]
    casos = {
        "ausentes": ({}, padrao),
        "invalidos": ({"PORT": "abc", "API_PRIVADA_PORTA": "porta",
                       "EVENTOS_ANEL_MAX": "-5", "EVENTOS_ANEL_MAX_BYTES": "9" * 40,
                       "RAILWAY_TCP_APPLICATION_PORT": "x"}, padrao),
        "vazios": ({"PORT": " ", "API_PRIVADA_PORTA": "", "EVENTOS_ANEL_MAX": "",
                    "EVENTOS_ANEL_MAX_BYTES": ""}, padrao),
        "fora_da_faixa": ({"PORT": "70000", "API_PRIVADA_PORTA": "80",
                           "EVENTOS_ANEL_MAX": "999", "EVENTOS_ANEL_MAX_BYTES": "1"},
                          padrao),
        "porta_acima": ({"API_PRIVADA_PORTA": "65536"}, padrao),
        "validos": ({"PORT": "8081", "API_PRIVADA_PORTA": "9100",
                     "EVENTOS_ANEL_MAX": "5000", "EVENTOS_ANEL_MAX_BYTES": str(8 << 20),
                     "RAILWAY_TCP_APPLICATION_PORT": "5432"},
                    [8081, 9100, True, 5000, 8 << 20, 5432]),
    }
    for nome, (amb, esperado) in casos.items():
        rc, valores = rodar_com(**amb)
        r.check(rc == 0 and valores == esperado, f"01.{nome}", str((rc, valores)))
    r.check(config._int_env.__module__ == "config", "01.funcao_em_config")


def test_02_sem_configuracao_nada_liga(r):
    motivo = api_privada.motivo_desligada(config.API_PRIVADA_PORTA,
                                          config.API_PRIVADA_SEGREDO,
                                          config._PORTA_PUBLICA,
                                          config._PORTA_TCP_PUBLICA)
    r.check(motivo.startswith("não configurada"), "02.motivo_nao_configurada", motivo)
    r.check(config.API_PRIVADA_PORTA == 0 and config.API_PRIVADA_SEGREDO == "",
            "02.padroes_desligados")
    eventos.desligar()
    n = 200_000
    t0 = time.perf_counter()
    for _ in range(n):
        eventos.emitir("x", {"a": 1})
    ns = (time.perf_counter() - t0) / n * 1e9
    r.check(eventos.registro_atual() is None and eventos.emitir("x") == 0,
            "02.emitir_noop")
    r.check(ns < 5_000, "02.noop_barato", f"{ns:.0f} ns/chamada")
    print(f"  [custo] emitir() desligado: {ns:.0f} ns/chamada")


def test_03_preparar_processo(r):
    prep = _funcao("_preparar_processo")
    corpo = prep.body
    i_db = _indice(corpo, _chamada("_init_db"))
    i_motivo = _indice(corpo, _chamada("api_privada.motivo_desligada"))
    i_inst = _indice(corpo, _chamada("eventos.instalar"))
    i_conn = _indice(corpo, _chamada("client.connect"))
    r.check(0 <= i_db < i_motivo <= i_inst < i_conn,
            "03.depois_do_banco_antes_do_telegram", str((i_db, i_motivo, i_inst, i_conn)))
    se = corpo[i_inst]
    r.check(isinstance(se, ast.If) and ast.unparse(se.test) == "_motivo"
            and not _contem(ast.Module(body=se.body, type_ignores=[]),
                            _chamada("eventos.instalar")),
            "03.instala_so_sem_motivo", ast.unparse(se.test) if isinstance(se, ast.If) else "")
    senao = se.orelse[0] if isinstance(se, ast.If) and se.orelse else None
    r.check(isinstance(senao, ast.If)
            and ast.unparse(senao.test).startswith("eventos.instalar(")
            and ast.unparse(senao.test).endswith("is None")
            and _contem(ast.Module(body=senao.orelse, type_ignores=[]),
                        lambda n: isinstance(n, ast.Call)
                        and ast.unparse(n.func) == "eventos.emitir"
                        and ast.unparse(n.args[0]) == "'processo.iniciado'"),
            "03.iniciado_so_se_instalou")
    r.check(sum(1 for n in ast.walk(ARV) if _chamada("eventos.instalar")(n)) == 1,
            "03.instalar_uma_vez")
    i_web = _indice(corpo, lambda n: isinstance(n, ast.Assign)
                    and ast.unparse(n.targets[0]) == "_TASKS_FUNDO['web']")
    i_api = _indice(corpo, lambda n: isinstance(n, ast.Assign)
                    and ast.unparse(n.targets[0]) == "_TASKS_FUNDO['api_privada']")
    r.check(i_web >= 0 and i_api == i_web + 1, "03.task_logo_depois_do_web",
            str((i_web, i_api)))
    bloco = corpo[i_api]
    r.check(isinstance(bloco, ast.If)
            and ast.unparse(bloco.test) == "eventos.registro_atual() is not None"
            and [ast.unparse(s) for s in bloco.body]
            == ["_TASKS_FUNDO['api_privada'] = asyncio.create_task(_ligar_api_privada())"],
            "03.task_so_com_registro")
    r.check(sum(1 for n in ast.walk(ARV) if _chamada("_ligar_api_privada")(n)) == 1,
            "03.task_uma_vez_por_processo")


def test_04_encerrar(r):
    corpo = _funcao("_encerrar").body
    i_aborta = _indice(corpo, lambda n: isinstance(n, ast.If)
                       and ast.unparse(n.test) == "g._buf"
                       and any(isinstance(s, ast.Return) for s in n.body))
    i_task = _indice(corpo, lambda n: isinstance(n, ast.Call)
                     and ast.unparse(n) == "_TASKS_FUNDO.get('api_privada')")
    i_web = _indice(corpo, _chamada("_encerrar_servidor_web"))
    i_api = _indice(corpo, _chamada("api_privada.encerrar"))
    i_db = _indice(corpo, _chamada("_fechar_db"))
    r.check(0 <= i_aborta < i_task, "04.task_so_depois_do_drain", str((i_aborta, i_task)))
    r.check(0 <= i_web < i_api < i_db, "04.api_depois_do_web_antes_do_banco",
            str((i_web, i_api, i_db)))
    r.check(isinstance(corpo[i_api], ast.Try), "04.encerrar_api_contido_em_try")


def test_05_sinal_online_lifecycle_e_sem_await(r):
    corpo = [ast.unparse(s) for s in _funcao("_on_sinal").body]
    i_flag = corpo.index("g._encerrando = True")
    i_ev = next(i for i, s in enumerate(corpo) if s.startswith("eventos.emitir('processo.encerrando'"))
    i_task = next(i for i, s in enumerate(corpo) if s.startswith("_TAREFA_SHUTDOWN = "))
    r.check(i_flag < i_ev < i_task, "05.encerrando_depois_da_flag_antes_do_teardown",
            str((i_flag, i_ev, i_task)))
    corpo = [ast.unparse(s) for s in _funcao("_run").body]
    i_on = corpo.index("log_sys.info('🚀 FOGUETÃO — ONLINE')")
    r.check(corpo[i_on + 1] == "eventos.emitir('processo.online', {'ciclo': _CICLO})",
            "05.online_logo_depois_do_marco", corpo[i_on + 1])
    lc = _funcao("_log_lifecycle")
    r.check(not _contem(lc, lambda n: isinstance(n, ast.Name)
                        and n.id in ("eventos", "api_privada")),
            "05.log_lifecycle_intocado")
    com_await = []
    for raiz, _, arquivos in os.walk(RAIZ):
        if any(p in raiz for p in (".git", "venv", "__pycache__")):
            continue
        for a in arquivos:
            if a.endswith(".py"):
                caminho = os.path.join(raiz, a)
                with open(caminho, encoding="utf-8") as f:
                    arv = ast.parse(f.read())
                for n in ast.walk(arv):
                    if (isinstance(n, ast.Await) and isinstance(n.value, ast.Call)
                            and ast.unparse(n.value.func).endswith("emitir")
                            and "eventos" in ast.unparse(n.value.func)):
                        com_await.append(os.path.relpath(caminho, RAIZ))
    r.check(com_await == [], "05.nenhum_await_em_emitir", str(com_await))


def test_06_ligar_api_privada_real(r):
    main = _main()
    original_iniciar = api_privada.iniciar
    original_vida, original_saude = main._VIDA_EVENTOS_S, main._saude_processo
    chamadas = []

    async def recusa(*a, **k):
        chamadas.append((a, k))
        return False

    async def explode(*a, **k):
        raise RuntimeError("falha simulada ao abrir")

    async def liga(*a, **k):
        return True

    async def rodar_por(segundos):
        t = asyncio.create_task(main._ligar_api_privada())
        await asyncio.sleep(segundos)
        vivo = not t.done()
        t.cancel()
        res = await asyncio.gather(t, return_exceptions=True)
        return vivo, res[0]

    try:
        # (a) a API não abre (ex.: sem IPv6) → proveniência inteira desligada
        eventos.desligar()
        eventos.instalar(1000, 1 << 22)
        api_privada.iniciar = recusa
        asyncio.run(main._ligar_api_privada())
        r.check(eventos.registro_atual() is None, "06.nao_abriu_desliga")
        a, k = chamadas[0]
        r.check(a[0] == config.API_PRIVADA_PORTA and a[1] == config.API_PRIVADA_SEGREDO
                and a[2] is eventos.registro_atual and a[3] is main._saude_processo
                and k == {"porta_publica": config._PORTA_PUBLICA,
                          "porta_tcp_publica": config._PORTA_TCP_PUBLICA},
                "06.repassa_a_configuracao", str(k))
        # (b) exceção ao abrir: contida, desligada, a task termina limpa
        eventos.instalar(1000, 1 << 22)
        api_privada.iniciar = explode
        try:
            asyncio.run(main._ligar_api_privada())
            contida = True
        except Exception:                              # noqa: BLE001
            contida = False
        r.check(contida and eventos.registro_atual() is None, "06.excecao_contida")
        # (c) ligada → processo.vida periódico, com o retrato do processo
        reg = eventos.instalar(1000, 1 << 22)
        api_privada.iniciar = liga
        main._VIDA_EVENTOS_S = 0.05
        vivo, fim = asyncio.run(rodar_por(0.4))
        vidas = [json.loads(c) for c in reg.ler(None, 1000, 1 << 30)["cargas"]]
        vidas = [e for e in vidas if e["tipo"] == "processo.vida"]
        r.check(vivo and isinstance(fim, asyncio.CancelledError) and len(vidas) >= 3,
                "06.vida_periodica", f"vivo={vivo} vidas={len(vidas)}")
        r.check(vidas and set(vidas[0]["dados"]) == {"fila", "workers", "encerrando",
                                                       "ciclo", "coleta", "anel"}
                and vidas[0]["dados"]["anel"]["boot_id"] == reg.boot_id
                and vidas[0]["dados"]["coleta"] == eventos.saude_coleta(),
                "06.vida_com_retrato", str(vidas[0]["dados"] if vidas else None))
        # (d) retrato que falha não mata o laço
        falhas = {"n": 0}

        def retrato_quebrado():
            falhas["n"] += 1
            if falhas["n"] <= 2:
                raise RuntimeError("retrato quebrado")
            return original_saude()

        main._saude_processo = retrato_quebrado
        antes = reg.saude()["seq_ultimo"]
        vivo, _ = asyncio.run(rodar_por(0.4))
        r.check(vivo and falhas["n"] > 2 and reg.saude()["seq_ultimo"] > antes,
                "06.laco_sobrevive_a_falha", f"falhas={falhas['n']}")
    finally:
        api_privada.iniciar = original_iniciar
        main._VIDA_EVENTOS_S, main._saude_processo = original_vida, original_saude
        eventos.desligar()


def test_07_fronteiras(r):
    proibidos = {"database", "database_conexao", "database_posts", "database_espelho",
                 "sqlite3", "telethon", "globals", "client", "pipeline", "config"}
    permitidos = {
        "eventos/anel.py": {"__future__", "json", "math", "secrets", "threading", "time",
                            "collections", "itertools", "typing"},
        "eventos/__init__.py": {"__future__", "typing", "eventos"},
        "eventos/api_privada.py": {"__future__", "asyncio", "hashlib", "hmac",
                                   "ipaddress", "json", "socket", "time", "functools",
                                   "typing", "aiohttp", "eventos", "logger"},
    }
    for rel, ok in permitidos.items():
        importados = set()
        for n in ast.walk(_arvore(rel)):
            if isinstance(n, ast.Import):
                importados |= {a.name.split(".")[0] for a in n.names}
            elif isinstance(n, ast.ImportFrom):
                importados.add((n.module or "").split(".")[0])
        r.check(importados <= ok and not (importados & proibidos),
                f"07.{rel}", str(sorted(importados)))
    importadores = []
    for raiz, _, arquivos in os.walk(RAIZ):
        rel_raiz = os.path.relpath(raiz, RAIZ)
        if rel_raiz.split(os.sep)[0] in (".git", "tests", "eventos", "venv", ".venv"):
            continue
        for a in arquivos:
            if not a.endswith(".py"):
                continue
            rel = os.path.normpath(os.path.join(rel_raiz, a))
            for n in ast.walk(_arvore(rel)):
                if ((isinstance(n, ast.Import) and any(x.name.split(".")[0] == "eventos"
                                                      for x in n.names))
                        or (isinstance(n, ast.ImportFrom)
                            and (n.module or "").split(".")[0] == "eventos")):
                    importadores.append(rel)
    r.check(sorted(set(importadores)) == ["main.py"], "07.so_main_importa_eventos",
            str(sorted(set(importadores))))


def test_08_rotas(r):
    app = api_privada.criar_app(lambda: None, "s" * 40)
    rotas = sorted((rt.method, rt.resource.canonical) for rt in app.router.routes())
    r.check(rotas == [("GET", "/v1/eventos"), ("GET", "/v1/saude")], "08.so_duas_rotas_get",
            str(rotas))
    # O app PÚBLICO (porta PORT, domínio público) não serve nada da API:
    # rotas próprias, nenhuma constante /v1, nenhum import de eventos.
    publico = _arvore("web/redirect.py")
    rotas_pub = sorted(ast.literal_eval(n.args[0]) for n in ast.walk(publico)
                       if isinstance(n, ast.Call)
                       and ast.unparse(n.func).endswith("router.add_get"))
    r.check(rotas_pub == ["/", "/health", "/{code}"], "08.app_publico_rotas_proprias",
            str(rotas_pub))
    r.check(not any(isinstance(n, ast.Constant) and isinstance(n.value, str)
                    and "/v1" in n.value for n in ast.walk(publico)),
            "08.app_publico_sem_v1")


def test_09_segredo_fora_dos_logs(r):
    vazamentos = []
    for rel in ("main.py", "eventos/api_privada.py", "config.py"):
        for n in ast.walk(_arvore(rel)):
            if not (isinstance(n, ast.Call)
                    and (ast.unparse(n.func).startswith(("log_sys.", "log_hc.", "logging."))
                         or ast.unparse(n.func) == "print")):
                continue
            for arg in list(n.args) + [k.value for k in n.keywords]:
                for x in ast.walk(arg):
                    nome = (x.id if isinstance(x, ast.Name)
                            else x.attr if isinstance(x, ast.Attribute) else "")
                    if nome in ("segredo", "API_PRIVADA_SEGREDO"):
                        vazamentos.append(f"{rel}:{n.lineno}")
    r.check(vazamentos == [], "09.nenhum_log_com_segredo", str(vazamentos))


if __name__ == "__main__":
    print(f"Telethon REAL {telethon.__version__}")
    sys.exit(rodar(globals(), "EVENTOS · fiação no worker e fronteiras · F1.1"))
