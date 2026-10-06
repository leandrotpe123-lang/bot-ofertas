"""
F1.2-A1 — CONTRATO verificado no CÓDIGO (AST) e catálogo.

  01  catálogo íntegro: formato dos tipos, terminais válidos, enums não
      vazios (prova em minúsculas, o resto em MAIÚSCULAS), locais,
      reservadas; os enums fechados do contrato fixados aqui
  02  resumo do catálogo: versão, hash estável de 16 hex que muda quando
      o conteúdo muda; nunca levanta
  03  o checador pega cada violação em código sintético — emitir direto,
      registro direto, tipo/local não literal ou fora do catálogo, local
      repetido, coletor que não é lambda/função, coletor assíncrono,
      coletor com banco/log/Telegram/I/O/mutação, mínimo ruim,
      argumentos extras ou desempacotados — e aceita o exemplo válido
  04  árvore REAL: nenhuma violação; emitir direto só em main.py e só
      processo.* do catálogo
  05  fronteiras: coleta.py e catalogo.py só stdlib + eventos; banco,
      plataformas, utils e web nunca importam eventos
  06  coleta.py é pura: sem await, sem log, sem print/open, sem sleep

    python tests/test_eventos_contrato.py
"""
from __future__ import annotations

import ast
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import RAIZ, preparar, rodar  # noqa: E402
preparar()

from eventos import catalogo                     # noqa: E402

_IGNORAR = {".git", "tests", "venv", ".venv", "__pycache__", "node_modules"}

# Chamadas proibidas dentro de um coletor (nome final da chamada).
_PROIBIDAS = {
    "print", "open", "exec", "eval", "compile", "input", "__import__", "setattr",
    "delattr", "sleep", "emitir", "emitir_de", "create_task", "ensure_future",
    "run", "run_until_complete", "gather", "wait_for", "get_messages",
    "send_message", "send_file", "edit_message", "delete_messages",
    "download_media", "iter_messages", "execute", "executemany", "executescript",
    "commit", "rollback", "connect", "urlopen", "request",
    # métodos que MUDAM estado: o coletor monta com literais/compreensões
    "append", "extend", "update", "pop", "popitem", "clear", "add", "remove",
    "insert", "discard", "setdefault", "sort", "reverse",
}
# Raízes de chamada proibidas (client.x(), _db().x(), g.x() …).
_RAIZES_PROIBIDAS = {"client", "_db", "db", "requests", "aiohttp", "socket",
                     "subprocess", "os", "asyncio", "sqlite3", "eventos", "config",
                     "g", "logging", "threading"}


def _arquivos():
    for raiz, dirs, arquivos in os.walk(RAIZ):
        dirs[:] = [d for d in dirs if d not in _IGNORAR]
        for a in arquivos:
            if a.endswith(".py"):
                yield os.path.relpath(os.path.join(raiz, a), RAIZ)


def _arvore(rel: str) -> ast.Module:
    with open(os.path.join(RAIZ, rel), encoding="utf-8") as f:
        return ast.parse(f.read())


def _raiz_de(no):
    while isinstance(no, (ast.Attribute, ast.Call, ast.Subscript)):
        no = no.func if isinstance(no, ast.Call) else no.value
    return no.id if isinstance(no, ast.Name) else ""


def _nome_final(func) -> str:
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return ""


def _funcoes(arv: ast.Module) -> dict:
    return {n.name: n for n in arv.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _impureza(corpo_no, funcoes: dict, vistos: set) -> list:
    """Motivos pelos quais o coletor não é puro (vazio = puro)."""
    erros = []
    for n in ast.walk(corpo_no):
        if isinstance(n, (ast.Await, ast.Yield, ast.YieldFrom)):
            erros.append("await/yield")
        elif isinstance(n, (ast.Global, ast.Nonlocal)):
            erros.append("global/nonlocal")
        elif isinstance(n, (ast.Assign, ast.AugAssign, ast.AnnAssign, ast.Delete)):
            alvos = (n.targets if isinstance(n, (ast.Assign, ast.Delete))
                     else [n.target])
            if any(isinstance(a, (ast.Attribute, ast.Subscript)) for a in alvos):
                erros.append("mutacao")
        elif isinstance(n, ast.NamedExpr):
            erros.append("walrus")
        elif isinstance(n, ast.Call):
            final, raiz = _nome_final(n.func), _raiz_de(n.func)
            if (final in _PROIBIDAS or final.startswith("db_") or raiz.startswith("log")
                    or final.startswith("log") or raiz in _RAIZES_PROIBIDAS):
                erros.append(f"chamada:{ast.unparse(n.func)}")
            elif (isinstance(n.func, ast.Name) and n.func.id in funcoes
                  and n.func.id not in vistos):
                vistos.add(n.func.id)
                alvo = funcoes[n.func.id]
                if isinstance(alvo, ast.AsyncFunctionDef):
                    erros.append(f"assincrona:{alvo.name}")
                else:
                    erros.extend(_impureza(alvo, funcoes, vistos))
    return erros


def _coletor(no, funcoes: dict) -> list:
    if isinstance(no, ast.Lambda):
        return _impureza(no.body, funcoes, set())
    if isinstance(no, ast.Name) and no.id in funcoes:
        alvo = funcoes[no.id]
        if isinstance(alvo, ast.AsyncFunctionDef):
            return [f"assincrona:{alvo.name}"]
        return _impureza(alvo, funcoes, {no.id})
    return ["coletor_nao_e_lambda_nem_funcao"]


def verificar(fonte: str, rel: str, tipos, locais, emitir_permitido: bool,
              locais_vistos: dict) -> list:
    """Violações do contrato de emissão num arquivo."""
    arv = ast.parse(fonte)
    funcoes = _funcoes(arv)
    erros = []
    for n in ast.walk(arv):
        if isinstance(n, ast.ImportFrom) and (n.module or "").split(".")[0] == "eventos":
            if any(a.name == "emitir" for a in n.names) and not emitir_permitido:
                erros.append(f"{rel}:{n.lineno} import de emitir direto")
        if not isinstance(n, ast.Call):
            continue
        txt = ast.unparse(n.func)
        final = _nome_final(n.func)
        if final == "emitir" and (txt in ("eventos.emitir", "emitir")
                                  or "registro" in txt or "eventos" in txt):
            if not emitir_permitido:
                erros.append(f"{rel}:{n.lineno} emitir direto")
            continue
        if final != "emitir_de":
            continue
        if any(isinstance(a, ast.Starred) for a in n.args) or any(
                k.arg is None for k in n.keywords):
            erros.append(f"{rel}:{n.lineno} argumentos desempacotados")
            continue
        if len(n.args) != 2:
            erros.append(f"{rel}:{n.lineno} esperados 2 argumentos posicionais")
            continue
        tipo = n.args[0]
        if not (isinstance(tipo, ast.Constant) and isinstance(tipo.value, str)):
            erros.append(f"{rel}:{n.lineno} tipo nao literal")
        elif tipo.value not in tipos:
            erros.append(f"{rel}:{n.lineno} tipo fora do catalogo: {tipo.value}")
        kws = {k.arg: k.value for k in n.keywords}
        extras = set(kws) - {"local", "minimo"}
        if extras:
            erros.append(f"{rel}:{n.lineno} argumentos extras {sorted(extras)}")
        local = kws.get("local")
        if not (isinstance(local, ast.Constant) and isinstance(local.value, str)):
            erros.append(f"{rel}:{n.lineno} local ausente ou nao literal")
        elif local.value not in locais:
            erros.append(f"{rel}:{n.lineno} local fora do catalogo: {local.value}")
        else:
            anterior = locais_vistos.setdefault(local.value, f"{rel}:{n.lineno}")
            if anterior != f"{rel}:{n.lineno}":
                erros.append(f"{rel}:{n.lineno} local repetido ({anterior})")
        for motivo in _coletor(n.args[1], funcoes):
            erros.append(f"{rel}:{n.lineno} coletor: {motivo}")
        if "minimo" in kws:
            for motivo in _coletor(kws["minimo"], funcoes):
                erros.append(f"{rel}:{n.lineno} minimo: {motivo}")
    return erros


def test_01_catalogo_integro(r):
    r.check(all(re.fullmatch(r"[a-z]+\.[a-z_]+", t) for t in catalogo.TIPOS),
            "01.formato_dos_tipos")
    resultados = catalogo.ENUMS["resultado_execucao"]
    r.check(all(v is None or v in resultados for v in catalogo.TIPOS.values()),
            "01.terminais_validos")
    ruins = []
    for nome, valores in catalogo.ENUMS.items():
        if type(valores) is not frozenset or not valores:
            ruins.append(nome)
            continue
        padrao = r"[a-z][a-z_]*" if nome.startswith("prova_") else r"[A-Z][A-Z0-9_]*"
        if not all(isinstance(v, str) and re.fullmatch(padrao, v) for v in valores):
            ruins.append(nome)
    r.check(ruins == [], "01.enums_fechados_e_caixa", str(ruins))
    r.check(all(re.fullmatch(r"[a-z_]+(\.[a-z0-9_]+)+", x) for x in catalogo.LOCAIS)
            and "eventos.execucao.fim" in catalogo.LOCAIS, "01.locais")
    r.check(catalogo.RESERVADAS_CORR == {"exec", "exec_pai"}
            and catalogo.RESERVADAS_DADOS == {"local", "ts_fato", "degradado",
                                              "erro_tipo", "cortado"}, "01.reservadas")
    e = catalogo.ENUMS
    r.check(e["motivo_descarte"] == {
        "EDIT_ANTIGO", "NOVA_ANTIGA", "ENCERRANDO", "FILA_CHEIA", "ERRO_ENTRADA",
        "ERRO_WORKER", "ERRO_INGESTAO", "ORIGEM_APAGADA", "JA_PROCESSADO",
        "ORIGEM_JA_PUBLICADA", "ERRO_NORMALIZAR", "NORMALIZACAO_VAZIA", "DEDUP",
        "ERRO_DEDUP", "ERRO_MONTAR", "REDIRECIONAMENTO_ESGOTADO"}, "01.motivos_de_descarte")
    r.check(e["fase_remocao"] == {"TENTATIVA", "DESFECHO"}
            and e["prova_banco"] == {"confirmado", "falhou_sem_escrita", "falhou_parcial",
                                     "nao_verificavel", "nao_aplicavel"}
            and e["fase_delete_antigo"] >= {"SUCESSO", "FALHA", "DESCONHECIDO"}
            and e["fase_novo_envio"] >= {"SUCESSO", "FALHA", "NAO_TENTADO", "DESCONHECIDO"}
            and "FANTASMA" in e["estado_reconciliacao"], "01.enums_do_contrato")
    r.check(catalogo.valido("via", "TELEGRAM") and not catalogo.valido("via", "telegram")
            and not catalogo.valido("inexistente", "X")
            and catalogo.terminal("post.publicado") == "PUBLICADA"
            and catalogo.terminal("origem.recebida") is None, "01.consultas")


def test_02_resumo_do_catalogo(r):
    a, b = catalogo.resumo(), catalogo.resumo()
    r.check(a == b and a["versao"] == 1 and re.fullmatch(r"[0-9a-f]{16}", a["hash"] or "")
            and a["tipos"] == len(catalogo.TIPOS), "02.estavel", str(a))
    h = catalogo._hash(catalogo.TIPOS, catalogo.ENUMS, catalogo.LOCAIS)
    h2 = catalogo._hash({**catalogo.TIPOS, "x.novo": None}, catalogo.ENUMS,
                        catalogo.LOCAIS)
    h3 = catalogo._hash(catalogo.TIPOS, catalogo.ENUMS,
                        catalogo.LOCAIS | {"x.novo"})
    r.check(h == a["hash"] and h != h2 and h != h3 and h2 != h3, "02.hash_muda_com_conteudo")
    original = catalogo._RESUMO
    catalogo._RESUMO, catalogo.TIPOS_ORIG = None, catalogo.TIPOS
    catalogo.TIPOS = None                              # sabota o cálculo
    try:
        res = catalogo.resumo()
    finally:
        catalogo.TIPOS, catalogo._RESUMO = catalogo.TIPOS_ORIG, original
        del catalogo.TIPOS_ORIG
    r.check(res == {"versao": 1, "hash": None, "tipos": None}, "02.nunca_levanta", str(res))


_TIPOS_T = {"origem.recebida", "post.publicado"}
_LOCAIS_T = {"a.b", "a.c", "a.d"}

_RUINS = {
    "emitir_direto": "import eventos\neventos.emitir('origem.recebida', {})\n",
    "import_emitir": "from eventos import emitir\nemitir('origem.recebida')\n",
    "registro_direto": "import eventos\neventos.registro_atual().emitir('origem.recebida')\n",
    "tipo_nao_literal": "import eventos\nt = 'origem.recebida'\n"
                        "eventos.emitir_de(t, lambda: ({}, {}), local='a.b')\n",
    "tipo_fora": "import eventos\neventos.emitir_de('origem.inventado', lambda: ({}, {}), "
                 "local='a.b')\n",
    "sem_local": "import eventos\neventos.emitir_de('origem.recebida', lambda: ({}, {}))\n",
    "local_nao_literal": "import eventos\nL = 'a.b'\n"
                         "eventos.emitir_de('origem.recebida', lambda: ({}, {}), local=L)\n",
    "local_fora": "import eventos\neventos.emitir_de('origem.recebida', lambda: ({}, {}), "
                  "local='z.z')\n",
    "local_repetido": "import eventos\n"
                      "eventos.emitir_de('origem.recebida', lambda: ({}, {}), local='a.b')\n"
                      "eventos.emitir_de('post.publicado', lambda: ({}, {}), local='a.b')\n",
    "coletor_chamada": "import eventos\n"
                       "eventos.emitir_de('origem.recebida', montar(), local='a.b')\n",
    "coletor_externo": "import eventos\nfrom x import col\n"
                       "eventos.emitir_de('origem.recebida', col, local='a.b')\n",
    "coletor_async": "import eventos\nasync def col():\n    return {}, {}\n"
                     "eventos.emitir_de('origem.recebida', col, local='a.b')\n",
    "coletor_db": "import eventos\n"
                  "eventos.emitir_de('origem.recebida', lambda: ({}, {'p': db_get_post(1)}), "
                  "local='a.b')\n",
    "coletor_log": "import eventos\n"
                   "eventos.emitir_de('origem.recebida', lambda: (log_sys.info('x'), {}), "
                   "local='a.b')\n",
    "coletor_telegram": "import eventos\n"
                        "eventos.emitir_de('origem.recebida', lambda: ({}, "
                        "{'m': client.get_messages(1)}), local='a.b')\n",
    "coletor_estado_global": "import eventos\n"
                             "eventos.emitir_de('origem.recebida', lambda: ({}, "
                             "{'x': g.midia_aceita_set(1, 2)}), local='a.b')\n",
    "coletor_mutacao": "import eventos\n"
                       "eventos.emitir_de('origem.recebida', lambda: ({}, "
                       "{'x': lista.append(1)}), local='a.b')\n",
    "coletor_atribui": "import eventos\ndef col():\n    estado.x = 1\n    return {}, {}\n"
                       "eventos.emitir_de('origem.recebida', col, local='a.b')\n",
    "coletor_helper_impuro": "import eventos\ndef _aux():\n    return db_x()\n"
                             "def col():\n    return {}, {'a': _aux()}\n"
                             "eventos.emitir_de('origem.recebida', col, local='a.b')\n",
    "coletor_open": "import eventos\n"
                    "eventos.emitir_de('origem.recebida', lambda: ({}, "
                    "{'x': open('f').read()}), local='a.b')\n",
    "minimo_ruim": "import eventos\n"
                   "eventos.emitir_de('origem.recebida', lambda: ({}, {}), local='a.b', "
                   "minimo=lambda: db_x())\n",
    "argumento_extra": "import eventos\n"
                       "eventos.emitir_de('origem.recebida', lambda: ({}, {}), local='a.b', "
                       "outro=1)\n",
    "desempacotado": "import eventos\neventos.emitir_de(*args, local='a.b')\n",
    "kwargs": "import eventos\n"
              "eventos.emitir_de('origem.recebida', lambda: ({}, {}), **kw)\n",
}

_BOM = (
    "import eventos\n"
    "def _ids(m):\n"
    "    return {'chat': str(m.chat_id), 'msg': int(m.id)}\n"
    "def _col_pub():\n"
    "    itens = [x for x in range(3)]\n"
    "    return {}, {'n': len(itens), 'ok': sorted(itens)}\n"
    "eventos.emitir_de('origem.recebida', lambda: (_ids(m), {'t': m.text}), local='a.b',\n"
    "                  minimo=lambda: _ids(m))\n"
    "eventos.emitir_de('post.publicado', _col_pub, local='a.c')\n"
)


_MOTIVO = {
    "emitir_direto": "emitir direto", "import_emitir": "import de emitir direto",
    "registro_direto": "emitir direto", "tipo_nao_literal": "tipo nao literal",
    "tipo_fora": "tipo fora do catalogo", "sem_local": "local ausente",
    "local_nao_literal": "local ausente ou nao literal",
    "local_fora": "local fora do catalogo", "local_repetido": "local repetido",
    "coletor_chamada": "coletor_nao_e_lambda_nem_funcao",
    "coletor_externo": "coletor_nao_e_lambda_nem_funcao",
    "coletor_async": "assincrona", "coletor_db": "chamada:db_get_post",
    "coletor_log": "chamada:log_sys.info", "coletor_telegram": "chamada:client.get_messages",
    "coletor_estado_global": "chamada:g.midia_aceita_set",
    "coletor_mutacao": "chamada:lista.append", "coletor_atribui": "mutacao",
    "coletor_helper_impuro": "chamada:db_x", "coletor_open": "chamada:open",
    "minimo_ruim": "minimo: chamada:db_x", "argumento_extra": "argumentos extras",
    "desempacotado": "argumentos desempacotados", "kwargs": "argumentos desempacotados",
}


def test_03_checador_pega_cada_violacao(r):
    nao_pegou = []
    for nome, fonte in _RUINS.items():
        erros = verificar(fonte, nome, _TIPOS_T, _LOCAIS_T, False, {})
        if not any(_MOTIVO[nome] in e for e in erros):
            nao_pegou.append((nome, erros))
    r.check(set(_MOTIVO) == set(_RUINS) and nao_pegou == [],
            "03.cada_violacao_pelo_motivo_certo", str(nao_pegou))
    erros_bom = verificar(_BOM, "bom", _TIPOS_T, _LOCAIS_T, False, {})
    r.check(erros_bom == [], "03.exemplo_valido_aceito", str(erros_bom))
    r.check(verificar(_RUINS["emitir_direto"], "main.py", _TIPOS_T, _LOCAIS_T, True, {})
            == [], "03.main_pode_emitir_direto")


def test_04_arvore_real(r):
    erros, vistos = [], {}
    processos = []
    for rel in _arquivos():
        if rel.split(os.sep)[0] == "eventos":
            continue
        fonte = open(os.path.join(RAIZ, rel), encoding="utf-8").read()
        erros += verificar(fonte, rel, catalogo.TIPOS, catalogo.LOCAIS, rel == "main.py",
                           vistos)
        if rel == "main.py":
            for n in ast.walk(ast.parse(fonte)):
                if isinstance(n, ast.Call) and ast.unparse(n.func) == "eventos.emitir":
                    a0 = n.args[0] if n.args else None
                    processos.append(a0.value if isinstance(a0, ast.Constant) else None)
    r.check(erros == [], "04.nenhuma_violacao", str(erros[:10]))
    r.check(processos and all(isinstance(p, str) and p.startswith("processo.")
                              and p in catalogo.TIPOS for p in processos),
            "04.main_so_processo_do_catalogo", str(processos))


def test_05_fronteiras(r):
    proibidos = {"database", "database_conexao", "database_posts", "database_espelho",
                 "database_links", "database_cupons", "database_manutencao", "sqlite3",
                 "telethon", "globals", "client", "pipeline", "config", "plataformas",
                 "logger", "utils", "web", "main", "aiohttp", "logging", "socket"}
    permitidos = {
        "eventos/coleta.py": {"__future__", "asyncio", "contextvars", "functools",
                              "hashlib", "itertools", "json", "math", "re", "time",
                              "typing", "urllib", "eventos"},
        "eventos/catalogo.py": {"__future__", "hashlib", "json", "typing"},
    }
    for rel, ok in permitidos.items():
        importados = set()
        for n in ast.walk(_arvore(rel)):
            if isinstance(n, ast.Import):
                importados |= {a.name.split(".")[0] for a in n.names}
            elif isinstance(n, ast.ImportFrom):
                importados.add((n.module or "").split(".")[0])
        r.check(importados <= ok and not (importados & proibidos), f"05.{rel}",
                str(sorted(importados)))
    importadores = []
    for rel in _arquivos():
        topo = rel.split(os.sep)[0]
        if not (topo.startswith("database") or topo in ("plataformas", "utils", "web")):
            continue
        for n in ast.walk(_arvore(rel)):
            if ((isinstance(n, ast.Import)
                 and any(a.name.split(".")[0] == "eventos" for a in n.names))
                    or (isinstance(n, ast.ImportFrom)
                        and (n.module or "").split(".")[0] == "eventos")):
                importadores.append(rel)
    r.check(importadores == [], "05.banco_plataformas_utils_web_sem_eventos",
            str(importadores))


def test_06_coleta_pura(r):
    impuros = []
    for n in ast.walk(_arvore("eventos/coleta.py")):
        if isinstance(n, (ast.Await, ast.AsyncFunctionDef, ast.AsyncWith, ast.AsyncFor)):
            impuros.append(f"async:{n.lineno}")
        elif isinstance(n, ast.Call):
            txt = ast.unparse(n.func)
            if (txt in ("print", "open", "input", "time.sleep", "asyncio.sleep")
                    or _raiz_de(n.func).startswith("log")):
                impuros.append(f"{txt}:{n.lineno}")
    r.check(impuros == [], "06.sem_await_log_print_open_sleep", str(impuros))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "EVENTOS · contrato no código (AST) e catálogo · F1.2-A1"))
