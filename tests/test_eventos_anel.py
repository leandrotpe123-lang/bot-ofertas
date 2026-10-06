"""
F1.1 — REGISTRO de eventos (eventos/anel.py) e fachada (eventos/__init__.py).

Prova, na árvore real, as garantias do emissor e do anel:
  01  seq de 1, contíguo; envelope v1
  02  boot_id: estável no processo, distinto entre processos
  03  fachada: no-op antes de instalar; instalar idempotente; desligar
  04  corte de strings (> 4096) em dict/lista/aninhado
  05  evento acima de 16 KiB: marcador determinístico, 1 seq, contador
  06  anel por QUANTIDADE: descarte pela frente, contado, contíguo
  07  anel por BYTES: rajada de 16 KiB nunca passa do teto
  08  leitura: limite, teto de bytes da resposta e cursor seguinte
  09  CURSOR: boot A em sequência
  10  CURSOR: reinício → boot B; Brain retoma com cursor de A → nova_epoca
  11  CURSOR: A:5 diante de B com seq > 5 NÃO continua (sem falsa continuidade)
  12  CURSOR: lacuna no mesmo boot; inicio com anel girado; cursor à frente
  13  seq TRANSACIONAL: falha ao montar/serializar não consome número
  14  seq TRANSACIONAL: falha no append não consome número
  15  nunca levanta: entradas estranhas, falha injetada → contador
  16  8 threads × 5000 emissões: seq único, contíguo e ordenado
  17  SEM I/O: socket, open e sqlite3.connect sabotados → emite igual
  18  SEM BLOQUEIO: emitir é síncrona (sem await), O(1) por evento
  19  CUSTO LIMITADO: entrada patológica (milhões de itens, MB de texto,
      bytes, aninhamento fundo, NaN, surrogate) → rápido, ≤ 16 KiB,
      determinístico
  20  TETO RÍGIDO: evento que nem reduzido cabe é recusado sem seq
  21  boot_id inválido é recusado na criação (cursor sempre legível)
  22  leitura: idêntica a uma referência ingênua (cursores, limites e
      tetos aleatórios) e, no fim do anel, custo O(novos) — não O(anel)
  23  OBSERVABILIDADE: bytes_anel é a soma exata dos eventos guardados e
      nunca passa de max_bytes_anel; o descarte diz a causa (quantidade
      ou bytes); /v1/saude traz ocupação e limites nas duas medidas

    python tests/test_eventos_anel.py
"""
from __future__ import annotations

import ast
import builtins
import inspect
import io
import json
import os
import socket
import sqlite3
import sys
import threading
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar  # noqa: E402
preparar()

import eventos                                   # noqa: E402
from eventos import anel                         # noqa: E402
from eventos.anel import (MAX_EVENTO_BYTES, MAX_TEXTO, Registro,  # noqa: E402
                          ler_cursor)

A = "a" * 16
B = "b" * 16
GRANDE = 1 << 30


def _seqs(res):
    return [json.loads(c)["seq"] for c in res["cargas"]]


def _ev(res, i=0):
    return json.loads(res["cargas"][i])


# ══════════════════════════════════════════════════════════════════
def test_01_seq_contiguo_e_envelope(r):
    reg = Registro(100, GRANDE, boot_id=A)
    seqs = [reg.emitir("processo.vida", {"fila": i}, {"dest": 10 + i})
            for i in range(5)]
    r.check(seqs == [1, 2, 3, 4, 5], "01.seq_1_a_5", str(seqs))
    ev = _ev(reg.ler(None, 10, GRANDE), 2)
    r.check(set(ev) == {"v", "tipo", "boot", "seq", "ts", "corr", "dados"},
            "01.campos_envelope", str(sorted(ev)))
    r.check(ev["v"] == 1 and ev["tipo"] == "processo.vida" and ev["boot"] == A
            and ev["seq"] == 3 and ev["dados"] == {"fila": 2}
            and ev["corr"] == {"dest": 12}, "01.valores", str(ev))


def test_02_boot_id(r):
    r1, r2 = Registro(10, GRANDE), Registro(10, GRANDE)
    r.check(len(r1.boot_id) == 16 and all(c in "0123456789abcdef" for c in r1.boot_id),
            "02.hex16", r1.boot_id)
    r.check(r1.boot_id != r2.boot_id, "02.distinto_entre_processos")
    r1.emitir("x")
    r1.emitir("y")
    bootes = {json.loads(c)["boot"] for c in r1.ler(None, 10, GRANDE)["cargas"]}
    r.check(bootes == {r1.boot_id}, "02.estavel_no_processo", str(bootes))


def test_03_fachada(r):
    eventos.desligar()
    r.check(eventos.registro_atual() is None, "03.desligada")
    r.check(eventos.emitir("x", {"a": 1}) == 0, "03.noop_antes_de_instalar")
    reg = eventos.instalar(100, GRANDE)
    r.check(eventos.instalar(5, 5) is reg, "03.instalar_idempotente")
    r.check(eventos.emitir("x") == 1 and eventos.emitir("y") == 2, "03.emite_instalado")
    eventos.desligar()
    r.check(eventos.emitir("z") == 0 and eventos.registro_atual() is None,
            "03.desligar_volta_ao_noop")


def test_04_corte_de_strings(r):
    reg = Registro(10, GRANDE, boot_id=A)
    longo = "x" * (MAX_TEXTO + 500)
    reg.emitir("t", {"texto": longo, "lista": [longo], "aninhado": {"k": longo},
                     "curto": "ok"})
    d = _ev(reg.ler(None, 1, GRANDE))["dados"]
    corte = "x" * MAX_TEXTO + "…[cortado]"
    r.check(d["texto"] == corte and d["lista"] == [corte]
            and d["aninhado"]["k"] == corte and d["curto"] == "ok",
            "04.cortes", str({k: len(str(v)) for k, v in d.items()}))


def test_05_evento_acima_do_limite(r):
    reg = Registro(10, GRANDE, boot_id=A)
    # Muitas strings curtas: cada uma passa no corte, o TOTAL não passa.
    gigante = {f"campo_{i:03d}": "y" * 4000 for i in range(20)}
    s1 = reg.emitir("grande", gigante, {"dest": 1})
    s2 = reg.emitir("grande", gigante, {"dest": 1})
    res = reg.ler(None, 10, GRANDE)
    e1, e2 = _ev(res, 0), _ev(res, 1)
    r.check((s1, s2) == (1, 2), "05.um_seq_por_evento", str((s1, s2)))
    r.check(all(len(c) <= MAX_EVENTO_BYTES for c in res["cargas"]),
            "05.cabe_no_limite", str([len(c) for c in res["cargas"]]))
    r.check(e1["dados"].get("_excedeu") is True
            and e1["dados"]["_chaves"] == sorted(gigante)[:50]
            and e1["dados"]["_bytes_dados"] > MAX_EVENTO_BYTES,
            "05.marcador", str(e1["dados"])[:200])
    r.check(e1["dados"] == e2["dados"] and e1["corr"] == e2["corr"] == {"dest": 1},
            "05.deterministico")
    r.check(e1["tipo"] == "grande" and e1["boot"] == A, "05.envelope_preservado")
    r.check(reg.saude()["excedidos"] == 2, "05.contador")
    # corr gigante também vira marcador, e o evento continua no limite
    s3 = reg.emitir("grande", gigante, {f"c{i}": "z" * 4000 for i in range(20)})
    e3 = _ev(reg.ler((A, 2), 10, GRANDE))
    r.check(s3 == 3 and e3["corr"].get("_excedeu") is True
            and len(reg.ler((A, 2), 10, GRANDE)["cargas"][0]) <= MAX_EVENTO_BYTES,
            "05.corr_grande", str(e3["corr"]))


def test_06_anel_por_quantidade(r):
    reg = Registro(5, GRANDE, boot_id=A)
    for i in range(8):
        reg.emitir("t", {"i": i})
    res = reg.ler(None, 100, GRANDE)
    s = reg.saude()
    r.check(_seqs(res) == [4, 5, 6, 7, 8], "06.contiguo", str(_seqs(res)))
    r.check(s["descartados"] == 3 and s["seq_primeiro"] == 4 and s["seq_ultimo"] == 8
            and s["eventos_anel"] == 5 and s["descartados_por_eventos"] == 3
            and s["descartados_por_bytes"] == 0, "06.contadores", str(s))


def test_07_anel_por_bytes(r):
    teto = 100 * 1024
    reg = Registro(100_000, teto, boot_id=A)
    pico = 0
    for i in range(200):
        reg.emitir("t", {"blob": "w" * 4000, "b2": "w" * 4000, "b3": "w" * 4000, "i": i})
        pico = max(pico, reg.saude()["bytes_anel"])
    res = reg.ler(None, 100_000, GRANDE)
    seqs = _seqs(res)
    r.check(pico <= teto, "07.nunca_passa_do_teto", f"pico={pico} teto={teto}")
    r.check(seqs == list(range(seqs[0], 201)), "07.contiguo", f"{seqs[:3]}..{seqs[-3:]}")
    s = reg.saude()
    r.check(s["descartados"] == 200 - len(seqs) == s["descartados_por_bytes"]
            and s["descartados_por_eventos"] == 0, "07.descarte_contado_por_bytes", str(s))


def test_08_leitura_limite_e_bytes(r):
    reg = Registro(1000, GRANDE, boot_id=A)
    for i in range(50):
        reg.emitir("t", {"i": i, "pad": "p" * 1000})
    res = reg.ler(None, 10, GRANDE)
    r.check(_seqs(res) == list(range(1, 11)) and res["meta"]["cursor"] == f"{A}:10",
            "08.limite", str(res["meta"]["cursor"]))
    tres = reg.ler((A, 10), 3, GRANDE)["cargas"]          # 11, 12, 13
    teto = sum(len(c) for c in tres)                       # cabe exatamente 3
    res2 = reg.ler((A, 10), 1000, teto)
    r.check(_seqs(res2) == [11, 12, 13] and res2["meta"]["cursor"] == f"{A}:13",
            "08.teto_de_bytes", str(_seqs(res2)))
    res2b = reg.ler((A, 10), 1000, teto - 1)
    r.check(_seqs(res2b) == [11, 12], "08.teto_de_bytes_um_a_menos", str(_seqs(res2b)))
    res3 = reg.ler((A, 50), 1000, GRANDE)
    r.check(res3["cargas"] == [] and res3["meta"]["cursor"] == f"{A}:50"
            and res3["meta"]["continuidade"] == "continua", "08.nada_novo")


def test_09_cursor_boot_A_em_sequencia(r):
    reg = Registro(1000, GRANDE, boot_id=A)
    for i in range(3):
        reg.emitir("t", {"i": i})
    res = reg.ler(None, 100, GRANDE)
    r.check(res["meta"]["continuidade"] == "inicio" and res["meta"]["lacunas"] == []
            and _seqs(res) == [1, 2, 3], "09.inicio")
    cursor = ler_cursor(res["meta"]["cursor"])
    reg.emitir("t", {"i": 3})
    reg.emitir("t", {"i": 4})
    res = reg.ler(cursor, 100, GRANDE)
    r.check(res["meta"]["continuidade"] == "continua" and res["meta"]["lacunas"] == []
            and _seqs(res) == [4, 5] and res["meta"]["cursor"] == f"{A}:5",
            "09.continua", str(res["meta"]))
    r.check(res["meta"]["boot_id"] == A and res["meta"]["epoca_anterior"] is None,
            "09.mesma_epoca")


def test_10_cursor_reinicio_boot_B(r):
    reg_a = Registro(1000, GRANDE, boot_id=A)
    for i in range(7):
        reg_a.emitir("t", {"i": i})
    cursor_a = ler_cursor(reg_a.ler(None, 5, GRANDE)["meta"]["cursor"])   # A:5
    # REINÍCIO: processo novo = registro novo = época nova
    reg_b = Registro(1000, GRANDE, boot_id=B)
    reg_b.emitir("processo.iniciado")
    reg_b.emitir("processo.online")
    res = reg_b.ler(cursor_a, 100, GRANDE)
    m = res["meta"]
    r.check(m["continuidade"] == "nova_epoca" and m["boot_id"] == B,
            "10.detecta_troca_de_boot", str(m))
    r.check(m["epoca_anterior"] == {"boot": A, "ultimo_seq_recebido": 5},
            "10.epoca_anterior", str(m["epoca_anterior"]))
    r.check(m["lacunas"] == [{"boot": A, "de": 6, "ate": None}],
            "10.fim_de_A_declarado_perdido", str(m["lacunas"]))
    r.check(_seqs(res) == [1, 2] and all(json.loads(c)["boot"] == B for c in res["cargas"]),
            "10.le_do_inicio_de_B", str(_seqs(res)))
    r.check(m["cursor"] == f"{B}:2", "10.cursor_seguinte_e_de_B", m["cursor"])
    # Com o cursor novo, B continua normalmente
    reg_b.emitir("processo.vida")
    res = reg_b.ler(ler_cursor(m["cursor"]), 100, GRANDE)
    r.check(res["meta"]["continuidade"] == "continua" and _seqs(res) == [3],
            "10.B_continua_depois")


def test_11_sem_falsa_continuidade(r):
    reg_b = Registro(1000, GRANDE, boot_id=B)
    for i in range(10):
        reg_b.emitir("t", {"i": i})
    res = reg_b.ler((A, 5), 100, GRANDE)       # cursor de A com seq 5
    r.check(res["meta"]["continuidade"] == "nova_epoca", "11.nao_e_continua")
    r.check(_seqs(res)[0] == 1 and _seqs(res) == list(range(1, 11)),
            "11.nao_pula_para_B6", str(_seqs(res)))
    r.check(A + ":" not in res["meta"]["cursor"], "11.cursor_nao_mistura_epocas")
    # mesmo seq, boots diferentes, nunca iguais
    r.check(ler_cursor(f"{A}:18472") != ler_cursor(f"{B}:1")
            and ler_cursor(f"{A}:1") != ler_cursor(f"{B}:1"), "11.cursores_distintos")


def test_12_cursor_lacuna_inicio_girado_e_invalidos(r):
    reg = Registro(5, GRANDE, boot_id=A)
    for i in range(20):
        reg.emitir("t", {"i": i})                        # anel: 16..20
    res = reg.ler((A, 3), 100, GRANDE)
    r.check(res["meta"]["continuidade"] == "lacuna"
            and res["meta"]["lacunas"] == [{"boot": A, "de": 4, "ate": 15}]
            and _seqs(res) == [16, 17, 18, 19, 20], "12.lacuna_mesmo_boot",
            str(res["meta"]["lacunas"]))
    res = reg.ler(None, 100, GRANDE)
    r.check(res["meta"]["continuidade"] == "inicio"
            and res["meta"]["lacunas"] == [{"boot": A, "de": 1, "ate": 15}],
            "12.inicio_com_anel_girado", str(res["meta"]["lacunas"]))
    res = reg.ler((A, 99), 100, GRANDE)
    r.check(res["meta"]["continuidade"] == "cursor_invalido"
            and _seqs(res) == [16, 17, 18, 19, 20], "12.cursor_a_frente")
    for ruim in ("", "abc", f"{A}", f"{A}:", f"{A}:-1", f"{A}:1:2", "ZZZZZZZZ:1",
                 f"{A}:1x", "a" * 70 + ":1"):
        r.check(ler_cursor(ruim) is None, "12.malformado", repr(ruim))
    r.check(ler_cursor(f"{A}:0") == (A, 0), "12.cursor_zero_valido")


def test_13_falha_ao_montar_nao_consome_seq(r):
    reg = Registro(100, GRANDE, boot_id=A)
    r.check(reg.emitir("t", {"i": 0}) == 1, "13.primeiro")

    class Venenoso:
        def __str__(self):
            raise RuntimeError("serialização impossível")

    r.check(reg.emitir("t", {"x": Venenoso()}) == 0, "13.recusado_devolve_0")
    r.check(reg.emitir("t", {"y": [Venenoso()]}) == 0, "13.recusado_aninhado")
    r.check(reg.emitir("t", {"i": 1}) == 2, "13.proximo_recebe_o_seq_seguinte")
    res = reg.ler(None, 100, GRANDE)
    r.check(_seqs(res) == [1, 2], "13.sem_buraco_artificial", str(_seqs(res)))
    r.check(reg.saude()["falhas_emissao"] == 2 and reg.saude()["seq_ultimo"] == 2,
            "13.falhas_contadas", str(reg.saude()))
    # Estrutura circular NÃO é falha: o corte de profundidade a torna
    # texto, de forma determinística — o evento existe e tem seu seq.
    circular = {}
    circular["eu"] = circular
    r.check(reg.emitir("t", circular) == 3 and reg.emitir("t", {"i": 2}) == 4,
            "13.circular_aceito_sem_buraco")
    r.check(_seqs(reg.ler(None, 100, GRANDE)) == [1, 2, 3, 4], "13.contiguo_ao_fim")


def test_14_falha_no_append_nao_consome_seq(r):
    reg = Registro(100, GRANDE, boot_id=A)
    reg.emitir("t", {"i": 0})

    class AnelSabotado(deque):
        sabotar = 2

        def append(self, item):
            if AnelSabotado.sabotar:
                AnelSabotado.sabotar -= 1
                raise MemoryError("append sabotado")
            super().append(item)

    reg._anel = AnelSabotado(reg._anel)
    bytes_antes = reg.saude()["bytes_anel"]
    r.check(reg.emitir("t", {"i": 1}) == 0 and reg.emitir("t", {"i": 2}) == 0,
            "14.recusados")
    r.check(reg.saude()["seq_ultimo"] == 1 and reg.saude()["bytes_anel"] == bytes_antes,
            "14.nada_comprometido", str(reg.saude()))
    r.check(reg.emitir("t", {"i": 3}) == 2, "14.proximo_seq_e_2")
    res = reg.ler(None, 100, GRANDE)
    r.check(_seqs(res) == [1, 2] and json.loads(res["cargas"][1])["dados"] == {"i": 3},
            "14.cada_seq_e_um_evento", str(_seqs(res)))


def test_15_nunca_levanta(r):
    reg = Registro(100, GRANDE, boot_id=A)
    estranhos = [(None, None, None), (123, [1, 2], "x"), ("t", {1: {2: {3: {4: {5: 6}}}}}, None),
                 ("t", {"s": {1, 2, 3}}, {"t": (1, 2)}), ("t" * 500, {"b": b"\x00\xff"}, None)]
    ok = True
    for tipo, dados, corr in estranhos:
        try:
            reg.emitir(tipo, dados, corr)
        except Exception as e:                       # noqa: BLE001
            ok = False
            r.check(False, "15.levantou", f"{tipo!r}: {e!r}")
    r.check(ok, "15.nenhuma_excecao_sobe")
    # ouvinte com defeito não chega a quem emite
    reg.adicionar_ouvinte(lambda: 1 / 0)
    try:
        s = reg.emitir("t")
        r.check(s > 0, "15.ouvinte_com_defeito_isolado")
    except Exception as e:                           # noqa: BLE001
        r.check(False, "15.ouvinte_vazou", repr(e))
    # fachada: registro quebrado não chega a quem emite
    eventos.desligar()
    eventos.instalar(10, GRANDE)

    def quebrado(*a, **k):
        raise RuntimeError("bug interno")

    eventos.registro_atual().emitir = quebrado
    try:
        r.check(eventos.emitir("t", {"a": 1}) == 0, "15.fachada_engole_bug")
    except Exception as e:                           # noqa: BLE001
        r.check(False, "15.fachada_vazou", repr(e))
    eventos.desligar()


def test_16_threads(r):
    reg = Registro(100_000, GRANDE, boot_id=A)
    def trabalhador(k):
        for i in range(5000):
            reg.emitir("t", {"k": k, "i": i})
    ts = [threading.Thread(target=trabalhador, args=(k,)) for k in range(8)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    res = reg.ler(None, 100_000, GRANDE)
    seqs = _seqs(res)
    r.check(seqs == list(range(1, 40_001)), "16.contiguo_ordenado_unico",
            f"n={len(seqs)} primeiro={seqs[:1]} ultimo={seqs[-1:]}")
    por_k = {}
    for c in res["cargas"]:
        d = json.loads(c)["dados"]
        por_k.setdefault(d["k"], []).append(d["i"])
    r.check(all(v == list(range(5000)) for v in por_k.values()) and len(por_k) == 8,
            "16.ordem_por_thread_preservada")


def test_17_sem_io(r):
    reg = Registro(1000, GRANDE, boot_id=A)
    chamadas = []

    def proibido(nome):
        def f(*a, **k):
            chamadas.append(nome)
            raise AssertionError(f"I/O proibido: {nome}")
        return f

    originais = (socket.socket, socket.create_connection, builtins.open, io.open,
                 os.open, sqlite3.connect)
    socket.socket = proibido("socket.socket")
    socket.create_connection = proibido("socket.create_connection")
    builtins.open = proibido("open")
    io.open = proibido("io.open")
    os.open = proibido("os.open")
    sqlite3.connect = proibido("sqlite3.connect")
    try:
        seqs = [reg.emitir("t", {"i": i, "s": "q" * 100}) for i in range(1000)]
        res = reg.ler((A, 500), 1000, GRANDE)
        saude = reg.saude()
    finally:
        (socket.socket, socket.create_connection, builtins.open, io.open,
         os.open, sqlite3.connect) = originais
    r.check(chamadas == [], "17.nenhuma_chamada_de_io", str(chamadas[:5]))
    r.check(seqs == list(range(1, 1001)) and len(res["cargas"]) == 500
            and saude["falhas_emissao"] == 0, "17.funcionou_sem_io")


def test_18_sem_bloqueio(r):
    r.check(not inspect.iscoroutinefunction(eventos.emitir)
            and not inspect.iscoroutinefunction(Registro.emitir), "18.sincrona")
    arvore = ast.parse(inspect.getsource(anel))
    r.check(not any(isinstance(n, (ast.Await, ast.AsyncFunctionDef, ast.AsyncFor,
                                   ast.AsyncWith)) for n in ast.walk(arvore)),
            "18.sem_await_no_anel")
    importados = set()
    for n in ast.walk(arvore):
        if isinstance(n, ast.Import):
            importados |= {a.name.split(".")[0] for a in n.names}
        elif isinstance(n, ast.ImportFrom):
            importados.add((n.module or "").split(".")[0])
    r.check(importados <= {"__future__", "json", "math", "secrets", "threading",
                           "time", "collections", "itertools", "typing"},
            "18.so_stdlib_sem_io", str(sorted(importados)))
    # O(1): custo por evento não cresce com o tamanho do anel
    import time as _t
    reg = Registro(100_000, GRANDE, boot_id=A)
    t0 = _t.perf_counter()
    for i in range(1000):
        reg.emitir("t", {"i": i})
    cedo = _t.perf_counter() - t0
    for i in range(50_000):
        reg.emitir("t", {"i": i})
    t0 = _t.perf_counter()
    for i in range(1000):
        reg.emitir("t", {"i": i})
    tarde = _t.perf_counter() - t0
    r.check(tarde < cedo * 10 + 0.05, "18.custo_nao_cresce_com_o_anel",
            f"1000 cedo={cedo*1e3:.2f}ms tarde={tarde*1e3:.2f}ms")


def test_19_custo_limitado_entrada_patologica(r):
    import time as _t
    profundo = {}
    no = profundo
    for _ in range(5000):                        # 5000 níveis: sem RecursionError
        no["d"] = {}
        no = no["d"]
    casos = {
        "lista_2M": {"l": list(range(2_000_000))},
        "texto_20MB": {"s": "x" * (20 << 20)},
        "dict_300k": {f"k{i}": i for i in range(300_000)},
        "conjunto_1M": {"c": set(range(1_000_000))},
        "conjunto_textos_300k": {"c": {f"id{i}" for i in range(300_000)}},
        "bytes_10MB": {"b": b"\x00" * (10 << 20)},
        "aninhado_5000": profundo,
        "lista_de_listas": {"l": [[[[i] * 200] * 200] * 200 for i in range(200)]},
        "int_enorme": {"n": 10 ** 5000},
        "nao_finitos": {"a": float("nan"), "b": float("inf"), "c": -float("inf")},
        "surrogate": {"s": "a\ud800b", "k\udfff": 1},
        "chaves_estranhas": {(1, 2): "tupla", None: 1, True: 2, 1.5: 3, 7: 4},
    }
    reg = Registro(1000, GRANDE, boot_id=A)
    piores = {}
    import gc
    for nome, dados in casos.items():
        # mede o trabalho do EMISSOR: a coleta de lixo das entradas gigantes
        # deste teste (milhões de objetos vivos) não é custo do emitir
        gc.collect()
        gc.disable()
        try:
            t0 = _t.perf_counter()
            s1 = reg.emitir("patologico", dados, {"caso": nome})
            piores[nome] = (_t.perf_counter() - t0) * 1e3
        finally:
            gc.enable()
        s2 = reg.emitir("patologico", dados, {"caso": nome})
        c1, c2 = (reg.ler((A, s - 1), 1, GRANDE)["cargas"][0] for s in (s1, s2))
        e1, e2 = json.loads(c1), json.loads(c2)
        r.check(s1 > 0 and s2 == s1 + 1, f"19.{nome}.aceito_um_seq_cada", str((s1, s2)))
        r.check(len(c1) <= MAX_EVENTO_BYTES and len(c2) <= MAX_EVENTO_BYTES,
                f"19.{nome}.cabe", str(len(c1)))
        r.check(e1["dados"] == e2["dados"], f"19.{nome}.deterministico")
        c1.decode("utf-8")                       # UTF-8 válido, sempre
    lento = {k: f"{v:.1f}ms" for k, v in piores.items() if v > 50}
    r.check(not lento, "19.custo_limitado_por_evento",
            " ".join(f"{k}={v:.1f}ms" for k, v in piores.items()))
    d = {json.loads(c)["corr"]["caso"]: json.loads(c)["dados"]
         for c in reg.ler(None, 1000, GRANDE)["cargas"]}
    r.check(d["lista_2M"]["l"][-1] == "…+1999800 itens" and len(d["lista_2M"]["l"]) == 201,
            "19.lista_marcada", str(d["lista_2M"]["l"][-1]))
    r.check(d["bytes_10MB"] == {"b": f"<{10 << 20} bytes>"}, "19.bytes_sem_conteudo")
    r.check(d["int_enorme"]["n"].startswith("<int de ")
            and d["nao_finitos"] == {"a": "nan", "b": "inf", "c": "-inf"}, "19.numeros")
    r.check(d["surrogate"]["s"] == "a?b", "19.surrogate_vira_interrogacao",
            str(d["surrogate"]))
    r.check(d["conjunto_1M"] == {"c": "…[conjunto de 1000000 itens]"}
            and reg.emitir("t", {"c": {3, 1, 2}, "s": {"b", "a"}}) > 0
            and json.loads(reg.ler((A, reg.saude()["seq_ultimo"] - 1), 1, GRANDE)
                           ["cargas"][0])["dados"] == {"c": [1, 2, 3], "s": ["a", "b"]},
            "19.conjunto_pequeno_ordenado_grande_marcado")
    r.check(d["chaves_estranhas"] == {"<tuple>": "tupla", "null": 1, "true": 2,
                                      "1.5": 3, "7": 4},
            "19.chaves_como_json", str(d["chaves_estranhas"]))
    r.check(reg.saude()["falhas_emissao"] == 0, "19.nenhuma_falha")


def test_20_teto_rigido(r):
    reg = Registro(100, GRANDE, boot_id=A)
    r.check(reg.emitir("t", {"i": 0}) == 1, "20.primeiro")
    original = anel.MAX_EVENTO_BYTES
    anel.MAX_EVENTO_BYTES = 60                   # nem o envelope cabe
    try:
        s = reg.emitir("t", {"x": "y" * 100})
    finally:
        anel.MAX_EVENTO_BYTES = original
    r.check(s == 0, "20.recusado", str(s))
    r.check(reg.saude()["seq_ultimo"] == 1 and reg.saude()["falhas_emissao"] == 1
            and reg.saude()["eventos_anel"] == 1, "20.sem_seq_e_sem_evento",
            str(reg.saude()))
    r.check(reg.emitir("t", {"i": 1}) == 2, "20.segue_contiguo")


def test_21_boot_id_valido(r):
    for ruim in ("", "abc", "A" * 16, "g" * 16, "a" * 65, "a" * 7, "a" * 15 + ":"):
        try:
            Registro(10, GRANDE, boot_id=ruim)
            ok = ruim == ""                       # vazio → gera um novo
        except ValueError:
            ok = ruim != ""
        r.check(ok, "21.recusa_boot_invalido", repr(ruim))
    reg = Registro(10, GRANDE)
    reg.emitir("t")
    r.check(ler_cursor(reg.ler(None, 1, GRANDE)["meta"]["cursor"]) == (reg.boot_id, 1),
            "21.cursor_do_registro_sempre_legivel")


def _ler_ingenuo(reg, cursor, limite, max_bytes):
    """Referência: a mesma leitura feita do jeito mais simples possível."""
    itens = list(reg._anel)
    primeiro = itens[0][0] if itens else reg._ultimo + 1
    if cursor is None or cursor[0] != reg.boot_id or cursor[1] > reg._ultimo:
        desde = 0
    else:
        desde = cursor[1]
    cargas, total, ultimo = [], 0, max(desde, primeiro - 1)
    for seq, carga in itens:
        if seq <= desde:
            continue
        if len(cargas) >= limite or (cargas and total + len(carga) > max_bytes):
            break
        cargas.append(carga)
        total += len(carga)
        ultimo = seq
    return cargas, f"{reg.boot_id}:{ultimo}"


def test_22_leitura_referencia_e_custo(r):
    import random
    import time as _t
    aleatorio = random.Random(20261006)
    divergencias = []
    for cap in (1, 2, 7, 64, 1000):
        reg = Registro(cap, GRANDE, boot_id=A)
        for i in range(aleatorio.randint(0, 3 * cap + 5)):
            reg.emitir("t", {"i": i, "pad": "p" * aleatorio.randint(0, 300)})
        ultimo = reg.saude()["seq_ultimo"]
        for _ in range(400):
            escolha = aleatorio.random()
            if escolha < 0.1:
                cursor = None
            elif escolha < 0.2:
                cursor = (B, aleatorio.randint(0, ultimo + 5))
            else:
                cursor = (A, aleatorio.randint(0, ultimo + 3))
            limite = aleatorio.choice((1, 2, 3, 10, 1000))
            max_bytes = aleatorio.choice((1, 200, 500, 5000, GRANDE))
            res = reg.ler(cursor, limite, max_bytes)
            esperado = _ler_ingenuo(reg, cursor, limite, max_bytes)
            if (res["cargas"], res["meta"]["cursor"]) != esperado:
                divergencias.append((cap, cursor, limite, max_bytes))
    r.check(not divergencias, "22.identica_a_referencia", str(divergencias[:3]))

    def custo_no_fim(cap):
        reg = Registro(cap, GRANDE, boot_id=A)
        for i in range(cap):
            reg.emitir("t", {"i": i})
        reg.emitir("t", {"i": "novo"})
        cursor = (A, cap)                        # Brain em dia: falta 1 evento
        t0 = _t.perf_counter()
        for _ in range(200):
            res = reg.ler(cursor, 1000, GRANDE)
        assert [json.loads(c)["seq"] for c in res["cargas"]] == [cap + 1]
        return (_t.perf_counter() - t0) / 200
    pequeno, grande = custo_no_fim(1_000), custo_no_fim(100_000)
    r.check(grande < pequeno * 10 + 0.0005, "22.fim_do_anel_O_novos",
            f"anel 1k={pequeno*1e6:.1f}µs anel 100k={grande*1e6:.1f}µs")


def test_23_observabilidade_do_anel(r):
    import random
    aleatorio = random.Random(23)
    for cap, cap_bytes in ((50, GRANDE), (100_000, 1 << 20), (300, 2 << 20)):
        reg = Registro(cap, cap_bytes, boot_id=A)
        pior = 0
        for i in range(2_000):
            reg.emitir("t", {"i": i, "pad": "p" * aleatorio.randint(0, 6_000)})
            s = reg.saude()
            pior = max(pior, s["bytes_anel"])
            if s["bytes_anel"] != sum(len(c) for _, c in reg._anel):
                r.check(False, "23.soma_exata", f"cap={cap} i={i}")
                break
        s = reg.saude()
        r.check(pior <= s["max_bytes_anel"] == max(MAX_EVENTO_BYTES, cap_bytes)
                and s["max_eventos_anel"] == cap and s["eventos_anel"] <= cap,
                f"23.limites_{cap}", str(s))
        r.check(s["descartados"] == s["descartados_por_eventos"] + s["descartados_por_bytes"]
                == 2_000 - s["eventos_anel"], f"23.descarte_por_causa_{cap}", str(s))
        if cap == 50:
            r.check(s["descartados_por_bytes"] == 0, "23.quantidade_manda", str(s))
        if cap == 100_000:
            r.check(s["descartados_por_eventos"] == 0 and s["descartados_por_bytes"] > 0,
                    "23.bytes_manda", str(s))
    campos = {"seq_primeiro", "seq_ultimo", "eventos_anel", "max_eventos_anel",
              "bytes_anel", "max_bytes_anel", "descartados", "descartados_por_eventos",
              "descartados_por_bytes", "falhas_emissao", "excedidos"}
    r.check(campos <= set(Registro(10, GRANDE, boot_id=A).saude()), "23.campos")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "EVENTOS · registro, anel e cursor · F1.1"))
