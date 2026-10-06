"""
F1.2-A1 — COLETA SEGURA (eventos/coleta.py): o evento nunca derruba o
pipeline (T2) e o fim de execução sai exatamente uma vez.

  01  desligado: o coletor NÃO roda; execucao/adiar/transferir são no-op
  02  coletor que levanta (inclusive CancelledError e GeneratorExit por
      engano) → evento degradado do MESMO tipo, com local e erro_tipo;
      nada propaga; a mensagem da exceção nunca entra
  03  só KeyboardInterrupt e SystemExit atravessam (encerramento)
  04  retorno inválido do coletor → degradado; (None, None) → vazio
  05  `minimo`: ids de correlação quando o coletor falha; mínimo que
      falha → corr vazio; ids da execução entram mesmo assim
  06  valores hostis → primitivos, NUNCA str(obj): __str__ que vaza ou
      levanta, __class__ forjado, subclasses com métodos sobrescritos,
      NamedTuple, IntEnum, conjunto misto, gerador, autorreferência,
      bytes, NaN, int gigante, chaves estranhas
  07  falha INTERNA em cada estágio (contexto, saneamento, máscara,
      ajuste, entrega, anel) → nada propaga; o último recurso registra
      o fato quando o anel ainda aceita
  08  execução: exec; exec_pai na chamada derivada (task filha); ids que
      levantam; contexto restaurado; chaves reservadas não se forjam
  09  execucao.fim exatamente uma vez, sem fila: descarte, erro,
      cancelamento, sem desfecho, encerramento repetido
  10  execucao.fim exatamente uma vez, transferida à task: término
      normal, cancelada antes de rodar, desfecho marcado (ERRO), espera_ms
  11  adiamento: captura (cópia) no escopo e despejo na saída, em ordem;
      exceção/cancelamento despejam e propagam iguais; task filha emite
      direto; aninhado despeja uma vez; estouro emite já; desligado no
      meio descarta sem erro; fim de execução depois dos capturados
  12  prova_banco: só ok is True confirma; parcial; sem escrita; o resto
      é nao_verificavel — nunca confirmado
  13  fluxo simulado de uma oferta (entrada → fila → publicação): anel
      desligado, ligado, coletores sabotados e estágios internos
      sabotados → o caminho e os efeitos da oferta são idênticos
  14  fachada: exportações REAIS (sem substituto), catálogo e contadores;
      import SEM rede: erro de programação em coleta.py ou catalogo.py
      (subprocesso, cópia do pacote) quebra o import com o tipo
      verdadeiro; a dormência vem só do caminho rápido (desligado,
      nenhum estágio interno roda e nenhum contador muda)
  15  custo desligado: limite folgado (sanidade, não benchmark)

    python tests/test_eventos_coleta.py
"""
from __future__ import annotations

import ast
import asyncio
import enum
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import NamedTuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import RAIZ, preparar, rodar  # noqa: E402
preparar()

import eventos                                   # noqa: E402
from eventos import anel, catalogo, coleta       # noqa: E402

SEGREDO = "CANARIO-SEGREDO-7f3a"


def _anel():
    eventos.desligar()
    return eventos.instalar(20000, 32 << 20)


def _lidos(reg):
    return [json.loads(c) for c in reg.ler(None, 100000, 1 << 30)["cargas"]]


def _bruto(reg) -> bytes:
    return b"".join(reg.ler(None, 100000, 1 << 30)["cargas"])


def _levanta(e):
    raise e


def _fins(reg):
    return [e for e in _lidos(reg) if e["tipo"] == "execucao.fim"]


def test_01_desligado_nada_roda(r):
    eventos.desligar()
    chamadas = []
    eventos.emitir_de("origem.recebida", lambda: chamadas.append(1) or ({}, {}),
                      local="t.01", minimo=lambda: chamadas.append(2) or {})
    r.check(chamadas == [], "01.coletor_nao_roda", str(chamadas))
    with eventos.execucao(lambda: chamadas.append(3) or {}):
        r.check(coleta.exec_atual() is None, "01.sem_exec")
        with eventos.adiar():
            eventos.emitir_de("post.publicado", lambda: chamadas.append(4) or ({}, {}),
                              local="t.01")
    eventos.marcar_desfecho("ERRO")
    eventos.inicio_na_fila()

    async def corpo():
        t = asyncio.get_running_loop().create_task(asyncio.sleep(0))
        eventos.transferir_execucao(t)
        await t
    asyncio.run(corpo())
    r.check(chamadas == [], "01.nada_rodou_nem_no_contexto", str(chamadas))
    r.check(coleta._EXEC.get() is None and coleta._ADIAMENTO.get() is None,
            "01.contexto_limpo")


def test_02_coletor_que_levanta_vira_degradado(r):
    reg = _anel()
    casos = [AttributeError("x"), KeyError("k"), TypeError("t"), ValueError("v"),
             ZeroDivisionError("z"), RecursionError("r"), MemoryError(),
             IndexError("i"), UnicodeDecodeError("utf-8", b"\xff", 0, 1, "ruim"),
             StopIteration(), asyncio.CancelledError(), GeneratorExit(),
             RuntimeError(SEGREDO)]
    antes = eventos.saude_coleta()["falhas_coleta"]
    propagou = []
    for e in casos:
        try:
            eventos.emitir_de("origem.recebida", lambda e=e: _levanta(e), local="t.02")
        except BaseException as x:                     # noqa: BLE001
            propagou.append(type(x).__name__)
    ev = _lidos(reg)
    r.check(propagou == [], "02.nada_propaga", str(propagou))
    r.check(len(ev) == len(casos) and all(e["tipo"] == "origem.recebida" for e in ev),
            "02.mesmo_tipo_um_por_falha", str([e["tipo"] for e in ev]))
    r.check([e["dados"]["erro_tipo"] for e in ev] == [type(e).__name__ for e in casos]
            and all(e["dados"]["degradado"] is True and e["dados"]["local"] == "t.02"
                    for e in ev), "02.degradado_com_local_e_classe")
    r.check(eventos.saude_coleta()["falhas_coleta"] - antes == len(casos), "02.contador")
    r.check(SEGREDO.encode() not in _bruto(reg), "02.mensagem_da_excecao_nao_entra")


def test_03_so_encerramento_atravessa(r):
    reg = _anel()
    for exc in (SystemExit(3), KeyboardInterrupt()):
        try:
            eventos.emitir_de("origem.recebida", lambda exc=exc: _levanta(exc), local="t.03")
            passou = False
        except type(exc):
            passou = True
        r.check(passou, f"03.{type(exc).__name__}_atravessa")
    r.check(_lidos(reg) == [], "03.nada_emitido")


def test_04_retorno_invalido(r):
    reg = _anel()
    for coletor in (lambda: 5, lambda: (1, 2, 3), lambda: ([], {}), lambda: ({}, "x"),
                    lambda: iter([{}])):
        eventos.emitir_de("origem.recebida", coletor, local="t.04")
    eventos.emitir_de("origem.recebida", lambda: (None, None), local="t.04.vazio")
    eventos.emitir_de("origem.recebida", lambda: ({"msg": 1}, None), local="t.04.corr")
    ev = _lidos(reg)
    r.check(len(ev) == 7 and all(e["dados"].get("degradado") is True for e in ev[:5]),
            "04.invalidos_degradados", str([e["dados"] for e in ev[:5]]))
    r.check(ev[5]["dados"].get("degradado") is None and ev[5]["corr"] == {}
            and set(ev[5]["dados"]) == {"local", "ts_fato"}, "04.none_none_vazio",
            str(ev[5]))
    r.check(ev[6]["corr"] == {"msg": 1}, "04.dados_none_vira_vazio", str(ev[6]))


def test_05_minimo(r):
    reg = _anel()
    eventos.emitir_de("origem.recebida", lambda: 1 / 0, local="t.05.a",
                      minimo=lambda: {"chat": "-100", "msg": 5})
    eventos.emitir_de("origem.recebida", lambda: 1 / 0, local="t.05.b",
                      minimo=lambda: 1 / 0)
    eventos.emitir_de("origem.recebida", lambda: 1 / 0, local="t.05.c",
                      minimo=lambda: [1])
    with eventos.execucao(lambda: {"chat": "-100", "msg": 6}):
        eventos.emitir_de("origem.recebida", lambda: 1 / 0, local="t.05.d",
                          minimo=lambda: 1 / 0)
    ev = [e for e in _lidos(reg) if e["tipo"] == "origem.recebida"]
    r.check(ev[0]["corr"] == {"chat": "-100", "msg": 5}, "05.minimo_usado", str(ev[0]))
    r.check(ev[1]["corr"] == {} and ev[2]["corr"] == {}, "05.minimo_ruim_vazio")
    r.check(ev[3]["corr"].get("chat") == "-100" and ev[3]["corr"].get("msg") == 6
            and isinstance(ev[3]["corr"].get("exec"), int)
            and ev[3]["dados"]["degradado"] is True, "05.ids_da_execucao_no_degradado",
            str(ev[3]))


class _Res(NamedTuple):
    ok: bool
    etapa: object


class _Nivel(enum.IntEnum):
    ALTO = 7


def test_06_valores_hostis(r):
    reg = _anel()

    class Vaza:
        access_hash = 987654321

        def __repr__(self):
            return f"Message(access_hash=987654321, file_reference=b'{SEGREDO}')"
        __str__ = __repr__

    class Explode:
        def __str__(self):
            raise RuntimeError(SEGREDO)
        __repr__ = __str__

    class Forjado:
        @property
        def __class__(self):
            return str

    class TextoRuim(str):
        def __str__(self):
            raise RuntimeError(SEGREDO)

        def __len__(self):
            raise RuntimeError(SEGREDO)

    class DictRuim(dict):
        def items(self):
            raise RuntimeError(SEGREDO)

        def __len__(self):
            raise RuntimeError(SEGREDO)

        def __iter__(self):
            raise RuntimeError(SEGREDO)

    class ListaRuim(list):
        def __iter__(self):
            raise RuntimeError(SEGREDO)

        def __len__(self):
            raise RuntimeError(SEGREDO)

    class FloatRuim(float):
        def __repr__(self):
            return SEGREDO

    auto_lista = []
    auto_lista.append(auto_lista)
    auto_dict = {}
    auto_dict["eu"] = auto_dict
    dados = {
        "vaza": Vaza(), "explode": Explode(), "forjado": Forjado(),
        "texto_ruim": TextoRuim("conteudo"), "dict_ruim": DictRuim(a=1),
        "lista_ruim": ListaRuim([1, 2]), "float_ruim": FloatRuim(1.5),
        "nt": _Res(True, None), "enum": _Nivel.ALTO,
        "misto": {1, "a"}, "inteiros": {3, 1, 2}, "textos": frozenset({"b", "a"}),
        "gerador": (x for x in []), "auto_lista": auto_lista, "auto_dict": auto_dict,
        "bytes": SEGREDO.encode(), "bytearray": bytearray(b"abc"),
        "memoria": memoryview(b"abcd"), "nan": float("nan"), "inf": float("inf"),
        "-inf": float("-inf"), "gigante": 10 ** 100, "excecao": ValueError(SEGREDO),
        "classe": Vaza, "funcao": _levanta,
        "chaves": {2: "int", None: "none", False: "bool", 1.5: "float", (1, 2): "tupla",
                   Vaza(): "obj", "k" * 200: "longa"},
    }
    propagou = None
    try:
        eventos.emitir_de("origem.recebida", lambda: ({"msg": 1}, dados), local="t.06")
    except BaseException as x:                         # noqa: BLE001
        propagou = x
    r.check(propagou is None, "06.nada_propaga", repr(propagou))
    ev = _lidos(reg)
    r.check(len(ev) == 1 and not ev[0]["dados"].get("degradado"), "06.um_evento_inteiro",
            str(ev))
    d = ev[0]["dados"] if ev else {}
    r.check(d.get("vaza") == "<objeto:Vaza>" and d.get("explode") == "<objeto:Explode>"
            and d.get("forjado") == "<objeto:Forjado>", "06.objetos_viram_nome_da_classe",
            str({k: d.get(k) for k in ("vaza", "explode", "forjado")}))
    r.check(d.get("texto_ruim") == "conteudo" and d.get("dict_ruim") == {"a": 1}
            and d.get("lista_ruim") == [1, 2] and d.get("float_ruim") == 1.5,
            "06.subclasses_pela_base", str({k: d.get(k) for k in
                                            ("texto_ruim", "dict_ruim", "lista_ruim")}))
    r.check(d.get("nt") == {"ok": True, "etapa": None} and d.get("enum") == 7,
            "06.namedtuple_e_enum", str((d.get("nt"), d.get("enum"))))
    r.check(d.get("misto") == "<conjunto:2>" and d.get("inteiros") == [1, 2, 3]
            and d.get("textos") == ["a", "b"], "06.conjuntos")
    r.check(d.get("gerador") == "<objeto:generator>"
            and d.get("auto_lista") == [[["<lista:1>"]]]
            and d.get("auto_dict") == {"eu": {"eu": {"eu": "<dict:1>"}}},
            "06.gerador_e_autorreferencia", str((d.get("auto_lista"), d.get("auto_dict"))))
    r.check(d.get("bytes") == f"<bytes:{len(SEGREDO)}>" and d.get("bytearray") == "<bytes:3>"
            and d.get("memoria") == "<bytes:4>", "06.bytes_sem_conteudo")
    r.check(d.get("nan") == "nan" and d.get("inf") == "inf" and d.get("-inf") == "-inf"
            and d.get("gigante") == "<int:333 bits>", "06.numeros")
    r.check(d.get("excecao") == "<objeto:ValueError>" and d.get("classe") == "<objeto:type>"
            and d.get("funcao") == "<objeto:function>", "06.excecao_classe_funcao")
    chaves = d.get("chaves", {})
    r.check(set(chaves) == {"2", "null", "false", "1.5", "<chave:tuple>", "<chave:Vaza>",
                            "k" * 63 + "…"}, "06.chaves", str(sorted(chaves)))
    bruto = _bruto(reg)
    r.check(SEGREDO.encode() not in bruto and b"987654321" not in bruto
            and b"access_hash" not in bruto and b"file_reference" not in bruto,
            "06.nada_de_str_do_objeto")


def test_07_falha_interna_em_cada_estagio(r):
    reg = _anel()
    sabotaveis = ["_rodar_coletor", "_raiz", "_capturar", "_entregar", "_despachar",
                  "_proteger", "_ajustar_dados", "_ajustar_corr", "_medir",
                  "mascarar_urls"]
    for nome in sabotaveis:
        original = getattr(coleta, nome)
        setattr(coleta, nome, lambda *a, **k: _levanta(RuntimeError(SEGREDO)))
        antes = eventos.saude_coleta()["falhas_internas"]
        seq0 = reg.saude()["seq_ultimo"]
        propagou = None
        try:
            eventos.emitir_de("origem.recebida",
                              lambda: ({"msg": 1}, {"t": "https://amzn.to/3xYz9"}),
                              local=f"t.07.{nome}")
        except BaseException as x:                     # noqa: BLE001
            propagou = x
        finally:
            setattr(coleta, nome, original)
        novos = [e for e in _lidos(reg) if e["seq"] > seq0]
        r.check(propagou is None, f"07.{nome}.nada_propaga", repr(propagou))
        r.check(eventos.saude_coleta()["falhas_internas"] - antes == 1,
                f"07.{nome}.contado")
        r.check(len(novos) == 1 and novos[0]["dados"] == {
            "degradado": True, "erro_tipo": "INTERNO", "local": f"t.07.{nome}"},
            f"07.{nome}.ultimo_recurso", str(novos))
    # contexto quebrado: a variável de contexto inteira
    original_exec = coleta._EXEC

    class Quebrado:
        def get(self, *a):
            raise RuntimeError(SEGREDO)

        def set(self, *a):
            raise RuntimeError(SEGREDO)

        def reset(self, *a):
            raise RuntimeError(SEGREDO)
    coleta._EXEC = Quebrado()
    try:
        with eventos.execucao(lambda: {"msg": 2}):
            eventos.emitir_de("origem.recebida", lambda: ({}, {}), local="t.07.contexto")
            eventos.inicio_na_fila()
            eventos.marcar_desfecho("ERRO")
        ok_contexto = True
    except BaseException:                              # noqa: BLE001
        ok_contexto = False
    finally:
        coleta._EXEC = original_exec
    r.check(ok_contexto, "07.contexto_quebrado_contido")
    # o próprio anel levanta: nem o último recurso grava, e nada propaga
    original_emitir = anel.Registro.emitir
    anel.Registro.emitir = lambda self, *a, **k: _levanta(RuntimeError(SEGREDO))
    try:
        eventos.emitir_de("origem.recebida", lambda: ({}, {}), local="t.07.anel")
        ok_anel = True
    except BaseException:                              # noqa: BLE001
        ok_anel = False
    finally:
        anel.Registro.emitir = original_emitir
    r.check(ok_anel, "07.anel_quebrado_contido")
    r.check(SEGREDO.encode() not in _bruto(reg), "07.mensagem_interna_nao_entra")


def test_08_execucao_exec_e_pai(r):
    reg = _anel()
    visto = {}

    async def corpo():
        with eventos.execucao(lambda: {"chat": "-1", "msg": 1}):
            visto["mae"] = coleta.exec_atual()
            eventos.emitir_de("origem.recebida",
                              lambda: ({"exec": 999, "exec_pai": 888, "post": 3}, {}),
                              local="t.08.mae")

            async def derivada():                      # ex.: sucessão → processar
                with eventos.execucao(lambda: {"chat": "-2", "msg": 2}):
                    visto["filha"] = coleta.exec_atual()
                    eventos.emitir_de("origem.recebida", lambda: ({}, {}),
                                      local="t.08.filha")
            await asyncio.get_running_loop().create_task(derivada())
            visto["depois_da_filha"] = coleta.exec_atual()
        visto["fora"] = coleta.exec_atual()
    asyncio.run(corpo())
    with eventos.execucao(lambda: 1 / 0):
        visto["ids_ruins"] = coleta.exec_atual()
        eventos.emitir_de("origem.recebida", lambda: ({}, {}), local="t.08.ids")
    eventos.emitir_de("origem.recebida", lambda: ({"exec": 7, "exec_pai": 8}, {}),
                      local="t.08.fora")
    ev = {e["dados"]["local"]: e for e in _lidos(reg) if e["tipo"] == "origem.recebida"}
    mae, filha = visto["mae"], visto["filha"]
    r.check(isinstance(mae, int) and isinstance(filha, int) and filha != mae,
            "08.ids_distintos", str(visto))
    r.check(ev["t.08.mae"]["corr"] == {"chat": "-1", "msg": 1, "post": 3, "exec": mae},
            "08.reservadas_nao_se_forjam", str(ev["t.08.mae"]["corr"]))
    r.check(ev["t.08.filha"]["corr"] == {"chat": "-2", "msg": 2, "exec": filha,
                                         "exec_pai": mae}, "08.exec_pai_na_derivada",
            str(ev["t.08.filha"]["corr"]))
    r.check(visto["depois_da_filha"] == mae and visto["fora"] is None,
            "08.contexto_restaurado", str(visto))
    r.check(isinstance(visto["ids_ruins"], int)
            and set(ev["t.08.ids"]["corr"]) == {"exec"}, "08.ids_que_levantam",
            str(ev["t.08.ids"]["corr"]))
    r.check(ev["t.08.fora"]["corr"] == {}, "08.reservadas_fora_de_execucao",
            str(ev["t.08.fora"]["corr"]))


def test_09_fim_exatamente_uma_vez_sem_fila(r):
    reg = _anel()
    resultados = {}

    def rodar(rotulo, corpo):
        with eventos.execucao(lambda: {"msg": rotulo}):
            resultados[rotulo] = coleta.exec_atual()
            corpo()

    rodar("descarte", lambda: eventos.emitir_de(
        "origem.descartada", lambda: ({}, {"motivo": "NOVA_ANTIGA"}), local="t.09"))
    try:
        rodar("erro", lambda: _levanta(ValueError(SEGREDO)))
    except ValueError:
        pass
    rodar("nada", lambda: None)

    async def cancelada():
        with eventos.execucao(lambda: {"msg": "cancelada"}):
            resultados["cancelada"] = coleta.exec_atual()
            await asyncio.sleep(10)

    async def corpo():
        t = asyncio.get_running_loop().create_task(cancelada())
        await asyncio.sleep(0)
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
    asyncio.run(corpo())
    with eventos.execucao(lambda: {"msg": "repetido"}):
        est = coleta._EXEC.get()
        resultados["repetido"] = est.id
    coleta._encerrar(est)
    coleta._encerrar(est, "ERRO")
    fins = _fins(reg)
    por_exec = {}
    for f in fins:
        por_exec.setdefault(f["corr"]["exec"], []).append(f)
    r.check(all(len(por_exec.get(i, [])) == 1 for i in resultados.values()),
            "09.um_fim_por_execucao", str({k: len(por_exec.get(v, []))
                                           for k, v in resultados.items()}))

    def res(rotulo):
        return por_exec[resultados[rotulo]][0]["dados"]
    r.check(res("descarte")["resultado"] == "DESCARTADA"
            and res("descarte")["eventos"] == 1, "09.descarte_antes_da_fila")
    r.check(res("erro")["resultado"] == "ERRO" and res("erro")["erro_tipo"] == "ValueError",
            "09.erro_de_admissao", str(res("erro")))
    r.check(res("nada")["resultado"] == "SEM_DESFECHO", "09.sem_desfecho")
    r.check(res("cancelada")["resultado"] == "CANCELADA", "09.cancelamento")
    r.check(res("repetido")["resultado"] == "SEM_DESFECHO", "09.encerrar_repetido_ignorado")
    r.check(all(f["corr"]["msg"] in resultados and "duracao_ms" in f["dados"]
                for f in fins), "09.fim_com_ids_e_duracao")
    r.check(SEGREDO.encode() not in _bruto(reg), "09.mensagem_do_erro_nao_entra")


def test_10_fim_exatamente_uma_vez_transferida(r):
    reg = _anel()
    ids = {}

    async def admitir(rotulo, lane):
        with eventos.execucao(lambda: {"msg": rotulo}):
            ids[rotulo] = coleta.exec_atual()
            t = asyncio.get_running_loop().create_task(lane())
            eventos.transferir_execucao(t)
        return t

    async def normal():
        eventos.inicio_na_fila()
        await asyncio.sleep(0.01)
        eventos.emitir_de("post.publicado", lambda: ({"post": 1}, {}), local="t.10")

    async def blindada():                              # o que _blindado faz
        try:
            await asyncio.sleep(0)
            raise RuntimeError(SEGREDO)
        except Exception as e:                         # noqa: BLE001
            eventos.marcar_desfecho("ERRO", erro=e)

    async def corpo():
        t1 = await admitir("normal", normal)
        t2 = await admitir("cancelada", lambda: asyncio.sleep(10))
        t2.cancel()
        t3 = await admitir("erro", blindada)
        await asyncio.gather(t1, t2, t3, return_exceptions=True)
        await asyncio.sleep(0)
    asyncio.run(corpo())
    ev = _lidos(reg)
    fins = [e for e in ev if e["tipo"] == "execucao.fim"]
    por_exec = {}
    for f in fins:
        por_exec.setdefault(f["corr"]["exec"], []).append(f["dados"])
    r.check(all(len(por_exec.get(i, [])) == 1 for i in ids.values()),
            "10.um_fim_por_execucao", str(por_exec))
    n = por_exec[ids["normal"]][0]
    r.check(n["resultado"] == "PUBLICADA" and "espera_ms" in n and n["eventos"] == 1,
            "10.termino_normal", str(n))
    r.check(por_exec[ids["cancelada"]][0]["resultado"] == "CANCELADA",
            "10.cancelada_antes_de_rodar")
    e = por_exec[ids["erro"]][0]
    r.check(e["resultado"] == "ERRO" and e["erro_tipo"] == "RuntimeError",
            "10.desfecho_marcado", str(e))
    seq_pub = next(x["seq"] for x in ev if x["tipo"] == "post.publicado")
    seq_fim = next(x["seq"] for x in fins if x["corr"]["exec"] == ids["normal"])
    r.check(seq_pub < seq_fim, "10.fim_depois_dos_eventos_da_execucao")
    r.check(SEGREDO.encode() not in _bruto(reg), "10.mensagem_nao_entra")


def test_11_adiamento(r):
    reg = _anel()
    obs = {}

    async def corpo():
        lista = ["a"]
        with eventos.adiar():
            eventos.emitir_de("decisao.tomada", lambda: ({}, {"lista": lista}),
                              local="t.11.um")
            eventos.emitir_de("post.publicado", lambda: ({}, {}), local="t.11.dois")
            lista.append("depois")                     # mutação pós-captura
            obs["durante"] = len(_lidos(reg))

            async def filha():
                eventos.emitir_de("espelho.publicado", lambda: ({}, {}),
                                  local="t.11.filha")
            await asyncio.get_running_loop().create_task(filha())
            obs["filha_ja_no_anel"] = len(_lidos(reg))
        obs["depois"] = [e["dados"]["local"] for e in _lidos(reg)]
        obs["lista"] = next(e["dados"]["lista"] for e in _lidos(reg)
                            if e["dados"]["local"] == "t.11.um")
    asyncio.run(corpo())
    r.check(obs["durante"] == 0 and obs["filha_ja_no_anel"] == 1,
            "11.captura_e_filha_direto", str(obs))
    r.check(obs["depois"] == ["t.11.filha", "t.11.um", "t.11.dois"], "11.despejo_em_ordem",
            str(obs["depois"]))
    r.check(obs["lista"] == ["a"], "11.copia_no_instante", str(obs["lista"]))

    # exceção e cancelamento: despejam e propagam iguais
    reg = _anel()
    erro = ValueError("x")
    try:
        with eventos.adiar():
            eventos.emitir_de("post.publicado", lambda: ({}, {}), local="t.11.exc")
            raise erro
    except ValueError as e:
        mesma = e is erro
    r.check(mesma and [e["dados"]["local"] for e in _lidos(reg)] == ["t.11.exc"],
            "11.excecao_despeja_e_propaga")

    async def cancelavel():
        with eventos.adiar():
            eventos.emitir_de("post.publicado", lambda: ({}, {}), local="t.11.cancel")
            await asyncio.sleep(10)

    async def cancela():
        t = asyncio.get_running_loop().create_task(cancelavel())
        await asyncio.sleep(0)
        t.cancel()
        res = await asyncio.gather(t, return_exceptions=True)
        return res[0]
    res = asyncio.run(cancela())
    r.check(isinstance(res, asyncio.CancelledError)
            and "t.11.cancel" in [e["dados"]["local"] for e in _lidos(reg)],
            "11.cancelamento_despeja_e_propaga")

    # aninhado: um despejo só, no de fora
    reg = _anel()
    with eventos.adiar():
        with eventos.adiar():
            eventos.emitir_de("post.publicado", lambda: ({}, {}), local="t.11.ninho")
        dentro = len(_lidos(reg))
    r.check(dentro == 0 and len(_lidos(reg)) == 1, "11.aninhado_um_despejo")

    # estouro: o excedente sai na hora; nada se perde
    reg = _anel()
    antes = eventos.saude_coleta()["adiamento_estourado"]
    with eventos.adiar():
        for i in range(70):
            eventos.emitir_de("post.publicado", lambda i=i: ({}, {"i": i}), local="t.11.estouro")
        no_anel = len(_lidos(reg))
    finais = [e["dados"]["i"] for e in _lidos(reg)]
    r.check(no_anel == 6 and len(finais) == 70 and sorted(finais) == list(range(70))
            and eventos.saude_coleta()["adiamento_estourado"] - antes == 6,
            "11.estouro_emite_ja", f"no_anel={no_anel} total={len(finais)}")

    # desligado entre a captura e o despejo: descarta sem erro
    _anel()
    try:
        with eventos.adiar():
            eventos.emitir_de("post.publicado", lambda: ({}, {}), local="t.11.desliga")
            eventos.desligar()
        ok = True
    except BaseException:                              # noqa: BLE001
        ok = False
    r.check(ok, "11.desligado_no_meio")

    # execucao.fim dentro do escopo vem depois dos capturados
    reg = _anel()
    with eventos.adiar():
        with eventos.execucao(lambda: {"msg": 1}):
            eventos.emitir_de("post.publicado", lambda: ({}, {}), local="t.11.ordem")
    tipos = [e["tipo"] for e in _lidos(reg)]
    r.check(tipos == ["post.publicado", "execucao.fim"], "11.fim_depois", str(tipos))


def test_12_prova_banco(r):
    class G:
        def __init__(self, **k):
            self.__dict__.update(k)

    class Quebrado:
        @property
        def ok(self):
            raise RuntimeError("x")

    casos = [
        (None, "nao_verificavel"), (True, "nao_verificavel"), (False, "nao_verificavel"),
        (G(ok=True), "confirmado"),
        (G(ok=True, efeitos_parciais=True), "confirmado"),
        (G(ok=False, etapas_ok=("post_estado",), efeitos_parciais=True), "falhou_parcial"),
        (G(ok=False, etapas_ok=("post_estado",)), "falhou_parcial"),
        (G(ok=False, efeitos_parciais=True), "falhou_parcial"),
        (G(ok=False, etapas_ok=(), efeitos_parciais=False), "falhou_sem_escrita"),
        (G(ok=False, efeitos_parciais=False), "falhou_sem_escrita"),
        (G(ok=False, etapas_ok=[]), "falhou_sem_escrita"),
        (G(ok=False, etapas_ok=("x",), efeitos_parciais=False), "falhou_parcial"),
        (G(ok=False), "nao_verificavel"),
        (G(ok=1), "nao_verificavel"), (G(ok="sim"), "nao_verificavel"),
        (G(), "nao_verificavel"), (Quebrado(), "nao_verificavel"),
        (_Res(True, None), "confirmado"), ({"ok": True}, "nao_verificavel"),
    ]
    erradas = [(repr(getattr(c, "__dict__", c)), coleta.prova_banco(c), esperado)
               for c, esperado in casos if coleta.prova_banco(c) != esperado]
    r.check(erradas == [], "12.tabela", str(erradas))
    r.check({coleta.prova_banco(c) for c, _ in casos}
            <= catalogo.ENUMS["prova_banco"], "12.so_valores_do_catalogo")


def test_13_fluxo_simulado_identico(r):
    """Uma 'oferta' passando pelos mesmos tipos de ponto que o pipeline
    terá: entrada (execução), descarte possível, fila (task + transferência),
    decisão e publicação sob adiamento. O efeito é a lista de passos."""

    async def oferta(efeitos, coletor):
        with eventos.execucao(lambda: coletor({"chat": "-100", "msg": 42})):
            eventos.emitir_de("origem.recebida", lambda: coletor(({}, {"texto": "x"})),
                              local="t.13.entrada")
            efeitos.append("recebida")

            async def lane():
                eventos.inicio_na_fila()
                efeitos.append("na_fila")
                with eventos.adiar():
                    eventos.emitir_de("decisao.tomada",
                                      lambda: coletor(({"post": 1}, {"acao": "PUBLICAR"})),
                                      local="t.13.decisao")
                    efeitos.append("decidiu")
                    await asyncio.sleep(0)             # o I/O do Telegram
                    efeitos.append("publicou")
                    eventos.emitir_de("post.publicado",
                                      lambda: coletor(({"post": 2}, {"texto": "y"})),
                                      local="t.13.post", minimo=lambda: coletor({"post": 2}))
                return "fim"
            t = asyncio.get_running_loop().create_task(lane())
            eventos.transferir_execucao(t)
            efeitos.append("admitida")
        efeitos.append(await t)

    def normal(x):
        return x

    def sabotado(_x):
        raise RuntimeError(SEGREDO)

    def rodar(modo):
        efeitos = []
        originais = {}
        if modo == "desligado":
            eventos.desligar()
        else:
            _anel()
        if modo == "interno":
            for nome in ("_raiz", "_proteger", "_ajustar_dados"):
                originais[nome] = getattr(coleta, nome)
                setattr(coleta, nome, lambda *a, **k: _levanta(RuntimeError(SEGREDO)))
        try:
            asyncio.run(oferta(efeitos, sabotado if modo == "coletor" else normal))
        finally:
            for nome, f in originais.items():
                setattr(coleta, nome, f)
        return efeitos, eventos.registro_atual()

    base, _ = rodar("desligado")
    ligado, reg_l = rodar("ligado")
    col, reg_c = rodar("coletor")
    inter, reg_i = rodar("interno")
    r.check(base == ["recebida", "admitida", "na_fila", "decidiu", "publicou", "fim"],
            "13.caminho_da_oferta", str(base))
    r.check(ligado == base and col == base and inter == base, "13.identico_em_todo_modo",
            str((ligado, col, inter)))
    tipos_l = [e["tipo"] for e in _lidos(reg_l)]
    r.check(tipos_l == ["origem.recebida", "decisao.tomada", "post.publicado",
                        "execucao.fim"], "13.eventos_ligado", str(tipos_l))
    ev_c = _lidos(reg_c)
    r.check([e["tipo"] for e in ev_c] == tipos_l
            and all(e["dados"].get("degradado") for e in ev_c[:3])
            and ev_c[3]["dados"]["resultado"] == "PUBLICADA", "13.coletor_sabotado_degrada",
            str([(e["tipo"], e["dados"]) for e in ev_c]))
    ev_i = _lidos(reg_i)
    r.check(all(e["dados"].get("erro_tipo") == "INTERNO" for e in ev_i[:3]),
            "13.interno_sabotado_ultimo_recurso", str([e["dados"] for e in ev_i]))
    r.check(all(SEGREDO.encode() not in _bruto(x) for x in (reg_l, reg_c, reg_i)),
            "13.nada_vaza")


_ROTEIRO_IMPORT = (
    "import sys\n"
    "sys.path.insert(0, sys.argv[1])\n"
    "if len(sys.argv) > 2:\n"
    "    sys.modules[sys.argv[2]] = None\n"
    "import eventos\n"
    "assert eventos.__file__.startswith(sys.argv[1]), eventos.__file__\n"
    "print('IMPORTOU')\n")


def _importar_copia(pasta: str, *extra: str):
    """`import eventos` num processo novo, isolado (-I), a partir da cópia
    do pacote em `pasta` — nunca do repositório."""
    ambiente = {k: v for k, v in os.environ.items()
                if not k.startswith(("API_PRIVADA", "EVENTOS_", "TELEGRAM"))}
    return subprocess.run([sys.executable, "-I", "-c", _ROTEIRO_IMPORT, pasta, *extra],
                          capture_output=True, text=True, timeout=60, env=ambiente,
                          cwd=pasta)


def test_14_fachada_e_import_sem_rede(r):
    r.check(eventos.emitir_de is coleta.emitir_de and eventos.execucao is coleta.execucao
            and eventos.adiar is coleta.adiar
            and eventos.transferir_execucao is coleta.transferir_execucao
            and eventos.inicio_na_fila is coleta.inicio_na_fila
            and eventos.marcar_desfecho is coleta.marcar_desfecho
            and eventos.saude_coleta is coleta.saude_coleta
            and eventos.catalogo is catalogo
            and not hasattr(eventos, "coleta_disponivel"), "14.exportacoes_reais")
    s = eventos.saude_coleta()
    r.check(set(s) == {"chamadas", "emitidos", "recusados", "falhas_coleta",
                       "falhas_internas", "cortados", "urls_protegidas", "adiados",
                       "adiamento_estourado", "execucoes", "fins"}
            and all(type(v) is int for v in s.values()), "14.contadores", str(s))
    rc = eventos.resumo_catalogo()
    r.check(rc["versao"] == catalogo.VERSAO == 1 and isinstance(rc["hash"], str)
            and len(rc["hash"]) == 16 and rc["tipos"] == len(catalogo.TIPOS),
            "14.resumo_catalogo", str(rc))
    original = catalogo._RESUMO
    catalogo._RESUMO, tipos = None, catalogo.TIPOS
    catalogo.TIPOS = None                              # sabota o cálculo
    try:
        rc = eventos.resumo_catalogo()
    finally:
        catalogo.TIPOS, catalogo._RESUMO = tipos, original
    r.check(rc == {"versao": 1, "hash": None, "tipos": None},
            "14.resumo_catalogo_nunca_levanta", str(rc))

    # Nenhum import do pacote fica dentro de try: erro de programação na
    # coleta ou no catálogo não pode virar no-op silencioso.
    for arq in ("__init__.py", "coleta.py", "catalogo.py"):
        with open(os.path.join(RAIZ, "eventos", arq), encoding="utf-8") as f:
            arvore = ast.parse(f.read())
        presos = [n.lineno for t in ast.walk(arvore)
                  if isinstance(t, (ast.Try, getattr(ast, "TryStar", ast.Try)))
                  for n in ast.walk(t) if isinstance(n, (ast.Import, ast.ImportFrom))]
        r.check(presos == [], f"14.import_fora_de_try.{arq}", str(presos))

    # Erro REAL de programação em coleta.py ou catalogo.py quebra o import
    # (e portanto a suíte e a CI), com o tipo verdadeiro; o pacote íntegro
    # importa (controle); eventos.coleta ausente também quebra (era o caso
    # que o import blindado da primeira versão da A1 calava).
    with tempfile.TemporaryDirectory() as tmp:
        shutil.copytree(os.path.join(RAIZ, "eventos"), os.path.join(tmp, "eventos"),
                        ignore=shutil.ignore_patterns("__pycache__"))
        p = _importar_copia(tmp)
        r.check(p.returncode == 0 and p.stdout.strip() == "IMPORTOU", "14.controle_importa",
                (p.stdout + p.stderr)[-600:])
        p = _importar_copia(tmp, "eventos.coleta")
        r.check(p.returncode != 0 and "ModuleNotFoundError" in p.stderr
                and "IMPORTOU" not in p.stdout, "14.coleta_ausente_quebra",
                (p.stdout + p.stderr)[-600:])
    for arq in ("coleta.py", "catalogo.py"):
        with tempfile.TemporaryDirectory() as tmp:
            shutil.copytree(os.path.join(RAIZ, "eventos"), os.path.join(tmp, "eventos"),
                            ignore=shutil.ignore_patterns("__pycache__"))
            with open(os.path.join(tmp, "eventos", arq), "a", encoding="utf-8") as f:
                f.write("\n_BUG_DE_PROGRAMACAO = nome_que_nao_existe_a1\n")
            p = _importar_copia(tmp)
        r.check(p.returncode != 0 and "NameError" in p.stderr
                and "nome_que_nao_existe_a1" in p.stderr and "IMPORTOU" not in p.stdout,
                f"14.bug_em_{arq}_aparece", (p.stdout + p.stderr)[-600:])

    # Dormência = só o caminho rápido: desligado, nenhum estágio interno
    # roda e nenhum contador muda.
    eventos.desligar()
    tocados = []
    nomes = ("_rodar_coletor", "_capturar", "_entregar", "_despachar", "_ultimo_recurso",
             "_raiz", "_encerrar")
    originais = {n: getattr(coleta, n) for n in nomes}
    try:
        for n in nomes:
            setattr(coleta, n, lambda *_a, _n=n, **_k: tocados.append(_n))
        antes = eventos.saude_coleta()
        eventos.emitir_de("origem.recebida", lambda: tocados.append("coletor") or ({}, {}),
                          local="t.14", minimo=lambda: tocados.append("minimo") or {})
        with eventos.execucao(lambda: tocados.append("ids") or {}):
            with eventos.adiar():
                eventos.marcar_desfecho("ERRO")
                eventos.inicio_na_fila()
                eventos.transferir_execucao(None)
        depois = eventos.saude_coleta()
    finally:
        for n, f in originais.items():
            setattr(coleta, n, f)
    r.check(tocados == [] and antes == depois, "14.dormencia_e_o_caminho_rapido",
            str((tocados, antes, depois)))


def test_15_custo_desligado(r):
    eventos.desligar()
    n = 100_000
    t0 = time.perf_counter()
    for i in range(n):
        eventos.emitir_de("origem.recebida", lambda: ({"msg": i}, {}), local="t.15")
    por_chamada_us = (time.perf_counter() - t0) / n * 1e6
    r.check(por_chamada_us < 20, "15.desligado_barato", f"{por_chamada_us:.3f} µs/chamada")
    r.check(math.isfinite(por_chamada_us), "15.medido")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "EVENTOS · coleta segura (T2) e execução · F1.2-A1"))
