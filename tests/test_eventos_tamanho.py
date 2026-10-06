"""
F1.2-A1 — TAMANHO: todo evento da coleta cabe em 12 KiB por construção e
fica DENTRO dos limites do anel F1.1, que o guarda intacto (sem marca de
corte do anel e com `excedidos` = 0).

  01  texto único de 4096 emojis (16 KiB) → cortado em bytes, ≤ 12 KiB
  02  muitos textos grandes → os maiores são cortados até caber
  03  escapes JSON (controle vale 6 bytes, aspas e barra 2) → mede o
      JSON real, não os caracteres
  04  50 links de ~500 bytes, profundidade, itens, nós e chaves longas
  05  carga que nem cortada cabe → último recurso: degradado TAMANHO com
      a identificação (exec, chat, msg); o lixo grande do corr sai
  06  sorteio determinístico de 400 cargas patológicas (emoji, controle,
      surrogate solto, URL, número enorme, NaN, bytes, aninhamento) →
      todas ≤ 12 KiB, JSON válido, sem corte do anel; a mesma carga dá
      o mesmo evento
  07  eventos comuns continuam pequenos; vazio e surrogate sem erro

    python tests/test_eventos_tamanho.py
"""
from __future__ import annotations

import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar  # noqa: E402
preparar()

import eventos                                   # noqa: E402
from eventos import anel, coleta                 # noqa: E402

TETO = coleta.TETO_EVENTO


def _anel():
    eventos.desligar()
    return eventos.instalar(20000, 64 << 20)


def _emitir(reg, dados, corr=None, tipo="origem.recebida"):
    seq0 = reg.saude()["seq_ultimo"]
    eventos.emitir_de(tipo, lambda: (corr if corr is not None else {"msg": 1}, dados),
                      local="t.tamanho")
    return reg.ler((reg.boot_id, seq0), 100, 1 << 30)["cargas"]


def _confere(r, nome, reg, cargas, cortado=None):
    ok = len(cargas) == 1
    r.check(ok, f"{nome}.um_evento", str(len(cargas)))
    if not ok:
        return {}
    c = cargas[0]
    ev = json.loads(c)
    r.check(len(c) <= TETO, f"{nome}.cabe_em_12KiB", f"{len(c)} bytes")
    r.check(anel._MARCA_CORTE.encode() not in c and reg.saude()["excedidos"] == 0,
            f"{nome}.anel_guarda_intacto")
    if cortado is not None:
        r.check(bool(ev["dados"].get("cortado")) == cortado, f"{nome}.marca_cortado",
                str(ev["dados"].get("cortado")))
    return ev


def test_01_texto_de_emojis(r):
    reg = _anel()
    ev = _confere(r, "01", reg, _emitir(reg, {"texto": "😀" * 4096}), cortado=True)
    t = ev.get("dados", {}).get("texto", "")
    r.check(t.endswith("…") and len(t.encode("utf-8")) <= 8 * 1024
            and set(t[:-1]) == {"😀"}, "01.corte_em_fronteira_de_caractere", str(len(t)))


def test_02_muitos_textos_grandes(r):
    reg = _anel()
    dados = {f"t{i}": ("á" * 4096 if i % 2 else "b" * 4096) for i in range(12)}
    ev = _confere(r, "02", reg, _emitir(reg, dados), cortado=True)
    r.check(all(isinstance(v, str) for k, v in ev.get("dados", {}).items()
                if k[1:].isdigit()), "02.textos_continuam_textos")
    pequeno = _confere(r, "02b", reg, _emitir(reg, {"a": "x" * 100, "b": "y" * 100}),
                       cortado=False)
    r.check(pequeno.get("dados", {}).get("a") == "x" * 100, "02.o_que_cabe_nao_muda")


def test_03_escapes_json(r):
    reg = _anel()
    # controle: 6 bytes de JSON por caractere → precisa cortar
    for nome, texto in (("controle", "\x01" * 4000), ("misto", "\x02é\"" * 1300)):
        ev = _confere(r, f"03.{nome}", reg, _emitir(reg, {"t": texto}), cortado=True)
        r.check(isinstance(ev.get("dados", {}).get("t"), str), f"03.{nome}.texto")
    # aspas e barras dobram no JSON, mas 4000 delas ainda cabem: intactas
    texto = '"\\' * 2000
    ev = _confere(r, "03.aspas", reg, _emitir(reg, {"t": texto}), cortado=False)
    r.check(ev.get("dados", {}).get("t") == texto, "03.aspas.intactas")


def test_04_links_profundidade_itens_nos_chaves(r):
    reg = _anel()
    links = [f"https://www.loja.com.br/produto/{'x' * 470}{i}" for i in range(60)]
    ev = _confere(r, "04.links", reg, _emitir(reg, {"links": links}), cortado=True)
    lst = ev.get("dados", {}).get("links", [])
    r.check(isinstance(lst, list) and lst[-1].startswith("…+"), "04.links_marcados",
            str(lst[-1:]))
    fundo = {"n": 0}
    atual = fundo
    for i in range(1, 12):
        atual["f"] = {"n": i}
        atual = atual["f"]
    ev = _confere(r, "04.profundidade", reg, _emitir(reg, {"fundo": fundo}), cortado=True)
    r.check("<dict:" in json.dumps(ev.get("dados", {})), "04.profundidade_marcada")
    ev = _confere(r, "04.itens", reg,
                  _emitir(reg, {"muitos": {f"k{i}": i for i in range(10000)}}), cortado=True)
    r.check(len(ev.get("dados", {}).get("muitos", {})) == 51, "04.itens_limitados")
    ev = _confere(r, "04.nos", reg,
                  _emitir(reg, {"grade": [[i] * 50 for i in range(50)]}), cortado=True)
    ev = _confere(r, "04.chaves", reg, _emitir(reg, {"k" * 1000: 1}), cortado=True)
    r.check(list(ev.get("dados", {}))[0] == "k" * 63 + "…", "04.chave_cortada")


def test_05_ultimo_recurso_de_tamanho(r):
    reg = _anel()
    dados = {f"g{j}": {f"c{i:02d}_" + "k" * 55: "v" * 75 for i in range(49)}
             for j in range(8)}
    with eventos.execucao(lambda: {"chat": "-100", "msg": 9}):
        cargas = _emitir(reg, dados, corr={"post": 3, "lixo": "x" * 3000})
    ev = _confere(r, "05", reg, cargas, cortado=True)
    d, c = ev.get("dados", {}), ev.get("corr", {})
    r.check(d.get("degradado") is True and d.get("erro_tipo") == "TAMANHO"
            and "g0" in d.get("chaves", [])
            and set(d.get("chaves", [])) <= {f"g{j}" for j in range(8)} | {"…"}
            and d.get("local") == "t.tamanho",
            "05.degradado_tamanho", str(d))
    r.check(c.get("chat") == "-100" and c.get("msg") == 9 and c.get("post") == 3
            and isinstance(c.get("exec"), int) and "lixo" not in c, "05.identificacao_fica",
            str(c))


def _sorteio(rng: random.Random, orc: list, nivel: int = 0):
    """Carga patológica com orçamento de nós (o gerador não explode)."""
    alfabeto = ["a", "é", "😀", "\x01", '"', "\\", "\ud800", " ", "\n", "/", ".", "?",
                "https://amzn.to/3xYz9 ", "https://www.loja.com.br/p/123 ", "tag=x"]
    orc[0] -= 1
    escolha = rng.random()
    if orc[0] <= 0 or nivel >= 6 or escolha < 0.35:
        tipo = rng.randrange(9)
        if tipo == 0:
            n = rng.randrange(4000, 6000) if rng.random() < 0.05 else rng.randrange(0, 300)
            return "".join(rng.choices(alfabeto, k=n))
        if tipo == 1:
            return rng.choice([0, -1, 2 ** 53, 2 ** 80, -(2 ** 90), 7])
        if tipo == 2:
            return rng.choice([0.5, float("nan"), float("inf"), -1e308])
        if tipo == 3:
            return rng.choice([None, True, False])
        if tipo == 4:
            return bytes(rng.randrange(256) for _ in range(rng.randrange(0, 50)))
        if tipo == 5:
            return {rng.randrange(100) for _ in range(rng.randrange(0, 70))}
        if tipo == 6:
            return object()
        return rng.choice(["", "x" * rng.randrange(0, 5000)])
    n = rng.randrange(0, 80)
    if escolha < 0.7:
        return [_sorteio(rng, orc, nivel + 1) for _ in range(n)]
    if escolha < 0.85:
        return tuple(_sorteio(rng, orc, nivel + 1) for _ in range(min(n, 10)))
    return {(f"k{i}" if rng.random() < 0.8 else rng.randrange(1000)) if rng.random() < 0.9
            else "c" * rng.randrange(0, 300): _sorteio(rng, orc, nivel + 1)
            for i in range(n)}


def _sem_ts(ev):
    """dados sem o relógio e sem a marca `cortado` — ela descreve o EVENTO
    inteiro (o corr também pode ter sido cortado); o conteúdo tem de bater."""
    d = dict(ev["dados"])
    d.pop("ts_fato", None)
    d.pop("cortado", None)
    return ev["corr"], d


def test_06_sorteio_deterministico(r):
    reg = _anel()
    rng = random.Random(20261006)
    maiores, ruins, diferentes = 0, [], []
    for i in range(400):
        carga = _sorteio(rng, [300])
        dados = carga if isinstance(carga, dict) else {"v": carga}
        a = _emitir(reg, dados, corr={"msg": i, "x": _sorteio(rng, [20], 4)})
        b = _emitir(reg, dados, corr={"msg": i, "x": "fixo"})
        for c in a + b:
            maiores = max(maiores, len(c))
            if len(c) > TETO or anel._MARCA_CORTE.encode() in c:
                ruins.append((i, len(c)))
        if a and b and _sem_ts(json.loads(a[0]))[1] != _sem_ts(json.loads(b[0]))[1]:
            diferentes.append(i)
    r.check(ruins == [], "06.todas_cabem_sem_corte_do_anel", str(ruins[:5]))
    r.check(reg.saude()["excedidos"] == 0, "06.excedidos_zero")
    r.check(diferentes == [], "06.mesma_carga_mesmo_evento", str(diferentes[:5]))
    r.check(0 < maiores <= TETO, "06.maior_evento", f"{maiores} bytes")


def test_07_comuns_pequenos_e_bordas(r):
    reg = _anel()
    with eventos.execucao(lambda: {"chat": "-1001825680721", "msg": 110093}):
        pass
    fim = reg.ler(None, 100000, 1 << 30)["cargas"][-1]
    r.check(json.loads(fim)["tipo"] == "execucao.fim" and len(fim) < 600,
            "07.fim_pequeno", f"{len(fim)} bytes")
    _confere(r, "07.vazio", reg, _emitir(reg, {}), cortado=False)
    _confere(r, "07.surrogate", reg, _emitir(reg, {"s": "a\ud800b", "\udc00": 1}),
             cortado=False)


if __name__ == "__main__":
    sys.exit(rodar(globals(), "EVENTOS · tamanho por construção (≤ 12 KiB) · F1.2-A1"))
