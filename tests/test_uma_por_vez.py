"""
Rede de regressão do mecanismo de exclusão mútua por chave.

Roda sob pytest (`pytest`) e também standalone, sem nenhuma
dependência além da biblioteca padrão:

    python tests/test_uma_por_vez.py

Não usa pytest-asyncio: cada teste é uma função síncrona que abre o
seu próprio loop com asyncio.run.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.uma_por_vez import _entradas, em_execucao, uma_por_vez  # noqa: E402


# ── Apoio ─────────────────────────────────────────────────────────
class Observador:
    """Conta execuções e o pico de simultaneidade por chave."""

    def __init__(self) -> None:
        self.dentro: dict = {}
        self.pico: dict = {}
        self.execucoes: dict = {}
        self.pico_global = 0
        self.dentro_global = 0

    def entra(self, chave: str) -> None:
        self.dentro[chave] = self.dentro.get(chave, 0) + 1
        self.pico[chave] = max(self.pico.get(chave, 0), self.dentro[chave])
        self.execucoes[chave] = self.execucoes.get(chave, 0) + 1
        self.dentro_global += 1
        self.pico_global = max(self.pico_global, self.dentro_global)

    def sai(self, chave: str) -> None:
        self.dentro[chave] -= 1
        self.dentro_global -= 1


def _operacao_que_cede(obs: Observador, chave: str, voltas: int = 3):
    async def op():
        obs.entra(chave)
        for _ in range(voltas):
            await asyncio.sleep(0)
        obs.sai(chave)
        return f"ok:{chave}"

    return op


# ── A — 100 chamadas concorrentes, mesma chave ────────────────────
async def _cenario_a():
    obs = Observador()
    op = _operacao_que_cede(obs, "k")
    r = await asyncio.gather(*[uma_por_vez("k", op) for _ in range(100)])

    assert obs.pico["k"] == 1, f"pico={obs.pico['k']} (exclusão falhou)"
    assert obs.execucoes["k"] == 100, obs.execucoes
    assert all(x == "ok:k" for x in r)
    assert em_execucao() == 0

    # Controle: sem o mecanismo, a mesma operação se sobrepõe.
    # Prova que o teste acima mede algo real.
    obs2 = Observador()
    op2 = _operacao_que_cede(obs2, "k")
    await asyncio.gather(*[op2() for _ in range(100)])
    assert obs2.pico["k"] > 1, "controle inválido: operação não se sobrepõe"


def test_a_mesma_chave_uma_por_vez():
    asyncio.run(_cenario_a())


# ── B — 100 chamadas, 20 chaves, locks independentes ──────────────
async def _cenario_b():
    obs = Observador()
    chamadas = []
    for i in range(100):
        chave = f"k{i % 20}"
        chamadas.append(uma_por_vez(chave, _operacao_que_cede(obs, chave)))
    await asyncio.gather(*chamadas)

    assert len(obs.pico) == 20, obs.pico
    for chave, pico in obs.pico.items():
        assert pico == 1, f"{chave} teve pico {pico}"
        assert obs.execucoes[chave] == 5, obs.execucoes
    # Sem lock global: chaves distintas correm ao mesmo tempo.
    assert obs.pico_global > 1, "chaves diferentes não correram em paralelo"
    assert em_execucao() == 0


def test_b_chaves_diferentes_sao_independentes():
    asyncio.run(_cenario_b())


# ── C — chave lenta não segura chave rápida ───────────────────────
async def _cenario_c():
    ordem = []

    async def lenta():
        await asyncio.sleep(0.20)
        ordem.append("X")

    async def rapida():
        await asyncio.sleep(0)
        ordem.append("Y")

    await asyncio.gather(
        uma_por_vez("X", lenta),
        uma_por_vez("Y", rapida),
    )
    assert ordem == ["Y", "X"], ordem
    assert em_execucao() == 0


def test_c_chave_rapida_nao_espera_chave_lenta():
    asyncio.run(_cenario_c())


# ── D — nunca duas entradas para a mesma chave ────────────────────
async def _cenario_d():
    """
    Reproduz o interleaving da prova:
    A entra · B entra · A termina · C entra · B termina.
    Todos os participantes devem enxergar a MESMA entrada.
    """
    ids = []
    a_dentro = asyncio.Event()
    liberar_a = asyncio.Event()

    async def op_a():
        ids.append(id(_entradas.get("k")))
        a_dentro.set()
        await liberar_a.wait()

    async def op_b():
        ids.append(id(_entradas.get("k")))
        await asyncio.sleep(0.05)

    async def op_c():
        ids.append(id(_entradas.get("k")))

    ta = asyncio.create_task(uma_por_vez("k", op_a))
    await a_dentro.wait()                      # A dentro, segurando o lock

    tb = asyncio.create_task(uma_por_vez("k", op_b))
    await asyncio.sleep(0.01)                  # B completa S1 e entra na fila
    assert _entradas["k"].n == 2, _entradas["k"].n

    liberar_a.set()                            # A termina: n 2->1, SEM pop
    await ta
    assert "k" in _entradas, "entrada foi removida com B ainda viva"
    assert _entradas["k"].n == 1

    tc = asyncio.create_task(uma_por_vez("k", op_c))
    await asyncio.sleep(0.01)                  # C completa S1
    assert _entradas["k"].n == 2

    await asyncio.gather(tb, tc)

    assert len(set(ids)) == 1, f"nasceram {len(set(ids))} entradas para a chave"
    assert em_execucao() == 0


def test_d_uma_chave_uma_entrada_um_lock():
    asyncio.run(_cenario_d())


# ── E — líder cancelado: o próximo assume ─────────────────────────
async def _cenario_e():
    a_dentro = asyncio.Event()
    b_concluiu = []

    async def op_a():
        a_dentro.set()
        await asyncio.sleep(3600)              # nunca conclui sozinha

    async def op_b():
        b_concluiu.append(True)
        return "b"

    ta = asyncio.create_task(uma_por_vez("k", op_a))
    await a_dentro.wait()
    tb = asyncio.create_task(uma_por_vez("k", op_b))
    await asyncio.sleep(0.01)

    ta.cancel()
    try:
        await ta
    except asyncio.CancelledError:
        pass

    assert await tb == "b"
    assert b_concluiu == [True]
    assert em_execucao() == 0


def test_e_lider_cancelado_proximo_assume():
    asyncio.run(_cenario_e())


# ── F — esperador cancelado: líder continua, fila intacta ─────────
async def _cenario_f():
    a_dentro = asyncio.Event()
    liberar_a = asyncio.Event()
    executou = []

    async def op_a():
        a_dentro.set()
        await liberar_a.wait()
        executou.append("A")

    async def op_b():
        executou.append("B")

    async def op_c():
        executou.append("C")

    ta = asyncio.create_task(uma_por_vez("k", op_a))
    await a_dentro.wait()
    tb = asyncio.create_task(uma_por_vez("k", op_b))
    await asyncio.sleep(0.01)
    tc = asyncio.create_task(uma_por_vez("k", op_c))
    await asyncio.sleep(0.01)

    tb.cancel()                                 # esperador cancelado
    try:
        await tb
    except asyncio.CancelledError:
        pass

    liberar_a.set()
    await ta
    await tc

    assert executou == ["A", "C"], executou
    assert em_execucao() == 0


def test_f_esperador_cancelado_lider_continua():
    asyncio.run(_cenario_f())


# ── G — exceção libera o lock e não contamina o próximo ───────────
async def _cenario_g():
    class Falha(Exception):
        pass

    async def op_ruim():
        await asyncio.sleep(0)
        raise Falha("falha do produtor")

    async def op_boa():
        await asyncio.sleep(0)
        return "ok"

    r = await asyncio.gather(
        uma_por_vez("k", op_ruim),
        uma_por_vez("k", op_boa),
        return_exceptions=True,
    )
    assert isinstance(r[0], Falha), r[0]
    assert r[1] == "ok", r[1]
    assert em_execucao() == 0

    # A chave continua utilizável depois da exceção.
    assert await uma_por_vez("k", op_boa) == "ok"
    assert em_execucao() == 0


def test_g_excecao_libera_lock():
    asyncio.run(_cenario_g())


# ── H — estrutura drenada ao fim ──────────────────────────────────
async def _cenario_h():
    obs = Observador()
    chamadas = []
    for i in range(200):
        chave = f"k{i % 37}"
        chamadas.append(uma_por_vez(chave, _operacao_que_cede(obs, chave)))
    await asyncio.gather(*chamadas)
    assert em_execucao() == 0, f"vazaram {em_execucao()} entradas"
    assert _entradas == {}


def test_h_estrutura_drenada():
    asyncio.run(_cenario_h())


# ── I — operação sem await real: custo e contenção zero ───────────
async def _cenario_i():
    # (1) o mecanismo não cede o event loop quando a operação não cede
    marcador = {"rodou": False}
    asyncio.get_running_loop().call_soon(lambda: marcador.__setitem__("rodou", True))

    async def op_pura():
        return 42

    assert await uma_por_vez("k", op_pura) == 42
    assert marcador["rodou"] is False, "o mecanismo cedeu o loop sem necessidade"
    assert em_execucao() == 0

    # (2) duas chamadas concorrentes na MESMA chave, operação que não
    # cede: o líder conclui e remove a entrada antes de a segunda
    # tarefa rodar. Cada uma tem a própria entrada => contenção zero.
    # É o caso Netshoes: colisão de chave que nunca vira espera.
    vistos = []

    async def op_registra():
        vistos.append(id(_entradas.get("n")))
        return "ok"

    await asyncio.gather(
        uma_por_vez("n", op_registra),
        uma_por_vez("n", op_registra),
    )
    assert len(vistos) == 2
    assert vistos[0] != vistos[1], "houve espera onde não deveria haver"
    assert em_execucao() == 0


def test_i_operacao_sem_await_nao_contende():
    asyncio.run(_cenario_i())


# ── J — bursts sucessivos da mesma chave não se misturam ──────────
async def _cenario_j():
    ids_por_burst = []

    async def burst():
        vistos = []
        obs = Observador()

        async def op():
            vistos.append(id(_entradas.get("k")))
            obs.entra("k")
            await asyncio.sleep(0)
            obs.sai("k")

        await asyncio.gather(*[uma_por_vez("k", op) for _ in range(5)])
        assert obs.pico["k"] == 1
        assert len(set(vistos)) == 1, "burst usou mais de uma entrada"
        ids_por_burst.append(vistos[0])

    await burst()
    assert em_execucao() == 0
    await burst()
    assert em_execucao() == 0

    assert ids_por_burst[0] != ids_por_burst[1], "entrada não foi reciclada"


def test_j_bursts_independentes():
    asyncio.run(_cenario_j())


# ── N — não-reentrância: proibida, e mesmo assim não vaza ─────────
async def _cenario_n():
    # Mesma chave aninhada: auto-deadlock por contrato. O teste
    # documenta a proibição e prova que o cancelamento drena tudo.
    async def interna():
        return "nunca"

    async def externa_mesma_chave():
        return await uma_por_vez("k", interna)

    try:
        await asyncio.wait_for(uma_por_vez("k", externa_mesma_chave), timeout=0.2)
        raise AssertionError("reentrância na mesma chave deveria travar")
    except asyncio.TimeoutError:
        pass
    assert em_execucao() == 0, "cancelamento por timeout vazou entrada"

    # Chaves diferentes aninhadas: seguro.
    async def externa_outra_chave():
        return await uma_por_vez("k2", interna)

    assert await uma_por_vez("k1", externa_outra_chave) == "nunca"
    assert em_execucao() == 0


def test_n_reentrancia():
    asyncio.run(_cenario_n())


# ── Execução standalone ───────────────────────────────────────────
if __name__ == "__main__":
    testes = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in testes:
        t()
        print(f"  ok  {t.__name__}")
    print(f"\n{len(testes)} testes passaram.")
