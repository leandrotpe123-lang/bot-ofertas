"""
Brain v0 — bancada de teste do juiz (o PORTÃO antes de qualquer deploy) e o
conjunto de casos reais. Hermético: nenhuma chamada real; a chave da API,
se existir no ambiente, nunca é lida nem impressa aqui.

    python tests/test_brain_avaliar.py
"""
import contextlib
import io
import json
import os
import re
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar                         # noqa: E402

RAIZ = preparar()
# brain/ é a raiz do serviço Brain; o pacote é brain/brain/
sys.path.insert(0, os.path.join(RAIZ, "brain"))
from brain import avaliar as A, cliente as CL, juiz as J         # noqa: E402


def caso(cid, rotulo, deve, tipo="par_canal", delta=1.0, aceitos=None, **kw):
    d = {"id": cid, "tipo": tipo, "rotulo": rotulo, "deve": deve,
         "fonte_rotulo": "dono",
         "alvo": {"id": f"{cid}:b", "origem": "fonte", "grupo": "g2", "delta_min": 0.0,
                  "plataforma": "shopee", "texto": "Cupom B"},
         "candidatos": [{"id": f"{cid}:a", "origem": "fonte", "grupo": "g1", "delta_min": -delta,
                         "plataforma": "shopee", "texto": "Cupom A"}]}
    if aceitos:
        d["aceitos"] = aceitos
    d.update(kw)
    return d


def ch(cid, veredito, certeza=0.95, lembrete=False, modelo="claude-haiku-5-5", effort="high", rep=0):
    return {"caso": cid, "rep": rep, "modelo": modelo, "effort": effort, "veredito": veredito,
            "certeza": certeza, "lembrete": lembrete, "latencia_ms": 900.0,
            "usage": {"input_tokens": 2000, "output_tokens": 700}, "custo_usd": 0.00055}


def test_01_holdout_deterministico_e_exemplo_do_prompt_fica_no_dev(r):
    r.check(A.holdout("x1") == A.holdout("x1"), "01.deterministico")
    ids = [f"c{i}" for i in range(200)]
    frac = sum(A.holdout(i) for i in ids) / len(ids)
    r.check(0.35 < frac < 0.65, "01.metade", frac)
    alvo = next(i for i in ids if A.holdout(i))
    r.check(A.em_holdout({"id": alvo}) and not A.em_holdout({"id": alvo, "inspirou_prompt": True}),
            "01.inspirou_prompt_nunca_holdout")


def test_02_resultado_pela_politica(r):
    par_n = caso("n", "LIBERACAO_NOVA", "DOIS_POSTS", aceitos=["LIBERACAO_NOVA", "PRODUTOS_DIFERENTES"])
    par_s = caso("s", "MESMA_CAMPANHA", "UM_POST")
    r.check(A.resultado(ch("n", "MESMA_CAMPANHA"), par_n) == "falso_destrutivo", "02.destrutivo")
    r.check(A.resultado(ch("n", "MESMA_CAMPANHA", 0.8), par_n) == "alerta_falso", "02.alerta_falso")
    r.check(A.resultado(ch("n", "LIBERACAO_NOVA"), par_n) == "certo", "02.separa_certo")
    r.check(A.resultado(ch("s", "MESMA_CAMPANHA"), par_s) == "certo", "02.funde_certo")
    r.check(A.resultado(ch("s", "INCERTO", 0.0), par_s) == "perdeu", "02.incerto_perde")
    # item 23 do dono: lembrete 16 min depois = dois posts certos
    lemb = caso("l", "MESMA_CAMPANHA", "DOIS_POSTS", delta=16.0,
                aceitos=["MESMA_CAMPANHA", "LIBERACAO_NOVA"])
    r.check(A.resultado(ch("l", "MESMA_CAMPANHA", lembrete=True), lemb) == "certo", "02.lembrete")
    r.check(A.resultado(ch("l", "MESMA_CAMPANHA"), lemb) == "falso_destrutivo", "02.lembrete_sem_flag")
    # absorvido: reinjetar o que estava certo cria duplicata
    abs_ok = caso("a", "MESMA_CAMPANHA", "UM_POST", tipo="absorvido")
    abs_fp = caso("f", "LIBERACAO_NOVA", "DOIS_POSTS", tipo="absorvido", delta=55.0)
    r.check(A.resultado(ch("a", "LIBERACAO_NOVA"), abs_ok) == "falso_reinjetar", "02.reinjetar_falso")
    r.check(A.resultado(ch("f", "LIBERACAO_NOVA"), abs_fp) == "certo", "02.reinjetar_certo")
    # armadilha da live: produtos diferentes, nunca funde
    live = caso("v", "PRODUTOS_DIFERENTES", "DOIS_POSTS", contexto={"so_container_live": True})
    r.check(A.resultado(ch("v", "MESMA_CAMPANHA"), live) == "alerta_falso", "02.live_nunca_funde")


def test_03_metricas_e_portao(r):
    meta = {d["id"]: d for d in (
        caso("s", "MESMA_CAMPANHA", "UM_POST"),
        caso("n", "LIBERACAO_NOVA", "DOIS_POSTS", aceitos=["LIBERACAO_NOVA", "PRODUTOS_DIFERENTES"]),
        caso("v", "PRODUTOS_DIFERENTES", "DOIS_POSTS", fonte_rotulo="claude"))}
    bom = [ch("s", "MESMA_CAMPANHA", rep=k) for k in range(3)] + \
          [ch("n", "PRODUTOS_DIFERENTES", rep=k) for k in range(3)] + \
          [ch("v", "PRODUTOS_DIFERENTES", rep=0), ch("v", "INCERTO", 0.0, rep=1),
           ch("v", "PRODUTOS_DIFERENTES", rep=2)]
    t = A.avaliar_resultados(bom, meta)[("claude-haiku-5-5", "high")]
    r.check(A.passa_no_portao(t), "03.passa", t)
    r.check(t["todos"]["acerto_veredito"] == 8 and t["todos"]["incerto"] == 1, "03.acerto_incerto")
    r.check(t["todos"]["casos_estaveis"] == 2, "03.estabilidade")
    r.check(t["dono"]["casos"] == 2, "03.recorte_do_dono")
    ruim = bom + [ch("n", "MESMA_CAMPANHA", modelo="claude-sonnet-5-5")]
    t2 = A.avaliar_resultados(ruim, meta)[("claude-sonnet-5-5", "high")]
    r.check(not A.passa_no_portao(t2) and t2["todos"]["falso_mesma_campanha"] == 1
            and t2["todos"]["r_falso_destrutivo"] == 1, "03.um_falso_reprova")


def test_04_cascata_offline_sobe_so_no_incerto(r):
    p = [ch("s", "INCERTO", 0.0), ch("n", "LIBERACAO_NOVA", 0.95)]
    s = [dict(ch("s", "MESMA_CAMPANHA", modelo="claude-sonnet-5-5"), custo_usd=0.01),
         dict(ch("n", "MESMA_CAMPANHA", modelo="claude-sonnet-5-5"), custo_usd=0.01)]
    out = {c["caso"]: c for c in A.cascata_offline(p + s, ("claude-haiku-5-5", "high"),
                                                     ("claude-sonnet-5-5", "high"))}
    r.check(out["s"]["veredito"] == "MESMA_CAMPANHA"
            and abs(out["s"]["custo_usd"] - (0.00055 + 0.01)) < 1e-12, "04.sobe_e_soma_custo")
    r.check(out["n"]["veredito"] == "LIBERACAO_NOVA", "04.fica_no_principal")


def test_05_rodar_para_antes_do_teto(r):
    casos = A.carregar()
    lista = [c for c, _ in casos][:5]
    ok = {"veredito": "INCERTO", "certeza": 0.1, "ids_relacionados": [], "evidencias": [],
          "acao_sugerida": "NADA", "lembrete": False}
    falso = CL.ClienteFalso(lambda p: {"stop_reason": "end_turn",
                                       "content": [{"type": "text", "text": json.dumps(ok)}],
                                       "usage": {"input_tokens": 3000, "output_tokens": 1500}})
    with tempfile.TemporaryDirectory() as d, contextlib.redirect_stdout(io.StringIO()):
        gasto = A.rodar(casos, [("t", "claude-sonnet-5-5", "high", lista, 3)], 0.10, d,
                        cliente=falso, dormir=lambda s: None)
        linhas = open(os.path.join(d, "chamadas.jsonl")).read().splitlines()
    r.check(gasto <= 0.10 and len(falso.chamadas) == len(linhas) < 15, "05.parou_no_teto",
            f"{gasto} {len(linhas)}")
    r.check(all(json.loads(l)["modelo"] == "claude-sonnet-5-5" for l in linhas), "05.registra_modelo")


def _falso_incerto(out_tokens=1500):
    ok = {"veredito": "INCERTO", "certeza": 0.1, "ids_relacionados": [], "evidencias": [],
          "acao_sugerida": "NADA", "lembrete": False}
    return CL.ClienteFalso(lambda p: {"stop_reason": "end_turn",
                                      "content": [{"type": "text", "text": json.dumps(ok)}],
                                      "usage": {"input_tokens": 3000, "output_tokens": out_tokens}})


def test_09_retoma_sem_refazer_e_paralelo_sem_duplicar(r):
    # o contêiner pode reciclar no meio de uma rodada longa: retomar a mesma
    # pasta não refaz (nem paga de novo) o que já está gravado
    casos = A.carregar()
    lista = [c for c, _ in casos][:6]
    fases = [("t", "claude-haiku-5-5", "high", lista, 2)]
    with tempfile.TemporaryDirectory() as d, contextlib.redirect_stdout(io.StringIO()):
        f1 = _falso_incerto()
        A.rodar(casos, [("t", "claude-haiku-5-5", "high", lista[:3], 1)], 5.0, d,
                cliente=f1, dormir=lambda s: None)
        f2 = _falso_incerto()
        A.rodar(casos, fases, 5.0, d, cliente=f2, dormir=lambda s: None, paralelo=4)
        linhas = [json.loads(l) for l in open(os.path.join(d, "chamadas.jsonl"))]
    chaves = [(l["fase"], l["caso"], l["rep"]) for l in linhas]
    r.check(len(f1.chamadas) == 3 and len(f2.chamadas) == 9, "09.retomou_so_o_que_faltava",
            f"{len(f1.chamadas)} {len(f2.chamadas)}")
    r.check(len(chaves) == len(set(chaves)) == 12, "09.sem_duplicata", len(chaves))
    # em paralelo, o teto continua valendo (reserva antes de chamar)
    with tempfile.TemporaryDirectory() as d, contextlib.redirect_stdout(io.StringIO()):
        f3 = _falso_incerto()
        gasto = A.rodar(casos, [("t", "claude-sonnet-5-5", "high", [c for c, _ in casos][:20], 3)],
                        0.10, d, cliente=f3, dormir=lambda s: None, paralelo=8)
    r.check(gasto <= 0.10 + 1e-9 and 0 < len(f3.chamadas) < 60, "09.teto_em_paralelo",
            f"{gasto} {len(f3.chamadas)}")


def test_12_retomar_exige_pasta_do_mesmo_comando(r):
    with tempfile.TemporaryDirectory() as d:
        outra = os.path.join(d, "20261015T000000Z_dev")
        os.makedirs(outra)
        with _chave("chave-falsa-do-teste"), contextlib.redirect_stdout(io.StringIO()):
            rc = A.main(["final", "--confirmar", "--retomar", outra, "--resultados", d])
        r.check(rc == 2 and "anthropic" not in sys.modules, "12.recusa_pasta_de_outro_comando", rc)


@contextlib.contextmanager
def _ambiente(**valores):
    """Troca variáveis só durante o bloco (None = remove); os valores
    originais nunca são exibidos e voltam intactos."""
    antes = {k: os.environ.get(k) for k in valores}
    try:
        for k, v in valores.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        yield
    finally:
        for k, v in antes.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _chave(valor):
    return _ambiente(BRAIN_TESTE_CHAVE=valor)


def test_06_chamada_real_exige_chave_e_confirmar(r):
    with _chave(None), contextlib.redirect_stdout(io.StringIO()) as out:
        r.check(A.main(["piloto", "--confirmar"]) == 2, "06.sem_chave_recusa")
    r.check("BRAIN_TESTE_CHAVE ausente" in out.getvalue(), "06.diz_o_nome_da_variavel")
    with _chave("chave-falsa-do-teste"), contextlib.redirect_stdout(io.StringIO()) as out:
        r.check(A.main(["final"]) == 2, "06.sem_confirmar_recusa")
    r.check("chave-falsa-do-teste" not in out.getvalue(), "06.nunca_imprime_a_chave")
    r.check("anthropic" not in sys.modules, "06.sdk_nao_importado")


def test_06b_anthropic_api_key_nunca_serve_de_chave_de_teste(r):
    # no Claude Code, ANTHROPIC_API_KEY é a credencial do próprio Claude
    # Code: com ela presente e BRAIN_TESTE_CHAVE ausente, a bancada recusa
    with _ambiente(BRAIN_TESTE_CHAVE=None, ANTHROPIC_API_KEY="credencial-do-claude-code"), \
            contextlib.redirect_stdout(io.StringIO()) as out:
        rc = A.main(["piloto", "--confirmar"])
        sem_chave = A.chave_de_teste() is None   # dentro do bloco: fora, a variável já voltou
    r.check(rc == 2 and sem_chave, "06b.recusa", rc)
    r.check("credencial-do-claude-code" not in out.getvalue(), "06b.nao_imprime")


def test_08_teto_total_soma_as_rodadas_gravadas(r):
    with tempfile.TemporaryDirectory() as d:
        os.makedirs(os.path.join(d, "r1")); os.makedirs(os.path.join(d, "r2"))
        for sub, custo in (("r1", 0.75), ("r2", 0.25)):
            with open(os.path.join(d, sub, "chamadas.jsonl"), "w") as f:
                f.write(json.dumps({"custo_usd": custo}) + "\n")
        r.check(abs(A.gasto_acumulado(d) - 1.0) < 1e-12, "08.soma")
        # teto total já gasto: recusa ANTES de qualquer chamada
        with _chave("chave-falsa-do-teste"), contextlib.redirect_stdout(io.StringIO()):
            rc = A.main(["dev", "--confirmar", "--teto-total-usd", "1.0", "--resultados", d])
        r.check(rc == 3 and "anthropic" not in sys.modules, "08.para_sem_chamar", rc)


def test_07_orcamento_sem_rede(r):
    with _chave(None), contextlib.redirect_stdout(io.StringIO()) as out:
        r.check(A.main(["orcamento", "--cambio", "5.0"]) == 0, "07.roda")
    txt = out.getvalue()
    r.check("TOTAL" in txt and "R$" in txt, "07.total_em_usd_e_brl", txt[-300:])


def test_13_cliente_real_nasce_sem_nenhuma_variavel_anthropic(r):
    # SDK FALSO no lugar do `anthropic`: registra os argumentos e o ambiente
    # que o construtor viu. O SDK 1.9.0 lê ANTHROPIC_BASE_URL e
    # ANTHROPIC_CUSTOM_HEADERS sozinho; no Claude Code elas existem.
    visto = {}

    class _Falso:
        class Anthropic:
            def __init__(self, **kw):
                visto["kw"] = kw
                visto["env"] = sorted(k for k in os.environ if k.startswith("ANTHROPIC_"))
    sujo = dict(ANTHROPIC_BASE_URL="http://endereco-do-claude-code",
                ANTHROPIC_CUSTOM_HEADERS="X-Do-Claude-Code: 1",
                ANTHROPIC_API_KEY="credencial-do-claude-code",
                ANTHROPIC_AUTH_TOKEN="token-do-claude-code")
    sys.modules["anthropic"] = _Falso
    try:
        with _ambiente(**sujo):
            cli = CL.ClienteAnthropic("chave-de-teste")
            cli._cliente()
            depois = {k: os.environ.get(k) for k in sujo}
    finally:
        del sys.modules["anthropic"]
    kw = visto.get("kw", {})
    r.check(kw.get("api_key") == "chave-de-teste", "13.chave_explicita")
    r.check(kw.get("base_url") == "https://api.anthropic.com", "13.endereco_fixo", kw.get("base_url"))
    r.check("auth_token" not in kw and kw.get("max_retries") == 0, "13.sem_token_sem_retry_do_sdk")
    r.check(visto.get("env") == [], "13.construtor_sem_ANTHROPIC_no_ambiente", visto.get("env"))
    r.check(depois == sujo, "13.ambiente_restaurado")
    r.check("chave-de-teste" not in repr(cli), "13.repr_esconde_a_chave", repr(cli))
    try:
        CL.ClienteAnthropic("")
        vazio = False
    except ValueError:
        vazio = True
    r.check(vazio, "13.chave_vazia_recusa_sem_descoberta")


def test_14_main_passa_a_chave_de_teste_ao_cliente(r):
    recebida = []

    def fabrica(chave):
        recebida.append(chave)
        return _falso_incerto()
    with tempfile.TemporaryDirectory() as d, \
            _ambiente(BRAIN_TESTE_CHAVE="  chave-de-teste  ", ANTHROPIC_API_KEY="credencial-do-claude-code"), \
            contextlib.redirect_stdout(io.StringIO()) as out:
        rc = A.main(["piloto", "--confirmar", "--resultados", d], fabrica_cliente=fabrica)
        pastas = [p for p in os.listdir(d) if p.endswith("_piloto")]
    r.check(rc == 0 and recebida == ["chave-de-teste"], "14.chave_de_teste_e_so_ela", recebida)
    r.check("chave-de-teste" not in out.getvalue() and "credencial" not in out.getvalue(),
            "14.nunca_imprime")
    r.check(len(pastas) == 1, "14.grava_a_rodada")


def test_15_extras_so_com_aprovacao_do_dono(r):
    with tempfile.TemporaryDirectory() as d, _chave("chave-falsa-do-teste"), \
            contextlib.redirect_stdout(io.StringIO()) as out:
        sem = [A.main([c, "--confirmar", "--resultados", d]) for c in ("sonnet-extra", "opus")]
        opus_sem_base = A.main(["opus", "--confirmar", "--aprovado-pelo-dono", "--resultados", d])
    r.check(sem == [2, 2] and "aprovado-pelo-dono" in out.getvalue(), "15.recusa_sem_aprovacao")
    r.check(opus_sem_base == 2, "15.opus_exige_rodadas_anteriores")
    linhas = A.orcamento(A.carregar(), 5.0)
    plano = [l for l in linhas if not l[0].startswith("SO_COM_APROVACAO")]
    nomes = [l[0] for l in plano]
    r.check(any(n.startswith("final_haiku_low") for n in nomes)
            and "sonnet_high" in nomes and not any("opus" in n for n in nomes)
            and not any(n in ("sonnet_medium", "sonnet_low") for n in nomes), "15.plano_em_etapas", nomes)
    total = next(l for l in linhas if l[0] == "TOTAL_PLANO")
    r.check(total[4] < 20.0, "15.plano_cabe_no_teto", total[4])


def test_16_tabela_consolida_rodadas_finais(r):
    meta = {d["id"]: d for d in json.load(open(A.CASOS, encoding="utf-8"))["casos"]}
    cid = sorted(meta)[0]
    with tempfile.TemporaryDirectory() as d:
        for sub, fase, modelo in (("a_dev", "dev_haiku_high", "claude-haiku-5-5"),
                                  ("b_final", "final_haiku_high", "claude-haiku-5-5"),
                                  ("c_sonnet", "sonnet_high", "claude-sonnet-5-5")):
            os.makedirs(os.path.join(d, sub))
            with open(os.path.join(d, sub, "chamadas.jsonl"), "w") as f:
                f.write(json.dumps(dict(ch(cid, "INCERTO", 0.0, modelo=modelo), fase=fase)) + "\n")
        with contextlib.redirect_stdout(io.StringIO()):
            ok = A.consolidar(d, meta, 5.0)
        tab = json.load(open(os.path.join(d, "consolidado", "tabela.json")))
        r.check(ok and set(tab) == {"claude-haiku-5-5|high", "claude-sonnet-5-5|high",
                                    "cascata|claude-haiku-5-5→claude-sonnet-5-5"}, "16.sem_dev", sorted(tab))
        r.check(abs(A.gasto_acumulado(d) - 3 * 0.00055) < 1e-12, "16.consolidado_nao_conta_gasto")


_URL = re.compile(r"https?://|t\.me/|s\.shopee|meli\.la|amzn\.to")


def test_10_conjunto_de_casos_reais_bem_formado(r):
    dados = json.load(open(A.CASOS, encoding="utf-8"))
    casos = dados["casos"]
    ids = [d["id"] for d in casos]
    r.check(len(ids) == len(set(ids)) and len(ids) >= 60, "10.ids_unicos", len(ids))
    erros = []
    for d in casos:
        aceitos = set(d.get("aceitos") or [d["rotulo"]])
        if d["rotulo"] not in J.VEREDITOS or not aceitos <= set(J.VEREDITOS) or d["rotulo"] not in aceitos:
            erros.append((d["id"], "rotulo"))
        if d["deve"] not in ("UM_POST", "DOIS_POSTS") or d["fonte_rotulo"] not in ("dono", "claude"):
            erros.append((d["id"], "deve/fonte"))
        if d.get("tipo", "par_canal") not in ("par_canal", "absorvido"):
            erros.append((d["id"], "tipo"))
        if (d["deve"] == "UM_POST") != ("MESMA_CAMPANHA" in aceitos) and not d.get("lembrete_esperado"):
            erros.append((d["id"], "deve x rotulo"))
        itens = [d["alvo"]] + d["candidatos"]
        if not d["candidatos"] or any(not it["texto"].strip() for it in itens):
            erros.append((d["id"], "texto vazio"))
        if any(_URL.search(it["texto"]) for it in itens):
            erros.append((d["id"], "link no texto"))
    r.check(not erros, "10.campos", erros[:5])
    r.check(all(d["fonte_rotulo"] == "dono" for d in casos if d["id"].startswith("SN")),
            "10.marcacao_do_dono_preservada")


def test_11_casos_do_dono_e_armadilhas_presentes(r):
    meta = {d["id"]: d for d in json.load(open(A.CASOS, encoding="utf-8"))["casos"]}
    # marcação do dono de 14/10, item 10 corrigido em 15/10 (R1)
    sn = "1S 2S 3S 4S 5S 6S 7S 8S 9S 10S 11S 12S 13S 14S 15N 16S 17N 18S 19S 20N 21S 22S 23N".split()
    for x in sn:
        k, v = int(x[:-1]), x[-1]
        d = meta.get(f"SN{k:02d}")
        r.check(d is not None and d["deve"] == ("UM_POST" if v == "S" else "DOIS_POSTS"),
                f"11.sn{k:02d}", d and d["deve"])
    r.check(meta["BASE_29047_29048"]["deve"] == "UM_POST", "11.base")
    armas = [d for d in meta.values() if d["id"].startswith("LIVE_2874")]
    r.check(len(armas) == 4 and all(d["deve"] == "DOIS_POSTS" and d["rotulo"] == "PRODUTOS_DIFERENTES"
                                    for d in armas), "11.armadilha_live", len(armas))
    for k, deve in (("TRIO_ML_111397", "UM_POST"), ("TRIO_ML_29156", "UM_POST"), ("RENOV_29161", "UM_POST")):
        r.check(meta.get(k, {}).get("deve") == deve and meta[k]["fonte_rotulo"] == "dono", f"11.{k}")
    live2 = [d for d in meta.values() if d["id"].startswith("LIVE_2914")]
    r.check(len(live2) == 5 and all(d["rotulo"] == "PRODUTOS_DIFERENTES" and d["contexto"]["so_container_live"]
                                    for d in live2), "11.live_29140_29145", len(live2))
    r.check(meta["F25"]["deve"] == "DOIS_POSTS" and meta["F25"].get("lembrete_esperado"), "11.F25_R2_lembrete")
    fps = [meta.get(k) for k in ("FP_111092_27046", "FP_111103_27126", "FP_120304_27055")]
    r.check(all(d and d["tipo"] == "absorvido" and d["deve"] == "DOIS_POSTS" for d in fps), "11.fps_do_dono")
    hold = sum(A.em_holdout(d) for d in meta.values())
    r.check(0.3 * len(meta) <= hold <= 0.6 * len(meta), "11.holdout_tamanho", hold)


if __name__ == "__main__":
    sys.exit(rodar(globals(), "BRAIN v0 · bancada e casos"))
