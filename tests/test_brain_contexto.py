"""
Brain — contexto do dia em tempo real (decisão do dono, 16/10): janela de
24 h CORRIDAS (nunca o dia do calendário), só em RAM, e a escolha dos
candidatos pelo código (mesmo link → mesmo código → mesmo produto → texto
parecido → mesma plataforma; no máximo N). Hermético.

    python tests/test_brain_contexto.py
"""
import contextlib
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar                         # noqa: E402

RAIZ = preparar()
sys.path.insert(0, os.path.join(RAIZ, "brain"))
from brain import cliente as CL, contexto as C, divisao, exemplos as E, juiz as J  # noqa: E402

H = 3600.0


def ts(iso):
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp()


def post(pid, iso, texto, links=(), codigos=(), estado="vivo", grupo=""):
    return C.Post(f"canal:{pid}", "canal", ts(iso), texto, tuple(links), tuple(codigos),
                  grupo=grupo, estado=estado)


# ══════════════════════════════════════════════════════════════════
# Contexto do dia (24 h corridas, RAM)
# ══════════════════════════════════════════════════════════════════
def test_10_janela_e_24h_corridas_e_cruza_a_meia_noite(r):
    alvo = post(10, "2026-10-16T03:10:00", "Cupons Shopee 20% OFF", ["https://s.shopee.com.br/AAA"])
    dentro = post(1, "2026-10-15T23:50:00", "Cupons Shopee", ["https://s.shopee.com.br/AAA"])   # outro dia BRT/UTC
    limite = post(2, "2026-10-15T03:10:00", "Cupons Shopee", ["https://s.shopee.com.br/AAA"])   # 24 h exatas
    fora = post(3, "2026-10-15T03:09:59", "Cupons Shopee", ["https://s.shopee.com.br/AAA"])     # 24 h + 1 s
    depois = post(4, "2026-10-16T03:11:00", "Cupons Shopee", ["https://s.shopee.com.br/AAA"])   # depois do alvo
    ids = [c.post.id for c in C.candidatos(alvo, [dentro, limite, fora, depois])]
    r.check(ids == ["canal:1", "canal:2"], "10.so_as_24h_antes_do_alvo", ids)


def test_11_ordem_dos_motivos_link_codigo_produto_texto_plataforma(r):
    alvo = post(20, "2026-10-16T12:00:00", "🔥 Fone Bluetooth JBL Tune 520\n\n✅ R$ 199\n🎟 CUPOM: FONE10",
                ["https://meli.la/AbC1"], ["FONE10"])
    plat = post(1, "2026-10-16T11:59:00", "Air fryer Mondial 4L\n\n✅ R$ 299", ["https://meli.la/ZZZ9"])
    texto = post(2, "2026-10-16T11:00:00", "Fone Bluetooth JBL Tune 520 com cupom FONE10 hoje")
    prod = post(3, "2026-10-16T10:00:00", "🔥 Fone Bluetooth JBL Tune 520\n\n✅ R$ 189", ["https://meli.la/OUTRO"])
    cod = post(4, "2026-10-16T09:00:00", "Cupom Mercado Livre", ["https://meli.la/X"], ["FONE10"])
    link = post(5, "2026-10-16T08:00:00", "Oferta", ["https://meli.la/AbC1/"])
    outra = post(6, "2026-10-16T11:30:00", "Cupons Shopee 10% OFF", ["https://s.shopee.com.br/Q"])
    res = C.candidatos(alvo, [plat, texto, prod, cod, link, outra])
    r.check([c.motivo for c in res] == ["mesmo_link", "mesmo_codigo", "mesmo_produto", "texto_parecido",
                                        "mesma_plataforma"], "11.ordem", [(c.post.id, c.motivo) for c in res])
    r.check("canal:6" not in [c.post.id for c in res], "11.outra_plataforma_sem_sinal_fica_fora")


def test_12_teto_de_candidatos_e_apagado_fora(r):
    alvo = post(100, "2026-10-16T12:00:00", "Cupons Shopee", ["https://s.shopee.com.br/A"])
    muitos = [post(i, f"2026-10-16T{10 + i // 60:02d}:{i % 60:02d}:00", "Cupons Shopee", ["https://s.shopee.com.br/A"])
              for i in range(1, 40)]
    r.check(len(C.candidatos(alvo, muitos)) == C.N_CANDIDATOS == 15, "12.teto_15")
    r.check(len(C.candidatos(alvo, muitos, n=5)) == 5, "12.n_configuravel")
    ap = post(1, "2026-10-16T11:00:00", "Cupons Shopee", ["https://s.shopee.com.br/A"], estado="apagado")
    r.check(C.candidatos(alvo, [ap]) == [], "12.apagado_nao_e_candidato")
    mais_perto = C.candidatos(alvo, muitos)[0].post.id
    r.check(mais_perto == "canal:39", "12.empate_vai_pro_mais_proximo", mais_perto)


def test_13_janela_so_em_ram_poda_e_relida(r):
    j = C.Janela()
    for i in range(5):
        j.ver(post(i, f"2026-10-15T0{i}:00:00", f"t{i}"))
    j.podar(ts("2026-10-16T02:30:00"))                    # corta antes de 15/10 02:30
    r.check(sorted(p.id for p in j.itens()) == ["canal:3", "canal:4"], "13.poda_24h")
    j.ver(post(4, "2026-10-15T04:00:00", "t4 editado"))
    r.check(len(j) == 2 and next(p for p in j.itens() if p.id == "canal:4").texto == "t4 editado",
            "13.edicao_substitui")
    j.apagado("canal:4")
    r.check(next(p for p in j.itens() if p.id == "canal:4").estado == "apagado", "13.post_not_found")
    pequeno = C.Janela(max_itens=3)
    for i in range(6):
        pequeno.ver(post(i, f"2026-10-15T0{i}:00:00", "x"))
    r.check(sorted(p.id for p in pequeno.itens()) == ["canal:3", "canal:4", "canal:5"], "13.guarda_de_memoria")
    fonte = open(C.__file__, encoding="utf-8").read()
    r.check(not any(s in fonte for s in ("sqlite3", "open(", "import os", "requests", "urllib")),
            "13.sem_banco_arquivo_nem_rede")


def test_14_sinais_plataforma_link_produto(r):
    p = post(1, "2026-10-16T00:00:00", "x", ["https://meli.la/1yww8rC"])
    r.check(C.plataforma(p) == "mercadolivre", "14.meli_la")
    r.check(C.plataforma(post(2, "2026-10-16T00:00:00", "x", ["https://s.shopee.com.br/AAHdNLxGSX"])) == "shopee",
            "14.shopee_curto")
    r.check(C.plataforma(post(3, "2026-10-16T00:00:00", "x", ["https://link.amazon/B0hJ8Hvya"])) == "amazon",
            "14.amazon")
    r.check(C.plataforma(post(4, "2026-10-16T00:00:00", "Cupom Magalu R$ 100 OFF")) == "magalu", "14.pelo_nome")
    r.check(C.link_normalizado("https://meli.la/1yww8rC/") == "meli.la/1yww8rC"
            and C.link_normalizado("https://meli.la/1YWW8RC") != "meli.la/1yww8rC", "14.caminho_diferencia_caixa")
    r.check(C.eh_produto("🔥 Fone\n\n✅ R$ 64") and not C.eh_produto("🎟 20% OFF acima de R$89, limite R$40"),
            "14.produto_x_cupom")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "Brain · contexto do dia (24 h corridas, RAM, candidatos)"))
