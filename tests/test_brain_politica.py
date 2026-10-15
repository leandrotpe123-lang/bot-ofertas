"""
Brain v0 — política determinística (veredito + contexto → ação) e
pré-filtro (o que vai para a IA). Hermético, sem rede.

    python tests/test_brain_politica.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar                         # noqa: E402

RAIZ = preparar()
# brain/ é a raiz do serviço Brain; o pacote é brain/brain/
sys.path.insert(0, os.path.join(RAIZ, "brain"))
from brain import config, juiz as J, politica as P, prefiltro as F   # noqa: E402

CFG = ("claude-haiku-5-5", "high")


def v(veredito, certeza=0.95, lembrete=False, acao="NADA", cfg=CFG):
    return J.Veredito(veredito, certeza, (), (), acao, lembrete, cfg[0], cfg[1])


class _Validada:
    """Liga a configuração como se tivesse passado no portão (só no teste)."""
    def __enter__(self):
        self.antes = config.CONFIGS_VALIDADAS
        config.CONFIGS_VALIDADAS = frozenset({CFG})

    def __exit__(self, *_):
        config.CONFIGS_VALIDADAS = self.antes


def ctx(**kw):
    base = dict(tipo="par_canal", delta_s=30.0, modo="ACAO")
    base.update(kw)
    return P.Contexto(**base)


def test_01_padroes_seguros(r):
    r.check(config.BRAIN_MODO == "SOMBRA", "01.modo_padrao_sombra")
    r.check(config.CONFIGS_VALIDADAS == frozenset(), "01.nenhuma_config_validada")
    d = P.decidir(v("MESMA_CAMPANHA"), ctx())
    r.check(d.acao == "REGISTRAR" and d.faria == "PEDIR_FUNDIR"
            and d.motivo.startswith("config_nao_validada"), "01.nao_validada_so_registra", str(d))


def test_02_sombra_registra_o_que_faria(r):
    with _Validada():
        d = P.decidir(v("MESMA_CAMPANHA"), ctx(modo="SOMBRA"))
    r.check(d.acao == "REGISTRAR" and d.faria == "PEDIR_FUNDIR", "02.sombra", str(d))


def test_03_limiares_de_certeza(r):
    with _Validada():
        alta = P.decidir(v("MESMA_CAMPANHA", 0.95), ctx())
        media = P.decidir(v("MESMA_CAMPANHA", 0.80), ctx())
        baixa = P.decidir(v("MESMA_CAMPANHA", 0.50), ctx())
    r.check(alta.acao == "PEDIR_FUNDIR", "03.alta", str(alta))
    r.check(media.acao == "ALERTAR", "03.media", str(media))
    r.check(baixa.acao == "REGISTRAR", "03.baixa", str(baixa))


def test_04_o_que_nunca_age(r):
    with _Validada():
        casos = {
            "post_apagado": ctx(candidato_apagado=True),
            "fora_da_janela": ctx(em_janela=False),
            "live_nunca_funde": ctx(so_container_live=True),
            "conteudo_exclusivo": ctx(exclusivo_no_perdedor=True),
        }
        for motivo, c in casos.items():
            d = P.decidir(v("MESMA_CAMPANHA", 0.99), c)
            r.check(not d.acao.startswith("PEDIR_") and motivo in d.motivo,
                    f"04.{motivo}", str(d))
        d = P.decidir(v("INCERTO", 0.0), ctx())
        r.check(d.acao == "REGISTRAR", "04.incerto", str(d))


def test_05_lembrete_minutos_depois_e_alerta_legitimo(r):
    # item 23 da marcação do dono (16 min, "Ainda ativo!") = N; já a
    # renovação no mesmo minuto (29047/29048) = UM post.
    with _Validada():
        tarde = P.decidir(v("MESMA_CAMPANHA", 0.95, lembrete=True), ctx(delta_s=16 * 60))
        junto = P.decidir(v("MESMA_CAMPANHA", 0.95, lembrete=True), ctx(delta_s=18))
    r.check(tarde.acao == "NADA" and tarde.motivo == "lembrete_alerta_legitimo", "05.tarde", str(tarde))
    r.check(junto.acao == "PEDIR_FUNDIR", "05.mesmo_minuto_funde", str(junto))


def test_06_liberacao_e_absorcao(r):
    with _Validada():
        lib = P.decidir(v("LIBERACAO_NOVA", 0.95), ctx(tipo="absorvido", delta_s=3300))
        lib_par = P.decidir(v("LIBERACAO_NOVA", 0.95), ctx())
        errada = P.decidir(v("PRODUTOS_DIFERENTES", 0.95), ctx(tipo="absorvido"))
        certo = P.decidir(v("PRODUTOS_DIFERENTES", 0.95), ctx())
    r.check(lib.acao == "PEDIR_REINJETAR", "06.liberacao_perdida", str(lib))
    r.check(lib_par.acao == "NADA", "06.liberacao_ja_tem_post", str(lib_par))
    r.check(errada.acao == "ALERTAR" and errada.motivo == "absorcao_errada", "06.absorcao_errada", str(errada))
    r.check(certo.acao == "NADA", "06.separados_certo", str(certo))


def test_07_a_acao_da_ia_nunca_decide(r):
    with _Validada():
        d = P.decidir(v("PRODUTOS_DIFERENTES", 0.99, acao="FUNDIR"), ctx())
    r.check(d.acao == "NADA", "07.ia_pede_fundir_mas_politica_nao", str(d))


def test_08_modo_alerta_e_limites_por_hora(r):
    with _Validada():
        a = P.decidir(v("MESMA_CAMPANHA"), ctx(modo="ALERTA"))
        lim = P.decidir(v("MESMA_CAMPANHA"), ctx(acoes_ultima_hora=config.ACOES_POR_HORA))
        lim2 = P.decidir(v("MESMA_CAMPANHA", 0.8),
                         ctx(alertas_ultima_hora=config.ALERTAS_POR_HORA))
        lixo = P.decidir(v("MESMA_CAMPANHA"), ctx(modo="QUALQUER"))
    r.check(a.acao == "ALERTAR", "08.alerta_nao_age", str(a))
    r.check(lim.acao == "ALERTAR" and "limite_acoes_hora" in lim.motivo, "08.limite_acoes", str(lim))
    r.check(lim2.acao == "REGISTRAR" and "limite_alertas_hora" in lim2.motivo, "08.limite_alertas", str(lim2))
    r.check(lixo.acao == "REGISTRAR", "08.modo_desconhecido_vira_sombra", str(lixo))


def test_09_caso_real_28697_28699_apagados_nunca_agem(r):
    # 13/10: Fumotom 39267 → 28697 e Samuel 121195 → 28699, mesmo produto
    # (MLB18725308); as duas fontes apagaram e o worker removeu os dois posts
    # ([ORIGEM_APAGADA] 14:16 e 14:20). O Brain nunca pede fusão nem
    # reinjeção com post apagado, nem com certeza máxima.
    with _Validada():
        for c in (ctx(candidato_apagado=True), ctx(alvo_apagado=True),
                  ctx(tipo="absorvido", alvo_apagado=True)):
            for ver in ("MESMA_CAMPANHA", "LIBERACAO_NOVA"):
                d = P.decidir(v(ver, 1.0), c)
                r.check(d.acao == "NADA" and d.motivo == "post_apagado",
                        f"09.{ver}.{c.tipo}", str(d))


def test_10_isolamento_do_brain(r):
    """O pacote do Brain (raiz do serviço = brain/) só importa biblioteca
    padrão, o SDK `anthropic` e ele mesmo. Nunca worker, Telethon nem
    sqlite3: o Brain não tem sessão do Telegram nem abre o banco do worker
    (CLAUDE.md §2), e na Railway o código do worker nem está na imagem."""
    import ast
    pasta = os.path.join(RAIZ, "brain", "brain")
    proibidos = {"telethon", "sqlite3", "aiosqlite", "pipeline", "database", "config",
                 "main", "eventos", "plataformas", "utils", "web", "client", "globals"}
    permitidos = set(sys.stdlib_module_names) - {"sqlite3"} | {"anthropic", "brain"}
    achados = []
    for nome in sorted(os.listdir(pasta)):
        if not nome.endswith(".py"):
            continue
        arv = ast.parse(open(os.path.join(pasta, nome), encoding="utf-8").read())
        for no in ast.walk(arv):
            mods = ([a.name for a in no.names] if isinstance(no, ast.Import)
                    else [no.module or ""] if isinstance(no, ast.ImportFrom) and not no.level else [])
            for m in mods:
                topo = m.split(".")[0]
                if topo in proibidos or topo not in permitidos:
                    achados.append(f"{nome}:{m}")
    r.check(not achados, "10.so_stdlib_anthropic_brain", achados)
    r.check(not any(os.path.isdir(os.path.join(pasta, x)) and x != "__pycache__"
                    for x in os.listdir(pasta)), "10.sem_subpacote_escondido")


def test_11_servico_fase0_nao_faz_nada(r):
    """`python -m brain` (start do serviço) existe, sai com 0 e não importa
    SDK, rede nem worker; a versão do Python vem de brain/.python-version
    (Railpack lê; o railway.toml não vale para serviço novo)."""
    import subprocess
    raiz_servico = os.path.join(RAIZ, "brain")
    p = subprocess.run([sys.executable, "-I", "-c",
                        "import runpy, sys; sys.path.insert(0, '.'); "
                        "runpy.run_module('brain', run_name='__main__')"],
                       cwd=raiz_servico, capture_output=True, text=True, timeout=60)
    r.check(p.returncode == 0 and "fase 0" in p.stdout, "11.sai_com_zero", (p.returncode, p.stdout, p.stderr[-300:]))
    versao = open(os.path.join(raiz_servico, ".python-version")).read().strip()
    r.check(versao == "3.13", "11.python_3_13_como_producao_e_ci", versao)


# ── pré-filtro (textos reais da prévia pública, links trocados) ──
T0 = 1_792_000_000.0
P29047 = F.PostPrevia("canal:29047", T0, "🔥 Cupons Shopee Renovados\n\n📌 Resgate aqui:\n1 x\n2 y",
                      ("https://s.shopee.com.br/AAH", "https://s.shopee.com.br/7fa"))
P29048 = F.PostPrevia("canal:29048", T0 + 8, "🚨🚨 Renovados! Dia de Cupons Shopee\n\n👇 Resgate todos os cupons aqui:",
                      ("https://s.shopee.com.br/4LK",))
PROD_A = F.PostPrevia("canal:1", T0, "🔥 Loção Hidratante Corporal Isdin Ureadin 10\n\n✅ R$ 64\n🎟 CUPOM: BELEZA25",
                      ("https://meli.la/1yww8rC",))
PROD_B = F.PostPrevia("canal:2", T0 + 60, "🔥 Loção Hidratante Corporal Isdin Ureadin 10\n\n✅ R$ 64",
                      ("https://meli.la/OUTRO",))
COD_A = F.PostPrevia("canal:3", T0, "Cupom Mercado Livre\n15% OFF: OFERTAMELI", ("https://meli.la/a",),
                     marcados=("OFERTAMELI",))
COD_B = F.PostPrevia("canal:4", T0 + 30, "🎟 Cupom de 15% no Mercado Livre\nCUPOM: OFERTAMELI", ("https://meli.la/b",),
                     marcados=("ofertameli ",))
LIVE = F.PostPrevia("canal:28741", T0, "🔴 Live Shopee — controle Xbox por R$ 299", ("https://live.shopee.com.br/live/7462128",))


def test_20_classificacao_pela_previa(r):
    r.check(F.tipo(P29047) == "cupom" and F.tipo(P29048) == "cupom", "20.cupom_seco_e_especifico")
    r.check(F.tipo(PROD_A) == "produto", "20.produto_com_preco")
    r.check(F.tipo(LIVE) == "live", "20.live")
    # post do canal: produto é só o que a montagem do worker marcou como PREÇO
    cup = F.PostPrevia("canal:28738", T0, "🔥 Cupom Mercado Livre Produtos Selecionados\n\n"
                       "🎟 10% OFF, Limite de R$ 20 OFF: MODA1310\n\n📌 Resgate o cupom aqui:")
    r.check(F.tipo(cup) == "cupom", "20.cupom_com_valor_nao_e_produto")
    r.check(F.tipo(F.PostPrevia("canal:26858", T0, "🔥 Cupons Mercado Livre\n\n🎟 R$ 10 em compras acima de R$ 49"))
            == "cupom", "20.cupom_em_reais")
    r.check(F.plataforma(P29047) == "shopee" and F.plataforma(PROD_A) == "mercadolivre", "20.plataforma")
    r.check(F.codigos(COD_A) == {"OFERTAMELI"} and F.codigos(COD_B) == {"OFERTAMELI"}, "20.codigo",
            str(F.codigos(COD_A)))
    # doutrina do worker (utils/cupom.py): só o marcado como código conta
    sem_marca = F.PostPrevia("x", 0, "🎟 CUPOM: OFERTAMELI\n🚨🚨 TUTORIAL: CUPOM SHOPEE VIP liberado")
    r.check(F.codigos(sem_marca) == frozenset(), "20.nada_inferido_do_texto")
    forma = F.PostPrevia("x", 0, "", marcados=("1200", "AB", "R$ 10 OFF", "C0M3C0U200"))
    r.check(F.codigos(forma) == {"C0M3C0U200"}, "20.forma_tecnica", str(F.codigos(forma)))


def test_21_classe1_seco_x_especifico_vai_para_ia(r):
    r.check([c.id for c in F.classe1(P29048, [P29047])] == ["canal:29047"], "21.par_29047_29048")


def test_22_mesmo_codigo_e_mesmo_link_ficam_fora(r):
    r.check(F.classe1(COD_B, [COD_A]) == [], "22.mesmo_codigo_deterministico")
    igual = F.PostPrevia("canal:9", T0 + 60, PROD_A.texto, PROD_A.links)
    r.check(F.classe2(igual, [PROD_A]) == [], "22.mesmo_link_deterministico")


def test_23_classe2_mesmo_titulo_e_preco_link_diferente(r):
    r.check([c.id for c in F.classe2(PROD_B, [PROD_A])] == ["canal:1"], "23.mlb_x_mlbu")
    outro_preco = F.PostPrevia("canal:5", T0 + 60, "🔥 Loção Hidratante Corporal Isdin Ureadin 10\n\n✅ R$ 89",
                               ("https://meli.la/z",))
    r.check(F.classe2(outro_preco, [PROD_A]) == [], "23.preco_diferente_fora")


def test_24_janela_e_plataforma(r):
    velho = F.PostPrevia("canal:7", T0 - 3700, P29047.texto, P29047.links)
    r.check(F.classe1(P29048, [velho]) == [], "24.fora_da_janela")
    r.check(F.classe1(P29048, [COD_A]) == [], "24.outra_plataforma")


def test_25_classe3_cupom_da_fonte_sem_post(r):
    msg = F.PostPrevia("fonte:fadadoscupons/17831", T0 + 8, P29048.texto, P29048.links, "fonte", "fadadoscupons")
    r.check([c.id for c in F.classe3(msg, [P29047], virou_post=False)] == ["canal:29047"], "25.sem_post")
    r.check(F.classe3(msg, [P29047], virou_post=True) == [], "25.virou_post_fora")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "BRAIN v0 · política e pré-filtro"))
