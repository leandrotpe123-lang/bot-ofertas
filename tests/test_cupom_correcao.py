"""
CUPOM SEM CÓDIGO — correções da sombra de 14/10 (frente CUPOM-SEM-CÓDIGO).

Casos reais (logs da Railway + prévia pública; marcação do dono em 15/10):

  (a) VALOR. 14/10 22:00 UTC, relâmpago ML de R$ 60 para a meia-noite:
      Fada 17836 "Cupom R$60 OFF Mercado Livre" → post 29152;
      Promotom 111397 "Próximo Cupom R$ 60 OFF em R$ 499" (Δ 69 s) foi
      absorvida no 29152 (certo), mas a sombra disse LIBERAÇÃO
      (beneficio_diferente: v:60-499 × v:60) — o dono marcou ERRADA (1N);
      Samuel 121483 (Δ 184 s) virou o post 29156 (duplicata, apagada à mão
      pelo dono); a sombra juntou só porque a 1ª versão da Samuel ainda não
      tinha o mínimo (2S). Regra do dono: mesmo desconto com um lado sem
      mínimo declarado = compatível; mínimos declarados diferentes =
      diferente; o resto não muda.

  (b) RENOVADOS. Fontes diferentes anunciam "cupons renovados" no mesmo
      instante, sem benefício dos dois lados: 14/10 14:59 UTC Promotom
      111375 "Cupons Shopee Renovados" (post 29047) × Fada 17831 "Renovados!
      Dia de Cupons Shopee" (post 29048, Δ 18 s); 14/10 23:00 Fada 17838
      (29160; Samuel 121488 absorvida) × Promotom 111400 (29161, Δ 75 s,
      duplicata apagada à mão). A sombra disse INCERTO sem_beneficio; o dono
      quer UM post e aprovou regra estreita: fontes diferentes, mesma
      plataforma, Δ ≤ RAJADA, os dois sem benefício e os dois com
      "renovad" → JUNTA (motivo `renovados`).

  (c) "EM SELECIONADOS". Sombra retroativa de 07–15/10, linhas 13 e 16
      (10/10): Samuel 120785 "Cupom Mercado Livre em Selecionados" × Fada
      17767 "em Moda", os dois com MODA1010; Samuel 120841 "em Selecionados"
      × Fada 17783 "em Auto e Moto", os dois com AUTOS1010. A regra dizia
      NOVA nome_conflitante (`selecionad` × `mod` / `aut-mot`). Decisão do
      dono (16/10): no Mercado Livre, "em selecionados" e "produtos
      selecionados" são genéricos; nunca "Contas Selecionadas" (Shopee,
      Promotom 111161) nem outra plataforma.

    python tests/test_cupom_correcao.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_cupom_lancamento as T                                # noqa: E402
from _harness_e5 import rodar                                   # noqa: E402

ML, SHP = T.ML, T.SHP

# Prévia pública (embed), texto fiel sem os links — 14/10/2026.
T.TEXTO.update({
    "fadadoscupons/17836": '🔥 Cupom R$60 OFF Mercado Livre \n\nResgate agora e use 00h\n\n🚨🚨 LEIA PARA RESGATAR:\n1. Entre no APP 👇\n\n2. Clique em "Início" no rodapé do APP\n3. Depois, escolha o banner “Cupons Relâmpago”\n4. Resgate o cupom',
    "promotom/111397": 'Próximo Cupom R$ 60 OFF em R$ 499 Mercado Livre, resgate agora e use meia-noite \n\n•Tutorial para resgate: \n1 Entre no APP do Meli aqui:  \n2 Clique em "Início" no rodapé do APP \n3 Depois no carrossel no canto superior esquerdo clique em "CUPONS RELÂMPAGO TODOS OS DIAS" \n4 Resgate o cupom',
    # versão FINAL da Samuel (editada 4×), com o mínimo
    "SamuelF3lipePromo/121483": '🔥 Próximo Cupom R$ 60 OFF em R$ 499 Mercado Livre\n\n⚠️ Resgate agora e use MEIA-NOITE\n\n⭐️ Tutorial para resgate:\n1. Entre no APP do Meli aqui: \n2. Clique em "Início" no rodapé do APP \n3. Depois no carrossel no canto superior esquerdo clique em "CUPONS RELÂMPAGO TODOS OS DIAS" \n4. Resgate o cupom\n\nanúncio',
})
T.TEXTO.update({
    "promotom/111375": 'Cupons Shopee Renovados\n\n-Resgate aqui:\n1 \n2',
    "fadadoscupons/17831": '🚨🚨 Renovados! Dia de Cupons Shopee\n\n👇 Resgate todos os cupons aqui:',
    "fadadoscupons/17838": '🚨🚨 Renovados! Dia de Cupons Shopee\n\n👇 Resgate todos os cupons aqui:',
    "SamuelF3lipePromo/121488": '🔥 Renovados! Dia de Cupons Shopee\n\n⭐️ Resgate todos os cupons aqui:\n\n\nanúncio',
    "promotom/111400": 'Cupons Shopee Renovados\n\n-Resgate aqui:\n1 \n2',
})
# 1ª versão da Samuel, a que o worker leu às 22:04:15 (a linha da sombra
# mostra valores=(v:60|v:60)): o mesmo texto sem " em R$ 499". DERIVADA do
# texto real — a prévia só guarda a versão final.
T.TEXTO["SamuelF3lipePromo/121483_v1"] = T.TEXTO["SamuelF3lipePromo/121483"].replace(" em R$ 499", "", 1)

# Δ reais entre as mensagens (data da prévia): 22:00:01 → 22:01:10 → 22:04:14
TRIO_V1 = [("fadadoscupons/17836", ML, 0), ("promotom/111397", ML, 69),
           ("SamuelF3lipePromo/121483_v1", ML, 184)]
TRIO_FINAL = [("fadadoscupons/17836", ML, 0), ("promotom/111397", ML, 69),
              ("SamuelF3lipePromo/121483", ML, 184)]


def test_01_mesmo_desconto_um_lado_sem_minimo_e_compativel(r):
    CL = T._CL()
    fada = CL.perfil(T.TEXTO["fadadoscupons/17836"], ML)
    prom = CL.perfil(T.TEXTO["promotom/111397"], ML)
    r.check(fada.valores == {"v:60"} and prom.valores == {"v:60-499"}, "01.assinaturas_reais",
            (set(fada.valores), set(prom.valores)))
    r.check(CL._beneficio(prom, fada) == "igual", "01.v60_499_x_v60_igual", CL._beneficio(prom, fada))
    r.check(CL._beneficio(fada, prom) == "igual", "01.simetrico")


def test_02_minimos_declarados_diferentes_e_desconto_diferente_seguem_diferentes(r):
    CL = T._CL()
    P = CL.Perfil
    casos = {
        "minimos_diferentes": (P("", "", frozenset({"v:60-499"}), False),
                               P("", "", frozenset({"v:60-299"}), False), "diferente"),
        "desconto_diferente_sem_minimo": (P("", "", frozenset({"v:60"}), False),
                                          P("", "", frozenset({"v:50-499"}), False), "diferente"),
        "mesmo_par": (P("", "", frozenset({"v:60-499"}), False),
                      P("", "", frozenset({"v:60-499"}), False), "igual"),
        "um_entre_varios": (P("", "", frozenset({"v:30-299", "v:90-899"}), False),
                            P("", "", frozenset({"v:90"}), False), "igual"),
        # nome × sem nome por valor continua não juntando (variante B)
        "nome_x_sem_nome": (P("tech", "", frozenset({"v:30-299"}), False),
                            P("", "", frozenset({"v:30"}), False), "valor_nome_x_sem_nome"),
    }
    for nome, (a, b, esperado) in casos.items():
        r.check(CL._beneficio(a, b) == esperado, f"02.{nome}", CL._beneficio(a, b))


def _decide(cand, chat, vivos, familia_hoje, delta_s):
    """A decisão com as MESMAS entradas da linha do log (família de hoje,
    fontes do post, Δ desde o nascimento)."""
    import config
    CL = T._CL()
    t0 = 1_792_000_000.0
    posts = [CL.PostVivo(mid, CL.perfil(T.TEXTO[k], ML), t0, frozenset(f)) for mid, k, f in vivos]
    return CL.classificar(CL.perfil(T.TEXTO[cand], ML), chat, t0 + delta_s, posts, familia_hoje,
                          config.CUPOM_RAJADA_S[ML], config.CUPOM_LIMITE_SUPERIOR_S)


def test_03a_reproduz_a_linha_1_da_sombra(r):
    # log 14/10 22:01:11: [CUPOM_LIBERACAO_SOMBRA] msg=(promotom,111397)
    # post:29152 motivo=beneficio_diferente Δ=1.1min hoje=post:29152
    # valores=(v:60-499|v:60) fontes_post=[fada] — o dono marcou ERRADA.
    d = _decide("promotom/111397", T.PROMOTOM, [(29152, "fadadoscupons/17836", {T.FADA})], 29152, 69)
    r.check(d.acao == "MANTER" and d.post == 29152, "03a.mesmo_lancamento_fica_junto",
            (d.acao, d.motivo))


def test_03b_reproduz_a_linha_2_nas_duas_versoes_da_samuel(r):
    # log 14/10 22:04:15: [CUPOM_ENCONTRO_SOMBRA] msg=(samuel,121483)
    # post:29152 motivo=mesmo_lancamento Δ=4.2min hoje=post_novo
    # valores=(v:60|v:60) — certa (2S), mas só pela 1ª versão sem mínimo.
    vivos = [(29152, "fadadoscupons/17836", {T.FADA, T.PROMOTOM})]
    for versao in ("SamuelF3lipePromo/121483_v1", "SamuelF3lipePromo/121483"):
        d = _decide(versao, T.SAMUEL, vivos, None, 253)
        r.check(d.acao == "JUNTA" and d.post == 29152 and d.motivo == "mesmo_lancamento",
                f"03b.{versao.split('/')[1]}", (d.acao, d.motivo))


# Reencenação com os textos FINAIS da prévia. Diferença conhecida da
# produção: a 1ª versão da Promotom 111397 tinha a chave `relampago` e casou
# no 29152 (OVERLAP_MATCH 22:01:11); o texto final dá `proximo-use`, então
# aqui a Promotom vira post novo hoje e a sombra a junta (ENCONTRO). A
# Samuel, depois, vê DOIS posts vivos do mesmo lançamento: INCERTO ambiguo
# (salvaguarda que já existia — nunca escolhe sozinha entre dois).
ESPERADO_SOMBRA = [("CUPOM_ENCONTRO_SOMBRA", "mesmo_lancamento"), ("CUPOM_INCERTO", "ambiguo")]


def test_03_sombra_do_trio_com_a_1a_versao_da_samuel(r):
    s = T._cena(TRIO_V1, "SOMBRA")
    r.check(T._marcas(s["linhas"]) == ESPERADO_SOMBRA, "03.encontro_e_ambiguo", T._marcas(s["linhas"]))
    d = T._cena(TRIO_V1, "SOMBRA", T._desligado())
    r.check(s["foto"] == d["foto"], "03.sombra_nao_muda_nada")


def test_04_sombra_do_trio_com_a_versao_final_da_samuel(r):
    s = T._cena(TRIO_FINAL, "SOMBRA")
    r.check(T._marcas(s["linhas"]) == ESPERADO_SOMBRA, "04.encontro_e_ambiguo", T._marcas(s["linhas"]))
    d = T._cena(TRIO_FINAL, "SOMBRA", T._desligado())
    r.check(s["foto"] == d["foto"], "04.sombra_nao_muda_nada")


def test_05_ativo_o_trio_vira_um_post_so(r):
    for nome, passos in (("v1", TRIO_V1), ("final", TRIO_FINAL)):
        a = T._cena(passos, "ATIVO")["foto"]
        r.check(a["novos"] == 1 and len(set(a["posts"])) == 1, f"05.{nome}", (a["novos"], a["posts"]))


def test_06_ativo_minimos_declarados_diferentes_seguem_dois_posts(r):
    # guarda: a Fada declara outro mínimo → campanha diferente
    T.TEXTO["sintetico/fada_60_299"] = T.TEXTO["fadadoscupons/17836"].replace(
        "R$60 OFF", "R$60 OFF em R$299", 1)
    T.VEICULO["sintetico"] = T.VEICULO["fadadoscupons"]      # o mesmo link de tutorial da Fada
    passos = [("sintetico/fada_60_299", ML, 0), ("promotom/111397", ML, 69)]
    a = T._cena(passos, "ATIVO")["foto"]
    r.check(a["novos"] == 2, "06.dois_posts", a["novos"])


# ══════════════════════════════════════════════════════════════════
# (b) RENOVADOS
# ══════════════════════════════════════════════════════════════════
# Δ reais (data da prévia): 14:59:24 → 14:59:42; 22:59:56 → 23:00:03 → 23:01:11
PAR_29047 = [("promotom/111375", SHP, 0), ("fadadoscupons/17831", SHP, 18)]
TRIO_29160 = [("fadadoscupons/17838", SHP, 0), ("SamuelF3lipePromo/121488", SHP, 7),
              ("promotom/111400", SHP, 68)]


def _decide_shp(cand, chat, vivos, familia_hoje, delta_s):
    import config
    CL = T._CL()
    t0 = 1_792_000_000.0
    posts = [CL.PostVivo(mid, CL.perfil(T.TEXTO[k], SHP), t0, frozenset(f)) for mid, k, f in vivos]
    return CL.classificar(CL.perfil(T.TEXTO[cand], SHP), chat, t0 + delta_s, posts, familia_hoje,
                          config.CUPOM_RAJADA_S[SHP], config.CUPOM_LIMITE_SUPERIOR_S)


def test_10_marcador_renovado_sem_beneficio(r):
    CL = T._CL()
    for k in ("promotom/111375", "fadadoscupons/17831", "fadadoscupons/17838",
              "SamuelF3lipePromo/121488", "promotom/111400"):
        pf = CL.perfil(T.TEXTO[k], SHP)
        r.check(pf.renovado and not pf.tem_beneficio, f"10.{k.split('/')[1]}", (pf.renovado, pf.tem_beneficio))
    r.check(not CL.perfil(T.TEXTO["promotom/111105"], SHP).renovado, "10.cupons_shopee_seco_nao")
    # só "renovad…" conta: palavra parecida não é o marcador
    for t in ("Cupons Shopee VIP — renovação automática\n\n-Resgate aqui:",
              "Renove seu VIP e ganhe cupons\n\n-Resgate aqui:"):
        r.check(not CL.perfil(t, SHP).renovado, f"10.nao_e_marcador.{t[:14]}")
    for t in ("Cupom Shopee Renovado!", "Cupons renovadas", "🔥CUPONS RENOVADOS"):
        r.check(CL.perfil(t, SHP).renovado, f"10.marcador.{t[:14]}")


def test_11_reproduz_29047_29048(r):
    # 14/10 14:59: a Fada 17831 virou o post 29048 ao lado do 29047 (Promotom)
    d = _decide_shp("fadadoscupons/17831", T.FADA, [(29047, "promotom/111375", {T.PROMOTOM})], None, 18)
    r.check(d.acao == "JUNTA" and d.post == 29047 and d.motivo == "renovados", "11.junta", (d.acao, d.motivo))


def test_12_reproduz_a_linha_3_da_sombra_29160_29161(r):
    # log 14/10 23:01:11: [CUPOM_INCERTO] msg=(promotom,111400) post:29160
    # motivo=sem_beneficio Δ=1.1min hoje=post_novo fontes_post=[samuel, fada]
    d = _decide_shp("promotom/111400", T.PROMOTOM,
                    [(29160, "fadadoscupons/17838", {T.FADA, T.SAMUEL})], None, 75)
    r.check(d.acao == "JUNTA" and d.post == 29160 and d.motivo == "renovados", "12.junta", (d.acao, d.motivo))


def test_13_cenas_sombra_e_ativo(r):
    for nome, passos in (("29047", PAR_29047), ("29160", TRIO_29160)):
        s = T._cena(passos, "SOMBRA")
        r.check(T._marcas(s["linhas"]) == [("CUPOM_ENCONTRO_SOMBRA", "renovados")],
                f"13.sombra_{nome}", T._marcas(s["linhas"]))
        d = T._cena(passos, "SOMBRA", T._desligado())
        r.check(s["foto"] == d["foto"], f"13.sombra_nao_muda_nada_{nome}")
        a = T._cena(passos, "ATIVO")["foto"]
        r.check(a["novos"] == 1, f"13.ativo_um_post_{nome}", (a["novos"], a["posts"]))


def test_14_guardas_da_regra_estreita(r):
    T.TEXTO["sintetico/renovados_com_valor"] = "Cupons Shopee Renovados\n\nR$ 10 OFF em R$ 50\n-Resgate aqui:"
    T.TEXTO["sintetico/vip_renovados"] = "Cupons Shopee VIP Renovados\n\n-Resgate aqui:"
    T.TEXTO["sintetico/tech_renovados"] = "Cupons Shopee Tech Renovados\n\n-Resgate aqui:"
    post = [(1, "fadadoscupons/17831", {T.FADA})]
    casos = {
        # Δ acima da RAJADA: o tempo sozinho não decide (zona cinzenta)
        "acima_da_rajada": (("promotom/111375", T.PROMOTOM, post, None, 9 * 60), "INCERTO", "faixa_cinzenta"),
        # um lado sem "renovad": fica como hoje (candidato seco e post seco)
        "um_lado_seco": (("promotom/111105", T.PROMOTOM, post, None, 30), "INCERTO", "sem_beneficio"),
        "post_seco": (("fadadoscupons/17831", T.FADA, [(1, "promotom/111105", {T.PROMOTOM})], None, 30),
                      "INCERTO", "sem_beneficio"),
        # um lado com benefício declarado: não é a regra RENOVADOS
        "um_lado_com_valor": (("sintetico/renovados_com_valor", T.PROMOTOM, post, None, 30),
                              "INCERTO", "sem_beneficio"),
        # a MESMA fonte repete "renovados": mesma fonte decide antes (F50)
        "mesma_fonte": (("fadadoscupons/17838", T.FADA, post, 1, 6.7 * 60), "INCERTO", "lembrete_mesma_fonte"),
        # nomes em conflito continuam campanhas diferentes
        "nomes_em_conflito": (("sintetico/tech_renovados", T.PROMOTOM,
                               [(1, "sintetico/vip_renovados", {T.FADA})], 1, 30), "NOVA", "nome_conflitante"),
    }
    for nome, (args, acao, motivo) in casos.items():
        d = _decide_shp(*args)
        r.check((d.acao, d.motivo) == (acao, motivo), f"14.{nome}", (d.acao, d.motivo))


# ══════════════════════════════════════════════════════════════════
# (c) "EM SELECIONADOS" NO MERCADO LIVRE
# ══════════════════════════════════════════════════════════════════
# Prévia pública (embed), texto fiel sem os links — 10/10/2026.
T.TEXTO.update({
    "fadadoscupons/17767": '🔥 Cupom Mercado Livre em Moda\n\n🎟 20% OFF acima de R$89, limite R$40: MODA1010\n\n✅ Lista de produtos:',
    "SamuelF3lipePromo/120785": '🔥 Cupom Mercado Livre em Selecionados\n\n🎟 20% OFF acima de R$ 89, limite R$ 40: MODA1010\n\n⭐️ Válido na lista ↓ clique em "Mostrar mais":\n\n\nanúncio',
    "promotom/111265": 'Cupom Mercado Livre\n\n20% OFF, Limite de R$ 60 OFF em AUTO E FERRAMENTAS: AUTOS1010\n\n-Resgate o cupom aqui: \n\n-Anúncio',
    "SamuelF3lipePromo/120841": '🔥 Cupom Mercado Livre em Selecionados\n\n🎟 20% OFF acima de R$ 249, limite R$ 60: AUTOS1010\n\n⭐️ Válido na lista ↓ clique em "Mostrar mais":\n\n\nanúncio',
    "fadadoscupons/17783": '🔥 Cupom Mercado Livre em Auto e Moto\n\n🎟 20% OFF acima de R$249, limite R$60: AUTOS1010\n\n✅ Lista de produtos:',
    "fadadoscupons/17733": '🔥 Cupons Mercado Livre em Selecionados\n\n🎟 50% OFF acima de R$39, limite R$25: JOGOS50\nLista: \n\n🎟 R$100 OFF acima de R$899: TECH100\nLista:',
})


def _decide_real(cand, chat, plat, t, vivos, familia_hoje):
    """A decisão com as entradas da sombra retroativa: cada post vivo com o
    seu nascimento real (segundos) e as fontes que tinha naquele instante."""
    import config
    CL = T._CL()
    t0 = 1_792_000_000.0
    posts = [CL.PostVivo(mid, CL.perfil(T.TEXTO[k], plat), t0 + nasc, frozenset(f))
             for mid, k, nasc, f in vivos]
    return CL.classificar(CL.perfil(T.TEXTO[cand], plat), chat, t0 + t, posts, familia_hoje,
                          config.CUPOM_RAJADA_S[plat], config.CUPOM_LIMITE_SUPERIOR_S)


def test_20_em_selecionados_no_ml_nao_e_nome(r):
    CL = T._CL()
    nome = lambda k_ou_texto, plat=ML: CL.perfil(T.TEXTO.get(k_ou_texto, k_ou_texto), plat).nome
    for k in ("SamuelF3lipePromo/120785", "SamuelF3lipePromo/120841", "fadadoscupons/17733"):
        r.check(nome(k) == "", f"20.sem_nome.{k.split('/')[1]}", nome(k))
    # o nome de verdade da outra fonte continua
    r.check(nome("fadadoscupons/17767") == "mod", "20.em_moda", nome("fadadoscupons/17767"))
    r.check(nome("fadadoscupons/17783") == "aut-mot", "20.em_auto_e_moto", nome("fadadoscupons/17783"))
    # "produtos selecionados" também é genérico, e o resto do título fica
    r.check(nome("🔥 Cupom Mercado Livre Produtos Selecionados\n\n🎟 20% OFF") == "",
            "20.produtos_selecionados")
    r.check(nome("🔥 Cupom Mercado Livre em Produtos Selecionados de Beleza\n\n🎟 20% OFF") == "belez",
            "20.resto_do_titulo_fica",
            nome("🔥 Cupom Mercado Livre em Produtos Selecionados de Beleza\n\n🎟 20% OFF"))


def test_21_reproduz_a_linha_13_da_sombra_retroativa(r):
    # 10/10 20:00:02 Promotom 111252 → post 27964; 20:00:22 Fada 17767 "em
    # Moda" → post 27967; 20:02:22 Samuel 120785 "em Selecionados", o mesmo
    # MODA1010, absorvida no 27967. A regra dizia NOVA nome_conflitante.
    d = _decide_real("SamuelF3lipePromo/120785", T.SAMUEL, ML, 140,
                     [(27964, "promotom/111252", 0, {T.PROMOTOM}),
                      (27967, "fadadoscupons/17767", 20, {T.FADA})], 27967)
    r.check((d.acao, d.post, d.motivo) == ("MANTER", 27967, "mesmo_lancamento"),
            "21.fica_no_mesmo_post", (d.acao, d.post, d.motivo))


def test_22_reproduz_a_linha_16_da_sombra_retroativa(r):
    # 10/10 23:59:57 Promotom 111265 → post 28054; 00:00:13 Samuel 120841 "em
    # Selecionados" → post 28056; 00:00:14 Fada 17783 "em Auto e Moto", o
    # mesmo AUTOS1010, absorvida no 28056. A regra dizia NOVA nome_conflitante.
    d = _decide_real("fadadoscupons/17783", T.FADA, ML, 17,
                     [(28054, "promotom/111265", 0, {T.PROMOTOM}),
                      (28056, "SamuelF3lipePromo/120841", 16, {T.SAMUEL})], 28056)
    r.check((d.acao, d.post, d.motivo) == ("MANTER", 28056, "mesmo_lancamento"),
            "22.fica_no_mesmo_post", (d.acao, d.post, d.motivo))


def test_23_guardas_do_generico(r):
    CL = T._CL()
    nome = lambda texto, plat: CL.perfil(T.TEXTO.get(texto, texto), plat).nome
    # Shopee: "Contas Selecionadas" é campanha (Promotom 111161; Samuel 120479)
    for k in ("promotom/111161", "SamuelF3lipePromo/120479"):
        r.check(nome(k, SHP) == "cont-selecionad", f"23.contas_selecionadas_shopee.{k.split('/')[1]}",
                nome(k, SHP))
    # o genérico é só do ML: em outra plataforma, "em Selecionados" segue nome
    r.check(nome("🔥 Cupons Shopee em Selecionados\n\n🎟 R$ 10 OFF", SHP) == "selecionad",
            "23.outra_plataforma_nao", nome("🔥 Cupons Shopee em Selecionados\n\n🎟 R$ 10 OFF", SHP))
    # no ML, só a expressão "em/produtos selecionados" sai; "Contas Selecionadas" fica
    r.check(nome("Cupons Mercado Livre Contas Selecionadas\n\n20% OFF", ML) == "cont-selecionad",
            "23.contas_selecionadas_ml", nome("Cupons Mercado Livre Contas Selecionadas\n\n20% OFF", ML))
    # "em Moda" × "em Auto e Moto" no ML continua conflito: o mesmo benefício da
    # Fada 17767, outra fonte, outro nome → campanha diferente
    T.TEXTO["sintetico/auto_moto_mesmo_beneficio"] = T.TEXTO["fadadoscupons/17767"].replace(
        "em Moda", "em Auto e Moto", 1)
    d = _decide_real("sintetico/auto_moto_mesmo_beneficio", T.SAMUEL, ML, 30,
                     [(1, "fadadoscupons/17767", 0, {T.FADA})], 1)
    r.check((d.acao, d.motivo) == ("NOVA", "nome_conflitante"), "23.moda_x_auto_moto_conflito",
            (d.acao, d.motivo))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CUPOM SEM CÓDIGO · correções da sombra de 14/10"))
