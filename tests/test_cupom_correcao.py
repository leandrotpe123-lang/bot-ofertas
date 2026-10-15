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


if __name__ == "__main__":
    sys.exit(rodar(globals(), "CUPOM SEM CÓDIGO · correções da sombra de 14/10"))
