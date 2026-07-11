# test_assunto.py — SUÍTE DO CLASSIFICADOR DE ASSUNTO
#
# pipeline.assunto é o coração da classificação do sistema. Esta suíte
# protege o CONTRATO PÚBLICO e congela o comportamento atual (C1).
#
# ATENÇÃO — leia antes de "consertar" um teste:
#   Os testes marcados FALSO_POSITIVO_C1 documentam DEFEITOS CONHECIDOS
#   do detector atual. Eles passam hoje porque o classificador AINDA NÃO
#   tem autoridade sobre a família (ancoras() ignora o resultado quando
#   há produto). São a linha de base do C2 (endurecimento): quando o C2
#   corrigir o detector, estes testes DEVEM mudar de propósito — e essa
#   mudança é a evidência de que o C2 funcionou.
#
# Rodar:  python3 -m pytest test_assunto.py -v
#     ou: python3 test_assunto.py

import pytest

from pipeline.assunto import (
    buscar_calendario_comercial,
    eh_lista_cupons,
    eh_post_cashback,
    eh_post_cupom,
    eh_post_evento,
    extrair_pct_cashback,
)


# ═════════════════════════════════════════════════════════════════
# eh_post_cupom — o assunto é um CUPOM?
# ═════════════════════════════════════════════════════════════════
class TestEhPostCupom:

    # ── Caso (a): palavra cupom/código no TÍTULO ──
    @pytest.mark.parametrize("titulo", [
        "CUPOM Amazon 20% OFF",
        "Cupom R$50 OFF na Amazon",
        "Novo codigo Magalu liberado",
        "CUPONS Shopee ativos",
        "Código exclusivo hoje",
    ])
    def test_titulo_anuncia_cupom(self, titulo):
        assert eh_post_cupom(titulo) is True

    # ── Caso (b): título já é "OFF: CODIGO" ──
    def test_titulo_formato_off_codigo(self):
        assert eh_post_cupom("R$ 100 OFF em R$ 900: INFLU100") is True
        assert eh_post_cupom("20% OFF: PROMO20") is True

    # ── Caso (c): lista de cupons (2+ linhas) ──
    def test_lista_de_cupons(self):
        texto = ("Promo do dia\n"
                 "R$ 50 OFF em R$ 300: AAA10\n"
                 "R$ 100 OFF em R$ 900: BBB20")
        assert eh_post_cupom(texto) is True

    # ── Casos (d)/(e): cashback com código (cards Shopee) ──
    def test_cashback_com_codigo(self):
        texto = "Ofertas Insanas\n50% Cashback ate R$30: BRUIANHEZ10"
        assert eh_post_cupom(texto) is True

    # ── Negativos: produto puro ──
    @pytest.mark.parametrize("texto", [
        "Air Fryer Mondial 4L por R$299",
        "Smart TV 50 4K por R$1.799",
        "Echo Dot 5a geracao\nDe R$399 por R$249",
        "Ofertas Insanas",
        "",
    ])
    def test_produto_puro_nao_e_cupom(self, texto):
        assert eh_post_cupom(texto) is False

    # ── DEFEITO CONHECIDO (linha de base do C2) ──
    # O detector dispara com QUALQUER menção de "cupom" no título, sem
    # distinguir o cupom como SUJEITO ("CUPOM Amazon 20% OFF") do cupom
    # como COMPLEMENTO ("Fone JBL R$199 (cupom: DESC10)" — o sujeito é
    # o produto). Hoje é inócuo: ancoras() ignora o resultado quando há
    # produto. Se o C3 desse autoridade sem o C2, dois produtos distintos
    # com o mesmo código colapsariam na mesma família.
    @pytest.mark.parametrize("texto", [
        "Fone JBL R$199 (cupom: DESC10)",
        "Echo Dot R$249 - use o cupom ECHO10",
    ])
    def test_FALSO_POSITIVO_C1_cupom_como_complemento(self, texto):
        # Congela o comportamento ATUAL. O C2 deve inverter para False.
        assert eh_post_cupom(texto) is True, (
            "Defeito conhecido: cupom como complemento e' lido como assunto"
        )


# ═════════════════════════════════════════════════════════════════
# eh_lista_cupons — 2+ linhas no formato de lista
# ═════════════════════════════════════════════════════════════════
class TestEhListaCupons:

    def test_duas_linhas_e_lista(self):
        texto = ("R$ 50 OFF em R$ 300: AAA10\n"
                 "R$ 100 OFF em R$ 900: BBB20")
        assert eh_lista_cupons(texto) is True

    def test_uma_linha_nao_e_lista(self):
        assert eh_lista_cupons("R$ 50 OFF em R$ 300: AAA10") is False

    def test_texto_sem_formato_nao_e_lista(self):
        assert eh_lista_cupons("cupom: ABC10\ncupom: DEF20") is False

    def test_vazio(self):
        assert eh_lista_cupons("") is False


# ═════════════════════════════════════════════════════════════════
# eh_post_cashback — assunto é CASHBACK (sem cupom code)
# ═════════════════════════════════════════════════════════════════
class TestEhPostCashback:

    def test_titulo_cashback_com_sinal(self):
        assert eh_post_cashback("30% Cashback hoje", True) is True

    def test_sem_sinal_e_sem_titulo(self):
        assert eh_post_cashback("Air Fryer 4L R$299", False) is False

    def test_flag_e_insumo_da_normalizacao(self):
        # A flag vem de norm.tem_sinal_cashback — o modulo NAO a deriva.
        texto = "Oferta do dia"
        assert eh_post_cashback(texto, False) is False


# ═════════════════════════════════════════════════════════════════
# eh_post_evento — assunto é CAMPANHA/EVENTO
# ═════════════════════════════════════════════════════════════════
class TestEhPostEvento:

    def test_calendario_comercial_no_titulo(self):
        assert eh_post_evento("Black Friday chegou", False) is True
        assert eh_post_evento("Esquenta Shopee", False) is True

    def test_host_de_campanha_sozinho_basta(self):
        # Sinal derivado pela normalizacao (URL de campanha).
        assert eh_post_evento("Oferta qualquer", True) is True

    def test_produto_puro_sem_host(self):
        assert eh_post_evento("Air Fryer 4L R$299", False) is False


# ═════════════════════════════════════════════════════════════════
# extrair_pct_cashback — percentual nas 5 primeiras linhas
# ═════════════════════════════════════════════════════════════════
class TestExtrairPctCashback:

    def test_extrai_percentual(self):
        assert extrair_pct_cashback("cashback 30%") == "30"

    def test_sem_percentual(self):
        assert extrair_pct_cashback("Air Fryer R$299") == ""

    def test_so_primeiras_5_linhas(self):
        texto = "a\nb\nc\nd\ne\n50%"   # 50% esta na 6a linha
        assert extrair_pct_cashback(texto) == ""

    def test_vazio(self):
        assert extrair_pct_cashback("") == ""


# ═════════════════════════════════════════════════════════════════
# buscar_calendario_comercial — devolve MATCH (posição importa)
# ═════════════════════════════════════════════════════════════════
class TestBuscarCalendarioComercial:

    def test_devolve_match_com_posicao(self):
        # _id_campanha usa .start() para ordenar candidatos: o contrato
        # e' o MATCH, nao um bool — a regex nunca sai do modulo.
        m = buscar_calendario_comercial("Chegou a Black Friday")
        assert m is not None
        assert m.group(0).lower() == "black friday"
        assert m.start() > 0

    def test_sem_calendario(self):
        assert buscar_calendario_comercial("Air Fryer R$299") is None

    def test_escopo_200_chars(self):
        texto = "x" * 250 + " black friday"
        assert buscar_calendario_comercial(texto) is None


# ═════════════════════════════════════════════════════════════════
# PUREZA — o contrato estrutural do modulo
# ═════════════════════════════════════════════════════════════════
class TestPureza:

    def test_nao_muta_a_entrada(self):
        texto = "CUPOM Amazon 20% OFF"
        copia = texto
        eh_post_cupom(texto)
        eh_post_cashback(texto, True)
        eh_post_evento(texto, True)
        extrair_pct_cashback(texto)
        assert texto == copia

    def test_deterministico(self):
        texto = "CUPOM Amazon 20% OFF\ncupom: AMZ20"
        assert [eh_post_cupom(texto) for _ in range(5)] == [True] * 5

    def test_modulo_nao_exporta_regex(self):
        # Fronteira: nenhuma regex publica. Consumidores usam funcoes.
        import pipeline.assunto as a
        publicos = [n for n in dir(a) if not n.startswith("_")]
        assert not [n for n in publicos if n.startswith("RE_")]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))

