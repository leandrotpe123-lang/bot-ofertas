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
    beneficio_do_cupom,
    buscar_calendario_comercial,
    eh_lista_cupons,
    eh_post_cashback,
    eh_post_cupom,
    eh_post_evento,
    extrair_pct_cashback,
)


# ═════════════════════════════════════════════════════════════════
# beneficio_do_cupom — IDENTIDADE do cupom SEM código (C3.1)
# ═════════════════════════════════════════════════════════════════
class TestBeneficioDoCupom:
    """O descritor substitui o hash do texto como identidade.

    MOTIVO (bug real corrigido): _alma() normaliza percentuais e valores
    ('20%' -> PCT, 'R$30' -> VALOR). Logo, por hash, "Cupom 20% OFF" e
    "Cupom 15% OFF" produziam a MESMA alma e COLAPSAVAM numa familia so —
    uma oferta legitima era engolida como duplicata.
    """

    # ── percentual (1..100; regex aceita 3 digitos) ──
    @pytest.mark.parametrize("texto,esperado", [
        ("Cupom 20% OFF Mercado Livre", "pct:20"),
        ("Cupom 50% OFF Shopee", "pct:50"),
        ("Cupom 100% OFF", "pct:100"),      # 3 digitos legitimo
        ("Cupom 5% OFF", "pct:5"),
        ("Cupom 1% OFF", "pct:1"),
    ])
    def test_percentual_valido(self, texto, esperado):
        assert beneficio_do_cupom(texto) == esperado

    # ── guarda de intervalo: lixo NAO vira pct ──
    @pytest.mark.parametrize("texto", [
        "Cupom 999% OFF",
        "Cupom 0% OFF",
        "Cupom 150% OFF",
    ])
    def test_percentual_invalido_e_descartado(self, texto):
        assert beneficio_do_cupom(texto) == "geral"

    def test_lixo_nao_derruba_percentual_legitimo(self):
        # A varredura CONTINUA: 999% e' descartado, 30% e' capturado.
        assert beneficio_do_cupom("Ate 999%... Cupom 30% OFF") == "pct:30"

    def test_lixo_nao_derruba_outro_sinal(self):
        assert beneficio_do_cupom("Cupom 999% frete gratis") == "frete"

    # ── valor ──
    def test_valor(self):
        assert beneficio_do_cupom("Cupom de R$50 Mercado Livre") == "vlr:50"

    def test_valor_com_condicao_de_minimo(self):
        # "acima de R$150" e' CONDICAO, nao beneficio. O beneficio e' R$30.
        assert beneficio_do_cupom("Cupom R$30 OFF acima de R$150") == "vlr:30"

    def test_percentual_vence_valor_de_condicao(self):
        # Havendo percentual, o R$ no texto e' condicao — nao entra.
        assert beneficio_do_cupom(
            "Cupom 20% OFF em compras acima de R$100") == "pct:20"

    # ── frete e primeira compra ──
    def test_frete(self):
        assert beneficio_do_cupom("Cupom de frete gratis") == "frete"

    def test_primeira_compra(self):
        assert beneficio_do_cupom("Cupom novos usuarios") == "1acompra"

    # ── COMPOSICAO (ordem canonica fixa) ──
    @pytest.mark.parametrize("texto,esperado", [
        ("Cupom 15% + frete gratis", "pct:15+frete"),
        ("Cupom de R$30 na primeira compra", "vlr:30+1acompra"),
        ("Cupom 100% OFF (frete)", "pct:100+frete"),
    ])
    def test_descritor_composto(self, texto, esperado):
        assert beneficio_do_cupom(texto) == esperado

    # ── bucket geral (conservador, consciente) ──
    @pytest.mark.parametrize("texto", [
        "🔥 Novo Cupom Shopee",
        "Resgate seu cupom exclusivo",
        "Cupom liberado hoje as 20h",
        "Cupom disponivel no app",
        "Cupom para usuarios selecionados",
    ])
    def test_bucket_geral(self, texto):
        assert beneficio_do_cupom(texto) == "geral"

    # ── ESTABILIDADE (a propriedade que o hash nao tinha) ──
    def test_ESTAVEL_mesma_oferta_textos_diferentes(self):
        # A mesma oferta descrita de 3 formas → MESMO descritor.
        a = beneficio_do_cupom("🔥 Cupom 20% OFF Mercado Livre\nResgate as 19h")
        b = beneficio_do_cupom("Novo Cupom Mercado Livre 20% OFF\nUse amanha")
        c = beneficio_do_cupom("CUPOM ML 20% — corre!")
        assert a == b == c == "pct:20"

    def test_CRITICO_beneficios_distintos_nao_colapsam(self):
        # Por hash, "20%" e "15%" colapsavam (ambos viram PCT em _alma).
        assert beneficio_do_cupom("Cupom 20% OFF") != \
               beneficio_do_cupom("Cupom 15% OFF")
        assert beneficio_do_cupom("Cupom de R$30") != \
               beneficio_do_cupom("Cupom de R$50")

    def test_deterministico(self):
        t = "Cupom 15% + frete gratis"
        assert len({beneficio_do_cupom(t) for _ in range(5)}) == 1


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

    # ── C2: CORRIGIDO — cupom como COMPLEMENTO não é assunto ──
    # HISTÓRICO (decisão arquitetural deliberada, não regressão):
    #   No C1 estes casos retornavam True. Era um DEFEITO conhecido e
    #   documentado (os antigos testes FALSO_POSITIVO_C1): o detector
    #   disparava com qualquer menção de "cupom" no título, sem
    #   distinguir o cupom como SUJEITO do cupom como COMPLEMENTO.
    #   Era inócuo enquanto o classificador não tinha autoridade sobre a
    #   família — mas teria colapsado dois produtos distintos com o mesmo
    #   código numa única oferta assim que o C3 desse essa autoridade.
    #   O C2 endureceu o caso (a) com análise sintática do título
    #   (_cupom_e_sujeito): cupom precedido de conectivo/pontuação, ou
    #   com preço aparecendo ANTES dele, é adorno de um produto.
    #   A inversão True → False abaixo é o OBJETIVO do C2 cumprido.
    @pytest.mark.parametrize("texto", [
        "Fone JBL R$199 (cupom: DESC10)",       # parênteses
        "Echo Dot R$249 - use o cupom ECHO10",  # conectivo "use o"
        "Echo Dot por R$ 249 (cupom ECHO10)",
        "Air Fryer 4L R$299 cupom AIR20",       # preço antes do cupom
        "Smart TV 4K R$1.799 (codigo TV10)",
        "Notebook Dell no cupom NOTE10",        # conectivo "no"
        "Air Fryer, cupom AIR20",               # vírgula
        "Monitor LG R$699 [codigo LG24]",       # colchete
    ])
    def test_C2_cupom_como_complemento_e_PRODUTO(self, texto):
        assert eh_post_cupom(texto) is False, (
            "cupom como complemento de um produto NAO e' assunto de cupom"
        )

    # ── C2: o cupom como SUJEITO continua sendo CUPOM ──
    # Guarda contra o endurecimento ter ido longe demais. Note o caso
    # "Cupom R$50 OFF": o preço vem DEPOIS da palavra — é o valor do
    # desconto, não o preço de um produto. Por isso a regra compara
    # POSIÇÕES, e não a mera presença de "R$".
    @pytest.mark.parametrize("titulo", [
        "CUPOM Amazon 20% OFF",
        "Cupom R$50 OFF na Amazon",
        "Cupom de R$ 30 na primeira compra",
        "ULTIMAS HORAS: cupom AMZ20",
        "Novo codigo Amazon liberado",
    ])
    def test_C2_cupom_como_sujeito_continua_CUPOM(self, titulo):
        assert eh_post_cupom(titulo) is True


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
    
