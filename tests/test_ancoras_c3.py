# test_ancoras_c3.py — AUTORIDADE DO ASSUNTO SOBRE A FAMÍLIA (C3)
#
# O C3 deu ao classificador de assunto autoridade sobre ancoras(): quando
# o CUPOM é o assunto do post, a oferta É o cupom — o produto no link é
# veículo/ilustração e NÃO ancora.
#
# Esta suíte protege as DUAS invariantes que tornam o C3 seguro:
#
#   1. CONVERGÊNCIA (o objetivo): o mesmo cupom relâmpago, ilustrado com
#      produtos DIFERENTES por grupos diferentes, é UMA oferta.
#
#   2. NÃO-COLAPSO (a proteção): produtos DIFERENTES que compartilham um
#      código genérico ("CHEGOU", "DESC10") continuam em famílias
#      SEPARADAS — porque ali o cupom é COMPLEMENTO, não assunto (C2).
#
#   Se a invariante 2 quebrar, o sistema passa a engolir ofertas legítimas
#   como duplicata. É o teste mais importante do arquivo.
#
# Rodar:  python3 -m pytest test_ancoras_c3.py -v

import pytest

from pipeline.identidade_oferta import ancoras


class _Norm:
    """Dublê mínimo de MensagemNormalizada (só o que ancoras() lê)."""

    def __init__(self, plat, texto, cupons=(), ids=(), camp=(), cash=False):
        self.plat = plat
        self.texto_limpo = texto
        self.cupons = list(cupons)
        self.cupom = self.cupons[0] if self.cupons else ""
        self.ids_globais = list(ids)
        self.chaves_campanha = list(camp)
        self.tem_sinal_cashback = cash
        self.tem_host_campanha = False
        self.chave_campanha = ""
        self.mapa = {}


def _chaves(norm):
    return sorted(a.chave for a in ancoras(norm))


# ═════════════════════════════════════════════════════════════════
# INVARIANTE 1 — CONVERGÊNCIA (o objetivo do C3)
# ═════════════════════════════════════════════════════════════════
class TestCupomEhOAssunto:

    def test_cupom_com_produto_ilustrativo_ancora_no_cupom(self):
        n = _Norm("amazon", "CUPOM Amazon 20% OFF\ncupom: AMZ20",
                  cupons=["AMZ20"], ids=["B0XXX"])
        assert _chaves(n) == ["amazon|cup|AMZ20"]

    def test_mesmo_cupom_produtos_ilustrativos_distintos_MESMA_familia(self):
        # O CASO QUE MOTIVOU O C3: dois grupos anunciam o mesmo cupom
        # relâmpago, cada um ilustrando com um produto diferente.
        a = _Norm("amazon", "CUPOM Amazon 20% OFF\ncupom: AMZ20",
                  cupons=["AMZ20"], ids=["B0XXX"])
        b = _Norm("amazon", "CUPOM Amazon 20% OFF\ncupom: AMZ20",
                  cupons=["AMZ20"], ids=["B0YYY"])
        assert _chaves(a) == _chaves(b), "mesmo cupom deve ser UMA oferta"

    def test_produto_ilustrativo_NAO_ancora(self):
        # Exclusiva, não aditiva: se o produto-vitrine ancorasse, ele
        # capturaria a oferta legítima daquele produto quando chegasse.
        n = _Norm("amazon", "Cupom R$50 OFF na Amazon\nCodigo: SAVE50",
                  cupons=["SAVE50"], ids=["B0YYY"])
        assert "amazon|B0YYY" not in _chaves(n)

    def test_varios_cupons_todos_ancoram(self):
        n = _Norm("shopee", "CUPONS Shopee ativos\ncupom: A10\ncupom: B20",
                  cupons=["A10", "B20"], ids=["SP1"])
        assert _chaves(n) == ["shopee|cup|A10", "shopee|cup|B20"]


# ═════════════════════════════════════════════════════════════════
# INVARIANTE 2 — NÃO-COLAPSO (a proteção; o teste mais importante)
# ═════════════════════════════════════════════════════════════════
class TestProdutoContinuaVencendo:

    @pytest.mark.parametrize("texto,cupom,pid", [
        ("Echo Dot R$249 (cupom ECHO10)", "ECHO10", "B0ECHO"),
        ("Fone JBL R$199 (cupom: DESC10)", "DESC10", "B0JBL"),
        ("Air Fryer 4L R$299 cupom DESC10", "DESC10", "B0AIR"),
        ("Smart TV 4K R$1.799 (codigo TV10)", "TV10", "B0TV"),
    ])
    def test_cupom_complemento_ancora_no_produto(self, texto, cupom, pid):
        n = _Norm("amazon", texto, cupons=[cupom], ids=[pid])
        assert _chaves(n) == [f"amazon|{pid}"]

    def test_CRITICO_produtos_distintos_mesmo_codigo_NAO_colapsam(self):
        # O caso CHEGOU. Se o C2 falhar, estes dois colapsam numa família
        # e uma oferta legítima é engolida como duplicata.
        jbl = _Norm("amazon", "Fone JBL R$199 (cupom: DESC10)",
                    cupons=["DESC10"], ids=["B0JBL"])
        air = _Norm("amazon", "Air Fryer 4L R$299 cupom DESC10",
                    cupons=["DESC10"], ids=["B0AIR"])
        assert _chaves(jbl) != _chaves(air), (
            "produtos distintos com codigo generico NAO podem colapsar"
        )
        assert _chaves(jbl) == ["amazon|B0JBL"]
        assert _chaves(air) == ["amazon|B0AIR"]


# ═════════════════════════════════════════════════════════════════
# NÃO-REGRESSÃO — o que já funcionava continua igual
# ═════════════════════════════════════════════════════════════════
class TestSemRegressao:

    def test_produto_puro(self):
        n = _Norm("amazon", "Smart TV 50 4K R$1799", ids=["B0TV2"])
        assert _chaves(n) == ["amazon|B0TV2"]

    def test_cupom_sem_produto_inalterado(self):
        n = _Norm("shopee", "Cupons ativos:\nR$50 OFF em R$300: AAA10",
                  cupons=["AAA10"])
        assert _chaves(n) == ["shopee|cup|AAA10"]

    def test_mesmo_produto_cupons_diferentes_MESMA_familia(self):
        # Evolução por cupom novo depende disto: mesmo produto, cupom
        # diferente → mesma família → EVOLUI (nao vira post novo).
        a = _Norm("amazon", "Echo Dot R$249 (cupom ECHO10)",
                  cupons=["ECHO10"], ids=["B0ECHO"])
        b = _Norm("amazon", "Echo Dot R$239 (cupom ECHO20)",
                  cupons=["ECHO20"], ids=["B0ECHO"])
        assert _chaves(a) == _chaves(b) == ["amazon|B0ECHO"]

    def test_campanha_inalterada(self):
        n = _Norm("shopee", "Black Friday chegou", camp=["black-friday"])
        assert _chaves(n) == ["shopee|camp|black-friday"]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
      
