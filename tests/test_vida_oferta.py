"""V1 — contrato mínimo da autoridade da vida operacional (puro)."""
from pipeline.vida_oferta import VIDA_OFERTA_S, estampar, viva


def test_estampa_e_vida():
    t0 = 1_000_000.0
    fim = estampar(t0)
    assert fim == t0 + VIDA_OFERTA_S == t0 + 1500.0
    assert viva(fim, t0) and viva(fim, t0 + VIDA_OFERTA_S - 1)
    assert not viva(fim, t0 + VIDA_OFERTA_S)   # fronteira exclusiva == decisao:57
