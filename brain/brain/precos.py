"""Preço por modelo (US$ por milhão de tokens) e custo de uma chamada pelo
`usage` que a API devolve. O custo do teste e da operação é MEDIDO assim,
nunca estimado depois do fato.

Fonte: tabela de modelos da skill claude-api (cache de 06/10/2026), conferida
em 14/10/2026. Haiku 5.5 tem dois preços: prompt até 100K tokens e acima.
Leitura de cache = 0,1× a entrada; escrita de cache de 5 min = 1,25×.
Pensamento é cobrado como saída (está em output_tokens)."""

PRECOS = {
    #                        entrada  saída  (US$/MTok)
    "claude-haiku-5-5": {"in": 0.10, "out": 0.50, "in_longo": 0.50,
                         "out_longo": 2.50, "limite_longo": 100_000},
    "claude-sonnet-5-5": {"in": 2.00, "out": 10.00},
    "claude-opus-5-5": {"in": 4.00, "out": 20.00},
}
FONTE = "skill claude-api, tabela de modelos (cache 2026-10-06), conferida 2026-10-14"


def _g(usage, campo):
    if usage is None:
        return 0
    v = usage.get(campo) if isinstance(usage, dict) else getattr(usage, campo, 0)
    return int(v or 0)


def custo_usd(modelo: str, usage) -> float:
    """Custo de UMA resposta. Modelo fora da tabela levanta (preço inventado
    nunca entra na conta)."""
    p = PRECOS[modelo]
    ent = _g(usage, "input_tokens")
    lida = _g(usage, "cache_read_input_tokens")
    escrita = _g(usage, "cache_creation_input_tokens")
    saida = _g(usage, "output_tokens")
    total_prompt = ent + lida + escrita
    pin, pout = p["in"], p["out"]
    if "limite_longo" in p and total_prompt > p["limite_longo"]:
        pin, pout = p["in_longo"], p["out_longo"]
    return (ent * pin + lida * pin * 0.1 + escrita * pin * 1.25
            + saida * pout) / 1_000_000


def estimar_usd(modelo: str, tokens_entrada: int, tokens_saida: int) -> float:
    return custo_usd(modelo, {"input_tokens": tokens_entrada,
                              "output_tokens": tokens_saida})
