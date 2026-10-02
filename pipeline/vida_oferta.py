# pipeline/vida_oferta.py — AUTORIDADE ÚNICA DA VIDA OPERACIONAL DA OFERTA
# Doutrina (ratificada): toda oferta publicada vive UM ciclo de 1 hora.
# Dentro dele, toda ocorrência converge para o mesmo post; fora dele, nasce
# um novo ciclo. Este módulo LEGISLA o tempo; nunca executa, nunca guarda
# estado — e responde UMA pergunta: o ciclo está aberto? Por doutrina,
# "esta ocorrência pertence ao ciclo" é a MESMA pergunta (família ≡ vida).
#
# 02/10: 25 min → 1 h (decisão do Léo, observando em tempo real): as
# fontes seguem editando a oferta — cupons novos na lista do Promotom —
# depois dos 25 min, e essas edições eram descartadas (EDIT_ANTIGO) com o
# post já congelado. Vale para quem nasce depois do deploy: a janela_fim
# é estampada no nascimento.

VIDA_OFERTA_S = 60 * 60          # 3600 s — a única janela de vida


def estampar(agora: float) -> float:
    """Nascimento de um ciclo → devolve o fim da vida (janela_fim)."""
    return agora + VIDA_OFERTA_S


def viva(fim: float, agora: float) -> bool:
    """O ciclo ainda está aberto? (fronteira exclusiva: agora == fim → morto)"""
    return agora < fim
