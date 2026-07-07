# pipeline/vida_oferta.py — AUTORIDADE ÚNICA DA VIDA OPERACIONAL DA OFERTA
# Doutrina (ratificada): toda oferta publicada vive UM ciclo de 25 minutos.
# Dentro dele, toda ocorrência converge para o mesmo post; fora dele, nasce
# um novo ciclo. Invariante: existe no máximo UM ciclo vivo por oferta.
# Este módulo LEGISLA o tempo; nunca executa, nunca guarda estado.

VIDA_OFERTA_S = 25 * 60          # 1500s — a única janela de vida

def estampar(agora: float) -> float:
    """Nascimento de um ciclo → devolve o fim da vida (janela_fim)."""
    return agora + VIDA_OFERTA_S

def viva(fim: float, agora: float) -> bool:
    """O ciclo ainda está aberto?"""
    return agora < fim

def mesmo_ciclo(fim: float, agora: float) -> bool:
    """Esta ocorrência pertence ao ciclo existente? Por doutrina, a MESMA
    resposta de viva() — família ≡ vida, uma pergunta, uma resposta."""
    return viva(fim, agora)

def encerrar(agora: float) -> float:
    """Fim explícito de ciclo (ex.: reativação válida). O executor grava;
    a política apenas define o valor."""
    return agora
