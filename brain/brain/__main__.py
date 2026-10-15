"""`python -m brain` — ponto de partida do futuro serviço Brain na Railway.

FASE 0: ainda não há serviço. Se alguém criar o serviço antes do PR do
Brain SOMBRA, este comando só avisa e sai com 0 (sem loop de reinício e
sem tocar em nada): nenhuma rede, nenhuma chave, nenhum Telegram."""
import sys


def main() -> int:
    print("Brain v0 — fase 0: este serviço ainda não tem função "
          "(docs/arquitetura/brain-v0.md). Nada a fazer.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
