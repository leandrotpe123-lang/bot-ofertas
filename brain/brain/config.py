"""Constantes do Brain v0 — de CÓDIGO, de propósito: mudar qualquer uma é PR
aprovado pelo dono. Sem I/O no import (mesma regra do config.py do worker).

O modo NÃO vem de variável de ambiente: ACAO só entra por PR que troca esta
linha, depois do portão de teste e da sombra (decisão do dono, 14/10/2026)."""

BRAIN_MODO = "SOMBRA"                     # SOMBRA | ALERTA | ACAO
MODOS = ("SOMBRA", "ALERTA", "ACAO")

# ── Juiz (modelos do teste: decisão do dono, 14/10/2026) ─────────────
MODELO_PRINCIPAL = ("claude-haiku-5-5", "high")
MODELO_REFERENCIA = ("claude-sonnet-5-5", "high")
MODELO_TETO = ("claude-opus-5-5", "high")
EFFORTS = ("low", "medium", "high")

# Configurações (modelo, effort) aprovadas pelo portão de teste. Vazio =
# nenhum veredito vira ação ou alerta, só registro. Entra aqui por PR, com o
# resultado da bancada (brain/teste) no corpo.
CONFIGS_VALIDADAS: frozenset = frozenset()

# Cascata: o principal julga tudo; a referência só quando o principal dá
# INCERTO, recusa ou certeza abaixo deste limiar.
CASCATA_CERTEZA_MIN = 0.80

# ── Credencial e endereço da API (sempre EXPLÍCITOS) ─────────────────
# O SDK, sozinho, lê ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN,
# ANTHROPIC_BASE_URL e ANTHROPIC_CUSTOM_HEADERS do ambiente. No ambiente do
# Claude Code essas variáveis pertencem ao próprio Claude Code, e
# ANTHROPIC_BASE_URL está definida. Por isso o Brain nunca deixa o SDK
# descobrir nada: lê a chave pelo nome abaixo, passa explícita, fixa o
# endereço e monta o cliente sem nenhuma ANTHROPIC_* no ambiente
# (brain/brain/cliente.py).
VAR_CHAVE_PRODUCAO = "ANTHROPIC_API_KEY"  # serviço brain na Railway
VAR_CHAVE_TESTE = "BRAIN_TESTE_CHAVE"     # bancada (sessão de teste do Claude Code)
API_BASE_URL = "https://api.anthropic.com"

# ── Resiliência da chamada ────────────────────────────────────────────
TIMEOUT_S = 30.0
TENTATIVAS = 3                            # 429 / 529 / 5xx / rede
BACKOFF_BASE_S = 2.0
BACKOFF_MAX_S = 30.0
DISJUNTOR_FALHAS = 5                      # falhas seguidas → abre
DISJUNTOR_PAUSA_S = 300.0                 # aberto por 5 min, depois meia-abertura
TETO_CHAMADAS_DIA = 400                   # medido: ~10–50/dia normal, ~110 no 10.10
TETO_USD_DIA = 1.00
MAX_TOKENS = 8000                         # pensamento conta no max_tokens

# ── Política ──────────────────────────────────────────────────────────
JANELA_S = 3600                           # vida da oferta no worker (VIDA_OFERTA_S)
CERTEZA_MIN_ALERTA = 0.70
CERTEZA_MIN_ACAO = 0.90
LEMBRETE_MIN_S = 5 * 60                   # lembrete minutos depois = alerta legítimo
ACOES_POR_HORA = 6
ALERTAS_POR_HORA = 12
