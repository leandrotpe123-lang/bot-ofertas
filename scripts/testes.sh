#!/usr/bin/env bash
# Suíte oficial do FOGUETÃO — o MESMO comando roda no CI e localmente:
#
#     bash scripts/testes.sh
#
# Cada tests/test_*.py é um programa standalone (runner próprio em
# tests/_harness_e5.py; quatro arquivos têm main() equivalente e
# test_uma_por_vez.py termina em exceção quando falha). O critério de
# sucesso é UM só: código de saída 0 em TODOS os arquivos.
# pytest NÃO é critério: os testes recebem `r` do harness, não fixture, e o
# pytest acusa erros que não existem.
#
# Travas do gate (mudar qualquer uma é mudança de governança, revisada à
# parte — ver CLAUDE.md):
#   · MIN_ARQUIVOS — a suíte não encolhe em silêncio;
#   · arquivo que roda ZERO testes conta como falha;
#   · TIMEOUT_S por arquivo — teste travado não prende o CI.
#
# A suíte nunca encosta em produção: as variáveis de configuração do bot
# são removidas do ambiente antes de rodar, então ela se comporta igual no
# CI, no notebook e num shell que por acaso tenha variáveis reais.

set -uo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$RAIZ" || exit 2

PYTHON="${PYTHON:-python3}"
MIN_ARQUIVOS=28   # tamanho da suíte em 05/10/2026 — só sobe
TIMEOUT_S=600     # por arquivo; medido em 05/10/2026: suíte ~1 min, arquivo mais lento ~25 s

# Configuração de produção lida pelo bot: fora do ambiente dos testes.
for variavel in \
    TELEGRAM_SESSION API_ID API_HASH DB_PATH CANAL_CUPONS \
    HEARTBEAT_PING_S SONDA_ENTREGA SHP_PERF REGISTRY_ENV PORT \
    RAILWAY_PUBLIC_DOMAIN RAILWAY_VOLUME_MOUNT_PATH \
    AMAZON_COOKIE AMAZON_STORE_ID AMAZON_TAG \
    MAGALU_PARTNER_ID MAGALU_PID MAGALU_PROMOTER_ID MAGALU_SLUG \
    ML_SESSION_COOKIE ML_CSRF_TOKEN ML_CSRF ML_SEC_PROPRIO ML_TAG \
    ML_AVISO_EXPIRACAO_DIAS ML_DESCOBERTA_TETO_KB ML_DESCOBERTA_TIMEOUT_S \
    ML_LIMITE_FALHAS ML_PAUSA_DISJUNTOR_S ML_SOCIAL_URLS ML_TIMEOUT_S \
    ML_USER_AGENT SHOPEE_APP_ID SHOPEE_SECRET; do
    unset "$variavel"
done

no_ci() { [[ "${GITHUB_ACTIONS:-}" == "true" ]]; }

if command -v timeout >/dev/null 2>&1; then
    com_timeout() { timeout --kill-after=15 "$TIMEOUT_S" "$@"; }
else
    echo "aviso: comando 'timeout' indisponível — rodando sem limite por arquivo" >&2
    com_timeout() { "$@"; }
fi

shopt -s nullglob
arquivos=(tests/test_*.py)
shopt -u nullglob

echo "Python: $("$PYTHON" --version 2>&1) | arquivos: ${#arquivos[@]} (mínimo ${MIN_ARQUIVOS})"

if (( ${#arquivos[@]} < MIN_ARQUIVOS )); then
    echo "FALHA: ${#arquivos[@]} arquivos tests/test_*.py, abaixo do mínimo ${MIN_ARQUIVOS}." >&2
    echo "A suíte não pode encolher sem revisão: ajuste MIN_ARQUIVOS em PR próprio." >&2
    exit 1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

falhas=()
linhas=()
for arquivo in "${arquivos[@]}"; do
    no_ci && echo "::group::${arquivo}"
    inicio=$SECONDS
    com_timeout "$PYTHON" "$arquivo" 2>&1 | tee "$tmp/saida.log"
    rc=${PIPESTATUS[0]}
    duracao=$(( SECONDS - inicio ))
    no_ci && echo "::endgroup::"

    motivo=""
    if (( rc == 124 || rc == 137 )); then
        motivo="timeout (${TIMEOUT_S}s)"
    elif (( rc != 0 )); then
        motivo="código de saída ${rc}"
    elif grep -Eq 'TESTES: 0/|^0 testes passaram' "$tmp/saida.log"; then
        motivo="nenhum teste executado"
    fi

    if [[ -n "$motivo" ]]; then
        falhas+=("${arquivo} — ${motivo}")
        linhas+=("| \`${arquivo}\` | FALHOU: ${motivo} | ${duracao}s |")
        no_ci && echo "::error file=${arquivo}::${motivo}"
    else
        linhas+=("| \`${arquivo}\` | ok | ${duracao}s |")
    fi
done

echo
echo "================================================================"
echo "SUÍTE: $(( ${#arquivos[@]} - ${#falhas[@]} ))/${#arquivos[@]} arquivos verdes"
for falha in ${falhas[@]+"${falhas[@]}"}; do
    echo "  FALHOU  ${falha}"
done
echo "================================================================"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
        echo "### Suíte standalone: $(( ${#arquivos[@]} - ${#falhas[@]} ))/${#arquivos[@]} arquivos verdes"
        echo
        echo "| arquivo | resultado | tempo |"
        echo "|---|---|---|"
        printf '%s\n' "${linhas[@]}"
    } >> "$GITHUB_STEP_SUMMARY"
fi

(( ${#falhas[@]} == 0 ))
