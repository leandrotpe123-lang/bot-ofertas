# Execuções do teste createLink

Registro operacional. Cada linha é uma execução real na Railway.

| # | Quando | Resultado | Causa |
|---|---|---|---|
| 1 | 02:07 | REPROVADO | cURL de outra requisição: 1/10 cookies, sem CSRF, quebras de linha no header |
| 2 | 02:58 | REPROVADO | valor de `ML_TEST_CURL` não começava com `curl` |
| 3 | 03:00 | em execução | — |

## Aprendizados de operação

- `redeploy` reaproveita o snapshot de configuração do build
  anterior. Trocar o start command exige um deploy **novo** (push).
- O worker é único: enquanto o teste roda, o bot fica fora do ar
  por cerca de um minuto. Nada se perde — as mensagens permanecem
  no Telegram e são lidas na volta.
- Restaurar sempre para `python main.py` logo após ler o log.
