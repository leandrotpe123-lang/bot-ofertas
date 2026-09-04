# Execuções do teste createLink

Registro operacional. Cada linha é uma execução real na Railway.

| # | Quando | Resultado | Causa |
|---|---|---|---|
| 1 | 02:07 | REPROVADO | cURL de outra requisição: 1/10 cookies, sem CSRF, quebras de linha no header |
| 2 | 02:58 | REPROVADO | valor de `ML_TEST_CURL` não começava com `curl` |
| 3 | 03:16 | **APROVADO** | — |

## Execução 3 — resultado

```
HTTP: 200
total_success: 1 | total_error: 0
Estrutura da resposta: urls[0] (objeto)
created: True
tag validada: SIM
short_url: https://meli.la/2B59gJm
FORMATO: meli.la
long_url presente: SIM (336 chars)
TESTE APROVADO
```

Entrada: URL longa de produto (`/p/MLB16016316`), tag
`leoofertas8270`, sessão com 23 cookies (8 dos 10 da lista do
Guilherme; ausentes `x-meli-session-id` e `x-bf-session-v6`).

## Fatos estabelecidos

1. **O endpoint funciona por replay de cookie.** POST em
   `/affiliate-program/api/v2/affiliates/createLink` com header
   `Cookie` + `x-csrf-token` + User-Agent de navegador devolve 200.
2. **O body mínimo basta.** `{"urls": [...], "tag": "..."}` —
   sem `itemId`, sem `itemAddToList`, sem `type`.
3. **A resposta prova propriedade.** O campo `tag` volta no corpo e
   bate com a tag enviada. É a validação anti-vazamento objetiva
   que o caminho por navegador não oferecia.
4. **A estrutura é `urls[0]` como objeto**, com `short_url`,
   `created`, `tag` e `long_url` em snake_case.
5. **O formato de saída é `meli.la`**, não `mercadolivre.com/sec/`,
   ao menos para URL de produto com esta conta. A hipótese de que
   `/sec/` fosse o formato atual **não se confirmou** neste cenário.
6. **A lista de 10 cookies não é toda obrigatória.** Passou com 8;
   `x-meli-session-id` e `x-bf-session-v6` ausentes. O conjunto
   mínimo real ainda é desconhecido — a requisição envia o Cookie
   inteiro (23 cookies), então nada aqui prova suficiência.

## Perguntas ainda abertas

- URL de **lista/campanha** (`/social/.../lists/...`) devolve
  `meli.la` ou `/sec/`? O tráfego real do bot é majoritariamente
  lista de cupom, e este teste cobriu apenas produto.
- Qual o conjunto mínimo de cookies? (descoberta por eliminação)
- Quanto tempo a sessão sobrevive?

## Aprendizados de operação

- `redeploy` reaproveita o snapshot de configuração do build
  anterior. Trocar o start command exige um deploy **novo** (push).
- O worker é único: enquanto o teste roda, o bot fica fora do ar
  por cerca de um minuto. Nada se perde — as mensagens permanecem
  no Telegram e são lidas na volta.
- Restaurar sempre para `python main.py` logo após ler o log.
- `ML_TEST_CURL` é credencial de sessão: apagar após o teste.
