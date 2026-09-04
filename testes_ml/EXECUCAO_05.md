# Execução 5 — diagnóstico de lista

Rodada dedicada a descobrir por que o `createLink` recusou a URL de
lista na execução 4.

## O que esta rodada faz

Duas chamadas, mesma sessão, mesma rodada:

- **A** — nosso body mínimo: `{"urls": [ML_TEST_URL], "tag": ML_TAG}`
- **B** — o body **oficial** extraído do `--data-raw` do cURL
  capturado no DevTools

A comparação separa duas causas que, de fora, parecem a mesma:

| A | B | Conclusão |
|---|---|---|
| falha | passa | o problema é o **formato do body** |
| falha | falha | o problema é a **URL** ou a sessão — ver `error_code` |
| passa | — | nosso formato serve |

## Logs adicionados

- envelope inteiro da resposta, campo a campo
- `urls[0]` completo, com todas as chaves
- bloco **DIAGNÓSTICO DA RECUSA**: `error_code`, `message`,
  `status`, `origin_url`
- URLs decompostas em host / path / parâmetros
- body oficial do cURL, decomposto

Valores longos (como `ref`) são truncados para o log ficar legível.

## O que continua fora do log

`Cookie`, `x-csrf-token` e qualquer credencial. O body do
`createLink` é impresso porque contém apenas URLs e a tag.

## Aguardando

`error_code` e `message` — são eles que dizem se o caminho é
sanitizar a URL (remover identidade de terceiro, preservando `ref`)
ou se listas exigem outro formato de requisição.
