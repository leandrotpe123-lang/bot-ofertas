# Execução 5 — CAUSA DA RECUSA ENCONTRADA

```
error_code = 111
message    = URL not allowed in affiliates program
status     = 200
```

Envelope: `status: 200`, `total_items: 1`, `total_success: 0`,
`total_error: 1`.

URL enviada: `/social/fadadoscupons` com `matt_word=fadadoscupons`,
`matt_tool=59965296`, `forceInApp=true`, `ref=BC78em…` (699 chars).

## Leitura

`URL not allowed in affiliates program` é recusa **da URL**, não da
sessão, não do body, não de autenticação. A mesma sessão, no mesmo
minuto, gerou link de produto com sucesso (execução 3). Portanto:

- sessão válida ✔
- CSRF válido ✔
- body no formato certo ✔
- **a URL é que não é elegível** ✘

O erro é por item, dentro de HTTP 200 — o envelope diz 200 mesmo
com falha total. Qualquer cliente definitivo precisa checar
`total_success` e `urls[0].error_code`, nunca só o status HTTP.

## A chamada B não pôde ser feita

O cURL capturado **não contém `--data-raw`**. Ele traz 24 headers
de navegador (`sec-ch-ua`, `device-memory`, `downlink`, `dpr`,
`traceparent`, `viewport-width`, `x-client-data`…) mas nenhum corpo.

Isso significa que a captura **não é de um POST createLink** — é de
uma requisição de página. Ao abrir a lista no navegador, nenhum
`createLink` foi disparado.

Esse fato, por si só, é informativo: **a ferramenta oficial não
gerou link para essa lista**. Não há requisição a copiar porque a
geração não aconteceu.

## Hipóteses a separar

1. **Vitrine de terceiro não é elegível.** `/social/<outro>` é a
   loja de outro afiliado; o programa recusaria qualquer URL assim,
   com ou sem parâmetros.
2. **Os parâmetros de terceiro é que barram.** Sanitizar
   (remover `matt_word`/`matt_tool`, preservar `ref`) tornaria a
   URL elegível.
3. **`/social/` inteiro é inelegível**, inclusive a nossa própria
   vitrine.

São distinguíveis por teste, e a ordem importa: a hipótese 2 é a
mais barata e a que mais muda a arquitetura se for verdadeira.

## Próximos testes propostos

| # | URL | Distingue |
|---|---|---|
| 5a | mesma lista **sanitizada** (sem `matt_word`/`matt_tool`, com `ref`) | hipótese 2 |
| 5b | **nossa própria** `/social/<nossa_tag>` | hipótese 3 |
| 5c | URL de produto de dentro da lista | rota alternativa |

Se 5a passar, `links.sanitizar_para_navegacao()` — que já existe e
é subtrativa — resolve o caso e a arquitetura segue como desenhada.

Se 5a e 5b falharem, listas de terceiros não são afiliáveis por
esta via, e o caminho passa a ser extrair os produtos da lista e
afiliar cada um.
