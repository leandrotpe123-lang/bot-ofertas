# Execução 6 — LISTA APROVADA

## Resultado

```
HTTP: 200
total_success = 1 | total_error = 0
created       = True
short_url     = https://meli.la/2TzpFAP
tag           = leoofertas8270
matt_word na long_url = leoofertas8270 (NOSSA)
```

Entrada:
`https://lista.mercadolivre.com.br/_Container_promotions-77-full?coupon_campaign_id=14194174#tracking_id=...&source=affiliate-profile`

## A distinção que resolveu o caso

| URL | O que é | createLink |
|---|---|---|
| `mercadolivre.com.br/social/<slug>` | vitrine de um afiliado | **recusa** — error_code 111 |
| `lista.mercadolivre.com.br/...` | listagem do próprio Mercado Livre | **aceita** |

A vitrine `/social/` pertence a um afiliado; o programa não afilia
a loja de outra pessoa. A listagem é conteúdo do Mercado Livre e é
elegível.

## Campos novos revelados pela resposta de sucesso

A resposta de lista traz mais campos que a de produto:

| Campo | Valor observado | Uso |
|---|---|---|
| `created` | `true` | sucesso do item |
| `id` | `2TzpFAP` | sufixo do meli.la |
| `short_url` | `https://meli.la/2TzpFAP` | **o que publicamos** |
| `long_url` | `/social/leoofertas8270?matt_word=leoofertas8270&…` | forma canônica |
| `origin_url` | a URL enviada, **sem o fragmento** | casamento pedido↔resposta |
| `tag` | `leoofertas8270` | validação de propriedade |
| `type_url` | `SOCIAL_PROFILE_ENCRYPTED` | tipo detectado pelo servidor |
| `regex` | `X9RFM6-C8ZX` | código de busca no app |
| `text` | texto pronto de divulgação | opcional |
| `generated_date` | `2026-09-04T04:10:01Z` | idade do link |

### O ciclo se fecha

A `long_url` devolvida é `/social/leoofertas8270?...&ref=BPRvHr…` —
ou seja, **o servidor converte a listagem na NOSSA vitrine
criptografada**, com a nossa identidade. É exatamente o formato que
foi recusado quando a vitrine era de terceiro.

Isso confirma o modelo: a URL de entrada tem que ser conteúdo do
Mercado Livre; a vitrine é o **produto** da afiliação, nunca a
entrada.

### O fragmento é descartado

Enviamos `#tracking_id=…&source=affiliate-profile`; o `origin_url`
devolvido não o contém. O servidor ignora o fragmento — não
atrapalha, e não precisa ser preservado.

### `type_url` é a classificação do servidor

`SOCIAL_PROFILE_ENCRYPTED` para lista. Vale registrar qual valor
aparece para produto na próxima captura, para sabermos se serve
como sinal de cenário confiável.

## Fatos consolidados

1. Endpoint funciona por replay de cookie: `Cookie` +
   `x-csrf-token` + User-Agent de navegador.
2. Body mínimo basta em ambos os cenários:
   `{"urls": [...], "tag": "..."}`.
3. **Produto** (`/p/MLB…`) → aprovado, creditado (confirmado em
   produção pelo operador).
4. **Listagem** (`lista.mercadolivre.com.br`) → aprovado, com
   `matt_word` próprio na `long_url`.
5. **Vitrine de terceiro** (`/social/<outro>`) → recusada,
   `error_code 111`.
6. Erro é por item dentro de HTTP 200: checar `total_success` e
   `urls[0].error_code`, nunca só o status.
7. Formato de saída é `meli.la`.
8. `long_url` volta na resposta — forma canônica sem request extra.
9. Fragmento (`#…`) é ignorado pelo servidor.

## Pergunta em aberto — a que decide o valor real

Os `meli.la` que chegam dos grupos monitorados expandem para qual
formato?

- se expandem para `lista.mercadolivre.com.br/...` → afiliáveis
- se expandem para `/social/<terceiro>` → recusados

É o que define a cobertura efetiva do Mercado Livre no pipeline.
Descobre-se expandindo alguns `meli.la` reais dos grupos.
