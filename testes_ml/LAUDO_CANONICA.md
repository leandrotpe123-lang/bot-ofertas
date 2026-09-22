# LAUDO — A canônica do Mercado Livre

Medição real. Railway, deploy `c2948e29`, `testes_ml/sonda_canonica.py`,
22/09/2026 03:52 UTC. Nenhum segredo impresso.

## O achado, em uma linha

**O `createLink` devolve sempre a NOSSA vitrine, seja qual for o
destino enviado.** A identidade do produto/lista morre aí.

## A medição

`F) RETORNADA (long_url)` foi **idêntica nos três casos**:

```
https://www.mercadolivre.com.br/social/leoofertas8270
  ?matt_word=leoofertas8270&matt_tool=78133265&forceInApp=true&ref=…
```

| # | enviado ao createLink (E) | devolvido (F) | canônica | ids_globais |
|---|---|---|---|---|
| 1 | `/p/MLB16016316` | `/social/leoofertas8270?…` | vitrine | `[]` |
| 2 | `_Container_promotions-77-full?coupon_campaign_id=14194174` | `/social/leoofertas8270?…` | vitrine | `[]` |
| 3 | `/monitor-gamer-samsung…/p/MLB51435621` | `/social/leoofertas8270?…` | vitrine | `[]` |

Em todos: `id_produto = AUSENTE`, `id_global = None`, `sku = ''`, e a
sonda respondeu **`canonica representa o DESTINO convertido? NAO`**.

O caso 3 é o mais eloquente: a descoberta funcionou perfeitamente —
`meli.la/1ipL9sf` → vitrine do `samuelf3lipe` → produto real
`MLB51435621` → enviado ao createLink. E a identidade foi jogada
fora mesmo assim.

## O que se perde

- do produto: o `MLB…`
- da lista: o `_Container_<slug>` **e** o `coupon_campaign_id`

Ou seja: o par `(container, campanha)` — a identidade que a Frente 7
tinha eleito como correta — **é destruído na conversão**, antes de
qualquer coisa chegar ao Core. Nenhuma mudança em `links.py` poderia
recuperá-lo, porque a URL que `extrai_identidade` recebe já não o
contém.

## Por que o cupom vira a identidade

Sem produto e sem campanha (o ML não declara `hosts_campanha`), a
oferta chega ao dedupe sem âncora estruturada. Sobra o código do
cupom. É a colisão `mercadolivre|cup|20BELEZA` medida em produção, e
explica a lista virar edição do produto.

## Nota de método — o controle desta sonda estava mal especificado

A sonda imprimiu `CONTROLE FALHOU`. O controle, como escrito, exigia
que o produto conhecido produzisse `id_produto` **atravessando o
fluxo** — mas é exatamente isso que está sob teste. Controle não pode
ter como premissa a conclusão.

O controle certo é do APARELHO, não do fluxo: alimentar
`derivar_produto` direto com uma URL de produto. Feito duas vezes,
fora do fluxo:

```
derivar_produto(['…/p/MLB46469571'])  -> ['MLB46469571']
derivar_produto(['…/p/MLB16016316'])  -> ['MLB16016316']
```

O aparelho funciona. Logo o `[]` das três linhas é resultado, não
defeito de medição. Fica registrado que a asserção do controle no
arquivo da sonda precisa ser trocada antes de reusá-la.

## Efeito colateral declarado

A sonda gastou 3 chamadas reais de `createLink` e gerou 3 links
nossos, com a nossa tag: `meli.la/2B59gJm`, `meli.la/1i59Jti`,
`meli.la/1y8HmSx`. Ficaram no cache de links. Inofensivo, mas é
consumo real e fica declarado.

## Correção mínima proposta — NÃO APLICADA

Guardar como `canonica` a URL que foi **enviada** ao createLink (o
destino real), em vez da `long_url` que o ML devolve.

- fica em `plataformas/mercadolivre/` (adapter), **sem tocar o Core**;
- conserta produto e lista de uma vez;
- `publicada` não muda — o post continua saindo com o `meli.la`.

Pontos que exigem decisão e medição antes de aplicar:

1. `derivar_ancora_url` também usa a canônica; mudar afeta a âncora
   de fallback.
2. A validação `atribuicao_propria` hoje roda sobre a `long_url`
   devolvida; ela precisa continuar rodando sobre a resposta do
   servidor, não sobre a URL que enviamos — senão a barreira de
   atribuição vira carimbo.
3. Para a lista, o Core continua sem lugar para o par
   `(container, campanha)`: `id_produto` numa lista diria
   `tem_produto=True` (falso), e a chave de campanha ignora a query,
   que é onde vive o `coupon_campaign_id`.

Os itens 1 e 2 são técnicos e mensuráveis. O item 3 é decisão de
arquitetura.

## Estado

Nada de produção foi alterado. `links.py` segue `b7e11ac6`. O
candidato `3b9b79b5` continua descartado. Bateria 93/93.
