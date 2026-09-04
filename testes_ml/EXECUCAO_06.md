# Execução 6 — a descoberta do formato certo de lista

## A distinção que faltava

O operador identificou que existem **dois tipos de URL** que
parecem "lista" mas são coisas diferentes:

| URL | O que é | Afiliável |
|---|---|---|
| `mercadolivre.com.br/social/<slug>` | vitrine/perfil de um afiliado | **NÃO** (error_code 111) |
| `lista.mercadolivre.com.br/_Container_...` | página de listagem do próprio Mercado Livre | **a testar** |

A vitrine `/social/` pertence ao afiliado que a montou. O programa
recusa afiliar a loja de outra pessoa — daí o
`URL not allowed in affiliates program`.

A página `lista.mercadolivre.com.br` é conteúdo do Mercado Livre,
não de um afiliado. Foi a partir dela que o operador conseguiu
gerar link pela ferramenta oficial.

## URL sob teste

```
https://lista.mercadolivre.com.br/_Container_promotions-77-full
  ?coupon_campaign_id=14194174
  #tracking_id=4a393864-...&source=affiliate-profile
```

Observações:

- `_Container_promotions-77-full` — container de promoções
- `coupon_campaign_id=14194174` — a campanha de cupom
- o **fragmento** (`#tracking_id=...`) normalmente não é enviado
  pelo navegador, mas aqui a URL viaja dentro do JSON do body,
  então segue inteiro. Vale observar se atrapalha.

## Por que isso importa para a arquitetura

`plataformas/mercadolivre/links.py` já declara
`lista.mercadolivre.com.br` em `_HOSTS_LISTAGEM` e o classifica
como cenário `lista`. A distinção entre vitrine e listagem já está
codificada — falta confirmar que a listagem é aceita pela API.

## Pergunta que esta rodada NÃO responde

Os `meli.la` que chegam dos grupos monitorados expandem para qual
dos dois formatos? Se expandirem para `/social/`, não são
afiliáveis por esta via e será preciso outro caminho. Isso se
descobre expandindo alguns `meli.la` reais.
