# Execução 11 — RESULTADO: o `_Container_` não vem no HTML servido

## O que saiu

```
fadadoscupons (vitrine)
  HTML: 294.375 bytes | hrefs distintos: 236
  carrosséis (ui-recommendations-title) : 0
  ui-recommendations-subtitle-link      : 0

  URLs lista.mercadolivre.com.br : 79
    _Container_       : 0
    #menu=categories  : 24
    outras            : 55
```

Mesmo resultado nas quatro páginas: promotom, samuelf3lipe e a
lista específica. **Zero `_Container_` em todas.**

## A execução 10 estava certa quanto ao fato, errada quanto ao método

A conclusão "não há `_Container_` no HTML" se confirma. Mas ela
tinha sido tirada de 5 exemplos de 79 — sorte, não análise. Agora
está medida.

E a classificação completa corrigiu outro engano meu: eu disse que
as 79 eram "todas menu de categorias". São **24** menus. As outras
**55** são buscas genéricas, que eu nunca tinha visto:

```
lista.mercadolivre.com.br/apple-watch
lista.mercadolivre.com.br/ar-condicionado-inverter
lista.mercadolivre.com.br/cafeteira
lista.mercadolivre.com.br/fogao-4-bocas
lista.mercadolivre.com.br/fone-de-ouvido-bluetooth
lista.mercadolivre.com.br/freezer-vertical
```

São links de busca por termo — sem `coupon_campaign_id`, sem
`_Container_`. Não correspondem a nenhum cupom específico.

## O número que explica tudo

```
carrosséis (ui-recommendations-title) : 0
ui-recommendations-subtitle-link      : 0
```

**Zero.** O container inteiro do carrossel não existe no HTML
servido.

O operador vê no DevTools:

```
rl-social-desktop__carousel
└ story-recommendations-container
  └ ui-recommendations-carousel-wrapper-ref
    └ ui-recommendations-carousel-snapped__header-titles
      └ a.ui-recommendations-subtitle-link → _Container_14216538
```

Nós, na leitura HTTP, não vemos nada disso.

Conclusão: **o carrossel de recomendações é montado no cliente**, por
JavaScript, depois do carregamento. O DevTools mostra o DOM já
renderizado; a leitura HTTP mostra o HTML servido. São coisas
diferentes, e a diferença aqui é justamente o link que interessa.

Isso é coerente com a execução 9, onde a listagem real
(`lista.mercadolivre.com.br`, também client-side) devolveu 0
produtos na leitura anônima enquanto a vitrine devolvia dezenas.

## O que continua verdadeiro

A vitrine **entrega os produtos** no HTML servido: 41 IDs MLB, 14
com `href` no formato `/p/MLB…` — comprovado na execução 10. Essa
parte é server-side e está acessível.

O que é client-side é a **camada de carrosséis de cupom**, e é
justamente onde mora o `_Container_`.

## Onde isso deixa a lista

| Caminho | Situação |
|---|---|
| `_Container_` a partir da vitrine | bloqueado por render no cliente |
| produtos da vitrine → `createLink` | **viável**, 14 URLs prontas |
| fallback `/sec/` próprio | viável, destino genérico |

## O que ainda pode ser tentado, sem navegador

O `_Container_14216538` tem o mesmo número no path e no
`coupon_campaign_id`. Se esse padrão se sustentar, a URL é
**construtível** a partir do id da campanha:

```
lista.mercadolivre.com.br/_Container_<id>?coupon_campaign_id=<id>
```

Falta descobrir de onde tirar o `<id>` sem executar JavaScript.
Duas frentes possíveis, ambas de leitura:

1. procurar `coupon_campaign_id` ou `campaign` em qualquer lugar do
   HTML servido (o teste atual só procurou dentro de `href`)
2. verificar se o `meli.la` do grupo, ao ser expandido, carrega o id
   em algum parâmetro

Nenhuma das duas exige Playwright. São mais uma passada de leitura
sobre bytes que já sabemos baixar.

## Lição de método, mantida

Amostra não é conclusão — mas o inverso também vale: **o DOM do
DevTools não é o HTML servido**. Confundir os dois custou uma
execução. Daqui em diante, todo achado do DevTools precisa ser
confirmado na leitura HTTP antes de virar premissa.
