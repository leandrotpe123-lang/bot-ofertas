# Execução 11 — caça ao `_Container_` na vitrine

## Por que esta execução existe

A execução 10 concluiu que **não existe URL de lista afiliável** na
vitrine de terceiro. A conclusão estava errada, e o erro foi de
análise, não de coleta:

> o teste contou **79** URLs `lista.mercadolivre.com.br` e imprimiu
> apenas as **5 primeiras**. Como o menu de categorias vem no topo
> do HTML, as cinco eram `#menu=categories`. Generalizei de 5 para
> 79.

O dado estava no HTML baixado. Não foi olhado.

## O que o operador encontrou

No DevTools, dentro da vitrine `/social/fadadoscupons`:

```html
<a href="https://lista.mercadolivre.com.br/_Container_14216538
         ?coupon_campaign_id=…#tracking_id=…&source=affiliate-profile"
   rel="nofollow" class="ui-recommendations-subtitle-link">
```

Árvore em que aparece:

```
rl-social-desktop__carousel
└ story-recommendations-container
  └ ui-recommendations-carousel-wrapper-ref
    └ ui-recommendations-carousel-snapped__header-titles
      ├ ui-recommendations-title       ← nome do cupom
      └ ui-recommendations-subtitle
        └ a.ui-recommendations-subtitle-link   ← A LISTA
```

É o subtítulo de cada carrossel. O `<section aria-label="Busca de
fada dos cupons" aria-roledescription="Carrossel">` confirma: cada
carrossel é um cupom, com seu próprio `_Container_`.

## Por que isso importa

O formato é o mesmo que o `createLink` **aprovou** na execução 6:

| | URL |
|---|---|
| execução 6, aprovada | `_Container_promotions-77-full?coupon_campaign_id=14194174` |
| achado no DevTools | `_Container_14216538?coupon_campaign_id=14216538` |

Se confirmar, a cadeia fecha:

```
meli.la de grupo → /social/<terceiro> → <a> do carrossel
→ lista.mercadolivre.com.br/_Container_<id>
→ createLink → meli.la NOSSO, de lista
```

Lista de terceiro passaria a virar lista nossa, com a curadoria
certa — sem fallback, sem afiliar produto a produto.

## O que este teste faz

Classifica **todas** as URLs `lista.mercadolivre.com.br` de cada
página, separando `_Container_`, `#menu=categories` e outras. Nada
é omitido do log.

Para cada `_Container_`, extrai o identificador e o
`coupon_campaign_id`, e casa com o título do carrossel em que
aparece — para saber a que cupom pertence.

## Páginas cobertas

| Página | Por quê |
|---|---|
| `/social/fadadoscupons` | onde o operador achou |
| `/social/promotom` | grupo que só posta `/sec/` |
| `/social/promotom/lists/8f90988a-…` | lista específica |
| `/social/samuelf3lipe` | terceiro grupo |

Quatro páginas mostram se o padrão é geral ou específico de uma
conta.

## Restrições

Somente leitura. Sem `createLink`, sem Playwright, sem tocar
produção, sem imprimir credencial.

## Lição de método

Amostra não é conclusão. Contar 79 e olhar 5 produziu uma afirmação
categórica falsa que quase encerrou a linha de investigação certa.
Quando o log traz contagem alta e exemplos poucos, a leitura correta
é "não sei o que são as outras 74".
