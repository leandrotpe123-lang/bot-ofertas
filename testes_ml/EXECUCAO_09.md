# Execução 9 — teste forense da vitrine /social/

## Pergunta

Uma URL afiliada que expande para `/social/…` ainda carrega
internamente a identidade do produto ou da lista de origem?

Se carregar, existe caminho para recuperar a oferta real a partir do
link afiliado de terceiro. Se não, o fallback próprio é a única
saída para o que vem do Promotom.

## Método

1. expandir com `utils.url_resolver.desencurtar` (o de produção)
2. **uma** leitura HTTP legítima da página de destino
3. mapear a estrutura da resposta

Nenhum endpoint descoberto é seguido. Esta etapa é só o mapa.

## O que é procurado

| Categoria | Itens |
|---|---|
| identidade de produto | `MLB…`, `MLBU…`, `item_id`, `itemId`, `productId`, `product_id`, `catalog_product_id`, `permalink` |
| identidade de lista | UUID no path, UUIDs no corpo, `listId`, `list_id`, `collection` |
| coleções | `products`, `items` |
| estado serializado | `__NEXT_DATA__` e suas chaves, `__PRELOADED_STATE__`, `__INITIAL_STATE__` |
| metadados | `og:url`, `canonical`, JSON-LD e seus `@type` |
| rotas | URLs de produto/lista explícitas, endpoints de API referenciados |

## Controle

A mesma leitura é feita sobre a listagem real que gerou `meli.la`
com sucesso na execução 6:

```
lista.mercadolivre.com.br/_Container_promotions-77-full?coupon_campaign_id=14194174
```

A comparação das duas estruturas responde se existe relação entre:

```
LISTA REAL → createLink → meli.la → /social/…/lists/UUID
```

## Critério de conclusão por URL

| Conclusão | Quando |
|---|---|
| RECUPERÁVEL | há URLs de produto explícitas na resposta |
| PARCIALMENTE RECUPERÁVEL | há IDs ou identificador de lista, sem URL montada |
| NÃO RECUPERÁVEL | nenhuma identidade na resposta, ou muro de captcha |

## Captcha

Se algum destino vier como muro, é registrado como resultado e o
link é pulado. Nenhuma tentativa de contorno.

## Dívida registrada

O arquivo `teste_ml_forense.py` tem uma expressão morta numa
f-string de log (`r[chr(39)+chr(39)] if False else …`). Compila em
Python 3.13, mas é código morto e deve ser limpo na próxima
alteração do arquivo.
