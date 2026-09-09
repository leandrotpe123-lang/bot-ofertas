# Execução 10 — forense da LISTA

## Escopo

Apenas `/social/<afiliado>/lists/<UUID>`. Produto não é
reinvestigado — já foi comprovado manualmente pelo operador:

```
meli.la → /social/promotom → poly-card → <a href> → "Ir para produto"
```

## URL sob teste

Lista real descoberta na execução 8, expandindo `/sec/2U6U32Q`:

```
/social/promotom/lists/8f90988a-1c69-4f23-8b26-76286c3cdc87
  ?matt_tool=54541970&forceInApp=true
```

Comparada com a vitrine do mesmo afiliado: `/social/promotom`.

Usar o mesmo afiliado nas duas leituras isola a variável: qualquer
diferença encontrada é de **tipo de página**, não de conta.

## Separação exigida

| Bloco | O que responde |
|---|---|
| A | produtos dentro da lista |
| B | identidade da lista |
| C | URL da lista |
| D | URL de cada produto |

## O que é procurado

**Elementos** — `poly-card`, `poly-card--list`, `poly-card--large`,
`poly-card--grid`, `poly-component__title`

**Links** — todo `<a href>`, separados em: href de produto (contém
`MLB`), href com `/lists/`, href de `lista.mercadolivre.com.br`,
href `/social/`. Mais a contagem de `"Ir para produto"` e de
`"Ir para a lista"` / `"Ver lista"`.

**Identificadores** — `MLB…`, `MLBU…`, UUID no path, UUIDs no corpo,
e a contagem das chaves `product_id`, `productId`, `item_id`,
`itemId`, `listId`, `list_id`, `list_uuid`, `permalink`.

**Estrutura** — títulos dos cards e a **ordem** dos pares
(href → texto) na sequência do documento.

A ordem importa mais do que parece: se a curadoria da lista vier
antes da moldura de recomendação, existe regra explorável. Se vier
embaralhada, extrair produto não resolve.

## A pergunta que decide

Existe uma URL própria da lista — algo como
`lista.mercadolivre.com.br/…` — dentro da própria resposta?

Se existir, há rota direta: a lista de terceiro apontaria para uma
listagem do Mercado Livre, que já provamos ser afiliável
(execução 6).

Se não existir, a lista de terceiro só oferece os produtos
individuais, e a decisão passa a ser entre afiliar produto a produto
ou usar o fallback próprio.

## Restrições

Somente leitura. Sem Playwright, sem navegador automatizado, sem
seguir endpoint inventado, sem tocar produção, sem imprimir
credencial. Destino que vier como muro de captcha é registrado e o
teste aborta, sem contorno.
