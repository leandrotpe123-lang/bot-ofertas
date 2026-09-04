# Execução 9 — RESULTADO: a vitrine ENTREGA os produtos

## Resumo

```
PARCIALMENTE RECUPERAVEL : 5/5
NAO RECUPERAVEL          : 0/5
```

| Link | Tipo | Produtos no HTML | UUIDs | Chaves |
|---|---|---|---|---|
| `meli.la/1ipL9sf` | SOCIAL | **57** | 32 | `item_id×4`, `product_id×42` |
| `meli.la/26eaLSW` | SOCIAL | **50** | 29 | `item_id×4`, `product_id×36` |
| `meli.la/1bZpCya` | SOCIAL | **42** | 29 | `item_id×4`, `product_id×16` |
| `meli.la/2nhS3s3` | SOCIAL | **80** | 39 | `item_id×4`, `product_id×50` |
| `/sec/2U6U32Q` | SOCIAL_LIST | **41** | 9 | `product_id×13`, `listId×1` |

Páginas de 218KB a 339KB, renderizadas no servidor. Sem
`__NEXT_DATA__`, sem estado pré-carregado, sem JSON-LD, sem
`canonical`, **sem nenhum endpoint de API referenciado**.

Exemplos reais de IDs extraídos (link 1):
`MLB4329495823`, `MLB51435621`, `MLBU3635468836`, `MLB4325411061`,
`MLB46560669`, `MLBU3629247668`, `MLB4660482345`, `MLB53838786`

## O achado

**A vitrine de terceiro carrega os produtos no próprio HTML.**

Cada `/social/<afiliado>` traz dezenas de identificadores `MLB` e
`MLBU` embutidos, junto de `product_id` e `item_id`. Não é uma
página vazia que busca dados depois — o conteúdo vem pronto.

Isso significa que a identidade da oferta **não se perde** quando o
grupo publica seu link afiliado. Ela está lá, legível.

## O `/sec/` é uma lista específica

`/sec/2U6U32Q` expandiu para
`/social/promotom/lists/8f90988a-1c69-4f23-8b26-76286c3cdc87`, com o
UUID **no próprio path** e `listId×1` no corpo — contra 29 a 39
UUIDs difusos nas vitrines genéricas.

Ou seja: o `/sec/` do Promotom aponta para uma curadoria específica,
não para a vitrine inteira. 41 produtos identificados.

## O controle inverteu a expectativa

A listagem real — a que **gerou** `meli.la` com sucesso na execução
6 — devolveu:

```
produtos identificados : 0
chaves de identidade   : (nenhuma)
CONCLUSÃO: NAO RECUPERAVEL
```

A página que **é afiliável** não entrega produto nenhum na leitura
anônima; a que **não é afiliável** entrega dezenas.

A explicação provável: `lista.mercadolivre.com.br` monta o conteúdo
no cliente, enquanto `/social/` é renderizada no servidor. São
tecnologias diferentes, e a leitura anônima só enxerga a segunda.

Consequência prática: elegibilidade no `createLink` e riqueza de
conteúdo no HTML são propriedades **independentes**. Uma não prevê a
outra.

## O que isso abre

Havia duas hipóteses para o que vem do Promotom e dos demais grupos:

1. fallback por `/sec/` próprio — link genérico, não corresponde à
   oferta anunciada
2. extrair os produtos e afiliar cada um — antes considerado
   inviável por falta de dados

**A hipótese 2 deixou de ser inviável.** Os IDs estão no HTML, e o
`createLink` já provou que afilia produto por `MLB`.

## O que ainda não sabemos

- **Qual produto corresponde à oferta anunciada?** A mensagem do
  grupo fala de UM produto; a vitrine devolve 42 a 80. Sem o texto
  da mensagem para casar, extrair 57 IDs não diz qual é o certo.
- **Os IDs são da oferta ou da moldura?** Parte pode ser
  recomendação, "quem viu também viu", banner. Não distinguimos.
- **Ordem tem significado?** Se o primeiro bloco for a curadoria e o
  resto for recomendação, muda tudo. Não medido.
- **Custo.** Cada leitura foi de 218KB a 339KB, ~1,3s por página.
  Somado à expansão, é caro para o caminho quente.

## Próximo passo sugerido

Antes de qualquer implementação: pegar UMA mensagem real de grupo
com o texto da oferta, expandir o link, extrair os IDs e verificar
se o produto anunciado está entre eles — e em que posição.

Se estiver sempre nas primeiras posições, há regra. Se estiver em
posição aleatória, extrair não resolve e o fallback continua sendo
a resposta.

## Dívida registrada

`teste_ml_forense.py` tem uma expressão morta numa f-string de log
(`r[chr(39)+chr(39)] if False else …`). Compila em Python 3.13, mas
é código morto — limpar na próxima alteração do arquivo.
