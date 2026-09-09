# Execução 10 — RESULTADO: a lista entrega URLs de produto prontas

## Comparação

| Campo | SOCIAL (`/lists`) | SOCIAL_LIST (`/lists/<UUID>`) |
|---|---|---|
| poly-card | SIM (90) | SIM (70) |
| poly-card--list | NAO | NAO |
| poly-card--large | NAO | NAO |
| poly-card--grid | SIM (18) | SIM (14) |
| href de produto | **SIM (18)** | **SIM (14)** |
| "Ir para produto" | NAO | NAO |
| listId | NAO | **SIM (1)** |
| uuid no path | NAO | `8f90988a-1c69-4f23-8b26-76286c3cdc87` |
| uuid no corpo | SIM (21) | SIM (7) |
| href `/lists/` | SIM (6 listas) | SIM (1) |
| href `lista.mercadolivre.com.br` | SIM (79) | SIM (79) |
| IDs MLB | — | 41 distintos |

## Respostas diretas

**A página `/lists` contém links individuais de produtos?** SIM.

**Aparecem em `href`?** SIM — 14 na lista específica, 18 na página
de listas. URLs completas e navegáveis:

```
/combo-cuide-se-bem-locao-corporal-deleite-400ml-refil-ros/p/MLB53980844
/samsung-galaxy-buds3-fone-de-ouvido-sem-fio-galaxy-ai-cinza/p/MLB38059088
/mouse-gamer-sem-fio-logitech-g-pro-x2-superstrike-lightspeed/p/MLB65536248
/air-fryer-britania-42l-dura-mais-1500w-bfr38-127v/p/MLB29694703
```

Formato `/p/MLB…` — exatamente o que o `createLink` aprovou na
execução 3.

**Existe `<a>` para a própria lista?** SIM. A página `/lists` do
promotom expõe **6 listas distintas**, cada uma com seu UUID:

```
/social/promotom/lists/8f90988a-1c69-4f23-8b26-76286c3cdc87
/social/promotom/lists/0ba6b879-2daa-4adf-9173-a8b4a55f7648
/social/promotom/lists/580f83ff-0007-42ca-8a68-ca76e320d7ff
/social/promotom/lists/aacee5d0-68f1-440a-a005-967bf647912d
/social/promotom/lists/3277e65c-d91b-4d5f-8b3d-aff1130ceffd
```

**Existe listId/UUID?** SIM. UUID no path e `listId` no corpo — mas
`listId` aparece **apenas** na lista específica, nunca na página de
listas. É o marcador que distingue as duas.

**Existe URL `lista.mercadolivre.com.br`?** SIM, 79 — mas todas são
**menu de categorias**, não a lista curada:

```
/celulares-telefones/acessorios-celulares/#menu=categories
/informatica/componentes-pc/#menu=categories
```

Navegação do site, presente igualmente nas duas páginas. **Não
serve como URL afiliável da lista.**

**HTML traz produtos completos ou só IDs?** Completos — `href` mais
título.

**Quantos produtos?** 41 IDs MLB distintos, mas apenas **14 cards
com href**. A diferença (27 IDs sem card) é moldura: recomendação,
telemetria, blocos laterais.

Os 14 com `href` e `poly-card--grid` são a curadoria real.

## Diferença estrutural

Só duas: **contagem de poly-card** e **presença de `listId`**.

Estruturalmente as páginas são a mesma coisa — mesma grade, mesmo
componente, mesmos `href` de produto. A lista específica é um
recorte da vitrine.

Note também que `"Ir para produto"` **não aparece no HTML** de
nenhuma das duas (contagem 0). O texto que o operador vê no
navegador é renderizado depois; o que existe no HTML servido é o
`href` direto — que é melhor, porque dispensa interação.

## A resposta que decide

**NÃO existe URL de lista afiliável.**

A lista de terceiro não aponta para nenhuma
`lista.mercadolivre.com.br/_Container_…` — só para menus de
categoria. Não há como converter a lista dele numa lista nossa.

**O que existe são 14 URLs de produto prontas**, no formato que o
`createLink` já aprovou.

## Consequência

Para lista de terceiro sobram dois caminhos, e a escolha é de
negócio, não técnica:

1. **Fallback `/sec/` próprio** — um link, destino genérico, não
   corresponde à oferta anunciada
2. **Afiliar os produtos** — 14 chamadas ao `createLink`, cada uma
   com link correto e creditando a nós

O caminho 2 é tecnicamente viável e não era antes. Custo: ~1,3s de
leitura da página, mais N chamadas de afiliação.

## Pergunta que ainda não foi respondida

Qual dos 14 corresponde à oferta anunciada na mensagem? A curadoria
tem 14 produtos; a mensagem do grupo fala de um cupom. Se o cupom
vale para a lista inteira, publicar vários faz sentido. Se vale para
um item, é preciso casar com o texto da mensagem.

Isso não se resolve lendo HTML — precisa de uma mensagem real com o
texto ao lado.
