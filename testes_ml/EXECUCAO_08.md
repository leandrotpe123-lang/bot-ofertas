# Execução 8 — RESULTADO: expandidor resolve, mas todo destino é vitrine

## Veredito em uma linha

O expandidor de produção **resolve o Mercado Livre sem captcha**.
Mas os cinco links expandiram para `/social/<afiliado>` — e nenhum
é afiliável pelo `createLink`.

```
Expandidos : 5/5
Captcha    : 0/5
Afiliáveis : 0/5
```

## Destinos encontrados

| Link recebido | Destino | Classe |
|---|---|---|
| `meli.la/1ipL9sf` | `/social/samuelf3lipe?matt_word=samuelf3lipe…` | SOCIAL |
| `meli.la/26eaLSW` | `/social/promotom?matt_word=promotom…` | SOCIAL |
| `meli.la/1bZpCya` | `/social/fadadoscupons?matt_word=fadadoscupons…` | SOCIAL |
| `meli.la/2nhS3s3` | `/social/samuelf3lipe?matt_word=samuelf3lipe…` | SOCIAL |
| `mercadolivre.com/sec/2U6U32Q` | `/social/promotom/lists/8f90988a-…?matt_tool=54541970` | SOCIAL |

Tempo: 737ms a ~1s por link. Sem erro, sem timeout, sem captcha.

## Achado 1 — o captcha era do expandidor caseiro

A execução 7 caiu em `/captcha/wall` porque o expandidor do teste
não mandava `Accept-Language`, usava UA fixo e seguia redirect
manualmente. O de produção passa. **O IP da Railway não está
bloqueado.**

## Achado 2 — os `meli.la` dos grupos são links de afiliado

Todos apontam para a vitrine de quem publicou, com `matt_word` do
terceiro. Não são links de produto nem de listagem do Mercado Livre.

Isso é coerente com tudo que já sabíamos: os grupos publicam os
**próprios links afiliados**. O `meli.la` deles é o equivalente do
`meli.la/2TzpFAP` que geramos — produto final da afiliação alheia.

Portanto: **`meli.la` de grupo ≡ `/sec/` de grupo**. Mesma natureza,
mesmo tratamento. Não há URL de origem a recuperar.

## Achado 3 — o `/sec/` revelou uma lista real

O `/sec/2U6U32Q` expandiu para:

```
/social/promotom/lists/8f90988a-1c69-4f23-8b26-76286c3cdc87
  ?matt_tool=54541970&forceInApp=true
```

Note: tem `/lists/<uuid>` e **não tem `matt_word`**. É uma lista
específica dentro da vitrine do promotom, não a vitrine genérica.

Não testado ainda: essa URL, sem `matt_word`, é aceita pelo
`createLink`? Se for, muda o quadro — teria caminho de conversão
para o que vem do Promotom.

Vale um teste. Se der `111`, encerra a questão e o fallback por
`/sec/` próprio é a única saída.

## Achado 4 — nenhuma plataforma declara encurtador

```
encurtadores declarados: 0 → (nenhum)
→ meli.la NÃO está declarado como encurtador
```

`_compor_encurtadores()` devolveu conjunto vazio. Como o
`_eh_intermediario` só continua a resolução para hosts declarados,
a expansão para no primeiro destino.

Aqui não fez diferença — um salto bastou. Mas se algum encurtador
passar a resolver por meta-refresh ou JS, a cadeia pararia cedo.

O pacote `plataformas/mercadolivre/` (ainda fora do repositório)
declara `ENCURTADORES_FORCA_GET` como conjunto vazio explícito, mas
não declara a capacidade `encurtadores`. Ponto a revisar antes de
publicar o pacote.

## Consequência para a arquitetura

A rota de conversão do Mercado Livre depende do que chega:

| Origem | Formato | Rota |
|---|---|---|
| link direto de produto | `/p/MLB…`, `/up/MLBU…`, `MLB-…` | `createLink` ✔ |
| link direto de listagem | `lista.mercadolivre.com.br/…` | `createLink` ✔ |
| `meli.la` de grupo | expande para `/social/<terceiro>` | fallback próprio |
| `/sec/` de grupo | expande para `/social/<terceiro>/lists/…` | fallback próprio (a confirmar) |

A expansão continua valendo a pena: é ela que **distingue** um
`meli.la` que aponta para produto de um que aponta para vitrine.
Custa ~1s e não bate em captcha.

## Próximo teste

`createLink` com a URL de lista revelada pelo `/sec/`:

```
https://www.mercadolivre.com.br/social/promotom/lists/8f90988a-1c69-4f23-8b26-76286c3cdc87?forceInApp=true
```

Com e sem `matt_tool`. Se passar, há rota para o Promotom. Se der
`111`, o fallback é definitivo.
