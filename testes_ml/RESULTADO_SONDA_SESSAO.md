# Sonda de sessão — Mercado Livre — resultado das 3 rodadas

Diagnóstico do `HTTP 401` / `CookieError` no `createLink`.
Nenhum valor de cookie, token ou segredo foi registrado em nenhuma
rodada: só contagens, tamanhos e booleanos.

## Linha do tempo medida

| momento | `cookies=` na carga | falha |
|---|---|---|
| credencial antiga | 0 | HTTP 401 |
| credencial nova (Create Link) | 28 | `CookieError` |
| sonda, jar vazio | 28 | HTTP 401 `Unauthorized` |

## O que a sonda mediu

**Leitura da credencial: correta.**

- `ML_SESSION_COOKIE` presente, 28 pares `nome=valor`
- **112 sequências `%XX`** — os valores estão percent-encoded, como
  num header de navegador de verdade
- `Cookie`: 0 caracteres de controle
- `ML_CSRF_TOKEN` presente, 38 chars crus → 36 após `_limpar`,
  sem caractere de controle

**Semeadura do jar: 24 aceitos, 4 recusados.**

Quatro fragmentos com nome ilegal, `len` 28 / 33 / 140 / 484,
caracteres proibidos `' '` e `'/'`. Cada um vem logo depois de um
cookie de rastreamento cujo valor contém `;` sem codificar:
`g_state`, `_cq_duid`, `ttcsid_*` — e um depois de `ssid`.

Não são cookies: são a cauda do valor anterior, partida pelo
`split(";")`. O navegador manda esses valores do mesmo jeito; quem
não tolera é o `http.cookies` do Python.

**Servidor: recusa.**

`POST createLink` com jar vazio e header completo →
`401 {"message": "Unauthorized"}`, JSON limpo da própria API.
Repetido com apenas os 24 cookies de nome legal → **mesmo 401**.

**`x-csrf-token` ≠ cookie `_csrf`** — 36 chars contra 25.

## Hipóteses minhas que a medição DERRUBOU

Ficam registradas para não voltarem.

1. **"Base64 sem padding"** — inferida por casamento de assinatura
   (`cookies=1` na sessão, `0` no cliente). O patch de padding foi
   correto e consertou dois defeitos latentes reais, mas **não era
   a causa** daquele 401.
2. **"Valores descodificados, `%3B` virou `;`"** — explicaria os
   espaços e barras dos fragmentos. Medido: há 112 sequências
   `%XX`. **Falsa.**
3. **`ValueError: Forbidden control character`** na rodada 1 — era
   defeito da própria sonda, que mandava o CSRF **cru** enquanto a
   produção manda `_limpar(csrf)`. Medir coisa diferente da que roda
   em produção é como a sonda inventa um defeito que não existe.
4. **`GET /affiliate-program/hub`** para testar se a sessão está
   viva — caminho chutado, respondeu **404**. Teste inconclusivo por
   erro meu, não por resultado.

## Duas frentes, independentes

**1 — Robustez (defeito nosso, real).**
`cliente._obter_sessao` semeia o jar com `jar.update_cookies(dict)`
numa chamada só. Um único nome ilegal derruba os 28 e levanta
`CookieError`, que sobe como "erro inesperado" e vira `AUSENTE`.

Um header `Cookie` legítimo de navegador contém cookies de
rastreamento com valor fora de spec. O sistema precisa tolerar isso.
Além do mais, o jar **não é o que autentica** — o `cliente` manda o
header `Cookie` explicitamente. O jar só serve para absorver
`Set-Cookie` da resposta.

**2 — Credencial (não é defeito de código).**
Com o jar fora do caminho, o servidor ainda responde `Unauthorized`.
Descartar os 4 fragmentos não muda. A requisição chega inteira e é
recusada.

O `x-csrf-token` e o cookie `_csrf` têm tamanhos diferentes (36 x
25). Os dois precisam vir da **mesma requisição**, capturados no
**mesmo momento** — token de um instante com cookie de outro é
recusado mesmo com sessão viva.
