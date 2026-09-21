# Sonda de sessão — Mercado Livre — resultado das 4 rodadas

Diagnóstico do `HTTP 401` / `CookieError` no `createLink`.
Nenhum valor de cookie, token ou segredo foi registrado em nenhuma
rodada: só contagens, tamanhos, nomes e booleanos.

## Linha do tempo medida

| momento | `cookies=` na carga | falha |
|---|---|---|
| credencial antiga | 0 | HTTP 401 |
| credencial nova (Create Link) | 28 | `CookieError` |
| sonda, jar vazio | 28 | HTTP 401 `Unauthorized` |
| sonda, só os 24 legais | 24 | HTTP 401 `Unauthorized` |
| sonda, transporte urllib | 28 | HTTP 401 `Unauthorized` |

## Rodada 4 — transporte x credencial

O teste histórico `teste_ml_createlink.py`, que já obteve 200 e gerou
`meli.la`, usa `urllib.request`; a produção usa `aiohttp`. Os dois
transportes receberam o MESMO dicionário de cabeçalhos, o mesmo
corpo, a mesma URL, a mesma tag e a mesma credencial — montados uma
única vez e compartilhados.

| TRANSPORTE | STATUS | RESULTADO |
|---|---|---|
| urllib | 401 | `Unauthorized` |
| aiohttp | 401 | `Unauthorized` |

**O transporte não explica a recusa.** Hipótese encerrada.

## Inventário de cookies — o que faltou

| cookie | estado |
|---|---|
| `ssid` | **PRESENTE** |
| `_csrf` | **PRESENTE** |
| `nsa_rotok` | **PRESENTE** |
| `orguserid` | ausente |
| `_d2id` | ausente |
| `x-meli-session-id` | ausente |
| `x-bf-session-v6` | ausente |
| `_mldataSessionId` | ausente |

Dos 24 nomes legais, a esmagadora maioria é rastreador de terceiro —
`__rtbh.*` (RTB House), `_hjSession*` (Hotjar), `_pin_unauth`
(Pinterest), `ttcsid_*` (TikTok), `QSI_SI_*` (Qualtrics), `_gads-ID`,
`g_state` (Google), `_cq_duid` (Cheq) — mais os `c_*` de experimento
do próprio Mercado Livre e itens de UI (`ml_cart-quantity`,
`nav_dab_closed`).

A credencial carrega quase tudo, menos identidade autenticada.

Isso casa com o relato do operador: a primeira captura, feita numa
requisição de `www.mercadolivre.com.br`, tinha ~56 cookies; a atual,
feita no createLink, tem 28. Sumiu metade — e sumiram justamente os
de sessão.

**Não está concluído que algum desses cookies seja obrigatório.**
Presença numa captura histórica não prova exigência. O que está
medido é a ausência.

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
5. **"É o transporte aiohttp"** — plausível, com um teste histórico
   de 200 por urllib para sustentá-la. Medida: **falsa**, os dois
   levam 401.

## Duas frentes, independentes

**1 — Robustez (defeito nosso). CORRIGIDO.**
`cliente._obter_sessao` semeava o jar com `jar.update_cookies(dict)`
numa chamada só, e um único nome ilegal derrubava os 28 com
`CookieError`. Agora a semeadura é individual, com `except
CookieError` específico, e a `ClientSession` é criada de qualquer
forma. O header `Cookie` bruto nunca passou por aí: é ele que
autentica, e segue intacto.

**2 — Credencial. EM ABERTO.**
Com o jar fora do caminho e com dois transportes diferentes, o
servidor responde `Unauthorized`. Descartar os 4 fragmentos não muda.
A requisição chega inteira e é recusada.

O que está medido e ainda não explicado: faltam 5 dos 8 cookies de
sessão conhecidos, e o `x-csrf-token` (36 chars) difere do cookie
`_csrf` (25 chars).
