# Execução 7 — CAPTCHA WALL no IP da Railway

Rodada do teste de expansão. Um único link processado (a variável
`ML_TEST_LINKS` continha só a URL de catálogo).

```
[1/1] .../kit-5-tops-.../up/MLBU3929...
  entrada: cenário=produto elegibilidade=AFILIAVEL
  saltos: 1
    302 → https://www.mercadolivre.com.br/captcha/wall?go_url=...
  URL final: .../captcha/wall?go_url=...
  cenário = outro-ml
  >>> DESCONHECIDO ?
```

## O achado

O Mercado Livre respondeu **302 para `/captcha/wall`**. Não é erro
do teste: o site detectou requisição vinda de IP de datacenter e
serviu o muro de captcha em vez da página.

Isso muda uma premissa do projeto e precisa ser tratado antes de
qualquer implementação.

## O que isso afeta

**Afeta a EXPANSÃO de links** (`meli.la` → URL final), que é
navegação anônima a partir do IP da Railway.

**NÃO afeta o `createLink`**, que continua funcionando: aquela
chamada vai autenticada por cookie de sessão, e foi aprovada
minutos antes do mesmo IP. Sessão autenticada não cai no muro.

## Por que importa

O fluxo desenhado é:

```
meli.la → expandir → classificar → createLink
```

Se a expansão bate no captcha, o passo 2 falha e o resto não
acontece — mesmo com a sessão válida.

## Hipóteses a testar

1. **O muro é só para navegação anônima.** Expandir enviando os
   cookies da sessão resolveria.
2. **É por User-Agent/comportamento.** Um `Accept-Language` e
   `Referer` plausíveis, ou seguir com GET em vez de HEAD, podem
   passar.
3. **É por reputação de IP e é permanente.** Nesse caso a expansão
   precisa de outra rota (ex.: usar `long_url` devolvida pelo
   próprio `createLink`, que já vem de graça na resposta).

## Observação importante sobre a hipótese 3

A resposta de sucesso do `createLink` **já devolve `long_url`**.
Se o `meli.la` recebido puder ser enviado direto ao `createLink`
sem expansão prévia, a expansão deixa de ser necessária no caminho
quente — e o problema do captcha desaparece.

Isso ainda não foi testado: nunca enviamos um `meli.la` cru ao
`createLink`. É o teste mais barato e o de maior impacto agora.

## Bug secundário do teste

O resumo foi impresso no meio do detalhamento. Os logs saem por
streams diferentes e se intercalaram. Não afeta o resultado, mas
prejudica a leitura — corrigir com uma única escrita ao final.
