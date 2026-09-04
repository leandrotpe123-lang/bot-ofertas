# Execução 8 — expandidor real de produção vs. Mercado Livre

Rodada dedicada a uma pergunta só: o `desencurtar` que já roda em
produção resolve links do Mercado Livre, ou cai no muro de captcha
como o expandidor caseiro da execução 7?

## O que muda em relação à execução 7

A execução 7 usava um expandidor escrito para o teste. Este importa
`utils.url_resolver.desencurtar`, o mesmo código que atende Shopee,
Amazon e Magalu hoje.

Diferenças que podem explicar um resultado melhor:

| Recurso | teste caseiro (exec 7) | produção (exec 8) |
|---|---|---|
| User-Agent | fixo | sorteado de `config.USER_AGENTS` |
| `Accept-Language` | ausente | `pt-BR,pt;q=0.9,en;q=0.8` |
| redirecionamento | manual, salto a salto | `allow_redirects` do aiohttp |
| fallback GET | sim | sim |
| meta-refresh | não | sim |
| redirect por JS | não | sim |
| `og:url` / canonical | não | sim |
| cache | não | em memória |

## Links sob teste

```
https://meli.la/1ipL9sf
https://meli.la/26eaLSW
https://meli.la/1bZpCya
https://meli.la/2nhS3s3
https://mercadolivre.com/sec/2U6U32Q
```

## O que o log deve mostrar por link

URL recebida, host de entrada, tempo, se expandiu, URL final, host
final, classificação (PRODUTO / LISTA / SEC / SOCIAL / CAPTCHA /
NÃO RESOLVIDO / OUTRO), identificador quando houver, afiliável
SIM/NÃO com motivo, e o erro exato em caso de falha.

## Verificação estrutural incluída

O teste também imprime a configuração efetiva do expandidor e,
principalmente, **se `meli.la` está declarado como encurtador** pelo
registry.

Isso importa: `_eh_intermediario` decide se a resolução continua
depois do primeiro destino. Se `meli.la` não estiver declarado, a
expansão para cedo — e seria um problema independente do captcha.

## Hipótese do operador sobre `meli.la` cru no createLink

O operador testou no gerador oficial: colar `meli.la` cru **dá
erro**. Isso torna improvável que o `createLink` aceite um `meli.la`
sem expansão — a hipótese levantada na execução 7 provavelmente
está errada, e a expansão continua sendo passo obrigatório.

Motivo a mais para saber se o expandidor de produção funciona.
