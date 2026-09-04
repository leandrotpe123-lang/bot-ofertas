# Formatos de URL do Mercado Livre — o que chega e o que converte

Referência viva, construída a partir de links reais dos grupos
monitorados e de testes contra a API `createLink`.

## Comportamento por grupo

| Grupo | Costuma postar |
|---|---|
| Promotom | quase sempre `/sec/` |
| Fumotom, Fada, Samuel | `lista.mercadolivre.com.br` e links de produto |

Regra observada: cupom **genérico** → `/sec/`; cupom **com lista
própria** → link da listagem.

## Os formatos

### Afiliáveis

| Formato | Exemplo | Status |
|---|---|---|
| `/p/MLB<dígitos>` | `/notebook-vaio-…/p/MLB45513180` | testado, aprovado |
| `/up/MLBU<dígitos>` | `/kit-5-tops-…/up/MLBU3929091094` | catálogo unificado |
| `MLB-<dígitos>-…_JM` | `produto.mercadolivre.com.br/MLB-6009607732-…_JM` | anúncio direto |
| `lista.mercadolivre.com.br/…` | `/_Container_promotions-77-full?coupon_campaign_id=…` | testado, aprovado |

### Não afiliáveis

| Formato | Motivo |
|---|---|
| `/social/<slug>` | vitrine de um afiliado — `error_code 111` |
| `/sec/XXXX` | link de afiliado de terceiro; o gerador oficial também não converte |

A razão é a mesma nos dois casos: **já são o produto da afiliação
de alguém**, não conteúdo do Mercado Livre. A prova apareceu na
resposta de sucesso da listagem, cuja `long_url` devolvida é
justamente `/social/<nossa_tag>?matt_word=<nossa_tag>&ref=…`.

A vitrine é a **saída** da afiliação, nunca a entrada.

### Opaco até expandir

`meli.la/XXXX` — pode virar qualquer um dos acima. Só a expansão
revela.

## Tratamento de `/sec/`

Confirmado manualmente pelo operador: desencurtou, limpou os dados
de terceiro e tentou no gerador oficial — não gera de jeito nenhum.

Regra: substituir pelo `/sec/` próprio (`ML_SEC_PROPRIO`).

Cuidado de identidade: a substituição é constante, então a URL
publicada é sempre a mesma. Para não colapsar a identidade de todas
as ofertas `/sec/` numa só, `afiliacao.py` devolve
`Afiliacao(publicada=<sec próprio>, canonica=<url recebida>)` — a
canônica é distinta por oferta e preserva a deduplicação.

## Lacuna conhecida em `links.py`

A regex atual é `/MLB[-]?(\d{5,})`, que exige dígito logo após
`MLB`. O formato `/up/MLBU3929091094` tem um `U` no meio e **não é
reconhecido** — link real de grupo que hoje cairia em `INVALIDO` e
seria descartado.

Testado em sandbox contra as URLs reais:

```
up/MLBU (catálogo)     invalido   AUSENTE          -
p/MLB (produto)        produto    MLB45513180      produto
produto.ml MLB-        produto    MLB6009607732    produto
produto.ml + lixo      produto    MLB6009607732    produto
```

Correção necessária antes de o pacote ir para produção.

## Falta também uma noção de elegibilidade

`links.py` classifica cenário, mas não responde "esta URL é
afiliável?". Sem isso, `afiliacao.py` enviaria `/social/<terceiro>`
à API e levaria `111` — degrada correto, mas gasta requisição e
retry à toa.

Proposta: `links.eh_afiliavel(url)`, reprovando `/social/` e
`/sec/` antes de qualquer I/O.
