# LAUDO — Frente 7

Sonda `sonda_frente7.py`, deploy `bbe79b31`, 22/09/2026.
Redirects seguidos à mão, salto a salto. Nenhum segredo em log.

## O achado

Os três alvos caem no **mesmo lugar**:

| alvo | destino final |
|---|---|
| `meli.la/1E3uX78` | `/social/promotom/lists/8cfd7017-…` |
| `meli.la/2pbjuX4` | `/social/promotom/lists/7323d1d5-…` |
| `mercadolivre.com/sec/2U6U32Q` | `/social/promotom/lists/8f90988a-…` |

**Não são três problemas. É um: lista de afiliado.**

O Promotom publica tudo nesse formato — é coerente com o
`FORMATOS.md`, que já registrava "Promotom: quase sempre `/sec/`".

Ponto de parada dos três, idêntico:

```
PONTO DE PARADA   elegibilidade | cenario=lista_afiliado
```

Nenhum chegou ao createLink. A frente do produto/lista não foi
tocada — ela continua 15/15.

## Autópsia da lista de afiliado

Idêntica nos três documentos:

| marca | ocorrências |
|---|---|
| `"recommendation_info"` | **0** |
| `"seeMoreLink"` | **0** |
| `_Container_` | **0** |
| `"polycards"` | **1** |
| `"appProps"` | 1 |
| `coupon_campaign_id` | 0 |

Leitura:

- a **estrutura da vitrine não existe** nessa página — por isso a
  descoberta atual não a resolve, e está correta em não resolver;
- **não há `seeMoreLink` nem `_Container_`** — não existe uma URL de
  lista afiliável pronta no documento;
- **mas `polycards` existe** — os produtos estão lá, em outro
  caminho do JSON.

### Classificação das hipóteses da Frente 7

- **(A) diretamente afiliável** — FALSO. Cenário `lista_afiliado`,
  fora de `_CENARIOS_ELEGIVEIS`; e o `FORMATOS.md` já registrava que
  o gerador oficial recusa vitrine de terceiro.
- **(C) apenas lista interna opaca** — FALSO. Os produtos estão no
  documento.
- **(B) transformável** — PLAUSÍVEL, e é o único caminho vivo: os
  produtos existem em `polycards`, fora do caminho que
  `descoberta.py` conhece.

**Isto é evidência, não autorização.** Extrair N produtos de uma
lista transforma um link em vários — decisão de produto, não
detalhe técnico.

## Onde o `/sec/` morre

`pipeline/filtros_bloco.py`, `_bloco_permanece`:

```python
if any(_publicavel(u, mapa, preservar) for u in urls):
    return True
if any(registry.resolver(_norm_url(u)) is not None for u in urls):
    return False        # ← aqui
```

Docstring: *"link DE PLATAFORMA que não converteu — sai"*. O
`registry` reconhece `mercadolivre.com`, o `/sec/` não converte, o
bloco inteiro cai — rótulo junto.

## A regra `/sec/` + lista já existe

`_segmentar` põe **cada URL em bloco próprio**, com o rótulo
anterior. `montar_texto` faz `mapa.get(u, u)`: URL sem conversão
volta à original, **não é apagada**.

Logo `/sec/` e lista **nunca competiram**. A regra desejada — lista
não apaga `/sec/`, `/sec/` não substitui lista — já é o
comportamento do core.

O que quebra é só o descarte do bloco não convertido. **Se o `/sec/`
passar a converter, o bloco sobrevive sozinho, sem tocar no core.**

## ML_SEC_PROPRIO

Uma única ocorrência no repositório inteiro, e é no
`testes_ml/FORMATOS.md`. Não está no código. Não está nas variáveis
do Railway. Decisão de projeto documentada e nunca construída:

> *"Regra: substituir pelo `/sec/` próprio (`ML_SEC_PROPRIO`). Para
> não colapsar a identidade de todas as ofertas `/sec/` numa só,
> `afiliacao.py` devolve `Afiliacao(publicada=<sec próprio>,
> canonica=<url recebida>)`."*

O cuidado de dedupe já estava resolvido no desenho.
