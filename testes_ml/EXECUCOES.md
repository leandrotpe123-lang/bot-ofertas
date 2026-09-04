# Execuções do teste createLink

Registro operacional. Cada linha é uma execução real na Railway.

| # | Quando | Entrada | Resultado |
|---|---|---|---|
| 1 | 02:07 | produto | REPROVADO — cURL de outra requisição |
| 2 | 02:58 | produto | REPROVADO — valor não começava com `curl` |
| 3 | 03:16 | produto `/p/MLB16016316` | **APROVADO** |
| 4 | 03:33 | lista `/social/fadadoscupons` | **REPROVADO pelo servidor** |

---

## Execução 3 — produto — APROVADO

```
HTTP: 200
total_success: 1 | total_error: 0
created: True
tag validada: SIM
short_url: https://meli.la/2B59gJm
long_url presente: SIM (336 chars)
```

**Confirmado em produção pelo operador: o link creditou a conta
correta.** Validação de mundo real, não apenas do teste.

---

## Execução 4 — lista de terceiro — REPROVADO pelo servidor

Entrada: `https://www.mercadolivre.com.br/social/fadadoscupons?
matt_word=fadadoscupons&matt_tool=59965296&forceInApp=true&ref=...`

```
matt_word na URL de entrada: fadadoscupons (DE TERCEIRO)
HTTP: 200
total_success: 0 | total_error: 1
Chaves no item: ['error_code', 'message', 'origin_url', 'status']
```

### O resultado mais importante desta rodada

O Mercado Livre **recusou gerar** o link. Não gerou um link com a
identidade do terceiro — devolveu erro por item.

Isso significa que o servidor **não é uma máquina cega de
encurtar**: ele valida a URL de origem e rejeita a vitrine de outro
afiliado. O risco de publicar comissão para o concorrente por esta
via está **descartado por comportamento do próprio servidor**.

A validação de `matt_word` na `long_url` (commit ab15764)
permanece no teste como rede de segurança. Ela não chegou a ser
exercida aqui porque não houve `long_url` — mas continua sendo a
barreira correta caso o comportamento mude.

### Erro parcial dentro de HTTP 200

O envelope traz `status: 200` no topo **e** falha no item:
`total_success: 0`, `total_error: 1`, com `error_code` e `message`
em `urls[0]`. Qualquer cliente definitivo precisa checar o item,
nunca só o status HTTP.

### Pendência para a próxima rodada

O log truncou antes de mostrar `error_code` e `message`. São eles
que dizem **por que** foi recusado — e se o caminho é sanitizar a
URL (remover `matt_word`/`matt_tool` de terceiro, preservando
`ref`) ou se listas de terceiros simplesmente não são afiliáveis.

Próximo teste: mesma lista, URL **sanitizada** — sem `matt_word`,
sem `matt_tool`, **preservando `ref`** (o `ref` é a curadoria, é
onde estão os produtos).

---

## Fatos estabelecidos até aqui

1. O endpoint funciona por replay de cookie: `Cookie` +
   `x-csrf-token` + User-Agent de navegador → HTTP 200.
2. Body mínimo basta: `{"urls": [...], "tag": "..."}`.
3. Produto: gera `meli.la` e credita corretamente (confirmado).
4. Estrutura de sucesso: `urls[0]` objeto, snake_case.
5. Formato de saída é `meli.la`, não `mercadolivre.com/sec/`.
6. `long_url` volta na resposta — serve como forma canônica de
   identidade sem request extra.
7. Erro é por item, dentro de HTTP 200.
8. URL de vitrine de terceiro com identidade dele é **recusada**.
9. 8 dos 10 cookies da lista bastaram (23 no total enviados) — o
   conjunto mínimo real segue desconhecido.

## Perguntas ainda abertas

- Qual o `error_code`/`message` da recusa de lista?
- Lista sanitizada (sem identidade de terceiro, com `ref`) é aceita?
- Quanto tempo a sessão sobrevive?
- Qual o conjunto mínimo de cookies?

---

## Aprendizados de operação

- `redeploy` reaproveita o snapshot de configuração do build
  anterior. Trocar o start command exige um deploy **novo** (push).
- O worker é único: enquanto o teste roda, o bot fica fora do ar
  por cerca de um minuto. Nada se perde.
- Restaurar sempre para `python main.py` logo após ler o log.
- `ML_TEST_CURL` é credencial de sessão: apagar após o teste.
