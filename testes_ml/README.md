# testes_ml

Testes isolados, executados manualmente. Nada aqui é importado por
código de produção.

**Não existe `__init__.py` nesta pasta de propósito**: assim ela não
é um pacote Python e não pode ser importada por engano, nem pelo
Auto Discovery de plataformas.

## teste_ml_createlink.py

Responde uma única pergunta: com uma sessão real do Mercado Livre,
o POST para `createLink` devolve o link curto de afiliado com a
minha tag?

Não usa Playwright, não sobe o bot, não toca o pacote
`plataformas/mercadolivre`, não escreve em disco e nunca imprime
valores de cookie, token ou credencial.

### Variáveis

| Variável | Obrigatória | Conteúdo |
|---|---|---|
| `ML_TEST_CURL` | sim | cURL da requisição `createLink`, copiado do DevTools |
| `ML_TAG` | sim | etiqueta de afiliado |
| `ML_TEST_URL` | não | URL longa de produto |

### Como capturar o `ML_TEST_CURL`

1. Navegador logado no Mercado Livre.
2. Abrir uma oferta com a barra de Afiliados visível.
3. F12 → aba **Network**.
4. Clicar em **Compartilhar** e gerar um link normalmente.
5. Na lista, achar a requisição **`createLink`** — é ela, não
   qualquer outra.
6. Botão direito → Copy → **Copy as cURL (bash)**.

O valor colado precisa **começar com a palavra `curl`**. Se começar
com `POST`, com uma URL, ou for um JSON, veio da opção errada do
menu (por exemplo "Copy request headers" ou "Copy as fetch").

O cURL correto contém, no mínimo:

- header `cookie:` com os cookies de sessão (`ssid`, `orguserid`,
  `_csrf`, ...) — não apenas cookies de banner e telemetria;
- header `x-csrf-token:`;
- header `user-agent:`.

### Diagnósticos e o que significam

| Mensagem | Causa |
|---|---|
| `conteúdo de ML_TEST_CURL não reconhecido` | não começa com `curl` — opção errada do menu Copy |
| `formato cmd do Windows` | usar "Copy as cURL (**bash**)" |
| `CSRF encontrado: NAO` | cURL de outra requisição |
| `ALERTA: só cookies de interface` | cURL de outra requisição |
| `header recusado pela biblioteca HTTP` | quebra de linha no valor colado |

### Como executar (Railway)

Trocar o start command do serviço temporariamente para:

```
python -u testes_ml/teste_ml_createlink.py
```

Ver o resultado nos logs e **devolver o comando original**
(`python main.py`). Depois, apagar `ML_TEST_CURL` — é credencial
de sessão.

Atenção: `redeploy` reaproveita o snapshot de configuração do build
anterior. Para que uma troca de start command tenha efeito, é
preciso um deploy novo (push).
