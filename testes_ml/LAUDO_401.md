# LAUDO — HTTP 401 no createLink

Bateria controlada, sequencial, 7 cenários, uma requisição cada,
2s de pausa. Nenhum valor de cookie, token ou segredo registrado.

## Matriz

| Cenário | HTTP | s | Resultado |
|---|---|---|---|
| 1 produção (aiohttp + jar) | 401 | 0.29 | `message=Unauthorized` |
| 2 aiohttp sem jar | 401 | 0.23 | `message=Unauthorized` |
| 3 urllib | 401 | 0.21 | `message=Unauthorized` |
| 4 **sem Cookie (só CSRF)** | **401** | 0.20 | `message=Unauthorized` |
| 5 sem CSRF (só Cookie) | 401 | 0.22 | `message=Unauthorized` |
| 6 só cookies do ML | 401 | 0.20 | `message=Unauthorized` |
| 7 + headers de navegador | 401 | 0.20 | `message=Unauthorized` |

## CAUSA PROVADA

**A variável não contém sessão.**

O cenário 4 é o que fecha o caso: mandando **cookie nenhum**, o
servidor responde exatamente igual ao cenário 1, que manda a
credencial inteira. Mesmo status, mesmo corpo, mesmo tempo.

Quando remover a credencial não muda a resposta, a credencial não é
o que está sendo avaliado. O servidor trata o que enviamos como
equivalente a não enviar nada — porque é equivalente.

O inventário confirma. Dos 8 cookies de sessão, **todos ausentes**:

| cookie | estado |
|---|---|
| `ssid` | AUSENTE |
| `orguserid` | AUSENTE |
| `_csrf` | AUSENTE |
| `x-meli-session-id` | AUSENTE |
| `x-bf-session-v6` | AUSENTE |
| `_d2id` | AUSENTE |
| `_mldataSessionId` | AUSENTE |
| `nsa_rotok` | presente |

Composição real das 18 entradas:

| função | qtd | nomes |
|---|---|---|
| ML-sessão | 1 | `nsa_rotok` |
| ML-experimento | 2 | `c_OlkuS`, `c_Z2jUesn` |
| ML-interface | 1 | `nav_dab_closed` |
| rastreador de 3º | 3 | `QSI_SI_*`, `_pin_unauth`, `ttcsid_*` |
| fragmento de valor | 7 | (cauda de valores com `;`) |

Sobram **4 cookies reais do Mercado Livre**, e nenhum deles carrega
identidade autenticada. `nsa_rotok` sozinho não é sessão.

Isto não é sessão expirada. Não é credencial recusada. É **ausência
de credencial**.

## HIPÓTESES REFUTADAS

Derivadas da própria matriz, não escritas à mão.

1. **"É o transporte aiohttp"** — cenário 2 (401) empata com o 3
   (401). O `urllib`, que é o transporte do teste histórico que
   obteve 200, leva o mesmo 401.
2. **"É o CookieJar"** — cenário 1 empata com o 2. Com jar e sem
   jar, idêntico.
3. **"É o conteúdo do cookie"** — cenário 4, sem cookie, dá o mesmo
   resultado do cenário 1.
4. **"Os rastreadores de terceiro atrapalham"** — cenário 6, só com
   cookies do ML, dá o mesmo.
5. **"Faltam headers de navegador"** — cenário 7 também dá 401.
   Esta era a única hipótese com diferença observada por trás (o
   teste histórico mandava `headers_extra` do cURL); foi testada com
   uma variável só e caiu.
6. **"Há dado de outra conta embutido no código"** — varredura do
   pacote ML: zero achados. O único acerto é um nome de parâmetro
   numa lista de bloqueio, não um valor. Toda credencial vem de
   variável de ambiente.

## DADO AUSENTE

- O header `cookie:` cru da requisição autenticada do navegador.
- Qual credencial produziu o HTTP 200 histórico — vinha de
  `ML_TEST_CURL`, variável que não existe mais.

## PRÓXIMO TESTE

Repor `ML_SESSION_COOKIE` com um header que contenha os cookies de
sessão, e rodar de novo o cenário 1. Se `ssid` e `orguserid`
aparecerem no inventário e o status mudar de 401, causa confirmada
por reversão.

Enquanto o inventário acusar zero cookies de sessão, qualquer
alteração de código é remédio para doença que não existe.
