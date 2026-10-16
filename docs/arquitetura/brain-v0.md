# Brain v0 — fase 0: desenho, dados e medições

> **Status:** fase 0. Nada roda em produção: não há serviço Brain, chave de
> produção nem bot. O protótipo está na branch `brain-v0-prototipo` (commit
> `85ddc42`), sem PR. Este documento é o relatório A–L pedido pelo dono em
> 14/10/2026, com os registros de 15/10 (§0, B, I, M).
>
> **Fontes:**
> - logs da Railway do worker (`ec1b8e14`), 07–14/10;
> - prévias públicas `t.me` (sem login);
> - código da `main` em `670024a`.
>
> **Hipóteses** vêm marcadas como tal. Todo número tem a fonte ao lado.

## 0. O que o Brain faz, e o que nunca faz

Logo depois de cada publicação, a IA procura três coisas:

- **(a)** a mesma campanha ou cupom em 2+ posts;
- **(b)** uma liberação nova da fonte que o canal perdeu (absorvida num post
  antigo);
- **(c)** uma fusão errada.

Estágios: **SOMBRA** (só registra) → **ALERTA** (DM ao dono) → **AÇÃO** (o
worker corrige em segundos). Cada passagem de estágio exige prova e aprovação
do dono.

**O v0 serve só para SOMBRA e ALERTA** (registro do dono, 15/10).
- O v0 lê a prévia pública. Ela atrasa até ~1 min (B) e não diz qual
  mensagem virou qual post.
- **AÇÃO só com o v1**, que tem os eventos da C1 (`decisao.*`, `post.*`, J).
  Nenhum PR de AÇÃO sai sobre o v0.

**Decisões do dono que não se reabrem:**
- serviço separado na Railway, no mesmo projeto, com código em `/brain`;
- sem volume no v0 (se um dia precisar, volume próprio, nunca `/data`);
- nunca `TELEGRAM_SESSION`, nem uma segunda sessão MTProto;
- nunca escreve no SQLite do worker;
- entrada: prévia pública no v0; eventos da F1 mais o texto da prévia no v1;
- a IA só recomenda; quem decide é regra determinística; quem executa é o
  worker, com os locks normais;
- a IA nunca atrasa a publicação;
- chave exclusiva do Brain, com limite de gasto, só nas variáveis da Railway;
- **o Brain nunca recebe token da Railway**, nem para ler logs. O token que
  lê logs também muda produção (deploy, variáveis, serviços). O Brain lê a
  prévia pública no v0 e os eventos da C1 no v1;
- bot do BotFather, só DM, nunca entra em canal ou grupo, e só responde ao
  `BRAIN_DONO_CHAT_ID` (I, passo 7);
- modelos:
  - principal: `claude-haiku-5-5`, effort high;
  - referência: `claude-sonnet-5-5`, effort high;
  - Haiku também medido em medium e low; Sonnet medium/low só com aprovação
    do dono, se o Haiku falhar (E);
  - cascata Haiku → Sonnet quando o Haiku dá INCERTO, certeza baixa ou recusa;
  - `claude-opus-5-5` só no subconjunto difícil, se o orçamento deixar;
  - Fable fora.

## A. Pré-filtro medido em 7 dias

O pré-filtro é grátis e determinístico. Ele decide o que vale uma chamada de
IA (`brain/brain/prefiltro.py`).

**Ficam FORA, porque o worker já junta sem IA:**
- mesmo código de cupom;
- mesmo link;
- mesmo SKU.

**Vão para a IA:**
- **classe 1:** post sem âncora de produto × posts sem âncora da mesma
  plataforma na última hora;
- **classe 2:** produto × produto com título e preço quase iguais e link
  diferente;
- **classe 3:** mensagem de cupom sem código (`cupb`) que não virou post
  próprio;
- **(c):** cada fusão feita pelo worker.

| Dia (UTC) | posts | sem âncora de produto | classe 1: pares / posts-alvo | classe 3 | fusões (c) | **chamadas/dia** | fração dos posts |
|---|---|---|---|---|---|---|---|
| 08/10 | 453 | 35 | 13 / 11 | 5 | 11 | **27** | 6,0% |
| 09/10 | 388 | 59 | 69 / 37 | 9 | 7 | **53** | 13,7% |
| 10/10 (10.10) | 995 | 125 | 320 / 100 | 12 | 12 | **124** | 12,5% |
| 11/10 | 358 | 22 | 11 / 8 | 2 | 1 | **11** | 3,1% |
| 12/10 | 185 | 13 | 9 / 6 | 1 | 1 | **8** | 4,3% |
| 13/10 | 159 | 22 | 19 / 9 | 3 | 0 | **12** | 7,5% |
| 14/10 (parcial) | 346 | 28 | 59 / 16 | 5 | 2 | 23 | — |

De onde vem cada coluna:
- **posts** e **sem âncora:** linhas `🚀 [OK]`, pela `identity` (sem
  `|MLB…`/SKU = sem âncora de produto);
- **classe 3:** linhas `✅ [PASSOU] … tipo=cupom` com âncora `cupb` cuja
  mensagem não ganhou post próprio; o grupo de teste fica de fora;
- **fusões:** linhas `🗑 [FUSAO_REMOVIDO]`;
- **chamadas/dia:** posts-alvo da classe 1, mais a classe 3, mais as fusões.
  Uma chamada compara o alvo com todos os vizinhos dele;
- 07/10 só tem log a partir das 18:28 e ficou fora.

**Classe 2:** zero pares em 08–14/10.
- Medido na prévia do canal: 2.841 posts, de 07/10 23:54 a 14/10 18:51 UTC.
- O dedup por SKU do worker já cobre produto. No v0, produto × produto não
  gasta IA.

**Média de 6 dias completos:** 39 chamadas/dia (~1.180/mês). O pico, no
10.10, foi de 124 chamadas.

**A prévia classifica como o worker.** O tipo do post vem da linha `✅ R$`
que a montagem do worker gera (`pipeline/montagem_texto.py`, papel PREÇO).
Contagem de "sem âncora", prévia × logs:

| Dia | prévia | logs |
|---|---|---|
| 08/10 | 34 | 35 |
| 09/10 | 62 | 59 |
| 11/10 | 25 | 22 |
| 12/10 | 13 | 13 |
| 13/10 | 22 | 22 |
| 10/10 | 116 | 125 |

No 10.10, os 9 de diferença são posts apagados, que a prévia não mostra.

**Código de cupom** segue a doutrina do worker (`utils/cupom.py`): só o que foi
marcado como código, que na prévia é o `<code>`. Nada é inferido do texto.

## B. Prévias públicas

**Existem e respondem sem login**, nos 7 canais medidos:
- o canal: `fullpromotion`;
- o canal de cupons: `FullPromotionCupons`;
- as fontes: `promotom`, `fumotom`, `fadadoscupons`, `SamuelF3lipePromo` e
  `fadapromos`.

Há duas formas:
- página `t.me/s/<canal>`: 20 mensagens por página, `?before=<id>` pagina;
- embed `t.me/<canal>/<id>?embed=1`: texto fiel, com o `<code>` do cupom.

**Medição**, coletor próprio com ≥10 s por página e jitter:

| | run1 | run2 | total |
|---|---|---|---|
| janela (UTC) | 18:21–18:51 | 20:35–21:09 | **64 min** |
| requisições | 530 (~1.060/h) | 556 (~990/h) | 1.086 |
| HTTP p50 / p95 | 808 / 1.031 ms | 768 / 1.026 ms | — |
| 429 | 0 | 0 | **0** |
| erro de transporte | 3,4% | 5,9% | — |

**Atraso até ficar visível** (data do post até o coletor vê-lo):

| | n | p50 | p75 | p90 | máximo |
|---|---|---|---|---|---|
| canal (`fullpromotion`) | 16 | 13 s | 57 s | 59 s | 67 s |
| todos os canais | 48 | 13 s | 30 s | 50 s | 67 s |

**Ficou abaixo das ≥2 h pedidas.** As duas runs morreram quando o contêiner
da sessão de medição foi reciclado: na primeira vez por reinício, na segunda
por inatividade. Medição longa em segundo plano não sobrevive aqui. Os 2 h
completos, com a rodada Shopee das 03 h UTC, ficam para:
- uma sessão ativa;
- ou o próprio serviço SOMBRA, que registra o atraso de cada post de forma
  contínua (recomendado).

Notas:
- os erros de transporte vêm do proxy do contêiner de medição (`URLError`),
  não do Telegram. É hipótese: todos foram sem resposta HTTP;
- a cadência do canal é de 10–14 s, então até ~14 s do atraso é o próprio
  ciclo de leitura.

**Achado — a prévia pública do Telegram atrasa até ~1 min, e não é só cache
da página:**
- o post 29138 (20:45:10) só apareceu às 20:46:06, com três respostas 200
  sem ele no meio;
- a **sonda do próximo id pelo embed** (20:57–21:09) não furou o atraso. O
  worker publicou 29140, 29141 e 29142 às 21:07:34, 21:08:05 e 21:08:28
  (`🚀 [OK]`). Os três ficaram públicos juntos:

| post | publicado (worker) | embed viu | `t.me/s` viu |
|---|---|---|---|
| 29140 | 21:07:34 | 21:08:34 (+61 s) | 21:08:41 (+67 s) |
| 29141 | 21:08:05 | 21:08:35 (+30 s) | 21:08:41 (+36 s) |
| 29142 | 21:08:28 | 21:08:35 (+8 s) | 21:08:41 (+13 s) |

- Conclusão: a prévia pública (embed e página) é atualizada em lotes, do
  lado do Telegram. **Nenhuma forma de ler a prévia garante menos de ~1
  min.** É o limite do v0 (G).

**Edições ficam visíveis** (marca "edited"):
- o worker registrou `✏️ Editado | dest_id=29112` às 18:31:36;
- a prévia mostrou a marca às 18:31:46;
- foram 13 edições nas duas runs (7 no canal).

**Edição na fonte × decisão do worker** (conferido nos logs):
- a Promotom 111394 e a Samuel 121476 (20:56) foram editadas várias vezes;
  na prévia, o texto cresceu de 126 para 321 caracteres;
- o worker descartou cada versão com `DESCARTE motivo=NORMALIZACAO_VAZIA`;
- é o certo: são kits da Terabyte (links cutt.ly/tidd.ly), loja fora das
  plataformas do worker (`plataformas/`).
- **Regra para o Brain:** descarte de plataforma não suportada nunca é
  "liberação perdida".
- No mesmo intervalo, o worker publicou 29140–29145 da **mesma live** Shopee
  7496585. É outra armadilha "live, produtos diferentes" para o conjunto de
  casos.

**Exclusões** aparecem como buraco na numeração. Conferido nos logs:

| post | origem | removido |
|---|---|---|
| 28697 | Fumotom 39267 | 14:16:16 |
| 28699 | Samuel 121195 | 14:20:37 |

Os dois eram MLB18725308, e os dois sumiram da prévia. Os removidos saem com
`[ORIGEM_APAGADA_REMOVIDO]`.

**Post apagado à mão pelo dono** aparece no embed como **"Post not found"**
(`tgme_widget_message_error`).
- Exemplos: 29156 e 29161. O dono os apagou em 15/10 e a prévia conferiu.
- **Regra para o Brain:** "Post not found" = post apagado, igual ao buraco
  na numeração. A política já trata post apagado como NADA (D, linha 3).
- O risco do lado do worker e a proposta de detecção estão em M.

**Orçamento de requisições do v0:**
- canal a cada 10–14 s: ~300/h;
- 6 fontes a cada 30–40 s: ~600/h;
- total ≤ ~25 mil/dia.

**Regras do coletor:**
- nunca login, cookie nem sessão;
- backoff crescente em erro ou 429;
- disjuntor;
- Brain fora do ar não afeta o worker.

## C. Juiz (`brain/brain/juiz.py`, `brain/brain/cliente.py`)

**Prompt v0** em português, com as regras do domínio:
- **MESMA_CAMPANHA:**
  - o mesmo lançamento por grupos diferentes, nos mesmos poucos minutos;
  - cupom seco × específico ao mesmo tempo, como decidido pelo dono;
  - o mesmo código; o mesmo produto.
- **LIBERACAO_NOVA:**
  - série do mesmo grupo, de hora em hora;
  - benefício que mudou.
- **PRODUTOS_DIFERENTES:** itens diferentes, inclusive da mesma live.
- **LEMBRETE** é uma marca, não um veredito: "ainda ativo", "esgotando".
- **INCERTO** é preferido a adivinhar.

**Saída fechada por JSON schema** (structured outputs, `additionalProperties:
false`):

| campo | valores |
|---|---|
| `veredito` | MESMA_CAMPANHA, LIBERACAO_NOVA, PRODUTOS_DIFERENTES, INCERTO |
| `certeza` | 0 a 1 |
| `ids_relacionados` | só ids da entrada |
| `evidencias` | até 5 × 200 caracteres |
| `acao_sugerida` | NADA, ALERTAR, FUNDIR, REINJETAR |
| `lembrete` | sim ou não |

**Contra injeção** (texto de grupo é dado não confiável):
- sem ferramentas;
- texto entre `<texto>…</texto>`, e um `</texto>` vindo do grupo é
  neutralizado;
- o prompt manda ignorar instruções dentro do texto;
- id fora do conjunto de entrada derruba para INCERTO;
- a `acao_sugerida` da IA nunca é usada pela política.

**Viram INCERTO, que só registra:**
- recusa (`stop_reason` "refusal");
- corte (`max_tokens`);
- JSON inválido;
- campo fora do schema;
- enum ou certeza inválidos.

**Registro:** todo veredito leva modelo, effort, versão do prompt, usage,
custo e latência. Config que não passou no portão (`CONFIGS_VALIDADAS`,
vazia hoje) nunca vira ação.

**Resiliência:**
- timeout de 30 s; 3 tentativas;
- respeita `retry-after`; senão, backoff exponencial com jitter (2 s a 30 s);
- 429/529 e 5xx são transitórios; 4xx é permanente, sem repetir;
- disjuntor: 5 falhas seguidas abrem por 300 s;
- tetos diários: 400 chamadas e US$ 1,00. Valores provisórios, ver E;
- sem IA, o Brain "só registra".

**Fatos da API** (skill `claude-api`, conferida em 14/10):
- Haiku 5.5:
  - US$ 0,10 / 0,50 por MTok;
  - não aceita temperature/top_p/top_k fora do padrão (400);
  - não aceita prefill;
  - pensamento adaptativo ligado por padrão.
- Sonnet 5.5: US$ 2 / 10 por MTok.
- Opus 5.5: US$ 4 / 20 por MTok.
- Pensamento conta como saída.
- SDK fixado em `anthropic==1.9.0`.

## D. Política determinística (`brain/brain/politica.py`)

A ordem importa: primeiro o que **nunca age**.

| # | condição | faria (em AÇÃO) |
|---|---|---|
| 1 | config não validada | só REGISTRAR |
| 2 | veredito INCERTO | REGISTRAR |
| 3 | post apagado (alvo ou candidato) | NADA |
| 4 | fora da janela (1 h) | NADA |
| 5 | MESMA_CAMPANHA, mensagem absorvida | NADA (absorção certa) |
| 6 | MESMA_CAMPANHA, lembrete ≥ 5 min depois | NADA (alerta legítimo) |
| 7 | MESMA_CAMPANHA, só o link da live em comum | ALERTAR (nunca funde) |
| 8 | MESMA_CAMPANHA, conteúdo exclusivo no post que sairia | ALERTAR (nunca apaga) |
| 9 | MESMA_CAMPANHA, certeza ≥ 0,90 | PEDIR_FUNDIR |
| 10 | MESMA_CAMPANHA, certeza 0,70–0,90 | ALERTAR |
| 11 | LIBERACAO_NOVA absorvida, certeza ≥ 0,90 | PEDIR_REINJETAR |
| 12 | LIBERACAO_NOVA absorvida, certeza 0,70–0,90 | ALERTAR |
| 13 | PRODUTOS_DIFERENTES absorvidos | ALERTAR (absorção errada) |
| 14 | PRODUTOS_DIFERENTES em posts separados | NADA (certo) |

Por cima da tabela:
- **Modo:**
  - `BRAIN_MODO` padrão SOMBRA, que só registra o que faria;
  - ALERTA troca PEDIR_* por ALERTAR;
  - modo desconhecido vira SOMBRA;
  - AÇÃO só por PR, e só no v1 (§0).
- **Limites por hora:** 6 ações (acima disso, vira alerta) e 12 alertas
  (acima disso, só registra).
- **PEDIR_\*** é só um **pedido** ao worker, que revalida tudo (F).

## E. Teste — o portão

**Conjunto:** `brain/teste/casos.json` (versão 2), **103 casos reais**:
56 UM_POST e 47 DOIS_POSTS.
- Texto fiel da prévia (embed). Links trocados por `[link-N]`, e o mesmo
  link vira o mesmo N dentro do caso.
- O texto é o atual da prévia: se a fonte editou depois, o bot pode ter lido
  outro texto naquela hora.

**Regras de rótulo do dono** (15/10):
- **R1:** mesma campanha, fontes diferentes, até ~15 min = **UM post**.
- **R2:** depois de ~15 min, com urgência nova ou valores novos = **post
  novo**.
- **R3:** cupom diferente = **post novo**.
- Pela R1–R3: SN10 passa a UM_POST; SN15, SN17, SN20 e SN23 são DOIS_POSTS;
  os demais SN são UM_POST. Dos meus, só F25 muda (DOIS_POSTS pela R2).

| Grupo | casos | rótulo |
|---|---|---|
| 23 pares da marcação S/N (SN01–SN23) | 23 | **dono** |
| base 29047/29048 e caso 1 (28391/28392) | 2 | **dono** |
| live 28741–28745 e live 29140–29145 (mesma live, produtos diferentes; conferidos pelo texto) | 9 | **dono** |
| falsos positivos 111092→27046, 111103→27126, 120304→27055 | 3 | **dono** |
| trio ML de 14/10 22:00 UTC (UM_POST) | 2 | **dono** |
| "Renovados" Shopee | 2 | 1 **dono** (29160/29161), 1 meu |
| demais pares simultâneos (F) | 30 | meu, marcado |
| demais encontros/absorções (E) | 32 | meu, marcado |

Ficaram de fora:
- F05, F33, F41, F49, F50 e E24: esperam a marcação do dono;
- E20: absorção duvidosa;
- 28913/28914: mesmo texto do caso 1;
- 29151: a fonte apagou;
- 28697→28699: os dois posts foram apagados; é caso de política, coberto em
  `tests/test_brain_politica.py::test_09`.

**Holdout:**
- 48 casos, escolhidos por hash do id (dev = 55);
- casos que inspiraram exemplo do prompt ficam sempre no dev: SN12, SN13,
  SN14, SN16, SN23, a base e o caso 1;
- a iteração (`dev`) só vê o dev.

**Portão** (`passa_no_portao`), sobre **todos** os casos:
- **ZERO falso destrutivo:** a política pediria fundir onde o certo são dois
  posts;
- **ZERO falso reinjetar**;
- **ZERO falso MESMA_CAMPANHA** nos negativos.
- Medido também: acerto do veredito (holdout em separado), "um post" e "dois
  posts" certos, % de INCERTO, estabilidade nas 3 repetições, alerta falso
  (ruído de DM), latência p50/p95, tokens e US$ por chamada.
- O item 23 do dono (lembrete 16 min depois) aceita MESMA_CAMPANHA com
  `lembrete=true`; sem a marca, conta como falso destrutivo.

**Orçamento do teste em etapas**, aprovado pelo dono em 15/10. Valores
presumidos antes de gastar (`python -m brain.avaliar orcamento --cambio
5.0441`, em `85ddc42`):

| etapa | chamadas | US$ | R$ |
|---|---|---|---|
| 1. piloto (Haiku high, 10 casos ×1) | 10 | 0,01 | 0,05 |
| 2. dev (Haiku high, 55 casos ×3, até 3 rodadas) | 495 | 0,51 | 2,60 |
| 3. final Haiku high (103 ×3) | 309 | 0,32 | 1,62 |
| 3. final Haiku medium (103 ×3) | 309 | 0,20 | 1,00 |
| 3. final Haiku low (103 ×3) | 309 | 0,11 | 0,57 |
| 4. Sonnet **só high** (103 ×3) | 309 | 6,42 | 32,38 |
| **total do plano** | **1.741** | **7,58** | **38,21** |

**Só se o Haiku falhar no portão E o dono aprovar**
(a bancada recusa sem `--aprovado-pelo-dono`):

| etapa extra | chamadas | US$ | R$ |
|---|---|---|---|
| Sonnet medium | 309 | 3,95 | 19,91 |
| Sonnet low | 309 | 2,25 | 11,34 |
| Opus high, subconjunto difícil (até 30 ×3) | 90 | 3,78 | 19,05 |

- **Câmbio:** R$ 5,0441, venda, página do Banco Central em 14/10 (pode ser
  preliminar). A PTAX de 13/10 foi R$ 5,0096.
- O cartão cobra IOF por cima; confira na fatura.
- A saída presumida é de 1.800 tokens em high, 1.000 em medium e 450 em low.
  O piloto mede o real e o orçamento é refeito antes do `dev`.
- **Teto total:** US$ 20, somado sobre todas as rodadas gravadas. A bancada
  para antes de passar dele.

**Execução:**
- chave **só de teste**, com limite de gasto, que o dono põe nas variáveis
  do ambiente do Claude Code com o nome **`BRAIN_TESTE_CHAVE`**:
  - nunca `ANTHROPIC_API_KEY`: é a credencial do próprio Claude Code, e as
    sessões passariam a gastar a chave de teste;
  - a bancada lê só esse nome e passa a chave explícita ao cliente, com
    endereço fixo `https://api.anthropic.com`;
  - o cliente do SDK nasce sem nenhuma `ANTHROPIC_*` no ambiente, porque o
    SDK lê `ANTHROPIC_BASE_URL` sozinho e o Claude Code a define;
  - na Railway, o serviço `brain` continua com `ANTHROPIC_API_KEY`;
- uma sessão nova segue `brain/teste/PASSAGEM.md`;
- a existência da chave se confere só como `presente`/`ausente`, sem exibir
  o valor nem o tamanho (comando na PASSAGEM);
- prova de alcance: `api.anthropic.com/v1/models` sem chave devolve **401**
  (conferido em 14/10);
- a chave nunca é impressa.

**Entrega da sessão de teste:**
- tabela modelo × effort, mais a cascata calculada sem gastar de novo;
- a opção mais barata que passa no portão;
- projeção mensal com o custo medido.

**Projeção mensal, presumida** (refeita com o medido; R$ 5,0441):
- 39 chamadas/dia ≈ 1.180 chamadas/mês;
- 11.11 e Black Friday: volume ×3 a ×5;
- custo por chamada presumido: Haiku high US$ 0,00104; Sonnet high US$ 0,0208;
- a cascata supõe que **30%** das chamadas sobem para o Sonnet. É hipótese:
  a fração real é medida no teste.

| | normal | ×3 | ×5 |
|---|---|---|---|
| só Haiku high | US$ 1,22 (R$ 6) | US$ 3,67 (R$ 19) | US$ 6,12 (R$ 31) |
| cascata, 30% sobe ao Sonnet | US$ 8,55 (R$ 43) | US$ 25,7 (R$ 129) | US$ 42,8 (R$ 216) |
| só Sonnet high | US$ 24,4 (R$ 123) | US$ 73,3 (R$ 370) | US$ 122 (R$ 616) |

**Tetos diários** (US$ 1,00 e 400 chamadas): ficam pequenos no ×5 com
cascata. Em 10.10 ×5 seriam ~620 chamadas/dia. São redimensionados por PR com
o custo medido, antes do ALERTA.

## F. Porta de pedidos do worker (desenho; frente de núcleo própria)

**Pedidos:** FUNDIR e REINJETAR, do Brain ao worker. A porta só entra em uso
com o v1 (§0): o v0 nunca pede ação.

**Transporte:**
- uma porta interna **nova** do worker, só POST;
- **não** é a API da F1.1, que continua só leitura por invariante (§9.2 do
  CLAUDE.md e `eventos/api_privada.py`);
- copia as camadas de proteção da F1.1:
  - só rede privada (`*.railway.internal`, dual-stack);
  - recusa cabeçalho de borda pública;
  - HMAC-SHA256 com carimbo de ±60 s;
  - balde de fichas;
- segredo **próprio** (ex.: `BRAIN_PEDIDOS_SEGREDO`, ≥ 32 caracteres);
- **desligada por padrão:** sem porta e segredo, nada liga.

**Pedido:**
- `pedido_id` (idempotência);
- `acao`;
- ids dos posts e das mensagens;
- resumo do veredito (modelo, effort, versão do prompt, certeza);
- `criado_em`.

**O que o worker faz, nesta ordem:**
1. **Autentica.** Falha → 401 genérico.
2. **Idempotência.** `pedido_id` já visto devolve a mesma resposta (tabela
   própria no SQLite do worker; só o worker escreve).
3. **Limite por hora** do lado do worker, independente do Brain.
4. **Revalida sob os locks normais:** ORIGEM → IDENTIDADE (ordenadas) → POST
   (msg_id crescente). Recusa se:
   - algum post está apagado;
   - está fora da janela;
   - o estado mudou desde o veredito (`edit_count`, score, origens);
   - o post que sairia tem conteúdo exclusivo.
5. **Executa só pelos caminhos que já existem:**
   - **REINJETAR:** busca a mensagem de origem (como
     `pipeline/sucessao.py::_buscar`) e reinjeta com
     `EventoRecuperado` + `processar`, no mesmo ponto de entrada. É o caminho
     preferido do CLAUDE.md §3 e **já existe**.
   - **FUNDIR:** hoje a convergência só funde por **contenção de âncoras
     fortes** (produto, cupom com código, destino) e mantém o texto do
     sobrevivente. Post `cupb` não tem âncora forte e nunca funde. Por isso
     FUNDIR **precisa de capacidade nova** na convergência: fusão por pedido
     com **união dos links sem repetição**, nunca apagando conteúdo exclusivo
     (preferência do dono, TAREFA 2). É frente de núcleo própria, com teste
     de reprodução do caso 29047/29048.
6. **Responde:** ACEITO, RECUSADO:<motivo> ou JA_FEITO. Marcador de log
   próprio (ex.: `[PEDIDO_BRAIN]`).
   - O Brain nunca repete uma recusa sozinho.

**Desligar:** sem a variável, a porta fecha. O Brain volta a SOMBRA por
variável. O worker não depende do Brain para nada.

## G. Metas de tempo

| Segmento | v0 (prévia) | fonte |
|---|---|---|
| fonte → post no canal (worker) | 1–4 s, mediana 2 s (n=11) | run1 |
| post → visível na prévia + ciclo de leitura | canal: p50 13 s, p90 59 s, máx. 67 s (n=16) | run1 + run2 |
| pré-filtro | milissegundos (Python puro) | hipótese; medir no serviço |
| IA | **medida no teste** (p50/p95 por config) | E |
| política | microssegundos | código puro |
| DM (Bot API) | hipótese: < 1 s | medir no ALERTA |

**v0 ≤ 60 s:**
- a mediana cabe com folga;
- o **p95 não cabe**: a prévia pública sozinha já chega a 59–67 s, e a sonda
  pelo embed não ajuda (B);
- proposta: v0 com meta **p50 ≤ 60 s e p95 ≤ ~90 s**;
- quem leva a ≤ 15 s é o v1 (eventos do worker). É mais um motivo para a C1
  vir cedo (J).

**v1 ≤ 15 s:**
- eventos pela API privada da F1.1 (espera longa, < 1 s);
- texto pelo embed (~0,8 s);
- a IA precisa de **p95 ≤ ~10 s**, provavelmente Haiku em medium ou low;
- o teste mede.

Cada segmento vai para o log do Brain com carimbo, para medir de ponta a
ponta.

## H. Laço de aprendizado

- **Registro:** todo veredito com entrada (ids), modelo, effort, versão do
  prompt, decisão da política e o que faria.
- **No ALERTA:** a DM tem dois botões, "é duplicata" e "não é". Cada resposta
  vira rótulo **dono** no conjunto de casos.
- **Toda semana:** rodar a bancada nos casos novos. Com Haiku custa centavos.
- **Indicadores:**
  - falso destrutivo (tem de ficar em zero);
  - concordância com o dono;
  - % de INCERTO;
  - alerta falso por dia;
  - latência.
- **Mudança de prompt** só por PR, com bancada antes e depois, e o holdout
  renovado (casos novos entram por hash).
- **Nada aprende sozinho em produção.** Nenhum limiar muda sem PR.

## I. Infra e governança — o que o dono cria e clica

**Horário calmo**, para o que pode reiniciar o worker. Média de posts por
hora (BRT) no canal, 6 dias sem o 10.10, pela prévia:

| hora (BRT) | posts/h |
|---|---|
| 02–06 h | ~0,3 |
| 22 h | 5,0 |
| 23 h | 2,7 |
| 00 h (rodada Shopee) | 54 |
| 09 h | 44 |

Valores aproximados (a prévia não mostra posts apagados). Use **22–23 h BRT**
(ou 02–06 h). Evite 00 h, 09 h, 12 h e 17 h, e nunca 19–20 h BRT (relâmpago
ML). A medição mais nova, de 5 dias sem o 10.10, está no PR #19: 22 h 3,8;
23 h 3,6; 14 h 10,8 posts/h.

**Agora** (orçamento em etapas aprovado em 15/10):
1. **Console da Anthropic:** criar a chave **só de teste**, com limite de
   gasto (sugestão: US$ 25). Pôr no ambiente do Claude Code com o nome
   **`BRAIN_TESTE_CHAVE`** (nunca `ANTHROPIC_API_KEY`; ver E), abrir uma
   sessão nova e colar `brain/teste/PASSAGEM.md`.
   - Qualquer horário: não toca produção.
2. **Ao fim do teste:** revogar a chave.

**Depois do portão, antes do PR do serviço SOMBRA:**
3. **Railway → worker → Settings → Watch Paths:** acrescentar a linha
   `!/brain/**`. As linhas atuais ficam; o padrão é estilo gitignore e a
   negação vale depois do `**`.
   - **Horário calmo.** Se a Railway pedir para aplicar mudanças, isso vira
     um deploy com uma janela de ~9 s sem bot.
4. **Merge do PR do Brain** (só o dono). Com o passo 3 feito, o deploy do
   worker sai SKIPPED, como nos #15 e #16.
5. **Railway → New Service**, repositório `leandrotpe123-lang/bot-ofertas`,
   nome `brain`:
   - Root Directory `/brain`;
   - Start Command `python -m brain`;
   - Watch Paths `/brain/**`;
   - **sem volume**, **sem domínio público**, 1 réplica, região do worker
     (us-west2).
   - **Nunca compartilhar as variáveis do worker** com este serviço.
   - **Sem Config File Path e sem `railway.toml`.** Pela documentação da
     Railway, Config as Code está descontinuado: serviço novo não pode
     usá-lo, e os arquivos deixam de ser lidos em 2026-12-01. A
     configuração do serviço fica nos campos do painel (acima).
   - A versão do Python vem de `brain/.python-version` (`3.13`). O Railpack
     a lê dentro da Root Directory; ela já está na branch do protótipo.
6. **Variáveis do `brain`** (só os nomes):
   - `ANTHROPIC_API_KEY`: a chave de **produção** do Brain, separada da de
     teste, com limite mensal pela tabela do teste;
   - `BRAIN_MODO=SOMBRA`.
   - **Nunca um token da Railway** (`RAILWAY_TOKEN`, token de projeto ou de
     conta). O token que lê logs também faz deploy e muda variáveis.

**Para o ALERTA:**
7. **BotFather**, só na hora de configurar o ALERTA:
   - criar o bot e gerar o token **nesse momento**, colando-o direto na
     variável `TELEGRAM_BOT_TOKEN` do `brain` (nunca em chat, repo, PR ou
     log);
   - `/setjoingroups` → **Disable**: o bot não pode ser posto em grupo;
   - nunca adicionar a canal;
   - o dono manda `/start` ao bot (sem isso, o bot não consegue escrever a
     ele);
   - variável `BRAIN_DONO_CHAT_ID` no `brain`;
   - o código só responde a mensagens do `BRAIN_DONO_CHAT_ID`. Qualquer
     outro chat é ignorado sem resposta, e cada recusa conta no log.

**Para o v1 e a AÇÃO** (cada um no seu PR):
8. Variáveis da F1.1 no worker (J) e da porta de pedidos (F). São mudanças de
   produção que só o dono faz, em horário calmo.

**O que continua valendo:**
- ambientes de PR da Railway desligados;
- testes `tests/test_brain_*.py` herméticos no CI atual, sem instalar nada:
  o SDK fica atrás do adaptador e os testes usam cliente falso;
- **zero mudança** em `.github/` e `scripts/testes.sh`;
- o `requirements.txt` do worker não muda (o Brain tem `brain/requirements.txt`);
- `CLAUDE.md` só muda em PR de governança próprio, quando o dono pedir.

## J. F1 — a C1 deve vir antes da A2-OFERTA?

**Recomendação: sim, a C1 antes.** As três tarefas do Brain dependem de ligar
mensagem → post:
- (b) liberação perdida: "esta mensagem foi absorvida no post X";
- (c) fusão errada: o post X saiu por fusão em Y;
- (a) dois posts da mesma campanha: qual mensagem criou cada post.

É exatamente a C1 (`decisao.*`, `post.*`).

O que a A2-OFERTA (`oferta.identificada`) traria, a âncora de cada mensagem,
o v0 já aproxima bem pela prévia:
- o tipo vem da linha `✅ R$` do worker;
- a contagem de "sem âncora" bate com os logs (A).

Já a ligação mensagem → post não aparece em prévia nenhuma: só o worker sabe.

**Ligar a proveniência** (`API_PRIVADA_PORTA`, `API_PRIVADA_SEGREDO`):
- **Mudança de produção:** só o dono, e gera deploy (janela de ~9 s; horário
  calmo).
- **Desempenho:** os portões da D.5 valem (TL Δp99 ≤ máx(1 ms; 2%); posse
  do `lock_post` Δp99 ≤ 50 µs). Foram medidos em bancada NORMAL, com a coleta
  ligada. Depois de ligar, comparar o p99 da TL de antes e de depois em
  produção.
- **Memória:** anel de 20 mil eventos / 32 MiB (`EVENTOS_ANEL_MAX*`).
- **Segurança:** só rede privada e HMAC. O Brain lê `/v1/eventos` por cursor
  e o worker nunca abre conexão.
- **Hermeticidade:** `scripts/testes.sh` não remove as `API_PRIVADA_*`
  (F1-contrato-vigente §9). Corrigir em PR de governança **antes** de ligar
  em qualquer lugar onde a suíte rode com essas variáveis.
- **Os eventos não têm URL nem texto integral.** Por isso o v1 é eventos mais
  o texto da prévia, como decidido.

## K. Calendário honesto até 11.11 e Black Friday (27/11)

| Quando | O quê |
|---|---|
| 15–16/10 | orçamento em etapas aprovado; chave `BRAIN_TESTE_CHAVE`; sessão nova roda piloto → dev → final Haiku → Sonnet high (≤ US$ 20) |
| 19–23/10 | resultado e decisão do portão; se passar, PR do serviço SOMBRA (prévia, pré-filtro, juiz, política, log; sem DM, sem ação); cliques 3–6 |
| 26/10–01/11 | SOMBRA em produção por 7 dias; o relatório diário traz os vereditos × a marcação do dono |
| 02–08/11 | se a SOMBRA tiver zero falso destrutivo no tráfego real: PR do ALERTA (DM) |
| **11/11** | **no máximo ALERTA. Sem AÇÃO no 11.11:** ainda não há prova do caminho de ação |
| em paralelo | frente de núcleo F (porta de pedidos; REINJETAR primeiro, pelo caminho que já existe); depois, a fusão por pedido com união de links |
| 12–20/11 | análise do 11.11 (amostra grande); ajuste do ALERTA por PR. **Sem AÇÃO:** o v0 não serve para AÇÃO (§0) |
| **27/11** | realista: **ALERTA**. Nenhuma AÇÃO na Black Friday |
| depois da BF | C1 da F1 e o v1 (≤ 15 s); só então o primeiro PR de AÇÃO, **só REINJETAR**, com limite por hora. FUNDIR vem depois |

## L. Os 10 riscos principais

| # | Risco | Mitigação |
|---|---|---|
| 1 | Fusão errada apaga post bom | lista do que nunca age; portão de zero falso destrutivo; AÇÃO só por PR; worker revalida sob locks; fusão com união de links, nunca apagando conteúdo exclusivo; limite por hora |
| 2 | Injeção pelo texto do grupo | sem ferramentas; schema fechado; delimitador; ids validados; a política ignora a ação da IA |
| 3 | Sessão do Telegram derrubada | o Brain nunca usa sessão (prévia pública e bot de DM); teste de isolamento trava import de telethon, sqlite3 e worker |
| 4 | Prévia muda de formato, atrasa (até ~67 s medido) ou dá 429 | cadência educada, backoff e disjuntor; meta do v0 em p50/p95 realista; v1 por eventos; Brain fora não afeta o worker |
| 5 | Custo dispara no BF | tetos diários e limite na chave; pré-filtro (3–14% dos posts); cascata só quando precisa; tetos refeitos com o medido |
| 6 | Modelo ou API mudam | modelo e effort no log; `CONFIGS_VALIDADAS`; recusa vira INCERTO; SDK fixado; bancada a cada mudança |
| 7 | IA lenta | a IA nunca está no caminho da publicação; timeout de 30 s; fora da janela não age; meta medida por segmento |
| 8 | Rótulos inconsistentes | dono e meu separados; itens 10, 15, 17 e 20 a confirmar (áudio × escrita); holdout; dono revisa os meus |
| 9 | Merge do Brain redeploya o worker | Watch Paths `!/brain/**` antes do primeiro merge; serviço com Root `/brain` |
| 10 | Vazamento de chave ou abuso da porta de pedidos | chave só em variável, nunca impressa; nenhum token da Railway no Brain; bot só responde ao dono e não entra em grupo; porta só na rede privada, com HMAC, idempotência, limite e segredo próprio |

## M. Post apagado à mão pelo dono: risco e proposta de detecção (sem código)

**Fatos** (prévia de 15/10 e código da `main` em `670024a`):
- 29156 e 29161 dão "Post not found": o dono os apagou à mão.
- Os espelhos 381 e 384 do canal de cupons também dão "Post not found".
  Não conferi nos logs quem os apagou. Hipótese: o dono, à mão.
- O worker só escuta exclusão nas **fontes**:
  `events.MessageDeleted(chats=fontes)` em `pipeline/origem_apagada.py:140`.
  Exclusão no canal de destino não chega a handler nenhum.
- O espelho de cupons só sai quando o próprio worker remove o post
  (`pipeline/convergencia.py:303` → `espelho_cupons.removido`).
- Editar um post que sumiu cai no `except Exception` de
  `pipeline/saida.py`:
  - 3 tentativas, cada uma com uma linha `❌ edit t=`;
  - depois `[SYNC_FALHOU]` e `return True`
    (`pipeline/publicacao_aplicadores.py:206`): a mensagem conta como
    tratada.

**Risco:**
- O worker continua achando que o post existe.
- Uma mensagem nova da mesma oferta ou campanha é absorvida num post que
  ninguém vê: em silêncio (MANTER) ou com `[SYNC_FALHOU]` (edição). A
  oferta some do canal.
- O espelho fica no ar sem o principal, salvo se o dono o apagar também.
- O dono costuma apagar a **duplicata** (29156 do trio ML; 29161 dos
  "Renovados"). Então a mensagem seguinte da mesma campanha deveria ir para
  o post que ficou, e não abrir um post novo. **Decisão do dono.**

**Proposta, em ordem** (cada item é frente própria, com teste de reprodução
do 29156/29161):

1. **O worker escuta a exclusão no próprio canal.** Um handler
   `MessageDeleted` sobre `GRUPO_DESTINO`, separado do das fontes.
   - Registra o fato (`post apagado pelo dono`) só com a prova do update
     do Telegram.
   - Aplica a decisão do dono pelo caminho de remoção que já existe (o mesmo
     da convergência, que também tira o espelho). Não cria um segundo
     caminho de escrita.
   - **Hipótese a provar antes:** a exclusão feita no celular chega como
     update à sessão do worker. Updates perdidos na janela de deploy não
     chegam; por isso existe o item 2.
2. **Conferência pelo próprio worker.** De tempos em tempos, um
   `get_messages` em lote sobre os posts vivos da janela (até 100 ids por
   chamada); o que volta vazio foi apagado.
   - Cobre o update perdido.
   - Custo: uma chamada por ciclo. A cadência se decide pela taxa de
     FloodWait medida.
3. **No caminho de edição:** reconhecer `MESSAGE_ID_INVALID` como "post
   sumiu" na 1ª tentativa, sem gastar as 3 tentativas nem o backoff, e cair
   no mesmo registro do item 1.
4. **O Brain só observa:**
   - v0: "Post not found" vira registro na SOMBRA e DM no ALERTA ("post X
     apagado à mão; o worker ainda o tem como vivo; espelho Y no ar ou
     apagado");
   - nunca age;
   - no v1, no máximo um pedido de **conferência** ao worker, que
     revalida com `get_messages` sob os locks normais.

**Recomendação:** 1 + 2 no worker, e o Brain só alertando. Até lá, o
relatório diário lista os posts "Post not found" e os espelhos deles.

## Pendências do dono

1. ~~Confirmar os itens 10, 15, 17 e 20.~~ Resolvido pela R1–R3 (E).
2. **F05, F33, F41, F49, F50 e E24:** esperam a sua marcação. Estão fora do
   teste.
3. ~~Aprovar o orçamento do teste.~~ Aprovado em etapas (E). Falta **criar
   a chave `BRAIN_TESTE_CHAVE`**.
4. **Post apagado à mão (M):** decidir o que a mensagem seguinte da mesma
   campanha faz (vai para o post que ficou, ou abre post novo) e se o
   espelho sai junto.
5. **Opcional:** revisar os 63 rótulos meus no `casos.json`
   (`fonte_rotulo: "claude"`).
