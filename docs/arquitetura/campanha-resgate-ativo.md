# Campanha por página de resgate — correção funcional (ATIVO atrás de flag)

Frente C. Depende do #34 (a regra em SOMBRA), que depende do #33. A flag
`config.CAMPANHA_RESGATE_MODO` nasce em `"SOMBRA"`: **com ela no padrão, nada
muda em produção** (seção 5). Ligar e desligar é PR do dono.

## 1. Causa comprovada — dois casos no mesmo dia

Fontes: logs da Railway (só leitura) e prévia pública t.me/s dos canais. Horas
em UTC, 28/10/2026.

| | 1º post | 2º post (duplicado) | Por que não juntou | O que o dono fez |
|---|---|---|---|---|
| 03:00 | 31943 — Promotom 112024 "Cupons Shopee", `shopee\|cupb\|geral`, score 3, nos dois canais | 31945 — Fada 18042 "Começou! Esquenta 11.11 Shopee", `shopee\|url\|<página>`, score 3, só no principal | nenhuma chave em comum: a natureza vem só do título, e o da Fada não tem "cupom" | apagou o 31943 e **encaminhou à mão** o 31945 ao canal de cupons (FullPromotionCupons 618, marcado "encaminhado") |
| 11:59 | 32138 — Fada 18049 "Cupons Renovados! Esquenta Shopee", `shopee\|cupb\|esquenta`, editada com o código 3SQU3NT411 (R$150 OFF em R$999), score 9 | 32139 — Promotom 112034 "Cupons Shopee", score 3 (`CUPOM_INCERTO motivo=sem_beneficio`) | a regra de cupom ficou em dúvida; a sombra do #34 nem olha post com código | **apagou o 32139** às 12:08 (`🗑 [APAGADO_DONO] post:32139 … via=TELEGRAM`) |

- **Mesmo destino:** os quatro posts publicaram o mesmo 1º link,
  `s.shopee.com.br/AAHdNLxGSX`, que é a página de resgate
  `https://shopee.com.br/m/cupom-de-desconto` (prova no #34, seção 4).
- **O que o dono fez à mão:** um post por campanha, nos dois canais, com o
  texto mais rico. Às 11:59 ficou o da Fada (score 9, com o código); às 03:00
  ficou o da Fada, encaminhado.
- **Controle (o que funciona hoje):** quando o título diz "Cupons", junta.
  Às 11:59, o Samuel 123584 entrou no 32138 pela chave `cupb|esquenta`
  (`SCORE_NAO_EVOLUI 9→3`). As outras 14 janelas de 21 a 28/10 já saem com
  um post por campanha (seção 6).

## 2. A correção

Tudo em `ATIVO`; em `SOMBRA`, nada disto acontece.

1. **Junção** (`campanha_resgate_sombra.familia`, chamada em
   `publicacao._enviar_inner` no lugar do `registrar`).
   - **Quando:** a mensagem NOVA que hoje abriria post novo (família `None`)
     e que a regra de cupom sem código não decidiu (`lancamento_cupom` não
     devolveu JUNTA nem NOVA).
   - **Para onde:** o post vivo da mesma campanha, pela regra do #34:
     - mesma página declarada como destino **principal** dos dois lados;
     - outra fonte;
     - até o SUPERIOR (30 min);
     - nome e benefício sem conflito;
     - exatamente um candidato.
   - **Inclui as junções "compatível"** (benefício ausente de um lado), que no
     #34 eram pendentes de aprovação. Os dois casos reais são desse tipo: um
     modo "só igual" corrigiria 0 de 2.
2. **Código de cupom deixa de excluir, sem nunca esconder código**
   (`campanha_resgate.codigo_preservado`). Produto e destino declarado
   continuam fora dos dois lados.
   - **Por que a conta é de score:** `familia.relacao_composicao` é None
     quando um lado só tem chave fraca, e então quem decide o texto é o score
     (`decisao.decidir`). Por isso a guarda usa a mesma régua: o score do
     candidato projetado na escala do post contra o do texto no ar, e
     `edit_count < _MAX_EDITS`.
   - **Só o post tem código:** junta só se o candidato **não** vence pelo
     score. O texto com o código fica (SCORE_NAO_EVOLUI/DUP_SILENCIOSO).
   - **Só o candidato tem código:** junta só se ele **vence** pelo score e o
     post ainda tem orçamento de edição. O texto com o código entra (EVOLUI).
   - **Códigos diferentes nos dois lados:** nunca (`codigos_distintos`).
   - **Dado ausente:** não junta.
3. **Texto mais rico: nenhum caminho novo.** Depois da junção, o texto é
   decidido pela decisão de evolução de sempre, sob o lock do post:
   - o mais rico evolui (edita principal e cópia juntos);
   - o mais pobre é absorvido (a família aprende a âncora);
   - no empate, fica o que está no ar.
4. **Trava sintética da campanha** (`campanha_resgate_sombra.mutexes`):
   `<plat>|resgate*|<página>` no conjunto de locks de identidade de
   `publicacao.enviar`.
   - **Quem pega:** só quem entra na regra. Produto nunca pega, então nunca
     atrasa os posts de produto do fumotom.
   - **Por quê:** duas mensagens da mesma campanha quase simultâneas passam a
     ser serializadas, e a segunda vê o post da primeira.
   - **Ordem global:** a mesma de sempre (ORIGEM → IDENTIDADE ordenadas →
     POST).
5. **Canal de cupons** (`espelho_cupons._de_cupons`, um predicado só para os
   quatro pontos que usavam `eh_identidade_cupom`): o post cujo único destino
   é a página declarada vai também ao canal de cupons. É o encaminhamento que
   o dono fez à mão.
   - **Por que é necessário:** sem isso, na ordem inversa (Fada primeiro), a
     junção deixaria a campanha só no principal, pior que hoje para o canal de
     cupons.
   - **Cópia apagada à mão:** continua AUSENTE e nunca volta.

## 3. Garantias (cada uma com teste)

| Garantia | Teste |
|---|---|
| SOMBRA = #34: mesmas chamadas ao Telegram, banco, decisões, locks e linhas | `test_campanha_resgate_ativo` 14; `test_campanha_resgate` (16/16, inalterado); diferencial da suíte (seção 5) |
| a junção só transforma "post novo"; família de hoje e decisão da regra de cupom nunca trocadas (inclusive a guarda de defesa) | 12 |
| nunca esconde código (os quatro ramos, dado ausente, régua igual à da decisão) | 02, 16, 05, 06 |
| campanhas distintas na mesma página nunca juntam (nome, códigos, mesma fonte, acima do SUPERIOR, outra página) | 07 |
| benefícios conflitantes nunca juntam (pela regra de cupom ou pela da campanha) | 08 |
| múltiplos candidatos: ambíguo = comportamento de hoje; um válido + um em conflito junta no válido | 09 |
| leitura que falha (vivos, post, post que some, exibida, ofertas, origens, espelho) nunca junta: INDETERMINADO em WARNING; erro interno e log que levanta não mudam a publicação | 10 |
| cópia apagada à mão (descoberta ou não) nunca volta; principal apagado pelo dono nunca é escolhido nem renasce; principal apagado ainda não descoberto não gera 2º post | 11 |
| a trava é necessária e suficiente na corrida | 13 |
| edição nunca passa pela regra (vínculo de origem) | 15 |

## 4. Antes × depois nos casos reais (testes 03–06)

| Cenário | Antes (SOMBRA) | Depois (ATIVO) | Texto que fica |
|---|---|---|---|
| 03:00 real (Promotom → Fada → Samuel) | 2 posts da campanha; a Fada só no principal | 1 post, nos dois canais | o que estava no ar (Promotom): empate 3 × 3 |
| 03:00 invertido (Fada primeiro) | 2 posts | 1 post, nos dois canais | o que estava no ar (Fada): empate |
| 11:59 real (Fada + código → Promotom → Samuel) | 2 posts nos dois canais | 1 post, nos dois canais | o da Fada (score 9, com código); Promotom absorvida |
| 11:59 invertido, código já na 1ª versão | 2 posts | 1 post: a Fada EVOLUI o post da Promotom (1 envio + 1 edição) | o da Fada (o mais rico entra) |

## 5. A flag no padrão não muda nada

- **Suíte:** `bash scripts/testes.sh` dá 46/46 (os 45 do #34 + o arquivo novo).
- **Diferencial da saída normalizada, #34 (721b9da) × esta branch:**
  - 45/45 arquivos com o mesmo código de saída;
  - **42 idênticos linha a linha**, inclusive `test_campanha_resgate` (1024
    linhas);
  - 3 diferem só em ruído já conhecido: a ordem de uma linha ⏱ por latência
    em `test_espelho_cupons`, a porta aleatória em `test_eventos_api` e
    ns/chamada em `test_eventos_main`;
  - nenhuma linha `modo=ATIVO` fora do arquivo novo.
- **Em SOMBRA, linha por linha:** `familia` chama o mesmo `registrar` do #34 e
  devolve a família de hoje; `mutexes` devolve `[]`; `_de_cupons` é
  exatamente `eh_identidade_cupom`.

## 6. Retroativa: 16 janelas reais de 21 a 28/10

Mesmas mensagens, ordem e Δ reais, pelo caminho real, com SOMBRA e com ATIVO:

- **14 janelas idênticas:** mesmo número de posts nos dois canais e mesmo post
  para cada mensagem.
- **2 mudam:** exatamente as duas que o dono corrigiu à mão (03:00 e 11:59),
  cada uma de 2 posts para 1.
- 0 erros. Nenhuma junção fora desses dois casos: o "Tech" de 26/10 continua
  separado e o "É amanhã 11.11" (página /m/11-11, não declarada) continua só
  no principal.

## 7. Limitações conhecidas

1. **Empate de score:** fica o primeiro texto. Às 03:00 isso é o da Promotom,
   e o dono preferiu o da Fada ("Começou! Esquenta 11.11"). Desempatar por
   nome de campanha é mudança no `score`/`decisao`, frente própria do núcleo.
2. **Ambíguo:** com dois posts válidos já no ar, a mensagem abre post novo,
   como hoje. A regra não resolve uma duplicata que já existe; quem funde é a
   convergência.
3. **Post apagado pelo dono:** nunca é escolhido pela regra (doutrina da
   Frente M). Se ele era o único da campanha, a próxima fonte abre post novo,
   como hoje.

   Pré-existente, visto hoje às 12:58: o post apagado segue dono da própria
   identidade por até 1 h. O "Vai sair cupom" da Fada 18055 (`cupb|geral`)
   foi absorvido pelo 32139 apagado e não saiu no canal.
4. **Âncora absorvida:** a junção deixa o post da campanha com a âncora do
   candidato (ex.: `cupb|geral`) até o fim da vida (1 h). Mensagens genéricas
   seguintes casam por sobreposição e caem na regra de cupom (NOVA por nome,
   benefício ou mesma fonte). É o mesmo de hoje quando o título já diz
   "Cupons".
5. **Estado lido antes do lock do post:** a guarda de código usa o estado
   lido antes do lock. Entre a decisão e o lock, outra mensagem pode evoluir
   o post; é a mesma janela de milissegundos da regra de cupom.
6. **Depois da junção, é a regra de sempre:** as edições da fonte absorvida
   seguem o vínculo de origem e a decisão de evolução. Uma edição mais rica
   sem código pode substituir um texto com código. É o comportamento de hoje
   em qualquer família por chave fraca: a Frente 8b só protege composição
   forte × forte.
7. **Reversão para SOMBRA:** um post vivo (≤ 1 h) só-página que tem cópia
   perde a cópia na próxima reconciliação (`deixou_de_ser_cupom`).
8. **Link publicado como prova:** herda a tensão declarada no #34. A
   igualdade de link publicado é a prova de mesmo destino (cache por destino
   da Shopee). Se o contrato quebrar, a regra só deixa de juntar.
9. **Só Shopee:** só a Shopee declara página de resgate
   (`/m/cupom-de-desconto`). Nas outras plataformas nada muda.
10. **Elegibilidade do cupom:** ir ao canal de cupons não é cupom elegível
    para a conta do dono (CLAUDE.md §9.1), e nada aqui verifica elegibilidade.

## 8. Ativação, vigilância e reversão

- **Ligar:** PR do dono trocando `CAMPANHA_RESGATE_MODO = "ATIVO"` em
  `config.py`, depois do merge do #33, do #34 e deste. Não há variável de
  ambiente.
- **Vigiar** (logs, só leitura):
  - `🎟 [RESGATE_SOMBRA] … modo=ATIVO aplicada=sim`: cada junção aplicada,
    com post, Δ, fontes, nome e benefício;
  - `aplicada=nao:<porquê>`;
  - `acao=INDETERMINADO` (WARNING);
  - `❌ [RESGATE_SOMBRA]`;
  - posts de campanha duplicados que sobrarem.
- **Reverter:** PR voltando para `"SOMBRA"`. Sem migração e sem limpeza de
  banco. A âncora absorvida morre com o post (1 h); ver o item 7 das
  limitações para a cópia de post só-página.
