# Campanha por página de resgate — causa raiz, regra em sombra e retroativa

Frente A (28/10/2026). **Só sombra**: nada aqui publica, edita, apaga, funde
ou muda estado. A regra só sai da sombra com revisão explícita do dono.

## 1. O caso real (logs da Railway × prévia pública dos canais)

| UTC 28/10 | Mensagem | Título | Identidade logada | Post | Canal de cupons |
|---|---|---|---|---|---|
| 02:59:18 | Promotom 112024 | "Cupons Shopee" | `shopee|cupb|geral` | 31943 | sim (cópia) |
| 03:00:02 | Fada 18042 | "🚨 Começou! Esquenta 11.11 Shopee" | `shopee|url|https://shopee.com.br/m/cupom-de-desconto` | **31945 (novo)** | não |
| 03:00:15 | Samuel 123470 | "🔥 Começou! Esquenta 11.11 Shopee" | `shopee|url|…/m/cupom-de-desconto` | DUP_SILENCIOSO no 31945 | — |
| 03:00:17 | — | — | `🗑 [APAGADO_DONO] post:31943 … sem_par` | o dono apagou o 31943 | a cópia saiu junto |

Os posts 31943 e 31945 publicaram o **mesmo** link, `s.shopee.com.br/AAHdNLxGSX`.
A Shopee dá um link publicado por destino (cache por destino em
`plataformas/shopee`). O link 1 da Promotom (`2B5snGi9NX`) já tinha virado
`AAHdNLxGSX` no post 30602 (21/10).

## 2. Causa raiz (arquivos, funções, condições)

1. **A natureza vem só do TÍTULO.** O caminho é `natureza.eh_entidade_cupom` →
   `assunto_especie._eh_post_cupom`, caso (a): `_cupom_e_sujeito` lê só a
   1ª linha.
   - "Cupons Shopee" dá natureza **cupom**: `Evidencias.entidade_cupom=True`,
     e `resolucao_identidade.resolver` devolve `shopee|cupb|geral`.
   - "Começou! Esquenta 11.11 Shopee" dá natureza **evento**. O "Resgate todos
     os cupons aqui" está na 3ª linha e não conta. O fallback devolve
     `shopee|url|<ancora_url>` (`normalizacao_identidade.derivar_ancora_url`,
     a 1ª canônica).
2. **Nenhuma chave em comum, então nenhuma família.**
   - `familia.post_da_familia` não acha o 31943.
   - `familia.lancamento_cupom` não olha: `cupom_lancamento.candidato` exige
     que todas as âncoras sejam `cupb`.
   - Resultado: post novo.
3. **O canal de cupons está acoplado à identidade.**
   `espelho_cupons.vai_para_cupons` = `resolucao_identidade.eh_identidade_cupom(chaves)`.
   Uma chave `url|…` nunca vai ao canal de cupons, e por isso o 31945 ficou só
   no principal.
4. **O #32 não causou nada disso.** O #32 tocou `espelho_cupons`,
   `publicacao_aplicadores`, `saida`, `medicao_edicao` e `espera_telethon`.
   Natureza, identidade, família e decisão ficaram intactas. A auditoria
   offline do código da `main` reproduz as três decisões de produção.

## 3. A regra proposta (`pipeline/campanha_resgate.py`, pura)

Duas mensagens/posts são a **mesma campanha** quando valem **todas** as
condições abaixo. Nunca é "mesmo link = mesma campanha".

1. Os dois lados estão **sem âncora forte** (produto, cupom com código,
   destino) e são da mesma plataforma.
2. A canônica que a normalização já deriva (`norm.ancora_url`) é uma página
   **declarada** pela plataforma como página de resgate de cupons. A
   declaração é a capacidade `Plataforma.paginas_resgate`; hoje existe só
   `https://shopee.com.br/m/cupom-de-desconto`.
3. A página é o link **principal** dos dois lados: o 1º link do candidato e o
   1º link publicado do post vivo são o mesmo link publicado.
4. As fontes são diferentes e o post está dentro do limite superior do
   lançamento (`cupom_lancamento.pode_mudar`, a mesma régua da regra de cupom).
5. Nome e benefício são compatíveis (`cupom_lancamento.compativeis`). Conflito
   nunca junta.
6. Há **exatamente um** post assim. Com dois ou mais, a decisão é INCERTO
   (ambíguo), que é o comportamento de hoje.

**Relação com a regra de cupom sem código (ATIVO).** Nos casos em que ela
fica em INCERTO por `sem_beneficio`, a página como link principal dos dois
lados é a prova que faltava, e a sombra junta. Há dois casos reais, e nos
dois o dono apagou o post duplicado: 17/10 (29546) e 28/10 (31943).

Onde a regra de cupom decide NOVA, esta regra **nunca** junta. Os três
casos de NOVA em `cupom_lancamento._relacao` são `mesma_fonte`,
`nome_conflitante` e `beneficio_diferente`; aqui eles caem em
`pode_mudar` ou em `compativeis` = conflito.

Onde a regra de cupom JUNTA (em ATIVO), a família já é o post escolhido e
esta regra não troca.

Com família já escolhida pelo pipeline, a regra **nunca troca de post**:
- se a família já tem a página, o resultado é MANTER `ja_no_mesmo_post`;
- se não tem, é INCERTO `familia_de_hoje_diferente`.

A regra não faz chamada de rede nova: usa a normalização e o link publicado
que já existem.

**Canal de cupons, separado da natureza.** Um post cujo link principal é a
página declarada é elegível ao canal de cupons mesmo sem "cupom" no título.
Na junção, vale o canal do post. "Elegível ao canal de cupons" é
roteamento, como o `eh_identidade_cupom` de hoje. **Não** significa que o
cupom vale para a conta de afiliado do dono: o CLAUDE.md §9.1 segue sem
implementação.

**Sombra** (`pipeline/campanha_resgate_sombra.py`):
- Chamada em `publicacao._enviar_inner` logo depois de
  `familia.lancamento_cupom`, sob os locks ORIGEM → IDENTIDADE, só para
  mensagem nova.
- Só lê o estado e devolve sempre `None`. Erro vira uma linha
  `❌ [RESGATE_SOMBRA] <Classe>` e mais nada (nem o log do erro escapa).
- Escreve uma linha só quando a regra junta, duvida, mudaria o canal ou vê
  outro post com a mesma página:

```
🎟 [RESGATE_SOMBRA] msg=(chat,id) acao=JUNTA|MANTER|INCERTO post:<id> motivo=…
   Δ=…min pagina=<canônica> hoje=post:<id>|post_novo ident_atual=[…]
   ident_proposta=[…, '<plat>|resgate|<página>'] destino_hoje=…
   destino_previsto=… nome=(cand|post) beneficio=(cand|post) outros=[…]
```

Custo medido:
- fora da regra (quase todas as mensagens): p50 1,5 µs, p99 4,7 µs;
- caminho completo (página declarada como 1º link): p50 0,3 ms, p99 0,7 ms.

## 4. Corpus real e resultado por caso

Fontes dos dados:
- textos: prévia pública t.me/s, HTML convertido para o markdown do Telethon;
- decisões de hoje: logs da Railway (`🚀 [OK]`, `🧭 TL … DECISAO`, `CUPOM_*`,
  `APAGADO_DONO`);
- link publicado: o alinhamento post × fonte.

A canônica só é afirmada com prova: a identidade logada, ou o link publicado
`AAHdNLxGSX`, que é a página. O resto é lacuna.

| # | Texto / origem (UTC) | Identidade hoje | Canal de cupons hoje | Post hoje | Decisão proposta | Justificativa | Teste |
|---|---|---|---|---|---|---|---|
| 1 | Promotom 112024 "Cupons Shopee", 28/10 02:59 | `cupb|geral` | sim | 31943 | MANTER (1º do lançamento) | página como 1º link, sem par vivo | 03 |
| 2 | Fada 18042 "Começou! Esquenta 11.11", 03:00 | `url|…/m/cupom-de-desconto` | não | 31945 novo | **JUNTA no 31943**, principal + cupons | mesma página como 1º link dos dois lados; Δ 0,7 min; fontes diferentes; nome só de um lado (compatível) | 03 |
| 3 | Samuel 123470, 03:00:15 | `url|…` | não | DUP no 31945 | MANTER `ja_no_mesmo_post`, outros=[31943] | família de hoje já tem a página; registra o risco | 03 |
| 4 | fumotom 42674, produto com a página como 1º link, 03:01 | `shopee|1930141978.58270173531` | não | 31947 | fora (âncora forte) | produto tem dono; 186 casos assim em 7 dias | 03, 06 |
| 5 | Promotom 111508 × Fada 17870 "Novo Cupom Shopee VIP", 17/10 (corpus de `test_post_apagado`) | `cupb|geral` × `cupb|vip` | sim × sim | 29546 × 29549; hoje em ATIVO: CUPOM_INCERTO sem_beneficio, 2 posts | **JUNTA** no da Promotom | mesma página como 1º link; o dono apagou o 29546 | 03b |
| 6 | Promotom 111724 (21/10 12:03) × Fada 17927 "Dia de Cupons" (12:00) | `cupb|geral` × `cupb|dia` | sim × sim | 30602 × 30591 (CUPOM_INCERTO sem_beneficio) | abstém | o link da Fada leva a outra página (`4LK2yNECtS`, canônica lacuna) | 05 |
| 7 | Promotom 111784 (3 links; 1º `LoB6msmOY`) → Fada 17953 → Samuel, 22/10 | `cupb|geral` | sim | 30837 (a Fada EVOLUI) | abstém (MANTER sem linha) | a página não é o 1º link do post; o cupb já junta hoje | 05 |
| 8 | Promotom 111915 → Fada 17997 → Samuel → Fada 17998 "Tech" → Samuel, 26/10 | `cupb|geral`, `cupb|tech` | sim | 31451, 31463 | abstém | no Tech a página é o 2º link (campanha própria) | 02, 05 |
| 9 | Promotom 111992 → Fada 18025 30% → Samuel → Fada 18028 (Δ 37 min), 27/10 | `cupb|geral` | sim | 31770, 31785 | abstém | 1º link do 31770 é outro (`60SGP3K099`); acima do superior | 05 |
| 10 | Fada 18038 "É AMANHÃ! ESQUENTA 11.11" | `url|…/m/11-11` | não | 31930 | fora | página de evento, não declarada | 09 |
| 11 | Fada 18042 sozinha (variação: sem a Promotom) | `url|…` | não | novo | MANTER, **previsto principal + cupons** | elegível sem "cupom" no título | 09 |
| 12 | Samuel 122540/122578 "Pessoal, pra algumas contas não está aparecendo…" | `cupb|geral` | sim | 30697 / 30743 | fora | o link leva a `LoB6msmOY`, não à página | retro |

Variações sintéticas, todas **marcadas** no teste 02, para pares que o corpus
não tem:
- nome em conflito: "VIP" × "Esquenta";
- benefício em conflito: 30% × 10%;
- dois pares (ambíguo);
- limite superior exato.

O teste 06 também troca a ordem (produto antes) e o teste 08 traz as falhas de
link.

## 5. Retroativa de 7 dias (21/10 11:02 → 28/10, a retenção da Railway)

**Método.**
- Cada uma das 15 janelas Shopee de cupom/evento sem âncora forte é
  reexecutada pelo caminho real (normalizar → enviar), na ordem e com os Δ
  reais. As janelas são as dos 45 posts fracos da Shopee; as lives
  (`live.shopee.com.br`) estão fora da regra por construção.
- As mensagens que entraram em cada post vêm das linhas `🧭 TL … DECISAO`.
- A réplica refaz a contagem de posts de produção em 14 das 15 janelas. A
  exceção é 21/10 14:02, onde houve exclusão pelo dono e edição, que a
  réplica não refaz.

| Resultado | Quantidade |
|---|---|
| JUNTA | **1**: Fada 18042 → post da Promotom (o caso real) |
| MANTER com risco registrado | 1: Samuel 123470 (outros = o post da Promotom) |
| INCERTO | 0 |
| Mudança de canal | 1: a própria Fada 18042 (principal → principal + cupons) |
| Falso positivo | **0** |
| Erro da sombra | 0 |

A lacuna dos links do Samuel não muda o resultado: a mesma retroativa com a
**hipótese** "todo link do Samuel em 'Resgate… aqui' é a página" dá as mesmas
2 linhas. O teste 05 repete isso na noite de 22/10.

**Candidatos a falso positivo examinados um a um** (todos ficam fora):
- 186 produtos do fumotom com a página como 1º link: âncora forte;
- Tech 31463 (26/10 03:02) × 31451 (Δ 3 min): página como 2º link;
- Promotom com 3 links (22, 23, 24, 25, 26 e 27/10): 1º link `LoB6msmOY`;
- 30591 × 30602 (21/10): páginas diferentes;
- 31770 × 31785 (27/10): 1º link diferente e Δ 37 min;
- avisos do Samuel (21/10): outra página.

## 6. Onde a regra se abstém (comportamento de hoje fica)

- **A página não é o 1º link.** É o caso do "Cupons Shopee" de 3 links da
  Promotom (1º `LoB6msmOY`). Hoje o `cupb|geral` já junta essas noites.
- **A canônica não é declarada:** `LoB6msmOY` ("Resgatar todos (12)" do Dia
  dos Cupons) e `4LK2yNECtS` ("Dia de Cupons"). As canônicas deles não são
  observáveis aqui, porque a rede da Shopee está bloqueada neste ambiente.
  Para declará-las é preciso a canônica vinda da produção: uma linha
  `url|…` no log ou o `links_cache`.
- A falha de reconhecimento nunca junta. Isso vale para: link sem conversão,
  canônica ainda encurtada, outra rota, e o mesmo destino com link publicado
  diferente (teste 08).
- Fonte igual, acima do limite superior, nome ou benefício em conflito, ou
  dois pares possíveis.

## 7. Riscos residuais

1. **O contrato depende de "um link publicado por destino".** Se a Shopee ou
   o cache passarem a dar links diferentes para o mesmo destino, a regra só
   deixa de juntar (o lado conservador). Isso está no teste 08.
2. **Página nova de resgate.** Só entra por declaração na plataforma, com
   prova. Até lá, abstenção.
3. **Junção onde a regra de cupom ficaria em INCERTO `sem_beneficio`**
   (caso 5).
   Também desempata o `ambiguo` da regra de cupom quando só um dos posts
   tem a página como link principal. Não houve caso assim nos 7 dias.
4. **Junção "compatível" sem benefício dos dois lados.** O caso real é o seco
   da Promotom × "Esquenta". É a mesma régua da regra de cupom que já está em
   ATIVO, e a sombra registra `nome=` e `beneficio=` para revisão.
5. **Exclusão do dono depois de uma junção.** Se a regra estivesse ativa, a
   Fada teria entrado no 31943 às 03:00:06, antes de o dono apagá-lo às
   03:00:17. Pela decisão M, apagar o post apaga a campanha inteira, sem
   ressurreição (teste 07). A ativação precisa da revisão do dono sobre isso.
6. **Tudo foi medido em 7 dias** (a retenção). Antes de ativar, a sombra deve
   rodar em produção e as linhas `🎟 [RESGATE_SOMBRA]` devem ser revisadas
   noite a noite.

## 8. Para sair da sombra (não feito; precisa de revisão explícita)

A ativação seria: `familia.lancamento_cupom` (ou um passo vizinho, em frente
própria) devolver o post da decisão JUNTA, e o canal seguir
`destino_previsto`.

O teste 10 já mostra, **só como simulação de teste**, que os aplicadores de
hoje dariam 1 post no principal e 1 cópia no canal de cupons para o caso
real.
