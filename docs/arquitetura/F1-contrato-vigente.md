# F1: contrato vigente e estado

Ponto de partida de toda sessão que for trabalhar na F1 (proveniência).

> **Regra confirmada pelo dono (14/10/2026):** toda fatia que muda o contrato
> atualiza este arquivo **no mesmo PR** da fatia.

> Situação em 14/10/2026, com a `main` em `e5b939d` (merge do PR #14). O CI do
> push nesse commit passou (run 38516089844) e o deploy do worker saiu com
> SUCCESS (Railway, deploy de 14/10 05:20 UTC, commit `e5b939d`).
>
> **Como ler este documento.** A fonte de verdade, nesta ordem:
> 1. **código e testes da `main`**;
> 2. **este documento**;
> 3. a descrição de PRs antigos.
>
> O texto original do contrato da F1.2 (seções A.x, B.x e D.5) **não está no
> repositório** e não se recupera palavra por palavra. Ele ficou em conversas
> anteriores, das quais só restam resumos. O que vem abaixo foi reconstruído
> a partir do código, dos testes, das mensagens de commit e das **decisões
> registradas do dono**. Se o dono tiver o texto original, ele complementa
> este; em conflito, vale a decisão mais recente do dono.

## 1. O que é a F1
- **F1.1:** anel de eventos em memória mais API privada só de leitura.
  - Em produção está **dormente**: o serviço não tem as variáveis
    `API_PRIVADA_*` (só os nomes foram consultados).
  - Por isso a **proveniência está desligada em produção**, e a ausência de
    evento não prova nada.
- **F1.2:** proveniência, isto é, eventos estruturados e versionados
  emitidos pelo worker.
  - Serve **só para observação, nunca como regra**. Desligada, não roda nada.
  - É a base do contrato que o Brain vai ler (CLAUDE.md §9.2); o grampo em log
    é só ponte.
- Peças:
  - `eventos/coleta.py`: a única porta dos pontos da F1.2, com as garantias
    C1–C7 no cabeçalho (preguiçosa, blindada, pura, segura, limitada,
    execução, adiamento);
  - `eventos/catalogo.py`: catálogo v1;
  - `eventos/anel.py`, `eventos/api_privada.py` e `eventos.emitir`
    (`eventos/__init__.py`, a porta da F1.1, usada só pelos `processo.*`).

## 2. Glossário das siglas citadas no código
| Sigla | Significado |
|---|---|
| **A.3** | Garantias da coleta: o mesmo C1–C7 do cabeçalho de `coleta.py`. A.3.6 é o caso do coletor que lê uma sentinela definida antes do `try`. |
| **A.5** | Vocabulário de prova, em minúsculas. Só `confirmado` (e `aplicada`, na mídia) confirma. `nao_verificavel` **nunca** equivale a gravado. |
| **A.6** | Raízes de execução, um conjunto fechado (`tipo_execucao`). `MENSAGEM` nasce em `processar`. `EXCLUSAO` nasce por id apagado, em `origem_apagada.apagadas`. `MANUTENCAO` é a retomada de remoções no boot. |
| **A.8** | Nada é emitido sob lock; a exceção é o próprio `enviar`. Emite-se depois do log de sempre e fora de todo lock. |
| **A.9** | Regras do catálogo. Acrescentar valor sobe a versão **menor**; renomear ou remover sobe a **maior**. Prova em minúsculas, os outros enums em MAIÚSCULAS. Os LOCAIS são registrados pela frente que liga o ponto. |
| **B.1 / B.2** (com ponto) | Tabela de campos por evento (B.1) e prova Telegram/banco por evento (B.2). |
| **B1–B5** (sem ponto) | Grupo B, mudanças de contrato **sem** mudança de comportamento: B1 banco · B2 Telegram · **B3 normalização e desfecho por link** · B4 saúde das plataformas · B5 via da exclusão (**feito**). |
| **C1** | Núcleo: ORIGEM_APAGADA/ORIGEM_JA_PUBLICADA decididas sob lock, REDIRECIONAMENTO_ESGOTADO, `decisao.*`, `post.*`. **Não iniciado.** |
| **D1** | A A2 foi dividida em A2-E (entrada), A2-R (recuperação) e **A2-OFERTA** (`oferta.*`, ver §5). |
| **D.5** | Portões de desempenho, com as fontes em §2.1. |
| **N1/N2** | Tasks de fundo nascem **sem exec**: `completude._recuperar` e `sucessao._rodar` (A2-R, F16). |
| **"decisões 1 a 4 do dono"** | As 4 decisões da fatia da EXCLUSÃO (§4). |

### 2.1 D.5: os limites e de onde vêm
Os limites não estão escritos em nenhum arquivo do repositório; a fonte são
as mensagens dos commits das fatias que os aplicaram.

| Portão | Limite | Fonte |
|---|---|---|
| TL (tempo de linha, só quando a frente toca a TL) | Δp99 ≤ **máx(1 ms; 2%)** da TL base. **NORMAL é portão**; **ESTRESSE só entra no relatório** | commit `2e66b51` (A2-E, PR #8): "limite = máx(1 ms; 0,02 × 16,604 ms) = 1,000 ms", "bench SEM/NORMAL/ESTRESSE … portão NORMAL", "ESTRESSE, só reporte" |
| Posse do `lock_post` | Δp99 (NORMAL − SEM) ≤ **50 µs** | commit `2e66b51` ("posse do lock_post Δp99 = +4,6 µs (limite 50 µs)") e commit `73ef155` (A2-R, PR #9: "Δp99 da posse do lock_post NORMAL − SEM = −8,8 µs (limite 50 µs)"; "ESTRESSE −28,4 µs, só reporte") |
| Custo desligado | ≤ **0,5 µs por ponto** | commit `2e66b51` ("desligado: emitir_de 0,262 µs e execução 0,463 µs por ponto (limite 0,5 µs)") e commit `73ef155` ("anel desligado, pior ponto 335 ns (limite 500 ns)") |

**Exceção autorizada pelo dono, só no ponto da raiz EXCLUSAO:** mediu
**347–594 ns** conforme o estado da máquina (pior 594 ns). O piso é a própria
`execucao(tipo=…)` da A1 (~460 ns no estado lento), dívida preexistente da A1,
registrada. Fonte: commit `ff89925` e o corpo do PR #10 ("EXCEÇÃO AUTORIZADA
pelo dono só neste ponto: a raiz EXCLUSAO mediu 347–594 ns"). No resto vale
0,5 µs.

## 3. O que já está na `main`
| Fatia | Conteúdo |
|---|---|
| F1.1 | anel + API privada (dormente) + os 4 `processo.*` em `main.py` |
| A1 | `coleta.py` e o catálogo (só infraestrutura) |
| A2-E | funil de entrada: 16 LOCAIS (`origem.recebida`, `origem.descartada`) |
| A2-R | completude e sucessão: 11 LOCAIS (`origem.buraco`, `origem.recuperada`, `origem.recuperacao_falhou`, `post.lideranca_transferida`, `post.sucessao_pendente`). PR #11 corrigiu a bancada CANCELA_BUSCA |
| EXCLUSÃO | PR #12: `origem.apagada`, raiz EXCLUSAO, via B5 e raiz MANUTENCAO (1 LOCAL). Já sai com a linhagem corrigida |
| Linhagem das tasks | PR #13 (o título do PR diz "F1.2-A2-O"; ver §5): remoção da fusão e espelho de cupons nascem em `contextvars.Context()` vazio |
| Documento do contrato | PR #14: este arquivo |

Números (conferidos em `eventos/catalogo.py` e no código da `main`):
- catálogo `VERSAO 1`, 43 TIPOS e 29 LOCAIS (A1 1 + A2-E 16 + A2-R 11 + EXCLUSÃO 1);
- **13 tipos com emissor real hoje:**
  - os 8 de `origem.*` e `post.*` da tabela acima (6 `origem.*` e 2 `post.*`),
    por `emitir_de`;
  - `execucao.fim`, pela própria coleta (`eventos/coleta.py`);
  - os 4 `processo.*`, por `eventos.emitir` em `main.py`:
    `processo.iniciado` (no boot, depois de instalar o registro),
    `processo.online` (a cada ciclo de conexão, logo depois do log
    `🚀 FOGUETÃO — ONLINE`), `processo.vida` (laço periódico da task da API
    privada) e `processo.encerrando` (no sinal de parada);
- **ainda sem emissor:** `oferta.*`, `decisao.*`, os outros `post.*`,
  `familia.*`, `duplicata.*`, `espelho.*`, `canal.*`, `plataforma.*` e
  `fonte.consultada`.

## 4. Decisões vigentes, e o que foi SUBSTITUÍDO
**Atenção:** o PR #10 e as decisões originais da EXCLUSÃO têm regras que **não valem mais**.

**Fonte da decisão 1/4 original:** uma mensagem do dono de 08/10/2026, **fora
do repositório**, com a regra "não introduzir `contextvars.Context` vazio ou
qualquer mecanismo novo de isolamento". Os PRs #12 e #13 a substituíram, por
decisão do dono. A "opção 1" (comando do dono) e a "estratégia A" / "Decisão
A" (corpo do PR #13) são **a mesma coisa**: o contexto vazio.

| Tema | Regra que vale hoje | Substituiu |
|---|---|---|
| Raiz EXCLUSAO (dec. 1/4) | Uma execução EXCLUSAO **por id**, aberta em `apagadas` antes de `_uma`, nas vias TELEGRAM e SUCESSAO. `origem_apagada.agendar` não abre execução. | — |
| Remoção física e sucessão | São **tasks próprias**. São agendadas **depois** que a execução EXCLUSAO fecha, sem await no meio, e **nascem sem exec**. A MANUTENCAO usa `fora.run(convergencia.retomar_remocoes)` (`main.py`). Critério do dono: *nenhum evento de uma execução surge depois do `execucao.fim` dela*. | Dec. 1/4 e 4/4 ("a remoção herda a execução"). Substituídas pelo **PR #12**. |
| Isolamento das tasks independentes | `convergencia.agendar_remocao` e `espelho_cupons._tarefa` criam a task com `context=contextvars.Context()`. ContextVar nova no processo **obriga a reavaliar** isso; `tests/test_eventos_tarefas.py` (E03/L08) quebra. | Dec. 1/4 ("não introduzir Context vazio"). Substituída pelo **PR #13**, por decisão explícita do dono (opção 1 = estratégia A). |
| `origem.apagada` (dec. 2/4) | As 7 situações com prova. `telegram=nao_tocado` sempre. `banco`: `confirmado` (MORTO, MANTIDO, SEM_POST, JA_REMOVIDO), `falhou_sem_escrita` (ERRO), `nao_aplicavel` (SEM_VINCULO, MUDOU). Exceção inesperada em `_uma` não gera evento, e a execução fecha em ERRO (`marcar_desfecho`). | — |
| Ponto de emissão (dec. 3/4) | Um único LOCAL em `apagadas`, depois de `_uma`, fora de todo lock e depois do log. `restam`: MANTIDO = n, MORTO = 0, nas outras = null. | — |
| B5 e MANUTENCAO (dec. 4/4) | Tudo em `main.py`: `functools.partial(origem_apagada.apagadas, via="SUCESSAO")` e `with eventos.execucao(tipo="MANUTENCAO")`. `sucessao.py` e `convergencia.py` não sabem da via. | — |
| Contexto vazio × raiz própria | Vale A (contexto vazio). C (raiz própria por remoção e por espelho, com causa por post) é **proposta não aprovada**: mudaria catálogo e contrato. | — |

## 5. Nomes: A2-OFERTA e "linhagem das tasks"
O nome "A2-O" foi usado para duas coisas diferentes. Decisão do dono
(14/10/2026):

1. **A2-OFERTA** é a fatia original do plano (D1): `oferta.normalizada` e
   `oferta.identificada`, junto com a B3 ou depois dela. Em 08/10 o dono fixou
   a ordem **1. EXCLUSÃO → 2. B3 → 3. A2-OFERTA** e manteve a A2-OFERTA
   bloqueada até a B3.
2. **"Linhagem das tasks"** é o PR #13 (título "F1.2-A2-O", branch
   `f1.2-a2-o`): uma frente aberta pelo dono em 13/10 sobre a linhagem das
   tasks de fundo. Não emite `oferta.*` nem toca a B3.

**Consequência:** a **B3 e os eventos `oferta.*` continuam por fazer**. O nome
"A2-O" que aparece no título, na branch e no commit `06e860b` do PR #13 se lê
como "linhagem das tasks".

## 6. Próxima fatia registrada
**B3: normalização e desfecho por link.** É mudança de contrato sem mudança de comportamento.
- O catálogo já tem os enums `desfecho_link` (CONVERTIDO, PRESERVADO,
  SEM_CONVERSAO, BLOQUEADO, EXPANSAO_FALHOU, SEM_PLATAFORMA, AUSENTE, EXCECAO)
  e `motivo_url_protegida`.
- **Os campos exatos da B3** e as linhas B.1 de `oferta.*` **não estão
  registrados**. Antes de qualquer código, propor o contrato (eventos, campos,
  LOCAIS, prova e ponto de emissão) e o dono aprovar.
- Normalização, identidade e afiliação são **núcleo** (CLAUDE.md §5): só em
  frente própria, com teste de reprodução.
- Depois da B3 vem a **A2-OFERTA** (`oferta.normalizada` e
  `oferta.identificada`). C1 e os demais grupos ainda não têm ordem definida.

## 7. Invariantes da F1.2 (quebrar = PARAR)
- **Execuções:**
  - cada execução tem **exatamente um** `execucao.fim`;
  - nenhum evento com `exec` ou `exec_pai` de uma execução já encerrada;
  - `exec_pai` não representa causa histórica.
- **Fila:** é a dona da MENSAGEM (`transferir_execucao`).
- **Tasks independentes:** nascem sem exec e com contexto vazio.
- **Done-callbacks:** só limpam estrutura. Eles rodam no contexto de quem os
  registrou, então nenhum pode emitir.
- **ContextVar:** só as 2 da coleta (`_EXEC`, `_ADIAMENTO`).
- **Importadores de `eventos`:** lista fechada (`test_eventos_main` 07):
  - `main.py`;
  - `pipeline/completude.py`, `pipeline/sucessao.py` e
    `pipeline/origem_apagada.py`;
  - `pipeline/orchestrator.py`, `pipeline/orchestrator_fila.py` e
    `pipeline/orchestrator_pipeline.py`.
- **Emissão:** nos pontos da F1.2, só por
  `emitir_de(tipo, lambda: (corr, dados), local=...)`. A exceção são os 4
  `processo.*` da F1.1, emitidos por `eventos.emitir` em `main.py` (§3).
  - O coletor é uma lambda pura; no caminho quente, use argumentos default,
    sem célula de closure.
  - `emitir_de` e `eventos.emitir` nunca levantam.
  - `test_eventos_contrato.py` confere tipo, LOCAL e enum.
- **Segurança:**
  - nada de URL crua (só `representar_url`) nem de texto integral;
  - exceção só pelo nome da classe;
  - nenhuma credencial;
  - nenhum `str(obj)` de objeto do Telethon.
- **Desligada:** a coleta não muda comportamento. O diferencial
  desligado/ligado/sabotado tem de dar idêntico.

## 8. Como o dono conduz cada frente (o padrão que funcionou)
0. **Branch nova por frente e PR em draft.** O PR #14 foi a exceção (branch
   de sessão reaproveitada e PR aberto fora de draft).
1. **Fase 0, só leitura:** estado real (SHAs, PRs, CI, deploy) e reconstrução do
   contrato. **Regra zero:** não inventar evento, campo ou LOCAL; se estiver
   ambíguo, PARAR e perguntar.
2. **Testes antes do código:** a bancada tem de **cair na `main`** e passar na
   candidata. A falha na `main` é classificada por script.
3. **Mudança mínima** no módulo dono. Sem carona e sem afrouxar teste fechado
   sem justificativa semântica.
4. **Validação:**
   - `bash scripts/testes.sh` 3× (pytest não conta);
   - focados, `compileall` e `git diff --check`;
   - mutantes, **todos mortos**;
   - diferencial desligado/ligado/sabotado;
   - repetições com coleta de tasks vivas e exceções;
   - D.5 quando aplicável (§2.1).
5. **Entrega:**
   - commit único, em português (causa, mudança, evidência);
   - push e PR **draft** só com autorização explícita;
   - conferir o CI no SHA exato;
   - **este arquivo atualizado no mesmo PR**, se a fatia muda contrato.
   - **Merge e deploy: só o dono.**

## 9. Pendências e esclarecimentos (14/10/2026)
- **Texto original do contrato F1.2:** não está no repositório. Este documento
  é a reconstrução; o texto verbatim não está disponível.
- **Próxima fatia:** B3 → A2-OFERTA (§5 e §6).
- **PR #10:** **fechado sem merge** em 14/10/2026, 05:48 UTC. Foi substituído
  pelo #12 e carrega a linhagem antiga; não vale como referência de regra.
- **CI do push em `e5b939d`:** **success** (run 38516089844, 14/10 05:21 UTC).
- **`ResourceWarning` em `test_eventos_exclusao`:** já existia antes do #13.
  Está na linha 697 na `main` atual (um `open()` sem fechar). Correção separada.
- **Hermeticidade de `scripts/testes.sh` — verificado:** o script **não**
  remove `API_PRIVADA_PORTA`, `API_PRIVADA_SEGREDO`, `EVENTOS_ANEL_MAX`,
  `EVENTOS_ANEL_MAX_BYTES` e `RAILWAY_TCP_APPLICATION_PORT`, todas lidas em
  `config.py`. Corrigir é PR de governança próprio, quando o dono pedir.
- **Armadilha de auditoria de logs (Railway):** o filtro `"[OK]"`, com
  colchete, volta vazio na busca de logs mesmo com linhas `🚀 [OK]` no log
  (auditoria de 14/10). Toda contagem de "zero ocorrências" usa filtro **sem
  colchete** e **controle positivo** (um filtro que comprovadamente acha
  linhas na mesma janela).
- **Etapa 2 da F0 (conta de máquina):** **decisão pendente do dono**
  (`docs/governanca/F0.md` §3 e §7).
