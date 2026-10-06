# FOGUETÃO — regras de arquitetura e operação

Lido automaticamente por toda sessão do Claude Code neste repositório.
Contém **somente regras**. O repositório é **público**: nenhum segredo,
token, cookie, sessão ou valor de variável de ambiente pode aparecer aqui —
nem em código, commit, PR, issue, comentário ou log.

## 1. O sistema

- Userbot do Telegram (Telethon, MTProto — conta de usuário, não Bot API) que
  lê os canais-fonte (`config.GRUPOS_ORIGEM`), converte links em links de
  afiliado e publica, edita, funde e remove posts no canal de destino
  (`config.GRUPO_DESTINO`). Post de cupom sai também no canal de cupons
  (`CANAL_CUPONS`).
- Produção: serviço `worker` na Railway (`python main.py`, uma réplica), com
  volume persistente em `/data` onde vive o SQLite (`DB_PATH`). O mesmo
  processo serve o encurtador público (`web/redirect.py`).
- A branch `main` é produção: todo commit na `main` vira deploy do worker.

## 2. Papéis — arquitetura aprovada

**Worker** — dono único da sessão do Telegram, do SQLite, dos locks e de toda
execução operacional (publicar, editar, sincronizar, fundir, remover).

**Brain** (futuro; ainda não existe) — serviço separado, de observação e
diagnóstico. Quando existir:

- nunca usa `TELEGRAM_SESSION` nem qualquer sessão MTProto da conta (a mesma
  sessão em dois processos pode ter a chave revogada pelo Telegram e derrubar
  o worker);
- nunca escreve no SQLite do worker, nem abre o arquivo;
- nunca chama o Telegram diretamente para ação operacional;
- qualquer ação é um **pedido ao worker**: o worker revalida o estado e
  adquire os locks normais (ORIGEM → IDENTIDADE → POST) antes de executar,
  pelos mesmos caminhos do pipeline;
- a IA do Brain só recomenda; quem decide é regra determinística, com lista
  fechada de ações e limites.

**Claude Code** — engenheiro. Muda código só por branch + PR + CI verde +
aprovação humana. Não faz merge, não faz deploy, não altera configuração de
produção.

**Dono (Léo)** — aprova e faz o merge dos PRs; faz os cliques de
configuração no GitHub e na Railway.

## 3. Invariantes do worker (não quebrar)

- Ordem global de locks: ORIGEM → IDENTIDADE (ordenadas) → POST (msg_id
  crescente). São `asyncio.Lock` em memória do processo (`pipeline/origem.py`,
  `pipeline/exclusao.py`) — só existem dentro do worker.
- Uma conexão SQLite por processo, serializada por mutex
  (`database_conexao.py`); acesso só via `_db()`. Em WAL, o estado é o
  diretório inteiro (`.db`, `-wal`, `-shm`).
- `_SEM_ENVIO` não é reentrante: as funções públicas de `pipeline/saida.py`
  adquirem o semáforo e as `_no_sem` não; nunca chamar uma pública de dentro
  de outra.
- `pipeline/decisao.py` é pura (sem Telegram, banco ou log). A decisão roda
  em `pipeline/publicacao.py` sob o lock do post; a execução fica nos
  aplicadores.
- `post_estado.score` descreve sempre o texto publicado.
- Fato só é registrado depois do I/O e com prova (ex.: mídia aplicada).
- Vida da oferta: `pipeline/vida_oferta.py` é a autoridade única.
- Duplicidade estrutural: `pipeline/convergencia.py` é a autoridade única;
  nunca se apaga conteúdo exclusivo.
- Vínculo de origem: invariantes I1–I7 em `pipeline/origem.py`.
- Plataformas só via `plataformas.registry`; plataforma nova é arquivo novo em
  `plataformas/`, sem tocar o núcleo.
- `config.py` é folha do grafo de imports: sem I/O no import. Handlers e
  tarefas de fundo nascem uma vez por processo (`main._preparar_processo`).
- Correção de estado preferida: reinjetar a mensagem de origem no mesmo ponto
  de entrada (`EventoRecuperado` + `processar`), como fazem
  `pipeline/sucessao.py` e `pipeline/completude.py`. Nunca criar um segundo
  caminho de escrita no canal ou no banco.

## 4. Testes

- A suíte real são os arquivos standalone `tests/test_*.py` (runner próprio
  em `tests/_harness_e5.py`). Critério: código de saída 0 em todos.
- Rodar tudo (o mesmo comando do CI): `bash scripts/testes.sh`.
- Rodar um arquivo: `python tests/test_x.py`.
- **pytest não é critério de sucesso**: o `pytest.ini` existe, mas o pytest
  não representa a suíte (os testes recebem `r` do harness, não fixture).
- Produção e CI rodam Python 3.13.
- A suíte é hermética: não usa rede externa nem variáveis de produção
  (`scripts/testes.sh` remove essas variáveis antes de rodar).
- Nunca apagar, pular, desativar ou afrouxar um teste para ficar verde.
  Teste instável se corrige na causa, com evidência.

## 5. Como mudar código

- Começar do caso real (id de post ou de mensagem, linha de log) e de um teste
  que o reproduza.
- Mudança mínima, no módulo dono da responsabilidade; sem refactor de carona.
- Os módulos do núcleo — identidade, família, decisão, publicação,
  convergência e afiliação — só mudam em frente própria, com teste de
  reprodução; nunca de carona em outra mudança.
- Commit em português explicando causa, mudança e evidência.

## 6. Git, CI e deploy — as barreiras são técnicas

- A `main` é protegida por ruleset: só recebe mudança por PR, com o check
  `testes` verde; force-push e exclusão da branch bloqueados; sem bypass.
- CI: `.github/workflows/ci.yml` roda `scripts/testes.sh` em todo PR para a
  `main` e em todo commit da `main`.
- Railway com **Wait for CI**: o deploy do worker só sai depois do CI verde
  no mesmo commit.
- `.claude/settings.json` bloqueia, no Claude Code, merge e auto-merge de PR e
  as ferramentas que alteram a Railway.
- O Claude Code age no GitHub com a identidade conectada pelo dono. Hoje é a
  mesma conta do dono, e o GitHub não distingue os dois (evidência e
  decisão pendente em `docs/governanca/F0.md`). Regra escrita aqui não é
  barreira; as barreiras são as desta seção.
- Deploy estrutural só depois que o dono aprova o PR.
- Arquivos de governança — `CLAUDE.md`, `.claude/`, `.github/`,
  `scripts/testes.sh`, `docs/governanca/` — só mudam em PR próprio, pedido
  pelo dono; nunca junto de outra mudança.

## 7. Operação

- Investigar produção é leitura: logs, deploys, métricas e nomes de variáveis.
- Logs: o bot escreve em stderr, então na Railway **toda** linha aparece com
  severidade `error`; o nível real está no texto (`| INFO |`, `| ERROR |`).
  Marcadores estáveis: `🧭 TL`, `🚀 [OK]`, `[EDITADO_OK]`, `[SYNC_FALHOU]`,
  `[FUSAO]`, `[ORIGEM_APAGADA]`, `[SUCESSAO]`.
- Variáveis de ambiente: consultar só os **nomes**; nunca ler, imprimir ou
  copiar valores.
- Deploy de serviço com volume tem uma janela curta sem bot (a Railway não
  sobrepõe deploys com volume); updates e exclusões nesse intervalo podem se
  perder.
- Ambientes de PR da Railway devem ficar **desligados**: copiariam o worker,
  com a sessão de produção, para um segundo processo.
- As funções Railway `sonda-sessao-e50` e `sonda-nav-e51` não devem ser
  tocadas (decisão do dono; a limpeza é tarefa separada).

## 8. Nunca

- usar ou copiar `TELEGRAM_SESSION`, nem rodar o bot com a sessão de produção
  fora do worker;
- push na `main`, merge, auto-merge, force-push, apagar branch protegida ou
  alterar ruleset;
- alterar a Railway de produção (deploy, redeploy, rollback, variáveis,
  serviços, volumes, domínios);
- ler, imprimir ou registrar segredo; colocar segredo em código, commit, PR,
  issue ou log;
- postar nos canais do Telegram;
- alterar a própria governança junto de outra mudança;
- desligar, pular ou afrouxar teste;
- fazer migração ou limpeza manual no banco de produção.

## 9. Requisitos arquiteturais registrados (ainda não implementados)

### 9.1 Elegibilidade de campanha e cupom

- Cupom detectado **não é** cupom elegível para o Léo. O Brain, e qualquer
  automação, não pode assumir elegibilidade a partir do texto da fonte.
- Será necessária uma fonte de verdade para: parceria, campanha, afiliado,
  cupom, link elegível e restrições.
- Hoje nenhum código verifica elegibilidade junto aos marketplaces. A
  "elegibilidade" em `plataformas/mercadolivre` trata de o link ser
  afiliável, não de o cupom valer para o Léo.

### 9.2 Contrato de eventos do Brain

- A F1 pode usar um grampo nos logs como **ponte temporária**.
- O contrato definitivo é evento estruturado e versionado, emitido pelo
  worker; nunca parsing permanente de texto ANSI/stderr.
