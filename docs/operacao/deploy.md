# Deploy do worker: procedimento e janela sem bot

Situação em 14/10/2026, `main` em `e5b939d`. Fontes: código da `main`,
`list-deployments` da Railway (só leitura) e a **auditoria de logs da Railway
de 14/10/2026** (citada como "auditoria de 14/10").

Contexto: o serviço `worker` roda `python main.py` (`Procfile`), uma réplica,
com volume em `/data` (SQLite). A Railway tem **Wait for CI** ligado: o deploy
de um commit da `main` só sai depois do CI verde nesse commit
(`docs/governanca/F0.md` §6, passo 4).

## 1. Procedimento

1. **O dono faz o merge** do PR (CLAUDE.md §2 e §8).
2. **Não remover, não reiniciar e não fazer redeploy do deploy que está no
   ar.** A troca é da Railway (decisão do dono, 14/10).
3. **CI falhou = o deploy não sai, e a versão anterior segue no ar.** Fonte:
   documentação da Railway sobre Wait for CI; o caso nunca foi observado na
   prática.
4. **Conferência pós-deploy** (só leitura, logs da Railway):
   - o deploy novo com status `SUCCESS`;
   - a linha `🚀 FOGUETÃO — ONLINE` (`main.py`) depois do boot;
   - **0** `❌` e **0** `Traceback` desde o boot;
   - toda contagem usa filtro **sem colchete** (`OK`, nunca `[OK]`; ver §6)
     e **controle positivo**: um filtro que comprovadamente acha linhas na
     mesma janela, para provar que a busca funciona antes de aceitar um zero.

## 2. As 7 trocas auditadas

Data, commit e id vêm de `list-deployments`; a janela e as assinaturas vêm da
auditoria de 14/10. A auditoria registrou para as trocas com remoção manual
a faixa de **1m26s a 2m27s** sem bot; o valor de cada uma não foi
reproduzido aqui.

| # | Troca | Deploy criado (UTC) | Commit | Como foi | Janela sem bot | Assinatura nos logs |
|---|---|---|---|---|---|---|
| 1 | PR #8 | 07/10 11:55 | `79ed70d` | dono removeu o deploy no ar | longa (faixa 1m26s–2m27s) | `sending signal SIGTERM to container` antes do build |
| 2 | PR #9 | 08/10 04:47 | `f6d18ae` | dono removeu o deploy no ar | longa (faixa 1m26s–2m27s) | `sending signal SIGTERM to container` antes do build |
| 3 | PR #11 | 10/10 20:15 | `6f88c31` | dono removeu o deploy no ar | longa (faixa 1m26s–2m27s) | `sending signal SIGTERM to container` antes do build |
| 4 | redeploy de 13/10 (mesmo commit do #11) | 13/10 04:27 | `6f88c31` | dono removeu o deploy no ar | longa (faixa 1m26s–2m27s) | `sending signal SIGTERM to container` antes do build |
| 5 | PR #12 | 13/10 04:59 | `a382cdb` | troca feita pela Railway | ~6 s | `Stopping Container` 1 s antes de o novo subir |
| 6 | PR #13 | 14/10 03:50 | `b47bbcb` | troca feita pela Railway | ~6 s | `Stopping Container` 1 s antes de o novo subir |
| 7 | PR #14 | 14/10 05:20 | `e5b939d` | dono removeu o deploy no ar | longa (faixa 1m26s–2m27s) | `sending signal SIGTERM to container` antes do build |

Conclusões (auditoria de 14/10):
- **A causa das janelas longas era a remoção manual** do deploy no ar antes
  de o novo estar pronto. Sem ela, a troca leva ~6 s. A causa está resolvida
  pela decisão do passo 2.
- **Nunca rodaram dois workers ao mesmo tempo:** nas 7 trocas, o antigo
  registrou `👋 encerrado` (`main.py`) antes do `Starting Container` do novo.
- **O volume é o que garante um worker só** (a Railway não sobrepõe deploys
  de serviço com volume). Ele não pode sair.

## 3. O que se perde na janela

Tudo o que chega com o bot fora do ar se perde: **mensagens novas, edições e
exclusões** nos grupos de origem.

| Por quê | Fonte |
|---|---|
| O client usa `StringSession`: o estado de updates não sobrevive à troca de processo | `client.py` |
| Não há `catch_up` no repositório | busca no código da `main`: 0 ocorrências |
| A completude guarda o estado só em memória (`_canais`) e não recupera a janela | `pipeline/completude.py` |
| Exclusão feita com o bot fora do ar não chega | `pipeline/origem_apagada.py:47` |

Recuperar isso **não é agora**; ver as candidatas em §7.

## 4. Watch Paths (configuração do dono no painel da Railway)

Objetivo: commit que só mexe em documentação, testes ou governança **não**
cria deploy. O bot não lê nem importa nada dos caminhos excluídos (conferido
no código: só há menções em comentários a `testes_ml/FORMATOS.md`), e
`Procfile` e `railway.toml` continuam disparando deploy.

```
**
!/docs/**
!/*.md
!/tests/**
!/testes_ml/**
!/.github/**
!/.claude/**
!/scripts/testes.sh
!/pytest.ini
!/requirements-dev.txt
```

**Teste no primeiro uso:**
1. o merge do PR de documentação (este) **não pode** criar deploy;
2. o próximo merge de código **tem de** criar deploy.

Se qualquer um dos dois falhar, a lista está errada e precisa ser revista
antes do próximo merge.

## 5. Overlap, draining e healthcheck (notas)

- `RAILWAY_DEPLOYMENT_OVERLAP_SECONDS`: com volume **não tem efeito** (não há
  sobreposição). O dono só confere o valor.
- `RAILWAY_DEPLOYMENT_DRAINING_SECONDS`: o worker espera até **30 s** pela
  drenagem ao encerrar (`_DRAIN_TIMEOUT = 30` em `main.py`). O dono confere se
  o valor configurado é **≥ 45 s**, para a Railway não matar o processo no
  meio da drenagem.
- Healthcheck: **não agora**.
- Nenhuma mudança na Railway é feita pelo Claude Code: os cliques são do dono
  (CLAUDE.md §8).

## 6. Armadilha de auditoria

Na busca de logs da Railway, o filtro `"[OK]"` (com colchete) volta **vazio**
mesmo com linhas `🚀 [OK]` no log (auditoria de 14/10). Por isso toda
contagem de "zero ocorrências" usa filtro sem colchete e controle positivo
(§1, passo 4).

## 7. Candidatas futuras para recuperar a janela (não é agora)

Entram no mapa do Brain como candidatas; nenhuma está aprovada.

- **R1:** último id visto por fonte gravado no banco + busca do buraco no
  boot.
- **R2:** estado de updates persistido + `catch_up` do Telethon — **nunca** a
  chave da sessão.
- **R3:** no boot, conferir as origens dos posts vivos (pega exclusões
  perdidas).
