# Medição das edições e canal de cupons — onde está, por quê, e o que fazer depois

Registro da arrumação do #32. Este documento **não muda regra nenhuma**:
descreve o código como está e propõe trabalhos futuros, cada um em PR próprio.

## 1. Pontos atuais da instrumentação

A medição tem um módulo próprio (`medicao_edicao.py`, folha, sem I/O no
import), mas é **chamada** de quatro módulos:

| Onde | O que faz | Por que está ali |
|---|---|---|
| `pipeline/publicacao_aplicadores.py:120, 226, 285` | cria o `rastro = (cid, op)` da evolução, da sincronização e do upgrade de mídia | só o aplicador sabe qual operação está fazendo; o mesmo cid vai para a edição do canal de cupons |
| `pipeline/publicacao_aplicadores.py:86-87` (`_editar_medido`) | abre a medida do principal em volta de `editar_msg` | a medida precisa viver dentro da task que faz o I/O |
| `pipeline/publicacao_aplicadores.py:357` | linha ⏱ `[ENVIO_MS]` da publicação do principal | já existia (#31) |
| `pipeline/saida.py:283` | `comecou()` logo depois de pegar a vaga do `_SEM_ENVIO` | o fim da fila só é conhecido dentro do semáforo |
| `pipeline/espelho_cupons.py:415-419` (`_medida`) | medida do canal de cupons, com o cid do principal | idem, do lado da cópia |
| `pipeline/espelho_cupons.py:423, 441, 446` | `with medir(...)` na edição simultânea, na conferência e na reconciliação | cada operação da cópia é uma task própria |
| `pipeline/espelho_cupons.py:392` (`_medido`) | linha ⏱ `[ENVIO_MS]` da publicação da cópia | já existia (#31) |
| `pipeline/espelho_cupons.py:652` (`_io`) | `comecou()` dentro do semáforo | idem ao `saida.py:283` |
| `espera_telethon.py:48` | `atual()` diz quem esperou na linha ⏳ | correlação da espera do Telethon |
| `pipeline/espelho_cupons.py:592` (`_registrar_remocao`) | linha 🗑 `[CUPONS_REMOCAO]` (este PR) | a remoção da cópia não deixava rastro |

**Por que o cid é passado à mão (`rastro=`) do aplicador para o canal de
cupons:** as tarefas do canal de cupons nascem em contexto vazio de propósito
(`espelho_cupons._tarefa`, regra F1.2-A2-O). Nenhuma ContextVar atravessa essa
fronteira, e criar uma ContextVar nova obriga a reavaliar o
`test_eventos_tarefas`. A passagem explícita é consequência dessa regra, não
descuido.

## 2. Achado: as linhas ⏱ estão dentro do caminho operacional

As linhas ⏱ de `_medido` (`espelho_cupons.py:392`, num `finally`), do
`medir.__exit__` (`medicao_edicao.py:93`) e do aplicador (`:357`) rodam no meio
da operação. O `StreamHandler` da stdlib trata erro de escrita em
`handleError` e **não levanta**, então hoje isso não muda nada. Mas se um dia
um handler próprio levantar, essas linhas passam a decidir.

Foi medido em bancada local, trocando o `log_sys` por um que levanta: a
publicação da cópia é vista como falha e `_concluir_publicacao` publica uma
**segunda** cópia. Não virou teste no repositório porque o teste afirmaria um
comportamento ruim. A linha nova 🗑 `[CUPONS_REMOCAO]` já nasce blindada
(`contextlib.suppress`), e o `test_cupons_corrida_midia` 25 prova que ela não
cria nem impede exclusão.

## 3. Proposta de centralização segura (futuro, PR próprio)

1. **Registro blindado único.** Um `medicao_edicao.registrar(nivel, linha)`
   que nunca levanta, usado por toda linha ⏱ e 🗑. Fecha o achado da seção 2
   sem mudar nenhuma decisão. Prova: o mesmo diferencial de trilha deste PR,
   mais um teste com o log quebrado em cada linha.
2. **Semáforo medido.** Envolver `config._SEM_ENVIO` num objeto cujo
   `__aenter__` chama `comecou()`. Isso tira os dois `comecou()`
   (`saida.py:283`, `espelho_cupons.py:652`). É risco maior: o semáforo é o
   orçamento global de todo envio. Pede PR próprio, com teste de equivalência
   da fila (ordem, contagem e FloodWait fora do semáforo).
3. **Manter o `rastro` explícito.** Ver a seção 1. Trocar por ContextVar
   brigaria com a regra do contexto vazio.

## 4. Refatoração futura: fila por post e "a mais nova vale"

`pipeline/espelho_cupons.py` tem hoje 662 linhas. A responsabilidade continua
uma só: manter a cópia igual ao principal. Mas dois mecanismos ali são
genéricos e não têm nada de cupom:

- **fila por post:** `_cadeia` (:126), `_encadear` (:328), `_em_ordem` (:339)
  e `_aguardar` (:344);
- **a mais nova vale:** `_pedidos` (:128), `_versoes` (:129) e
  `_reconciliar_pedido` (:445).

A proposta é extrair os dois para `pipeline/fila_por_post.py`, com testes
unitários próprios. Isso **move lógica funcional** (ordem e obsolescência das
operações da cópia), então não entra na arrumação. Fica para um PR com o
diferencial de trilha do Telegram (o mesmo usado aqui) e os testes 17 e 21 da
bancada como guarda.
