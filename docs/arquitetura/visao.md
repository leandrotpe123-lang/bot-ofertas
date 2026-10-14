# Visão do FOGUETÃO

A bússola das frentes. Regras e proibições ficam no `CLAUDE.md`, que vale
acima deste texto.

## O sistema

Userbot do Telegram (Telethon, conta de usuário) que lê os grupos de origem
(`config.GRUPOS_ORIGEM`), converte os links em links de afiliado e publica no
canal do dono (`config.GRUPO_DESTINO`): publica, edita, funde e remove. Post
de cupom sai também no canal de cupons. Produção: worker na Railway, uma
réplica, SQLite no volume `/data`.

**O objetivo:** um sistema que se vigia sozinho 24 h, **sem perder
velocidade**, porque o dono não consegue ficar olhando o canal.

## O caso principal

**A mesma oferta nunca aparece duas vezes no canal.**
- Mesma campanha, mesmo código de cupom e mesma plataforma, vinda de qualquer
  grupo, vira **um post só**.
- Se a oferta muda, o post é **editado**. Nunca duplicado.

## Dois níveis, nesta ordem

**1. Prevenção, no worker, na hora de publicar.**
- A identidade da oferta, a família/convergência e a memória de cupom decidem:
  post novo, evolução ou descarte.
- Quem impede a duplicata é o worker.
- O Brain **nunca** fica no caminho da publicação.

**2. Vigilância, no Brain (serviço futuro), depois do fato.** O Brain:
- enxerga as fontes e o canal pelos eventos do worker (a F1);
- compara o que entrou com o que está publicado;
- detecta o que escapou: duplicata, cupom errado ou vencido, publicação que
  falhou, link sem afiliado, post que devia ter saído, bot parado;
- pede a correção ao worker, por regra fixa;
- avisa o dono no Telegram (meio a definir; **nunca** a sessão do userbot);
- transforma cada caso em proposta de melhoria: PR do Claude Code, merge do
  dono.

É assim que o sistema se aperfeiçoa: cada duplicata que escapa vira teste e
regra nova, com aprovação do dono. **Nada se altera sozinho em produção.**

## Limites que não mudam

- `CLAUDE.md` §2: o Brain não usa a sessão do Telegram, não escreve no banco
  do worker, e a IA dele só recomenda; quem decide é regra determinística.
- `CLAUDE.md` §9.1: cupom detectado **não é** cupom elegível.
- "Editar como humano": não editar post enterrado debaixo de vários outros. O
  critério entra no contrato quando chegar a hora.
- Velocidade: toda instrumentação passa nos portões da D.5
  (`docs/arquitetura/F1-contrato-vigente.md` §2.1).
