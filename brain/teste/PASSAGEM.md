# Passagem — sessão de TESTE do juiz do Brain v0

Cole este texto numa sessão NOVA do Claude Code, no ambiente onde o dono pôs
a chave **só de teste** (com limite de gasto) na variável `ANTHROPIC_API_KEY`.
Leia o `CLAUDE.md` inteiro antes de começar: ele vale aqui também.

## Regras desta sessão

- A chave nunca é lida, impressa, copiada nem gravada (nem em log, commit,
  PR ou chat). Para saber se ela existe, só `presente`/`ausente`. Quem a lê é
  o SDK.
- Nada de merge, deploy, Railway (só leitura), segredo, postagem nos canais
  ou mudança de governança.
- Trabalhe na branch `brain-v0-prototipo`. Pode fazer push nela. Não abra PR.
- Teto total: o valor aprovado pelo dono (padrão: US$ 20). A bancada soma o
  custo **medido** de todas as rodadas gravadas e para antes de passar do teto.
- O holdout nunca é usado para mexer no prompt. A iteração (`dev`) só vê o
  DEV. Se mudar o prompt, suba `VERSAO_PROMPT` em `brain/brain/juiz.py` e
  registre o motivo no commit (caso do DEV que falhou → regra que mudou).
- Erro, gasto acima do previsto ou qualquer coisa estranha: PARE e avise.

## Passo a passo (cada passo tem uma saída esperada)

```bash
git fetch origin brain-v0-prototipo && git checkout brain-v0-prototipo
python3 -m venv /tmp/vb && /tmp/vb/bin/pip install -q -r requirements.txt -r brain/requirements.txt
/tmp/vb/bin/python -c "import anthropic; print(anthropic.__version__)"   # 1.9.0

# 1) a API responde (sem chave no pedido → 401); NÃO passe a chave no curl
curl -s -o /dev/null -w "%{http_code}\n" https://api.anthropic.com/v1/models   # 401

# 2) a chave existe, sem exibir
/tmp/vb/bin/python -c "import os; print('presente' if os.environ.get('ANTHROPIC_API_KEY') else 'ausente')"

# 3) a suíte inteira continua verde (hermética, nenhuma chamada real)
PYTHON=/tmp/vb/bin/python bash scripts/testes.sh

# 4) orçamento ANTES de gastar (câmbio informado com data e fonte)
cd brain
/tmp/vb/bin/python -m brain.avaliar orcamento --cambio <R$ por US$>

# 5) piloto: 10 casos do DEV, Haiku high, teto US$ 0,50
/tmp/vb/bin/python -m brain.avaliar piloto --confirmar --cambio <R$ por US$>
```

Depois do piloto, leia `brain/teste/resultados/<carimbo>_piloto/tabela.md` e
o `chamadas.jsonl`. Compare `tokens_out_med` com o presumido
(`SAIDA_PRESUMIDA` em `avaliar.py`). Se o real for maior, refaça o orçamento
com o real. Se a projeção passar do teto aprovado, **PARE e avise o dono**
antes do passo 6.

**Rodadas longas** (`dev` e `final`): rode em segundo plano. O `final` tem
~1.800 chamadas; com `--paralelo 4`, que é o padrão, leva da ordem de horas.
O contêiner pode reciclar no meio. A bancada grava cada chamada na hora e é
retomável: repita o mesmo comando com `--retomar <pasta da rodada>`, e o que
já foi feito (e pago) não se repete. Se aparecer 429 em sequência, baixe para
`--paralelo 2`.

```bash
# 6) iteração: Haiku high, só DEV, 3x por caso (até 3 rodadas no orçamento)
/tmp/vb/bin/python -m brain.avaliar dev --confirmar --cambio <R$ por US$>

# 7) medição final: Haiku e Sonnet x {high, medium, low}, todos os casos 3x;
#    depois, Opus high só no subconjunto difícil, se sobrar teto
/tmp/vb/bin/python -m brain.avaliar final --confirmar --cambio <R$ por US$>

# se a rodada foi interrompida: retoma a MESMA pasta (não repete o que já foi pago)
/tmp/vb/bin/python -m brain.avaliar final --confirmar --cambio <R$ por US$> \
    --retomar teste/resultados/<carimbo>_final
```

## O que entregar ao dono

1. A `tabela.md` do final, com estas colunas por config (modelo × effort e a
   cascata Haiku → Sonnet calculada sem gastar de novo):
   - PORTÃO;
   - falso destrutivo, falso reinjetar e falso MESMA_CAMPANHA (os três têm de
     ser ZERO);
   - acerto do veredito no holdout;
   - "um post" e "dois posts" certos;
   - INCERTO;
   - estáveis nas 3 repetições;
   - latência p50/p95;
   - US$ por chamada.
2. A config mais barata que PASSA no portão. Se nenhuma passar, diga isso
   com os casos que reprovaram (id e veredito). Não proponha baixar a régua.
3. A projeção mensal com o custo MEDIDO, em US$ e R$:
   - chamadas/dia de `docs/arquitetura/brain-v0.md` §A;
   - normal, e 11.11/Black Friday com volume ×3 e ×5;
   - na cascata, a fração que sobe para o Sonnet, também medida.
4. O gasto total medido, por rodada e somado.
5. Commit em português dos `resultados/` (sem a chave, claro) e push na
   branch `brain-v0-prototipo`.
6. A última linha da resposta: "Dono: revogue a chave de teste no Console."
