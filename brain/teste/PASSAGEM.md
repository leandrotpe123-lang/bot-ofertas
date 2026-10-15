# Passagem — sessão de TESTE do juiz do Brain v0

Cole este texto numa sessão NOVA do Claude Code, no ambiente onde o dono pôs a
chave **só de teste** (com limite de gasto) na variável **`BRAIN_TESTE_CHAVE`**.
Leia o `CLAUDE.md` inteiro antes de começar: ele vale aqui também.

## Por que `BRAIN_TESTE_CHAVE` e não `ANTHROPIC_API_KEY`

No Claude Code, `ANTHROPIC_API_KEY` é o nome que o próprio Claude Code usa para
se autenticar. Com a chave de teste nesse nome, as sessões passariam a gastá-la.

A bancada lê só `BRAIN_TESTE_CHAVE` e passa a chave explícita ao cliente. O
endereço fica fixo em `https://api.anthropic.com`. O cliente do SDK é montado
sem nenhuma variável `ANTHROPIC_*` no ambiente. O motivo: o SDK, sozinho, lê
`ANTHROPIC_BASE_URL` e `ANTHROPIC_CUSTOM_HEADERS`, e no ambiente do Claude
Code `ANTHROPIC_BASE_URL` existe.

Na Railway (serviço `brain`, produção), o nome continua `ANTHROPIC_API_KEY`.

## Regras desta sessão

- A chave nunca é lida para exibir, impressa, copiada nem gravada (nem em log,
  commit, PR ou chat). Para saber se ela existe, só `presente`/`ausente`.
- Nada de merge, deploy, Railway (só leitura), segredo, postagem nos canais
  ou mudança de governança.
- Trabalhe na branch `brain-v0-prototipo`. Pode fazer push nela. Não abra PR.
- **Teto total: US$ 20.** A bancada soma o custo **medido** de todas as
  rodadas gravadas e para antes de passar.
- **Plano aprovado, em etapas:**
  1. piloto;
  2. dev (Haiku);
  3. final Haiku (high, medium e low);
  4. Sonnet só em high.
- `sonnet-extra` (Sonnet medium/low) e `opus` só rodam se o Haiku falhar no
  portão **e** o dono aprovar. A bancada recusa sem `--aprovado-pelo-dono`.
- O holdout nunca é usado para mexer no prompt. A iteração (`dev`) só vê o
  DEV. Se mudar o prompt:
  - suba `VERSAO_PROMPT` em `brain/brain/juiz.py`;
  - registre no commit o motivo: caso do DEV que falhou → regra que mudou.
- Erro, gasto acima do previsto ou qualquer coisa estranha: PARE e avise.

## Passo a passo (cada passo tem uma saída esperada)

```bash
git fetch origin brain-v0-prototipo && git checkout brain-v0-prototipo
python3 -m venv /tmp/vb && /tmp/vb/bin/pip install -q -r requirements.txt -r brain/requirements.txt
/tmp/vb/bin/python -c "import anthropic; print(anthropic.__version__)"   # 1.9.0

# 1) a chave de teste existe, sem exibir o valor (nem o tamanho)
/tmp/vb/bin/python -c "import os; print('BRAIN_TESTE_CHAVE:', 'presente' if os.environ.get('BRAIN_TESTE_CHAVE','').strip() else 'ausente')"
#    esperado: BRAIN_TESTE_CHAVE: presente
#    NUNCA rode env, printenv, set nem echo $BRAIN_TESTE_CHAVE

# 2) a API responde (sem chave no pedido → 401); NÃO passe a chave no curl
curl -s -o /dev/null -w "%{http_code}\n" https://api.anthropic.com/v1/models   # 401

# 3) a suíte inteira continua verde (hermética, nenhuma chamada real)
PYTHON=/tmp/vb/bin/python bash scripts/testes.sh

# 4) orçamento ANTES de gastar (câmbio informado com data e fonte)
cd brain
/tmp/vb/bin/python -m brain.avaliar orcamento --cambio <R$ por US$>

# 5) piloto: 10 casos do DEV, Haiku high, teto US$ 0,50
/tmp/vb/bin/python -m brain.avaliar piloto --confirmar --cambio <R$ por US$>
```

Depois do piloto, faça estas conferências:
- **Saída.** Leia `brain/teste/resultados/<carimbo>_piloto/tabela.md` e o
  `chamadas.jsonl`. A primeira linha da saída tem de dizer "chave de teste:
  presente em BRAIN_TESTE_CHAVE (não exibida); endereço fixo
  https://api.anthropic.com".
- **Tokens.** Compare `tokens_out_med` com o presumido (`SAIDA_PRESUMIDA` em
  `avaliar.py`). Se o real for maior, refaça o orçamento com o real.
- **Teto.** Se a projeção passar de US$ 20, **PARE e avise o dono** antes do
  passo 6.

**Rodadas longas** (`dev`, `final`, `sonnet`):
- Rode em segundo plano. Com `--paralelo 4` (padrão), cada uma leva da ordem
  de uma hora ou mais.
- O contêiner pode reciclar no meio. A bancada grava cada chamada na hora e é
  retomável: repita o mesmo comando com `--retomar <pasta da rodada>`, e o que
  já foi feito (e pago) não se repete.
- Se aparecer 429 em sequência, baixe para `--paralelo 2`.

```bash
# 6) iteração: Haiku high, só DEV, 3x por caso (até 3 rodadas no orçamento)
/tmp/vb/bin/python -m brain.avaliar dev --confirmar --cambio <R$ por US$>

# 7) final Haiku: high, medium e low, todos os casos 3x
/tmp/vb/bin/python -m brain.avaliar final --confirmar --cambio <R$ por US$>

# 8) Sonnet só high, todos os casos 3x (referência e cascata)
/tmp/vb/bin/python -m brain.avaliar sonnet --confirmar --cambio <R$ por US$>

# 9) tabela única (sem rede): todas as configs e a cascata Haiku → Sonnet
/tmp/vb/bin/python -m brain.avaliar tabela --cambio <R$ por US$>

# rodada interrompida: retoma a MESMA pasta (não repete o que já foi pago)
/tmp/vb/bin/python -m brain.avaliar final --confirmar --cambio <R$ por US$> \
    --retomar teste/resultados/<carimbo>_final

# SÓ se o Haiku falhar no portão E o dono aprovar por escrito:
# /tmp/vb/bin/python -m brain.avaliar sonnet-extra --confirmar --aprovado-pelo-dono ...
# /tmp/vb/bin/python -m brain.avaliar opus --confirmar --aprovado-pelo-dono ...
```

## O que entregar ao dono

1. **A `brain/teste/resultados/consolidado/tabela.md`.** Uma linha por config
   (Haiku high/medium/low, Sonnet high e a cascata Haiku → Sonnet calculada
   sem gastar de novo), com:
   - PORTÃO;
   - falso destrutivo, falso reinjetar e falso MESMA_CAMPANHA (os três têm de
     ser ZERO);
   - acerto do veredito no holdout;
   - "um post" e "dois posts" certos;
   - INCERTO;
   - estáveis nas 3 repetições;
   - latência p50/p95;
   - US$ e R$ por chamada.
2. **A config mais barata que PASSA no portão.** Se nenhuma passar, diga isso
   com os casos que reprovaram (id e veredito). Não proponha baixar a régua;
   peça a aprovação do dono para as etapas extras.
3. **A projeção mensal com o custo MEDIDO, em US$ e R$:**
   - chamadas/dia de `docs/arquitetura/brain-v0.md` §A;
   - normal, e 11.11/Black Friday com volume ×3 e ×5;
   - na cascata, a fração que sobe para o Sonnet, também medida.
4. **O gasto total medido**, por rodada e somado.
5. **Commit** em português dos `resultados/` (sem a chave, claro) e push na
   branch `brain-v0-prototipo`.
6. **A última linha da resposta:** "Dono: revogue a chave de teste (BRAIN_TESTE_CHAVE) no Console."
