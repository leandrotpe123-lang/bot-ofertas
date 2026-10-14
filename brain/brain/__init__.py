"""
Brain v0 — FASE 0 (protótipo de desenho e medição). NADA AQUI RODA EM PRODUÇÃO.

Serviço separado do worker (docs/arquitetura/brain-v0.md):
  · nunca usa TELEGRAM_SESSION nem sessão MTProto;
  · nunca abre nem escreve o SQLite do worker;
  · entrada v0 = prévia pública dos canais; v1 = eventos do worker (F1);
  · a IA (juiz) só RECOMENDA; a política determinística decide; quem
    executa é o worker, com os locks normais (CLAUDE.md §2);
  · sem IA, o Brain degrada para "só registra" — o worker nunca sente nada.

Módulos:
  config     constantes de código (BRAIN_MODO = "SOMBRA"; ACAO só por PR)
  precos     preço por modelo e custo medido pelo `usage`
  juiz       prompt v0, schema JSON fechado, validação do veredito
  cliente    adaptador da API (SDK importado só quando usado), tetos, disjuntor
  politica   tabela determinística veredito → ação, com o que nunca age
  prefiltro  o que vai para a IA (classes 1–3), sem custo
  avaliar    bancada de teste com orçamento (só roda com chave de TESTE)
"""
