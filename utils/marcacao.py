"""
Utilitário — Marcação de apresentação do Telegram.

Responsabilidade ÚNICA: conhecer o dialeto de marcação que o cliente do
Telegram interpreta e aplicá-lo a um texto JÁ PRONTO. É o único módulo
do sistema que sabe quais caracteres produzem negrito, código monoespaçado
e escape.

NÃO faz:
  - reconhecer cupom            (autoridade: utils.cupom)
  - reconhecer URL de plataforma (autoridade: plataformas.registry)
  - decidir o que entra no post  (autoridade: pipeline.filtros)
  - montar linha, escolher emoji, ordenar conteúdo (pipeline.montagem)
  - falar com o Telegram         (pipeline.saida)

Recebe fatos já reconhecidos por quem tem autoridade e devolve texto
marcado. Camada PURA: sem I/O, sem estado, sem log, sem rede.

═══════════════════════════════════════════════════════════════════
DIALETO — INVARIANTE EXTERNO (I-M0)
═══════════════════════════════════════════════════════════════════
O envio usa parse_mode="md" (pipeline.saida), que é o parser PRÓPRIO da
Telethon. Seus delimitadores são:

    **negrito**   __itálico__   ~~tachado~~   `código`

NÃO é o Markdown do WhatsApp nem o MarkdownV2 do Bot API. Nesses dois,
negrito é UM asterisco; aqui UM asterisco é caractere literal e apareceria
cru na mensagem. O resultado RENDERIZADO de `**x**` aqui é idêntico ao de
`*x*` no WhatsApp — muda a grafia de origem, não o que o leitor vê.

Trocar parse_mode em pipeline.saida INVALIDA este módulo inteiro.

═══════════════════════════════════════════════════════════════════
INVARIANTES DO MÓDULO (contrato)
═══════════════════════════════════════════════════════════════════
I-M1  Marcação é terminal. O texto que sai daqui não volta a ser
      analisado por nenhuma camada semântica. Nada em identidade,
      score ou deduplicação pode depender do que este módulo escreve.

I-M2  Marcação nunca altera conteúdo. Só insere delimitadores. Remover
      todos os delimitadores inseridos devolve a entrada byte a byte.

I-M3  URL é intocável. Nenhuma URL entra em segmento de negrito e
      nenhum caractere de URL é alterado além do escape de `proteger_url`.

I-M4  Código é intocável. O literal do cupom nunca é modificado — apenas
      envolvido em crases pelo `codigo()`, que recebe os literais de
      quem tem autoridade sobre eles.

I-M5  Na dúvida, NÃO marca. Segmento que já contenha caractere de
      marcação é devolvido intacto. Marcação errada é pior que ausente:
      delimitador ímpar corrompe o render da mensagem inteira.

I-M7  Elegibilidade vem de fora. Este módulo NUNCA decide se uma linha
      merece negrito: recebe a decisão de pipeline.montagem, que é quem
      classifica a linha. Zero heurística textual aqui.

I-M6  Sem aninhamento. O parser da Telethon não garante entidade dentro
      de entidade. O negrito SEMPRE termina antes da primeira crase e
      antes da primeira URL da linha.
"""
from __future__ import annotations

import re
from typing import Iterable

__all__ = ["proteger_url", "codigo", "negrito"]


# ─────────────────────────────────────────────────────────────────
# ESCAPE DE URL
# ─────────────────────────────────────────────────────────────────
def proteger_url(url: str) -> str:
    """Protege URLs contra interpretação de Markdown do Telegram."""
    return (
        url.replace('\\', '\\\\')
           .replace('*', '\\*')
           .replace('`', '\\`')
           .replace('[', '\\[')
    )


# ─────────────────────────────────────────────────────────────────
# CÓDIGO MONOESPAÇADO (crases)
# ─────────────────────────────────────────────────────────────────
def codigo(linha: str, codigos: Iterable[str]) -> str:
    """Envolve em crases os literais JÁ RECONHECIDOS por quem tem
    autoridade sobre eles.

    Este módulo NÃO reconhece cupom: recebe os literais prontos. Depende
    da garantia de literalidade documentada em utils.cupom
    (extrair_todos_cupons): todo código devolvido é recorte literal do
    texto, em maiúsculas.

    Linha com URL ou já contendo crase é devolvida intacta.
    """
    if "http" in linha or "`" in linha:
        return linha

    codigos = list(codigos or ())
    if not codigos:
        return linha

    # Mais longos primeiro: evita que um código prefixo de outro
    # consuma a alternativa antes da forma completa.
    alvo = sorted(set(codigos), key=len, reverse=True)

    padrao = re.compile(
        r'\b(?:' + "|".join(re.escape(c) for c in alvo) + r')\b'
    )

    return padrao.sub(lambda m: f"`{m.group(0)}`", linha)


# ─────────────────────────────────────────────────────────────────
# NEGRITO
# ─────────────────────────────────────────────────────────────────
# Início de URL — fronteira de corte do negrito (I-M3/I-M6).
_RE_URL_CORTE = re.compile(r'https?://')

# Caracteres que já carregam significado de marcação. Presentes no
# segmento-alvo, a linha é devolvida intacta (I-M5):
#   *   negrito/itálico já marcado na origem, ou asterisco literal
#       do conteúdo ("Rede 3*2m") → delimitador ímpar
#   [   abertura de link markdown → `[texto](url)` seria partido
#   \   escape → altera o significado do que vier depois
#   __  itálico → entidade dentro de entidade
_MARCAS_INSEGURAS = ("*", "[", "\\", "__")


def negrito(linha: str, elegivel: bool) -> str:
    """Aplica negrito ao segmento semântico da linha.

    ELEGIBILIDADE NÃO É DECIDIDA AQUI (I-M7). Este módulo não lê o texto
    para adivinhar o que a linha é: recebe o veredito de quem classificou.
    A autoridade é pipeline.montagem, que no mesmo passo decidiu se a
    linha é título e qual emoji semântico ela carrega. Quando aquele
    classificador melhorar, o negrito acompanha sem tocar neste módulo.

    RECORTE — o negrito vai do início da linha até o primeiro ponto de
    escape: a primeira crase OU a primeira URL, o que vier antes. Não
    havendo nenhum, vai até o fim da linha. É o que produz
    `**🎟 Cupom:**` + código monoespaçado e `**⭐ Lista:**` + link.

    APARA — espaço entre o fim do texto e o delimitador fica FORA do
    negrito.

    VETO — segmento com marcação preexistente é devolvido intacto (I-M5).

    Idempotente: a segunda passada encontra `*` no segmento e recua.
    """
    if not elegivel or not linha:
        return linha

    cortes = []
    pos_crase = linha.find("`")
    if pos_crase != -1:
        cortes.append(pos_crase)
    m_url = _RE_URL_CORTE.search(linha)
    if m_url:
        cortes.append(m_url.start())
    corte = min(cortes) if cortes else len(linha)

    alvo, resto = linha[:corte], linha[corte:]

    texto = alvo.rstrip()
    if not texto:
        return linha

    if any(marca in texto for marca in _MARCAS_INSEGURAS):
        return linha

    espaco = alvo[len(texto):]
    return f"**{texto}**{espaco}{resto}"

