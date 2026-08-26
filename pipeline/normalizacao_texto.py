"""
Camada 3 — Normalização / Forma do texto.

Responsabilidade ÚNICA: normalizar a FORMA de uma string e responder
perguntas sobre a forma dela.

NÃO conhece URL, plataforma, identidade, cupom nem pipeline. A única
dependência é `re`.

NÃO decide política de conteúdo — isso é de pipeline.filtros,
consumido em pipeline.normalizacao.normalizar.

Extraído de pipeline.normalizacao sem qualquer alteração de
comportamento. As formas aqui são idênticas às originais.
"""
from __future__ import annotations

import re

__all__ = ["limpar_texto", "sem_marcacao", "_tem_emoji"]

# ─────────────────────────────────────────────────────────────────
# LIMPEZA DE TEXTO
# ─────────────────────────────────────────────────────────────────
_RE_INVISIVEIS = re.compile(r'[\u200b\u200c\u200d\u00a0\u2060\ufeff]')
_RE_EMOJI_CHK = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF\u2B50\u2B55]"
)


def _tem_emoji(s: str) -> bool:
    return bool(_RE_EMOJI_CHK.search(s))


def limpar_texto(texto: str) -> str:
    """Normaliza a FORMA do texto: remove caracteres invisíveis e
    unifica quebras de linha. NÃO decide política de conteúdo — isso
    é de pipeline.filtros, consumido em `normalizar`.
    """
    return (
        _RE_INVISIVEIS.sub(" ", texto)
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )

# ─────────────────────────────────────────────────────────────────
# REMOÇÃO DE MARCAÇÃO — projeção de ANÁLISE
# ─────────────────────────────────────────────────────────────────
# A ingestão lê `message.text`, que traz o texto COM a marcação do
# Telegram (negrito, itálico, monospace, riscado, spoiler, link
# embutido). Essa marcação é APRESENTAÇÃO: a publicação depende dela
# e não pode perdê-la. Os detectores semânticos, ao contrário, são
# regex sobre texto contíguo, e a marcação os quebra — medido:
# "**R$ 30 OFF** em **R$ 60**" impede o reconhecimento da compra
# mínima, e "acima de **R$45**" impede o reconhecimento do escopo
# de loja.
#
# Este módulo já é o dono da FORMA do texto; a projeção de análise
# é irmã de limpar_texto e mora aqui pela mesma razão: nenhum outro
# módulo precisa conhecer o dialeto do Telegram.
_RE_URL_BRUTA = re.compile(r'https?://\S+', re.I)
_RE_MD_PRE = re.compile(r'```(?:[a-z0-9+#-]*\n)?(.*?)```', re.S | re.I)
_RE_MD_CODE = re.compile(r'`([^`\n]+)`')
_RE_MD_LINK = re.compile(r'\[([^\]\n]*)\]\((?:[^)\s]+)\)')
_RE_MD_PARES = (
    re.compile(r'\*\*(.+?)\*\*'),
    re.compile(r'__(.+?)__'),
    re.compile(r'~~(.+?)~~'),
    re.compile(r'\|\|(.+?)\|\|'),
)
_MD_SENTINELA = "\x00URL%d\x00"


def sem_marcacao(texto: str) -> str:
    """Projeção do texto SEM marcação de apresentação.

    Remove apenas marcadores que formam PAR. Asterisco solto, par não
    fechado, crase órfã e underscore de código (`CUPOM_A_B`) ficam
    intactos — a função nunca inventa remoção.

    URLs são preservadas byte a byte: `https://x.com/a__b__c` não pode
    virar `https://x.com/abc`. Por isso são mascaradas antes e
    restauradas depois.

    Pura, determinística e idempotente. Texto já sem marcação é ponto
    fixo. As posições de caractere do resultado NÃO correspondem às do
    texto de entrada — quem precisa localizar para apresentar (ver
    utils.marcacao) deve continuar operando sobre texto_limpo.
    """
    guardadas: list = []

    def _guardar(m):
        guardadas.append(m.group(0))
        return _MD_SENTINELA % (len(guardadas) - 1)

    t = _RE_MD_LINK.sub(lambda m: m.group(1), texto)
    t = _RE_URL_BRUTA.sub(_guardar, t)
    t = _RE_MD_PRE.sub(lambda m: m.group(1), t)
    t = _RE_MD_CODE.sub(lambda m: m.group(1), t)
    for padrao in _RE_MD_PARES:
        anterior = None
        while anterior != t:
            anterior = t
            t = padrao.sub(lambda m: m.group(1), t)
    for i, url in enumerate(guardadas):
        t = t.replace(_MD_SENTINELA % i, url)
    return t
