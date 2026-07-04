"""
Camada — Enriquecimento.

Responsabilidade única: transformar a MensagemNormalizada (o snapshot
bruto/normalizado) no conjunto de derivados PRONTOS para consumo pelas
camadas de decisão (deduplicação) e publicação. É a fronteira entre o
"snapshot base" e os "derivados prontos": as camadas a jusante consomem
o que recebem, sem recalcular identidade, tipo, score ou cupom por conta
própria.

Superfície pública:
  - enriquecer(norm) -> MensagemEnriquecida

EFEITO COLATERAL RECONHECIDO (preservado, NÃO alterado):
  identidade_canonica chama _id_cupom_indexado, que registra códigos no
  índice cupom_idx (compartilhado entre mensagens) e muta
  norm._cupom_novos. Esse efeito vivia em deduplicacao.deve_enviar_async
  e é aqui reposicionado SEM mudar:
    - sua frequência (1x por mensagem — enriquecer chama identidade_canonica
      uma vez, como a dedup fazia);
    - sua ordem relativa às leituras de norm._cupom_novos, que continuam
      a jusante (dedup, decisao, publicacao);
    - sua fonte única de verdade: norm._cupom_novos permanece um atributo
      VIVO de norm, lido pelos consumidores atuais sem alteração. Este
      módulo NÃO congela esse valor num campo — fazê-lo criaria duas
      fontes de verdade.

Composição, não herança: MensagemEnriquecida carrega a REFERÊNCIA a norm
mais os derivados; não estende MensagemNormalizada nem mistura o contrato
do snapshot bruto com o contrato de consumo pronto.

NÃO faz:
  - decisão de duplicidade (deduplicacao)
  - publicação / edição (publicacao)
  - normalização / derivação base (normalizacao)
"""
from __future__ import annotations

from dataclasses import dataclass

from pipeline.normalizacao import MensagemNormalizada
from pipeline.identidade_oferta import tipo_de_oferta, identidade_canonica, identidades
from pipeline.score import calcular_score


@dataclass(frozen=True)
class MensagemEnriquecida:
    """Snapshot normalizado + derivados prontos para consumo.

    Imutável: os derivados são calculados uma vez, na fronteira do
    enriquecimento. `norm` é a referência viva ao snapshot — cujo atributo
    dinâmico _cupom_novos permanece a fonte única do efeito de cupom.

    Expõe `tipo` e `canonica` (consumidos pela deduplicação) e, desde o
    Corte 2, `ofertas` e `score` prontos — consumidos pela publicação no
    caminho NOVO, que deixou de rederivá-los. Todos puros exceto o efeito
    de cupom, que ocorre 1x dentro de `identidade_canonica`.
    """
    norm:     MensagemNormalizada
    tipo:     str
    canonica: str
    ofertas:  list[str]
    score:    int


def enriquecer(norm: MensagemNormalizada) -> MensagemEnriquecida:
    """Produz os derivados prontos a partir do snapshot normalizado.

    A ORDEM reproduz exatamente a de deduplicacao.deve_enviar_async: tipo
    primeiro (puro), identidade canônica depois — e é dentro de
    identidade_canonica que o efeito de cupom (cupom_idx + norm._cupom_novos)
    ocorre, 1x, como antes. As camadas a jusante consomem tipo/canonica
    prontos, sem redisparar o efeito.
    """
    tipo     = tipo_de_oferta(norm)
    canonica = identidade_canonica(norm)
    ofertas  = identidades(norm)
    score    = calcular_score(norm)
    return MensagemEnriquecida(
        norm=norm, tipo=tipo, canonica=canonica,
        ofertas=ofertas, score=score)
  
