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

efeito preservado (frequência/ordem), e que desde a Fase 2 o valor é congelado em cupons_novos após o efeito (P8), com norm._cupom_novos mantido como ponte legada para decisao:78/publicacao:251 até a fase que tocar esses módulos.

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
from pipeline.identidade_oferta import tipo_de_oferta, identidade_canonica, ancoras, Ancora
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
    ancoras:  tuple[Ancora, ...] = ()
    cupons_novos: int = 0

def enriquecer(norm: MensagemNormalizada) -> MensagemEnriquecida:
    """Produz os derivados prontos a partir do snapshot normalizado.

    A ORDEM reproduz exatamente a de deduplicacao.deve_enviar_async: tipo
    primeiro (puro), identidade canônica depois — e é dentro de
    identidade_canonica que o efeito de cupom (cupom_idx + norm._cupom_novos)
    ocorre, 1x, como antes. As camadas a jusante consomem tipo/canonica
    prontos, sem redisparar o efeito.
    """
    tipo     = tipo_de_oferta(norm)
    canonica = identidade_canonica(norm)          # efeito de cupom AQUI, 1x
    ancs     = ancoras(norm)                      # puro (autoritativa, P6)
    ofertas  = [a.chave for a in ancs]            # projeção == identidades(norm)
    score    = calcular_score(norm)
    novos    = getattr(norm, "_cupom_novos", 0)   # congela PÓS-efeito (P8)
    return MensagemEnriquecida(
        norm=norm, tipo=tipo, canonica=canonica,
        ofertas=ofertas, score=score,
        ancoras=tuple(ancs), cupons_novos=novos)
