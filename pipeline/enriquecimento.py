"""
Camada — Enriquecimento.

Responsabilidade única: transformar a MensagemNormalizada (o snapshot
bruto/normalizado) no conjunto de derivados PRONTOS para consumo pelas
camadas de decisão (deduplicação) e publicação. É a fronteira entre o
"snapshot base" e os "derivados prontos": as camadas a jusante consomem
o que recebem, sem recalcular identidade, tipo, score ou cupom por conta
própria.

Superfície pública:
  - derivar(norm)    -> MensagemEnriquecida   (puro, todos os caminhos)
  - enriquecer(norm) -> MensagemEnriquecida   (derivar + efeito, só NOVO)

O efeito de memória de cupom (frequência e ordem preservadas) é
congelado em `cupons_novos` (P8). Este contrato é a via ÚNICA pela
qual o valor chega a decisao e publicacao: nada viaja por atributo
dinâmico em `norm`.
Composição, não herança: MensagemEnriquecida carrega a REFERÊNCIA a norm
mais os derivados; não estende MensagemNormalizada nem mistura o contrato
do snapshot bruto com o contrato de consumo pronto.

NÃO faz:
  - decisão de duplicidade (deduplicacao)
  - publicação / edição (publicacao)
  - normalização / derivação base (normalizacao)
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from logger import log_enr
from pipeline.normalizacao import MensagemNormalizada
from pipeline.identidade_oferta import identidade_canonica, ancoras, Ancora
from pipeline.natureza import natureza
from pipeline.score import calcular_score
from pipeline.memoria_cupom import registrar_uso


@dataclass(frozen=True)
class MensagemEnriquecida:
    """Snapshot normalizado + derivados prontos para consumo.

    Imutável: os derivados são calculados uma vez, na fronteira do
    enriquecimento. `norm` é a referência viva ao snapshot, nunca mutada;
    `cupons_novos` é a fonte ÚNICA do efeito de cupom.

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

def derivar(norm: MensagemNormalizada) -> MensagemEnriquecida:
    """DERIVAÇÃO PURA — determinística, sem I/O e sem mutação.

    Roda em TODOS os caminhos (novo e edição). Produz o contrato
    completo exceto cupons_novos, que é fruto do efeito e só existe
    quando o efeito roda. Chamar duas vezes devolve o mesmo resultado.
    """
    ancs = ancoras(norm)                          # autoritativa (P6)
    enr = MensagemEnriquecida(
        norm=norm,
        tipo=natureza(norm),
        canonica=identidade_canonica(norm),       # PURA desde F1e
        ofertas=[a.chave for a in ancs],          # projeção de ancoras()
        score=calcular_score(norm),
        ancoras=tuple(ancs),
        cupons_novos=0)
    log_enr.info(f"🔬 P3 id={norm.msg_id} tipo={enr.tipo} canonica={enr.canonica} ofertas={enr.ofertas} cupons={norm.cupons} codes={len(norm.code_entities)} score={enr.score}")
    return enr


def enriquecer(norm: MensagemNormalizada) -> MensagemEnriquecida:
    """DERIVAÇÃO + EFEITO — exclusivo do caminho NOVO (P9).

    Deriva e, em seguida, aplica o efeito de memória de cupom: registra
    os códigos sob a identidade eleita e conta os inéditos. O efeito
    ocorre 1x por mensagem, DEPOIS da derivação, e nunca em edições —
    é o que impede um mesmo código de ser recontado como novo a cada
    edição do post.
    """
    base = derivar(norm)
    if base.tipo != "cupom":
        # F1c: num post de PRODUTO o código é atributo, não sujeito —
        # não indexa e não conta novidade. Consome a natureza já
        # decidida; não reclassifica.
        return base
    novos = registrar_uso(norm, norm.plat, base.canonica)
    return replace(base, cupons_novos=novos)
  
