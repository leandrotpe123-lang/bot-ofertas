"""
Camada — Identidade de Oferta (Famílias por oferta).

Responsabilidade ÚNICA: derivar a IDENTIDADE de oferta de um post — o
conjunto de identidades por oferta (produto, campanha, cupom, cashback)
e a chave canônica de deduplicação. É a autoridade de DERIVAÇÃO de
identidade; não é autoridade de DECISÃO de duplicidade (essa é da
deduplicação, que consome o resultado pronto daqui).

Superfície pública (estável):
  - identidades(norm)         -> list[str]  : conjunto per-oferta
  - ancoras(norm)             -> list[Ancora]: âncoras TIPADAS (P6) — autoritativa
  - identidades(norm)         -> list[str]  : projeção plana de ancoras()
  - Natureza da oferta: ver pipeline.natureza (dono único)

CONSUMO DE IDENTIDADE DERIVADA:
  A identidade de produto e de campanha é derivada pela normalização —
  autoridade única dessa derivação — sobre as URLs afiliadas LONGAS e
  antes do encurtamento terminal. Esta camada CONSOME os campos já
  derivados (ids_globais, sku, chave_campanha, chaves_campanha,
  tem_host_campanha, tem_sinal_cashback) e NÃO os reextrai do mapa. A
  única leitura legítima do mapa é o nível de fallback operacional NÃO
  semântico de _id_url, explicitamente reconhecido.

NÃO faz:
  - decisão de duplicidade / claim / janela  (deduplicação)
  - cálculo de score                         (deduplicação → score)
  - publicação / edição / substituição       (publicação)
  - normalização / afiliação de links        (normalização)
"""
from __future__ import annotations

from dataclasses import dataclass

from pipeline.memoria_cupom import buscar_identidade
from pipeline.normalizacao import MensagemNormalizada
from pipeline.natureza import eh_entidade_cupom
from utils.hashes import _fp4
from utils.textos import _alma


# ── API pública ──────────────────────────────────────────────────
__all__ = [
    "identidades",
    "identidade_canonica",
    "ancoras",
]

# ─────────────────────────────────────────────────────────────────
# Classificação do ASSUNTO do post → pipeline.assunto (C1)
# Os detectores e suas regex foram extraídos para o módulo próprio.
# Consumimos APENAS a API pública — nenhuma regex atravessa a fronteira.
# O classificador NÃO tem autoridade sobre a família nesta fase:
# ancoras() decide exatamente como antes.
# ─────────────────────────────────────────────────────────────────
from pipeline.assunto import (          # noqa: E402
    tema_da_campanha,
    eh_post_cashback,
    beneficio_do_cupom,
)
from pipeline.resolucao_identidade import (   # noqa: E402
    Evidencias,
    resolver,
)

def _percentual(texto: str) -> str:
    """Percentual do benefício, lido de beneficio_do_cupom.

    Fonte ÚNICA: o descritor de assunto_oferta, que já é o dono desta
    extração. Aqui não há regex nem parsing — só leitura do campo
    "pct:" do descritor já pronto. Sem percentual, cadeia vazia.
    """
    for parte in beneficio_do_cupom(texto).split("+"):
        if parte.startswith("pct:"):
            return parte.split(":", 1)[1]
    return ""

# ─────────────────────────────────────────────────────────────────
# IDENTIDADE CANÔNICA — coração do sistema anti-duplicação
# ─────────────────────────────────────────────────────────────────
def identidade_canonica(norm: "MensagemNormalizada") -> str:
    """
    Chave estável de dedupe, eleita por PRECEDÊNCIA DE ESPÉCIE (MB §11.10).

    Eleição: a âncora de PRODUTO vence qualquer outra classe quando
    presente — espelha _id_produto (min dos ids_globais). Sem produto,
    mantém-se o representante lex-menor de identidades(), como desempate
    APENAS entre classes não-produto. A eleição não pode depender do
    alfabeto de ID da plataforma (P7): o lex-menor puro elegia camp/cash/
    cup para SKUs minúsculos (ex.: Magalu) por acidente.

    A grudação transitiva por código (memoria_cupom.resolver_identidade)
    só se aplica quando a natureza é cupom-entidade (MB §11.7a/b): num
    post de PRODUTO o código é atributo, não sujeito — redirecionar a
    canônica por ele uniria produtos distintos (viola R4/I4) e faria a
    canônica divergir das âncoras, que permanecem no produto.
    """
    gate = eh_entidade_cupom(norm)
    if gate:
        # [F-C4 / INV-E5] A natureza decidida pelo gate alimenta TODAS
        # as camadas: se a entidade é a campanha de cupom, a canônica
        # (claims/dedup) é a do cupom — nunca a do produto-vitrine.
        base = sorted(identidades(norm))[0]
    elif norm.ids_globais: 
        base = min(f"{p}|{i}" for p, i, _ in norm.idents)
    else:
        base = sorted(identidades(norm))[0]
    if not gate:
        return base
    return buscar_identidade(norm, norm.plat, base)


@dataclass(frozen=True)
class Ancora:
    """Âncora tipada de família (MB v1.1, P6). `chave` é byte-idêntica à
    string historicamente emitida por identidades() — que agora é VISTA."""
    especie: str   # "produto" | "cupom" | "campanha" | "cashback" | "fallback"
    chave: str


def _evidencias(norm: "MensagemNormalizada") -> Evidencias:
    """Traduz o snapshot normalizado no contrato do resolvedor.

    Aqui — e só aqui — é que as evidências são LIDAS do snapshot. O
    resolvedor não conhece MensagemNormalizada; recebe proposições.
    Toda derivação continua acontecendo nos donos de sempre
    (natureza, assunto, adaptadores); esta função apenas reúne.
    """
    texto = norm.texto_analise
    return Evidencias(
        plataforma      = norm.plat,
        entidade_cupom  = eh_entidade_cupom(norm),
        codigos         = tuple(norm.cupons),
        tema_campanha   = tema_da_campanha(texto),
        produtos        = tuple(norm.idents),
        tem_produto     = bool(norm.ids_globais),
        chaves_campanha = tuple(norm.chaves_campanha),
        natureza_cash   = eh_post_cashback(texto, norm.tem_sinal_cashback),
        percentual      = _percentual(texto),
        link_canonico   = norm.ancora_url,
        fingerprint     = _fp4(_alma(texto)),
    )


def ancoras(norm: "MensagemNormalizada") -> list[Ancora]:
    """
    Derivação AUTORITATIVA das âncoras de família (MB v1.1, P3/P6).

    A POLÍTICA de precedência foi extraída para
    pipeline.resolucao_identidade (Fase 1 — extração byte-equivalente,
    sem evolução). Esta função permanece dona de DUAS coisas, que são
    vocabulário do consumidor e não do resolvedor:
      - reunir as evidências do snapshot (_evidencias);
      - rotular cada entidade resolvida na ESPÉCIE de Ancora.

    Separar as duas responsabilidades é o que permite que uma mecânica
    nova mude só a política, num arquivo só, sem tocar em quem emite.
    """
    return [Ancora(e.papel, e.chave) for e in resolver(_evidencias(norm))]


def identidades(norm: "MensagemNormalizada") -> list[str]:
    """VISTA plana de ancoras() — compatibilidade com os consumidores
    atuais (fallback de edição na publicação; ramo sem-produto da
    canônica). A doutrina de emissão vive em ancoras()."""
    return [a.chave for a in ancoras(norm)]
  
