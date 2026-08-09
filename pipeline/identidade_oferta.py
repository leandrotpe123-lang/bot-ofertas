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

from logger import log_enr
from pipeline.memoria_cupom import buscar_identidade
from pipeline.normalizacao import MensagemNormalizada
from pipeline.natureza import eh_entidade_cupom
from utils.hashes import _fp4
from utils.textos import _alma
from utils.urls import _cache_key


# ── API pública ──────────────────────────────────────────────────
__all__ = [
    "identidades",
    "identidade_canonica",
    "ancoras",
]


# ── KILL-SWITCH do domínio cupom ─────────────────────────────────
# True  → identidade de cupom por CÓDIGO COMPARTILHADO (índice).
# False → comportamento antigo (um código / fingerprint do conjunto).
# Vire False para reverter NA HORA se algum cupom legítimo sumir.

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
)

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


def ancoras(norm: "MensagemNormalizada") -> list[Ancora]:
    """
    Derivação AUTORITATIVA das âncoras de família (MB v1.1, P3/P6):
    com PRODUTO presente, cupom e cashback são ATRIBUTOS e não ancoram —
    só ancoram quando o post não tem produto. camp| segue emitida até a
    fase per-link (§11.11; risco declarado em §11.5). Sem oferta
    estruturada, percorre a hierarquia de resolvers (mesma da canônica,
    sem chamá-la — quebra a circularidade do Estágio 3).
    """
    plat = norm.plat
    texto = norm.texto_limpo
    saida: list[Ancora] = []
    vistos: set[str] = set()

    def _add(especie: str, chave: str) -> None:
        if chave not in vistos:
            vistos.add(chave)
            saida.append(Ancora(especie, chave))

    # ══ C3 — AUTORIDADE DO ASSUNTO ══
    # Quando o CUPOM é o assunto do post (classificador endurecido no C2),
    # a oferta É o cupom — o produto no link é apenas VEÍCULO/ilustração e
    # NÃO ancora. Sem isso, o mesmo cupom relâmpago ilustrado com produtos
    # diferentes por grupos diferentes viraria N ofertas distintas.
    #
    # A âncora é EXCLUSIVA (só o cupom), nunca aditiva: se o produto
    # ilustrativo também ancorasse, um post-vitrine capturaria a oferta
    # legítima daquele produto quando ela chegasse.
    #
    # Segurança: o C2 garante que "Echo Dot R$249 (cupom ECHO10)" NÃO é
    # post de cupom (cupom como complemento) — logo produtos distintos que
    # compartilham um código genérico seguem em famílias separadas.
    # Gate R1×R2 (MB ratificado): com produto identificado, o cupom só
    # ancora exclusivo se o post caracterizar CLARAMENTE benefício de
    # loja; na dúvida, prevalece o produto (cupom vira atributo).
    # R2+ (emenda ratificada): 2+ códigos e nenhum preço de item →
    # a lista de códigos É a oferta; o produto presente é vitrine.
    if eh_entidade_cupom(norm):
        if norm.cupons:
            # COM código: o código É a identidade (elemento mais estável
            # disponível — INV-E3; troca de código na vida da campanha é
            # responsabilidade do Motor de Estado, Fase 3).
            for cod in norm.cupons:
                _add("cupom", f"{plat}|cup|{cod.upper()}")
        else:
            # [F-C4] SEM código: a identidade é o TEMA da campanha
            # (INV-E2/E3: benefício/percentual/limite são ESTADO e nunca
            # entram na chave). Sem tema → "geral" (bucket do ciclo, R5).
            # cupb| NÃO é código: não entra no cupom_idx (que só indexa
            # códigos reais) — garantido por construção, pois o efeito de
            # cupom só roda sobre norm.cupons, aqui vazio.
            _add("cupom-beneficio",
                 f"{plat}|cupb|{tema_da_campanha(texto)}")
        return saida

    for plat_link, pid, _tipo in norm.idents:
        _add("produto", f"{plat_link}|{pid}")

    for k in norm.chaves_campanha:
        _add("campanha", f"{plat}|camp|{k}")

    if not norm.ids_globais:
        for cod in norm.cupons:
            _add("cupom", f"{plat}|cup|{cod.upper()}")

        if eh_post_cashback(texto, norm.tem_sinal_cashback):
    # [F-C4 / INV-E2] O MB nomeia cashback como ESTADO: o
            # percentual varia (e pode faltar) entre mensagens legítimas
            # da mesma campanha. A identidade é a NATUREZA na plataforma
            # dentro do ciclo (tolerância declarada: campanhas de
            # cashback simultâneas na mesma plataforma colapsam).
            _add("cashback", f"{plat}|cash")

    if saida:
        return saida

    # ── Cauda: sem oferta estruturada ────────────────────────────
    # MB §11.9 (ratificado): evento é FALLBACK, não espécie própria —
    # âncora por palavra-chave colidia entre eventos distintos da mesma
    # mecânica. Ordem: link primeiro, texto como terminal. Nunca vazio.
    primeira_url = next(iter(norm.mapa.values()), None) if norm.mapa else None
    if primeira_url:
        log_enr.info(f"🔬 CAN3 id={norm.msg_id} plat={plat} fonte=norm.mapa url={primeira_url[:70]} tem_canonica={hasattr(norm, 'canonicas')}")
        return [Ancora("fallback", f"{plat}|url|{_cache_key(primeira_url)}")]
    return [Ancora("fallback", f"{plat}|txt|{_fp4(_alma(texto))}")]


def identidades(norm: "MensagemNormalizada") -> list[str]:
    """VISTA plana de ancoras() — compatibilidade com os consumidores
    atuais (fallback de edição na publicação; ramo sem-produto da
    canônica). A doutrina de emissão vive em ancoras()."""
    return [a.chave for a in ancoras(norm)]
  
