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
  - tipo_de_oferta(norm)      -> str        : "produto" | "cupom" | "evento"

CONSUMO DE IDENTIDADE DERIVADA:
  A identidade de produto e de campanha é derivada pela normalização —
  autoridade única dessa derivação — sobre as URLs afiliadas LONGAS e
  antes do encurtamento terminal. Esta camada CONSOME os campos já
  derivados (ids_globais, sku, chave_campanha, chaves_campanha,
  tem_host_campanha, tem_sinal_cashback) e NÃO os reextrai do mapa. A
  única leitura legítima do mapa é o nível de fallback operacional NÃO
  semântico de _id_url, explicitamente reconhecido.

DELEGAÇÃO DE EFEITO (C4.2):
  A memória de códigos foi extraída para pipeline.memoria_cupom, que é o
  dono único do índice cupom_idx e da contagem norm._cupom_novos. Esta
  camada DERIVA identidade e DELEGA a resolução por código — não lê nem
  escreve banco por conta própria. O efeito colateral continua existindo,
  mas agora tem casa própria e fronteira declarada.

NÃO faz:
  - decisão de duplicidade / claim / janela  (deduplicação)
  - cálculo de score                         (deduplicação → score)
  - publicação / edição / substituição       (publicação)
  - normalização / afiliação de links        (normalização)
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from pipeline.estado_evento import _KW_EVENTO
from pipeline.memoria_cupom import resolver_identidade
from pipeline.normalizacao import MensagemNormalizada
from utils.cupom import _KW_CUPOM
from utils.hashes import _fp4
from utils.textos import _alma
from utils.urls import _cache_key


# ── API pública ──────────────────────────────────────────────────
__all__ = [
    "identidades",
    "identidade_canonica",
    "tipo_de_oferta",
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
    beneficio_e_de_loja,
    tem_preco_de_item,
    tema_da_campanha,
    buscar_calendario_comercial,
    eh_post_cashback,
    eh_post_cupom,
    eh_post_evento,
    extrair_pct_cashback,
)

# ─────────────────────────────────────────────────────────────────
# TIPO DA OFERTA
# ─────────────────────────────────────────────────────────────────
def tipo_de_oferta(norm: MensagemNormalizada) -> str:
    """
    Detecta o TIPO da oferta (independente de plataforma).
      - "cupom"   : tem cupom code claro E é post centrado em cupom
      - "produto" : tem ID de produto (ASIN, SKU, ItemID)
      - "evento"  : campanha/roleta/sem ID claro
    """
    texto = norm.texto_limpo

    # P1: tem ID de produto → produto. PRIORIDADE PRODUTO sobre cupom:
    # um produto pode trazer cupom embutido (ex.: Shopee); a presença
    # de id de produto define o tipo, e o cupom passa a ser ATRIBUTO
    # do produto, não um evento de cupom separado.
    if norm.ids_globais:
        return "produto"

    # P2: é post-cupom (cupom domina o título) → cupom
    if norm.cupom and eh_post_cupom(texto):
        return "cupom"

    # P3: tem cupom mas sem ID — cupom standalone
    if norm.cupom and not norm.ids_globais:
        return "cupom"

    # (P4 removido: a prioridade de produto em P1 já cobre todo caso
    #  com ids_globais; este ramo havia se tornado inalcançável.)

    # P5: cashback sem cupom code
    if eh_post_cashback(texto, norm.tem_sinal_cashback):
        return "evento"

    # P6: campanha/evento — consome o campo derivado tem_host_campanha
    if eh_post_evento(texto, norm.tem_host_campanha):
        return "evento"

    # Fallback
    if norm.cupom:
        return "cupom"
    return "evento"


# ─────────────────────────────────────────────────────────────────
# IDENTIDADE CANÔNICA — coração do sistema anti-duplicação
# ─────────────────────────────────────────────────────────────────
def _id_post_cupom(norm, plat, texto):
    # POST DE CUPOM vence produto: quando o cupom é o ASSUNTO do post
    # (_eh_post_cupom), a oferta É o cupom — não o produto-veículo. Dois
    # posts do mesmo código (ex.: CURTEAI) com produtos diferentes são a
    # MESMA oferta. Produto só vence quando NÃO é post de cupom.
    if not norm.cupom or not eh_post_cupom(texto):
        return None
    fallback = f"{plat}|cup|{norm.cupom.upper()}"
    return resolver_identidade(norm, plat, fallback)


def _id_produto(norm, plat, texto):
    # Produto vence cupom APENAS quando NÃO é post de cupom (esse caso já
    # foi resolvido por _id_post_cupom acima). Aqui: produto comum, cujo
    # cupom eventual é melhoria avaliada por SCORE em enviar(), não dup.
    if not norm.ids_globais:
        return None
    return f"{plat}|{min(norm.ids_globais)}"


def _id_cupom_sem_produto(norm, plat, texto):
    # AUTORIDADE DO CUPOM — fato objetivo, não heurística de formato.
    # Condição exclusiva: cupom válido extraído E ausência de produto.
    # `not norm.ids_globais` é redundante com a precedência (produto já
    # foi avaliado antes), mas é DECLARADO aqui para que a autoridade do
    # nível não dependa da posição na sequência.
    if not norm.cupom or norm.ids_globais:
        return None
    fallback = f"{plat}|cup|{norm.cupom.upper()}"
    return resolver_identidade(norm, plat, fallback)


def _id_cashback(norm, plat, texto):
    if not eh_post_cashback(texto, norm.tem_sinal_cashback) or norm.cupom:
        return None
    pct = extrair_pct_cashback(texto)
    if not pct:
        return None
    return f"{plat}|cash|{pct}"


def _id_campanha(norm, plat, texto):
    if not eh_post_evento(texto, norm.tem_host_campanha):
        return None
    if norm.chave_campanha:
        return f"{plat}|camp|{norm.chave_campanha}"
    candidatos = [
        m for m in (
            _KW_EVENTO.search(texto[:200]),
            buscar_calendario_comercial(texto),
        ) if m
    ]
    if candidatos:
        primeiro = min(candidatos, key=lambda m: m.start())
        return f"{plat}|camp|{primeiro.group(0).lower()}"
    return None


def _id_url(norm, plat, texto):
    # Fallback operacional NÃO semântico — única leitura do mapa aqui.
    if not norm.mapa:
        return None
    primeira_url = next(iter(norm.mapa.values()), None)
    if not primeira_url:
        return None
    return f"{plat}|url|{_cache_key(primeira_url)}"


def _id_texto(norm, plat, texto):
    # Fallback terminal: nunca devolve None.
    return f"{plat}|txt|{_fp4(_alma(texto))}"


# A ordem desta tupla É a precedência. Não há precedência implícita.
_HIERARQUIA_IDENTIDADE = (
    _id_post_cupom, 
    _id_produto,
    _id_cupom_sem_produto,
    _id_cashback,
    _id_campanha,
    _id_url,
    _id_texto,
)


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
    segue aplicada por cima em ambos os ramos: se qualquer código do post
    já foi visto na janela, a identidade da corrente sobrepõe a base.
    """
    if _eh_entidade_cupom(norm):
        # [F-C4 / INV-E5] A natureza decidida pelo gate alimenta TODAS
        # as camadas: se a entidade é a campanha de cupom, a canônica
        # (claims/dedup) é a do cupom — nunca a do produto-vitrine.
        base = sorted(identidades(norm))[0]
    elif norm.ids_globais: 
        _idents = getattr(norm, "idents", None) or [
            (norm.plat, pid, "") for pid in norm.ids_globais]
        base = min(f"{p}|{i}" for p, i, _ in _idents)  
    else:
        base = sorted(identidades(norm))[0]
    return resolver_identidade(norm, norm.plat, base)


@dataclass(frozen=True)
class Ancora:
    """Âncora tipada de família (MB v1.1, P6). `chave` é byte-idêntica à
    string historicamente emitida por identidades() — que agora é VISTA."""
    especie: str   # "produto" | "cupom" | "campanha" | "cashback" | "fallback"
    chave: str


_ESPECIE_POR_TAG = {"cup": "cupom", "cupb": "cupom-beneficio",
                    "camp": "campanha", "cash": "cashback",
                    "url": "fallback", "txt": "fallback"}


def _especie_da_chave(chave: str) -> str:
    # Só para chaves nascidas no FALLBACK da hierarquia (camp/url/txt na
    # prática; cupom lá é inalcançável — união não-vazia quando há código).
    partes = chave.split("|", 2)
    return _ESPECIE_POR_TAG.get(partes[1], "produto") if len(partes) >= 2 else "produto"

def _eh_entidade_cupom(norm) -> bool:
    """[F-C4 / INV-E5] Decisão ÚNICA de natureza cupom-como-entidade
    (gate R1×R2 + R2+). Alimenta âncoras E canônica — nenhuma camada
    pode enxergar uma natureza diferente das demais."""
    texto = norm.texto_limpo
    return eh_post_cupom(texto) and (
        not norm.ids_globais
        or beneficio_e_de_loja(texto)
        or (len(norm.cupons) >= 2 and not tem_preco_de_item(texto)))


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
    if _eh_entidade_cupom(norm):
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

    _idents = getattr(norm, "idents", None) or [
        (plat, pid, "") for pid in norm.ids_globais]
    for plat_link, pid, _tipo in _idents:
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
    # Fallback AUTOSSUFICIENTE — mesma hierarquia, mesmos efeitos (inalcançáveis
    # para cupom aqui), byte-idêntico por construção.
    for resolver in _HIERARQUIA_IDENTIDADE:
        ident = resolver(norm, plat, texto)
        if ident is not None:
            return [Ancora(_especie_da_chave(ident), ident)]
    chave = f"{plat}|txt|{_fp4(_alma(texto))}"
    return [Ancora("fallback", chave)]


def identidades(norm: "MensagemNormalizada") -> list[str]:
    """VISTA plana de ancoras() — compatibilidade com os consumidores
    atuais (fallback de edição na publicação; ramo sem-produto da
    canônica). A doutrina de emissão vive em ancoras()."""
    return [a.chave for a in ancoras(norm)]
  
