"""
Camada 3 — Normalização.

Responsabilidade única: transformar uma MensagemBruta em uma
MensagemNormalizada, orquestrando a resolução e a afiliação dos
links contidos na mensagem, e produzindo os metadados derivados
necessários às camadas seguintes.

AUTORIDADE DE DERIVAÇÃO DE IDENTIDADE:
  A normalização é a autoridade única de derivação de identidade
  semântica. Toda identidade — produto e campanha — é derivada das
  URLs afiliadas LONGAS, antes do encurtamento terminal, e
  transportada adiante como dado consolidado. As camadas seguintes
  consomem identidade já derivada; NENHUMA delas reextrai identidade
  a partir do mapa.

SEPARAÇÃO CONCEITUAL — FRONTEIRA RÍGIDA:
  - ids_globais, sku, chave_campanha, tem_host_campanha :
        identidade canônica, derivada das URLs afiliadas LONGAS.
  - mapa :
        tabela de tradução de URL para publicação. Após o bloco
        terminal de encurtamento, NÃO é fonte de identidade
        semântica. Exceção explícita: o nível 6 de identidade_canonica
        usa o mapa como fallback operacional NÃO semântico.
  - URL afiliada longa : base semântica.
  - URL curta          : forma terminal de publicação; nunca
                         participa de identidade ou deduplicação.

CONTRATO COM A DEDUPLICAÇÃO:
  Os campos chave_campanha e tem_host_campanha são parte do contrato
  consumido pela deduplicação. A sua derivação é responsabilidade
  exclusiva desta camada. A deduplicação os consome e não reextrai
  identidade de campanha a partir do mapa.

Operações que constituem a normalização:
  - limpeza estrutural do texto (limpar_texto)
  - resolução e afiliação de links (via registry e contrato)
  - derivação de identidade (produto e campanha)
  - encurtamento terminal das URLs de publicação

NÃO faz:
  - filtragem de conteúdo (responsabilidade de pipeline.filtros)
  - detecção/validação de cupom (responsabilidade de utils.cupom)
  - resolução de URL via rede (responsabilidade de utils.url_resolver)
  - registro de posts pendentes (responsabilidade do orquestrador)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from globals import _log_cache_stats
from logger import log_nrm
from pipeline.ingestao import MensagemBruta
from pipeline.normalizacao_identidade import (
    derivar_ancora_url,
    derivar_campanha,
    derivar_produto,
    remover_cupons_da_entidade,
)
from pipeline.normalizacao_links import (
    _encurtar_mapa,
    particionar_links,
    resolver_e_afiliar,
)
from pipeline.normalizacao_texto import (_tem_emoji, limpar_texto,
                                         sem_marcacao)
from utils.cupom import extrair_todos_cupons

# ── API pública do módulo ─────────────────────────────────────────
__all__ = [
    "MensagemNormalizada", "normalizar", "limpar_texto",
    "_tem_emoji",
]

# ─────────────────────────────────────────────────────────────────
# CONTRATO DE SAÍDA
# ─────────────────────────────────────────────────────────────────
@dataclass
class MensagemNormalizada:
    msg_id:            int
    chat:              str
    texto_limpo:       str
    mapa:              Dict[str, str]
    preservar:         List[str]
    plat:              str
    sku:               str
    tem_midia:         bool
    # Projeção de ANÁLISE de texto_limpo, sem marcação de
    # apresentação. Os detectores semânticos (natureza, assunto)
    # consomem ESTE campo; a publicação consome texto_limpo.
    # ATENÇÃO: as posições de caractere NÃO são intercambiáveis entre
    # os dois campos — nada deve localizar em um o que achou no outro.
    texto_analise:     str          = ""
    media_obj:         object
    ids_globais:       List[str]    = field(default_factory=list)
    # Vínculo POR LINK que a plataforma entrega via contrato e que a
    # travessia PRESERVA: (plataforma_do_link, id_produto, tipo_link).
    # ids_globais é projeção de idents — mesma passada de derivação.
    idents: List[Tuple[str, str, str]] = field(default_factory=list)
    # Lista completa de cupons — snapshot derivado UMA vez na normalização
    # (extrair_todos_cupons sobre texto_limpo). Consumida por identidade e
    # score. É o ÚNICO fato de cupom do contrato (Cupom-1).
    cupons:            List[str]    = field(default_factory=list)
    # ── Identidade de campanha derivada — CONTRATO COM A DEDUPLICAÇÃO.
    # Derivados das URLs afiliadas LONGAS, antes do encurtamento.
    # Derivação é responsabilidade EXCLUSIVA da normalização; a
    # deduplicação os consome e não reextrai identidade do mapa.
    chave_campanha:    str          = ""
    chaves_campanha:   List[str]    = field(default_factory=list)
    tem_host_campanha: bool         = False
    tem_sinal_cashback: bool        = False
    # Identidade de FALLBACK derivada — chave de URL usada quando o post
    # não tem oferta estruturada. Derivada da URL afiliada LONGA canônica
    # pela normalização; os consumidores apenas a leem, nunca a recalculam.
    ancora_url:        str          = ""
    # Entidades de código (monospace) capturadas na ingestão — insumo que
    code_entities:     List[str]    = field(default_factory=list)
    # ──────────────────────────────────────────────────────────────
    is_reply:          bool         = False
    reply_to:          int          = 0
    is_override:       bool         = False


# ─────────────────────────────────────────────────────────────────
# ENTRYPOINT — NORMALIZAÇÃO
# ─────────────────────────────────────────────────────────────────
async def normalizar(
    bruta: MensagemBruta,
    is_override: bool = False,
) -> Optional[MensagemNormalizada]:
    """
    Transforma uma MensagemBruta em uma MensagemNormalizada.

    ORDEM DAS OPERAÇÕES (invariante de ordem):
      1. limpeza e viabilidade do texto
      2. resolução e afiliação dos links (URLs afiliadas LONGAS)
      3. derivação de identidade — produto E campanha — sobre as
         URLs afiliadas LONGAS
      4. encurtamento terminal do mapa — ÚLTIMA transformação
      5. construção da MensagemNormalizada
    """
    if not bruta.texto.strip():
        return None

    from pipeline.institucional import deve_descartar, tem_contexto
    veto = deve_descartar(bruta.texto)
    if veto:
        log_nrm.info(f"🚫 Post vetado | motivo='{veto}' | @{bruta.chat}")
        return None

    if not tem_contexto(bruta.texto):
        return None

    from pipeline.filtros import filtrar
    texto_limpo = filtrar(limpar_texto(bruta.texto))

    converter, preservar_lst = particionar_links(bruta.links)
  
    if not converter and not preservar_lst:
        return None
      
    mapa, canonicas, plats = await resolver_e_afiliar(
        converter, bruta.msg_id)

    if converter and not mapa and not preservar_lst:
        log_nrm.warning(f"🚫 Zero links convertidos | @{bruta.chat}")
        return None

    plat_dom = max(set(plats), key=plats.count) if plats else ""
    cupons   = extrair_todos_cupons(texto_limpo, getattr(bruta, "code_entities", None))

    # ── DERIVAÇÃO DE IDENTIDADE ───────────────────────────────────
    # Opera EXCLUSIVAMENTE sobre as URLs afiliadas LONGAS (conv),
    # antes de qualquer encurtamento. A URL original (orig) NÃO é
    # fonte de identidade: a fonte canônica é a URL afiliada longa.
    # A normalização é a autoridade única de derivação — produto
    # (ids_globais, sku) e campanha (chave_campanha, tem_host_campanha).
    urls_longas = list(canonicas.values())

    ids_globais, idents, sku = derivar_produto(urls_longas)

    cupons = remover_cupons_da_entidade(cupons, ids_globais)

    (tem_host_campanha, chave_campanha,
     chaves_campanha, tem_sinal_cashback) = derivar_campanha(
        urls_longas, texto_limpo)

    ancora_url = derivar_ancora_url(urls_longas)
    # ── ENCURTAMENTO TERMINAL ─────────────────────────────────────
    # ÚLTIMA transformação antes do retorno. Toda a identidade —
    # produto e campanha — já foi derivada acima sobre as URLs
    # longas. A partir daqui o mapa passa a conter a forma de
    # publicação. Nenhuma lógica pode ser inserida entre a derivação
    # de identidade e este ponto sem revisão arquitetural.
    mapa_publicacao, n_encurtadas = _encurtar_mapa(mapa)

    # Política de bloco: só permanece o bloco que gerou oferta
    # publicável. Depende do mapa de publicação, por isso roda
    # Projeção de ANÁLISE do texto final. Derivada DEPOIS de
    # filtrar_blocos de propósito: os dois campos descrevem sempre o
    # mesmo conteúdo, e nunca podem divergir em blocos.
    #
    # texto_limpo  → o que vai ao ar (marcação preservada)
    # texto_analise → o que os detectores semânticos leem
    #
    # A marcação do Telegram é artefato de APRESENTAÇÃO e alterava a
    # natureza da oferta: "**R$30 OFF** em **R$60**" fazia a compra
    # mínima virar preço de item, e o post de cupom virar produto.
    texto_analise = sem_marcacao(texto_limpo)
    # aqui e não junto da filtragem de linha.
    from pipeline.filtros import filtrar_blocos
    texto_limpo = filtrar_blocos(
        texto_limpo, mapa_publicacao, preservar_lst)

    log_nrm.info(
    f"✅ id={bruta.msg_id} {len(mapa_publicacao)}/{len(converter)} | "
    f"plat={plat_dom or 'none'} cupons={cupons} sku={sku} "
    f"ids_globais={ids_globais} "
    f"chave_campanha='{chave_campanha}' "
    f"chaves_campanha={chaves_campanha} "
    f"ancora_url=…{ancora_url[-60:]} len={len(ancora_url)} "
    f"encurtadas={n_encurtadas} " 
    f"override={is_override}"
  )
    _log_cache_stats()

    return MensagemNormalizada(
        msg_id=bruta.msg_id, chat=bruta.chat, texto_limpo=texto_limpo,
        texto_analise=texto_analise,
        mapa=mapa_publicacao, preservar=preservar_lst, plat=plat_dom,
        cupons=cupons, sku=sku, tem_midia=bruta.tem_midia,
        media_obj=bruta.media_obj,
        ids_globais=ids_globais, idents=idents,
        chave_campanha=chave_campanha,
        chaves_campanha=chaves_campanha,
        tem_host_campanha=tem_host_campanha,
        tem_sinal_cashback=tem_sinal_cashback,
        ancora_url=ancora_url,
        code_entities=bruta.code_entities,
        is_reply=bruta.is_reply,
        reply_to=bruta.reply_to, is_override=is_override,
  )
