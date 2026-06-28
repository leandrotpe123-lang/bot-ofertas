"""
Camada 4 — Deduplicação inteligente, score evolutivo e identidade
canônica.

Responsabilidade única: decidir duplicidade. A deduplicação é a
autoridade de DECISÃO sobre duplicidade; não é autoridade de
derivação de identidade.

CONSUMO DE IDENTIDADE DERIVADA:
  A identidade — de produto e de campanha — é derivada pela
  normalização, autoridade única dessa derivação, sobre as URLs
  afiliadas LONGAS e antes do encurtamento terminal. Esta camada
  CONSOME os campos já derivados (ids_globais, sku, chave_campanha,
  tem_host_campanha) e NÃO os reextrai do mapa.

  O campo mapa, quando a deduplicação executa, já contém URLs na
  forma de publicação — possivelmente encurtadas. Reinterpretá-lo
  para derivar identidade violaria a invariante de que a URL curta
  jamais participa de derivação de identidade. A única leitura
  legítima do mapa nesta camada é o nível 6 da identidade canônica,
  fallback operacional NÃO semântico e explicitamente reconhecido.
"""
from __future__ import annotations
import re
import time
from typing import Optional, Tuple

import config
from database import (
    db_set_dedupe,
    db_cupom_idx_buscar,
    db_cupom_idx_registrar,
)
import globals as g
from logger import log_ded
from pipeline.estado_evento import _KW_EVENTO, _RE_RETORNO
from pipeline.normalizacao import MensagemNormalizada
from pipeline.identidade import username_de
from utils.cupom import _KW_CUPOM, extrair_todos_cupons
from utils.hashes import _fp4
from utils.textos import (
    _alma,
    _cupons_set,
    _benef_set,
)
from utils.urls import _cache_key


# ── Constantes ───────────────────────────────────────────────────
# Janela curta usada APENAS pra reativação ("voltou", "reativado").
# Permite uma reativação legítima passar mas bloqueia flood quando
# múltiplos grupos mandam "voltou" do mesmo evento em sequência.
_JANELA_REATIVACAO_S = 30.0
# Reativação de CUPOM: janela longa (10 min) — regra do domínio cupom.
# Produto e campanha continuam em _JANELA_REATIVACAO_S (30s). Não mistura.
_JANELA_REATIVACAO_CUPOM_S = 600.0

# ── KILL-SWITCH do domínio cupom ─────────────────────────────────
# True  → identidade de cupom por CÓDIGO COMPARTILHADO (índice).
# False → comportamento antigo (um código / fingerprint do conjunto).
# Vire False para reverter NA HORA se algum cupom legítimo sumir.
_CUPOM_IDX_ON = True

# ─────────────────────────────────────────────────────────────────
# Detecção do TIPO do post (define qual identidade usar)
# ─────────────────────────────────────────────────────────────────
_RE_TITULO_CUPOM = re.compile(
    r'\b(?:cupom|cupons|c[oó]digo|coupon|novo\s+cupom|cupom\s+novo)\b',
    re.I,
)
_RE_TITULO_CASHBACK = re.compile(
    r'\b(?:cashback|cash\s*back)\b',
    re.I,
)
_RE_PCT = re.compile(r'(\d{1,2})\s*%')

# Resíduo NOMEADO de calendário comercial — fronteira explícita.
# NÃO pertence à família interativa canônica (_KW_EVENTO, dono:
# pipeline.estado_evento) e NÃO deve subir para lá: no canônico,
# este vocabulário furaria a saturação e mudaria títulos no pico
# comercial. Serve apenas ao gate/chave de identidade de campanha.
_RE_CALENDARIO_COMERCIAL = re.compile(
    r'\b(?:black\s*friday|esquenta)\b',
    re.I,
)

# Padrão "lista de cupons" — formato típico:
#   "🔥 R$ 100 OFF em R$ 900: INFLU100"
_RE_LINHA_CUPOM_LISTA = re.compile(
    r'(?:r\$\s*\d+|\d+\s*%)\s+off(?:\s+em\s+r\$\s*\d+)?\s*:\s*[A-Z0-9][A-Z0-9_-]{3,19}',
    re.I,
)

# Padrão "OFF: CODIGO" no título
_RE_TITULO_OFF_COD = re.compile(
    r'(?:r\$\s*\d+|\d+\s*%)\s+off(?:\s+em\s+r\$\s*\d+)?\s*:\s*[A-Z0-9][A-Z0-9_-]{3,19}',
    re.I,
)

# Linha de cashback (sem precisar de "OFF" literal). Usado pra
# detectar posts Shopee de cashback como post-cupom.
_RE_CASHBACK_LINHA = re.compile(
    r'\d+\s*%\s+cash\s*back',
    re.I,
)


def _eh_lista_cupons(texto: str) -> bool:
    """
    Detecta se o post é uma LISTA DE CUPONS (não um cupom único).
    Critério: 2+ linhas no formato "R$ X OFF em R$ Y: CODIGO".
    """
    linhas = texto.strip().split("\n")
    linhas_lista = sum(
        1 for l in linhas
        if _RE_LINHA_CUPOM_LISTA.search(l)
    )
    return linhas_lista >= 2


def _eh_post_cupom(texto: str) -> bool:
    """
    Detecta se o post é 'tipo cupom' — onde o cupom é o ASSUNTO PRINCIPAL.

    Casos cobertos:
      a) palavra "cupom"/"cupons"/"código" no título
      b) título já é "R$ X OFF: CODIGO" / "X% OFF: CODIGO"
      c) 2+ linhas formato lista de cupons (delegada a _eh_lista_cupons)
      d) cashback nas primeiras linhas + código presente
      e) linha "X% Cashback ... : CODIGO" (mesmo sem palavra cupom)

    Casos (d) e (e) cobrem cards Shopee com título genérico ("Leo Indica
    / Ofertas Insanas") + linha "🎟️ 50% Cashback ... : BRUIANHEZ10".
    """
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]

    # Caso (a): palavra "cupom" no título
    if _RE_TITULO_CUPOM.search(titulo):
        return True

    # Caso (b): título já é "R$ X OFF: CODIGO" / "X% OFF: CODIGO"
    if _RE_TITULO_OFF_COD.search(titulo):
        return True

    # Caso (c): 2+ linhas formato lista (reusa _eh_lista_cupons)
    if _eh_lista_cupons(texto):
        return True

    # Caso (d): cashback presente nas primeiras 5 linhas + cupom
    # mencionado no texto inteiro
    primeiras = "\n".join(linhas[:5])
    if (_RE_CASHBACK_LINHA.search(primeiras)
            and _KW_CUPOM.search(texto)):
        return True

    # Caso (e): linha "X% Cashback ... : CODIGO" no formato KV
    for linha in linhas[:6]:
        if (_RE_CASHBACK_LINHA.search(linha)
                and re.search(r':\s*[A-Z0-9][A-Z0-9_-]{3,19}\b', linha)):
            return True

    return False


def _eh_post_cashback(texto: str, tem_sinal_cashback: bool) -> bool:
    """
    Detecta se o post é especificamente sobre cashback (sem cupom code).
    Combina o vocabulário genérico de cashback no título (universal)
    com o sinal derivado tem_sinal_cashback — vocabulário específico
    de plataforma, declarado pela própria plataforma e composto pelo
    registry. A dedup não conhece mais termos de marca diretamente.
    """
    linhas = [l for l in texto.strip().split("\n") if l.strip()]
    if not linhas:
        return False
    titulo = linhas[0]
    if _RE_TITULO_CASHBACK.search(titulo):
        return True
    return tem_sinal_cashback


def _eh_post_evento(texto: str, tem_host_campanha: bool) -> bool:
    """
    Detecta evento/campanha/roleta (sem ASIN, sem cupom claro).

    CONSUMO DE IDENTIDADE DERIVADA:
      Esta função NÃO reinterpreta o mapa de URLs. A identidade de
      campanha é derivada pela normalização — autoridade única dessa
      derivação — sobre as URLs afiliadas LONGAS, antes do
      encurtamento terminal. A deduplicação apenas consome o campo
      derivado tem_host_campanha.

    A detecção combina dois sinais independentes:
      - vocabulário de evento no texto, em duas famílias nomeadas:
        a interativa canônica (_KW_EVENTO, dono: estado_evento) e o
        resíduo local de calendário comercial — classificação de
        conteúdo, legítima nesta camada por operar sobre o texto;
      - presença de host de campanha: consumida do campo derivado.
    """
    if _KW_EVENTO.search(texto[:200]):
        return True
    if _RE_CALENDARIO_COMERCIAL.search(texto[:200]):
        return True
    return tem_host_campanha


def _eh_reativacao(texto: str) -> bool:
    """
    Detecta linguagem de reativação ('voltou', 'reativado' etc.).
    Consome o vocabulário canônico _RE_RETORNO (dono: estado_evento);
    a DECISÃO — gate anti-flood de 30s — permanece desta camada, e o
    escopo de busca ([:300]) é do chamador, não do vocabulário.
    """
    return bool(_RE_RETORNO.search(texto[:300]))


def _extrair_pct_cashback(texto: str) -> str:
    """Extrai o percentual de cashback (ex: '30' de 'cashback 30%')."""
    primeiras = " ".join(texto.split("\n")[:5])
    m = _RE_PCT.search(primeiras)
    return m.group(1) if m else ""


# ─────────────────────────────────────────────────────────────────
# Atomic locks (in-memory, evita race entre tasks)
# ─────────────────────────────────────────────────────────────────
async def _get_atomic_lck():
    return g._atomic_lck_obj


_ATOMIC_TTL_MAX = 4 * 60 * 60      # 4h
_ATOMIC_CLEANUP_THRESHOLD = 500


def _cleanup_atomic_mem_locked() -> int:
    """
    Remove entradas antigas de g._atomic_mem.
    DEVE ser chamado com g._atomic_lck_obj já adquirido.
    """
    if len(g._atomic_mem) <= _ATOMIC_CLEANUP_THRESHOLD:
        return 0
    agora = time.monotonic()
    antigos = [
        k for k, ts in g._atomic_mem.items()
        if agora - ts > _ATOMIC_TTL_MAX
    ]
    for k in antigos:
        g._atomic_mem.pop(k, None)
    return len(antigos)


async def _atomic_check_and_claim(fp: str, janela: float) -> Tuple[bool, Optional[float]]:
    """
    Atômico: verifica se fp existe DENTRO da janela e, se não, faz claim.
    Retorna (na_janela, ts_existente).
      - na_janela=True  → identidade já está sendo processada/foi recente
      - na_janela=False → claim feito agora, primeira vez nessa janela

    Tudo em UM lock pra evitar race entre check e claim. Ao reentrar
    dentro da janela, atualiza o timestamp pra estender.
    """
    async with (await _get_atomic_lck()):
        agora = time.monotonic()
        # Cleanup oportunista
        removidos = _cleanup_atomic_mem_locked()
        if removidos:
            log_ded.debug(
                f"🧹 _atomic_mem cleanup: removidos {removidos} | "
                f"restam {len(g._atomic_mem)}"
            )
        ts = g._atomic_mem.get(fp)
        if ts is not None and (agora - ts) < janela:
            # Atualiza pra estender
            g._atomic_mem[fp] = agora
            return True, ts
        # Claim
        g._atomic_mem[fp] = agora
        return False, ts


# ─────────────────────────────────────────────────────────────────
# Score evolutivo
# ─────────────────────────────────────────────────────────────────
# ── Bônus de QUANTIDADE (riqueza) no score — D3 ──────────────────
# Aditivos sobre a presença; recompensam ofertas ALÉM da primeira,
# com teto para a quantidade não dominar a qualidade. Tunáveis.
_SCORE_POR_LINK_EXTRA  = 2
_SCORE_POR_CUPOM_EXTRA = 2
_MAX_EXTRAS_CONTADOS   = 3


def calcular_score(norm: MensagemNormalizada) -> int:
    """
    Calcula score de qualidade do post.
    Mídia tem peso configurável: grupos em config._GRUPOS_IMG_RUIM
    recebem peso reduzido. A PRESENÇA de links/cupom mantém o peso
    histórico; a QUANTIDADE (ofertas além da primeira) soma por cima,
    com teto — é o que faz o post mais rico vencer pelo score (D3).
    """
    texto = norm.texto_limpo
    score = 0

    if norm.mapa:
        score += 3
    if re.search(r'r\$\s*[\d.,]+', texto, re.I):
        score += 2
    if norm.cupom:
        score += 2
    if re.search(r'\d+\s*%\s*off', texto, re.I):
        score += 2
    if re.search(r'r\$\s*[\d.,]+\s*off', texto, re.I):
        score += 2
    if re.search(r'(acima|mínimo|min)\s+de\s+r\$', texto, re.I):
        score += 1
    if re.search(r'frete\s+gr[aá]t', texto, re.I):
        score += 1
    if norm.sku:
        score += 1

    # ── Quantidade (riqueza): ofertas ALÉM da primeira somam, com teto.
    #    A presença acima fica intacta → posts de 1 link/1 cupom NÃO mudam.
    n_links_extra = max(0, len(norm.mapa) - 1)
    if n_links_extra:
        score += min(n_links_extra, _MAX_EXTRAS_CONTADOS) * _SCORE_POR_LINK_EXTRA
    n_cupons = len(extrair_todos_cupons(texto, getattr(norm, "code_entities", None)))
    n_cupons_extra = max(0, n_cupons - 1)
    if n_cupons_extra:
        score += min(n_cupons_extra, _MAX_EXTRAS_CONTADOS) * _SCORE_POR_CUPOM_EXTRA

    # Mídia: peso varia conforme grupo de origem. A identidade é numérica;
    # resolvemos o @username via identidade p/ consultar a lista legível.
    if norm.tem_midia:
        if username_de(norm.chat) in config._GRUPOS_IMG_RUIM:
            score += config._SCORE_MIDIA_RUIM
        else:
            score += config._SCORE_MIDIA_NORMAL

    return score

# ─────────────────────────────────────────────────────────────────
# IDENTIDADE CANÔNICA — coração do sistema anti-duplicação
# ─────────────────────────────────────────────────────────────────
def _detectar_tipo_oferta(norm: MensagemNormalizada) -> str:
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
    if norm.cupom and _eh_post_cupom(texto):
        return "cupom"

    # P3: tem cupom mas sem ID — cupom standalone
    if norm.cupom and not norm.ids_globais:
        return "cupom"

    # (P4 removido: a prioridade de produto em P1 já cobre todo caso
    #  com ids_globais; este ramo havia se tornado inalcançável.)

    # P5: cashback sem cupom code
    if _eh_post_cashback(texto, norm.tem_sinal_cashback):
        return "evento"

    # P6: campanha/evento — consome o campo derivado tem_host_campanha
    if _eh_post_evento(texto, norm.tem_host_campanha):
        return "evento"

    # Fallback
    if norm.cupom:
        return "cupom"
    return "evento"


def _janela_por_tipo(tipo: str) -> float:
    """Retorna a janela de dedupe em segundos pelo tipo da oferta."""
    if tipo == "cupom":   return float(config._JANELA_CUPOM_S)
    if tipo == "produto": return float(config._JANELA_PRODUTO_S)
    return float(config._JANELA_EVENTO_S)

def _id_cupom_indexado(norm, plat: str, texto: str, fallback: str) -> str:
    """Resolve a identidade de cupom pelo ÍNDICE POR CÓDIGO.

    Se QUALQUER código do post já foi visto (plat + código) dentro da
    janela de cupom, reusa a identidade daquele post. Senão usa
    `fallback` (identidade nova). Em ambos os casos registra TODOS os
    códigos sob a identidade resolvida — preserva todos, não só um. O
    link nunca participa: a chave é código + plataforma."""
    codes = sorted(set(
        c.upper() for c in extrair_todos_cupons(
            texto, getattr(norm, "code_entities", None)
        ) if c
    ))
    if not codes:
        return fallback
    janela = float(config._JANELA_CUPOM_S)
    existente = db_cupom_idx_buscar(plat, codes, janela)
    identity = existente or fallback
    # max() preserva a contagem REAL de códigos novos: identidade_canonica
    # roda 2x por post (uma no dedup, outra no enviar). Na 2ª vez os
    # códigos já estão no índice e registrar devolve 0 — sem o max,
    # _cupom_novos zeraria e o cupom enriquecido nunca editaria o post.
    norm._cupom_novos = max(
        getattr(norm, "_cupom_novos", 0),
        db_cupom_idx_registrar(plat, codes, identity))
    return identity


def _id_post_cupom(norm, plat, texto):
    # POST DE CUPOM vence produto: quando o cupom é o ASSUNTO do post
    # (_eh_post_cupom), a oferta É o cupom — não o produto-veículo. Dois
    # posts do mesmo código (ex.: CURTEAI) com produtos diferentes são a
    # MESMA oferta. Produto só vence quando NÃO é post de cupom.
    if not norm.cupom or not _eh_post_cupom(texto):
        return None
    fallback = f"{plat}|cup|{norm.cupom.upper()}"
    if _CUPOM_IDX_ON:
        return _id_cupom_indexado(norm, plat, texto, fallback)
    return fallback


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
    if _CUPOM_IDX_ON:
        return _id_cupom_indexado(norm, plat, texto, fallback)
    return fallback


def _id_cashback(norm, plat, texto):
    if not _eh_post_cashback(texto, norm.tem_sinal_cashback) or norm.cupom:
        return None
    pct = _extrair_pct_cashback(texto)
    if not pct:
        return None
    return f"{plat}|cash|{pct}"


def _id_campanha(norm, plat, texto):
    if not _eh_post_evento(texto, norm.tem_host_campanha):
        return None
    if norm.chave_campanha:
        return f"{plat}|camp|{norm.chave_campanha}"
    candidatos = [
        m for m in (
            _KW_EVENTO.search(texto[:200]),
            _RE_CALENDARIO_COMERCIAL.search(texto[:200]),
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
    Chave estável de dedupe, DERIVADA do conjunto identidades().

    A canônica é uma VISTA determinística de identidades(): escolhe o
    representante lex-menor (sorted[0] — independente de ordem) e aplica a
    grudação transitiva por código (_id_cupom_indexado) por cima. Assim há
    UM só cálculo de identidade de oferta (identidades), e a canônica é
    função dele — não um caminho paralelo.

    A grudação preserva a transitividade do índice de cupom: se qualquer
    código do post já foi visto na janela, a identidade da corrente
    sobrepõe a base. Famílias de cupom colapsam mesmo quando a base (o
    produto-veículo) diverge entre posts do mesmo código.
    """
    base = sorted(identidades(norm))[0]
    return _id_cupom_indexado(norm, norm.plat, norm.texto_limpo, base)

def identidades(norm: "MensagemNormalizada") -> list[str]:
    """
    Conjunto de identidades de OFERTA do post (modelo container/oferta).
    Cada produto, campanha, cupom e cashback é uma oferta — emitidos em
    UNIÃO, sem colapso por min(). Por D4, post de cupom também emite os
    produtos associados. Sem oferta estruturada, percorre a mesma
    hierarquia de resolvers da canônica (sem chamá-la — quebra a
    circularidade que o Estágio 3 exige).

    ADITIVO: ainda NÃO consumido. identidade_canonica segue como chave
    única até o consumo per-oferta ser ligado (Estágio 3).
    """
    plat = norm.plat
    texto = norm.texto_limpo
    ofertas: list[str] = []

    def _add(chave: str) -> None:
        if chave not in ofertas:
            ofertas.append(chave)

    for pid in norm.ids_globais:
        _add(f"{plat}|{pid}")

    for k in norm.chaves_campanha:
        _add(f"{plat}|camp|{k}")

    for cod in extrair_todos_cupons(texto, getattr(norm, "code_entities", None)):
        _add(f"{plat}|cup|{cod.upper()}")

    if _eh_post_cashback(texto, norm.tem_sinal_cashback):
        pct = _extrair_pct_cashback(texto)
        if pct:
            _add(f"{plat}|cash|{pct}")

    if ofertas:
        return ofertas
    # Fallback AUTOSSUFICIENTE: percorre a MESMA hierarquia da canônica, sem
    # chamar identidade_canonica (quebra a circularidade do Estágio 3). Byte-
    # idêntico por construção (incl. efeito colateral do índice de cupom).
    for resolver in _HIERARQUIA_IDENTIDADE:
        ident = resolver(norm, plat, texto)
        if ident is not None:
            return [ident]
    return [f"{plat}|txt|{_fp4(_alma(texto))}"] 


# ─────────────────────────────────────────────────────────────────
# REATIVAÇÃO
# ─────────────────────────────────────────────────────────────────
async def _checar_reativacao(norm: MensagemNormalizada) -> bool:
    """Verifica se o post é uma reativação válida."""
    if not _eh_reativacao(norm.texto_limpo):
        return False
    return True

def _persistir_dedupe(fp, plat, cupons, alma, tipo, ids_globais, benef, cupom_id):
    # id_prod e cupom_id são as colunas canônicas de identidade —
    # produto e cupom — para toda plataforma. cupom_id é o cupom
    # representativo já em caixa alta, derivado no chamador a partir
    # da mesma fonte da identity (norm.cupom). Sem ramo por plataforma.
    id_prod = ids_globais[0] if ids_globais else ""
    db_set_dedupe(fp, plat, cupons, alma, tipo, id_prod, benef, cupom_id)


# ─────────────────────────────────────────────────────────────────
# DEVE_ENVIAR — entrypoint da camada 4
# ─────────────────────────────────────────────────────────────────
async def deve_enviar_async(norm: MensagemNormalizada) -> bool:
    """
    Decide se a mensagem prossegue pra montagem/publicação.

    Retorna True quando há identidade — a camada 6 (enviar) decide
    via score se publica, edita ou ignora.

    Retorna False APENAS pra bloquear flood de reativação (múltiplas
    mensagens "voltou" do mesmo evento em sequência).
    """
    try:
        texto       = norm.texto_limpo
        plat        = norm.plat
        ids_globais = norm.ids_globais
        cupons      = _cupons_set(texto)
        alma_v      = _alma(texto)
        benef       = _benef_set(texto)
        chat        = (norm.chat or "").lower()

        # ── TIPO + IDENTIDADE + JANELA ────────────────────────────
        tipo     = _detectar_tipo_oferta(norm)
        identity = identidade_canonica(norm)
        janela   = _janela_por_tipo(tipo)

        # ── REATIVAÇÃO ────────────────────────────────────────────
        # Permite a reativação real passar (uma vez por evento), mas
        # bloqueia flood quando múltiplos grupos mandam "voltou" do
        # mesmo evento dentro de _JANELA_REATIVACAO_S.
        if await _checar_reativacao(norm):
            fp_reativ = _fp4(f"reativ|{identity}")
            janela_reativ = (
                _JANELA_REATIVACAO_CUPOM_S if tipo == "cupom"
                else _JANELA_REATIVACAO_S
            )
            na_janela, ts_ant = await _atomic_check_and_claim(
                fp_reativ, janela_reativ,
            )
            if na_janela:
                novos = getattr(norm, "_cupom_novos", 0)
                if tipo == "cupom" and novos > 0:
                    log_ded.info(
                        f"♻️ [REATIVACAO_MAIS_CODIGOS] {identity} "
                        f"novos={novos} chat={chat} → enviar() decide"
                    )
                    return True
                delta = int(time.monotonic() - ts_ant) if ts_ant else 0
                log_ded.info(
                    f"♻️ [REATIVACAO_FLOOD] {identity} delta={delta}s "
                    f"chat={chat} → bloqueada (já reativou recente)"
                )
                return False
            log_ded.info(
                f"♻️ [REATIVACAO_OK] {identity} tipo={tipo} "
                f"chat={chat} → enviar() decide"
            )
            return True

        # ── CHECK + CLAIM ATÔMICO ────────────────────────────────
        fp_identity = _fp4(f"identity|{identity}")
        na_janela, ts_anterior = await _atomic_check_and_claim(
            fp_identity, janela,
        )
        if na_janela:
            log_ded.info(
                f"🔄 [IDENTITY_NA_JANELA] {identity} tipo={tipo} "
                f"delta={int(time.monotonic() - ts_anterior)}s "
                f"chat={chat} → enviar() decide"
            )
            return True

        # ── PERSISTÊNCIA ─────────────────────────────────────────
        _persistir_dedupe(
            fp_identity, plat, list(cupons), alma_v, tipo,
            ids_globais, list(benef), (norm.cupom or "").upper(),
              )

        log_ded.info(
            f"✅ [PASSOU] {identity} tipo={tipo} chat={chat} "
            f"→ enviar() decide"
        )
        return True

    except Exception as e:
        # Em caso de erro inesperado, deixa passar pra não bloquear
        # ofertas legítimas.
        log_ded.error(f"❌ ERRO DEDUPE: {e}", exc_info=True)
        return True
