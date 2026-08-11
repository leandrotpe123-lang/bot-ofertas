"""
Camada 4 — Throttle de reativação.

Responsabilidade única: barrar flood de reposts de reativação. Não
deriva identidade, não calcula score e não reexporta nada: consome
enr.canonica e enr.tipo, prontos do enriquecimento (P2/P8).

A decisão de família vive na publicação; a janela do ciclo, em
vida_oferta. O claim de identidade foi aposentado em F4 — não
decidia (ambos os ramos passavam).
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
#
# Implementação do mecanismo de claim: pipeline.deduplicacao_claim.
# Este arquivo retém a POLÍTICA — detectar reativação, escolher a
# janela por tipo, decidir e logar. _atomic_check_and_claim é
# CHAMADO aqui, não reexportado.
from __future__ import annotations
import time

from logger import log_ded
from pipeline.reativacao import eh_reativacao
# ── Camada fina de compatibilidade (Front 1, passo de extração) ──

from pipeline.normalizacao import MensagemNormalizada
from pipeline.enriquecimento import MensagemEnriquecida
from utils.hashes import _fp4

# Contrato INTERNO da camada — ver cabeçalho de deduplicacao_claim.
from pipeline.deduplicacao_claim import _atomic_check_and_claim

# ── Constantes ───────────────────────────────────────────────────
# Janela curta usada APENAS pra reativação ("voltou", "reativado").
# Permite uma reativação legítima passar mas bloqueia flood quando
# múltiplos grupos mandam "voltou" do mesmo evento em sequência.
_JANELA_REATIVACAO_S = 30.0
# Reativação de CUPOM: janela longa (10 min) — regra do domínio cupom.
# Produto e campanha continuam em _JANELA_REATIVACAO_S (30s). Não mistura.
_JANELA_REATIVACAO_CUPOM_S = 600.0

# Detecção de reativação: delegada ao dono canônico (pipeline.reativacao.
# eh_reativacao). Esta camada mantém APENAS o throttle anti-flood acima —
# não detecta linguagem de retorno por conta própria.

# ─────────────────────────────────────────────────────────────────
# REATIVAÇÃO
# ─────────────────────────────────────────────────────────────────
async def _checar_reativacao(norm: MensagemNormalizada) -> bool:
    """Verifica se o post é uma reativação válida. Detecção delegada ao
    dono canônico (pipeline.reativacao); esta camada só decide o throttle."""
    if not eh_reativacao(norm.texto_limpo):
        return False
    return True

# ─────────────────────────────────────────────────────────────────
# DEVE_ENVIAR — entrypoint da camada 4
# ─────────────────────────────────────────────────────────────────
async def deve_enviar_async(enr: MensagemEnriquecida) -> bool:
    """
    Decide se a mensagem prossegue pra montagem/publicação.

    Retorna True quando há identidade — a camada 6 (enviar) decide
    via score se publica, edita ou ignora.

    Retorna False APENAS pra bloquear flood de reativação (múltiplas
    mensagens "voltou" do mesmo evento em sequência).
    """
    try:
        norm        = enr.norm
        texto       = norm.texto_limpo
        plat        = norm.plat
        ids_globais = norm.ids_globais
        chat        = (norm.chat or "").lower()

        # ── TIPO + IDENTIDADE (prontos do enriquecimento) ─────────
        #   Consumidos de enr; o efeito de cupom já ocorreu lá, 1x.
        tipo     = enr.tipo
        identity = enr.canonica

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
                # Repetição dentro da janela → flood, para TODOS os tipos.
                # Opção A ratificada: cupom com códigos novos deixou de ser
                # exceção. Uma reativação por identidade/janela; o throttle é
                # o único dono dessa pergunta (sem segundo relógio).
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
                             
