"""
Camada 2 — Orquestração.

Responsabilidade única: coordenar o fluxo entre as camadas da pipeline.
Recebe eventos do Telegram, enfileira com prioridade, gerencia worker
e chama cada camada na ordem correta.

Camadas chamadas em sequência:
    1. Ingestão       → pipeline.ingestao.ingerir
    2. Idempotência   → pipeline.idempotencia.ja_processado
    3. Coalescing     → pipeline.coalescing.deve_coalescer
    4. Normalização   → pipeline.normalizacao.normalizar
    5. Deduplicação   → pipeline.deduplicacao.deve_enviar_async
    6. Montagem       → pipeline.montagem.montar
    7. Publicação     → pipeline.publicacao.enviar (com is_edit)

NÃO faz:
  - leitura de texto do evento (ingestão é a fonte oficial)
  - decisão de coalescing (camada própria)
  - controle de idempotência (camada própria)
  - decisão de edição (publicação resolve)
  - acesso a banco
  - lógica de plataforma
"""
#
# Implementação: pipeline.orchestrator_fila (heap, prioridade, TTL,
# workers) e pipeline.orchestrator_pipeline (sequência das camadas).
# Este arquivo retém a ADMISSÃO — as travas de idade de entrada — e
# entrega à fila. _enfileirar e _pipeline NÃO são reexportados: são
# contrato interno da camada.
from __future__ import annotations

from config import _MAX_IDADE_NOVA_S
from logger import log_sys, _idade_str, _idade_seg
from pipeline.orchestrator_fila import _enfileirar, _iniciar_orchestrator
from pipeline.vida_oferta import VIDA_OFERTA_S


# ── Entrypoint público ────────────────────────────────────────────
async def processar(event, is_edit: bool = False) -> None:
    """Chamado pelos handlers do Telethon em `main.py`."""
    # ── Trava: edição de mensagem antiga (fora da vida da oferta) ─────
    # Uma edição só pode pertencer a um ciclo se chegou dentro da vida
    # operacional (VIDA_OFERTA_S, da autoridade vida_oferta). Mais velha
    # que isso é post reeditado tarde na origem → descarta JÁ NA ENTRADA,
    # antes da fila. Quem casa/evolui/sincroniza é a cadeia rio abaixo
    # (overlap V3 filtra por ciclo vivo). NÃO afeta NewMessage. Idade -1
    # não corta.
    if is_edit:
        idade = _idade_seg(event.message.date)
        if idade > VIDA_OFERTA_S:
            log_sys.info(
                f"🧭 TL | id={event.message.id} chat={event.chat_id} | "
                f"DESCARTE | motivo=EDIT_ANTIGO "
                f"idade={_idade_str(event.message.date)}")
            return
    # ── Trava NOVA_ANTIGA: oferta NOVA velha (frescor de entrada) ─────
    # Mensagem nova com idade > _MAX_IDADE_NOVA_S (120s) é oferta velha
    # ressurgida → descarta na entrada. NÃO atrasa nada novo: a rajada
    # acontece em segundos, MUITO abaixo de 120s, então passa inteira.
    if not is_edit:
        idade = _idade_seg(event.message.date)
        if idade > _MAX_IDADE_NOVA_S:
            log_sys.info(
                f"🧭 TL | id={event.message.id} chat={event.chat_id} | "
                f"DESCARTE | motivo=NOVA_ANTIGA "
                f"idade={_idade_str(event.message.date)}")
            return
    await _enfileirar(event, is_edit)
            
