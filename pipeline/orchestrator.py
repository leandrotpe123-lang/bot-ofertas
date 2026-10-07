"""
Camada 2 — Orquestração.

Responsabilidade única: coordenar o fluxo entre as camadas da pipeline.
Recebe eventos do Telegram, aplica as travas de idade de ADMISSÃO e
entrega ao dispatch, que chama cada camada na ordem correta.

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
# Implementação: pipeline.orchestrator_fila (teto de admissão, lane por
# origem, orçamento de execução) e pipeline.orchestrator_pipeline
# (sequência das camadas).
# Este arquivo retém a ADMISSÃO — as travas de idade de entrada — e
# entrega ao dispatch. _enfileirar e _pipeline NÃO são reexportados: são
# contrato interno da camada.
from __future__ import annotations

import eventos
from config import _MAX_IDADE_NOVA_S
from logger import log_sys, _idade_str, _idade_seg
from pipeline import ingestao
from pipeline.completude import EventoRecuperado
from pipeline.identidade import username_de
from pipeline.orchestrator_fila import _enfileirar, _iniciar_orchestrator
from pipeline.vida_oferta import VIDA_OFERTA_S

# [F1.2-A2-E] origem.recebida leva no máximo estes links; os demais só
# contam em links_n (limite declarado do contrato).
_MAX_LINKS_EVENTO = 20


# ── Entrypoint público ───────────────────────────────────────
async def processar(event, is_edit: bool = False) -> None:
    """Chamado pelos handlers do Telethon em `main.py`.

    [F1.2-A2-E] Cada chamada é UMA execução (eventos.execucao): a
    origem.recebida sai antes de qualquer trava; um descarte daqui sai
    como origem.descartada, logo depois do log de sempre; o execucao.fim
    sai na saída do `with` — ou, se a mensagem entrou na fila, quando a
    task dela terminar (_enfileirar transfere a execução). A exceção que
    escapa daqui vira ERRO_ENTRADA (só a classe, em `excecao`) e segue
    adiante, a MESMA. Sem a proveniência ligada, nada disso roda (eventos
    dormente)."""
    with eventos.execucao(lambda: _ids(event)):
        eventos.emitir_de("origem.recebida",
                          lambda: (_ids(event), _retrato(event, is_edit)),
                          local="orchestrator.processar.recebida")
        try:
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
                    eventos.emitir_de("origem.descartada", lambda: (_ids(event), {
                        "motivo": "EDIT_ANTIGO", "etapa": "ADMISSAO", "ponto": "PRE",
                        "is_edit": bool(is_edit), "efeitos_parciais": "NENHUM",
                        "idade_s": round(idade, 1)}),
                        local="orchestrator.processar.edit_antigo")
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
                    eventos.emitir_de("origem.descartada", lambda: (_ids(event), {
                        "motivo": "NOVA_ANTIGA", "etapa": "ADMISSAO", "ponto": "PRE",
                        "is_edit": bool(is_edit), "efeitos_parciais": "NENHUM",
                        "idade_s": round(idade, 1)}),
                        local="orchestrator.processar.nova_antiga")
                    return
            await _enfileirar(event, is_edit)
        except Exception as e:
            eventos.emitir_de("origem.descartada", lambda: (_ids(event), {
                "motivo": "ERRO_ENTRADA", "etapa": "ENTRADA", "ponto": "PRE",
                "is_edit": bool(is_edit), "efeitos_parciais": "NENHUM",
                "excecao": type(e).__name__}),
                local="orchestrator.processar.erro_entrada")
            raise


# ── [F1.2-A2-E] Coletores da entrada — puros: só leem atributos ──
def _ids(event) -> dict:
    """Correlação da execução: o chat como em identidade.chat_canonico (o
    id numérico em texto, a mesma origem da chave da lane) e a mensagem."""
    return {"chat": str(event.chat_id), "msg": int(event.message.id)}


def _retrato(event, is_edit) -> dict:
    """origem.recebida — retrato SEGURO da mensagem que chegou: nunca o
    texto integral nem URL (previa, texto_h12, texto_len e os links como
    referências {host, h12, motivo, plataforma}). Só lê o que o Telethon
    já desserializou; o texto e os links vêm das MESMAS funções da
    ingestão. plataforma fica nula na entrada: resolvê-la é da
    normalização."""
    m = event.message
    texto = ingestao.texto_de(m)
    links = ingestao.links_de(texto)
    data = getattr(m, "date", None)
    editada = getattr(m, "edit_date", None)
    midia = getattr(m, "media", None)
    album = getattr(m, "grouped_id", None)
    return {
        "via": event.via if isinstance(event, EventoRecuperado) else "TELEGRAM",
        "is_edit": bool(is_edit),
        "date": data.isoformat() if data is not None else None,
        "edit_date": editada.isoformat() if editada is not None else None,
        "idade_s": round(_idade_seg(data), 1),
        "grupo": username_de(str(event.chat_id)),
        "previa": eventos.previa(texto),
        "texto_h12": eventos.h12(texto),
        "texto_len": len(texto),
        "links": [eventos.representar_url(u) for u in links[:_MAX_LINKS_EVENTO]],
        "links_n": len(links),
        "midia": {"tipo": type(midia).__name__ if midia is not None else None,
                  "key": ingestao.chave_midia(m)},
        # id de álbum é inteiro de 64 bits: vai em texto (a coleta não guarda
        # inteiro acima de 2^53 — troca por um marcador).
        "grouped_id": str(album) if isinstance(album, int) else None,
        "reply_to": getattr(getattr(m, "reply_to", None), "reply_to_msg_id", None),
        "encaminhada": _encaminhada(getattr(m, "fwd_from", None)),
    }


def _encaminhada(fwd):
    """Encaminhada de CANAL: {canal (id canônico, como `chat`), msg}. De
    usuário, oculta ou desconhecida: {canal: None, msg: None} — id de
    usuário NUNCA entra. Não encaminhada: None."""
    if fwd is None:
        return None
    canal = getattr(getattr(fwd, "from_id", None), "channel_id", None)
    if not isinstance(canal, int):
        return {"canal": None, "msg": None}
    post = getattr(fwd, "channel_post", None)
    return {"canal": str(-(10 ** 12 + canal)),
            "msg": post if isinstance(post, int) else None}
