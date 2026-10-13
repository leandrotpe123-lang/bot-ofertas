"""
Proveniência [F1.2] — CATÁLOGO v1: tipos de evento, enums e rótulos.

Fonte ÚNICA dos nomes que o worker pode emitir. O Brain lê a versão e o
hash no processo.iniciado e interpreta o resto por este catálogo.

REGRAS (contrato F1.2, A.9):
  - acrescentar valor sobe a versão MENOR (quem lê trata valor
    desconhecido como DESCONHECIDO); renomear ou remover sobe a MAIOR;
  - valores de prova em minúsculas (vocabulário A.5); os demais enums em
    MAIÚSCULAS;
  - LOCAIS são os rótulos dos pontos de emissão: cada frente registra os
    seus junto com o ponto (tests/test_eventos_contrato.py confere que
    todo emitir_de usa tipo, local e enum daqui);
  - só constantes: sem I/O e sem import fora da stdlib. resumo() nunca
    levanta.
"""
from __future__ import annotations

import hashlib
import json
from typing import Dict, FrozenSet, Optional

VERSAO = 1

# tipo → resultado de execução que ele encerra (None: não encerra).
# Um evento terminal emitido dentro de uma execução vira o desfecho dela
# no execucao.fim (o último terminal vale).
TIPOS: Dict[str, Optional[str]] = {
    # F1.1 — ciclo do processo (emitidos só por main.py)
    "processo.iniciado": None,
    "processo.online": None,
    "processo.vida": None,
    "processo.encerrando": None,
    # execução (emitido pela própria coleta)
    "execucao.fim": None,
    # origem
    "origem.recebida": None,
    "origem.descartada": "DESCARTADA",
    "origem.buraco": None,
    "origem.recuperada": None,
    "origem.recuperacao_falhou": None,
    "origem.apagada": None,
    # oferta e decisão
    "oferta.normalizada": None,
    "oferta.identificada": None,
    "decisao.tomada": None,
    "decisao.redirecionada": None,
    # post
    "post.publicado": "PUBLICADA",
    "post.renascido": "RENASCIDA",
    "post.publicacao_falhou": "FALHOU",
    "post.evoluido": "EVOLUIDA",
    "post.sincronizado": "SINCRONIZADA",
    "post.substituido": "SUBSTITUIDA",
    "post.substituicao_falhou": "FALHOU",
    "post.midia_trocada": "MIDIA_TROCADA",
    "post.midia_nao_aplicada": "MIDIA_NAO_APLICADA",
    "post.edicao_falhou": "FALHOU",
    "post.sync_falhou": "FALHOU",
    "post.remocao": None,
    "post.lideranca_transferida": None,
    "post.sucessao_pendente": None,
    # família e duplicata (candidata: NUNCA veredito)
    "familia.fundida": None,
    "familia.conflito": None,
    "familia.ancora_absorvida": None,
    "duplicata.candidata": None,
    # espelho de cupons
    "espelho.publicado": None,
    "espelho.recriado": None,
    "espelho.editado": None,
    "espelho.substituido": None,
    "espelho.removido": None,
    "espelho.falhou": None,
    "espelho.orfao": None,
    # canal de destino, plataformas, leitura da fonte pelo worker
    "canal.mensagem_apagada": None,
    "plataforma.alerta": None,
    "fonte.consultada": None,
}

ENUMS: Dict[str, FrozenSet[str]] = {
    "via": frozenset({"TELEGRAM", "RECUPERACAO", "SUCESSAO"}),
    "tipo_execucao": frozenset({"MENSAGEM", "EXCLUSAO", "MANUTENCAO"}),
    "resultado_execucao": frozenset({
        "DESCARTADA", "PUBLICADA", "RENASCIDA", "EVOLUIDA", "SINCRONIZADA",
        "SUBSTITUIDA", "MIDIA_TROCADA", "MIDIA_NAO_APLICADA", "IGNORADA",
        "FALHOU", "ERRO", "CANCELADA", "SEM_DESFECHO"}),
    "motivo_descarte": frozenset({
        "EDIT_ANTIGO", "NOVA_ANTIGA", "ENCERRANDO", "FILA_CHEIA",
        "ERRO_ENTRADA", "ERRO_WORKER", "ERRO_INGESTAO", "ORIGEM_APAGADA",
        "JA_PROCESSADO", "ORIGEM_JA_PUBLICADA", "ERRO_NORMALIZAR",
        "NORMALIZACAO_VAZIA", "DEDUP", "ERRO_DEDUP", "ERRO_MONTAR",
        "REDIRECIONAMENTO_ESGOTADO"}),
    "sub_motivo_descarte": frozenset({
        "TEXTO_VAZIO", "VETO_INSTITUCIONAL", "SEM_CONTEXTO", "SEM_LINKS",
        "ZERO_CONVERTIDOS", "REATIVACAO_FLOOD", "DESCONHECIDO"}),
    "etapa": frozenset({"ENTRADA", "ADMISSAO", "FILA", "PIPELINE",
                        "PUBLICACAO"}),
    "ponto_descarte": frozenset({"PRE", "SOB_LOCK"}),
    "efeitos_parciais": frozenset({"NENHUM", "POSSIVEIS"}),
    "motivo_recuperacao": frozenset({"ERRO_BUSCA", "AUSENTE", "SERVICO",
                                     "ENTREGA_FALHOU", "ABORTADA"}),
    "situacao_origem_apagada": frozenset({
        "MORTO", "MANTIDO", "SEM_VINCULO", "SEM_POST", "JA_REMOVIDO",
        "ERRO", "MUDOU"}),
    "desfecho_link": frozenset({
        "CONVERTIDO", "PRESERVADO", "SEM_CONVERSAO", "BLOQUEADO",
        "EXPANSAO_FALHOU", "SEM_PLATAFORMA", "AUSENTE", "EXCECAO"}),
    "motivo_url_protegida": frozenset({
        "LONGA", "INVALIDA", "CREDENCIAL", "PARAMETROS", "AFILIADA",
        "CODIGO"}),
    "causa_redirecionamento": frozenset({"FUSAO", "ADOCAO_OBSOLETA"}),
    "operacao_edicao": frozenset({"EVOLUCAO", "UPGRADE_MIDIA"}),
    "fase_delete_antigo": frozenset({"SUCESSO", "SEM_EFEITO", "FALHA",
                                     "FLOODWAIT_ABORTADO", "DESCONHECIDO"}),
    "fase_novo_envio": frozenset({"SUCESSO", "FALHA", "FLOODWAIT_ABORTADO",
                                  "NAO_TENTADO", "DESCONHECIDO"}),
    "estado_reconciliacao": frozenset({
        "COERENTE", "FANTASMA", "FANTASMA_PREEXISTENTE", "SUBSTITUIDO",
        "SUBSTITUIDO_SEM_PROVA_BANCO", "ORFAO", "DESCONHECIDO"}),
    # post.remocao sai por TENTATIVA e no DESFECHO: `fase` desfaz a
    # ambiguidade sem um segundo tipo de evento.
    "fase_remocao": frozenset({"TENTATIVA", "DESFECHO"}),
    "resultado_remocao": frozenset({"REMOVIDO", "FALHOU", "ADIADA"}),
    "motivo_remocao": frozenset({"FUSAO", "ORIGEM_APAGADA"}),
    "motivo_sucessao_pendente": frozenset({"CICLO_FECHADO", "BUSCA_FALHOU"}),
    "sinal_duplicata": frozenset({
        "SOBREPOSICAO_PENDENTE", "OUTRO_DESTINO", "CONTAINER_RECUSADO",
        "RENASCIMENTO", "ANCORA_PRESERVADA", "REMOCAO_FALHOU",
        "POST_SEM_REGISTRO"}),
    "veredito_worker": frozenset({"NAO_AVALIADO", "BLOQUEADO",
                                  "MANTIDO_POR_REGRA"}),
    "alerta_plataforma": frozenset({
        "DISJUNTOR_ABERTO", "DISJUNTOR_FECHADO", "SESSAO_EXPIRANDO",
        "SESSAO_EXPIRADA", "SESSAO_RESTAURADA", "PLUGIN_RECUSADO"}),
    # Vocabulário de prova (A.5). Só `confirmado` (e `aplicada`) confirmam;
    # `nao_verificavel` NUNCA equivale a gravado.
    "prova_telegram": frozenset({
        "confirmado", "sem_mudanca", "aceito_sem_efeito_provado", "falhou",
        "nao_tocado", "desconhecido"}),
    "prova_banco": frozenset({
        "confirmado", "falhou_sem_escrita", "falhou_parcial",
        "nao_verificavel", "nao_aplicavel"}),
    "prova_midia": frozenset({"aplicada", "nao_aplicada", "nao_tentada",
                              "nao_aplicavel"}),
}

# Rótulos dos pontos de emissão: um por ponto, registrado pela frente que
# liga o ponto. A1: o fim de execução, emitido pela própria coleta. A2-E:
# o funil de entrada (pipeline/orchestrator*.py), até a chamada a enviar.
# A2-R: a completude e a sucessão (pipeline/completude.py e
# pipeline/sucessao.py), que não abrem execução. Exclusão: origem.apagada
# (pipeline/origem_apagada.py), na raiz EXCLUSAO de cada id apagado.
LOCAIS: FrozenSet[str] = frozenset({
    "eventos.execucao.fim",
    # A2-E — origem.recebida e os descartes antes da fila
    "orchestrator.processar.recebida",
    "orchestrator.processar.edit_antigo",
    "orchestrator.processar.nova_antiga",
    "orchestrator.processar.erro_entrada",
    "orchestrator_fila.enfileirar.encerrando",
    "orchestrator_fila.enfileirar.fila_cheia",
    "orchestrator_fila.blindado.erro_worker",
    # A2-E — os descartes PRE do _pipeline
    "orchestrator_pipeline.pipeline.erro_ingestao",
    "orchestrator_pipeline.pipeline.origem_apagada",
    "orchestrator_pipeline.pipeline.ja_processado",
    "orchestrator_pipeline.pipeline.origem_ja_publicada",
    "orchestrator_pipeline.pipeline.erro_normalizar",
    "orchestrator_pipeline.pipeline.normalizacao_vazia",
    "orchestrator_pipeline.pipeline.dedup",
    "orchestrator_pipeline.pipeline.erro_dedup",
    "orchestrator_pipeline.pipeline.erro_montar",
    # A2-R — a completude: buraco, busca e entrega da recuperada
    "completude.observar.buraco",
    "completude.observar.buraco_grande",
    "completude.buscar_e_entregar.erro_busca",
    "completude.buscar_e_entregar.ausente",
    "completude.buscar_e_entregar.servico",
    "completude.buscar_e_entregar.recuperada",
    "completude.buscar_e_entregar.entrega_falhou",
    "completude.recuperar.abortada",
    # A2-R — a sucessão da chefe
    "sucessao.suceder.ciclo_fechado",
    "sucessao.suceder.busca_falhou",
    "sucessao.suceder.lideranca_transferida",
    # Exclusão — a origem apagada, depois de _uma
    "origem_apagada.apagadas.apagada",
})

# Chaves que a coleta preenche: o que o coletor puser nelas é sobrescrito.
RESERVADAS_CORR: FrozenSet[str] = frozenset({"exec", "exec_pai"})
RESERVADAS_DADOS: FrozenSet[str] = frozenset({
    "local", "ts_fato", "degradado", "erro_tipo", "cortado"})


def terminal(tipo: str) -> Optional[str]:
    """Resultado de execução que `tipo` encerra, ou None."""
    return TIPOS.get(tipo)


def valido(enum: str, valor) -> bool:
    """`valor` pertence ao enum `enum` deste catálogo?"""
    return valor in ENUMS.get(enum, ())


def _canonico(tipos, enums, locais) -> str:
    return json.dumps(
        {"versao": VERSAO,
         "tipos": sorted([k, v or ""] for k, v in tipos.items()),
         "enums": {k: sorted(v) for k, v in sorted(enums.items())},
         "locais": sorted(locais)},
        ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _hash(tipos, enums, locais) -> str:
    return hashlib.sha256(
        _canonico(tipos, enums, locais).encode("ascii")).hexdigest()[:16]


_RESUMO: Optional[dict] = None


def resumo() -> dict:
    """{versao, hash, tipos} para o processo.iniciado. Calculado uma vez,
    sob demanda (nada roda no import). Nunca levanta."""
    global _RESUMO
    try:
        if _RESUMO is None:
            _RESUMO = {"versao": VERSAO, "hash": _hash(TIPOS, ENUMS, LOCAIS),
                       "tipos": len(TIPOS)}
        return dict(_RESUMO)
    except Exception:                                  # noqa: BLE001
        return {"versao": VERSAO, "hash": None, "tipos": None}
