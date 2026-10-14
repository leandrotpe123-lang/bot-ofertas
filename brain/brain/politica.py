"""Política determinística: veredito do juiz + contexto → ação do Brain.

A IA só recomenda (a `acao_sugerida` dela é registrada e NUNCA usada aqui);
quem decide é esta tabela, pura e testável. A ordem importa: primeiro o que
NUNCA age, depois os limiares, por fim o modo e os limites por hora.

NUNCA AGE (no máximo registra ou alerta):
  · configuração (modelo, effort) não validada pelo portão de teste;
  · INCERTO;
  · post já apagado (o alvo ou o candidato);
  · fora da janela de vida da oferta (1 h);
  · live com produtos diferentes (só o link da live em comum) — nunca funde;
  · conteúdo exclusivo no post que sairia — fundir apagaria esse conteúdo;
  · lembrete que chegou minutos depois — é alerta legítimo para o leitor.

Ações possíveis: NADA, REGISTRAR, ALERTAR, PEDIR_FUNDIR, PEDIR_REINJETAR.
PEDIR_* é só um PEDIDO ao worker, que revalida o estado e pega os locks
normais antes de executar (docs/arquitetura/brain-v0.md §F)."""
from __future__ import annotations

from dataclasses import dataclass

from brain import config

ACOES = ("NADA", "REGISTRAR", "ALERTAR", "PEDIR_FUNDIR", "PEDIR_REINJETAR")
TIPOS = ("par_canal", "absorvido", "sem_post")


@dataclass(frozen=True)
class Contexto:
    tipo: str                         # par_canal | absorvido | sem_post
    delta_s: float                    # do candidato até o alvo
    em_janela: bool = True
    alvo_apagado: bool = False
    candidato_apagado: bool = False
    so_container_live: bool = False   # só o link da live em comum
    exclusivo_no_perdedor: bool = False
    modo: str = config.BRAIN_MODO
    acoes_ultima_hora: int = 0
    alertas_ultima_hora: int = 0


@dataclass(frozen=True)
class Decisao:
    acao: str                         # o que o Brain faz neste modo
    faria: str                        # o que faria em ACAO (para a sombra)
    motivo: str


def _base(v, ctx: Contexto):
    """(ação que faria em ACAO, motivo), antes de modo e limites."""
    if v.veredito == "INCERTO":
        return "REGISTRAR", "incerto"
    if ctx.alvo_apagado or ctx.candidato_apagado:
        return "NADA", "post_apagado"
    if not ctx.em_janela:
        return "NADA", "fora_da_janela"
    c = v.certeza
    if v.veredito == "MESMA_CAMPANHA":
        if ctx.tipo != "par_canal":
            return "NADA", "absorcao_correta"
        if v.lembrete and ctx.delta_s >= config.LEMBRETE_MIN_S:
            return "NADA", "lembrete_alerta_legitimo"
        if ctx.so_container_live:
            return ("ALERTAR" if c >= config.CERTEZA_MIN_ALERTA else "REGISTRAR",
                    "live_nunca_funde")
        if ctx.exclusivo_no_perdedor:
            return ("ALERTAR" if c >= config.CERTEZA_MIN_ALERTA else "REGISTRAR",
                    "conteudo_exclusivo")
        if c >= config.CERTEZA_MIN_ACAO:
            return "PEDIR_FUNDIR", "mesma_campanha"
        if c >= config.CERTEZA_MIN_ALERTA:
            return "ALERTAR", "mesma_campanha_certeza_media"
        return "REGISTRAR", "certeza_baixa"
    if v.veredito == "LIBERACAO_NOVA":
        if ctx.tipo == "par_canal":
            return "NADA", "liberacao_ja_tem_post"
        if c >= config.CERTEZA_MIN_ACAO:
            return "PEDIR_REINJETAR", "liberacao_perdida"
        if c >= config.CERTEZA_MIN_ALERTA:
            return "ALERTAR", "liberacao_perdida_certeza_media"
        return "REGISTRAR", "certeza_baixa"
    if v.veredito == "PRODUTOS_DIFERENTES":
        if ctx.tipo == "absorvido":
            # fusão/absorção errada: só humano decide nesta fase
            return ("ALERTAR" if c >= config.CERTEZA_MIN_ALERTA else "REGISTRAR",
                    "absorcao_errada")
        return "NADA", "separados_correto"
    return "REGISTRAR", "veredito_desconhecido"


def decidir(v, ctx: Contexto) -> Decisao:
    faria, motivo = _base(v, ctx)
    if (v.modelo, v.effort) not in config.CONFIGS_VALIDADAS and faria not in ("NADA",):
        return Decisao("REGISTRAR", faria, f"config_nao_validada:{motivo}")
    modo = ctx.modo if ctx.modo in config.MODOS else "SOMBRA"
    acao = faria
    if modo == "SOMBRA" and acao not in ("NADA",):
        acao = "REGISTRAR"
    elif modo == "ALERTA" and acao.startswith("PEDIR_"):
        acao = "ALERTAR"
    if acao.startswith("PEDIR_") and ctx.acoes_ultima_hora >= config.ACOES_POR_HORA:
        acao, motivo = "ALERTAR", f"limite_acoes_hora:{motivo}"
    if acao == "ALERTAR" and ctx.alertas_ultima_hora >= config.ALERTAS_POR_HORA:
        acao, motivo = "REGISTRAR", f"limite_alertas_hora:{motivo}"
    return Decisao(acao, faria, motivo)
