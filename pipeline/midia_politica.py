"""
Camada de política de MÍDIA — autoridade única sobre a imagem publicada.

Responsabilidade ÚNICA: responder "a imagem publicada deve ser trocada
por esta nova?". NÃO consulta score, texto, líder, edit_count nem
janela. A decisão de CONTEÚDO é de pipeline.decisao; esta é a de MÍDIA,
e nenhuma das duas pode produzir efeito colateral na outra.

TRI-ESTADO de post_estado.midia_chat:
    None  (NULL)  → legado, anterior à Fase 2: classe DESCONHECIDA
    ""            → post publicado SEM mídia
    "<chat>"      → origem da mídia atualmente publicada

A distinção entre None e "" é essencial: os dois exigem comportamentos
opostos (preservar vs aceitar a primeira imagem) e seriam
indistinguíveis numa coluna nullable sem sentinela.

MONOTÔNICA: só sobe de classe. Mesma classe nunca troca — é o que
impede churn e o que garante que uma evolução de TEXTO jamais troque a
imagem como efeito colateral.

DÍVIDA REGISTRADA: `midia_ruim` ainda vive em pipeline.score porque a
Fase 2 não pode tocar score. Quando a Fase 4 remover mídia do score, o
predicado deve migrar para cá — este módulo é o dono natural.
"""
from __future__ import annotations

from pipeline.score import midia_ruim

DESCONHECIDA = None
SEM_MIDIA = ""

PRESERVA_SEM_IMAGEM = "PRESERVA/sem_imagem_nova"
PRESERVA_DESCONHECIDA = "PRESERVA/midia_desconhecida"
PRESERVA_NAO_REBAIXA = "PRESERVA/nao_rebaixa"
PRESERVA_MESMA_CLASSE = "PRESERVA/mesma_classe"
TROCA_POST_SEM_MIDIA = "TROCA/post_sem_midia"
TROCA_UPGRADE = "TROCA/upgrade"


def politica_midia(imagem_nova, chat_novo: str, estado: dict):
    """(trocar: bool, motivo: str). Estado ausente → DESCONHECIDA."""
    if not imagem_nova:
        return (False, PRESERVA_SEM_IMAGEM)
    dono = estado.get("midia_chat", DESCONHECIDA)
    if dono is DESCONHECIDA:
        return (False, PRESERVA_DESCONHECIDA)
    if dono == SEM_MIDIA:
        return (True, TROCA_POST_SEM_MIDIA)
    nova_ruim = midia_ruim(chat_novo)
    atual_ruim = midia_ruim(dono)
    if nova_ruim and not atual_ruim:
        return (False, PRESERVA_NAO_REBAIXA)
    if not nova_ruim and atual_ruim:
        return (True, TROCA_UPGRADE)
    return (False, PRESERVA_MESMA_CLASSE)
