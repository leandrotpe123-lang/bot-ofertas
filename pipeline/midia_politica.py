from __future__ import annotations
from pipeline.score import midia_ruim
DESCONHECIDA = None
SEM_MIDIA = ""
PRESERVA_SEM_IMAGEM   = "PRESERVA/sem_imagem_nova"
PRESERVA_DESCONHECIDA = "PRESERVA/midia_desconhecida"
PRESERVA_NAO_REBAIXA  = "PRESERVA/nao_rebaixa"
PRESERVA_MESMA_CLASSE = "PRESERVA/mesma_classe"
TROCA_POST_SEM_MIDIA  = "TROCA/post_sem_midia"
TROCA_UPGRADE         = "TROCA/upgrade"
TROCA_FONTE_EDITOU = "TROCA/fonte_editou"
# [E4.0] Único motivo novo. Não substitui nenhum dos acima: eles
# continuam decidindo a POLÍTICA; este só reporta que a política
# autorizou uma troca que não tem delta e por isso não vira I/O.
PRESERVA_MIDIA_IGUAL  = "PRESERVA/midia_igual"


def _regra_de_midia(imagem_nova, chat_novo, estado, is_edit=False):
    """A POLÍTICA. Corpo preservado byte a byte; nenhuma das seis
    saídas mudou de condição, de ordem ou de motivo."""
    if not imagem_nova: return (False, PRESERVA_SEM_IMAGEM)
    dono = estado.get("midia_chat", DESCONHECIDA)
    if dono is DESCONHECIDA: return (False, PRESERVA_DESCONHECIDA)
    if dono == SEM_MIDIA: return (True, TROCA_POST_SEM_MIDIA)
    if is_edit and chat_novo == dono: return (True, TROCA_FONTE_EDITOU)
    nr, ar = midia_ruim(chat_novo), midia_ruim(dono)
    if nr and not ar: return (False, PRESERVA_NAO_REBAIXA)
    if not nr and ar: return (True, TROCA_UPGRADE)
    return (False, PRESERVA_MESMA_CLASSE)


def politica_midia(imagem_nova, chat_novo, estado, is_edit=False,
                   chave_nova="", chave_aceita=""):
    """AUTORIDADE ÚNICA de mídia. Continua sendo esta função.

    [E4.0] GUARDA DE DELTA — não é política nova. A política roda
    primeiro, intacta; a guarda só responde à pergunta seguinte:
    "a troca autorizada mudaria alguma coisa?". Quando a mídia
    candidata JÁ É a mídia aceita naquele post, não há delta e a
    troca não vira escrita no Telegram.

    Fica AQUI, e não no chamador, porque decisao._com_midia rebaixa
    permite_substituir e exigir_imagem a partir deste retorno: uma
    guarda aplicada depois deixaria o fallback delete+repost
    reintroduzir a mídia pela porta dos fundos.

    Qualquer chave ausente ("") ⇒ NÃO SEI ⇒ resultado da política,
    sem alteração. A guarda nunca inventa igualdade.
    """
    troca, motivo = _regra_de_midia(
        imagem_nova, chat_novo, estado, is_edit)
    if troca and chave_nova and chave_aceita and chave_nova == chave_aceita:
        return (False, PRESERVA_MIDIA_IGUAL)
    return (troca, motivo)
