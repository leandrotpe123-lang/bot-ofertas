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

def politica_midia(imagem_nova, chat_novo, estado, is_edit=False):
    if not imagem_nova: return (False, PRESERVA_SEM_IMAGEM)
    dono = estado.get("midia_chat", DESCONHECIDA)
    if dono is DESCONHECIDA: return (False, PRESERVA_DESCONHECIDA)
    if dono == SEM_MIDIA: return (True, TROCA_POST_SEM_MIDIA)
    if is_edit and chat_novo == dono: return (True, TROCA_FONTE_EDITOU)
    nr, ar = midia_ruim(chat_novo), midia_ruim(dono)
    if nr and not ar: return (False, PRESERVA_NAO_REBAIXA)
    if not nr and ar: return (True, TROCA_UPGRADE)
    return (False, PRESERVA_MESMA_CLASSE)
    
