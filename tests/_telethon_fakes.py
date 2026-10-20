"""
Contrato do Telethon 1.45.0 para os clientes falsos da suíte.

`client.delete_messages(canal, ids)` devolve uma lista de
`types.messages.AffectedMessages` (uma por bloco de 100 ids); `pts_count` é
quantas mensagens o Telegram apagou DE FATO (0 quando a mensagem já não
existia). O cliente falso devolve o mesmo formato — nunca `True` — para que
o código que lê a prova da exclusão (pipeline.saida) seja exercitado como em
produção.
"""


def afetadas(ids, apagadas=None):
    """O retorno de delete_messages num canal. `apagadas` = quantas sumiram
    de fato (padrão: todas as pedidas). O import é tardio: o harness
    (tests/_harness_e5.preparar) instala o Telethon da bancada antes, e
    importar o pacote aqui no topo o impediria."""
    from telethon.tl import types
    lista = list(ids) if isinstance(ids, (list, tuple)) else [ids]
    n = len(lista) if apagadas is None else int(apagadas)
    return [types.messages.AffectedMessages(pts=1, pts_count=n)]
