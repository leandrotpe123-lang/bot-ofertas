def _limpar_continuacoes(texto: str) -> str:
    """
    Junta as linhas de um cURL multilinha, removendo as barras de
    continuação. Necessário porque o valor colado no painel de
    variáveis preserva as quebras de linha.
    """
    return re.sub(r"\\\s*\n", " ", texto)


def _sanear_header(valor: str) -> str:
    """
    Remove quebras de linha e caracteres de controle de um valor de
    header.

    Necessário porque o painel de variáveis pode inserir quebras
    dentro do valor colado. Um header com \\n é recusado pela
    biblioteca HTTP com ValueError ANTES da requisição sair — falha
    de transporte que não diz nada sobre a sessão.

    Só remove controle e espaço em volta; não altera o conteúdo.
    """
    if not valor:
        return ""
    limpo = re.sub(r"[\r\n\t]+", "", valor)
    limpo = "".join(c for c in limpo if ord(c) >= 32 or c == " ")
    return limpo.strip()
