"""
Mediação de Cache de Links.

Fronteira pela qual as plataformas consultam e registram URLs
afiliadas no cache, sem acessar diretamente o banco de dados.

A delimitação negativa do contrato proíbe que uma plataforma
acesse a persistência diretamente. Este módulo é a alternativa
legítima: o core oferece as operações de cache, e o banco de
dados permanece sob responsabilidade exclusiva do core.

O cache de links opera em duas camadas, coordenadas internamente
por este módulo:
  - cache em memória  : consulta e gravação rápidas, volátil
  - cache persistente : tabela de links no banco, sobrevive a
                        reinícios do processo

As plataformas conhecem apenas as duas funções públicas deste
módulo. Não conhecem o banco nem a estrutura de cache em memória.

Este módulo pertence ao core. Depende das camadas de cache e de
persistência existentes; não depende de nenhuma plataforma.
"""
from __future__ import annotations

from typing import Optional

from database import db_get_link, db_set_link
from globals import _get_final, _set_final
from logger import log_nrm


def consultar_link(url_original: str) -> Optional[str]:
    """
    Consulta a URL afiliada correspondente a uma URL original.

    Verifica primeiro o cache em memória e, em caso de ausência,
    o cache persistente no banco de dados. A ordem reproduz o
    comportamento atual das plataformas.

    Retorna a URL afiliada quando encontrada em qualquer das duas
    camadas, ou None quando a URL original ainda não foi afiliada.
    None é resultado de ausência legítima, não de falha.
    """
    em_memoria = _get_final(url_original)
    if em_memoria:
        return em_memoria

    no_banco = db_get_link(url_original)
    if no_banco:
        return no_banco

    return None


def registrar_link(url_original: str, url_afiliada: str, plataforma: str) -> None:
    """
    Registra a correspondência entre uma URL original e a sua URL
    afiliada, em ambas as camadas de cache.

    Grava no cache em memória e no cache persistente, de modo que
    consultas futuras à mesma URL original sejam atendidas sem
    reprocessamento. O parâmetro `plataforma` é o identificador da
    plataforma, registrado junto à entrada persistente para fins
    de organização e observabilidade.

    Reproduz o comportamento de gravação atual das plataformas.
    """
    if not url_original or not url_afiliada:
        return

    _set_final(url_original, url_afiliada)
    try:
        db_set_link(url_original, url_afiliada, plataforma)
    except Exception as e:
        # A falha na gravação persistente não compromete a operação:
        # o cache em memória já foi atualizado, e a ausência da
        # entrada no banco apenas implicará reprocessamento futuro.
        log_nrm.warning(f"⚠️ registrar_link (persistência): {e}")
