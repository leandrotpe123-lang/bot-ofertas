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

from plataformas.contrato import Afiliacao
from database import db_get_link, db_set_link
from globals import _get_final, _set_final
from logger import log_nrm


def consultar_link(url_original: str) -> Optional[Afiliacao]:
    """
    Consulta a afiliação correspondente a uma URL original.

    Verifica primeiro o cache em memória e, em caso de ausência,
    o cache persistente no banco de dados. A ordem reproduz o
    comportamento atual das plataformas.

    Retorna um Afiliacao (publicada + canonica) quando encontrado
    em qualquer das duas camadas, ou None quando a URL original
    ainda não foi afiliada. None é ausência legítima, não falha.
    """
    em_memoria = _get_final(url_original)
    if em_memoria:
        return em_memoria

    no_banco = db_get_link(url_original)
    if no_banco:
        return Afiliacao(publicada=no_banco[0], canonica=no_banco[1])

    return None


def registrar_link(url_original: str, afiliacao: Afiliacao | str,
                   plataforma: str) -> None:
    """
    Registra a correspondência entre uma URL original e a sua
    afiliação, em ambas as camadas de cache.

    Aceita um Afiliacao (publicada + canonica) ou, de forma
    retrocompatível, uma str — caso em que publicada e canonica
    são a mesma forma. O parâmetro `plataforma` é o identificador
    da plataforma, registrado junto à entrada persistente para
    fins de organização e observabilidade.

    Reproduz o comportamento de gravação atual das plataformas.
    """
    if not url_original or not afiliacao:
        return

    # Retrocompatível: plataforma que devolve str (Amazon, Magalu,
    # Mercado Livre) registra publicada = canonica. A Shopee registra
    # um Afiliacao com as duas formas distintas.
    af = (afiliacao if isinstance(afiliacao, Afiliacao)
          else Afiliacao(publicada=afiliacao, canonica=afiliacao))
    if not af.publicada:
        return

    _set_final(url_original, af)
    try:
        db_set_link(url_original, af.publicada, af.canonica, plataforma)
    except Exception as e:
        # A falha na gravação persistente não compromete a operação:
        # o cache em memória já foi atualizado, e a ausência da
        # entrada no banco apenas implicará reprocessamento futuro.
        log_nrm.warning(f"⚠️ registrar_link (persistência): {e}")
