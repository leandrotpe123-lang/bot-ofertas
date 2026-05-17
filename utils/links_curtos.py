"""
Mediação de Persistência de Links Curtos.

Fronteira pela qual o serviço de encurtamento do core registra
correspondências entre códigos curtos e URLs de destino, e pela
qual o servidor de redirecionamento as consulta. Encapsula o
acesso à tabela de links curtos do banco de dados.

A delimitação negativa do contrato proíbe que uma plataforma
acesse a persistência diretamente. A lógica de encurtamento é,
por decisão arquitetural, um serviço do core e não de uma
plataforma; ainda assim, a sua persistência é mediada por este
módulo, de modo que o acesso ao banco permaneça concentrado e
sob responsabilidade exclusiva do core.

Este módulo pertence ao core. Depende da camada de persistência
existente; não depende de nenhuma plataforma, do serviço de
encurtamento ou da pipeline.

Convenção de erros:
  - entrada inválida (argumentos obrigatórios ausentes) é uso
    incorreto da função e resulta em ValueError;
  - falha operacional em tempo de execução é registrada e
    isolada, sem propagação.

Consumidores previstos:
  - serviço de encurtamento de links (registro)
  - servidor de redirecionamento (consulta)
"""
from __future__ import annotations

from typing import Optional

from database import db_get_short, db_set_short
from logger import log_sys


def registrar_codigo(codigo: str, url_destino: str) -> bool:
    """
    Registra a correspondência entre um código curto e a sua URL
    de destino.

    A gravação não sobrescreve um código já existente, preservando
    a primeira correspondência registrada para um dado código. Esta
    semântica reproduz o comportamento atual do sistema e é coerente
    com o fato de o código ser derivado de forma estável da URL.

    Entrada inválida: a ausência de `codigo` ou de `url_destino`
    constitui uso incorreto da função e resulta em ValueError.

    Falha operacional: uma falha na gravação é registrada e isolada,
    e a função retorna False. A função retorna True quando a
    gravação é concluída.
    """
    if not codigo or not url_destino:
        raise ValueError("codigo e url_destino são obrigatórios")
    try:
        db_set_short(codigo, url_destino)
        return True
    except Exception as e:
        log_sys.warning(f"⚠️ registrar_codigo: {e}")
        return False


def consultar_codigo(codigo: str) -> Optional[str]:
    """
    Consulta a URL de destino correspondente a um código curto.

    Entrada inválida: a ausência de `codigo` constitui uso incorreto
    da função e resulta em ValueError.

    Retorno: a URL de destino quando o código está registrado, ou
    None quando o código não existe. A consulta adota degradação suave 
    deliberada: tanto a ausência legítima quanto uma falha operacional 
    de leitura resultam em None, pois o consumidor da função trata ambas 
    as condições como impossibilidade de resolver a URL de destino.
    """
    if not codigo:
        raise ValueError("codigo é obrigatório")
    try:
        return db_get_short(codigo)
    except Exception as e:
        log_sys.warning(f"⚠️ consultar_codigo: {e}")
        return None
