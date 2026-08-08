"""Camada de política de conteúdo — dono declarado por normalizacao.py.

Responsabilidade ÚNICA: decidir quais linhas do texto permanecem e
quais são removidas. NÃO transforma a forma do texto (normalização) e
NÃO decide o que uma URL é — para isso consulta as autoridades

═══════════════════════════════════════════════════════════════════
PRINCÍPIO DA PRESERVAÇÃO
═══════════════════════════════════════════════════════════════════
Uma oferta válida deve atravessar o pipeline completa. Filtro remove
RUÍDO; não empobrece conteúdo. Toda regra aqui parte da preservação:

  remover uma linha exige EVIDÊNCIA POSITIVA de que ela é ruído.
  A ausência de evidência de que é conteúdo NÃO é evidência de que
  é ruído. Na dúvida, preserva.

═══════════════════════════════════════════════════════════════════
AUTORIDADES CONSUMIDAS (nunca duplicadas)
═══════════════════════════════════════════════════════════════════
  - plataformas.registry          : o que é URL de plataforma
  - utils.categorias_universais   : categorias universais de URL

Este módulo não reimplementa nenhum desses conhecimentos.
"""
#
# Implementação: pipeline.filtros_linha (política de linha,
# pré-conversão), pipeline.filtros_bloco (política de bloco,
# pós-conversão) e pipeline.filtros_estrutura (vocabulário
# estrutural). Este arquivo é FACHADA: sem lógica, regex ou
# constante. O vocabulário estrutural NÃO é reexportado — é
# contrato interno da camada, não público.
from __future__ import annotations

from pipeline.filtros_bloco import filtrar_blocos
from pipeline.filtros_linha import filtrar
