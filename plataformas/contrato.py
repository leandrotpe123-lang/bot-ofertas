"""
Contrato de Plataforma — definição formal.

Este módulo define a estrutura que toda plataforma deve cumprir para
ser integrada à pipeline. É a camada neutra entre o core e as
plataformas: o core depende deste contrato, cada plataforma depende
deste contrato, e nenhum dos dois depende do outro diretamente.

Este módulo não contém nenhuma plataforma concreta e não contém
lógica de pipeline. Define apenas a forma do contrato, o conjunto
fechado de tipos de link, o sentinela de ausência explícita e a
versão suportada.

Baseline arquitetural: documentos de especificação do contrato,
do registry e dos testes de arquitetura.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, Optional


# ── Versão do contrato ────────────────────────────────────────────
# Inteiro simples. Incrementado apenas em mudança não aditiva do
# contrato. Capacidades novas entram como opcionais e NÃO exigem
# incremento de versão.
CONTRACT_VERSION = 1


# ── Tipo de link: conjunto fechado ────────────────────────────────
# Conjunto fechado por decisão de design: o tipo de link influencia
# deduplicação, filtros e score. Uma plataforma deve classificar
# todos os seus links exclusivamente dentro deste conjunto. A
# introdução de uma categoria nova é alteração de contrato e exige
# decisão arquitetural formal.
class TipoLink(Enum):
    PRODUTO   = "produto"     # item individual identificável
    BUSCA     = "busca"       # listagem ou pesquisa, sem produto único
    CAMPANHA  = "campanha"    # campanha promocional ou página institucional
    EVENTO    = "evento"      # evento interativo (missão, roleta)
    ENCURTADO = "encurtado"   # URL não expandida, natureza ainda desconhecida
    INVALIDO  = "invalido"    # pertence ao domínio mas não é aproveitável


# ── Sentinela de ausência explícita ───────────────────────────────
# Representa um resultado legítimo e bem-sucedido cujo valor é a
# inexistência de um dado. É categoricamente distinto de falha.
# Toda capacidade que pode não ter um dado a devolver usa este
# mesmo sentinela, e o core o reconhece de forma uniforme.
class _Ausente:
    """Tipo do sentinela de ausência explícita. Uso interno."""
    _instancia: Optional["_Ausente"] = None

    def __new__(cls) -> "_Ausente":
        if cls._instancia is None:
            cls._instancia = super().__new__(cls)
        return cls._instancia

    def __repr__(self) -> str:
        return "AUSENTE"

    def __bool__(self) -> bool:
        return False


# Valor único de ausência explícita, compartilhado por todo o sistema.
AUSENTE = _Ausente()


# ── Resultado da extração de identidade do produto ────────────────
@dataclass(frozen=True)
class IdentidadeProduto:
    """
    Resultado da capacidade de extração de identidade.

    Campos obrigatórios:
      - tipo_link        : sempre presente, valor do conjunto TipoLink
      - id_produto       : sempre presente como campo; o valor é uma
                           string não vazia, ou AUSENTE quando a URL
                           não corresponde a um produto individual

    Campo opcional:
      - id_global        : chave única de sistema (plataforma + produto);
                           presente apenas quando id_produto está presente
    """
    tipo_link:  TipoLink
    id_produto: object                       # str | _Ausente (AUSENTE)
    id_global:  Optional[str] = None


# ── Capacidades opcionais: parâmetros temporais ───────────────────
@dataclass(frozen=True)
class ParametrosTemporais:
    """
    Parâmetros temporais de deduplicação declarados por uma plataforma.

    São DADOS fornecidos ao core, não política. O core mantém a
    soberania sobre a decisão final de deduplicação e pode aplicar
    limites ou regras universais sobre estes valores.

    Valores em segundos.
    """
    janela_s:       float
    ttl_restock_s:  float


# ── Definição da plataforma ───────────────────────────────────────
@dataclass(frozen=True)
class Plataforma:
    """
    Definição formal de uma plataforma que cumpre o contrato.

    Cada módulo de plataforma constrói e expõe uma instância desta
    estrutura. As capacidades são funções referenciadas pelos campos
    abaixo.

    ── Identidade ────────────────────────────────────────────────
      - identificador     : string estável, minúscula, única
      - versao_contrato   : inteiro; verificado no cadastro

    ── Capacidades obrigatórias ──────────────────────────────────
      - reconhece         : (url: str) -> bool
                            Pura, determinística, sem I/O.
      - extrai_identidade : (url: str) -> IdentidadeProduto
                            Pura, determinística, sem I/O.
      - afilia            : async (url, sessao) -> str | AUSENTE
                            Efeito colateral controlado (rede, cache).
                            Não propaga exceção: falha vira AUSENTE.

    ── Capacidades opcionais (None quando ausentes) ──────────────
      - parametros_temporais : ParametrosTemporais | None
                               Dado, não política.
      - pos_processa         : async (contexto_leitura) -> None | None
                               Isolada, assíncrona, não altera fluxo.
      - limpa_url            : (url: str) -> str | None
                               Pura, determinística, sem I/O.
    """
    # Identidade
    identificador:   str
    versao_contrato: int

    # Capacidades obrigatórias
    reconhece:         Callable[[str], bool]
    extrai_identidade: Callable[[str], IdentidadeProduto]
    afilia:            Callable[..., Awaitable[object]]   # -> str | _Ausente

    # Capacidades opcionais
    parametros_temporais: Optional[ParametrosTemporais] = None
    pos_processa:         Optional[Callable[..., Awaitable[None]]] = None
    limpa_url:            Optional[Callable[[str], str]] = None
