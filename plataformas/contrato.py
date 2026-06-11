"""
Contrato de Plataforma.

Camada neutra entre o core e as plataformas. O core depende deste
contrato; cada plataforma depende deste contrato; nenhum depende do
outro diretamente.

Define a estrutura que uma plataforma deve cumprir, o conjunto
fechado de tipos de link, o sentinela de ausência e a versão.
Não contém plataforma concreta nem lógica de pipeline. Permanece
declarativo: lógica, helpers e validações comportamentais não
pertencem a este módulo.

Detalhamento das capacidades, garantias e invariantes: ver os
documentos de especificação do contrato e do registry.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, Optional


# ── Versão do contrato ────────────────────────────────────────────
# Inteiro simples. Incrementado apenas em mudança não aditiva.
# Capacidades opcionais novas são aditivas e não exigem incremento.
CONTRACT_VERSION = 1


# ── Tipo de link: conjunto fechado ────────────────────────────────
# Conjunto fechado: o tipo de link influencia deduplicação, filtros
# e score. Uma categoria nova é alteração de contrato e exige
# decisão arquitetural formal.
class TipoLink(Enum):
    PRODUTO   = "produto"     # item individual identificável
    BUSCA     = "busca"       # listagem ou pesquisa
    CAMPANHA  = "campanha"    # campanha ou página institucional
    EVENTO    = "evento"      # evento interativo (missão, roleta)
    ENCURTADO = "encurtado"   # URL não expandida, natureza desconhecida
    INVALIDO  = "invalido"    # pertence ao domínio mas não é aproveitável


# ── Sentinela de ausência explícita ───────────────────────────────
# Resultado legítimo cujo valor é a inexistência de um dado.
# Distinto de falha. Usado por toda capacidade que pode não ter um
# dado a devolver, e reconhecido de forma uniforme pelo core.
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


# ── Resultado da extração de identidade ───────────────────────────
@dataclass(frozen=True)
class IdentidadeProduto:
    """
    Resultado da capacidade de extração de identidade.

      - tipo_link  : obrigatório; valor do conjunto TipoLink.
      - id_produto : obrigatório como campo; valor é string não
                     vazia, ou AUSENTE quando não há produto único.
      - id_global  : opcional; chave de sistema (plataforma+produto),
                     presente apenas quando id_produto está presente.
    """
    tipo_link:  TipoLink
    id_produto: object                       # str | _Ausente
    id_global:  Optional[str] = None


# ── Parâmetros temporais de deduplicação ──────────────────────────
@dataclass(frozen=True)
class ParametrosTemporais:
    """
    Parâmetros temporais de deduplicação declarados por uma
    plataforma. São dados fornecidos ao core, não política: o core
    decide a deduplicação. Valores em segundos.
    """
    janela_s:       float
    ttl_restock_s:  float


# ── Definição da plataforma ───────────────────────────────────────
@dataclass(frozen=True)
class Plataforma:
    """
    Definição formal de uma plataforma. Cada módulo de plataforma
    constrói e expõe uma instância desta estrutura.

    Identidade:
      - identificador   : string estável, minúscula, única.
      - versao_contrato : inteiro; verificado no cadastro.

    Capacidades obrigatórias:
      - reconhece         : (url) -> bool. Pura, sem I/O.
      - extrai_identidade : (url) -> IdentidadeProduto. Pura, sem I/O.
      - afilia            : async (url, sessao) -> str | AUSENTE.
                            Efeito colateral controlado; não propaga
                            exceção (falha resulta em AUSENTE).

    Capacidades opcionais:
      - parametros_temporais : ParametrosTemporais | None. Dado.
      - limpa_url            : (url) -> str | None. Pura, sem I/O.
      - requer_encurtamento  : bool, padrão False. Declaração de
                               intenção: indica que os links desta
                               plataforma devem ser submetidos ao
                               encurtador do core. Não contém lógica
                               de encurtamento.
      - encurtadores_forca_get : frozenset[str] | None. Conjunto
                                 de hosts cujos servidores não
                                 respondem corretamente a HEAD,
                                 exigindo GET direto na resolução
                                 de redirecionamento. Conhecimento
                                 de quirk HTTP, não de identidade
                                 de plataforma. None equivale a
                                 não declarar; o core ignora.

      - hosts_campanha : frozenset[str] | None. Conjunto de hosts
                         cujas URLs afiliadas LONGAS caracterizam uma
                         página de campanha desta plataforma (ex.: a
                         landing de uma roleta, uma campanha sazonal).
                         Conhecimento de host de campanha, declarado
                         localmente. O core compõe a UNIÃO das
                         contribuições de todas as plataformas
                         registradas e a utiliza para derivar
                         tem_host_campanha e chave_campanha. Não
                         contém lógica de campanha. None equivale a
                         não declarar; o core ignora.                   

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
    limpa_url:            Optional[Callable[[str], str]] = None
    requer_encurtamento:  bool = False
    encurtadores_forca_get: Optional[frozenset[str]] = None
    hosts_campanha:       Optional[frozenset[str]] = None
