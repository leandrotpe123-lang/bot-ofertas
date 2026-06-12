"""
Camada — Classificação de Estado de Evento.

Responsabilidade única: determinar o estado de ciclo de vida de uma
oferta no momento em que é processada, comparando-a contra o histórico
de deduplicação.

Estados possíveis:
  - NEW       : oferta nunca vista
  - SEEN      : oferta vista recentemente, ainda dentro da janela
  - EXPIRED   : oferta vista há muito tempo, além do TTL
  - RESTOCKED : oferta reativada (texto indica retorno explícito)

NÃO faz:
  - normalização de mensagem (responsabilidade da normalização)
  - deduplicação ou claim de identidade (responsabilidade da deduplicação)
  - detecção de cupom (responsabilidade de utils.cupom)
  - verificação de viabilidade de texto (permanece na normalização)
  - persistência (apenas consulta o banco de deduplicação)

ORIGEM DOS PARÂMETROS TEMPORAIS:
  Janela e TTL de restock pertencem a cada plataforma e são
  declarados via capacidade parametros_temporais do contrato. Esta
  camada lê esses valores do registry através de
  obter_parametros_temporais; jamais mantém estruturas internas
  indexadas por nome de plataforma como fonte da verdade. O core
  fornece um _DEFAULTS local aplicado como fallback quando a
  plataforma não é resolvida pelo registry ou não declara a
  capacidade.
"""
from __future__ import annotations

import re
import time
from enum import Enum
from typing import Tuple

from database import db_get_dedupe
from logger import log_nrm
from plataformas import registry
from plataformas.contrato import ParametrosTemporais
from utils.hashes import _fp_c3


# ── Defaults do core ──────────────────────────────────────────────
# Política do core para a ausência de declaração da plataforma.
# Aplicados em dois casos: quando o registry não reconhece o nome
# da plataforma (URL não afiliada, plataforma futura ainda não
# registrada, nome inválido) e quando a plataforma é reconhecida
# mas não declara a capacidade parametros_temporais. NÃO substitui
# a declaração da plataforma: convive com ela como fallback estrito.
_DEFAULTS = ParametrosTemporais(
    janela_s      = 120.0,
    ttl_restock_s = 3600.0,
)


def _obter_parametros_temporais_com_origem(
    plat: str,
) -> Tuple[ParametrosTemporais, bool]:
    """
    Helper interno do módulo.

    Devolve (params, encontrado):
      - params:     sempre uma ParametrosTemporais válida — declarada
                    pela plataforma quando disponível, _DEFAULTS do
                    core em qualquer outro caso;
      - encontrado: True somente quando a plataforma foi resolvida
                    pelo registry E declara a capacidade
                    parametros_temporais; False em qualquer outra
                    situação (nome vazio, plataforma desconhecida,
                    plataforma reconhecida sem declaração).

    O flag encontrado é o sinal EXPLÍCITO de fallback usado pelo
    shim de compatibilidade e por qualquer consumidor que precise
    distinguir os casos. Evita acoplar a semântica de fallback à
    identidade de objeto do singleton _DEFAULTS.

    Os logs de warning para integração incompleta são emitidos aqui,
    junto da decisão de fallback, para garantir que qualquer chamador
    desta função — público ou compat — observe a mesma sinalização.
    """
    if not plat:
        return _DEFAULTS, False
    plataforma = registry.acessar(plat)
    if plataforma is None:
        log_nrm.warning(
            f"⚠️ [PARAMETROS_TEMPORAIS] plataforma '{plat}' "
            f"desconhecida pelo registry — aplicando defaults do "
            f"core. Possível integração incompleta; verificar "
            f"registro em main.py."
        )
        return _DEFAULTS, False
    if plataforma.parametros_temporais is None:
        log_nrm.warning(
            f"⚠️ [PARAMETROS_TEMPORAIS] plataforma '{plat}' "
            f"reconhecida pelo registry mas não declara a "
            f"capacidade parametros_temporais — aplicando defaults "
            f"do core. Integração parcial; revisar o módulo da "
            f"plataforma."
        )
        return _DEFAULTS, False
    return plataforma.parametros_temporais, True


def obter_parametros_temporais(plat: str) -> ParametrosTemporais:
    """
    Devolve os parâmetros temporais aplicáveis à plataforma indicada.

    Lê a capacidade parametros_temporais do contrato, acessando a
    plataforma pelo seu identificador nominal via registry.acessar
    — a API nominal já existente do registry. Aplica _DEFAULTS do
    core quando o nome não é reconhecido, ou quando a plataforma é
    reconhecida mas não declara a capacidade. Nunca devolve None.

    LEITURA PURA EM MEMÓRIA:
      Consulta exclusivamente as estruturas em memória mantidas pelo
      registry, populadas no boot da aplicação por main.py via
      registry.cadastrar. NÃO realiza IO, NÃO acessa rede, NÃO
      consulta banco de dados. Custo operacional: uma busca em
      dicionário no registry e uma leitura de atributo de dataclass.
      Pode ser chamada livremente no caminho quente da pipeline sem
      impacto de latência.

    OBSERVABILIDADE:
      Emite log de warning em DUAS situações que caracterizam
      integração incompleta:
        - quando o nome de plataforma é não vazio mas não consta do
          registry — plataforma esquecida no main.py, typo na
          resolução nominal a montante, ou plataforma removida sem
          atualização dos consumidores;
        - quando a plataforma é reconhecida pelo registry mas não
          declara a capacidade parametros_temporais — módulo de
          plataforma incompleto.
      Nomes vazios ou ausentes não geram log: representam a
      ausência normal de plataforma identificada a montante e não
      constituem erro.
    """
    params, _ = _obter_parametros_temporais_com_origem(plat)
    return params


# ─────────────────────────────────────────────────────────────────
# COMPATIBILIDADE TRANSITÓRIA — _JANELA_C3
#
# Superfície de mapping reduzida para a janela de deduplicação por
# plataforma. Existe APENAS para preservar a importação do nome
# _JANELA_C3 pelos consumidores legados ainda não migrados:
#   - pipeline/normalizacao.py reexporta este símbolo;
#   - utils/textos.py o importa via reexportação.
#
# Suporta EXCLUSIVAMENTE os modos de acesso utilizados hoje por
# esses consumidores: __getitem__ e get. NÃO é uma abstração
# genérica de mapping. Iteração, keys, values, items, len, in,
# update e demais operações de Mapping NÃO são suportadas e NÃO
# devem ser introduzidas.
#
# Sem estado próprio. Sem cache. Sem mutação preguiçosa. Cada
# acesso delega a _obter_parametros_temporais_com_origem, que
# distingue fallback por flag explícito — sem dependência de
# identidade de objeto sobre o singleton _DEFAULTS.
#
# REMOÇÃO PREVISTA: em conjunto com a limpeza das reexportações
# temporárias de compatibilidade da normalização, momento em que
# os consumidores legados migrarão diretamente para
# obter_parametros_temporais.
#
# SEMÂNTICA DE FALLBACK EM get:
# Chamadas com default explícito devolvem o default do chamador
# quando a plataforma não é resolvida pelo registry, preservando
# a semântica dos consumidores legados que sempre fornecem default.
#
# Chamadas sem default devolvem None nessa situação — comportamento
# alinhado a dict.get() e deliberadamente distinto do antigo
# dicionário literal, que nunca devolvia None para suas chaves
# declaradas.
#
# Os consumidores ativos sempre passam default explícito.
# Novos consumidores devem ter essa diferença em mente.
# ─────────────────────────────────────────────────────────────────
class _JanelaC3Compat:
    __slots__ = ()

    def __getitem__(self, plat):
        if plat == "default":
            return _DEFAULTS.janela_s
        params, _ = _obter_parametros_temporais_com_origem(plat)
        return params.janela_s

    def get(self, plat, default=None):
        if plat == "default":
            return _DEFAULTS.janela_s
        params, encontrado = _obter_parametros_temporais_com_origem(plat)
        if encontrado:
            return params.janela_s
        return default


_JANELA_C3 = _JanelaC3Compat()


# ── Vocabulário de evento e de reativação ─────────────────────────
# _KW_EVENTO é a definição canônica da família INTERATIVA de evento:
# dinâmicas de participação (quiz, roleta, missão, sorteio...).
# Vocabulário de CALENDÁRIO COMERCIAL (black friday, esquenta) NÃO
# pertence a esta família e não deve entrar aqui: no canônico, ele
# alteraria a saturação (publicacao) e os títulos (montagem) no pico
# comercial. Esse vocabulário vive como resíduo nomeado na
# deduplicação (_RE_CALENDARIO_COMERCIAL).
_KW_EVENTO = re.compile(
    r'\b(?:quiz|roleta|miss[aã]o|arena|girar|gire|roda|jogar|'
    r'jogue|desafio|sorteio)\b',
    re.I,
)

# _RE_RETORNO é a definição canônica da linguagem de RETORNO de
# oferta ("voltou", "reativado", "restock"...). Fonte única do
# VOCABULÁRIO — as DECISÕES que o consomem permanecem separadas:
#   - ciclo de vida (detectar_estado_evento, aqui): RESTOCKED exige
#     histórico fora da janela da plataforma;
#   - gate anti-flood (deduplicacao._eh_reativacao): janela curta
#     de 30s sobre o texto, com ou sem histórico.
# Consolidar o vocabulário NÃO funde as decisões.
# Compostos com "voltou" (ex.: "voltou ao estoque") são subsumidos
# por \bvoltou\b e não se re-declaram.
_RE_RETORNO = re.compile(
    r'\b(?:voltou|voltando|reativad[oa]|reativa[çc][aã]o|'
    r'ativ[oa]\s+novamente|dispon[ií]vel\s+novamente|de\s+volta|'
    r'normalizou|relan[çc]amento|restock)\b',
    re.I,
)


# ── Contrato de saída ─────────────────────────────────────────────
class EstadoEvento(Enum):
    NEW       = "new"
    SEEN      = "seen"
    EXPIRED   = "expired"
    RESTOCKED = "restocked"


# ── Núcleo da classificação ───────────────────────────────────────
def detectar_estado_evento(
    texto: str,
    id_global: str,
    plat: str,
) -> EstadoEvento:
    """
    Classifica o estado de ciclo de vida de uma oferta.

    Consulta a entrada de deduplicação correspondente ao identificador
    global e à plataforma. Se não houver registro anterior, a oferta é
    NEW. Havendo registro, compara o tempo decorrido contra a janela e
    o TTL da plataforma — ambos obtidos via obter_parametros_temporais,
    que lê parametros_temporais do contrato via registry e aplica
    _DEFAULTS do core como fallback:

      - dentro da janela              → SEEN
      - fora da janela, texto indica retorno → RESTOCKED
      - além do TTL                   → EXPIRED
      - demais casos                  → SEEN

    PRECEDÊNCIA DE RESTOCKED SOBRE EXPIRED FORA DA JANELA:
      Quando o tempo decorrido excede a janela, a presença de
      vocabulário de retorno explícito no texto (eh_restock=True)
      classifica a oferta como RESTOCKED com PRECEDÊNCIA SOBRE
      EXPIRED, INDEPENDENTEMENTE do TTL. Uma oferta que retorna
      explicitamente após muito tempo é tratada como reativação,
      e não como expiração, ainda que delta já tenha ultrapassado
      o ttl_restock_s da plataforma. A condição de TTL só é
      avaliada quando NÃO há sinal de retorno no texto. A ordem
      das verificações no corpo da função reflete essa regra.
    """
    eh_restock = bool(_RE_RETORNO.search(texto))
    entrada    = db_get_dedupe(_fp_c3(id_global, plat))

    if not entrada:
        return EstadoEvento.NEW

    ts_anterior = entrada.get("ts", 0)
    delta       = time.time() - ts_anterior
    params      = obter_parametros_temporais(plat)
    janela      = params.janela_s
    ttl         = params.ttl_restock_s

    if delta < janela:
        return EstadoEvento.SEEN
    if eh_restock:
        return EstadoEvento.RESTOCKED
    if delta > ttl:
        return EstadoEvento.EXPIRED
    return EstadoEvento.SEEN
  
