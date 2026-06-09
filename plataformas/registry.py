"""
Registry de Plataformas.

Único ponto de contato entre o core da pipeline e as plataformas.
A pipeline nunca importa uma plataforma concreta: consulta o registry.

O registry é o dono da CAMADA COLETIVA da casa: além de mediar o
acesso às plataformas, ele retém e expõe as verdades sobre o CONJUNTO
de plataformas. A primeira dessas verdades é a FORMAÇÃO do catálogo —
o que sobreviveu, o que foi ignorado e o que falhou ao se constituir.

Funções:
  - cadastrar : registra uma plataforma, com verificação estrutural,
                e grava o resultado dessa admissão na formação
  - resolver  : determina qual plataforma reconhece uma URL
  - acessar   : devolve o objeto de uma plataforma por identificador
  - registrar_ignorado / registrar_falha_formacao :
                ledger de formação para eventos que ocorrem ANTES da
                admissão e que o registry não tem como observar por si
                (reportados pela descoberta)
  - formacao  : expõe a verdade coletiva de formação do catálogo
  - plataformas_registradas : leitura dos identificadores vivos

O registry é MECANISMO, não autoridade: compõe e retém o que as
plataformas declaram e o que a sua própria admissão produz. NÃO
interpreta semântica de plataforma (soberania da plataforma) nem
inventa regra (autoridade do contrato). NÃO executa lógica de
pipeline: NÃO classifica, NÃO normaliza, NÃO calcula score, NÃO
deduplica, NÃO publica. NÃO acessa rede, banco ou cache. NÃO mantém
estado de processamento. Mantém o catálogo e a formação do conjunto,
estabelecidos na inicialização e estáveis durante a vida do processo.

Baseline arquitetural: Documento 2 — Especificação do Registry.
"""

from __future__ import annotations

import os

from typing import Dict, List, Optional, Tuple

from logger import log_sys
from plataformas.contrato import CONTRACT_VERSION, Plataforma


# ── Exceção de cadastro ───────────────────────────────────────────
class ErroCadastroPlataforma(Exception):
    """
    Emitida quando um cadastro de plataforma é rejeitado.

    A rejeição de cadastro é um erro de inicialização, não uma
    condição silenciosa: deve interromper a inicialização do
    sistema e ser visível.
    """


# ── Catálogo interno ──────────────────────────────────────────────
# Dicionário indexado pelo identificador da plataforma. Estabelecido
# na inicialização e estável durante a vida do processo.
_catalogo: Dict[str, Plataforma] = {}


# ── Formação do catálogo (camada coletiva) ────────────────────────
# Verdade sobre COMO o conjunto se constituiu. Os SOBREVIVENTES vivem
# em _catalogo e NÃO são duplicados aqui; este estado guarda apenas os
# NÃO-sobreviventes: o que foi ignorado e o que falhou (com motivo).
# Escrito apenas na inicialização (import único, mono-thread) e estável
# durante a vida do processo — mesma natureza de _catalogo, sem lock.
_formacao_ignorados: List[str] = []
_formacao_falhas: List[Tuple[str, str]] = []


# ── Função 1: cadastro ────────────────────────────────────────────
def cadastrar(plataforma: Plataforma) -> None:
    """
    Registra uma plataforma no catálogo.

    Executa três verificações estruturais, todas de inicialização:

      1. versão do contrato compatível com a suportada pelo core
      2. identificador único entre as plataformas já registradas
      3. presença das três capacidades obrigatórias

    Qualquer falha emite ErroCadastroPlataforma e a plataforma NÃO
    é registrada. Não verifica a correção da lógica interna da
    plataforma, apenas sua conformidade estrutural com o contrato.

    A admissão é uma operação própria do registry, então o registry
    grava o seu RESULTADO na formação: uma rejeição vai para
    _formacao_falhas (com o motivo nascido aqui) antes de ser emitida.
    A regra violada permanece do contrato; o registry apenas detecta e
    retém — mecanismo, não autoridade.
    """
    # Verificação 1: versão do contrato
    if plataforma.versao_contrato != CONTRACT_VERSION:
        motivo = (
            f"versão de contrato {plataforma.versao_contrato} "
            f"incompatível com a suportada ({CONTRACT_VERSION})"
        )
        _formacao_falhas.append((plataforma.identificador, motivo))
        raise ErroCadastroPlataforma(
            f"Plataforma '{plataforma.identificador}': {motivo}."
        )

    # Verificação 2: unicidade de identidade
    if plataforma.identificador in _catalogo:
        motivo = "identificador já registrado"
        _formacao_falhas.append((plataforma.identificador, motivo))
        raise ErroCadastroPlataforma(
            f"Plataforma '{plataforma.identificador}': {motivo}."
        )

    # Verificação 3: conformidade mínima com o contrato
    obrigatorias = ("reconhece", "extrai_identidade", "afilia")
    for nome in obrigatorias:
        capacidade = getattr(plataforma, nome, None)
        if not callable(capacidade):
            motivo = (
                f"capacidade obrigatória '{nome}' ausente ou inválida"
            )
            _formacao_falhas.append((plataforma.identificador, motivo))
            raise ErroCadastroPlataforma(
                f"Plataforma '{plataforma.identificador}': {motivo}."
            )

    _catalogo[plataforma.identificador] = plataforma
    log_sys.info(
        f"🧩 Plataforma registrada | id={plataforma.identificador} "
        f"contrato=v{plataforma.versao_contrato}"
    )


# ── Modo de execução ──────────────────────────────────────────────
# Em desenvolvimento, uma falha em reconhece() é defeito de contrato
# e deve interromper a resolução, ficando explícita. Em produção, a
# falha é registrada e isolada para preservar a continuidade.
_MODO_PRODUCAO = os.environ.get("REGISTRY_ENV", "dev").lower() == "prod"


def resolver(url: str) -> Optional[Plataforma]:
    """
    Determina qual plataforma reconhece uma URL.

    Consulta a capacidade de reconhecimento de cada plataforma
    registrada. O contrato garante reconhecimento mutuamente
    exclusivo (4ª invariante), portanto há no máximo uma
    correspondência e a ordem de consulta é irrelevante.

    Pura e determinística: apoia-se apenas nas capacidades de
    reconhecimento, que o contrato define como puras. Não acessa
    rede, banco ou cache.

    Comportamento em caso de exceção em reconhece(): como a
    capacidade foi especificada como não-falhável, uma exceção é
    sempre defeito estrutural da plataforma.
      - desenvolvimento: a exceção é propagada e interrompe a
        resolução, tornando o defeito explícito;
      - produção: a exceção é registrada e isolada, e a resolução
        prossegue com as demais plataformas.

    Retorna a plataforma correspondente, ou None quando nenhuma
    plataforma reconhece a URL.
    """
    if not url:
        return None
    for plataforma in _catalogo.values():
        try:
            if plataforma.reconhece(url):
                return plataforma
        except Exception as e:
            log_sys.error(
                f"❌ reconhece() falhou — defeito de contrato | "
                f"plataforma={plataforma.identificador}: {e}"
            )
            if not _MODO_PRODUCAO:
                raise
    return None


# ── Função 3: acesso ──────────────────────────────────────────────
def acessar(identificador: str) -> Optional[Plataforma]:
    """
    Devolve o objeto de uma plataforma a partir de seu identificador.

    Pura e determinística. Retorna None quando o identificador não
    corresponde a nenhuma plataforma registrada, de forma coerente
    com a função de resolução.
    """
    return _catalogo.get(identificador)


# ── Camada coletiva: ledger de formação ───────────────────────────
def registrar_ignorado(modulo: str) -> None:
    """
    Reporta ao registry um candidato que NÃO é plugin (módulo sem
    PLATAFORMA válida). Esse evento ocorre na descoberta, ANTES da
    admissão, e o registry não tem como observá-lo por si — por isso
    é reportado. O registry apenas retém; não julga.
    """
    _formacao_ignorados.append(modulo)


def registrar_falha_formacao(origem: str, motivo: str) -> None:
    """
    Reporta ao registry uma falha de formação observável apenas FORA
    da admissão — tipicamente um import malsucedido, que acontece
    antes de cadastrar. As rejeições produzidas pela própria admissão
    são gravadas internamente por cadastrar; esta porta existe para o
    que o registry não pode ver sozinho. O registry retém o motivo
    verbatim; não o interpreta.
    """
    _formacao_falhas.append((origem, motivo))


def formacao() -> Dict[str, tuple]:
    """
    Expõe a verdade coletiva de formação do catálogo:

      - registrados : identificadores que sobreviveram (derivados do
                      catálogo — fonte única, sem duplicação)
      - ignorados   : candidatos que não eram plugins
      - falhas      : pares (origem, motivo) de tudo que falhou —
                      rejeições de admissão e imports malsucedidos

    Leitura pura. É a porta pela qual o conjunto responde "em que
    estado eu me constituí?", visível independentemente do volume de
    plataformas. Não participa do fluxo da pipeline.
    """
    return {
        "registrados": tuple(_catalogo.keys()),
        "ignorados": tuple(_formacao_ignorados),
        "falhas": tuple(_formacao_falhas),
    }

# Cache das composições coletivas, por nome de capacidade. Catálogo
# imóvel após o boot ⇒ composição estável por processo. Fonte única
# de cache da camada coletiva — os consumidores não cacheiam mais.
_composicao_cache: Dict[str, Dict[str, frozenset[str]]] = {}

def compor_capacidade(nome: str) -> Dict[str, frozenset[str]]:
    """
    Compõe uma capacidade agregável (frozenset[str]) ao longo do
    catálogo RETENDO a origem: devolve elemento -> frozenset dos
    identificadores que o declararam.

    `nome` é o atributo de Plataforma a compor (ex.:
    "encurtadores_forca_get", "hosts_campanha"); valor frozenset[str]
    | None. O registry NÃO conhece o significado da capacidade —
    compõe o frozenset nomeado pelo chamador; o sentido permanece da
    plataforma. A visão plana (a união de hoje) é o conjunto de
    chaves deste mapa.

    A origem retida torna AVALIÁVEL a disjunção de espaços com dono:
    uma chave com mais de um declarante é uma colisão observável. O
    registry compõe e expõe; não julga se a colisão é legítima —
    isso é regra do contrato.
    """
  em_cache = _composicao_cache.get(nome)
    if em_cache is not None:
        return em_cache
    mapa: Dict[str, set[str]] = {}
    for ident, plataforma in _catalogo.items():
        contrib = getattr(plataforma, nome, None)
        if not contrib:
            continue
        for elemento in contrib:
            mapa.setdefault(elemento, set()).add(ident)
          resultado = {elemento: frozenset(donos) for elemento, donos in mapa.items()}
    _composicao_cache[nome] = resultado
    return resultado


# ── Apoio à inicialização e à observabilidade ─────────────────────
def plataformas_registradas() -> tuple:
    """
    Devolve os identificadores de todas as plataformas registradas.

    Função de leitura, destinada a logs de inicialização e à
    verificação operacional. Não participa do fluxo da pipeline.
    """
    return tuple(_catalogo.keys())
    
