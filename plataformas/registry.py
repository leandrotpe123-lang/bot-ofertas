"""
Registry de Plataformas.

Único ponto de contato entre o core da pipeline e as plataformas.
A pipeline nunca importa uma plataforma concreta: consulta o registry.

O registry possui exatamente três funções:
  - cadastrar : registra uma plataforma, com verificação estrutural
  - resolver  : determina qual plataforma reconhece uma URL
  - acessar   : devolve o objeto de uma plataforma por identificador

O registry NÃO executa lógica de pipeline. NÃO classifica conteúdo,
NÃO normaliza, NÃO calcula score, NÃO deduplica, NÃO publica. NÃO
acessa rede, banco ou cache. NÃO mantém estado de processamento.
Mantém apenas o catálogo de plataformas, estabelecido na
inicialização.

Baseline arquitetural: Documento 2 — Especificação do Registry.
"""
from __future__ import annotations

from typing import Dict, Optional

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
    """
    # Verificação 1: versão do contrato
    if plataforma.versao_contrato != CONTRACT_VERSION:
        raise ErroCadastroPlataforma(
            f"Plataforma '{plataforma.identificador}': versão de "
            f"contrato {plataforma.versao_contrato} incompatível "
            f"com a suportada ({CONTRACT_VERSION})."
        )

    # Verificação 2: unicidade de identidade
    if plataforma.identificador in _catalogo:
        raise ErroCadastroPlataforma(
            f"Plataforma '{plataforma.identificador}': identificador "
            f"já registrado."
        )

    # Verificação 3: conformidade mínima com o contrato
    obrigatorias = ("reconhece", "extrai_identidade", "afilia")
    for nome in obrigatorias:
        capacidade = getattr(plataforma, nome, None)
        if not callable(capacidade):
            raise ErroCadastroPlataforma(
                f"Plataforma '{plataforma.identificador}': capacidade "
                f"obrigatória '{nome}' ausente ou inválida."
            )

    _catalogo[plataforma.identificador] = plataforma
    log_sys.info(
        f"🧩 Plataforma registrada | id={plataforma.identificador} "
        f"contrato=v{plataforma.versao_contrato}"
    )


# ── Função 2: resolução ───────────────────────────────────────────
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

    Retorna a plataforma correspondente, ou None quando nenhuma
    plataforma reconhece a URL. None é o resultado de não
    reconhecimento; a política de fallback de três estágios é
    decidida pelo core.
    """
    if not url:
        return None
    for plataforma in _catalogo.values():
        try:
            if plataforma.reconhece(url):
                return plataforma
        except Exception as e:
            # Uma capacidade de reconhecimento conforme ao contrato
            # não falha. Uma falha aqui indica plataforma fora de
            # conformidade; é registrada e a resolução prossegue,
            # para não derrubar o processamento por defeito de uma
            # única plataforma.
            log_sys.error(
                f"❌ reconhece() falhou | plataforma="
                f"{plataforma.identificador}: {e}"
            )
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


# ── Apoio à inicialização e à observabilidade ─────────────────────
def plataformas_registradas() -> tuple:
    """
    Devolve os identificadores de todas as plataformas registradas.

    Função de leitura, destinada a logs de inicialização e à
    verificação operacional. Não participa do fluxo da pipeline.
    """
    return tuple(_catalogo.keys())
