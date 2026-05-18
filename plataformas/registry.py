"""
Registry de Plataformas.

Único ponto de contato entre o core da pipeline e as plataformas.
A pipeline nunca importa uma plataforma concreta: consulta o registry.

Três funções: cadastrar, resolver, acessar. O registry não executa
lógica de pipeline, não acessa rede ou banco, e não mantém estado
de processamento — apenas o catálogo de plataformas.

Baseline arquitetural: Documento 2 — Especificação do Registry.
"""
from __future__ import annotations

import os
from typing import Dict, Optional

from logger import log_sys
from plataformas.contrato import CONTRACT_VERSION, Plataforma


# ── Modo de execução ──────────────────────────────────────────────
# Em desenvolvimento, uma falha em reconhece() é defeito de contrato
# e interrompe a resolução. Em produção, é registrada e isolada.
_MODO_PRODUCAO = os.environ.get("REGISTRY_ENV", "dev").lower() == "prod"


# ── Exceção de cadastro ───────────────────────────────────────────
class ErroCadastroPlataforma(Exception):
    """
    Emitida quando um cadastro de plataforma é rejeitado. A rejeição
    é erro de inicialização, não condição silenciosa.
    """


# ── Catálogo interno ──────────────────────────────────────────────
_catalogo: Dict[str, Plataforma] = {}


# ── Função 1: cadastro ────────────────────────────────────────────
def cadastrar(plataforma: Plataforma) -> None:
    """
    Registra uma plataforma no catálogo.

    Verificações estruturais de inicialização:
      1. versão do contrato compatível com a suportada pelo core;
      2. capacidades obrigatórias presentes;
      3. identidade — distingue dois casos:
           - identificador já ocupado pelo MESMO objeto:
             operação sem efeito (recadastro idêntico, tolerado
             para permitir reexecução do startup no mesmo processo);
           - identificador já ocupado por objeto DIFERENTE:
             ErroCadastroPlataforma (colisão real de identidade).

    Qualquer falha das verificações 1 ou 2, ou colisão na 3, emite
    ErroCadastroPlataforma e a plataforma NÃO é registrada.
    """
    # Verificação 1: versão do contrato
    if plataforma.versao_contrato != CONTRACT_VERSION:
        raise ErroCadastroPlataforma(
            f"Plataforma '{plataforma.identificador}': versão de "
            f"contrato {plataforma.versao_contrato} incompatível "
            f"com a suportada ({CONTRACT_VERSION})."
        )

    # Verificação 2: conformidade mínima com o contrato
    obrigatorias = ("reconhece", "extrai_identidade", "afilia")
    for nome in obrigatorias:
        if not callable(getattr(plataforma, nome, None)):
            raise ErroCadastroPlataforma(
                f"Plataforma '{plataforma.identificador}': capacidade "
                f"obrigatória '{nome}' ausente ou inválida."
            )

    # Verificação 3: identidade
    existente = _catalogo.get(plataforma.identificador)
    if existente is not None:
        if existente is plataforma:
            # Recadastro idêntico: operação sem efeito. Permite a
            # reexecução de _run() no mesmo processo (restart parcial).
            return
        # Colisão real: identificador ocupado por objeto diferente.
        raise ErroCadastroPlataforma(
            f"Plataforma '{plataforma.identificador}': identificador "
            f"já ocupado por outra definição de plataforma."
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

    Consulta a capacidade de reconhecimento de cada plataforma. O
    contrato garante reconhecimento mutuamente exclusivo, portanto
    há no máximo uma correspondência e a ordem é irrelevante.

    Pura e determinística. Retorna a plataforma correspondente, ou
    None quando nenhuma reconhece a URL.

    Exceção em reconhece() é defeito de contrato: em desenvolvimento
    é propagada; em produção é registrada e isolada.
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
    Devolve o objeto de uma plataforma a partir do identificador.
    Pura e determinística. Retorna None quando o identificador não
    corresponde a nenhuma plataforma registrada.
    """
    return _catalogo.get(identificador)


# ── Apoio à observabilidade ───────────────────────────────────────
def plataformas_registradas() -> tuple:
    """Identificadores das plataformas registradas. Apenas leitura."""
    return tuple(_catalogo.keys())
