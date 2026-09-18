"""
Ajustes operacionais — Mercado Livre.

Módulo FOLHA: não importa nenhum outro módulo do pacote e não
contém lógica de fluxo. Responsabilidade ÚNICA: ler os parâmetros
de ajuste do ambiente sem NUNCA levantar exceção.

═══════════════════════════════════════════════════════════════════
POR QUE ESTE MÓDULO EXISTE
═══════════════════════════════════════════════════════════════════
As leituras de ajuste acontecem em nível de módulo, no import. E o
Auto Discovery, em REGISTRY_ENV=dev (o padrão), RE-LANÇA qualquer
falha de import e interrompe o boot inteiro.

O encadeamento, medido:

    ML_TIMEOUT_S="20s"
      → float("20s") levanta ValueError no import de cliente.py
      → _descobrir_plugins re-lança em modo dev
      → o processo não sobe

Ou seja: um dígito errado numa variável OPCIONAL do Mercado Livre
derrubaria Amazon, Shopee, Magalu e Netshoes junto. O contrato
manda o oposto — o Mercado Livre não pode derrubar as outras
plataformas —, e essa garantia não pode valer só depois do boot.

Aqui o valor inválido é ignorado e o padrão prevalece. O sistema
sobe degradado no ajuste, nunca ausente.

Existe como módulo próprio, e não como função copiada dentro de
`cliente` e `sessao`, pelo mesmo motivo que `afiliado` existe: as
duas camadas precisam da mesma leitura, e uma importar a outra só
para isso inverteria a direção de dependência entre transporte e
credencial.
"""
from __future__ import annotations

import os


def _bruto(nome: str) -> str:
    """Valor cru da variável, sempre string, sempre sem espaços."""
    try:
        return (os.environ.get(nome) or "").strip()
    except Exception:
        # Ambiente inacessível é cenário teórico, mas o custo de
        # cobri-lo é uma linha e o custo de não cobri-lo é o boot.
        return ""


def _avisar(nome: str, valor: str, padrao: object) -> None:
    """
    Registra o valor descartado. Falha de log jamais propaga: este
    módulo tem como única promessa não levantar exceção.

    O `logger` é importado aqui dentro, e não no topo, para que nem
    a indisponibilidade dele possa quebrar o import do pacote.
    """
    try:
        from logger import log_sys
        log_sys.warning(
            f"🛒 ML ajuste inválido | {nome}={valor!r} "
            f"— usando padrão {padrao}"
        )
    except Exception:
        pass


def numero(nome: str, padrao: float) -> float:
    """Lê um ajuste decimal. Valor ausente ou inválido → padrão."""
    valor = _bruto(nome)
    if not valor:
        return float(padrao)
    try:
        convertido = float(valor)
    except (TypeError, ValueError):
        _avisar(nome, valor, padrao)
        return float(padrao)
    if convertido <= 0:
        # Zero ou negativo em timeout, pausa ou prazo não é ajuste:
        # é desligamento acidental de uma proteção.
        _avisar(nome, valor, padrao)
        return float(padrao)
    return convertido


def inteiro(nome: str, padrao: int) -> int:
    """Lê um ajuste inteiro. Valor ausente ou inválido → padrão."""
    valor = _bruto(nome)
    if not valor:
        return int(padrao)
    try:
        convertido = int(valor)
    except (TypeError, ValueError):
        _avisar(nome, valor, padrao)
        return int(padrao)
    if convertido <= 0:
        _avisar(nome, valor, padrao)
        return int(padrao)
    return convertido


def texto(nome: str, padrao: str) -> str:
    """Lê um ajuste textual. Valor ausente → padrão."""
    return _bruto(nome) or padrao
