"""
Encurtador de Links.

Componente do core, de função fechada e única: encurtar um link
afiliado longo.

O encurtamento não é característica de uma plataforma específica.
É um comportamento exigido por múltiplas plataformas cujos links
afiliados são longos demais para uma apresentação profissional, e
a sua lógica é idêntica entre elas. Por satisfazer o critério de
transversalidade genuína, o encurtamento reside no core.

CRITÉRIO DE TRANSVERSALIDADE (restrição arquitetural):
  Um comportamento só reside no core como capacidade compartilhada
  quando for genuinamente transversal a múltiplas plataformas, com
  lógica idêntica entre elas. Afiliação, reconhecimento e extração
  de identidade NÃO satisfazem esse critério: são capacidades do
  contrato, implementadas por cada plataforma.

CONVENÇÃO DE ERROS:
  - configuração ausente é defeito de configuração e é verificada
    na importação do módulo, falhando de imediato e de forma
    visível, e não silenciada em tempo de execução;
  - entrada inválida é uso incorreto da função e resulta em
    ValueError, levantado fora da blindagem;
  - falha operacional inesperada em tempo de execução é capturada
    e isolada, e a função recorre ao escape para o link longo.

Este módulo entrega a FASE SÍNCRONA do encurtamento. A FASE
POSTERIOR será entregue em sub-passo próprio, com coordenação
explícita e direta com a camada de publicação, sem infraestrutura
assíncrona genérica.

Este módulo pertence ao core. Depende da mediação de persistência
de links curtos e da configuração do sistema. Não depende de
nenhuma plataforma, da pipeline ou da camada de publicação. Não
conhece banco de dados: a persistência é mediada.
"""
from __future__ import annotations

import hashlib

from config import _SHORT_BASE as SHORT_BASE_URL
from logger import log_sys
from utils.links_curtos import registrar_codigo


# ── Validação de configuração (defeito de configuração) ───────────
# Um domínio base ausente é defeito de configuração, não condição
# de runtime. É verificado na importação do módulo, falhando de
# imediato, para que a inconsistência se manifeste na inicialização
# do sistema e não seja mascarada como escape operacional.
if not SHORT_BASE_URL:
    raise RuntimeError(
        "SHORT_BASE_URL não configurado: o encurtador exige um "
        "domínio base de redirecionamento."
    )


# ── Derivação do código curto ─────────────────────────────────────
_TAMANHO_CODIGO = 7
# Códigos tentados por URL antes do escape para o link longo. 28 bits
# por código: a chance de 8 colisões seguidas é desprezível; o teto só
# limita o custo quando o banco está falhando.
_MAX_TENTATIVAS_CODIGO = 8


def _derivar_codigo(url_afiliada: str, tentativa: int = 0) -> str:
    """
    Deriva um código curto estável e determinístico a partir da URL
    afiliada. Uma mesma URL produz sempre a mesma SEQUÊNCIA de códigos,
    o que torna o registro idempotente e coerente com a semântica de
    não sobrescrita da mediação de persistência.

    Tentativa 0 é o sha256(url)[:7] de sempre — todo código já
    publicado continua idêntico. Só na colisão (o código já pertence a
    outra URL) a tentativa n ≥ 1 deriva de `url + NUL + n`: NUL não
    ocorre em URL, então a semente salgada nunca é a de outra URL.
    """
    semente = (url_afiliada if not tentativa
               else f"{url_afiliada}\x00{tentativa}")
    return hashlib.sha256(
        semente.encode()
    ).hexdigest()[:_TAMANHO_CODIGO]


def _compor_url_curta(codigo: str) -> str:
    """Compõe a URL curta a partir do domínio base e do código."""
    return f"{SHORT_BASE_URL}/{codigo}"


# ── Fase síncrona do encurtamento ─────────────────────────────────
def encurtar(url_afiliada: str) -> str:
    """
    Fase síncrona do encurtamento de um link afiliado.

    Devolve SEMPRE uma URL utilizável para publicação:
      - em caso de êxito, a URL curta;
      - em caso de falha no registro de persistência, a própria URL
        afiliada longa (escape);
      - em caso de falha operacional inesperada em qualquer ponto
        do fluxo, a própria URL afiliada longa (escape).

    A blindagem do fluxo garante, em definitivo, o princípio de que
    a falha de encurtamento nunca impede a publicação. Ela cobre a
    falha operacional de runtime; não cobre, deliberadamente, o
    defeito de configuração, verificado na importação do módulo.

    Entrada inválida: a ausência de `url_afiliada` constitui uso
    incorreto da função e resulta em ValueError, levantado fora da
    blindagem por ser defeito de programação, não falha operacional.

    Os registros em log utilizam exclusivamente o código derivado
    ou o tipo da exceção, nunca a URL afiliada, que pode conter
    tokens ou assinaturas.
    """
    if not url_afiliada:
        raise ValueError("url_afiliada é obrigatória")

    try:
        # O código publicado tem de apontar para ESTA URL: registrar_codigo
        # só confirma quando aponta. Na colisão, o próximo da sequência
        # determinística; esgotada (ou banco falhando), escape longo.
        for tentativa in range(_MAX_TENTATIVAS_CODIGO):
            codigo = _derivar_codigo(url_afiliada, tentativa)
            if registrar_codigo(codigo, url_afiliada):
                url_curta = _compor_url_curta(codigo)
                log_sys.info(
                    f"🔗 encurtado | codigo={codigo}"
                    + (f" | tentativa={tentativa}" if tentativa else ""))
                return url_curta

        log_sys.warning(
            f"⚠️ encurtar: nenhum código registrado em "
            f"{_MAX_TENTATIVAS_CODIGO} tentativas, escape para link longo "
            f"| codigo={_derivar_codigo(url_afiliada)}"
        )
        return url_afiliada

    except Exception as exc:
        log_sys.warning(
            f"⚠️ encurtar: falha inesperada, escape para link "
            f"longo | erro={type(exc).__name__}"
        )
        return url_afiliada
