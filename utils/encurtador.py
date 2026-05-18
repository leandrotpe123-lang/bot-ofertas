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

from config import SHORT_BASE_URL
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


def _derivar_codigo(url_afiliada: str) -> str:
    """
    Deriva um código curto estável e determinístico a partir da URL
    afiliada. Uma mesma URL produz sempre o mesmo código, o que
    torna o registro idempotente e coerente com a semântica de não
    sobrescrita da mediação de persistência.
    """
    return hashlib.sha256(
        url_afiliada.encode()
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
        codigo = _derivar_codigo(url_afiliada)
        registrado = registrar_codigo(codigo, url_afiliada)

        if not registrado:
            log_sys.warning(
                f"⚠️ encurtar: falha no registro, escape para link "
                f"longo | codigo={codigo}"
            )
            return url_afiliada

        url_curta = _compor_url_curta(codigo)
        log_sys.info(f"🔗 encurtado | codigo={codigo}")
        return url_curta

    except Exception as exc:
        log_sys.warning(
            f"⚠️ encurtar: falha inesperada, escape para link "
            f"longo | erro={type(exc).__name__}"
        )
        return url_afiliada
