"""
Caso de uso de afiliação — Mercado Livre.

Dono EXCLUSIVO do fluxo. Único módulo do pacote que conhece ao
mesmo tempo o conhecimento de URL (`links`), a credencial
(`sessao`), o transporte (`cliente`) e o cache do core.

Nada abaixo deste módulo conhece cache, cenário ou publicação.

═══════════════════════════════════════════════════════════════════
FLUXO
═══════════════════════════════════════════════════════════════════
    reconhece
      → cache            (antes de qualquer I/O)
      → expande          (só encurtador próprio, com o core)
      → classifica
      → elegibilidade    (portão anterior à rede)
      → sanitiza
      → sessão
      → cliente
      → valida
      → cache

═══════════════════════════════════════════════════════════════════
DEGRADAÇÃO
═══════════════════════════════════════════════════════════════════
Falha em qualquer ponto resulta em AUSENTE. Nenhuma exceção
atravessa a fronteira da plataforma — o contrato exige que
`afilia` não propague erro, e o Mercado Livre não pode derrubar
Amazon, Shopee, Magalu ou Netshoes.

Nunca publica link parcial, link de terceiro ou a URL original
como se fosse afiliada.

═══════════════════════════════════════════════════════════════════
SESSÃO EXPIRADA
═══════════════════════════════════════════════════════════════════
Registra, alerta uma vez e devolve AUSENTE. Não repete, não tenta
login, não busca autenticação alternativa. As chamadas seguintes
saem cedo, sem tocar a rede, porque `sessao.disponivel()` passa a
ser falso.
"""
from __future__ import annotations

from typing import Optional

import aiohttp

import config
from logger import log_nrm
from plataformas.contrato import AUSENTE, Afiliacao
from utils.cache_links import consultar_link, registrar_link
from utils.url_resolver import desencurtar
from utils.urls import _sanitizar_url

from . import afiliado, cliente, links, sessao


# Uma tentativa extra apenas para falha transitória de rede ou
# resposta malformada. NÃO se aplica a credencial recusada nem a
# URL recusada, onde repetir só multiplica o dano.
_TENTATIVAS = 2


async def _expandir(
    url: str,
    sessao_http: aiohttp.ClientSession,
) -> Optional[str]:
    """
    Resolve um encurtador próprio usando o desencurtador do core.

    Devolve None quando a expansão sai do domínio do Mercado Livre
    — sinal de link adulterado ou de redirecionamento inesperado.
    """
    try:
        async with config._SEM_HTTP:
            expandida = await desencurtar(url, sessao_http)
    except Exception as exc:
        log_nrm.warning(f"🛒 ML expansão falhou: {type(exc).__name__}")
        return None

    if not links.reconhece(expandida):
        log_nrm.warning("🛒 ML expansão saiu do domínio — descarta")
        return None
    return expandida


async def afilia(url: str, sessao_http: aiohttp.ClientSession) -> object:
    """
    Converte uma URL do Mercado Livre em afiliação própria.

    Devolve Afiliacao quando gera link validado, ou AUSENTE em
    qualquer outro caso. Nunca levanta exceção.
    """
    url = _sanitizar_url(url)
    if not url or not links.reconhece(url):
        return AUSENTE

    if not afiliado.configurado():
        log_nrm.error("🛒 ML sem ML_TAG configurada → ausente")
        return AUSENTE

    # ── Cache antes de qualquer I/O ───────────────────────────────
    em_cache = consultar_link(url)
    if em_cache:
        return em_cache

    # ── Expansão, só quando é encurtador próprio ──────────────────
    alvo = url
    if links.precisa_expandir(url):
        expandida = await _expandir(url, sessao_http)
        if expandida is None:
            return AUSENTE
        alvo = expandida

    # ── Elegibilidade: portão anterior à rede ─────────────────────
    cenario = links.cenario_de(alvo)
    if not links.eh_elegivel(alvo):
        log_nrm.info(
            f"🛒 ML não elegível nesta frente | cenario={cenario or '?'}"
        )
        return AUSENTE

    # ── Sanitização e última barreira de identidade ───────────────
    alvo = links.sanitizar(alvo)
    if links.tem_identidade_externa(alvo):
        log_nrm.error("🛒 ML identidade externa sobreviveu → descarta")
        return AUSENTE

    # ── Credencial ────────────────────────────────────────────────
    if not sessao.disponivel():
        if sessao.expirada():
            log_nrm.info("🛒 ML sessão expirada — aguardando renovação")
        else:
            log_nrm.error(
                "🛒 ML sem credencial provisionada "
                "(ML_SESSION_COOKIE / ML_CSRF_TOKEN) → ausente"
            )
        return AUSENTE

    try:
        credencial = sessao.carregar()
    except sessao.SessaoExpirada:
        return AUSENTE
    except sessao.SessaoAusente as exc:
        log_nrm.error(f"🛒 ML credencial ausente: {exc}")
        return AUSENTE

    # ── Chamada ───────────────────────────────────────────────────
    # A sessão do core NÃO é usada aqui: o cliente tem jar próprio,
    # para que os cookies de afiliado não vazem para a sessão
    # compartilhada por todas as plataformas. A geração permite ao
    # cliente descartar o jar quando a credencial muda.
    geracao = sessao.geracao()

    resultado = None
    for tentativa in range(1, _TENTATIVAS + 1):
        try:
            resultado = await cliente.criar_link(alvo, credencial, geracao)
            break

        except cliente.CredencialRecusada as exc:
            # Sem retry: outra tentativa com a mesma credencial
            # morta só produz o mesmo erro.
            await sessao.marcar_expirada(str(exc))
            return AUSENTE

        except cliente.UrlRecusada as exc:
            # Sessão continua válida; esta URL é que não serve.
            log_nrm.info(f"🛒 ML URL recusada pelo programa: {exc}")
            return AUSENTE

        except cliente.DisjuntorAberto as exc:
            # Proteção da conta, não falha desta oferta. Repetir
            # derrotaria o propósito do disjuntor.
            log_nrm.info(f"🛒 ML chamadas suspensas: {exc}")
            return AUSENTE

        except cliente.ErroCliente as exc:
            log_nrm.warning(f"🛒 ML tentativa {tentativa} falhou: {exc}")
            if tentativa >= _TENTATIVAS:
                return AUSENTE

        except Exception as exc:
            log_nrm.error(f"🛒 ML erro inesperado: {type(exc).__name__}")
            return AUSENTE

    if resultado is None:
        return AUSENTE

    afiliacao = Afiliacao(
        publicada=resultado.publicada,
        canonica=resultado.canonica,
    )
    registrar_link(url, afiliacao, afiliado.IDENTIFICADOR)
    log_nrm.info(f"🛒 ML afiliado | {resultado.publicada}")
    return afiliacao
