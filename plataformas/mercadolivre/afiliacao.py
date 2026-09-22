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
                         entrada de contrato anterior → reparo:
                         expande + descobre, sem createLink,
                         `publicada` preservada
      → expande          (só encurtador próprio, com o core)
      → descobre         (vitrine → produto ou lista, query intacta)
      → /sec/ próprio    (substituição, sem rede)
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

from collections import OrderedDict
from typing import Optional

import aiohttp

import config
from logger import log_nrm
from plataformas.contrato import AUSENTE, Afiliacao
from utils.cache_links import consultar_link, registrar_link
from utils.url_resolver import desencurtar
from utils.urls import _sanitizar_url

from . import afiliado, cliente, descoberta, links, sessao


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


async def _resolver_alvo(
    url: str,
    sessao_http: aiohttp.ClientSession,
) -> Optional[str]:
    """
    Do link recebido ao DESTINO: expansão do encurtador próprio e
    descoberta da vitrine. Devolve o alvo ainda NÃO sanitizado, ou
    None quando o destino não pôde ser determinado.

    Único dono destes dois passos — usado pela afiliação nova e pelo
    reparo de entrada antiga, para que as duas cheguem ao MESMO alvo
    pelo MESMO caminho. Nunca levanta exceção.
    """
    # ── Expansão, só quando é encurtador próprio ──────────────────
    alvo = url
    if links.precisa_expandir(url):
        expandida = await _expandir(url, sessao_http)
        if expandida is None:
            return None
        alvo = expandida

    # ── Descoberta da vitrine ─────────────────────────────────────
    # A vitrine é porta de entrada, não destino. O documento servido
    # diz, em estrutura nomeada, se ela representa um produto ou uma
    # lista — e qual.
    #
    # A posição no fluxo não é arbitrária:
    #
    #   antes da ELEGIBILIDADE — a vitrine não é elegível por si só,
    #   e com razão: ela carrega a identidade de um afiliado de
    #   terceiro. O portão a barraria antes de qualquer descoberta.
    #   É a descoberta que produz o alvo elegível.
    #
    #   antes da SANITIZAÇÃO — `sanitizar` prepara a URL que vai ao
    #   createLink, não uma URL que vamos BUSCAR. A busca usa a URL
    #   como chegou: o `ref` é condição comprovada para o servidor
    #   entregar a vitrine do item compartilhado (sem ele, 302 para
    #   `/social/<afiliado>/lists`), e sobre os `matt_*` não há
    #   prova de que sejam ignorados na escolha. Quem é sanitizado é
    #   o DESTINO descoberto, logo abaixo — e é só ele que viaja.
    if links.precisa_descobrir(alvo):
        destino = await descoberta.descobrir(alvo, sessao_http)
        if destino is None:
            log_nrm.info("🛒 ML vitrine sem destino reconhecível → ausente")
            return None
        log_nrm.info(
            f"🛒 ML descoberta | tipo={destino.tipo} | origem=social"
        )
        alvo = destino.url

    return alvo


# ══════════════════════════════════════════════════════════════════
# VIGÊNCIA DO CACHE
# ══════════════════════════════════════════════════════════════════
# O cache de links é PERSISTENTE (sobrevive a deploys) e a leitura
# renova o carimbo de tempo: entrada muito usada nunca expira.
#
# Antes da correção da canônica, este adaptador gravava como
# `canonica` a `long_url` devolvida pelo createLink — medido: é SEMPRE
# a nossa vitrine `/social/<tag>?…&ref=<aleatório>`, qualquer que seja
# o destino. Em formato ainda mais antigo, gravava só uma string, e a
# camada de cache devolve `canonica = publicada` (o nosso `meli.la`).
# Nos dois formatos o produto e a lista estão PERDIDOS, e toda
# mensagem que reusa um link já visto sai sem identidade: produto com
# cupom ancora no cupom; produto sem cupom ancora na vitrine com um
# `ref` diferente a cada link — o mesmo produto duplica.
#
# O contrato VIGENTE grava exatamente dois formatos — é tudo o que
# `afilia` registra:
#
#   createLink   canonica = alvo SANITIZADO e ELEGÍVEL (produto ou
#                listagem) — o destino real;
#   /sec/        publicada = o nosso `/sec/`, canonica = a URL
#                recebida (distinta por oferta, de propósito).
#
# Qualquer outra forma é de contrato anterior. O teste é estrutural,
# sobre a própria entrada: nada de heurística, nada de data.
def afiliacao_vigente(afiliacao: object) -> bool:
    """
    Capacidade opcional do contrato. Pura, sem I/O.

    Verdadeiro se a entrada de cache obedece ao contrato vigente —
    e então pode ser servida sem rede.
    """
    publicada = getattr(afiliacao, "publicada", None)
    if publicada is None and isinstance(afiliacao, str):
        publicada = afiliacao
    canonica = getattr(afiliacao, "canonica", None) or publicada or ""

    if links.eh_elegivel(canonica):
        return True

    nosso_sec = afiliado.sec_proprio()
    return bool(nosso_sec) and publicada == nosso_sec


# URLs cujo reparo já falhou NESTE processo. Sem isto, uma entrada
# irreparável (vitrine cujo `ref` expirou, por exemplo) buscaria a
# rede a cada mensagem que a reusa. Limitado, para não crescer sem
# fim; o reinício do processo concede uma nova tentativa.
_REPARO_FALHOU: "OrderedDict[str, None]" = OrderedDict()
_REPARO_FALHOU_LIMITE = 2048


def _memorizar_falha(url: str) -> None:
    _REPARO_FALHOU[url] = None
    _REPARO_FALHOU.move_to_end(url)
    while len(_REPARO_FALHOU) > _REPARO_FALHOU_LIMITE:
        _REPARO_FALHOU.popitem(last=False)


async def _reparar(
    url: str,
    em_cache: object,
    sessao_http: aiohttp.ClientSession,
) -> object:
    """
    Recalcula a canônica de uma entrada de contrato anterior.

    Refaz SÓ o que determina o destino — expansão e descoberta, pelo
    mesmo `_resolver_alvo` da afiliação nova — e aplica o mesmo portão
    de elegibilidade e a mesma sanitização. Resultado: a canônica é
    idêntica à que uma afiliação nova da mesma URL produziria.

    O que NÃO muda:
      · `publicada` — o link que já está no ar continua o mesmo; o
        reparo não gera link novo;
      · o createLink não é chamado, e a credencial não é tocada
        (a descoberta é anônima — INV-ML-9);
      · em qualquer falha, devolve a entrada antiga intacta: o
        comportamento é o de antes do reparo, nunca pior.

    Nunca levanta exceção.
    """
    if url in _REPARO_FALHOU:
        return em_cache

    try:
        alvo = await _resolver_alvo(url, sessao_http)
        if alvo is not None and links.eh_elegivel(alvo):
            alvo = links.sanitizar(alvo)
            if not links.tem_identidade_externa(alvo):
                publicada = getattr(em_cache, "publicada", None) or str(em_cache)
                reparada = Afiliacao(publicada=publicada, canonica=alvo)
                registrar_link(url, reparada, afiliado.IDENTIFICADOR)
                log_nrm.info(
                    f"🛒 ML canônica reparada | "
                    f"cenario={links.cenario_de(alvo)}"
                )
                return reparada
    except Exception as exc:
        log_nrm.warning(f"🛒 ML reparo falhou: {type(exc).__name__}")

    _memorizar_falha(url)
    log_nrm.info("🛒 ML canônica não reparável — mantém a entrada anterior")
    return em_cache


async def afilia(url: str, sessao_http: aiohttp.ClientSession) -> object:
    """
    Converte uma URL do Mercado Livre em afiliação própria.

    Devolve Afiliacao quando gera link validado, ou AUSENTE em
    qualquer outro caso. Nunca levanta exceção.
    """
    url = _sanitizar_url(url)
    if not url or not links.reconhece(url):
        return AUSENTE

    # ── Cache antes de qualquer I/O ───────────────────────────────
    # Entrada vigente: servida sem rede (INV-ML-3). Entrada gravada
    # sob o contrato anterior: reparada uma única vez, sem createLink.
    #
    # Vem ANTES da checagem da tag de propósito: o core sempre serviu
    # entradas de cache sem consultar a tag, e o reparo não usa a tag
    # (não chama createLink). Uma entrada antiga nunca pode virar
    # AUSENTE só porque passou a ser encaminhada para cá.
    em_cache = consultar_link(url)
    if em_cache:
        if afiliacao_vigente(em_cache):
            return em_cache
        return await _reparar(url, em_cache, sessao_http)

    if not afiliado.configurado():
        log_nrm.error("🛒 ML sem ML_TAG configurada → ausente")
        return AUSENTE

    # ── Expansão e descoberta ─────────────────────────────────────
    alvo = await _resolver_alvo(url, sessao_http)
    if alvo is None:
        return AUSENTE

    # ── `/sec/` de terceiro: substituição pelo nosso ──────────────
    # O `/sec/` alheio não converte — medido contra o gerador
    # oficial e registrado em testes_ml/FORMATOS.md. E a sonda da
    # Frente 7 mostrou que ele expande para
    # `/social/<slug>/lists/<uuid>`, que também não é afiliável.
    # Não há rota de conversão.
    #
    # Sem substituição o core descarta o BLOCO inteiro
    # (`filtros_bloco`: "link DE PLATAFORMA que não converteu —
    # sai"), e o cupom vai junto. Publicar o nosso `/sec/` preserva
    # a oferta e credita a nós.
    #
    # IDENTIDADE: a `publicada` é CONSTANTE — é sempre o mesmo
    # link. Se ela fosse também a canônica, todas as ofertas
    # `/sec/` colapsariam numa identidade só e a deduplicação
    # descartaria as seguintes como repetidas. Por isso a canônica
    # é a URL RECEBIDA, que é distinta por oferta.
    #
    # Sem `ML_SEC_PROPRIO` configurado, nada muda: segue para a
    # elegibilidade e termina em AUSENTE, como hoje.
    if links.cenario_de(alvo) == links.CENARIO_SEC:
        nosso = afiliado.sec_proprio()
        if nosso:
            afiliacao_sec = Afiliacao(publicada=nosso, canonica=url)
            registrar_link(url, afiliacao_sec, afiliado.IDENTIFICADOR)
            log_nrm.info("🛒 ML /sec/ de terceiro → /sec/ próprio")
            return afiliacao_sec

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

    # ── CANÔNICA = O ALVO ENVIADO ────────────────────────────────
    # Medido na sonda: o createLink devolve SEMPRE a nossa própria
    # vitrine social como `long_url`, qualquer que seja a URL
    # enviada. Usar essa resposta como canônica destrói a identidade
    # do destino — todo post de ML passava a ter a mesma canônica.
    #
    # As duas coisas são distintas e continuam distintas:
    #   canônica / destino    = `alvo`, o que a oferta APONTA
    #   atribuição própria    = validada pela resposta do servidor,
    #                           que segue sendo lida em `resultado`
    #                           e NÃO é substituída por esta linha.
    #
    # `resultado.publicada` — o que de fato vai ao ar — permanece
    # exatamente o que o servidor devolveu. Nada no cliente HTTP
    # nem na sessão é tocado.
    afiliacao = Afiliacao(
        publicada=resultado.publicada,
        canonica=alvo,
    )
    registrar_link(url, afiliacao, afiliado.IDENTIFICADOR)
    log_nrm.info(f"🛒 ML afiliado | {resultado.publicada}")
    return afiliacao
