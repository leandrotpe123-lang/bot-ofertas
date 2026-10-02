"""
Plataforma Shopee — afiliação.

Domínio único: a integração com o serviço externo de afiliados da
Shopee — chamada GraphQL autenticada por assinatura criptográfica,
política de novas tentativas, expansão de encurtador, cache de links
mediado e repasse direto. É a capacidade de efeito colateral
controlado do contrato (acessa rede).

Fluxo de dependência: depende de `links` (reconhecimento de
encurtador, canonização e identidade da plataforma) e dos recursos
externos do core (cache, resolver, config). Não é importado por
`links` — a direção é única.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from typing import Optional
from collections import OrderedDict

import aiohttp

import config
from logger import log_nrm
from plataformas.contrato import AUSENTE, Afiliacao
from utils.cache_links import consultar_link, descartar_link, registrar_link
from utils.url_resolver import desencurtar
from utils.urls import _cache_key, _netloc, _sanitizar_url

from .links import (
    _DOMINIOS,
    _ENCURTADORES,
    _IDENTIFICADOR,
    PAGINA_CARRINHO,
    PAGINA_CARTEIRA,
    _bate_dominio,
    _canonica_live,
    _url_produto_canonica,
    limpa_url,
    pagina_da_conta,
)


_SHP_APP_ID = os.environ.get("SHOPEE_APP_ID", "")
_SHP_SECRET = os.environ.get("SHOPEE_SECRET", "")


# ── Endpoint do serviço de afiliados ──────────────────────────────
_ENDPOINT_AFILIADOS = "https://open-api.affiliate.shopee.com.br/graphql"
# TETO de tempo. A API responde em ~0,2 s (média medida em produção,
# 83 chamadas, 0 falha). Antes: 3 tentativas de 12 s + esperas de 1,5 e
# 3 s — uma conexão travada prendia o post por até 40 s, e a tentativa
# seguinte (que costuma responder na hora) só vinha depois de 12 s.
# Agora o prazo CRESCE por tentativa: a travada é abandonada em 3 s e
# refeita numa conexão nova; uma API lenta, mas viva, ainda tem 5 e
# 8 s. Pior caso 3+0,5+5+1+8 = 17,5 s. O prazo vale só para a
# requisição — a espera na fila do semáforo HTTP não conta.
_TIMEOUTS_AFILIACAO = (3, 5, 8)
_TENTATIVAS_AFILIACAO = len(_TIMEOUTS_AFILIACAO)
_ESPERA_ENTRE_TENTATIVAS = 0.5      # 0,5 s, depois 1 s
# ── Instrumentação TEMPORÁRIA de desempenho ───────────────────────
# Ligada só com SHP_PERF=1; desligada, cada gancho é um `if` sobre
# uma constante de módulo — custo nulo. Não grava em disco, não faz
# I/O e não altera nenhum retorno. Remover quando a medição terminar.
#
# Acumula em memória e emite um resumo em DEBUG a cada _PERF_RESUMO
# afiliações, para não inflar o volume de log já existente.
_PERF = os.environ.get("SHP_PERF", "") == "1"
_PERF_RESUMO = 25
_PERF_VISTAS_MAX = 2000
_perf_c: dict = {}
_perf_t: dict = {}
_perf_vistas: OrderedDict = OrderedDict()


def _perf_agora() -> float:
    return time.monotonic() if _PERF else 0.0


def _perf_entrada(url: str) -> None:
    """Conta a chamada e detecta reprocessamento da MESMA URL."""
    if not _PERF:
        return
    _perf_c["n"] = _perf_c.get("n", 0) + 1
    if url in _perf_vistas:
        _perf_c["repetida"] = _perf_c.get("repetida", 0) + 1
    _perf_vistas[url] = 1
    _perf_vistas.move_to_end(url)
    if len(_perf_vistas) > _PERF_VISTAS_MAX:
        _perf_vistas.popitem(last=False)


def _perf_marca(via: str, t0: float = 0.0) -> None:
    """Registra o caminho tomado e, quando t0 vem, o tempo gasto."""
    if not _PERF:
        return
    _perf_c[via] = _perf_c.get(via, 0) + 1
    if t0:
        _perf_t[via] = _perf_t.get(via, 0.0) + (time.monotonic() - t0)
    n = _perf_c.get("n", 0)
    if n and n % _PERF_RESUMO == 0 and n != _perf_c.get("_ult"):
        _perf_c["_ult"] = n
        medias = " ".join(
            f"{k}={_perf_t[k] / max(_perf_c.get(k, 1), 1) * 1000:.0f}ms"
            for k in sorted(_perf_t)
        )
        contas = " ".join(
            f"{k}={v}" for k, v in sorted(_perf_c.items())
            if not k.startswith("_")
        )
        log_nrm.debug(f"📊 SHP perf | {contas} | media {medias}")

# ── Política de repasse direto (sem afiliação) ────────────────────
# Domínio cujas URLs são publicadas como recebidas, sem passar pelo
# serviço de afiliados. É política de afiliação, não classificação.
_REPASSE_DIRETO = frozenset({"flapremios.com.br"})

# ── LINKS FIXOS do operador: carteira de cupons e carrinho ────────
# Mesmo princípio do `/sec/` próprio do Mercado Livre: a página é a
# mesma para todo mundo, então sai sempre o NOSSO link — os que o
# canal publica há semanas (a API devolve exatamente estes para as
# duas páginas; conferido no histórico do canal desde 24/09).
_LINK_FIXO = {
    PAGINA_CARTEIRA: "https://s.shopee.com.br/8pkZllbmly",
    PAGINA_CARRINHO: "https://s.shopee.com.br/1qapQu2VFM",
}

# Encurtadores CONHECIDOS que levam a essas páginas — trocados NA HORA,
# sem expandir e sem chamar a API: o código curto da Shopee é fixo
# (sempre o mesmo destino). Valor = a canônica que a expansão daria (a
# página limpa), usada só quando o cache não tem a gravada — a
# identidade do post fica a mesma. Fonte: URLs expandidas enviadas
# pelo operador em 02/10 e o corpus dos canais (Fada 1qU7Zs67MB e
# 7plKiu5H62; Promotom/fumotom 70GSVaUJxC e 7fW9IpEnZG; os nossos).
# Código fora daqui que leve a essas páginas também sai com o link
# fixo, mas só depois de expandido (regra de destino em `afilia`):
# adivinhar pelo rótulo "Resgate aqui" levaria cupom de página
# própria para a carteira.
_CURTOS_CONHECIDOS = {
    "1qU7Zs67MB": "https://shopee.com.br/user/voucher-wallet",   # Fada · Resgate aqui
    "7plKiu5H62": "https://shopee.com.br/cart/",                 # Fada · Carrinho
    "70GSVaUJxC": "https://shopee.com.br/user/voucher-wallet",   # Promotom · Resgate aqui
    "7fW9IpEnZG": "https://shopee.com.br/cart",                  # Promotom · Carrinho
    "8pkZllbmly": "https://shopee.com.br/user/voucher-wallet",   # nosso · carteira
    "1qapQu2VFM": "https://shopee.com.br/cart",                  # nosso · carrinho
}


def _curto_conhecido(url: str) -> Optional[str]:
    """A canônica de um encurtador conhecido de carteira/carrinho, ou
    None. Compara o código exato (a Shopee diferencia maiúsculas) e só
    em `s.shopee.com.br` — cada encurtador tem os próprios códigos;
    query e fragmento não mudam o destino de um código curto."""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    if (parsed.netloc or "").lower() != "s.shopee.com.br":
        return None
    return _CURTOS_CONHECIDOS.get((parsed.path or "").strip("/"))


def _fixo(canonica: str) -> Optional[Afiliacao]:
    """O nosso link fixo para a página da conta, com a canônica
    recebida (a identidade não muda); None para qualquer outra página."""
    pagina = pagina_da_conta(canonica)
    if pagina is None:
        return None
    return Afiliacao(publicada=_LINK_FIXO[pagina], canonica=canonica)

async def _chamar_servico_afiliados(
    url_produto: str, sessao: aiohttp.ClientSession,
) -> Optional[str]:
    """
    Chama o serviço externo de geração de link afiliado.

    Aplica autenticação por assinatura criptográfica e política de
    novas tentativas com intervalo progressivo. Devolve o link
    afiliado, ou None quando o serviço não o produz.
    """
    for tentativa, prazo in enumerate(_TIMEOUTS_AFILIACAO, start=1):
        try:
            ts = str(int(time.time()))
            payload = json.dumps(
                {"query": (
                    f'mutation {{ generateShortLink(input: '
                    f'{{ originUrl: "{url_produto}" }}) '
                    f'{{ shortLink }} }}'
                )},
                separators=(",", ":"),
            )
            assinatura = hashlib.sha256(
                f"{_SHP_APP_ID}{ts}{payload}{_SHP_SECRET}".encode()
            ).hexdigest()
            headers = {
                "Authorization": (
                    f"SHA256 Credential={_SHP_APP_ID},"
                    f"Timestamp={ts},Signature={assinatura}"
                ),
                "Content-Type": "application/json",
            }
            async with config._SEM_HTTP:
                async with sessao.post(
                    _ENDPOINT_AFILIADOS,
                    data=payload, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=prazo),
                ) as resposta:
                    dados = await resposta.json()
                    link = (
                        dados.get("data", {})
                        .get("generateShortLink", {})
                        .get("shortLink")
                    )
                    if link:
                        log_nrm.info(
                            f"✅ SHP t={tentativa}: {link}"
                        )
                        return link
                    log_nrm.warning(
                        f"⚠️ SHP serviço t={tentativa}: "
                        f"{dados.get('errors') or dados.get('error')}"
                    )
        except Exception as e:
            log_nrm.warning(f"⚠️ SHP t={tentativa}: {e}")
        if tentativa < _TENTATIVAS_AFILIACAO:
            await asyncio.sleep(tentativa * _ESPERA_ENTRE_TENTATIVAS)
    return None


def afiliacao_vigente(afiliacao: object) -> bool:
    """
    Capacidade opcional do contrato. Pura, sem I/O.

    Falsa quando a canônica gravada ainda é um ENCURTADOR da Shopee: a
    expansão não aconteceu (desencurtar devolve a própria entrada em
    timeout ou falha de rede) e a identidade derivada seria a URL curta
    — única por link, nunca o produto. Entradas assim, gravadas antes
    desta guarda, deixam de ser servidas: `afilia` refaz a expansão e,
    no sucesso, sobrescreve a entrada.
    """
    publicada = getattr(afiliacao, "publicada", None)
    if publicada is None and isinstance(afiliacao, str):
        publicada = afiliacao
    canonica = getattr(afiliacao, "canonica", None) or publicada or ""
    if _netloc(canonica) in _ENCURTADORES:
        return False
    # Carteira e carrinho saem SEMPRE com o nosso link fixo: entrada que
    # publicaria outro link para essas páginas não é servida — `afilia`
    # a refaz (sem API) e sobrescreve.
    fixo = _fixo(canonica)
    return fixo is None or publicada == fixo.publicada


async def afilia(url: str, sessao: aiohttp.ClientSession) -> object:
    """
    Converte uma URL da Shopee na sua forma afiliada.

    Capacidade com efeito colateral controlado: acessa a rede para
    expandir encurtadores e para chamar o serviço externo de
    afiliados, e consulta o cache de links mediado. Não propaga
    exceções: qualquer falha legítima resulta no sentinela AUSENTE.

    Garante que a URL devolvida pertence à Shopee.
    """
    url = _sanitizar_url(url)
    netloc = _netloc(url)
    _perf_entrada(url)

    # Domínio de repasse direto: devolvido sem afiliação.
    if _bate_dominio(netloc, _REPASSE_DIRETO):
        log_nrm.info(f"↩️ SHP repasse direto: {url[:60]}")
        registrar_link(url, url, _IDENTIFICADOR)
        _perf_marca("repasse")
        return url

    # Carteira e carrinho por encurtador CONHECIDO: o nosso link fixo,
    # na hora — sem expandir e sem API. A canônica é a que o cache já
    # tem para essa página (a identidade gravada fica byte a byte); sem
    # ela, a da tabela.
    canonica_conhecida = _curto_conhecido(url)
    if canonica_conhecida:
        gravada = getattr(consultar_link(url), "canonica", None)
        if not gravada or pagina_da_conta(gravada) != pagina_da_conta(canonica_conhecida):
            gravada = canonica_conhecida
        resultado = _fixo(gravada)
        registrar_link(url, resultado, _IDENTIFICADOR)
        _perf_marca("fixo")
        return resultado

    # Consulta ao cache mediado. Entrada cuja canônica ainda é encurtador
    # (gravada antes da guarda de expansão abaixo) não é servida — e é
    # DESCARTADA: a leitura renova o ts e a entrada ruim nunca expiraria.
    cache = consultar_link(url)
    if cache:
        if afiliacao_vigente(cache):
            _perf_marca("cache")
            return cache
        descartar_link(url)

    # Expansão de encurtador próprio, quando aplicável.
    url_expandida = url
    if netloc in _ENCURTADORES:
        try:
            _t = _perf_agora()
            async with config._SEM_HTTP:
                url_expandida = await desencurtar(url, sessao)
            _perf_marca("expansao", _t)
        except Exception as e:
            log_nrm.warning(f"⚠️ SHP expansão falhou: {e}")
            return AUSENTE
        # desencurtar devolve a PRÓPRIA entrada em timeout/falha de rede.
        # Continuar daria à API a URL curta e gravaria no cache uma
        # canônica curta — identidade única por link, nunca o produto.
        # Mesmo contrato da Amazon: expansão que não saiu do encurtador
        # é ausência, sem API e sem cache.
        if _netloc(url_expandida) in _ENCURTADORES:
            log_nrm.warning(
                f"⚠️ SHP expansão não resolveu o encurtador — ausente: {url[:60]}")
            return AUSENTE

    url_limpa = limpa_url(url_expandida)
    canonica = url_limpa
    if (_netloc(url_limpa) or "").lower() == "live.shopee.com.br":
        canonica = _canonica_live(url_limpa)

    # Destino é a carteira ou o carrinho: o nosso link fixo, sem API.
    fixo = _fixo(canonica)
    if fixo:
        registrar_link(url, fixo, _IDENTIFICADOR)
        _perf_marca("fixo_destino")
        return fixo

    # Cache por DESTINO: outro link (de outra fonte) já levou a esta
    # mesma URL limpa — a API devolve o mesmo link para a mesma URL de
    # origem, então o que ela deu da outra vez é reusado sem chamá-la.
    # Só entrada feita pela API (link curto da Shopee) é reusada.
    anterior = consultar_link(url_limpa)
    if (isinstance(anterior, Afiliacao)
            and _netloc(anterior.publicada) in _ENCURTADORES
            and afiliacao_vigente(anterior)):
        resultado = Afiliacao(publicada=anterior.publicada, canonica=canonica)
        registrar_link(url, resultado, _IDENTIFICADOR)
        _perf_marca("destino")
        return resultado

    # Chamada ao serviço de afiliados, sobre a URL limpa.
    _t = _perf_agora()
    link = await _chamar_servico_afiliados(url_limpa, sessao)
    _perf_marca("api", _t)
    pela_limpa = bool(link)

    # Mecanismo de recuperação: tenta a URL canônica de produto.
    if not link:
        url_canonica = _url_produto_canonica(url_expandida)
        if url_canonica and url_canonica != url_limpa:
            log_nrm.info(f"🔄 SHP recuperação: {url_canonica[:60]}")
            link = await _chamar_servico_afiliados(url_canonica, sessao)

    # Falha legítima: ausência explícita.
    if not link:
        log_nrm.warning(f"⚠️ SHP sem afiliação: {url[:60]}")
        return AUSENTE

    # Validação da URL afiliada resultante.
    if "shopee" not in _netloc(link):
        log_nrm.warning(f"⚠️ SHP validação falhou: {link}")
        return AUSENTE

    resultado = Afiliacao(publicada=link, canonica=canonica)
    registrar_link(url, resultado, _IDENTIFICADOR)
    # Grava também pelo destino — só o que a API deu para a PRÓPRIA URL
    # limpa (a recuperação usou outra URL de origem).
    if (pela_limpa and _netloc(link) in _ENCURTADORES
            and _cache_key(url_limpa) != _cache_key(url)):
        registrar_link(url_limpa, resultado, _IDENTIFICADOR)
    return resultado

