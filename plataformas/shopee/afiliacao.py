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
from urllib.parse import urlparse

import aiohttp

import config
from logger import log_nrm
from plataformas.contrato import AUSENTE, Afiliacao
from utils.cache_links import consultar_link, registrar_link
from utils.url_resolver import desencurtar
from utils.urls import _netloc, _sanitizar_url

from .links import (
    _DOMINIOS,
    _ENCURTADORES,
    _IDENTIFICADOR,
    _bate_dominio,
    _canonica_live,
    _url_produto_canonica,
    limpa_url,
)


_SHP_APP_ID = os.environ.get("SHOPEE_APP_ID", "")
_SHP_SECRET = os.environ.get("SHOPEE_SECRET", "")


# ── Endpoint do serviço de afiliados ──────────────────────────────
_ENDPOINT_AFILIADOS = "https://open-api.affiliate.shopee.com.br/graphql"
_TENTATIVAS_AFILIACAO = 3
_TIMEOUT_AFILIACAO = 12
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
    if n and n % _PERF_RESUMO == 0:
        medias = " ".join(
            f"{k}={_perf_t[k] / max(_perf_c.get(k, 1), 1) * 1000:.0f}ms"
            for k in sorted(_perf_t)
        )
        contas = " ".join(f"{k}={v}" for k, v in sorted(_perf_c.items()))
        log_nrm.debug(f"📊 SHP perf | {contas} | media {medias}")

# ── Política de repasse direto (sem afiliação) ────────────────────
# Domínio cujas URLs são publicadas como recebidas, sem passar pelo
# serviço de afiliados. É política de afiliação, não classificação.
_REPASSE_DIRETO = frozenset({"flapremios.com.br"})

# ── Tabela de configuração: link fixo por path ────────────────────
# Substituição de URL DECLARADA, não inferida. Cada entrada mapeia um
# PATH normalizado (minúsculo, sem barra final) para um link afiliado
# que o dono do bot já possui.
#
# Para cobrir uma página nova, ACRESCENTE UMA LINHA aqui. A lógica do
# gate não conhece nenhum path e não muda.
#
# É política de afiliação, não classificação: mesma natureza de
# _REPASSE_DIRETO, e sem efeito sobre a taxonomia de links — links.py
# permanece a única fonte do conhecimento de URL da Shopee.
_LINKS_FIXOS_POR_PATH = {
    "/user/voucher-wallet": "https://s.shopee.com.br/8pkZllbmly",
    "/cart":               "https://s.shopee.com.br/1qapQu2VFM",
    "/m/cupom-de-desconto": "https://s.shopee.com.br/6AjowpqGMh",
    "/shopeevip":        "https://s.shopee.com.br/6AjqXCz9ys",
    "/m/espaco-tecnologia": "https://s.shopee.com.br/2VqZm3mz6m",
}


def _link_fixo(url: str) -> str:
    """
    Consulta _LINKS_FIXOS_POR_PATH para uma URL FINAL e devolve o link
    configurado, ou cadeia vazia quando o path não está na tabela.

    CRITÉRIO ÚNICO — dois testes, nada além deles:
      1. host de VITRINE da Shopee (em _DOMINIOS, fora de
         _ENCURTADORES; subdomínio conta);
      2. path normalizado IGUAL a uma chave da tabela.

    NÃO assume nada sobre a página remota: nem que seja pública, nem
    permanente, nem que o serviço de afiliados a aceite ou recuse. Não
    lê query, fragmento, texto do post nem estado. Pura e
    determinística.

    Host encurtador está fora por definição: nele o path é código
    opaco, não rota — é o que impede que um link curto devolvido sem
    expansão (desencurtar devolve a URL recebida em falha de rede)
    acione uma entrada da tabela.

    Path fora da tabela devolve cadeia vazia e o fluxo de afiliação
    segue intacto.
    """
    netloc = _netloc(url)
    if netloc in _ENCURTADORES or not _bate_dominio(netloc, _DOMINIOS):
        return ""
    caminho = (urlparse(url).path or "").rstrip("/").lower()
    return _LINKS_FIXOS_POR_PATH.get(caminho, "")

async def _chamar_servico_afiliados(
    url_produto: str, sessao: aiohttp.ClientSession,
) -> Optional[str]:
    """
    Chama o serviço externo de geração de link afiliado.

    Aplica autenticação por assinatura criptográfica e política de
    novas tentativas com intervalo progressivo. Devolve o link
    afiliado, ou None quando o serviço não o produz.
    """
    for tentativa in range(1, _TENTATIVAS_AFILIACAO + 1):
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
                    timeout=aiohttp.ClientTimeout(
                        total=_TIMEOUT_AFILIACAO,
                    ),
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
            await asyncio.sleep(tentativa * 1.5)
    return None


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
        _perf_marca("repasse")
        return url

    # Consulta ao cache mediado.
    cache = consultar_link(url)
    if cache:
        _perf_marca("cache")
        return cache

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

    # Tabela de link fixo por path: publica o link configurado, sem
    # chamar o serviço de afiliados. Decidido sobre a URL FINAL.
    fixo = _link_fixo(url_expandida)
    if fixo:
        _perf_marca("fixo")
        return fixo

    # Chamada ao serviço de afiliados, sobre a URL limpa.
    url_limpa = limpa_url(url_expandida)
    _t = _perf_agora()
    link = await _chamar_servico_afiliados(url_limpa, sessao)
    _perf_marca("api", _t)

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

    canonica = url_limpa
    if (_netloc(url_limpa) or "").lower() == "live.shopee.com.br":
        canonica = _canonica_live(url_limpa)
    resultado = Afiliacao(publicada=link, canonica=canonica)
    registrar_link(url, resultado, _IDENTIFICADOR)
    return resultado

