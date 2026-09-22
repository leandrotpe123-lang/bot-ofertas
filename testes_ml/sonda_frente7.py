"""
SONDA DA FRENTE 7 — os caminhos que ficaram fora.

Mapeia, salto a salto, o que acontece com:

  1. meli.la/1E3uX78          (não converteu em produção)
  2. meli.la/2pbjuX4          (não converteu em produção)
  3. um /sec/ REAL            (mercadolivre.com/sec/2U6U32Q)
  4. o /social/<slug>/lists/<uuid> que aparecer no caminho

Para cada alvo, o estágio exato:

  EXPANSÃO → CLASSIFICAÇÃO → DESCOBERTA → PRODUTO/LISTA
           → CREATE LINK → RESULTADO

Não basta "falhou": o relatório diz em QUAL degrau parou e por quê.

═══════════════════════════════════════════════════════════════════
REDIRECTS SÃO SEGUIDOS À MÃO
═══════════════════════════════════════════════════════════════════
`allow_redirects=False` em cada salto, de propósito. Deixar a
biblioteca seguir sozinha entrega só a URL final e esconde o
caminho — e é justamente o caminho que esta sonda existe para
descobrir. Cada salto registra status e Location.

═══════════════════════════════════════════════════════════════════
A LISTA DE AFILIADO GANHA AUTÓPSIA PRÓPRIA
═══════════════════════════════════════════════════════════════════
`/social/<slug>/lists/<uuid>` nunca foi medida. A sonda não assume
que é igual à vitrine nem que é impossível: abre o documento e
relata a estrutura encontrada — se tem `recommendation_info`, se
tem `seeMoreLink`, quantos `polycards`. É o que separa as hipóteses
A/B/C/D.

═══════════════════════════════════════════════════════════════════
SEGREDO
═══════════════════════════════════════════════════════════════════
O createLink usa a credencial real, mas nenhum valor de cookie,
token ou header é impresso. Do `ref` sai só presença e tamanho — é
identificador de curadoria, não segredo, mas não custa.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
from urllib.parse import urljoin, urlparse, parse_qs

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import aiohttp

from plataformas.mercadolivre import (
    afiliado, cliente, descoberta, links, sessao,
)

_ALVOS = [
    ("1E3uX78", "https://meli.la/1E3uX78"),
    ("2pbjuX4", "https://meli.la/2pbjuX4"),
    ("sec real", "https://mercadolivre.com/sec/2U6U32Q"),
]

_UA = {
    "User-Agent": cliente._USER_AGENT,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "*/*;q=0.8"),
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}

_REDIR = (301, 302, 303, 307, 308)
_PAUSA = 1.5
LISTAS_AFILIADO = []


def secao(t):
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}", flush=True)


def sub(t):
    print(f"\n  ── {t} " + "─" * max(0, 52 - len(t)), flush=True)


def linha(r, v):
    print(f"     {r:<32} {v}", flush=True)


# ══════════════════════════════════════════════════════════════════
# ETAPA 1 — EXPANSÃO, salto a salto
# ══════════════════════════════════════════════════════════════════
async def cadeia(url: str, s: aiohttp.ClientSession):
    """Segue os redirects à mão, registrando cada salto."""
    sub("EXPANSÃO — cadeia de redirects")
    atual, saltos = url, []
    for i in range(10):
        try:
            async with s.get(atual, headers=_UA, allow_redirects=False,
                             timeout=aiohttp.ClientTimeout(total=25)) as r:
                destino = r.headers.get("Location", "")
                print(f"     [{i}] {r.status}  {_curta(atual)}", flush=True)
                if destino:
                    print(f"          → Location: {_curta(destino)}",
                          flush=True)
                saltos.append((r.status, atual, destino))
                if r.status in _REDIR and destino:
                    atual = urljoin(atual, destino)
                    continue
                corpo = await r.text(errors="ignore")
                return saltos, atual, r.status, corpo
        except Exception as e:
            linha("EXCEÇÃO", f"{type(e).__name__}")
            return saltos, atual, "EXC", ""
    linha("ALERTA", "10 saltos sem parar")
    return saltos, atual, "LOOP", ""


def _curta(u, n=96):
    return u if len(u) <= n else u[:n] + "…"


# ══════════════════════════════════════════════════════════════════
# ETAPA 2 — CLASSIFICAÇÃO pelo links.py de produção
# ══════════════════════════════════════════════════════════════════
def classificar(final: str):
    sub("CLASSIFICAÇÃO — pelo links.py de produção")
    p = urlparse(final)
    q = parse_qs(p.query)
    cen = links.cenario_de(final)
    linha("host", p.netloc)
    linha("path", _curta(p.path, 70))
    linha("parâmetros na query", sorted(q.keys()) or "(nenhum)")
    ref = (q.get("ref") or [""])[0]
    linha("ref presente", f"sim (len={len(ref)})" if ref else "NÃO")
    linha("reconhece()", links.reconhece(final))
    linha("cenario_de()", cen or "(desconhecido)")
    linha("eh_elegivel()", links.eh_elegivel(final))
    linha("precisa_expandir()", links.precisa_expandir(final))
    linha("precisa_descobrir()", links.precisa_descobrir(final))
    return cen


# ══════════════════════════════════════════════════════════════════
# ETAPA 3 — DESCOBERTA
# ══════════════════════════════════════════════════════════════════
def autopsiar(corpo: str, rotulo: str):
    """
    Abre a estrutura do documento sem decidir nada.

    Existe para a lista de afiliado, que nunca foi medida: em vez de
    concluir "não dá", mostra o que o documento TEM.
    """
    sub(f"AUTÓPSIA DA ESTRUTURA — {rotulo}")
    linha("tamanho do documento", len(corpo))
    if not corpo:
        return
    for marca in ('"recommendation_info"', '"seeMoreLink"', '"polycards"',
                  '_Container_', '"appProps"', 'coupon_campaign_id'):
        linha(f"contém {marca}", corpo.count(marca))

    # O mesmo caminho que a produção usa, mas sem julgar o resultado.
    blocos = []
    for m in descoberta._RE_SCRIPT.finditer(corpo):
        if not descoberta._RE_SRC.search(m.group(1)):
            blocos.append(m.group(2))
    achou = None
    for b in blocos:
        if not any(x in b for x in descoberta._MARCADORES):
            continue
        estado = descoberta._estado_do_script(b)
        if estado is None:
            continue
        no = descoberta._descer(estado, descoberta._CAMINHO)
        if isinstance(no, dict):
            achou = no
            break
    if achou is None:
        linha("components[1].recommendation_info", "NÃO ENCONTRADO")
        return
    linha("components[1].recommendation_info", "encontrado")
    linha("  chaves", sorted(achou.keys())[:12])
    ver = achou.get("seeMoreLink")
    linha("  seeMoreLink", f"preenchido ({len(ver)} chars)"
          if isinstance(ver, str) and ver.strip() else "vazio/ausente")
    cartoes = achou.get("polycards")
    linha("  polycards", len(cartoes) if isinstance(cartoes, list)
          else "ausente")


async def descobrir(final: str, corpo: str, s):
    sub("DESCOBERTA")
    if not links.precisa_descobrir(final):
        linha("entra na descoberta?", "NÃO — não é vitrine")
        return None
    destino = descoberta.interpretar(corpo)
    if destino is None:
        linha("resultado", "None — estrutura não reconhecida")
        autopsiar(corpo, "vitrine que não rendeu destino")
        return None
    linha("tipo", destino.tipo)
    linha("url do destino", _curta(destino.url, 88))
    linha("cenário do destino", links.cenario_de(destino.url))
    linha("destino é elegível?", links.eh_elegivel(destino.url))
    return destino


# ══════════════════════════════════════════════════════════════════
# ETAPA 4 — CREATE LINK
# ══════════════════════════════════════════════════════════════════
async def criar(alvo: str, s):
    sub("CREATE LINK")
    if not links.eh_elegivel(alvo):
        linha("chamou createLink?", "NÃO — alvo inelegível")
        linha("PONTO DE PARADA", f"elegibilidade | cenario="
              f"{links.cenario_de(alvo) or '?'}")
        return
    limpo = links.sanitizar(alvo)
    corpo = json.dumps({"urls": [limpo], "tag": afiliado.TAG})
    cab = {
        "User-Agent": cliente._USER_AGENT,
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": cliente._ORIGIN,
        "Referer": cliente._REFERER,
        "Cookie": sessao._limpar(
            sessao._decodificar(os.environ.get("ML_SESSION_COOKIE") or "")),
        "x-csrf-token": sessao._limpar(
            os.environ.get("ML_CSRF_TOKEN") or ""),
    }
    t0 = time.monotonic()
    try:
        async with s.post(cliente.ENDPOINT, data=corpo.encode(),
                          headers=cab, allow_redirects=False,
                          timeout=aiohttp.ClientTimeout(total=25)) as r:
            texto = await r.text(errors="ignore")
            linha("status HTTP", r.status)
            linha("duração (s)", round(time.monotonic() - t0, 2))
            try:
                d = json.loads(texto)
            except Exception:
                linha("corpo", f"não-JSON, {len(texto)} chars")
                return
            linha("total_success", d.get("total_success"))
            linha("total_error", d.get("total_error"))
            urls = d.get("urls")
            if isinstance(urls, list) and urls and isinstance(urls[0], dict):
                it = urls[0]
                linha("created", it.get("created"))
                linha("error_code", it.get("error_code"))
                linha("message", it.get("message"))
                curto = it.get("short_url")
                linha("short_url", curto or "(ausente)")
                linha("tag confirmada", it.get("tag") or "(ausente)")
                if curto:
                    linha("RESULTADO", f"CONVERTEU → {curto}")
                else:
                    linha("PONTO DE PARADA", "createLink recusou o item")
            else:
                linha("message", d.get("message"))
                linha("PONTO DE PARADA", f"HTTP {r.status}")
    except Exception as e:
        linha("EXCEÇÃO", type(e).__name__)


# ══════════════════════════════════════════════════════════════════
async def rodar(rotulo, url, s):
    secao(f"ALVO: {rotulo} — {url}")
    saltos, final, status, corpo = await cadeia(url, s)
    sub("URL FINAL")
    linha("final", _curta(final, 96))
    linha("status final", status)
    linha("saltos", len(saltos))

    cen = classificar(final)

    # Guarda listas de afiliado para autópsia dedicada.
    if cen == links.CENARIO_LISTA_AFILIADO:
        LISTAS_AFILIADO.append((final, corpo))
        autopsiar(corpo, "lista de afiliado")

    destino = await descobrir(final, corpo, s)
    alvo = destino.url if destino else final
    await criar(alvo, s)


async def principal():
    print("\n" + "#" * 70, flush=True)
    print("#  SONDA DA FRENTE 7 — os caminhos que ficaram fora", flush=True)
    print("#" * 70, flush=True)
    linha("ML_TAG", "presente" if afiliado.TAG else "AUSENTE")

    s = aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar())
    try:
        for rotulo, url in _ALVOS:
            await rodar(rotulo, url, s)
            await asyncio.sleep(_PAUSA)

        if LISTAS_AFILIADO:
            secao("LISTA DE AFILIADO — o que dá pra fazer com ela")
            for final, corpo in LISTAS_AFILIADO:
                linha("url", _curta(final, 88))
                # Procura, no documento, QUALQUER _Container_ — sem
                # usar isso como regra, só para saber se existe.
                achados = re.findall(
                    r'https?:\\?/\\?/lista\.mercadolivre\.com\.br'
                    r'\\?/_Container_[^"\'\\ ]{0,120}', corpo)
                linha("_Container_ no documento", len(achados))
                if achados:
                    limpo = descoberta._com_esquema(achados[0])
                    linha("  primeiro (amostra)", _curta(limpo, 88))
                    linha("  cenário dele", links.cenario_de(limpo))
                    linha("  elegível?", links.eh_elegivel(limpo))
        else:
            secao("LISTA DE AFILIADO")
            linha("apareceu nesta rodada?", "NÃO")
    finally:
        await s.close()

    print("\n=== FIM FRENTE 7 ===", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(principal()))
