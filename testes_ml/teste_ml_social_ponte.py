"""
Sonda da PONTE — /social/<afiliado> → produto ou lista real.

FRENTE ÚNICA. Não toca createLink, não toca /p/api/deferred, não
entra em página de produto, não usa credencial nenhuma.

═══════════════════════════════════════════════════════════════════
A PERGUNTA, E SÓ ELA
═══════════════════════════════════════════════════════════════════
    meli.la
      → desencurtador
      → /social/<afiliado>
      → ???                  ← ESTA sonda
      → /p/MLB… ou _Container_…
      → createLink

Veredito exigido, um dos dois:

    (A) a ponte está no HTML/JSON servido do /social
    (B) a ponte nasce depois, via JS/API — e qual recurso

═══════════════════════════════════════════════════════════════════
PISTA QUE ORIENTA A SONDA
═══════════════════════════════════════════════════════════════════
O fragmento da URL de produto capturada pelo operador ao sair do
/social/promotom:

    #polycard_client=recommendations_home_affiliate-profile
    &reco_backend=item_decorator
    &reco_client=home_affiliate-profile
    &source=affiliate-profile
    &c_id=/home/card-featured/element
    &reco_item_pos=0

Isso diz que o card do produto foi montado pelo sistema de
RECOMENDAÇÕES, com cliente `home_affiliate-profile`. É indício
forte de (B), mas indício NÃO É PROVA — por isso a sonda mede os
dois casos e deixa a resposta sair dos dados.

═══════════════════════════════════════════════════════════════════
ZONAS
═══════════════════════════════════════════════════════════════════
Cada ocorrência é classificada pela zona onde caiu, porque é a zona
que decide se existe rota HTTP pura:

  HTML_DOC      marcação servida, fora de <script>       → (A)
  JSON_EMBUTIDO <script type="application/json"> e
                window.__STATE__ dentro do documento     → (A)
  JS_INLINE     <script> com código dentro do documento  → (A), com ressalva
  JS_EXTERNO    arquivo .js baixado à parte              → (B)

═══════════════════════════════════════════════════════════════════
SEGREDO
═══════════════════════════════════════════════════════════════════
Roda ANÔNIMA. Nenhum cookie, nenhum CSRF, nenhuma credencial é
lida, enviada ou impressa. Se a página exigir sessão para revelar a
ponte, isso por si só é um achado e aparece no relatório.
"""
from __future__ import annotations

import asyncio
import html
import json
import os
import re
import sys
import urllib.parse
from typing import Optional

import aiohttp


# ── Alvos ─────────────────────────────────────────────────────────
_PADRAO = ("https://www.mercadolivre.com.br/social/promotom"
           "?matt_word=promotom&matt_tool=38634560")

ALVOS = [u.strip() for u in (os.environ.get("ML_SOCIAL_URLS") or "").split("|")
         if u.strip()] or [_PADRAO]

AGULHAS = ("/p/MLB", "permalink", "_Container_",
           "coupon_campaign_id", "item_id", "MLB")

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/148.0.0.0 Safari/537.36")

CAB = {
    "User-Agent": UA,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}

_RE_SCRIPT = re.compile(
    r"<script([^>]*)>(.*?)</script>", re.I | re.S)
_RE_TIPO = re.compile(r'type\s*=\s*["\']([^"\']+)["\']', re.I)
_RE_SRC = re.compile(r'src\s*=\s*["\']([^"\']+)["\']', re.I)
_RE_STATE = re.compile(
    r"(?:window\.__PRELOADED_STATE__|window\.__NEXT_DATA__|"
    r"window\.__INITIAL_STATE__|window\.__PRELOADED__)\s*=\s*", re.I)

# Formas concretas do que a ponte precisaria devolver.
_RE_PRODUTO = re.compile(r"/p/MLBU?-?\d{5,}", re.I)
_RE_CONTAINER = re.compile(r"_Container_[A-Za-z0-9_\-]+", re.I)
_RE_CAMPANHA = re.compile(r"coupon_campaign_id[\"'=:%\s]+(\d+)", re.I)
_RE_ITEM = re.compile(r"MLBU?\d{6,}", re.I)


def bloco(t: str) -> None:
    print("\n" + "═" * 70)
    print(t)
    print("═" * 70)


def sub(t: str) -> None:
    print(f"\n── {t} " + "─" * max(0, 66 - len(t)))


def _sem_frag(url: str) -> str:
    """O servidor ignora o fragmento; mandar só suja o log."""
    return url.split("#", 1)[0]


# ══════════════════════════════════════════════════════════════════
# FATIAMENTO EM ZONAS
# ══════════════════════════════════════════════════════════════════
def fatiar(documento: str) -> dict:
    """
    Separa o documento nas zonas que decidem o veredito.

    O que sobra depois de remover todo <script> é marcação servida.
    Dentro de <script>, o `type` separa dado de código.
    """
    json_embutido: list = []
    js_inline: list = []
    externos: list = []

    def _coletar(m):
        atributos, corpo = m.group(1), m.group(2)
        src = _RE_SRC.search(atributos)
        if src:
            externos.append(src.group(1))
            return " "
        tipo = (_RE_TIPO.search(atributos).group(1).lower()
                if _RE_TIPO.search(atributos) else "")
        if "json" in tipo:
            json_embutido.append(corpo)
        elif _RE_STATE.search(corpo):
            json_embutido.append(corpo)
        else:
            js_inline.append(corpo)
        return " "

    html_doc = _RE_SCRIPT.sub(_coletar, documento)

    return {
        "HTML_DOC": html_doc,
        "JSON_EMBUTIDO": "\n".join(json_embutido),
        "JS_INLINE": "\n".join(js_inline),
        "_externos": externos,
        "_n_json": len(json_embutido),
        "_n_inline": len(js_inline),
    }


def contexto(texto: str, agulha: str, quantos: int = 3) -> list:
    """Trechos ao redor das primeiras ocorrências."""
    saida, inicio = [], 0
    baixo, alvo = texto.lower(), agulha.lower()
    for _ in range(quantos):
        i = baixo.find(alvo, inicio)
        if i < 0:
            break
        a, b = max(0, i - 90), min(len(texto), i + len(agulha) + 90)
        trecho = texto[a:b].replace("\n", " ").replace("\r", " ")
        trecho = re.sub(r"\s{2,}", " ", trecho)
        saida.append(trecho)
        inicio = i + len(agulha)
    return saida


def medir(zonas: dict, rotulo: str) -> dict:
    """Conta cada agulha por zona e mostra o entorno."""
    print(f"\n  ┌─ {rotulo}")
    print(f"  │  HTML_DOC={len(zonas['HTML_DOC'])}B  "
          f"JSON_EMBUTIDO={len(zonas['JSON_EMBUTIDO'])}B "
          f"({zonas['_n_json']} blocos)  "
          f"JS_INLINE={len(zonas['JS_INLINE'])}B "
          f"({zonas['_n_inline']} blocos)")
    print(f"  │  scripts externos referenciados: {len(zonas['_externos'])}")
    print("  └─")

    resultado = {}
    for agulha in AGULHAS:
        linha = {}
        for zona in ("HTML_DOC", "JSON_EMBUTIDO", "JS_INLINE"):
            linha[zona] = zonas[zona].lower().count(agulha.lower())
        resultado[agulha] = linha
        total = sum(linha.values())
        marca = "🎯" if total else "  "
        print(f"\n  {marca} {agulha:22s} "
              f"HTML_DOC={linha['HTML_DOC']:<4} "
              f"JSON_EMBUTIDO={linha['JSON_EMBUTIDO']:<4} "
              f"JS_INLINE={linha['JS_INLINE']}")
        for zona in ("HTML_DOC", "JSON_EMBUTIDO", "JS_INLINE"):
            if not linha[zona]:
                continue
            for trecho in contexto(zonas[zona], agulha):
                print(f"        [{zona}] …{trecho}…")
    return resultado


def extrair_alvos(zonas: dict) -> dict:
    """O que a ponte precisaria devolver, se estiver ali."""
    achados = {}
    for zona in ("HTML_DOC", "JSON_EMBUTIDO", "JS_INLINE"):
        texto = zonas[zona]
        # Desescapa as formas que o Mercado Livre usa em JSON embutido.
        expandido = (texto
                     .replace("\\u002F", "/").replace("\\u002f", "/")
                     .replace("\\/", "/"))
        try:
            expandido += " " + urllib.parse.unquote(texto[:400000])
        except Exception:
            pass
        expandido += " " + html.unescape(texto[:400000])

        achados[zona] = {
            "produtos": sorted(set(_RE_PRODUTO.findall(expandido)))[:40],
            "containers": sorted(set(_RE_CONTAINER.findall(expandido)))[:20],
            "campanhas": sorted(set(_RE_CAMPANHA.findall(expandido)))[:20],
            "itens": sorted(set(_RE_ITEM.findall(expandido)))[:40],
        }
    return achados


# ══════════════════════════════════════════════════════════════════
# SONDAS
# ══════════════════════════════════════════════════════════════════
async def baixar(s: aiohttp.ClientSession, url: str,
                 accept: Optional[str] = None) -> tuple:
    """GET anônimo. Devolve (status, url_final, corpo, set_cookie_nomes)."""
    cab = dict(CAB)
    if accept:
        cab["Accept"] = accept
    try:
        async with s.get(_sem_frag(url), headers=cab, allow_redirects=True,
                         timeout=aiohttp.ClientTimeout(total=40)) as r:
            corpo = await r.text(errors="ignore")
            nomes = [c.split("=", 1)[0].strip()
                     for c in r.headers.getall("Set-Cookie", [])]
            return r.status, str(r.url), corpo, nomes
    except Exception as exc:
        return 0, url, f"__FALHA__ {type(exc).__name__}: {exc}", []


async def saltos(s: aiohttp.ClientSession, url: str) -> None:
    """Cadeia de redirecionamento, salto a salto."""
    sub("cadeia de redirecionamento")
    atual = url
    for n in range(1, 9):
        try:
            async with s.get(_sem_frag(atual), headers=CAB,
                             allow_redirects=False,
                             timeout=aiohttp.ClientTimeout(total=30)) as r:
                destino = r.headers.get("Location")
                print(f"    {n}. HTTP {r.status}  {atual[:110]}")
                if not destino:
                    print(f"       → FINAL")
                    return
                print(f"       → {destino[:150]}")
                atual = urllib.parse.urljoin(atual, destino)
        except Exception as exc:
            print(f"    {n}. FALHOU {type(exc).__name__}")
            return


async def sondar_pagina(s: aiohttp.ClientSession, url: str) -> dict:
    bloco(f"PÁGINA SOCIAL\n{url[:150]}")
    await saltos(s, url)

    sub("GET do documento")
    status, final, corpo, cookies = await baixar(s, url)
    print(f"    HTTP {status}")
    print(f"    url final : {final[:150]}")
    print(f"    tamanho   : {len(corpo)} bytes")
    print(f"    Set-Cookie: {cookies or 'nenhum'}")

    if corpo.startswith("__FALHA__"):
        print(f"    {corpo[:200]}")
        return {}

    zonas = fatiar(corpo)
    sub("ocorrências por zona")
    contagem = medir(zonas, "documento servido")

    sub("o que a ponte precisaria devolver")
    alvos = extrair_alvos(zonas)
    for zona, a in alvos.items():
        print(f"\n    [{zona}]")
        print(f"      produtos /p/MLB : {len(a['produtos'])} "
              f"{a['produtos'][:12]}")
        print(f"      _Container_     : {len(a['containers'])} "
              f"{a['containers'][:8]}")
        print(f"      coupon_campaign : {len(a['campanhas'])} "
              f"{a['campanhas'][:8]}")
        print(f"      ids MLB soltos  : {len(a['itens'])} "
              f"{a['itens'][:12]}")

    sub("scripts externos referenciados pelo documento")
    proprios = [u for u in zonas["_externos"]
                if "mercadoli" in u or "mlstatic" in u]
    for u in proprios[:25]:
        print(f"    {u[:150]}")
    if len(proprios) > 25:
        print(f"    … mais {len(proprios) - 25}")

    return {"zonas": zonas, "contagem": contagem, "alvos": alvos,
            "externos": proprios, "corpo": corpo}


async def sondar_apis(s: aiohttp.ClientSession, slug: str) -> None:
    """
    Candidatos a recurso que ENTREGA a ponte, para o caso (B).

    Escolhidos a partir do fragmento capturado pelo operador:
    `reco_client=home_affiliate-profile`, `source=affiliate-profile`,
    `c_id=/home/card-featured/element`.
    """
    bloco("CANDIDATOS A RECURSO DA PONTE (caso B)")

    candidatos = [
        f"https://www.mercadolivre.com.br/api/home/recommendations/social/tabs?matt_word={slug}",
        f"https://www.mercadolivre.com.br/api/home/recommendations/social?matt_word={slug}",
        f"https://www.mercadolivre.com.br/recommendations?client=home_affiliate-profile&matt_word={slug}&site_platform=&web_device=desktop",
        f"https://www.mercadolivre.com.br/recommendations?client=home_affiliate-profile&site_platform=&web_device=desktop",
        f"https://www.mercadolivre.com.br/api/home/affiliate-profile?matt_word={slug}",
    ]

    for url in candidatos:
        sub(url[:120])
        status, final, corpo, cookies = await baixar(
            s, url, accept="application/json, text/plain, */*")
        print(f"    HTTP {status} | {len(corpo)} bytes | "
              f"Set-Cookie: {cookies or 'nenhum'}")
        if corpo.startswith("__FALHA__"):
            print(f"    {corpo[:160]}")
            continue

        for agulha in AGULHAS:
            n = corpo.lower().count(agulha.lower())
            if n:
                print(f"    🎯 {agulha}: {n}")
                for t in contexto(corpo, agulha, 2):
                    print(f"        …{t}…")

        produtos = sorted(set(_RE_PRODUTO.findall(
            corpo.replace("\\u002F", "/").replace("\\/", "/"))))
        if produtos:
            print(f"    >>> PRODUTOS: {len(produtos)} {produtos[:12]}")
        conts = sorted(set(_RE_CONTAINER.findall(corpo)))
        if conts:
            print(f"    >>> CONTAINERS: {conts[:8]}")

        if corpo.strip().startswith(("{", "[")):
            try:
                d = json.loads(corpo)
                chaves = list(d)[:30] if isinstance(d, dict) else f"lista[{len(d)}]"
                print(f"    JSON ok | chaves de topo: {chaves}")
            except Exception:
                print("    corpo começa como JSON mas não parseou")


def veredito(resultados: list) -> None:
    bloco("VEREDITO")

    servido = 0
    externo_only = 0
    for r in resultados:
        if not r:
            continue
        for zona in ("HTML_DOC", "JSON_EMBUTIDO", "JS_INLINE"):
            a = r["alvos"][zona]
            servido += len(a["produtos"]) + len(a["containers"])

    if servido:
        print("  (A) A PONTE ESTÁ NO HTML/JSON SERVIDO DO /social")
        print()
        print("  Estrutura encontrada — ver as seções acima, zona por")
        print("  zona. Implementação possível: GET + extração, sem")
        print("  navegador.")
    else:
        print("  (B) A PONTE NÃO ESTÁ NO HTML SERVIDO")
        print()
        print("  Nenhuma URL /p/MLB e nenhum _Container_ apareceu em")
        print("  HTML_DOC, JSON_EMBUTIDO ou JS_INLINE do documento.")
        print("  A informação nasce depois da execução de JavaScript.")
        print()
        print("  Recurso a investigar em seguida: ver a seção")
        print("  'CANDIDATOS A RECURSO DA PONTE' — o que devolveu")
        print("  produto é o caminho.")
    print()
    print("  NADA foi implementado. NADA foi alterado em produção.")


async def main() -> int:
    bloco("SONDA DA PONTE /social → PRODUTO|LISTA")
    print(f"  alvos      : {len(ALVOS)}")
    print(f"  credencial : NENHUMA (sonda anônima, por desenho)")
    print(f"  agulhas    : {list(AGULHAS)}")

    resultados = []
    async with aiohttp.ClientSession() as s:
        for url in ALVOS:
            resultados.append(await sondar_pagina(s, url))

        achado = re.search(r"/social/([A-Za-z0-9_.\-]+)", ALVOS[0])
        await sondar_apis(s, achado.group(1) if achado else "promotom")

    veredito(resultados)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
