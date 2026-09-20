"""
Sonda da PONTE — rodada 2: REGRA DE SELEÇÃO.

A rodada 1 já fechou a pergunta estrutural:

    (A) a ponte está no HTML SERVIDO, como <a href> puro.
        JSON_EMBUTIDO = 0 blocos. JS_INLINE = 0 ocorrências de /p/MLB.
        Sem credencial.

Sobrou UMA pergunta, e é a que impede implementar:

    a vitrine traz 7 a 12 produtos. QUAL deles é o da oferta?

═══════════════════════════════════════════════════════════════════
POR QUE ESTA RODADA DECIDE
═══════════════════════════════════════════════════════════════════
Desta vez o operador mandou os links JUNTO COM o que eles são:

    meli.la/1FE6ohY  → Monitor AOC 23,8" 100Hz 1ms Gaming HDMI  R$391
    meli.la/2tA9txv  → Teclado Mecânico F75 Sem Fio 75% RGB     R$166

Ou seja: a resposta certa é conhecida ANTES do teste. Não se trata
mais de observar estrutura e supor — dá para conferir se a regra
candidata acerta o produto anunciado. É teste, não observação.

A regra candidata, vinda de três indícios convergentes da rodada 1:

    "a PRIMEIRA âncora /p/MLB do HTML_DOC é o produto da oferta"

Indícios: o alvo era a 1ª ocorrência; vinha com selo MAIS VENDIDO; e
o fragmento da URL capturada trazia reco_item_pos=0 e
c_id=/home/card-featured/element.

Três indícios convergentes de UMA amostra ainda são uma amostra. Por
isso esta rodada extrai as âncoras EM ORDEM, com o slug — o slug
carrega o nome do produto, então o acerto ou o erro fica visível.

═══════════════════════════════════════════════════════════════════
LISTA
═══════════════════════════════════════════════════════════════════
Dois alvos são lista. A rodada 1 não respondeu esse caso: na vitrine
do promotom, `_Container_` e `coupon_campaign_id` deram zero. Aqui
eles têm chance real de aparecer.

═══════════════════════════════════════════════════════════════════
SEGREDO
═══════════════════════════════════════════════════════════════════
ANÔNIMA. Nenhum cookie, CSRF ou credencial é lido, enviado ou
impresso. De Set-Cookie registra apenas os NOMES.
"""
from __future__ import annotations

import asyncio
import html as _html
import os
import re
import sys
import urllib.parse
from typing import Optional

import aiohttp


# ── Alvos: "url :: o que o grupo anunciou" ────────────────────────
_PADRAO = (
    "https://meli.la/1hGSoc3 :: LISTA (informado pelo operador)|"
    "https://meli.la/13iNqPB :: LISTA (informado pelo operador)|"
    "https://meli.la/1FE6ohY :: PRODUTO Monitor AOC 23,8 pol 100Hz Gaming R$391|"
    "https://meli.la/2tA9txv :: PRODUTO Teclado Mecanico F75 Sem Fio 75% RGB R$166"
)

ALVOS = []
for item in (os.environ.get("ML_SOCIAL_URLS") or _PADRAO).split("|"):
    item = item.strip()
    if not item:
        continue
    url, _, esperado = item.partition("::")
    ALVOS.append((url.strip(), esperado.strip() or "(não informado)"))

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

_RE_SCRIPT = re.compile(r"<script([^>]*)>(.*?)</script>", re.I | re.S)
_RE_SRC = re.compile(r'src\s*=\s*["\']([^"\']+)["\']', re.I)

# Âncora de produto: captura href inteiro para preservar a ordem.
_RE_ANCORA = re.compile(
    r'<a\b[^>]*?href\s*=\s*["\']([^"\']*?/(?:p/MLB|up/MLBU)[^"\']*)["\']',
    re.I)
_RE_ANCORA_LISTA = re.compile(
    r'<a\b[^>]*?href\s*=\s*["\']([^"\']*?(?:_Container_|coupon_campaign_id)[^"\']*)["\']',
    re.I)

_RE_ID = re.compile(r"/(?:p/(MLB\d{5,})|up/(MLBU\d{5,}))", re.I)
_RE_SLUG = re.compile(r"mercadolivre\.com\.br/([^/?#]+)/(?:p|up)/", re.I)
_RE_CONTAINER = re.compile(r"_Container_[A-Za-z0-9_\-]+", re.I)
_RE_CAMPANHA = re.compile(r"coupon_campaign_id[\"'=:%\s]+(\d+)", re.I)
_RE_TEXTO = re.compile(r">([^<>]{3,60})<")


def bloco(t: str) -> None:
    print("\n" + "═" * 72)
    print(t)
    print("═" * 72)


def sub(t: str) -> None:
    print(f"\n── {t} " + "─" * max(0, 68 - len(t)))


def fatiar(documento: str) -> dict:
    """HTML servido separado do que é <script>."""
    js_inline, externos = [], []

    def _coletar(m):
        src = _RE_SRC.search(m.group(1))
        if src:
            externos.append(src.group(1))
        else:
            js_inline.append(m.group(2))
        return " "

    return {
        "HTML_DOC": _RE_SCRIPT.sub(_coletar, documento),
        "JS_INLINE": "\n".join(js_inline),
        "externos": externos,
    }


def rotulo_antes(html_doc: str, pos: int) -> str:
    """
    Texto visível logo antes da âncora — é onde ficam selos como
    MAIS VENDIDO, OFERTA DO DIA, PATROCINADO.
    """
    janela = html_doc[max(0, pos - 320):pos]
    achados = _RE_TEXTO.findall(janela)
    limpos = [_html.unescape(a).strip() for a in achados]
    limpos = [a for a in limpos if a and not a.startswith(("http", "{", "."))]
    return " | ".join(limpos[-3:]) if limpos else ""


def ancoras_em_ordem(html_doc: str) -> list:
    """
    Âncoras de produto NA ORDEM DO DOCUMENTO.

    A ordem é o dado central desta rodada: a regra candidata diz que
    a primeira é a certa. Sem ordem não há como testar isso.
    """
    saida, vistos = [], set()
    for m in _RE_ANCORA.finditer(html_doc):
        href = _html.unescape(m.group(1))
        achado = _RE_ID.search(href)
        if not achado:
            continue
        ident = achado.group(1) or achado.group(2)
        if ident in vistos:
            continue
        vistos.add(ident)
        slug = _RE_SLUG.search(href)
        saida.append({
            "pos": len(saida) + 1,
            "id": ident,
            "slug": (slug.group(1) if slug else "")[:78],
            "rotulo": rotulo_antes(html_doc, m.start())[:70],
            "href": href[:160],
        })
    return saida


async def saltos(s: aiohttp.ClientSession, url: str) -> str:
    """Cadeia de redirecionamento. Devolve a URL final."""
    sub("expansão salto a salto")
    atual = url
    for n in range(1, 10):
        try:
            async with s.get(atual, headers=CAB, allow_redirects=False,
                             timeout=aiohttp.ClientTimeout(total=30)) as r:
                destino = r.headers.get("Location")
                print(f"    {n}. HTTP {r.status}  {atual[:120]}")
                if not destino:
                    print("       → FINAL")
                    return atual
                print(f"       → {destino[:160]}")
                atual = urllib.parse.urljoin(atual, destino)
        except Exception as exc:
            print(f"    {n}. FALHOU {type(exc).__name__}")
            return atual
    return atual


async def sondar(s: aiohttp.ClientSession, url: str, esperado: str) -> None:
    bloco(f"ALVO  {url}\nESPERADO: {esperado}")

    final = await saltos(s, url)

    sub("GET do documento final")
    try:
        async with s.get(final, headers=CAB, allow_redirects=True,
                         timeout=aiohttp.ClientTimeout(total=40)) as r:
            corpo = await r.text(errors="ignore")
            nomes = [c.split("=", 1)[0].strip()
                     for c in r.headers.getall("Set-Cookie", [])]
            status, url_final = r.status, str(r.url)
    except Exception as exc:
        print(f"    FALHOU {type(exc).__name__}: {exc}")
        return

    print(f"    HTTP {status} | {len(corpo)} bytes")
    print(f"    url final : {url_final[:160]}")
    print(f"    Set-Cookie: {nomes or 'nenhum'}")

    zonas = fatiar(corpo)
    doc = zonas["HTML_DOC"]
    print(f"    HTML_DOC={len(doc)}B  JS_INLINE={len(zonas['JS_INLINE'])}B")

    # ── A pergunta desta rodada ───────────────────────────────────
    sub("ÂNCORAS DE PRODUTO, NA ORDEM DO DOCUMENTO")
    ancoras = ancoras_em_ordem(doc)
    if not ancoras:
        print("    NENHUMA âncora de produto no HTML servido.")
    for a in ancoras[:15]:
        marca = ">>>" if a["pos"] == 1 else "   "
        print(f"  {marca} #{a['pos']:<2} {a['id']:<16} {a['slug']}")
        if a["rotulo"]:
            print(f"        rótulo antes: {a['rotulo']}")
    if len(ancoras) > 15:
        print(f"    … mais {len(ancoras) - 15}")

    if ancoras:
        p = ancoras[0]
        print(f"\n    REGRA CANDIDATA (primeira âncora) escolheria:")
        print(f"      id   : {p['id']}")
        print(f"      slug : {p['slug']}")
        print(f"      ESPERADO: {esperado}")
        print(f"      ^^ conferir se o slug bate com o esperado ^^")

    # ── Lista ─────────────────────────────────────────────────────
    sub("sinais de LISTA no HTML servido")
    expandido = (doc.replace("\\u002F", "/").replace("\\/", "/"))
    try:
        expandido += " " + urllib.parse.unquote(doc[:400000])
    except Exception:
        pass
    expandido += " " + _html.unescape(doc[:400000])

    conts = sorted(set(_RE_CONTAINER.findall(expandido)))
    camps = sorted(set(_RE_CAMPANHA.findall(expandido)))
    print(f"    _Container_        : {len(conts)} {conts[:10]}")
    print(f"    coupon_campaign_id : {len(camps)} {camps[:10]}")

    ancoras_lista = []
    for m in _RE_ANCORA_LISTA.finditer(doc):
        h = _html.unescape(m.group(1))
        if h not in ancoras_lista:
            ancoras_lista.append(h)
    print(f"    âncoras de lista   : {len(ancoras_lista)}")
    for h in ancoras_lista[:10]:
        print(f"      {h[:170]}")

    # Também no JS inline, para separar servido de montado.
    c_js = len(set(_RE_CONTAINER.findall(zonas["JS_INLINE"])))
    print(f"    (_Container_ em JS_INLINE: {c_js})")

    sub("bundles próprios referenciados")
    proprios = [u for u in zonas["externos"]
                if "affiliates" in u or "recommendations" in u
                or "social" in u]
    for u in proprios[:10]:
        print(f"    {u[:150]}")


async def main() -> int:
    bloco("SONDA DA PONTE — RODADA 2: REGRA DE SELEÇÃO")
    print(f"  alvos      : {len(ALVOS)}")
    print(f"  credencial : NENHUMA (anônima, por desenho)")
    print(f"  pergunta   : a PRIMEIRA âncora /p/MLB é o produto da oferta?")

    async with aiohttp.ClientSession() as s:
        for url, esperado in ALVOS:
            try:
                await sondar(s, url, esperado)
            except Exception as exc:
                print(f"\n  ALVO FALHOU: {type(exc).__name__}: {exc}")

    bloco("FIM")
    print("  Conferir, alvo a alvo, se a âncora #1 bate com o ESPERADO.")
    print("  Se bater nos dois produtos, a regra está comprovada.")
    print("  Se não bater, a regra está errada e NÃO deve ser")
    print("  implementada — o log mostra qual posição acertaria.")
    print()
    print("  NADA foi implementado. NADA foi alterado em produção.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
