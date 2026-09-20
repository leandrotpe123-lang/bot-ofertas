"""
Sonda da PONTE — rodada 3: DE ONDE SAI O `_Container_`.

═══════════════════════════════════════════════════════════════════
O ERRO QUE ESTA RODADA CORRIGE
═══════════════════════════════════════════════════════════════════
A rodada 2 IMPRIMIU a pista e eu não puxei:

    alvos de LISTA    → (_Container_ em JS_INLINE: 1)
    alvos de PRODUTO  → (_Container_ em JS_INLINE: 0)

Diferença sistemática, 2 a 2. O `_Container_` ESTÁ na página de
lista — dentro de <script> inline. Eu contei e não extraí, e daí
concluí "os quatro alvos eram produto". Errado.

`<script>` inline faz parte do DOCUMENTO SERVIDO. Então continua
sendo rota HTTP pura: GET + extração. Só muda a zona.

═══════════════════════════════════════════════════════════════════
A RESPOSTA CERTA É CONHECIDA
═══════════════════════════════════════════════════════════════════
O operador mandou o destino real do meli.la/1hGSoc3:

    lista.mercadolivre.com.br/_Container_promotions-77-full
      ?coupon_campaign_id=13657213
      #tracking_id=…&source=affiliate-profile

E a mensagem do grupo confirma a natureza:

    "Válido na lista ↓ clique em 'Mostrar mais'"

Logo, esta sonda não procura às cegas. Ela procura POR ESSAS
AGULHAS EXATAS e reporta em qual bloco cada uma caiu. Ou acha, ou
prova que não está no servido.

═══════════════════════════════════════════════════════════════════
MÉTODO
═══════════════════════════════════════════════════════════════════
Cada <script> inline é tratado como um bloco NUMERADO e medido
separadamente — sem isso, "está no JS_INLINE" não diz de onde sai.

Para cada bloco: tamanho, quais agulhas contém, o contexto ao
redor, e tentativa de parse quando tem cara de estado.

ANÔNIMA. Nenhum cookie, CSRF ou credencial.
"""
from __future__ import annotations

import asyncio
import html as _html
import json
import os
import re
import sys
import urllib.parse

import aiohttp


_PADRAO = (
    "https://meli.la/1hGSoc3 :: LISTA _Container_promotions-77-full "
    "coupon_campaign_id=13657213|"
    "https://meli.la/13iNqPB :: LISTA (mesma lista, outro cupom)|"
    "https://meli.la/1FE6ohY :: PRODUTO Monitor AOC (controle)"
)

ALVOS = []
for item in (os.environ.get("ML_SOCIAL_URLS") or _PADRAO).split("|"):
    item = item.strip()
    if not item:
        continue
    url, _, esperado = item.partition("::")
    ALVOS.append((url.strip(), esperado.strip() or "(não informado)"))

# Agulhas da LISTA. As quatro primeiras são a resposta conhecida.
AGULHAS = (
    "_Container_",
    "coupon_campaign_id",
    "13657213",
    "promotions-77-full",
    "lista.mercadolivre",
    "Mostrar mais",
    "ui-recommendations-subtitle",
    "subtitle-link",
    "affiliate-profile",
)

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
_RE_TIPO = re.compile(r'type\s*=\s*["\']([^"\']+)["\']', re.I)

_RE_URL_LISTA = re.compile(
    r"https?:(?:\\u002[Ff]|\\/|/){2}lista\.mercadolivre\.com\.br"
    r"(?:[^\s\"'<>\\]|\\u002[Ff]|\\/)+", re.I)
_RE_CONTAINER = re.compile(r"_Container_[A-Za-z0-9_\-]+", re.I)
_RE_CAMPANHA = re.compile(r"coupon_campaign_id[\"'=:%\s\\u0]*?(\d{4,})", re.I)


def bloco(t: str) -> None:
    print("\n" + "═" * 72)
    print(t)
    print("═" * 72)


def sub(t: str) -> None:
    print(f"\n── {t} " + "─" * max(0, 68 - len(t)))


def desescapar(texto: str) -> str:
    """As quatro formas em que o ML escreve URL dentro de script."""
    saida = (texto.replace("\\u002F", "/").replace("\\u002f", "/")
                  .replace("\\/", "/").replace("\\u0026", "&"))
    try:
        saida += " ||UNQUOTE|| " + urllib.parse.unquote(texto)
    except Exception:
        pass
    try:
        saida += " ||UNESCAPE|| " + _html.unescape(texto)
    except Exception:
        pass
    return saida


def contexto(texto: str, agulha: str, quantos: int = 2, raio: int = 260) -> list:
    saida, inicio = [], 0
    baixo, alvo = texto.lower(), agulha.lower()
    for _ in range(quantos):
        i = baixo.find(alvo, inicio)
        if i < 0:
            break
        a, b = max(0, i - raio), min(len(texto), i + len(agulha) + raio)
        t = texto[a:b].replace("\n", " ").replace("\r", " ")
        saida.append(re.sub(r"\s{2,}", " ", t))
        inicio = i + len(agulha)
    return saida


def blocos_de_script(documento: str) -> tuple:
    """
    Cada <script> inline vira um bloco NUMERADO.

    Sem numerar, "está no JS_INLINE" não diz de onde sai — e era
    exatamente essa a lacuna da rodada 2.
    """
    inline, externos = [], []

    def _coletar(m):
        atributos, corpo = m.group(1), m.group(2)
        src = _RE_SRC.search(atributos)
        if src:
            externos.append(src.group(1))
        else:
            tipo = (_RE_TIPO.search(atributos).group(1)
                    if _RE_TIPO.search(atributos) else "")
            inline.append({"i": len(inline), "tipo": tipo, "corpo": corpo})
        return " "

    html_doc = _RE_SCRIPT.sub(_coletar, documento)
    return html_doc, inline, externos


def varrer(nome: str, texto: str, mostrar_contexto: bool = True) -> dict:
    """Conta as agulhas e mostra o entorno das que importam."""
    achou = {}
    expandido = desescapar(texto)
    for agulha in AGULHAS:
        n = expandido.lower().count(agulha.lower())
        if n:
            achou[agulha] = n
    if not achou:
        return {}

    print(f"\n    ┌─ {nome}  ({len(texto)} chars)")
    for agulha, n in achou.items():
        print(f"    │  🎯 {agulha:28s} {n}")
    print("    └─")

    if mostrar_contexto:
        for agulha in ("_Container_", "coupon_campaign_id",
                       "promotions-77-full", "13657213"):
            if agulha not in achou:
                continue
            for t in contexto(expandido, agulha):
                print(f"       [{agulha}] …{t}…")
    return achou


def extrair_urls_lista(texto: str) -> list:
    """URLs de lista.mercadolivre, já desescapadas."""
    expandido = desescapar(texto)
    brutas = _RE_URL_LISTA.findall(expandido)
    limpas = []
    for u in brutas:
        u = (u.replace("\\u002F", "/").replace("\\u002f", "/")
              .replace("\\/", "/").replace("\\u0026", "&"))
        u = _html.unescape(u)
        if u not in limpas:
            limpas.append(u)
    return limpas


async def sondar(s: aiohttp.ClientSession, url: str, esperado: str) -> None:
    bloco(f"ALVO  {url}\nESPERADO: {esperado}")

    # ── expansão ──────────────────────────────────────────────────
    sub("expansão")
    atual = url
    for n in range(1, 8):
        try:
            async with s.get(atual, headers=CAB, allow_redirects=False,
                             timeout=aiohttp.ClientTimeout(total=30)) as r:
                dest = r.headers.get("Location")
                print(f"    {n}. HTTP {r.status}  {atual[:110]}")
                if not dest:
                    break
                print(f"       → {dest[:150]}")
                atual = urllib.parse.urljoin(atual, dest)
        except Exception as exc:
            print(f"    {n}. FALHOU {type(exc).__name__}")
            return

    # ── documento ─────────────────────────────────────────────────
    try:
        async with s.get(atual, headers=CAB, allow_redirects=True,
                         timeout=aiohttp.ClientTimeout(total=45)) as r:
            corpo = await r.text(errors="ignore")
            status = r.status
    except Exception as exc:
        print(f"    GET FALHOU {type(exc).__name__}: {exc}")
        return

    html_doc, inline, externos = blocos_de_script(corpo)
    print(f"\n    HTTP {status} | documento {len(corpo)}B | "
          f"HTML_DOC {len(html_doc)}B | {len(inline)} blocos inline")

    # ── 1. marcação servida ───────────────────────────────────────
    sub("ZONA 1 — HTML_DOC (marcação servida)")
    if not varrer("HTML_DOC", html_doc):
        print("    nenhuma agulha de lista.")

    # ── 2. cada bloco inline, NUMERADO ────────────────────────────
    sub("ZONA 2 — CADA <script> INLINE, bloco a bloco")
    blocos_com_agulha = []
    for b in inline:
        achou = varrer(f"bloco #{b['i']}  type={b['tipo'] or '(sem type)'}",
                       b["corpo"])
        if achou:
            blocos_com_agulha.append((b, achou))
    if not blocos_com_agulha:
        print("    nenhum bloco inline contém agulha de lista.")

    # ── 3. o que interessa: a URL inteira ─────────────────────────
    sub("ZONA 3 — URLs de lista.mercadolivre recuperadas")
    todas = []
    for rotulo, texto in ([("HTML_DOC", html_doc)] +
                          [(f"bloco #{b['i']}", b["corpo"]) for b in inline]):
        for u in extrair_urls_lista(texto):
            todas.append((rotulo, u))
    if todas:
        for rotulo, u in todas[:25]:
            print(f"    >>> [{rotulo}] {u[:200]}")
    else:
        print("    NENHUMA URL de lista.mercadolivre no documento servido.")

    conts, camps = [], []
    for _, texto in ([("x", html_doc)] + [("y", b["corpo"]) for b in inline]):
        conts += _RE_CONTAINER.findall(desescapar(texto))
        camps += _RE_CAMPANHA.findall(desescapar(texto))
    print(f"\n    _Container_ (todos)        : {sorted(set(conts))[:10]}")
    print(f"    coupon_campaign_id (todos) : {sorted(set(camps))[:10]}")

    # ── 4. estado embutido, se houver ─────────────────────────────
    sub("ZONA 4 — blocos com cara de estado, parseados")
    for b, _ in blocos_com_agulha:
        corpo_b = b["corpo"].strip()
        candidato = None
        if corpo_b.startswith(("{", "[")):
            candidato = corpo_b
        else:
            m = re.search(r"=\s*(\{.*\})\s*;?\s*$", corpo_b, re.S)
            if m:
                candidato = m.group(1)
        if not candidato:
            print(f"    bloco #{b['i']}: não tem forma de estado "
                  f"(início: {corpo_b[:70]!r})")
            continue
        try:
            dados = json.loads(candidato)
        except Exception as exc:
            print(f"    bloco #{b['i']}: não parseou ({type(exc).__name__})")
            continue
        chaves = list(dados)[:25] if isinstance(dados, dict) else "lista"
        print(f"    bloco #{b['i']}: JSON ok | chaves de topo: {chaves}")

    sub("bundles próprios")
    for u in [u for u in externos
              if "affiliates" in u or "recommendations" in u][:8]:
        print(f"    {u[:150]}")


async def main() -> int:
    bloco("SONDA — RODADA 3: DE ONDE SAI O _Container_")
    print(f"  alvos      : {len(ALVOS)}")
    print(f"  credencial : NENHUMA (anônima)")
    print(f"  resposta conhecida do 1hGSoc3:")
    print(f"    _Container_promotions-77-full?coupon_campaign_id=13657213")

    async with aiohttp.ClientSession() as s:
        for url, esperado in ALVOS:
            try:
                await sondar(s, url, esperado)
            except Exception as exc:
                print(f"\n  ALVO FALHOU: {type(exc).__name__}: {exc}")

    bloco("FIM")
    print("  Se a URL da lista apareceu em ZONA 3, a ponte da LISTA")
    print("  também é HTTP pura — só muda a zona de extração.")
    print("  Se não apareceu em zona nenhuma, então nasce de XHR e")
    print("  o próximo passo é outro.")
    print()
    print("  NADA implementado. NADA alterado em produção.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
