"""
Rodada curta: `components[1]….polycards[0]` é o item anunciado?

Última lacuna da forense. O inventário mostrou que o CANDIDATO #1 de
um alvo de produto é

    $.appProps.pageProps.data.components[1]
      .recommendation_data.recommendation_info.polycards[0]

mas eu não capturei o VALOR de `url`/`title` dele para comparar com
o que o grupo anunciou. Sem isso a regra não está provada — e tratar
por provado é o erro que já cometi duas vezes nesta investigação.

Esta sonda imprime só o que decide, e faz a comparação SOZINHA:
o veredito sai calculado, não interpretado por mim depois.

Critério: todas as palavras-chave do anúncio têm de aparecer na
`url` ou no `title` do polycards[0]. Comparação sem acento e sem
caixa; nada de similaridade difusa — é continência de palavra.

ANÔNIMA. Nenhum cookie, CSRF ou credencial.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import unicodedata
import urllib.parse
from typing import Any, Optional

import aiohttp


_PADRAO = (
    "https://meli.la/1FE6ohY :: monitor aoc|"
    "https://meli.la/2tA9txv :: teclado f75|"
    "https://meli.la/1hGSoc3 :: |"
    "https://meli.la/13iNqPB :: "
)

ALVOS = []
for item in (os.environ.get("ML_SOCIAL_URLS") or _PADRAO).split("|"):
    item = item.strip()
    if not item:
        continue
    url, _, chaves = item.partition("::")
    ALVOS.append((url.strip(), [p for p in chaves.strip().split() if p]))

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/148.0.0.0 Safari/537.36")

CAB = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}

_RE_SCRIPT = re.compile(r"<script([^>]*)>(.*?)</script>", re.I | re.S)
_RE_SRC = re.compile(r'src\s*=\s*["\']([^"\']+)["\']', re.I)

CAMINHO = ("appProps", "pageProps", "data", "components", 1,
           "recommendation_data", "recommendation_info")


def bloco(t: str) -> None:
    print("\n" + "═" * 72)
    print(t)
    print("═" * 72)


def _fim(texto: str, i: int) -> int:
    """Fim do objeto aberto em `i`, respeitando string e escape."""
    prof, n, s, esc = 0, len(texto), False, False
    while i < n:
        c = texto[i]
        if s:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                s = False
        elif c == '"':
            s = True
        elif c == "{":
            prof += 1
        elif c == "}":
            prof -= 1
            if prof == 0:
                return i + 1
        i += 1
    return -1


MARCADORES = ("recommendation_info", "appProps")


def maior_objeto(corpo: str) -> Optional[Any]:
    """
    O maior objeto JSON atribuído que CONTÉM o que procuramos.

    O critério é conteúdo, não tamanho. Um limiar de bytes rejeita
    objeto pequeno legítimo e aceita objeto grande irrelevante —
    exigir o marcador acerta nos dois casos e ainda deixa a função
    testável fora de produção.
    """
    melhor = None
    for m in re.finditer(r"=\s*(\{)", corpo):
        f = _fim(corpo, m.start(1))
        if f <= 0:
            continue
        cru = corpo[m.start(1):f]
        if not any(f'"{alvo}"' in cru for alvo in MARCADORES):
            continue
        try:
            obj = json.loads(cru)
        except Exception:
            continue
        if melhor is None or len(cru) > melhor[0]:
            melhor = (len(cru), obj)
    return melhor[1] if melhor else None


def descer(no: Any, caminho) -> Any:
    """Desce pelo caminho; devolve None se qualquer degrau faltar."""
    for passo in caminho:
        try:
            no = no[passo]
        except Exception:
            return None
    return no


def simples(texto: str) -> str:
    """Sem acento, sem caixa, sem pontuação — para conter palavra."""
    t = unicodedata.normalize("NFKD", str(texto))
    t = "".join(c for c in t if not unicodedata.combining(c)).lower()
    return re.sub(r"[^a-z0-9]+", " ", t)


def mostrar_card(rotulo: str, card: Any) -> str:
    """Imprime o essencial de um polycard. Devolve url+title."""
    if not isinstance(card, dict):
        print(f"    {rotulo}: (ausente)")
        return ""
    meta = card.get("metadata") or {}
    url = str(card.get("url") or meta.get("url") or "")
    titulo = str(card.get("title") or meta.get("title") or "")
    print(f"    {rotulo}")
    print(f"      url           : {url[:130]}")
    print(f"      title         : {titulo[:110]}")
    print(f"      pid           : {meta.get('pid')}")
    print(f"      id (anuncio)  : {meta.get('id')}")
    print(f"      pid_extended  : {meta.get('pid_extended')}")
    print(f"      product_id    : {meta.get('product_id')}")
    return f"{url} {titulo}"


async def sondar(s: aiohttp.ClientSession, url: str, chaves: list) -> None:
    esperado = " ".join(chaves) if chaves else "(LISTA - sem palavra-chave)"
    bloco(f"ALVO {url}\nESPERADO: {esperado}")

    atual = url
    for _ in range(8):
        async with s.get(atual, headers=CAB, allow_redirects=False,
                         timeout=aiohttp.ClientTimeout(total=30)) as r:
            dest = r.headers.get("Location")
            if not dest:
                break
            atual = urllib.parse.urljoin(atual, dest)

    async with s.get(atual, headers=CAB, allow_redirects=True,
                     timeout=aiohttp.ClientTimeout(total=50)) as r:
        corpo = await r.text(errors="ignore")
        print(f"    HTTP {r.status} | {len(corpo)}B")

    inline = []

    def _c(m):
        if not _RE_SRC.search(m.group(1)):
            inline.append(m.group(2))
        return " "

    _RE_SCRIPT.sub(_c, corpo)

    estado = None
    for b in inline:
        if len(b) < 2000:
            continue
        obj = maior_objeto(b)
        if obj and descer(obj, CAMINHO[:3]) is not None:
            estado = obj
            break

    if estado is None:
        print("    ESTADO NAO EXTRAIDO - nada a concluir neste alvo.")
        return

    info = descer(estado, CAMINHO)
    if info is None:
        print("    components[1].recommendation_info AUSENTE.")
        return

    print(f"\n    seeMoreLink          : "
          f"{str(info.get('seeMoreLink') or '(vazio)')[:140]}")
    print(f"    isRecommendationList : {info.get('isRecommendationList')}")
    print(f"    affiliateName        : {info.get('affiliateName')}")
    print(f"    totalElements        : {info.get('totalElements')}")

    cards = info.get("polycards") or []
    print(f"    polycards            : {len(cards)}")
    print()

    agulheiro = ""
    for i in range(min(3, len(cards))):
        texto = mostrar_card(f"polycards[{i}]", cards[i])
        if i == 0:
            agulheiro = texto
        print()

    if not chaves:
        print("    (alvo de LISTA - veredito de produto nao se aplica)")
        return

    if not agulheiro:
        print("    VEREDITO: polycards[0] sem url/title - NAO COMPROVA")
        return

    campo = simples(agulheiro)
    faltando = [k for k in chaves if simples(k).strip() not in campo]
    if faltando:
        print(f"    VEREDITO: *** NAO BATE *** faltou {faltando}")
        print(f"      procurei em: {campo[:150]}")
    else:
        print(f"    VEREDITO: *** BATE *** todas as palavras "
              f"{chaves} estao em polycards[0]")


async def main() -> int:
    bloco("RODADA CURTA - polycards[0] de components[1]")
    print("  pergunta: o polycards[0] E o item anunciado pelo grupo?")
    print("  criterio: continencia de palavra, sem acento e sem caixa.")
    print("  credencial: NENHUMA (anonima).")

    async with aiohttp.ClientSession() as s:
        for url, chaves in ALVOS:
            try:
                await sondar(s, url, chaves)
            except Exception as exc:
                print(f"\n  ALVO FALHOU: {type(exc).__name__}: {exc}")

    bloco("FIM")
    print("  Dois BATE nos alvos de produto => regra comprovada por")
    print("  ESTRUTURA: sem heuristica, sem titulo da mensagem, sem")
    print("  posicao de ancora. Qualquer NAO BATE derruba a regra.")
    print()
    print("  NADA implementado. NADA alterado em producao.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
