#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
TESTE — o link de LISTA AFILIÁVEL escondido na vitrine /social/
═══════════════════════════════════════════════════════════════════

O QUE ESTE TESTE CORRIGE
    A execução 10 contou 79 URLs `lista.mercadolivre.com.br` na
    vitrine e imprimiu apenas as 5 primeiras. Como o menu de
    categorias vem no topo do HTML, as 5 eram todas
    `#menu=categories` — e a conclusão foi "não existe URL de lista
    afiliável".

    A conclusão foi tirada de 5 de 79. Errada.

    O operador encontrou no DevTools, dentro da própria vitrine:

        <a href="https://lista.mercadolivre.com.br/_Container_14216538
                 ?coupon_campaign_id=…#tracking_id=…&source=affiliate-profile"
           rel="nofollow" class="ui-recommendations-subtitle-link">

    É o mesmo formato que o createLink APROVOU na execução 6:

        lista.mercadolivre.com.br/_Container_promotions-77-full
          ?coupon_campaign_id=14194174

O QUE ESTE TESTE FAZ
    Classifica TODAS as URLs `lista.mercadolivre.com.br` da página,
    separando:
        _Container_        → candidata a lista afiliável
        #menu=categories   → navegação do site
        outras             → listadas à parte, sem descarte

    E casa cada `_Container_` com o título do carrossel em que
    aparece, para saber a que cupom pertence.

O QUE NÃO FAZ
    Não chama createLink. Não gera link. Não usa Playwright. Não
    altera produção, Core, pipeline. Não imprime credencial.

COMO RODAR
    start command: python -u testes_ml/teste_ml_container.py
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
import urllib.parse
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PAGINAS = [
    ("fadadoscupons (vitrine)",
     "https://www.mercadolivre.com.br/social/fadadoscupons"),
    ("promotom (vitrine)",
     "https://www.mercadolivre.com.br/social/promotom"),
    ("promotom (lista específica)",
     "https://www.mercadolivre.com.br/social/promotom/lists/"
     "8f90988a-1c69-4f23-8b26-76286c3cdc87"),
    ("samuelf3lipe (vitrine)",
     "https://www.mercadolivre.com.br/social/samuelf3lipe"),
]

LIMITE_HTML = 4_000_000


def log(msg: str = "") -> None:
    print(f"[ML-CONT] {msg}", flush=True)


def bloco(t: str) -> None:
    log("═" * 60)
    log(t)
    log("═" * 60)


_RE_HREF = re.compile(r'href=["\']([^"\']+)["\']', re.I)
_RE_TITULO = re.compile(
    r'class=["\'][^"\']*ui-recommendations-title[^"\']*["\'][^>]*>(.*?)</',
    re.S | re.I,
)
_RE_TAGS = re.compile(r"<[^>]+>")
_RE_CAMPANHA = re.compile(r"coupon_campaign_id=(\d+)", re.I)
_RE_CONTAINER = re.compile(r"/_Container_([^/?#]+)", re.I)


def limpar(bruto: str) -> str:
    return re.sub(r"\s+", " ", _RE_TAGS.sub("", bruto)).strip()


def unicos(seq) -> list:
    return list(OrderedDict.fromkeys(seq))


def analisar(html: str, rotulo: str, url: str) -> dict:
    """Classifica TODAS as URLs de lista. Nada é omitido."""
    hrefs = unicos(_RE_HREF.findall(html))
    listas = [h for h in hrefs if "lista.mercadolivre.com.br" in h.lower()]

    containers = [h for h in listas if "/_container_" in h.lower()]
    menus = [h for h in listas if "#menu=categories" in h.lower()]
    outras = [h for h in listas
              if h not in containers and h not in menus]

    # Casar cada _Container_ com o título do carrossel anterior.
    pares = []
    for m in re.finditer(
        r'href=["\']([^"\']*lista\.mercadolivre\.com\.br[^"\']*'
        r'_Container_[^"\']*)["\']', html, re.I
    ):
        janela = html[max(0, m.start() - 2500):m.start()]
        titulos = _RE_TITULO.findall(janela)
        titulo = limpar(titulos[-1])[:70] if titulos else ""
        pares.append((m.group(1), titulo))

    return {
        "rotulo": rotulo,
        "url": url,
        "tamanho": len(html),
        "href_total": len(hrefs),
        "lista_total": len(listas),
        "containers": unicos(containers),
        "menus": menus,
        "outras": outras,
        "pares": pares,
        "carrosseis": len(_RE_TITULO.findall(html)),
        "subtitle_link": html.lower().count(
            "ui-recommendations-subtitle-link"
        ),
    }


def relatar(r: dict) -> None:
    bloco(r["rotulo"])
    log(f"URL: {r['url'][:120]}")
    log(f"HTML: {r['tamanho']:,} bytes | hrefs distintos: {r['href_total']}")
    log(f"carrosséis (ui-recommendations-title): {r['carrosseis']}")
    log(f"ui-recommendations-subtitle-link: {r['subtitle_link']}")
    log()
    log(f"URLs lista.mercadolivre.com.br: {r['lista_total']}")
    log(f"  _Container_       : {len(r['containers'])}")
    log(f"  #menu=categories  : {len(r['menus'])}")
    log(f"  outras            : {len(r['outras'])}")
    log()

    if r["containers"]:
        log(">>> LISTAS AFILIÁVEIS ENCONTRADAS <<<")
        for i, u in enumerate(r["containers"], 1):
            mc = _RE_CONTAINER.search(u)
            mid = _RE_CAMPANHA.search(u)
            log(f"  {i:2}. {u[:150]}")
            log(f"      container={mc.group(1) if mc else '?'} "
                f"| campanha={mid.group(1) if mid else '(sem id)'}")
        log()
        if r["pares"]:
            log("CASAMENTO carrossel → lista:")
            for u, titulo in r["pares"][:20]:
                mid = _RE_CAMPANHA.search(u)
                log(f"  '{titulo or '(sem título)'}'")
                log(f"    → campanha {mid.group(1) if mid else '?'} | {u[:100]}")
        log()
    else:
        log("Nenhuma URL _Container_ nesta página.")
        log()

    if r["outras"]:
        log(f"Outras URLs de lista (nem _Container_ nem menu): "
            f"{len(r['outras'])}")
        for u in r["outras"][:10]:
            log(f"  {u[:130]}")
        log()


async def rodar() -> int:
    bloco("CAÇA AO _Container_ DENTRO DA VITRINE /social/")
    log("Corrige a execução 10, que olhou 5 de 79 URLs e concluiu")
    log("que não havia lista afiliável. Agora classifica todas.")
    log()

    try:
        import random
        import aiohttp
        import config
        import globals as g
    except Exception as exc:
        log(f"ERRO ao importar: {type(exc).__name__}: {exc}")
        return 2
    g._init_globals()
    sessao = await g._get_session()

    async def ler(url: str) -> tuple:
        hdrs = {
            "User-Agent": random.choice(config.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        }
        try:
            async with sessao.get(
                url, headers=hdrs, allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=25),
            ) as resp:
                return (await resp.text(errors="ignore"))[:LIMITE_HTML], ""
        except Exception as exc:
            return "", f"{type(exc).__name__}: {exc}"

    todos = []
    for rotulo, url in PAGINAS:
        html, erro = await ler(url)
        if erro or not html:
            log(f"[{rotulo}] ERRO: {erro or 'vazio'}")
            continue
        if "/captcha" in html[:3000].lower():
            log(f"[{rotulo}] muro de captcha — pulado")
            continue
        r = analisar(html, rotulo, url)
        relatar(r)
        todos.append(r)

    partes = ["═" * 60, "RESUMO", "═" * 60]
    total_cont = 0
    for r in todos:
        n = len(r["containers"])
        total_cont += n
        partes.append(
            f"{r['rotulo']:32} lista={r['lista_total']:3} "
            f"_Container_={n:2} menu={len(r['menus']):3}"
        )
    partes.append("")
    partes.append(f"Total de listas afiliáveis encontradas: {total_cont}")
    partes.append("")
    if total_cont:
        partes.append("CONFIRMADO: a vitrine de terceiro EXPÕE as URLs de")
        partes.append("lista do próprio Mercado Livre, no formato")
        partes.append("_Container_<id>?coupon_campaign_id=<id> — o mesmo")
        partes.append("que o createLink aprovou na execução 6.")
        partes.append("")
        partes.append("A cadeia fecha:")
        partes.append("  meli.la de grupo → /social/<terceiro>")
        partes.append("  → <a> do carrossel → _Container_")
        partes.append("  → createLink → meli.la NOSSO, de lista")
    else:
        partes.append("Nenhum _Container_ encontrado. A execução 10")
        partes.append("estava certa e o achado do DevTools precisa de")
        partes.append("outra explicação (render no cliente, por exemplo).")
    print("\n".join(f"[ML-CONT] {p}" for p in partes), flush=True)
    return 0


def main() -> int:
    try:
        return asyncio.run(rodar())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
