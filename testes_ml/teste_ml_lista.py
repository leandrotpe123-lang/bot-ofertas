#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
TESTE FORENSE — LISTA: /social/<afiliado>/lists/<UUID>
═══════════════════════════════════════════════════════════════════

OBJETIVO ÚNICO
    Descobrir como a página de LISTA entrega os produtos e se existe
    uma URL própria da lista, recuperável e utilizável no createLink.

FORA DE ESCOPO
    Produto já foi comprovado manualmente:
        meli.la → /social/promotom → poly-card → <a href> → produto
    Este teste NÃO reinvestiga produto, exceto para comparar
    estruturas.

O QUE ESTE TESTE NÃO FAZ
    Não altera produção, Core, pipeline. Não cria cliente.py. Não
    implementa nada. Não usa Playwright nem navegador. Não segue
    endpoint inventado. Não imprime credencial.

MÉTODO
    Leitura HTTP única por página, com os mesmos cabeçalhos que o
    expandidor de produção usa. Depois, inspeção do HTML.

SEPARAÇÃO EXIGIDA
    A) PRODUTOS DENTRO DA LISTA
    B) IDENTIDADE DA LISTA
    C) URL DA LISTA
    D) URL DE CADA PRODUTO

COMO RODAR
    start command: python -u testes_ml/teste_ml_lista.py
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
import urllib.parse
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Lista real descoberta na execução 8, expandindo /sec/2U6U32Q.
URL_LISTA = (
    "https://www.mercadolivre.com.br/social/promotom/lists/"
    "8f90988a-1c69-4f23-8b26-76286c3cdc87?matt_tool=54541970&forceInApp=true"
)

# Vitrine do mesmo afiliado, para comparação estrutural.
URL_SOCIAL = "https://www.mercadolivre.com.br/social/promotom"

LIMITE_HTML = 4_000_000


def log(msg: str = "") -> None:
    print(f"[ML-LISTA] {msg}", flush=True)


def bloco(t: str) -> None:
    log("═" * 60)
    log(t)
    log("═" * 60)


def sub(t: str) -> None:
    log("─" * 60)
    log(t)


# ── Padrões ───────────────────────────────────────────────────────
_RE_MLB = re.compile(r"\bMLB[-]?U?\d{6,}\b")
_RE_UUID = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}\b", re.I
)
_RE_HREF = re.compile(r'<a\b[^>]*?href=["\']([^"\']+)["\'][^>]*>', re.I)
_RE_TAG_A = re.compile(r'<a\b[^>]*?>(.*?)</a>', re.S | re.I)
_RE_TITULO_CARD = re.compile(
    r'class=["\'][^"\']*poly-component__title[^"\']*["\'][^>]*>(?:\s*<a[^>]*>)?'
    r'(.*?)<', re.S | re.I
)
_RE_TAGS = re.compile(r"<[^>]+>")


def contar(html: str, agulha: str) -> int:
    return html.lower().count(agulha.lower())


def unicos(seq) -> list:
    return list(OrderedDict.fromkeys(seq))


def limpar_texto(bruto: str) -> str:
    return re.sub(r"\s+", " ", _RE_TAGS.sub("", bruto)).strip()


def eh_url_produto(u: str) -> bool:
    return bool(_RE_MLB.search(u))


def analisar(html: str, url: str, rotulo: str) -> dict:
    """Mapa estrutural da página. Não segue nada."""
    r: dict = {"rotulo": rotulo, "url": url, "tamanho": len(html)}

    # ── 1. Elementos de card ──────────────────────────────────────
    r["poly_card"] = contar(html, "poly-card")
    r["poly_card_list"] = contar(html, "poly-card--list")
    r["poly_card_large"] = contar(html, "poly-card--large")
    r["poly_card_grid"] = contar(html, "poly-card--grid")
    r["poly_component_title"] = contar(html, "poly-component__title")

    # ── 2. Links ──────────────────────────────────────────────────
    hrefs = unicos(_RE_HREF.findall(html))
    r["href_total"] = len(hrefs)

    hrefs_produto = [h for h in hrefs if eh_url_produto(h)]
    r["href_produto_total"] = len(hrefs_produto)
    r["href_produto_exemplos"] = [h[:120] for h in hrefs_produto[:6]]

    hrefs_lista = [h for h in hrefs if "/lists/" in h.lower()]
    r["href_lista_total"] = len(hrefs_lista)
    r["href_lista_exemplos"] = [h[:120] for h in hrefs_lista[:5]]

    hrefs_listaml = [h for h in hrefs
                     if "lista.mercadolivre.com.br" in h.lower()]
    r["href_listaml_total"] = len(hrefs_listaml)
    r["href_listaml_exemplos"] = [h[:120] for h in hrefs_listaml[:5]]

    hrefs_social = [h for h in hrefs if "/social/" in h.lower()]
    r["href_social_total"] = len(hrefs_social)
    r["href_social_exemplos"] = [h[:120] for h in hrefs_social[:5]]

    r["ir_para_produto"] = contar(html, "Ir para produto")
    r["ir_para_lista"] = contar(html, "Ir para a lista") + contar(
        html, "Ver lista"
    )

    # ── 3. Identificadores ────────────────────────────────────────
    mlbs = unicos(_RE_MLB.findall(html))
    r["mlb_total"] = len(mlbs)
    r["mlb_exemplos"] = mlbs[:10]

    uuids = unicos(_RE_UUID.findall(html))
    r["uuid_total"] = len(uuids)
    r["uuid_exemplos"] = uuids[:5]

    try:
        path = urllib.parse.urlparse(url).path or ""
    except Exception:
        path = ""
    m = _RE_UUID.search(path)
    r["uuid_no_path"] = m.group(0) if m else ""

    for chave in ("product_id", "productId", "item_id", "itemId",
                  "listId", "list_id", "list_uuid", "permalink"):
        r[f"k_{chave}"] = contar(html, f'"{chave}"')

    # ── 4. Cards: título + href, na ordem do documento ────────────
    titulos = [limpar_texto(t) for t in _RE_TITULO_CARD.findall(html)]
    titulos = [t for t in titulos if t][:12]
    r["titulos"] = titulos

    # Pares (href, texto) onde o href é de produto — ordem preservada.
    pares = []
    for m in _RE_HREF.finditer(html):
        href = m.group(1)
        if not eh_url_produto(href):
            continue
        trecho = html[m.end():m.end() + 400]
        texto = limpar_texto(trecho.split("</a>")[0])[:70]
        pares.append((href, texto))
    r["cards_ordenados"] = pares[:10]
    r["cards_total"] = len(pares)

    return r


def sim_nao(valor) -> str:
    if isinstance(valor, int):
        return f"SIM ({valor})" if valor else "NAO"
    return "SIM" if valor else "NAO"


def relatar(r: dict) -> None:
    bloco(f"{r['rotulo']}")
    log(f"URL: {r['url'][:140]}")
    log(f"tamanho HTML: {r['tamanho']:,} bytes")
    log()

    sub("1. ELEMENTOS DE CARD")
    log(f"  poly-card              : {r['poly_card']}")
    log(f"  poly-card--list        : {r['poly_card_list']}")
    log(f"  poly-card--large       : {r['poly_card_large']}")
    log(f"  poly-card--grid        : {r['poly_card_grid']}")
    log(f"  poly-component__title  : {r['poly_component_title']}")
    log()

    sub("A) PRODUTOS DENTRO DA LISTA")
    log(f"  IDs MLB distintos      : {r['mlb_total']}")
    if r["mlb_exemplos"]:
        log(f"    {', '.join(r['mlb_exemplos'])}")
    log(f"  cards com href produto : {r['cards_total']}")
    log(f"  'Ir para produto'      : {r['ir_para_produto']}")
    log()

    sub("D) URL DE CADA PRODUTO")
    log(f"  hrefs de produto       : {r['href_produto_total']}")
    for h in r["href_produto_exemplos"]:
        log(f"    {h}")
    if not r["href_produto_total"]:
        log("    (nenhum href de produto no HTML)")
    log()

    if r["cards_ordenados"]:
        sub("ORDEM DOS CARDS (href → texto)")
        for i, (href, texto) in enumerate(r["cards_ordenados"], 1):
            log(f"  {i:2}. {href[:78]}")
            if texto:
                log(f"      {texto}")
        log()

    if r["titulos"]:
        sub("TÍTULOS DOS CARDS")
        for i, t in enumerate(r["titulos"], 1):
            log(f"  {i:2}. {t[:80]}")
        log()

    sub("B) IDENTIDADE DA LISTA")
    log(f"  uuid no path           : {r['uuid_no_path'] or '(ausente)'}")
    log(f"  uuids no corpo         : {r['uuid_total']}")
    if r["uuid_exemplos"]:
        log(f"    {', '.join(r['uuid_exemplos'])}")
    log(f"  \"listId\"               : {r['k_listId']}")
    log(f"  \"list_id\"              : {r['k_list_id']}")
    log(f"  \"list_uuid\"            : {r['k_list_uuid']}")
    log(f"  \"product_id\"           : {r['k_product_id']}")
    log(f"  \"productId\"            : {r['k_productId']}")
    log(f"  \"item_id\"              : {r['k_item_id']}")
    log(f"  \"itemId\"               : {r['k_itemId']}")
    log(f"  \"permalink\"            : {r['k_permalink']}")
    log()

    sub("C) URL DA LISTA")
    log(f"  hrefs com /lists/      : {r['href_lista_total']}")
    for h in r["href_lista_exemplos"]:
        log(f"    {h}")
    log(f"  hrefs lista.mercadolivre.com.br : {r['href_listaml_total']}")
    for h in r["href_listaml_exemplos"]:
        log(f"    {h}")
    if not r["href_listaml_total"]:
        log("    (nenhuma URL lista.mercadolivre.com.br)")
    log(f"  hrefs /social/         : {r['href_social_total']}")
    for h in r["href_social_exemplos"]:
        log(f"    {h}")
    log(f"  'Ir para a lista'/'Ver lista' : {r['ir_para_lista']}")
    log()


def tabela(a: dict, b: dict) -> list:
    """Comparação SOCIAL x SOCIAL_LIST."""
    linhas = [
        "| Campo | SOCIAL | SOCIAL_LIST |",
        "|---|---|---|",
    ]

    def par(rotulo, chave, formato=sim_nao):
        linhas.append(f"| {rotulo} | {formato(a.get(chave))} "
                      f"| {formato(b.get(chave))} |")

    par("poly-card", "poly_card")
    par("poly-card--list", "poly_card_list")
    par("poly-card--large", "poly_card_large")
    par("href produto", "href_produto_total")
    par("'Ir para produto'", "ir_para_produto")
    par("product_id", "k_product_id")
    par("item_id", "k_item_id")
    par("listId", "k_listId")
    par("uuid no corpo", "uuid_total")
    linhas.append(
        f"| uuid no path | {a.get('uuid_no_path') or 'NAO'} "
        f"| {b.get('uuid_no_path') or 'NAO'} |"
    )
    par("href /lists/", "href_lista_total")
    par("href lista.mercadolivre", "href_listaml_total")
    par("IDs MLB", "mlb_total")
    linhas.append(
        f"| tamanho HTML | {a.get('tamanho', 0):,} "
        f"| {b.get('tamanho', 0):,} |"
    )
    return linhas


async def rodar() -> int:
    bloco("TESTE FORENSE — LISTA /social/<afiliado>/lists/<UUID>")
    log("Somente leitura. Sem Playwright. Sem createLink.")
    log("Produto NÃO é reinvestigado — só comparado.")
    log()

    try:
        import random
        import aiohttp
        import config
        import globals as g
    except Exception as exc:
        log(f"ERRO ao importar: {type(exc).__name__}: {exc}")
        return 2
    log("Import OK.")
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
                txt = await resp.text(errors="ignore")
                return txt[:LIMITE_HTML], str(resp.url), ""
        except Exception as exc:
            return "", url, f"{type(exc).__name__}: {exc}"

    # ── SOCIAL_LIST ───────────────────────────────────────────────
    html_l, final_l, erro_l = await ler(URL_LISTA)
    if erro_l or not html_l:
        log(f"ERRO ao ler a lista: {erro_l or 'resposta vazia'}")
        return 1
    if "/captcha" in final_l.lower():
        log("Destino veio como muro de captcha — sem contorno. Abortado.")
        return 1
    r_lista = analisar(html_l, final_l, "SOCIAL_LIST — lista específica")
    relatar(r_lista)

    # ── SOCIAL ────────────────────────────────────────────────────
    html_s, final_s, erro_s = await ler(URL_SOCIAL)
    if erro_s or not html_s:
        log(f"ERRO ao ler a vitrine: {erro_s or 'resposta vazia'}")
        r_social = {}
    else:
        r_social = analisar(html_s, final_s, "SOCIAL — vitrine do afiliado")
        relatar(r_social)

    # ── Comparação e respostas ────────────────────────────────────
    partes = ["═" * 60, "COMPARAÇÃO SOCIAL x SOCIAL_LIST", "═" * 60]
    if r_social:
        partes += tabela(r_social, r_lista)
    else:
        partes.append("(vitrine não pôde ser lida)")
    partes.append("")
    partes += ["═" * 60, "RESPOSTAS", "═" * 60]

    tem_href_prod = r_lista["href_produto_total"] > 0
    partes.append(
        f"A página /lists contém links individuais de produtos? "
        f"{'SIM' if tem_href_prod else 'NAO'}"
    )
    partes.append(
        f"Esses links aparecem em href? "
        f"{'SIM' if tem_href_prod else 'NAO'} "
        f"({r_lista['href_produto_total']})"
    )
    partes.append(
        f"Existe <a> para a própria lista? "
        f"{'SIM' if r_lista['href_lista_total'] else 'NAO'} "
        f"({r_lista['href_lista_total']})"
    )
    partes.append(
        f"Existe listId/UUID? uuid_no_path="
        f"{r_lista['uuid_no_path'] or 'NAO'} | "
        f"listId={r_lista['k_listId']} | uuids_corpo={r_lista['uuid_total']}"
    )
    partes.append(
        f"Existe URL lista.mercadolivre.com.br? "
        f"{'SIM' if r_lista['href_listaml_total'] else 'NAO'} "
        f"({r_lista['href_listaml_total']})"
    )
    partes.append(
        f"HTML traz produtos completos ou só IDs? "
        f"{'completos (href+título)' if tem_href_prod else 'somente IDs'}"
    )
    partes.append(f"Quantos produtos? {r_lista['mlb_total']} IDs distintos, "
                  f"{r_lista['cards_total']} cards com href")

    if r_social:
        dif = []
        if r_social["poly_card"] != r_lista["poly_card"]:
            dif.append("contagem de poly-card")
        if bool(r_social["href_produto_total"]) != bool(
                r_lista["href_produto_total"]):
            dif.append("presença de href de produto")
        if bool(r_social["k_listId"]) != bool(r_lista["k_listId"]):
            dif.append("presença de listId")
        partes.append(
            "Diferença estrutural SOCIAL x SOCIAL_LIST: "
            + (", ".join(dif) if dif else "nenhuma relevante detectada")
        )

    partes.append("")
    if r_lista["href_listaml_total"]:
        partes.append("URL de lista afiliável ENCONTRADA na resposta.")
    elif tem_href_prod:
        partes.append("Não há URL de lista afiliável; há URLs de produto.")
    else:
        partes.append("Nem URL de lista nem href de produto.")

    print("\n".join(f"[ML-LISTA] {p}" for p in partes), flush=True)
    return 0


def main() -> int:
    try:
        return asyncio.run(rodar())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
