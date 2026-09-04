#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
TESTE FORENSE — a vitrine /social/ carrega a identidade original?
═══════════════════════════════════════════════════════════════════

PERGUNTA ÚNICA
    Uma URL afiliada que expande para /social/… ainda carrega
    internamente a identidade do produto ou da lista de origem?

    Se carregar, existe caminho para recuperar a oferta real a
    partir do link afiliado de terceiro.
    Se não carregar, o fallback próprio é a única saída.

ETAPAS
    1. expandir com o desencurtador REAL do Foguetão
    2. uma leitura HTTP legítima da página de destino
    3. mapear a estrutura da resposta

O QUE ESTE TESTE NÃO FAZ
    Não segue nenhum endpoint descoberto — esta etapa é só o mapa.
    Não usa Playwright. Não chama createLink. Não gera link.
    Não tenta contornar captcha: se a página vier como muro, isso
    é registrado como resultado e o link é pulado.
    Não altera produção, Core, pipeline ou publicação.
    Não imprime cookie, token, header sensível ou credencial.

COMO RODAR
    start command: python -u testes_ml/teste_ml_forense.py
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import urllib.parse
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ALVOS = [
    "https://meli.la/1ipL9sf",
    "https://meli.la/26eaLSW",
    "https://meli.la/1bZpCya",
    "https://meli.la/2nhS3s3",
    "https://mercadolivre.com/sec/2U6U32Q",
]

# Controle: lista REAL, comprovadamente afiliável (execução 6).
CONTROLE = (
    "https://lista.mercadolivre.com.br/_Container_promotions-77-full"
    "?coupon_campaign_id=14194174"
)

LIMITE_HTML = 3_000_000


def log(msg: str = "") -> None:
    print(f"[ML-FOR] {msg}", flush=True)


def bloco(t: str) -> None:
    log("═" * 58)
    log(t)
    log("═" * 58)


# ── Padrões de identidade ─────────────────────────────────────────
_RE_MLB = re.compile(r"\bMLB[-]?U?\d{6,}\b")
_RE_UUID = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}\b", re.I
)
_RE_NEXT = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', re.S | re.I
)
_RE_PRELOAD = re.compile(
    r"(?:window\.__PRELOADED_STATE__|window\.__INITIAL_STATE__"
    r"|__APP_DATA__|window\.__STATE__)\s*=", re.I
)
_RE_OG = re.compile(
    r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']+)', re.I
)
_RE_CANON = re.compile(
    r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)', re.I
)
_RE_JSONLD = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.S | re.I,
)

# Chaves de identidade procuradas no corpo.
CHAVES = [
    "item_id", "itemId", "productId", "product_id",
    "listId", "list_id", "collection", "products", "items",
    "catalog_product_id", "permalink",
]

# Endpoints que a própria página referencia.
_RE_ENDPOINT = re.compile(
    r'["\'](https?://[a-z0-9.\-]*mercadol[a-z]+\.com(?:\.br)?/'
    r'[^"\'\s]{0,120})["\']', re.I
)
_RE_API_REL = re.compile(r'["\'](/api/[^"\'\s]{0,110})["\']', re.I)


def host_de(url: str) -> str:
    try:
        h = (urllib.parse.urlparse(url).hostname or "").lower()
        return h[4:] if h.startswith("www.") else h
    except Exception:
        return ""


def tipo_de(url: str) -> str:
    try:
        path = urllib.parse.urlparse(url).path or "/"
    except Exception:
        return "OUTRO"
    p = path.lower()
    if "/captcha" in p:
        return "CAPTCHA"
    if "/social/" in p and "/lists/" in p:
        return "SOCIAL_LIST"
    if "/social/" in p:
        return "SOCIAL"
    if "/sec/" in p:
        return "SEC"
    if host_de(url) == "lista.mercadolivre.com.br":
        return "LISTA_REAL"
    if _RE_MLB.search(path):
        return "PRODUTO"
    return "OUTRO"


def unicos(valores) -> list:
    return list(OrderedDict.fromkeys(valores))


def analisar_html(html: str, url_final: str) -> dict:
    """Mapeia a estrutura da resposta. Não segue nada."""
    r: dict = {}
    r["tamanho"] = len(html)

    mlbs = unicos(_RE_MLB.findall(html))
    r["mlb_total"] = len(mlbs)
    r["mlb_exemplos"] = mlbs[:8]

    uuids = unicos(_RE_UUID.findall(html))
    r["uuid_total"] = len(uuids)
    r["uuid_exemplos"] = uuids[:4]

    # Identidade no próprio path
    try:
        path = urllib.parse.urlparse(url_final).path or ""
    except Exception:
        path = ""
    m = _RE_UUID.search(path)
    r["uuid_no_path"] = m.group(0) if m else ""

    # Chaves de identidade
    presentes = []
    for chave in CHAVES:
        n = html.count(f'"{chave}"')
        if n:
            presentes.append(f"{chave}×{n}")
    r["chaves"] = presentes

    # Estados serializados
    mnext = _RE_NEXT.search(html)
    r["next_data"] = bool(mnext)
    r["next_tamanho"] = len(mnext.group(1)) if mnext else 0
    if mnext:
        try:
            dados = json.loads(mnext.group(1))
            r["next_chaves"] = sorted(list(dados.keys()))[:12]
            props = dados.get("props") or {}
            pp = props.get("pageProps") or {}
            if isinstance(pp, dict):
                r["next_pageprops"] = sorted(list(pp.keys()))[:14]
        except Exception as exc:
            r["next_erro"] = type(exc).__name__
    r["preloaded_state"] = bool(_RE_PRELOAD.search(html))

    # Metadados canônicos
    mog = _RE_OG.search(html)
    r["og_url"] = mog.group(1) if mog else ""
    mcanon = _RE_CANON.search(html)
    r["canonical"] = mcanon.group(1) if mcanon else ""

    # JSON-LD
    lds = _RE_JSONLD.findall(html)
    r["jsonld_blocos"] = len(lds)
    tipos = []
    for ld in lds[:4]:
        try:
            d = json.loads(ld)
            if isinstance(d, dict) and d.get("@type"):
                tipos.append(str(d["@type"]))
        except Exception:
            pass
    r["jsonld_tipos"] = tipos

    # URLs explícitas de produto/lista dentro da resposta
    achadas = unicos(_RE_ENDPOINT.findall(html))
    urls_produto = [u for u in achadas if _RE_MLB.search(u)]
    urls_lista = [u for u in achadas if "/lists/" in u.lower()
                  or "lista.mercadolivre" in u.lower()]
    r["urls_produto_total"] = len(urls_produto)
    r["urls_produto_exemplos"] = [u[:110] for u in urls_produto[:4]]
    r["urls_lista_total"] = len(urls_lista)
    r["urls_lista_exemplos"] = [u[:110] for u in urls_lista[:3]]

    # Endpoints de API referenciados
    apis = [u for u in achadas
            if "/api/" in u.lower() or u.lower().endswith(".json")]
    apis += unicos(_RE_API_REL.findall(html))
    apis = unicos(apis)
    r["api_total"] = len(apis)
    r["api_exemplos"] = [u[:110] for u in apis[:6]]

    return r


def concluir(r: dict, tipo: str) -> tuple:
    """Devolve (conclusão, motivo)."""
    if tipo == "CAPTCHA":
        return ("NAO RECUPERAVEL", "página veio como muro de captcha")

    tem_produto = r.get("mlb_total", 0) > 0
    tem_url_produto = r.get("urls_produto_total", 0) > 0
    tem_lista = bool(r.get("uuid_no_path")) or r.get("urls_lista_total", 0) > 0
    tem_api = r.get("api_total", 0) > 0

    if tem_url_produto and tem_produto:
        return ("RECUPERAVEL",
                "há URLs de produto explícitas na própria resposta")
    if tem_produto:
        return ("PARCIALMENTE RECUPERAVEL",
                "há IDs MLB no corpo, mas sem URL de produto montada")
    if tem_lista and tem_api:
        return ("PARCIALMENTE RECUPERAVEL",
                "há identificador de lista e endpoint que a alimenta")
    if tem_lista:
        return ("PARCIALMENTE RECUPERAVEL",
                "há identificador de lista, sem endpoint visível")
    if tem_api:
        return ("PARCIALMENTE RECUPERAVEL",
                "sem identidade no HTML; produtos vêm de endpoint")
    return ("NAO RECUPERAVEL",
            "nenhuma identidade de produto ou lista na resposta")


def relatar(rotulo: str, curta: str, final: str, r: dict) -> tuple:
    tipo = tipo_de(final)
    log("─" * 58)
    log(f"{rotulo}")
    log(f"  URL curta      : {curta}")
    log(f"  URL expandida  : {final[:150]}")
    log(f"  host           : {host_de(final)}")
    log(f"  tipo           : {tipo}")

    if r.get("erro"):
        log(f"  ERRO na leitura: {r['erro']}")
        return (tipo, "NAO RECUPERAVEL", "falha ao ler a resposta")

    log(f"  tamanho HTML   : {r['tamanho']:,} bytes")
    log()
    log(f"  identificador de produto? "
        f"{'SIM' if r['mlb_total'] else 'NAO'} ({r['mlb_total']} distintos)")
    if r["mlb_exemplos"]:
        log(f"    exemplos: {', '.join(r['mlb_exemplos'])}")

    tem_lista = bool(r["uuid_no_path"]) or r["urls_lista_total"] > 0
    log(f"  identificador de lista?   {'SIM' if tem_lista else 'NAO'}")
    if r["uuid_no_path"]:
        log(f"    uuid no path: {r['uuid_no_path']}")
    log(f"    uuids no corpo: {r['uuid_total']}")
    if r["uuid_exemplos"]:
        log(f"    exemplos: {', '.join(r['uuid_exemplos'][:3])}")

    log(f"  produtos identificados    : {r['mlb_total']}")
    log()
    log(f"  URLs de produto na resposta: {r['urls_produto_total']}")
    for u in r["urls_produto_exemplos"]:
        log(f"    {u}")
    log(f"  URLs de lista na resposta  : {r['urls_lista_total']}")
    for u in r["urls_lista_exemplos"]:
        log(f"    {u}")
    log()
    log(f"  endpoints referenciados    : {r['api_total']}")
    for u in r["api_exemplos"]:
        log(f"    {u}")
    log()
    log(f"  __NEXT_DATA__  : {'SIM' if r['next_data'] else 'NAO'}"
        f"{f' ({r[chr(39)+chr(39)] if False else r['next_tamanho']:,} bytes)' if r['next_data'] else ''}")
    if r.get("next_chaves"):
        log(f"    chaves       : {', '.join(r['next_chaves'])}")
    if r.get("next_pageprops"):
        log(f"    pageProps    : {', '.join(r['next_pageprops'])}")
    if r.get("next_erro"):
        log(f"    parse falhou : {r['next_erro']}")
    log(f"  estado pré-carregado: {'SIM' if r['preloaded_state'] else 'NAO'}")
    log(f"  chaves de identidade: {', '.join(r['chaves']) or '(nenhuma)'}")
    log(f"  og:url    : {r['og_url'][:120] or '(ausente)'}")
    log(f"  canonical : {r['canonical'][:120] or '(ausente)'}")
    log(f"  JSON-LD   : {r['jsonld_blocos']} bloco(s) "
        f"{r['jsonld_tipos'] or ''}")

    conclusao, motivo = concluir(r, tipo)
    log()
    log(f"  >>> CONCLUSÃO: {conclusao}")
    log(f"      motivo: {motivo}")
    return (tipo, conclusao, motivo)


async def rodar() -> int:
    bloco("TESTE FORENSE — identidade dentro da vitrine /social/")
    log("Expansão pelo desencurtador REAL. Leitura única por página.")
    log("Nenhum endpoint é seguido. Nenhum link é gerado.")
    log()

    try:
        import config
        import globals as g
        from utils.url_resolver import desencurtar
        import aiohttp
        import random
    except Exception as exc:
        log(f"ERRO ao importar: {type(exc).__name__}: {exc}")
        return 2
    log("Import OK — módulos de produção carregados.")
    g._init_globals()
    sessao = await g._get_session()

    async def ler(url: str) -> tuple:
        """Uma leitura HTTP legítima. Devolve (html, erro)."""
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
                texto = await resp.text(errors="ignore")
                return texto[:LIMITE_HTML], ""
        except Exception as exc:
            return "", f"{type(exc).__name__}: {exc}"

    resultados = []

    # ── Alvos ─────────────────────────────────────────────────────
    bloco("ALVOS — links recebidos dos grupos")
    for i, curta in enumerate(ALVOS, 1):
        try:
            async with config._SEM_HTTP:
                final = await desencurtar(curta, sessao)
        except Exception as exc:
            final = curta
            log(f"[{i}] falha ao expandir: {exc}")

        if tipo_de(final) == "CAPTCHA":
            log("─" * 58)
            log(f"[{i}/{len(ALVOS)}] {curta}")
            log("  destino é muro de captcha — pulado, sem contorno")
            resultados.append((curta, "CAPTCHA", "NAO RECUPERAVEL"))
            continue

        html, erro = await ler(final)
        r = analisar_html(html, final) if html else {"erro": erro or "vazio"}
        if html and tipo_de(final) != "CAPTCHA" and "captcha" in html[:2000].lower():
            log("  AVISO: corpo menciona captcha nos primeiros 2000 chars")
        tipo, conc, _ = relatar(
            f"[{i}/{len(ALVOS)}]", curta, final, r
        )
        resultados.append((curta, tipo, conc))

    # ── Controle ──────────────────────────────────────────────────
    bloco("CONTROLE — lista REAL, comprovadamente afiliável")
    log("Serve para comparar a estrutura da vitrine com a da")
    log("listagem que gerou meli.la com sucesso na execução 6.")
    log()
    html_c, erro_c = await ler(CONTROLE)
    r_c = analisar_html(html_c, CONTROLE) if html_c else {"erro": erro_c}
    tipo_c, conc_c, _ = relatar("[CONTROLE]", CONTROLE, CONTROLE, r_c)

    # ── Resumo ────────────────────────────────────────────────────
    partes = ["═" * 58, "RESUMO", "═" * 58]
    for curta, tipo, conc in resultados:
        partes.append(f"{conc:26} {tipo:12} {curta}")
    partes.append(f"{conc_c:26} {tipo_c:12} [controle] lista real")
    partes.append("")
    rec = sum(1 for _, _, c in resultados if c == "RECUPERAVEL")
    par = sum(1 for _, _, c in resultados if c.startswith("PARCIAL"))
    nao = sum(1 for _, _, c in resultados if c == "NAO RECUPERAVEL")
    partes.append(f"RECUPERAVEL              : {rec}/{len(resultados)}")
    partes.append(f"PARCIALMENTE RECUPERAVEL : {par}/{len(resultados)}")
    partes.append(f"NAO RECUPERAVEL          : {nao}/{len(resultados)}")
    print("\n".join(f"[ML-FOR] {p}" for p in partes), flush=True)
    return 0


def main() -> int:
    try:
        return asyncio.run(rodar())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
