#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
TESTE ISOLADO — o expandidor REAL do Foguetão vs. Mercado Livre
═══════════════════════════════════════════════════════════════════

PERGUNTA ÚNICA
    O `desencurtar` que já roda em produção consegue expandir links
    curtos do Mercado Livre, ou cai no muro de captcha?

    O teste anterior (teste_ml_expansao.py) usava um expandidor
    PRÓPRIO, escrito para o teste, e recebeu:

        302 → /captcha/wall

    Este aqui NÃO reimplementa nada: importa e chama
    `utils.url_resolver.desencurtar`, o mesmo código que o Foguetão
    usa hoje para Shopee, Amazon e Magalu.

POR QUE O RESULTADO PODE SER DIFERENTE
    O expandidor de produção faz coisas que o teste caseiro não fazia:
      - User-Agent sorteado de config.USER_AGENTS a cada chamada
      - Accept e Accept-Language pt-BR
      - allow_redirects do aiohttp (segue a cadeia internamente)
      - fallback GET quando HEAD falha
      - fallback por meta-refresh, JavaScript, og:url e canonical
      - cache em memória

    Qualquer um desses pode ser a diferença entre passar e cair no
    muro.

O QUE ESTE TESTE NÃO FAZ
    Não cria expandidor. Não usa Playwright nem navegador. Não chama
    createLink. Não altera produção, Core, cupons, dedupe ou
    publicação. Não escreve em disco. Não imprime credencial.

COMO RODAR
    start command: python -u testes_ml/teste_ml_desencurtador.py
    ML_TEST_LINKS = um link por linha (opcional; há embutidos)
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
import time
import urllib.parse

# Raiz do projeto no path — o teste vive em testes_ml/.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LINKS_PADRAO = """
https://meli.la/1ipL9sf
https://meli.la/26eaLSW
https://meli.la/1bZpCya
https://meli.la/2nhS3s3
https://mercadolivre.com/sec/2U6U32Q
"""


def log(msg: str = "") -> None:
    print(f"[ML-DESENC] {msg}", flush=True)


def bloco(t: str) -> None:
    log("═" * 58)
    log(t)
    log("═" * 58)


# ── Classificação do destino ──────────────────────────────────────
_RE_MLBU = re.compile(r"/MLBU[-]?(\d{5,})", re.I)
_RE_P_MLB = re.compile(r"/p/MLB[-]?(\d{5,})", re.I)
_RE_MLB = re.compile(r"/MLB[-]?(\d{5,})", re.I)


def host_de(url: str) -> str:
    try:
        h = (urllib.parse.urlparse(url).hostname or "").lower()
        return h[4:] if h.startswith("www.") else h
    except Exception:
        return ""


def classificar(url: str) -> str:
    """PRODUTO / LISTA / SEC / SOCIAL / CAPTCHA / OUTRO."""
    host = host_de(url)
    try:
        path = urllib.parse.urlparse(url).path or "/"
    except Exception:
        return "OUTRO"

    if "/captcha" in path.lower():
        return "CAPTCHA"
    if "/sec/" in path.lower():
        return "SEC"
    if "/social/" in path.lower():
        return "SOCIAL"
    if host == "lista.mercadolivre.com.br":
        return "LISTA"
    if host == "meli.la":
        return "NAO RESOLVIDO"
    if _RE_MLBU.search(path) or _RE_P_MLB.search(path) or _RE_MLB.search(path):
        return "PRODUTO"
    if "mercadolivre" in host or "mercadolibre" in host:
        return "OUTRO"
    return "OUTRO"


def identificador(url: str) -> str:
    try:
        path = urllib.parse.urlparse(url).path or ""
    except Exception:
        return ""
    m = _RE_MLBU.search(path)
    if m:
        return f"MLBU{m.group(1)}"
    m = _RE_P_MLB.search(path) or _RE_MLB.search(path)
    return f"MLB{m.group(1)}" if m else ""


AFILIAVEL_POR_CLASSE = {
    "PRODUTO": "SIM",
    "LISTA": "SIM",
    "SEC": "NAO — já é rota afiliada de terceiro; usar fallback próprio",
    "SOCIAL": "NAO — vitrine de afiliado; createLink recusa (111)",
    "CAPTCHA": "NAO — expansão bloqueada, destino desconhecido",
    "NAO RESOLVIDO": "NAO — não expandiu",
    "OUTRO": "INDEFINIDO — destino não mapeado",
}


# ── Execução ──────────────────────────────────────────────────────
async def rodar() -> int:
    bloco("EXPANDIDOR REAL DO FOGUETÃO vs. MERCADO LIVRE")
    log("Usa utils.url_resolver.desencurtar — o mesmo de produção.")
    log("Sem Playwright. Sem navegador. Sem createLink.")
    log()

    # ── Importa o código REAL ─────────────────────────────────────
    log("Importando módulos de produção...")
    try:
        import globals as g
        from utils.url_resolver import (
            _PROFUNDIDADE_MAX,
            _TIMEOUT_GET,
            _TIMEOUT_HEAD,
            _compor_encurtadores,
            _compor_forca_get,
            desencurtar,
        )
        import config
    except Exception as exc:
        log(f"ERRO ao importar: {type(exc).__name__}: {exc}")
        return 2
    log("Import OK — expandidor de produção carregado.")

    # Semáforos vivem no config e só existem dentro do loop.
    g._init_globals()

    log()
    log("Configuração efetiva do expandidor:")
    log(f"  profundidade máxima : {_PROFUNDIDADE_MAX}")
    log(f"  timeout HEAD        : {_TIMEOUT_HEAD}s")
    log(f"  timeout GET         : {_TIMEOUT_GET}s")
    log(f"  User-Agents no pool : {len(config.USER_AGENTS)}")

    try:
        forca_get = _compor_forca_get()
        log(f"  hosts que exigem GET: {len(forca_get)} → "
            f"{', '.join(sorted(forca_get)) or '(nenhum)'}")
    except Exception as exc:
        log(f"  hosts que exigem GET: falha ({exc})")

    try:
        encurtadores = _compor_encurtadores()
        if encurtadores is None:
            log("  encurtadores declarados: COMPOSIÇÃO FALHOU (None)")
        else:
            log(f"  encurtadores declarados: {len(encurtadores)} → "
                f"{', '.join(sorted(encurtadores)) or '(nenhum)'}")
            if "meli.la" in encurtadores:
                log("  → meli.la ESTÁ declarado como encurtador ✔")
            else:
                log("  → meli.la NÃO está declarado como encurtador ✘")
                log("    (a resolução para no primeiro destino)")
    except Exception as exc:
        log(f"  encurtadores declarados: falha ({exc})")
    log()

    bruto = os.environ.get("ML_TEST_LINKS", "").strip() or LINKS_PADRAO
    links = [l.strip() for l in bruto.splitlines()
             if l.strip().startswith("http")]
    origem = "ML_TEST_LINKS" if os.environ.get("ML_TEST_LINKS") else "embutidos"
    log(f"Links: {len(links)} (origem: {origem})")
    log()

    sessao = await g._get_session()
    linhas = []

    for n, link in enumerate(links, 1):
        log("─" * 58)
        log(f"[{n}/{len(links)}] recebida: {link}")
        log(f"  host de entrada : {host_de(link)}")
        log(f"  classe entrada  : {classificar(link)}")

        t0 = time.monotonic()
        erro = ""
        try:
            async with config._SEM_HTTP:
                final = await desencurtar(link, sessao)
        except Exception as exc:
            final = link
            erro = f"{type(exc).__name__}: {exc}"
        ms = (time.monotonic() - t0) * 1000

        expandiu = final != link
        classe = classificar(final)
        ident = identificador(final)

        log(f"  tempo           : {ms:.0f}ms")
        log(f"  expandiu        : {'SIM' if expandiu else 'NÃO'}")
        if erro:
            log(f"  ERRO            : {erro}")
        log(f"  URL final       : {final[:150]}")
        log(f"  host final      : {host_de(final)}")
        log(f"  classificação   : {classe}")
        if ident:
            log(f"  identificador   : {ident}")
        log(f"  afiliável       : {AFILIAVEL_POR_CLASSE.get(classe, '?')}")

        if classe == "CAPTCHA":
            log("  >>> MURO DE CAPTCHA — mesmo resultado do teste anterior")
        elif not expandiu and classe == "NAO RESOLVIDO":
            log("  >>> NÃO RESOLVEU — devolveu a URL recebida")
        elif expandiu:
            log("  >>> RESOLVEU")

        linhas.append((link, expandiu, classe, ident, ms, erro))

    # ── Resumo, escrito de uma vez só ─────────────────────────────
    partes = []
    partes.append("═" * 58)
    partes.append("RESUMO")
    partes.append("═" * 58)
    for link, expandiu, classe, ident, ms, erro in linhas:
        marca = "OK " if expandiu else "-- "
        partes.append(
            f"{marca} {classe:14} {ident:14} {ms:6.0f}ms  {link[:46]}"
        )
    partes.append("")

    total = len(linhas)
    resolvidos = sum(1 for l in linhas if l[1])
    captcha = sum(1 for l in linhas if l[2] == "CAPTCHA")
    afiliaveis = sum(1 for l in linhas if l[2] in ("PRODUTO", "LISTA"))

    partes.append(f"Expandidos    : {resolvidos}/{total}")
    partes.append(f"Captcha       : {captcha}/{total}")
    partes.append(f"Afiliáveis    : {afiliaveis}/{total}")
    partes.append("")
    if captcha:
        partes.append("VEREDITO: o expandidor de produção TAMBÉM cai no muro.")
    elif resolvidos:
        partes.append("VEREDITO: o expandidor de produção RESOLVE o ML.")
        partes.append("          O fluxo meli.la → URL real → classificar")
        partes.append("          → createLink é viável sem workaround.")
    else:
        partes.append("VEREDITO: não expandiu, e não foi captcha. Ver erros.")

    print("\n".join(f"[ML-DESENC] {p}" for p in partes), flush=True)
    return 0


def main() -> int:
    try:
        return asyncio.run(rodar())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
