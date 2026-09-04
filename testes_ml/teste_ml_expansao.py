#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
TESTE DE EXPANSÃO E CLASSIFICAÇÃO — Mercado Livre
═══════════════════════════════════════════════════════════════════

PERGUNTA QUE ESTE TESTE RESPONDE
    Os links que chegam dos grupos monitorados apontam para onde?
    E, chegando lá, são afiliáveis?

    meli.la/XXXX  →  segue redirecionamentos  →  URL final
                  →  classifica  →  AFILIÁVEL / SUBSTITUIR / NÃO

NÃO PRECISA DE SESSÃO
    Só segue redirecionamento HTTP. Sem cookie, sem CSRF, sem
    gastar a sessão do createLink. Pode rodar quantas vezes quiser.

ISOLAMENTO
    Não importa NADA do projeto. Só biblioteca padrão. Não sobe o
    bot, não escreve em disco, não afilia nada, não altera arquivo.

REGRAS DE ELEGIBILIDADE — derivadas dos testes reais
    produto  /p/MLB…  /up/MLBU…  MLB-…      → AFILIÁVEL
    listagem lista.mercadolivre.com.br/…    → AFILIÁVEL
    vitrine  /social/<slug>                 → NÃO (error_code 111)
    /sec/                                   → SUBSTITUIR pelo próprio

    A vitrine e o /sec/ já SÃO produto de afiliação de alguém.
    O programa não afilia a loja de outra pessoa.

COMO RODAR
    ML_TEST_LINKS = um link por linha (opcional; há embutidos)
    start command : python -u testes_ml/teste_ml_expansao.py
"""
from __future__ import annotations

import gzip
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import zlib
from typing import List, Tuple

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)

TIMEOUT_S = 20
MAX_SALTOS = 8

# Links reais dos grupos monitorados. Não são segredo.
LINKS_PADRAO = """
https://meli.la/1ipL9sf
https://meli.la/26eaLSW
https://meli.la/1bZpCya
https://meli.la/2nhS3s3
https://mercadolivre.com/sec/2U6U32Q
https://www.mercadolivre.com.br/kit-5-tops-feminino-regata-alcinha-canelado-lisolistrado/up/MLBU3929091094
https://www.mercadolivre.com.br/notebook-vaio-fe16-amd-ryzen-5-5625u-windows-11-home-16gb-ram-512gb-ssd-wi-fi-6-tela-16-ips-full-hd-wuxga-cinza-grafite/p/MLB45513180
https://produto.mercadolivre.com.br/MLB-6009607732-mochila-viagem-feminina-masculina-para-notebook-executiva-_JM
"""

# ── Classificação ─────────────────────────────────────────────────
_RE_MLB = re.compile(r"/MLB[-]?(\d{5,})", re.I)
_RE_MLBU = re.compile(r"/MLBU[-]?(\d{5,})", re.I)
_RE_P_MLB = re.compile(r"/p/MLB[-]?(\d{5,})", re.I)

AFILIAVEL = "AFILIAVEL"
SUBSTITUIR = "SUBSTITUIR"
NAO_AFILIAVEL = "NAO-AFILIAVEL"
DESCONHECIDO = "DESCONHECIDO"


def log(msg: str = "") -> None:
    print(f"[ML-EXP] {msg}", flush=True)


def host_de(url: str) -> str:
    try:
        h = (urllib.parse.urlparse(url).hostname or "").lower()
        return h[4:] if h.startswith("www.") else h
    except Exception:
        return ""


def id_produto_de(path: str) -> str:
    """
    Extrai o identificador do produto, cobrindo os três formatos
    observados em links reais:
        /p/MLB45513180        catálogo comum
        /up/MLBU3929091094    catálogo unificado (prefixo MLBU)
        /MLB-6009607732-...   anúncio direto
    """
    m = _RE_MLBU.search(path)
    if m:
        return f"MLBU{m.group(1)}"
    m = _RE_P_MLB.search(path) or _RE_MLB.search(path)
    return f"MLB{m.group(1)}" if m else ""


def classificar(url: str) -> Tuple[str, str, str]:
    """Devolve (cenário, elegibilidade, identificador)."""
    host = host_de(url)
    try:
        path = urllib.parse.urlparse(url).path or "/"
    except Exception:
        return ("?", DESCONHECIDO, "")

    if "/sec/" in path.lower():
        return ("sec", SUBSTITUIR, "")

    if host == "meli.la":
        return ("encurtado", DESCONHECIDO, "")

    if "/social/" in path.lower():
        return ("vitrine", NAO_AFILIAVEL, "")

    if host == "lista.mercadolivre.com.br":
        return ("listagem", AFILIAVEL, "")

    ident = id_produto_de(path)
    if ident:
        return ("produto", AFILIAVEL, ident)

    if "mercadolivre" in host or "mercadolibre" in host:
        return ("outro-ml", DESCONHECIDO, "")

    return ("fora-do-ml", NAO_AFILIAVEL, "")


# ── Expansão ──────────────────────────────────────────────────────
class SemRedirect(urllib.request.HTTPRedirectHandler):
    """Impede o redirecionamento automático: queremos ver cada salto."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _abrir(url: str, metodo: str):
    req = urllib.request.Request(
        url, headers={"User-Agent": UA, "Accept": "*/*"}, method=metodo
    )
    opener = urllib.request.build_opener(SemRedirect)
    return opener.open(req, timeout=TIMEOUT_S)


def expandir(url: str) -> Tuple[str, List[str], str]:
    """
    Segue os redirecionamentos manualmente.
    Devolve (url_final, cadeia_de_saltos, observação).
    """
    atual = url
    cadeia: List[str] = []

    for _ in range(MAX_SALTOS):
        for metodo in ("HEAD", "GET"):
            try:
                with _abrir(atual, metodo) as r:
                    return atual, cadeia, f"{metodo} {r.status} (final)"
            except urllib.error.HTTPError as e:
                if e.code in (301, 302, 303, 307, 308):
                    destino = e.headers.get("Location") or ""
                    if not destino:
                        return atual, cadeia, f"{e.code} sem Location"
                    destino = urllib.parse.urljoin(atual, destino)
                    cadeia.append(f"{e.code} → {destino[:110]}")
                    atual = destino
                    break
                if metodo == "GET":
                    return atual, cadeia, f"HTTP {e.code}"
            except urllib.error.URLError as e:
                if metodo == "GET":
                    return atual, cadeia, f"rede: {e.reason}"
            except Exception as e:
                if metodo == "GET":
                    return atual, cadeia, f"{type(e).__name__}"
        else:
            return atual, cadeia, "sem resposta"

    return atual, cadeia, f"parou em {MAX_SALTOS} saltos"


# ── Execução ──────────────────────────────────────────────────────
def main() -> int:
    log("═" * 58)
    log("EXPANSÃO E CLASSIFICAÇÃO — links dos grupos")
    log("═" * 58)
    log("Não usa sessão. Não afilia nada. Só segue redirecionamento.")
    log()

    bruto = os.environ.get("ML_TEST_LINKS", "").strip() or LINKS_PADRAO
    links = [l.strip() for l in bruto.splitlines() if l.strip().startswith("http")]

    if not links:
        log("ERRO: nenhum link para testar.")
        return 2

    log(f"Links a processar: {len(links)}")
    origem = "ML_TEST_LINKS" if os.environ.get("ML_TEST_LINKS") else "embutidos"
    log(f"Origem da lista: {origem}")
    log()

    resumo = {AFILIAVEL: 0, SUBSTITUIR: 0, NAO_AFILIAVEL: 0, DESCONHECIDO: 0}
    detalhe = []

    for n, link in enumerate(links, 1):
        log("─" * 58)
        log(f"[{n}/{len(links)}] {link[:100]}")

        cen_in, eleg_in, _ = classificar(link)
        log(f"  entrada: cenário={cen_in} elegibilidade={eleg_in}")

        # /sec/ não precisa expandir: a regra é substituir.
        if cen_in == "sec":
            log("  → /sec/ é link de afiliado de terceiro.")
            log("  → Não converte (confirmado no gerador oficial).")
            log("  → AÇÃO: substituir pelo /sec/ próprio.")
            resumo[SUBSTITUIR] += 1
            detalhe.append((link, "sec", SUBSTITUIR, 0))
            continue

        final, cadeia, obs = expandir(link)

        if cadeia:
            log(f"  saltos: {len(cadeia)}")
            for salto in cadeia:
                log(f"    {salto}")
        else:
            log("  saltos: 0 (não é encurtador)")
        log(f"  observação: {obs}")

        cen, eleg, ident = classificar(final)
        log(f"  URL final: {final[:110]}")
        log(f"  host  = {host_de(final)}")
        log(f"  cenário = {cen}")
        if ident:
            log(f"  id_produto = {ident}")

        marca = {
            AFILIAVEL: "AFILIÁVEL ✔",
            SUBSTITUIR: "SUBSTITUIR",
            NAO_AFILIAVEL: "NÃO-AFILIÁVEL ✘",
            DESCONHECIDO: "DESCONHECIDO ?",
        }[eleg]
        log(f"  >>> {marca}")

        resumo[eleg] += 1
        detalhe.append((link, cen, eleg, len(cadeia)))

    # ── Resumo ────────────────────────────────────────────────────
    log()
    log("═" * 58)
    log("RESUMO")
    log("═" * 58)
    total = len(links)
    for chave in (AFILIAVEL, SUBSTITUIR, NAO_AFILIAVEL, DESCONHECIDO):
        qtd = resumo[chave]
        pct = (100 * qtd / total) if total else 0
        log(f"{chave:15} {qtd:3}  ({pct:5.1f}%)")
    log()

    log("Por link:")
    for link, cen, eleg, saltos in detalhe:
        log(f"  {eleg:14} {cen:10} {saltos} salto(s)  {link[:60]}")
    log()

    convertivel = resumo[AFILIAVEL]
    log(f"COBERTURA REAL: {convertivel}/{total} links geram afiliado próprio.")
    if resumo[SUBSTITUIR]:
        log(f"                {resumo[SUBSTITUIR]} viram substituição por /sec/ próprio.")
    if resumo[NAO_AFILIAVEL]:
        log(f"                {resumo[NAO_AFILIAVEL]} não têm caminho — investigar.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
