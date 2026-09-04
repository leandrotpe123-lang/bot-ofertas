#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
TESTE ISOLADO — createLink do Mercado Livre
Executável na nuvem (Railway), sessão fornecida por secret
═══════════════════════════════════════════════════════════════════

OBJETIVO DESTA VERSÃO
    Diagnóstico profundo da recusa de listas. Mostra TUDO que o
    servidor devolve (error_code, message, status, origin_url,
    estrutura completa de urls[0]) e compara DOIS bodies:

      A) o nosso body mínimo      {"urls": [...], "tag": "..."}
      B) o body OFICIAL capturado do cURL (--data-raw)

    A comparação A x B é a forma mais direta de descobrir o que o
    Mercado Livre usa de verdade para gerar link de lista: o body
    do cURL é literalmente o que a ferramenta oficial enviou.

FORA DE ESCOPO
    Playwright, renovação de sessão, cliente.py, integração com o
    pacote mercadolivre, pipeline, produção.

ISOLAMENTO
    Não importa NADA do projeto. Só biblioteca padrão. Não sobe o
    bot, não escreve em disco, não altera arquivo algum.

SEGURANÇA DE LOG
    Cookie, CSRF e User-Agent NUNCA são impressos — em nenhum
    caminho. O body do createLink é impresso porque não contém
    credencial: são apenas URLs e a tag. Ainda assim, valores longos
    (como o parâmetro 'ref') são truncados.
"""
from __future__ import annotations

import gzip
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import zlib
from http.cookies import SimpleCookie
from typing import Dict, List, Optional, Tuple


# ═══════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ═══════════════════════════════════════════════════════════════════

ENDPOINT = (
    "https://www.mercadolivre.com.br"
    "/affiliate-program/api/v2/affiliates/createLink"
)

REFERER = "https://www.mercadolivre.com.br/affiliate-program"
ORIGIN = "https://www.mercadolivre.com.br"

USER_AGENT_PADRAO = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)

COOKIES_LISTA_GUILHERME = [
    "ssid", "orguserid", "orguseridp", "orgnickp", "_csrf",
    "x-meli-session-id", "x-bf-session-v6", "_d2id",
    "_mldataSessionId", "nsa_rotok",
]

_COOKIES_SO_DE_INTERFACE = {
    "ml_affiliates_hub_visit_count",
    "ml_affiliates_orders_fraud_banner_first_shown",
    "ml_affiliates_orders_fraud_banner_action",
    "ml_affiliates_onboarding_banner_visits",
    "nav_dab_closed",
    "g_state",
}

URL_PADRAO = (
    "https://www.mercadolivre.com.br/apple-iphone-15-128-gb-preto"
    "/p/MLB1040287441"
)

TIMEOUT_S = 30

# Truncamento de valores longos no log (ex.: parâmetro 'ref').
_MAX_VALOR_LOG = 120


# ═══════════════════════════════════════════════════════════════════
# LOG
# ═══════════════════════════════════════════════════════════════════

def log(msg: str = "") -> None:
    print(f"[ML-TEST] {msg}", flush=True)


def bloco(titulo: str) -> None:
    log("═" * 58)
    log(titulo)
    log("═" * 58)


def sub(titulo: str) -> None:
    log("─" * 58)
    log(titulo)
    log("─" * 58)


def _encurtar(valor, limite: int = _MAX_VALOR_LOG) -> str:
    """Representação curta de um valor, para log legível."""
    texto = valor if isinstance(valor, str) else json.dumps(
        valor, ensure_ascii=False
    )
    if len(texto) <= limite:
        return texto
    return f"{texto[:limite]}… (+{len(texto) - limite} chars)"


def log_dict(rotulo: str, dados: dict, indent: str = "  ") -> None:
    """
    Imprime um dicionário campo a campo, com valores truncados.

    Usado para dar visibilidade TOTAL da resposta do servidor sem
    despejar um JSON gigante numa linha só.
    """
    if not isinstance(dados, dict):
        log(f"{rotulo}: (não é objeto) {_encurtar(dados)}")
        return
    log(f"{rotulo}:")
    if not dados:
        log(f"{indent}(vazio)")
        return
    for chave in sorted(dados.keys()):
        valor = dados[chave]
        if isinstance(valor, (dict, list)):
            log(f"{indent}{chave} = {_encurtar(valor, 200)}")
        else:
            log(f"{indent}{chave} = {_encurtar(valor)}")


# ═══════════════════════════════════════════════════════════════════
# LEITURA DA SESSÃO (do secret)
# ═══════════════════════════════════════════════════════════════════

def _limpar_continuacoes(texto: str) -> str:
    """Junta linhas de um cURL multilinha, tirando as barras."""
    return re.sub(r"\\\s*\n", " ", texto)


def _sanear_header(valor: str) -> str:
    """
    Remove quebras de linha e caracteres de controle de um valor de
    header. O painel de variáveis insere quebras no valor colado, e
    a biblioteca HTTP recusa o header com ValueError antes de sair.
    """
    if not valor:
        return ""
    limpo = re.sub(r"[\r\n\t]+", "", valor)
    limpo = "".join(c for c in limpo if ord(c) >= 32 or c == " ")
    return limpo.strip()


def extrair_do_curl(texto: str) -> Tuple[str, Dict[str, str]]:
    """Extrai (header_cookie, headers_extra) de um comando cURL."""
    texto = _limpar_continuacoes(texto)
    headers: Dict[str, str] = {}
    cookie = ""

    for m in re.finditer(r"""-H\s+(['"])(.*?)\1""", texto, re.S):
        linha = m.group(2)
        if ":" not in linha:
            continue
        nome, _, valor = linha.partition(":")
        nome = nome.strip()
        valor = _sanear_header(valor)
        if nome.lower() == "cookie":
            cookie = valor
        elif nome:
            headers[nome] = valor

    if not cookie:
        m = re.search(r"""(?:-b|--cookie)\s+(['"])(.*?)\1""", texto, re.S)
        if m:
            cookie = _sanear_header(m.group(2))

    return cookie, headers


def extrair_body_do_curl(texto: str) -> str:
    """
    Extrai o corpo enviado no cURL (--data-raw, --data, -d).

    Este é o BODY OFICIAL: exatamente o que a ferramenta do Mercado
    Livre enviou ao gerar o link. É a peça mais valiosa da captura —
    revela o formato real, sem suposição.

    Não contém credencial (só URLs e tag), por isso pode ser logado.
    """
    texto = _limpar_continuacoes(texto)
    padrao = r"""(?:--data-raw|--data-binary|--data|-d)\s+(['"])(.*?)\1"""
    m = re.search(padrao, texto, re.S)
    return m.group(2).strip() if m else ""


def carregar_sessao() -> Tuple[str, Dict[str, str], str]:
    """
    Lê a sessão do secret ML_TEST_CURL.
    Devolve (header_cookie, headers_extra, body_oficial).
    """
    bruto = os.environ.get("ML_TEST_CURL", "").strip()

    if not bruto:
        log("ERRO: variável ML_TEST_CURL não definida.")
        sys.exit(2)

    if "^\"" in bruto or bruto.lstrip().lower().startswith("curl.exe"):
        log("ERRO: o cURL parece estar no formato cmd do Windows.")
        log("      Recapture usando 'Copy as cURL (bash)'.")
        sys.exit(2)

    if bruto.lstrip().startswith("curl"):
        log("Formato detectado: comando cURL")
        cookie, headers = extrair_do_curl(bruto)
        return cookie, headers, extrair_body_do_curl(bruto)

    if "=" in bruto and ";" in bruto:
        log("Formato detectado: header Cookie bruto")
        return _sanear_header(bruto), {}, ""

    log("ERRO: conteúdo de ML_TEST_CURL não reconhecido.")
    log("      O valor precisa começar com a palavra 'curl'.")
    sys.exit(2)


def nomes_de_cookies(header_cookie: str) -> List[str]:
    """Nomes presentes no header Cookie. NUNCA devolve valores."""
    nomes = []
    for parte in header_cookie.split(";"):
        nome = parte.split("=", 1)[0].strip()
        if nome:
            nomes.append(nome)
    return nomes


def valor_de_cookie(header_cookie: str, alvo: str) -> str:
    """Valor de um cookie — usado só para o CSRF. Jamais impresso."""
    try:
        jar = SimpleCookie()
        jar.load(header_cookie)
        if alvo in jar:
            return jar[alvo].value
    except Exception:
        pass
    for parte in header_cookie.split(";"):
        nome, _, valor = parte.strip().partition("=")
        if nome.strip() == alvo:
            return valor.strip()
    return ""


# ═══════════════════════════════════════════════════════════════════
# INSPEÇÃO DE URL
# ═══════════════════════════════════════════════════════════════════

def matt_word_de(url: str) -> str:
    """
    Extrai o matt_word de uma URL.

    É este parâmetro que determina QUEM RECEBE A COMISSÃO — não o
    campo 'tag' da resposta, que apenas ecoa o que pedimos.
    """
    try:
        q = urllib.parse.urlparse(url).query
        valores = urllib.parse.parse_qs(q).get("matt_word") or []
        return valores[0].strip() if valores else ""
    except Exception:
        return ""


def slug_social_de(url: str) -> str:
    """Slug em /social/<slug>, ou vazio. Só para diagnóstico."""
    try:
        m = re.search(
            r"/social/([^/?#]+)", urllib.parse.urlparse(url).path or "", re.I
        )
        return m.group(1) if m else ""
    except Exception:
        return ""


def descrever_url(rotulo: str, url: str) -> None:
    """Decompõe uma URL no log: host, path e parâmetros."""
    try:
        p = urllib.parse.urlparse(url)
    except Exception:
        log(f"{rotulo}: (não parseável)")
        return
    log(f"{rotulo}:")
    log(f"  host = {p.netloc}")
    log(f"  path = {p.path}")
    params = urllib.parse.parse_qs(p.query, keep_blank_values=True)
    if params:
        for chave in sorted(params):
            log(f"  ?{chave} = {_encurtar(params[chave][0], 60)}")
    else:
        log("  (sem parâmetros)")


# ═══════════════════════════════════════════════════════════════════
# CHAMADA HTTP
# ═══════════════════════════════════════════════════════════════════

def _corpo_decodificado(resposta) -> str:
    """Lê o corpo tratando gzip/deflate."""
    bruto = resposta.read()
    codificacao = (resposta.headers.get("Content-Encoding") or "").lower()
    try:
        if codificacao == "gzip":
            bruto = gzip.decompress(bruto)
        elif codificacao == "deflate":
            bruto = zlib.decompress(bruto, -zlib.MAX_WBITS)
    except Exception:
        pass
    return bruto.decode("utf-8", errors="replace")


def postar(
    body: str,
    header_cookie: str,
    csrf: str,
    user_agent: str,
) -> Tuple[int, str, Dict[str, str]]:
    """Executa o POST com o body dado. Não imprime credencial."""
    headers = {
        "User-Agent": _sanear_header(user_agent),
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Origin": ORIGIN,
        "Referer": REFERER,
        "Cookie": _sanear_header(header_cookie),
    }
    if csrf:
        headers["x-csrf-token"] = _sanear_header(csrf)

    req = urllib.request.Request(
        ENDPOINT, data=body.encode("utf-8"), headers=headers, method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:
            return r.status, _corpo_decodificado(r), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, _corpo_decodificado(e), dict(e.headers or {})
    except urllib.error.URLError as e:
        log(f"ERRO de rede: {e.reason}")
        return -1, "", {}
    except ValueError as e:
        log(f"ERRO: header recusado pela stdlib ({str(e)[:40]})")
        return -1, "", {}


# ═══════════════════════════════════════════════════════════════════
# ANÁLISE DA RESPOSTA — DIAGNÓSTICO COMPLETO
# ═══════════════════════════════════════════════════════════════════

_RE_CURTO = re.compile(
    r"https?://(?:meli\.la/\S+|(?:www\.)?mercadolivre\.com/sec/\S+)", re.I
)


def _primeiro(dic, *chaves):
    """Primeiro valor não-vazio entre as chaves informadas."""
    if not isinstance(dic, dict):
        return None
    for chave in chaves:
        if chave in dic and dic[chave] not in (None, ""):
            return dic[chave]
    return None


def analisar(status: int, corpo: str, tag_esperada: str,
             url_enviada: str = "") -> bool:
    """
    Diagnóstico completo. Imprime TUDO que o servidor devolveu.
    Devolve True somente se gerou link válido creditando a nós.
    """
    log(f"HTTP: {status}")

    if status == -1:
        return False

    if corpo.lstrip()[:200].lower().startswith(("<!doctype", "<html")):
        log("Corpo: HTML (esperado JSON)")
        if re.search(r"login|signin|authoriz", corpo[:3000], re.I):
            log("Motivo: sessão inválida — servidor devolveu login")
        return False

    try:
        dados = json.loads(corpo)
    except json.JSONDecodeError:
        log("Corpo: não é JSON válido")
        log(f"Trecho: {corpo[:300]}")
        return False

    if not isinstance(dados, dict):
        log(f"Corpo: JSON de tipo inesperado ({type(dados).__name__})")
        return False

    # ── Envelope completo ─────────────────────────────────────────
    sub("RESPOSTA — ENVELOPE")
    envelope = {k: v for k, v in dados.items() if k != "urls"}
    log_dict("campos do topo", envelope)
    log(f"  status (envelope) = {dados.get('status')}")
    log(f"  total_items       = {dados.get('total_items')}")
    log(f"  total_success     = {dados.get('total_success')}")
    log(f"  total_error       = {dados.get('total_error')}")

    urls = dados.get("urls")
    if not isinstance(urls, list) or not urls:
        log("urls: ausente ou vazia")
        return False

    log(f"  urls: lista com {len(urls)} item(ns)")

    # ── Item completo — a informação decisiva ─────────────────────
    sub("RESPOSTA — urls[0] COMPLETO")
    item = urls[0] if isinstance(urls[0], dict) else {}
    if not item:
        log(f"urls[0] não é objeto: {_encurtar(urls[0], 300)}")
        return False

    log_dict("urls[0]", item)

    # Campos de erro, destacados.
    error_code = _primeiro(item, "error_code", "errorCode", "code")
    mensagem = _primeiro(item, "message", "msg", "error", "detail")
    status_item = item.get("status")
    origin_url = _primeiro(item, "origin_url", "originUrl")

    sub("DIAGNÓSTICO DA RECUSA")
    log(f"error_code = {error_code if error_code is not None else '(ausente)'}")
    log(f"message    = {mensagem if mensagem is not None else '(ausente)'}")
    log(f"status     = {status_item if status_item is not None else '(ausente)'}")
    if origin_url:
        descrever_url("origin_url devolvida", str(origin_url))

    if error_code is not None or mensagem is not None:
        log("")
        log(">>> ESTA É A RAZÃO DA RECUSA. Copie estas 3 linhas. <<<")

    # ── Sucesso? ──────────────────────────────────────────────────
    criado = _primeiro(item, "created", "is_created", "isCreated")
    curto = (
        _primeiro(item, "short_url", "shortUrl", "shortURL")
        or _primeiro(dados, "short_url", "shortUrl", "shortURL")
        or ""
    )
    if not isinstance(curto, str) or not _RE_CURTO.search(curto):
        achado = _RE_CURTO.search(corpo)
        curto = achado.group(0) if achado else ""

    if not curto:
        log("")
        log("Resultado: nenhum link curto gerado.")
        return False

    sub("RESULTADO")
    log(f"created   = {criado}")
    log(f"short_url = {curto}")
    if "/sec/" in curto:
        log("FORMATO: mercadolivre.com/sec/")
    elif "meli.la" in curto:
        log("FORMATO: meli.la")

    tag_resposta = _primeiro(item, "tag", "affiliate_tag", "affiliateTag") or ""
    tag_ok = bool(tag_resposta) and tag_resposta == tag_esperada
    log(f"tag validada: {'SIM' if tag_ok else 'NAO'} "
        f"(esperada={tag_esperada} recebida={tag_resposta or '(ausente)'})")

    # ── Quem recebe a comissão — a validação que vale ─────────────
    longa = _primeiro(item, "long_url", "longUrl")
    dono_ok = True
    if longa:
        longa = str(longa)
        descrever_url("long_url devolvida", longa)
        dono = matt_word_de(longa)
        if dono:
            dono_ok = dono == tag_esperada
            log(f"matt_word na long_url: {dono} "
                f"{'(NOSSA)' if dono_ok else '(DE TERCEIRO)'}")
            if not dono_ok:
                log("FALHA GRAVE: o link gerado credita OUTRO afiliado.")
            dono_in = matt_word_de(url_enviada) if url_enviada else ""
            if dono_in and dono and dono != dono_in:
                log(f"=> A API SUBSTITUIU a identidade ({dono_in} → {dono}).")
        else:
            log("matt_word na long_url: (ausente)")
    else:
        log("long_url ausente — não dá para verificar quem é creditado")

    return bool(criado is not False) and tag_ok and dono_ok


# ═══════════════════════════════════════════════════════════════════
# EXECUÇÃO
# ═══════════════════════════════════════════════════════════════════

def main() -> int:
    bloco("TESTE createLink — DIAGNÓSTICO DE LISTA")
    log("Não sobe o bot. Não toca nenhum módulo do projeto.")
    log()

    tag = os.environ.get("ML_TAG", "").strip()
    if not tag:
        log("ERRO: variável ML_TAG não definida.")
        return 2

    url_teste = (
        (sys.argv[1] if len(sys.argv) > 1 else "").strip()
        or os.environ.get("ML_TEST_URL", "").strip()
        or URL_PADRAO
    )

    log(f"Tag configurada: {tag}")
    descrever_url("URL de teste (ML_TEST_URL)", url_teste)

    dono_entrada = matt_word_de(url_teste)
    if dono_entrada:
        log(f"matt_word na entrada: {dono_entrada} "
            f"{'(nossa)' if dono_entrada == tag else '(DE TERCEIRO)'}")
    log()

    # ── Sessão ────────────────────────────────────────────────────
    header_cookie, headers_extra, body_oficial = carregar_sessao()
    if not header_cookie:
        log("ERRO: nenhum cookie encontrado em ML_TEST_CURL.")
        return 2
    log("Sessão carregada")

    presentes = nomes_de_cookies(header_cookie)
    achados = [c for c in COOKIES_LISTA_GUILHERME if c in presentes]
    faltando = [c for c in COOKIES_LISTA_GUILHERME if c not in presentes]
    log(f"Cookies da lista do Guilherme: {len(achados)}/"
        f"{len(COOKIES_LISTA_GUILHERME)} | total enviado: {len(presentes)}")
    if faltando:
        log(f"Da lista, não presentes: {', '.join(faltando)}")

    if set(presentes) <= _COOKIES_SO_DE_INTERFACE | {"_d2id"}:
        log("ALERTA: só cookies de interface. cURL de outra requisição.")

    minusculos = {k.lower(): v for k, v in headers_extra.items()}
    csrf = (
        _sanear_header(os.environ.get("ML_CSRF", ""))
        or minusculos.get("x-csrf-token", "")
        or valor_de_cookie(header_cookie, "_csrf")
    )
    log(f"CSRF encontrado: {'SIM' if csrf else 'NAO'}")

    user_agent = (
        _sanear_header(os.environ.get("ML_USER_AGENT", ""))
        or minusculos.get("user-agent", "")
        or USER_AGENT_PADRAO
    )
    log(f"User-Agent: {'do cURL' if minusculos.get('user-agent') else 'padrão'}")
    log(f"Headers presentes no cURL: {', '.join(sorted(minusculos)) or '(nenhum)'}")
    log()

    # ── BODY OFICIAL capturado ────────────────────────────────────
    bloco("BODY OFICIAL — o que o Mercado Livre enviou")
    if body_oficial:
        log(f"Tamanho: {len(body_oficial)} chars")
        try:
            oficial = json.loads(body_oficial)
            log_dict("campos do body oficial", oficial)
            urls_of = oficial.get("urls")
            if isinstance(urls_of, list):
                for i, u in enumerate(urls_of[:3]):
                    descrever_url(f"  urls[{i}] do body oficial", str(u))
        except json.JSONDecodeError:
            log(f"Body não é JSON: {_encurtar(body_oficial, 300)}")
    else:
        log("Nenhum body encontrado no cURL (--data-raw ausente).")
        log("Isso indica que o cURL capturado NÃO é de um POST")
        log("createLink — provavelmente é um GET de página.")
    log()

    # ── CHAMADA A: nosso body mínimo ──────────────────────────────
    bloco("CHAMADA A — nosso body mínimo")
    body_a = json.dumps({"urls": [url_teste], "tag": tag})
    log(f"body enviado: {_encurtar(body_a, 200)}")
    log()
    status_a, corpo_a, headers_a = postar(
        body_a, header_cookie, csrf, user_agent
    )
    if headers_a.get("Set-Cookie"):
        log("Servidor devolveu Set-Cookie")
    ok_a = analisar(status_a, corpo_a, tag, url_teste)
    log()
    log(f"CHAMADA A: {'APROVADA' if ok_a else 'REPROVADA'}")
    log()

    # ── CHAMADA B: body oficial do cURL ───────────────────────────
    ok_b = False
    if body_oficial and body_oficial.strip() != body_a.strip():
        bloco("CHAMADA B — body OFICIAL do cURL (réplica exata)")
        log("Se A falhar e B passar, a diferença está no BODY,")
        log("não na sessão. É o que queremos descobrir.")
        log()
        status_b, corpo_b, headers_b = postar(
            body_oficial, header_cookie, csrf, user_agent
        )
        ok_b = analisar(status_b, corpo_b, tag)
        log()
        log(f"CHAMADA B: {'APROVADA' if ok_b else 'REPROVADA'}")
    elif body_oficial:
        log("CHAMADA B dispensada: body oficial idêntico ao nosso.")
    log()

    # ── Veredito ──────────────────────────────────────────────────
    bloco("VEREDITO")
    log(f"A (nosso body mínimo) : {'APROVADA' if ok_a else 'REPROVADA'}")
    if body_oficial:
        log(f"B (body oficial)      : {'APROVADA' if ok_b else 'REPROVADA'}")
    log()
    if ok_a:
        log("O nosso formato funciona para esta URL.")
    elif ok_b:
        log("O body oficial funciona e o nosso NÃO.")
        log("=> A diferença está no formato do body. Compare acima")
        log("   os campos do body oficial com os nossos.")
    else:
        log("Nenhum dos dois passou. A causa está em 'DIAGNÓSTICO DA")
        log("RECUSA' acima — error_code e message.")
    log()
    log("Lembre de APAGAR ML_TEST_CURL após o teste.")

    return 0 if (ok_a or ok_b) else 1


if __name__ == "__main__":
    sys.exit(main())
