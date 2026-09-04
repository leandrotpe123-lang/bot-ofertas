#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
TESTE ISOLADO — createLink do Mercado Livre
Executável na nuvem (Railway), sessão fornecida por secret
═══════════════════════════════════════════════════════════════════

PERGUNTA ÚNICA QUE ESTE TESTE RESPONDE
    Com uma sessão real, o POST HTTP para createLink devolve o link
    curto de afiliado com a MINHA tag?

        sessão real → Cookie + CSRF → POST createLink
                    → HTTP 200 → short_url → tag = minha tag

FORA DE ESCOPO (deliberadamente)
    Playwright, renovação de sessão, integração com o pacote
    mercadolivre, cache, pipeline, monitoramento de grupos.

ISOLAMENTO — GARANTIAS DESTE ARQUIVO
    - Não importa NADA do projeto. Só biblioteca padrão do Python.
    - Não é importado por nenhum módulo de produção.
    - Não inicia Telethon, banco, orchestrator nem monitoramento.
    - Não escreve em disco. Não altera nenhum arquivo.
    - A pasta testes_ml/ NÃO tem __init__.py de propósito: assim
      não é pacote e não pode ser importada por engano.

SEGURANÇA DE LOG
    Nenhum valor de cookie, token, credencial ou header sensível é
    impresso — em nenhum caminho, nem em caso de erro. O log mostra
    apenas nomes de cookies, contagens, status HTTP, e o link
    resultante.

───────────────────────────────────────────────────────────────────
COMO EXECUTAR NA RAILWAY
───────────────────────────────────────────────────────────────────

PASSO 1 — Capturar a sessão (num navegador logado)
    a) Faça login no Mercado Livre.
    b) Abra uma página de produto com a barra de Afiliados visível.
    c) F12 → aba Network.
    d) Clique em "Compartilhar" e gere um link normalmente.
    e) Na lista de requisições, ache  createLink
       (tem que ser ESSA requisição, não outra da página)
    f) Botão direito → Copy → **Copy as cURL (bash)**
       (no Windows, escolha a opção "bash", não "cmd")

PASSO 2 — Criar as variáveis na Railway
    ML_TEST_CURL   = (cole o cURL inteiro, várias linhas, tudo bem)
    ML_TAG         = sua_etiqueta_de_afiliado
    ML_TEST_URL    = https://www.mercadolivre.com.br/... (URL LONGA)

PASSO 3 — Rodar UMA vez, sem subir o bot
    Troque o start command do serviço para:

        python -u testes_ml/teste_ml_createlink.py

    ATENÇÃO: 'redeploy' reaproveita o snapshot de configuração do
    build anterior. Para a troca valer, é preciso um deploy NOVO
    (push). Depois do teste, devolva o comando original.

PASSO 4 — Depois do teste
    Apague ML_TEST_CURL. É credencial de sessão.

───────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import gzip
import json
import os
import re
import sys
import urllib.error
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

# Lista indicada pelo Guilherme. Usada APENAS para diagnóstico.
COOKIES_LISTA_GUILHERME = [
    "ssid", "orguserid", "orguseridp", "orgnickp", "_csrf",
    "x-meli-session-id", "x-bf-session-v6", "_d2id",
    "_mldataSessionId", "nsa_rotok",
]

# Cookies que indicam captura da requisição ERRADA: são de banner e
# telemetria da interface, presentes mesmo sem sessão de afiliado.
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


# ═══════════════════════════════════════════════════════════════════
# LOG — nunca imprime valor sensível
# ═══════════════════════════════════════════════════════════════════

def log(msg: str) -> None:
    print(f"[ML-TEST] {msg}", flush=True)


def bloco(titulo: str) -> None:
    log("─" * 55)
    log(titulo)
    log("─" * 55)


# ═══════════════════════════════════════════════════════════════════
# LEITURA DA SESSÃO (do secret)
# ═══════════════════════════════════════════════════════════════════

def _limpar_continuacoes(texto: str) -> str:
    """Junta linhas de um cURL multilinha, tirando as barras."""
    return re.sub(r"\\\s*\n", " ", texto)


def _sanear_header(valor: str) -> str:
    """
    Remove quebras de linha e caracteres de controle de um valor de
    header.

    Necessário porque o painel de variáveis pode inserir quebras
    dentro do valor colado. Um header com \\n é recusado pela
    biblioteca HTTP com ValueError ANTES da requisição sair — falha
    de transporte que não diz nada sobre a sessão e mascara o
    resultado real do teste.

    Só remove controle e espaço em volta; não altera o conteúdo.
    """
    if not valor:
        return ""
    limpo = re.sub(r"[\r\n\t]+", "", valor)
    limpo = "".join(c for c in limpo if ord(c) >= 32 or c == " ")
    return limpo.strip()


def extrair_do_curl(texto: str) -> Tuple[str, Dict[str, str]]:
    """
    Extrai (header_cookie, headers_extra) de um comando cURL.

    Aceita -H 'Nome: valor', -H "Nome: valor", -b e --cookie.
    Todo valor passa por _sanear_header.
    """
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


def carregar_sessao() -> Tuple[str, Dict[str, str]]:
    """Lê a sessão do secret ML_TEST_CURL."""
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
        return extrair_do_curl(bruto)

    if "=" in bruto and ";" in bruto:
        log("Formato detectado: header Cookie bruto")
        return _sanear_header(bruto), {}

    log("ERRO: conteúdo de ML_TEST_CURL não reconhecido.")
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


def chamar_create_link(
    url_produto: str,
    tag: str,
    header_cookie: str,
    csrf: str,
    user_agent: str,
) -> Tuple[int, str, Dict[str, str]]:
    """
    Executa o POST. Devolve (status, corpo, headers_da_resposta).

    Body mínimo: {"urls": ["URL_LONGA"], "tag": "MINHA_TAG"}
    """
    corpo = json.dumps({"urls": [url_produto], "tag": tag}).encode("utf-8")

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
        ENDPOINT, data=corpo, headers=headers, method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:
            return r.status, _corpo_decodificado(r), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, _corpo_decodificado(e), dict(e.headers or {})
    except urllib.error.URLError as e:
        log(f"ERRO de rede: {e.reason}")
        sys.exit(3)
    except ValueError as e:
        # Header recusado pela stdlib. Não vaza o valor: só o motivo.
        log(f"ERRO: header recusado pela biblioteca HTTP ({e.args[0][:40]})")
        log("      Provável quebra de linha dentro do valor colado.")
        sys.exit(3)


# ═══════════════════════════════════════════════════════════════════
# ANÁLISE DA RESPOSTA
# ═══════════════════════════════════════════════════════════════════

_RE_CURTO = re.compile(
    r"https?://(?:meli\.la/\S+|(?:www\.)?mercadolivre\.com/sec/\S+)", re.I
)


def _primeiro(dic, *chaves):
    """
    Primeiro valor não-vazio entre as chaves informadas.

    Existe para que diferença de nomenclatura (short_url vs
    shortUrl) nunca produza falso negativo.
    """
    if not isinstance(dic, dict):
        return None
    for chave in chaves:
        if chave in dic and dic[chave] not in (None, ""):
            return dic[chave]
    return None


def _localizar_item(dados: dict) -> Tuple[dict, str]:
    """
    Localiza o objeto que carrega o resultado, tolerando variações
    de envelope. Devolve (item, descrição_da_estrutura).
    """
    envelopes = [dados]
    if isinstance(dados.get("data"), dict):
        envelopes.append(dados["data"])

    for envelope in envelopes:
        lista = _primeiro(envelope, "urls", "links", "results")
        if isinstance(lista, list) and lista:
            primeiro = lista[0]
            onde = "urls[0]" if envelope is dados else "data.urls[0]"
            if isinstance(primeiro, dict):
                return primeiro, f"{onde} (objeto)"
            if isinstance(primeiro, str):
                return {"short_url": primeiro}, f"{onde} (string)"

    if _primeiro(dados, "short_url", "shortUrl", "shortURL"):
        return dados, "campos no topo da resposta"

    return {}, ""


def analisar(status: int, corpo: str, tag_esperada: str) -> bool:
    """
    Interpreta a resposta e imprime o diagnóstico.
    Devolve True somente se o teste passou por completo.
    """
    log(f"HTTP: {status}")

    if corpo.lstrip()[:200].lower().startswith(("<!doctype", "<html")):
        log("Corpo: HTML (esperado JSON)")
        if re.search(r"login|signin|authoriz", corpo[:3000], re.I):
            log("Motivo: sessão inválida — servidor devolveu tela de login")
        else:
            log("Motivo: resposta HTML inesperada")
        return False

    try:
        dados = json.loads(corpo)
    except json.JSONDecodeError:
        log("Corpo: não é JSON válido")
        log(f"Trecho: {corpo[:200]}")
        return False

    if not isinstance(dados, dict):
        log(f"Corpo: JSON de tipo inesperado ({type(dados).__name__})")
        return False

    if status in (401, 403):
        log("Motivo: falha de autenticação/CSRF")
        msg = _primeiro(dados, "message", "error", "msg")
        if msg:
            log(f"Mensagem do servidor: {str(msg)[:160]}")
        return False

    if status >= 400:
        msg = _primeiro(dados, "message", "error", "msg") or ""
        log(f"Motivo: erro do servidor — {str(msg)[:160]}")
        if "csrf" in corpo.lower():
            log("Indício: menção a CSRF na resposta")
        return False

    total_sucesso = _primeiro(dados, "total_success", "totalSuccess")
    total_erro = _primeiro(dados, "total_error", "totalError")
    if total_sucesso is not None:
        log(f"total_success: {total_sucesso} | total_error: {total_erro}")

    item, origem = _localizar_item(dados)
    if origem:
        log(f"Estrutura da resposta: {origem}")

    criado = _primeiro(item, "created", "is_created", "isCreated")
    log(f"created: {criado}")

    curto = (
        _primeiro(item, "short_url", "shortUrl", "shortURL", "url")
        or _primeiro(dados, "short_url", "shortUrl", "shortURL")
        or ""
    )
    if not isinstance(curto, str) or not _RE_CURTO.search(curto):
        achado = _RE_CURTO.search(corpo)
        if achado:
            if curto:
                log("AVISO: link obtido por varredura do corpo, não por chave")
            curto = achado.group(0)
        elif not isinstance(curto, str):
            curto = ""

    # Tag — busca tolerante para não reportar "ausente" por
    # nomenclatura. A COMPARAÇÃO é estrita: é a barreira que impede
    # aceitar link de outro afiliado.
    tag_resposta = (
        _primeiro(item, "tag", "affiliate_tag", "affiliateTag")
        or _primeiro(dados, "tag", "affiliate_tag", "affiliateTag")
        or ""
    )
    tag_ok = bool(tag_resposta) and tag_resposta == tag_esperada
    log(f"tag validada: {'SIM' if tag_ok else 'NAO'}")
    if not tag_ok:
        log(f"  tag esperada : {tag_esperada}")
        log(f"  tag recebida : {tag_resposta or '(ausente)'}")

    if not curto:
        log("Motivo: resposta sem link curto reconhecível")
        log(f"Chaves no topo: {sorted(dados.keys())}")
        if item:
            log(f"Chaves no item: {sorted(item.keys())}")
        return False

    log(f"short_url: {curto}")

    if "/sec/" in curto:
        log("FORMATO: mercadolivre.com/sec/  (formato oficial atual)")
    elif "meli.la" in curto:
        log("FORMATO: meli.la")

    longa = _primeiro(item, "long_url", "longUrl")
    if longa:
        log(f"long_url presente: SIM ({len(str(longa))} chars)")

    return bool(criado is not False) and tag_ok


# ═══════════════════════════════════════════════════════════════════
# EXECUÇÃO
# ═══════════════════════════════════════════════════════════════════

def main() -> int:
    bloco("TESTE createLink — Mercado Livre")
    log("Iniciando teste")
    log("Este teste NÃO sobe o bot e NÃO toca nenhum módulo do projeto.")

    tag = os.environ.get("ML_TAG", "").strip()
    if not tag:
        log("ERRO: variável ML_TAG não definida.")
        return 2

    url_produto = (
        (sys.argv[1] if len(sys.argv) > 1 else "").strip()
        or os.environ.get("ML_TEST_URL", "").strip()
        or URL_PADRAO
    )

    log(f"Tag configurada: {tag}")
    log(f"URL de teste: {url_produto[:90]}")
    if url_produto == URL_PADRAO:
        log("AVISO: usando a URL padrão embutida.")
    if "meli.la" in url_produto or "/sec/" in url_produto:
        log("AVISO: URL encurtada. O body espera a URL LONGA.")

    # ── Sessão ────────────────────────────────────────────────────
    header_cookie, headers_extra = carregar_sessao()
    if not header_cookie:
        log("ERRO: nenhum cookie encontrado em ML_TEST_CURL.")
        return 2
    log("Sessão carregada")

    presentes = nomes_de_cookies(header_cookie)
    achados = [c for c in COOKIES_LISTA_GUILHERME if c in presentes]
    faltando = [c for c in COOKIES_LISTA_GUILHERME if c not in presentes]

    log(
        f"Cookies da lista do Guilherme presentes na sessão: "
        f"{len(achados)}/{len(COOKIES_LISTA_GUILHERME)}"
    )
    log(f"Total de cookies enviados na requisição: {len(presentes)}")
    if faltando:
        log(f"Da lista, não presentes: {', '.join(faltando)}")
    log(
        "NOTA: esta contagem é apenas informativa. Ela NÃO indica "
        "quantos cookies são necessários."
    )
    log(
        "      A requisição envia o header Cookie INTEIRO da sessão, "
        "não apenas os da lista."
    )
    log(
        "      Descobrir o conjunto realmente exigido é etapa "
        "posterior, por eliminação."
    )

    # Detecção de captura errada: só cookies de banner/telemetria.
    so_interface = set(presentes) <= _COOKIES_SO_DE_INTERFACE | {"_d2id"}
    if so_interface:
        log("")
        log("ALERTA: a sessão só contém cookies de interface (banners,")
        log("        contadores). Nenhum cookie de sessão autenticada.")
        log("        O cURL provavelmente veio de OUTRA requisição.")
        log("        Recapture a requisição chamada 'createLink'.")

    # ── CSRF ──────────────────────────────────────────────────────
    minusculos = {k.lower(): v for k, v in headers_extra.items()}
    csrf = (
        _sanear_header(os.environ.get("ML_CSRF", ""))
        or minusculos.get("x-csrf-token", "")
        or valor_de_cookie(header_cookie, "_csrf")
    )
    log(f"CSRF encontrado: {'SIM' if csrf else 'NAO'}")
    if not csrf:
        log("AVISO: sem x-csrf-token. A chamada provavelmente será")
        log("       recusada. Confira se o cURL é o da createLink.")

    user_agent = (
        _sanear_header(os.environ.get("ML_USER_AGENT", ""))
        or minusculos.get("user-agent", "")
        or USER_AGENT_PADRAO
    )
    origem_ua = "do cURL" if minusculos.get("user-agent") else "padrão do teste"
    log(f"User-Agent: {origem_ua}")

    # ── Chamada ───────────────────────────────────────────────────
    bloco("Enviando createLink...")
    status, corpo, resp_headers = chamar_create_link(
        url_produto, tag, header_cookie, csrf, user_agent
    )

    if resp_headers.get("Set-Cookie"):
        log("Servidor devolveu Set-Cookie (sessão pode ter sido renovada)")

    aprovado = analisar(status, corpo, tag)

    bloco("TESTE APROVADO" if aprovado else "TESTE REPROVADO")

    if not aprovado:
        log("Envie estas linhas [ML-TEST] para diagnóstico.")
        log("NUNCA envie o conteúdo de ML_TEST_CURL.")
        log("Corpo da resposta (200 primeiros caracteres):")
        log(f"  {corpo[:200]}")
    else:
        log("Lembre de APAGAR a variável ML_TEST_CURL após o teste.")

    return 0 if aprovado else 1


if __name__ == "__main__":
    sys.exit(main())
