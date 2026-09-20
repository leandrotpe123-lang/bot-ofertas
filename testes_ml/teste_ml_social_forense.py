"""
Sonda FORENSE da página /social — inventário bruto, sem heurística.

NÃO escolhe produto por heurística. NÃO usa o título da mensagem do
grupo. NÃO assume que a primeira âncora é a certa. NÃO assume que
`reco_item_pos=0` basta. NÃO usa similaridade textual, preço, nem
posição como prova.

Responde UMA pergunta:

    QUAIS produtos/listas existem, e QUAIS METADADOS o próprio
    Mercado Livre fornece para diferenciá-los?

═══════════════════════════════════════════════════════════════════
A PISTA QUE ORIENTA
═══════════════════════════════════════════════════════════════════
A rodada 3 revelou, no bloco inline:

    "client":"home_affiliate-profile","lazy":true,
    "subtitle":{"text":"Mostrar mais","url":"…_Container_…"},
    "highlight_seeds":false,"is_polycard":true,"slides_per_view":"5"

`highlight_seeds` é terminologia de motor de recomendação: SEED é o
item de ORIGEM, o que gerou as recomendações. Se o payload conhece
`highlight_seeds`, é provável que conheça `seeds`.

Se `seeds` existir e apontar para um item, esse é o campo explícito
que identifica o principal — e cai a necessidade de heurística.
Se não existir, esta sonda declara isso com todas as letras.

═══════════════════════════════════════════════════════════════════
MÉTODO
═══════════════════════════════════════════════════════════════════
O bloco inline é JS, não JSON puro — na rodada 3 ele não parseou.
Então aqui:

  1. imprime a CABEÇA de cada bloco, para revelar o formato do
     invólucro (assignment, JSON.parse, __next_f.push, etc.);
  2. tenta várias estratégias de extração, em ordem;
  3. como rede de segurança, faz varredura de CHAVES BALANCEADAS
     ancorada em marcadores — acha o objeto mesmo sem saber o
     invólucro.

Depois caminha a estrutura e emite um registro POR CANDIDATO, com
tudo que o Mercado Livre anexou àquele item.

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
from typing import Any, Optional

import aiohttp


_PADRAO = (
    "https://meli.la/1FE6ohY :: PRODUTO Monitor AOC|"
    "https://meli.la/2tA9txv :: PRODUTO Teclado F75|"
    "https://meli.la/1hGSoc3 :: LISTA cupom 13657213|"
    "https://meli.la/13iNqPB :: LISTA cupom 13657057"
)

ALVOS = []
for item in (os.environ.get("ML_SOCIAL_URLS") or _PADRAO).split("|"):
    item = item.strip()
    if not item:
        continue
    url, _, rotulo = item.partition("::")
    ALVOS.append((url.strip(), rotulo.strip() or "(sem rótulo)"))

# Campos que podem estabelecer a relação vitrine → item principal.
CAMPOS_CAUSAIS = (
    "seed", "seeds", "highlight_seeds", "featured", "card-featured",
    "origin", "origin_url", "share", "share_actions", "target",
    "object_id", "item_id", "product_id", "user_product_id",
    "pid_extended", "permalink", "affiliate", "affiliate-profile",
    "source", "tracking", "seeMoreLink", "isRecommendationList",
    "affiliateName", "totalElements", "is_main", "main", "primary",
    "position", "reco_item_pos", "matt_tool_id", "is_seed",
)

# Marcadores para ancorar a varredura de chaves balanceadas.
MARCADORES = ("seeMoreLink", "isRecommendationList", "highlight_seeds",
              "polycards", "permalink", "seeds", "components")

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
_RE_ANCORA = re.compile(r"<a\b([^>]*)>(.*?)</a>", re.I | re.S)
_RE_ATTR = re.compile(r'([\w:-]+)\s*=\s*"([^"]*)"')
_RE_TAGS = re.compile(r"<[^>]+>")
_RE_ID_ML = re.compile(r"\bMLBU?\d{5,}\b")


def bloco(t: str) -> None:
    print("\n" + "═" * 74)
    print(t)
    print("═" * 74)


def sub(t: str) -> None:
    print(f"\n── {t} " + "─" * max(0, 70 - len(t)))


# ══════════════════════════════════════════════════════════════════
# EXTRAÇÃO DE JSON DE DENTRO DE JAVASCRIPT
# ══════════════════════════════════════════════════════════════════
def _fim_do_objeto(texto: str, inicio: int) -> int:
    """
    Índice logo após o `}` que fecha o objeto aberto em `inicio`.

    Conta chaves respeitando string e escape — sem isso, uma `}`
    dentro de texto fecharia o objeto errado.
    """
    prof, i, n = 0, inicio, len(texto)
    em_string = False
    escapado = False
    while i < n:
        c = texto[i]
        if em_string:
            if escapado:
                escapado = False
            elif c == "\\":
                escapado = True
            elif c == '"':
                em_string = False
        else:
            if c == '"':
                em_string = True
            elif c == "{":
                prof += 1
            elif c == "}":
                prof -= 1
                if prof == 0:
                    return i + 1
        i += 1
    return -1


def _inicio_do_objeto(texto: str, pos: int) -> int:
    """Sobe do índice de uma chave até o `{` que abre o objeto."""
    prof, i = 0, pos
    while i >= 0:
        c = texto[i]
        if c == "}":
            prof += 1
        elif c == "{":
            if prof == 0:
                return i
            prof -= 1
        i -= 1
    return -1


def objetos_ancorados(texto: str, marcador: str, limite: int = 6) -> list:
    """
    Objetos JSON que contêm `marcador`, achados por varredura de
    chaves balanceadas.

    Rede de segurança: funciona sem conhecer o invólucro do script.
    """
    achados, inicio_busca = [], 0
    for _ in range(limite):
        p = texto.find(f'"{marcador}"', inicio_busca)
        if p < 0:
            break
        inicio_busca = p + len(marcador)
        a = _inicio_do_objeto(texto, p)
        if a < 0:
            continue
        b = _fim_do_objeto(texto, a)
        if b < 0:
            continue
        try:
            achados.append(json.loads(texto[a:b]))
        except Exception:
            # Sobe um nível e tenta de novo — às vezes o objeto
            # imediato é um fragmento.
            a2 = _inicio_do_objeto(texto, a - 1)
            if a2 >= 0:
                b2 = _fim_do_objeto(texto, a2)
                if b2 > 0:
                    try:
                        achados.append(json.loads(texto[a2:b2]))
                    except Exception:
                        pass
    return achados


def extrair_json(corpo: str) -> list:
    """Estratégias de extração, da mais específica à mais bruta."""
    saida = []

    # 1) bloco inteiro
    try:
        saida.append(("bloco inteiro", json.loads(corpo.strip())))
    except Exception:
        pass

    # 2) atribuição  X = {...};  — fica com o MAIOR objeto, não com
    #    o primeiro: o primeiro costuma ser configuração pequena, e
    #    parar nele faria a sonda perder o estado de verdade.
    maior = None
    for m in re.finditer(r"=\s*(\{)", corpo):
        b = _fim_do_objeto(corpo, m.start(1))
        if b <= 0 or b - m.start(1) < 500:
            continue
        try:
            obj = json.loads(corpo[m.start(1):b])
        except Exception:
            continue
        if maior is None or (b - m.start(1)) > maior[0]:
            maior = (b - m.start(1), obj)
    if maior:
        saida.append((f"atribuição ({maior[0]}B)", maior[1]))

    # 3) JSON.parse("…")
    for m in re.finditer(r"JSON\.parse\(\s*(['\"])", corpo):
        aspas = m.group(1)
        fim = corpo.find(aspas + ")", m.end())
        if fim < 0:
            continue
        cru = corpo[m.end():fim]
        try:
            saida.append(("JSON.parse",
                          json.loads(json.loads(aspas + cru + aspas))))
            break
        except Exception:
            pass

    # 4) varredura ancorada
    if not saida:
        for marcador in MARCADORES:
            for obj in objetos_ancorados(corpo, marcador, limite=3):
                saida.append((f"ancorado:{marcador}", obj))
            if saida:
                break
    return saida


# ══════════════════════════════════════════════════════════════════
# CAMINHADA
# ══════════════════════════════════════════════════════════════════
def caminhar(no: Any, caminho: str = "$", saida: Optional[list] = None,
             prof: int = 0) -> list:
    """Todo par (caminho, valor) da estrutura. Sem amostragem."""
    saida = [] if saida is None else saida
    if prof > 20:
        return saida
    if isinstance(no, dict):
        for k, v in no.items():
            caminhar(v, f"{caminho}.{k}", saida, prof + 1)
    elif isinstance(no, list):
        for i, v in enumerate(no[:120]):
            caminhar(v, f"{caminho}[{i}]", saida, prof + 1)
    else:
        saida.append((caminho, no))
    return saida


def campos_causais(pares: list) -> dict:
    """Pares cujo CAMINHO menciona um campo causal."""
    achados = {}
    for caminho, valor in pares:
        ultimo = caminho.rsplit(".", 1)[-1].split("[")[0].lower()
        for campo in CAMPOS_CAUSAIS:
            if campo.lower() == ultimo:
                achados.setdefault(campo, []).append((caminho, valor))
    return achados


def _desescapar(valor: str) -> str:
    """
    Desfaz o escape de barra que o Mercado Livre usa em URL dentro
    de JSON.

    Necessário mesmo depois do `json.loads`: quando o payload é JSON
    aninhado dentro de string, a barra chega como `\\u002F` literal e
    um `"/p/MLB" in valor` daria FALSO NEGATIVO — a classe de erro
    que já custou caro nesta investigação.
    """
    return (valor.replace("\\u002F", "/").replace("\\u002f", "/")
                 .replace("\\/", "/"))


def candidatos_de(pares: list) -> dict:
    """
    Agrupa por PREFIXO de caminho todo objeto que tenha um
    permalink de produto — cada um vira um CANDIDATO.
    """
    grupos = {}
    for caminho, valor in pares:
        if not isinstance(valor, str):
            continue
        valor = _desescapar(valor)
        if "/p/MLB" in valor or "/up/MLBU" in valor:
            prefixo = caminho.rsplit(".", 1)[0]
            grupos.setdefault(prefixo, {})["__url__"] = valor
            grupos[prefixo]["__caminho__"] = caminho
    # Recolhe os irmãos de cada candidato.
    for caminho, valor in pares:
        prefixo = caminho.rsplit(".", 1)[0]
        for base in list(grupos):
            if prefixo == base or prefixo.startswith(base + "."):
                chave = caminho[len(base):].lstrip(".")
                if len(str(valor)) < 300:
                    grupos[base][chave] = valor
    return grupos


# ══════════════════════════════════════════════════════════════════
# ÂNCORAS COM CONTEXTO ESTRUTURAL
# ══════════════════════════════════════════════════════════════════
def ancoras_detalhadas(html_doc: str) -> list:
    saida = []
    for m in _RE_ANCORA.finditer(html_doc):
        attrs_cru, interno = m.group(1), m.group(2)
        attrs = dict(_RE_ATTR.findall(attrs_cru))
        href = _html.unescape(attrs.get("href", ""))
        if "/p/MLB" not in href and "/up/MLBU" not in href:
            continue
        texto = _html.unescape(_RE_TAGS.sub(" ", interno))
        texto = re.sub(r"\s{2,}", " ", texto).strip()
        antes = html_doc[max(0, m.start() - 700):m.start()]
        secao = ""
        s = re.findall(r'class="([^"]*(?:section|carousel|card|grid|'
                       r'recommendations|featured)[^"]*)"', antes, re.I)
        if s:
            secao = s[-1][:110]
        ident = _RE_ID_ML.findall(href)
        frag = href.split("#", 1)[1] if "#" in href else ""
        query = urllib.parse.urlsplit(href).query
        saida.append({
            "pos": len(saida) + 1,
            "href": href,
            "ids": ident,
            "texto": texto[:130],
            "classe": attrs.get("class", "")[:110],
            "data": {k: v[:70] for k, v in attrs.items()
                     if k.startswith("data-")},
            "secao": secao,
            "query": dict(urllib.parse.parse_qsl(query)) if query else {},
            "fragmento": dict(urllib.parse.parse_qsl(frag)) if frag else {},
        })
    return saida


# ══════════════════════════════════════════════════════════════════
# SONDA
# ══════════════════════════════════════════════════════════════════
async def sondar(s: aiohttp.ClientSession, url: str, rotulo: str) -> None:
    bloco(f"ALVO  {url}\nRÓTULO: {rotulo}")

    sub("1-3. entrada, redirects, status")
    atual, status = url, 0
    for n in range(1, 8):
        try:
            async with s.get(atual, headers=CAB, allow_redirects=False,
                             timeout=aiohttp.ClientTimeout(total=30)) as r:
                status = r.status
                dest = r.headers.get("Location")
                print(f"    {n}. HTTP {r.status}  {atual[:110]}")
                if not dest:
                    break
                print(f"       -> {dest[:150]}")
                atual = urllib.parse.urljoin(atual, dest)
        except Exception as exc:
            print(f"    {n}. FALHOU {type(exc).__name__}")
            return

    sub("4. parametros da URL final")
    partes = urllib.parse.urlsplit(atual)
    print(f"    host={partes.netloc}  path={partes.path}")
    for k, v in urllib.parse.parse_qsl(partes.query):
        marca = "  " if k != "ref" else ">>"
        print(f"    {marca} {k} = {v[:100]}{'…' if len(v) > 100 else ''}"
              f"  ({len(v)} chars)")

    try:
        async with s.get(atual, headers=CAB, allow_redirects=True,
                         timeout=aiohttp.ClientTimeout(total=50)) as r:
            corpo = await r.text(errors="ignore")
            status = r.status
    except Exception as exc:
        print(f"    GET FALHOU {type(exc).__name__}")
        return

    inline, externos = [], []

    def _coletar(m):
        src = _RE_SRC.search(m.group(1))
        if src:
            externos.append(src.group(1))
        else:
            inline.append(m.group(2))
        return " "

    html_doc = _RE_SCRIPT.sub(_coletar, corpo)
    print(f"\n    HTTP {status} | documento {len(corpo)}B | "
          f"HTML_DOC {len(html_doc)}B | {len(inline)} blocos inline")

    sub("CABECA DE CADA BLOCO INLINE (revela o involucro)")
    for i, b in enumerate(inline):
        if len(b) < 3000:
            continue
        cabeca = re.sub(r"\s+", " ", b[:150])
        print(f"    #{i:<3} {len(b):>7}B  {cabeca}")

    sub("5-8, 15, 19-21. ANCORAS, na ordem do documento")
    ancoras = ancoras_detalhadas(html_doc)
    print(f"    total: {len(ancoras)}")
    for a in ancoras[:10]:
        print(f"\n  CANDIDATO-HTML #{a['pos']}")
        print(f"    href      : {a['href'][:150]}")
        print(f"    ids       : {a['ids']}")
        print(f"    texto     : {a['texto']!r}")
        print(f"    classe    : {a['classe']!r}")
        if a["data"]:
            print(f"    data-*    : {a['data']}")
        if a["secao"]:
            print(f"    secao     : {a['secao']!r}")
        if a["query"]:
            print(f"    query     : {a['query']}")
        if a["fragmento"]:
            print(f"    fragmento : {a['fragmento']}")

    sub("22-27. ESTRUTURA EMBUTIDA — extracao e caminhada")
    pares_todos = []
    for i, b in enumerate(inline):
        if len(b) < 3000:
            continue
        extraidos = extrair_json(b)
        if not extraidos:
            print(f"    bloco #{i}: nenhuma estrategia extraiu JSON")
            continue
        for estrategia, dados in extraidos:
            pares = caminhar(dados)
            pares_todos += pares
            print(f"    bloco #{i}: {estrategia} -> {len(pares)} pares")

    if not pares_todos:
        print("    NENHUMA estrutura extraida. Inventario so por HTML.")

    sub("C/D. CAMPOS CAUSAIS presentes na estrutura")
    causais = campos_causais(pares_todos)
    if not causais:
        print("    NENHUM campo causal encontrado.")
    for campo in sorted(causais):
        ocorr = causais[campo]
        print(f"\n    >> {campo}  ({len(ocorr)} ocorrencia(s))")
        for caminho, valor in ocorr[:6]:
            v = str(valor)
            print(f"        {caminho}")
            print(f"          = {v[:190]}{'…' if len(v) > 190 else ''}")

    sub("A. INVENTARIO DE CANDIDATOS (da estrutura)")
    grupos = candidatos_de(pares_todos)
    print(f"    candidatos com permalink: {len(grupos)}")
    for i, (base, campos) in enumerate(list(grupos.items())[:8], 1):
        print(f"\n  CANDIDATO-JSON #{i}   base: {base}")
        print(f"    url    : {str(campos.get('__url__'))[:150]}")
        interessantes = {k: v for k, v in campos.items()
                         if not k.startswith("__")}
        for k in sorted(interessantes)[:28]:
            print(f"    {k[:40]:<42} = {str(interessantes[k])[:90]}")

    sub("E. ESTRUTURA DE LISTA")
    for chave in ("seeMoreLink", "isRecommendationList", "affiliateName",
                  "totalElements", "subtitle", "highlight_seeds"):
        ocorr = [(c, v) for c, v in pares_todos
                 if c.rsplit(".", 1)[-1].split("[")[0] == chave]
        if ocorr:
            print(f"\n  LISTA — {chave}: {len(ocorr)}")
            for c, v in ocorr[:4]:
                print(f"      {c} = {str(v)[:180]}")
    print()
    for agulha in ("_Container_", "coupon_campaign_id"):
        n = sum(1 for _, v in pares_todos
                if isinstance(v, str) and agulha in v)
        print(f"    {agulha}: {n} valor(es) na estrutura")


async def main() -> int:
    bloco("SONDA FORENSE DA PAGINA /social — INVENTARIO BRUTO")
    print(f"  alvos      : {len(ALVOS)}")
    print(f"  credencial : NENHUMA (anonima, por desenho)")
    print(f"  pergunta   : que metadados o ML da para diferenciar")
    print(f"               o item PRINCIPAL das recomendacoes?")
    print(f"  proibido   : titulo da mensagem, similaridade, 1a ancora,")
    print(f"               preco, posicao como prova, chute.")

    async with aiohttp.ClientSession() as s:
        for url, rotulo in ALVOS:
            try:
                await sondar(s, url, rotulo)
            except Exception as exc:
                print(f"\n  ALVO FALHOU: {type(exc).__name__}: {exc}")

    bloco("F. CONCLUSAO")
    print("  Ler as secoes 'CAMPOS CAUSAIS' e 'INVENTARIO'.")
    print("  Se algum campo apontar o item principal, a regra sai da")
    print("  estrutura e nao de heuristica. Se nenhum apontar, isso")
    print("  fica declarado — e a decisao volta para o operador.")
    print()
    print("  NADA implementado. NADA alterado em producao.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
