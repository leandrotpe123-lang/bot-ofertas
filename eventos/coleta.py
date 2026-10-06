"""
Proveniência [F1.2-A1] — COLETA SEGURA: a única porta do pipeline para o
anel de eventos.

O pipeline nunca monta payload fora daqui. Ele chama
    eventos.emitir_de("tipo", lambda: (corr, dados), local="modulo.ponto")
e esta camada garante (contrato F1.2 A.3/A.6/A.8; testes em
tests/test_eventos_{coleta,tamanho,segredos,contrato}.py):

  C1 Preguiçosa ... desligada (sem registro), o coletor nem roda.
  C2 Blindada ..... nada da instrumentação chega ao pipeline. Falha do
                    coletor, da conversão, da máscara, do ajuste de
                    tamanho ou do anel vira contador e, quando dá, evento
                    `degradado` do MESMO tipo: o fato não some, só perde
                    detalhe. Só KeyboardInterrupt e SystemExit passam.
  C3 Pura ......... sem await, sem I/O, sem banco, sem log.
  C4 Segura ....... só primitivos: objeto desconhecido vira
                    "<objeto:Classe>", NUNCA str(obj) — um Message do
                    Telethon impresso traria access_hash. URL que carrega
                    afiliado, parâmetro, código curto ou credencial vira
                    ⟨url:host:h12⟩ em qualquer texto, chave ou valor.
  C5 Limitada ..... evento ≤ TETO_EVENTO por construção e sempre dentro
                    dos limites do anel F1.1, que o guarda intacto.
  C6 Execução ..... `exec` por execução e `exec_pai` nas derivadas;
                    EXATAMENTE um execucao.fim por execução criada,
                    inclusive descarte antes da fila, erro e cancelamento.
  C7 Adiamento .... dentro de adiar(), a task dona só CAPTURA (cópia
                    saneada); máscara, ajuste e anel saem no fim do
                    escopo, fora dos locks. Tasks filhas emitem direto.

h12 é um PSEUDÔNIMO estável para comparar igualdade (sha256 truncado em
12 hexadecimais, sem sal, igual entre boots). NÃO é segredo nem mecanismo
de segurança: quem tem uma URL candidata confirma se ela bate. A proteção
vem de a URL não entrar no evento; a URL exata continua recuperável pelo
worker a partir de (chat, msg).

Uso a partir do laço do worker (uma thread), como todo o pipeline.
"""
from __future__ import annotations

import asyncio
import contextvars
import functools
import hashlib
import itertools
import json
import math
import re
import time
from typing import Any, Callable, Optional, Tuple
from urllib.parse import urlsplit

import eventos as _pacote        # fachada: o registro vive em _pacote._registro
from eventos import catalogo
from eventos.anel import MAX_TEXTO

__all__ = ["emitir_de", "execucao", "transferir_execucao", "inicio_na_fila",
           "marcar_desfecho", "exec_atual", "adiar", "prova_banco", "h12",
           "representar_url", "mascarar_urls", "saude_coleta", "TETO_EVENTO"]

# ── Limites — todos DENTRO dos do anel F1.1, que então não corta nada ──
TETO_EVENTO = 12 * 1024          # evento serializado, envelope incluso
_FOLGA_ENVELOPE = 512            # v, tipo, boot, seq, ts e as chaves deles
_ORCAMENTO_CORPO = TETO_EVENTO - _FOLGA_ENVELOPE   # JSON de corr + dados
# corr e dados têm orçamentos PRÓPRIOS: um corr grande nunca muda o corte
# dos dados — a mesma carga dá sempre o mesmo evento.
_ORCAMENTO_CORR = 2 * 1024       # JSON do corr (ids de correlação)
# Folga: ts_fato e a marca `cortado` entram DEPOIS do ajuste — o tamanho
# do relógio no JSON varia e não pode mudar o corte (determinismo).
_ORCAMENTO_DADOS = _ORCAMENTO_CORPO - _ORCAMENTO_CORR - 64  # JSON dos dados
_MAX_CHARS_TEXTO = MAX_TEXTO     # por string (o anel corta acima disto)
_MAX_BYTES_TEXTO = 8 * 1024      # por string, em UTF-8
_MAX_CHARS_TOTAL = 32 * 1024     # texto somado dos dados na captura
_MAX_CHARS_CORR = 2 * 1024       # texto somado do corr na captura
_MAX_CHAVE = 64                  # caracteres por chave
_MAX_ITENS = 50                  # itens por coleção
_MAX_NOS = 400                   # chaves + valores dos dados
_MAX_NOS_CORR = 60               # chaves + valores do corr
_PROFUNDIDADE = 4                # contêiner neste nível vira marcador (= anel)
_MAX_ROTULO = 100                # tipo e local
_MAX_ADIADOS = 64                # eventos por escopo de adiamento
_INT_LIMITE = 2 ** 53
_MARCA = "…"
_PASSAM = (KeyboardInterrupt, SystemExit)

_C = {"chamadas": 0, "emitidos": 0, "recusados": 0, "falhas_coleta": 0,
      "falhas_internas": 0, "cortados": 0, "urls_protegidas": 0,
      "adiados": 0, "adiamento_estourado": 0, "execucoes": 0, "fins": 0}


def saude_coleta() -> dict:
    """Contadores da coleta (/v1/saude e processo.vida). Nunca levanta."""
    try:
        return dict(_C)
    except Exception:                                  # noqa: BLE001
        return {}


def _nome(t) -> str:
    """Nome da classe sem executar código dela (nem da metaclasse)."""
    try:
        n = type.__dict__["__name__"].__get__(t)
    except Exception:                                  # noqa: BLE001
        return "?"
    return n[:40] if type(n) is str else "?"


def _rotulo(x) -> str:
    return x[:_MAX_ROTULO] if type(x) is str else "?"


def _nao_finito(x: float) -> str:
    return "nan" if x != x else ("inf" if x > 0 else "-inf")


# ─────────────────────────────────────────────────────────────────
# URLs — em claro só a que não carrega afiliado, parâmetro, código
# curto nem credencial. O resto vira ⟨url:host:h12⟩.
# ─────────────────────────────────────────────────────────────────
_SEM_URL = "\\s<>\"'`\u27e8\u27e9\u200b-\u200d\u2060"
_RE_URL = re.compile(
    rf"https?://[^{_SEM_URL}]+"
    rf"|(?<![\w@.:/\u27e8-])(?:[a-z0-9][a-z0-9-]{{0,62}}\.)+[a-z]{{2,24}}"
    rf"(?::\d{{1,5}})?(?:/[^{_SEM_URL}]*)?",
    re.IGNORECASE)
_FIM_URL = ".,;)>]}!?"           # o mesmo corte final de ingestao.ingerir
_RE_IP = re.compile(r"[\d.]+")
_RE_HOST = re.compile(r"[a-z0-9.-]{1,253}")
_RE_CODIGO = re.compile(r"[A-Za-z0-9_-]{4,24}")
_SUFIXOS_INTERNOS = (".internal", ".local", ".localhost", ".lan", ".intranet")

# Defesa em profundidade — a fonte de verdade sobre plataformas continua
# sendo o registry. Host listado (ou subdomínio dele) nunca vai em claro.
_HOSTS_SENSIVEIS = frozenset({
    # encurtadores e domínios de afiliado das plataformas atendidas
    "amzn.to", "amzn.com", "a.co", "amzn.eu", "amzlink.to",
    "s.shopee.com.br", "shope.ee", "shp.ee",
    "mercadolivre.com", "meli.la",
    "magazinevoce.com.br", "divulgador.magalu.com",
    # redes de afiliado
    "tidd.ly", "awin1.com", "prf.hn", "lomadee.com", "linksynergy.com",
    "go.hotmart.com",
    # encurtadores genéricos
    "bit.ly", "bitly.com", "tinyurl.com", "cutt.ly", "is.gd", "v.gd",
    "t.co", "ow.ly", "buff.ly", "goo.gl", "rebrand.ly", "rb.gy",
    "shorturl.at", "tiny.cc", "s.id", "encurtador.com.br", "encr.pw",
    "l.ead.me", "abre.ai", "linktr.ee", "lnkd.in",
})
_SEGMENTOS_SENSIVEIS = frozenset({
    "sec", "aff", "afiliado", "afiliados", "affiliate", "affiliates", "ref",
    "redirect", "redir", "click", "track", "tracking", "out", "go", "r", "l"})


def h12(valor: str) -> str:
    """Pseudônimo estável de 12 hexadecimais (sha256 truncado, sem sal).
    Serve para IGUALDADE entre eventos; não é segredo nem proteção."""
    return hashlib.sha256(valor.encode("utf-8", "surrogatepass")).hexdigest()[:12]


def _host_sensivel(host: str) -> bool:
    if host in _HOSTS_SENSIVEIS:
        return True
    i = host.find(".")
    while i != -1:
        if host[i + 1:] in _HOSTS_SENSIVEIS:
            return True
        i = host.find(".", i + 1)
    return False


def _parece_codigo(seg: str) -> bool:
    """Segmento único com cara de código de encurtador (3xYzAb, +convite)."""
    if seg.startswith("+"):
        return True
    if not _RE_CODIGO.fullmatch(seg):
        return False
    digito = any(c.isdigit() for c in seg)
    letra = any(c.isalpha() for c in seg)
    misto = any(c.isupper() for c in seg) and any(c.islower() for c in seg)
    return (digito and letra) or misto


def _classificar(u: str) -> Tuple[Optional[str], str]:
    """(motivo, host). motivo None: a URL pode ir em claro."""
    base = u if u[:8].lower().startswith(("http://", "https://")) else "https://" + u
    try:
        p = urlsplit(base)
        host = (p.hostname or "").rstrip(".")
        p.port                                         # porta inválida levanta
    except ValueError:
        return "INVALIDA", ""
    if len(u) > 512:
        return "LONGA", host
    if (not host or _RE_IP.fullmatch(host) or ":" in host or "." not in host
            or host == "localhost" or host.endswith(_SUFIXOS_INTERNOS)):
        return "INVALIDA", host                        # IP, host interno ou de um rótulo
    if "@" in p.netloc:
        return "CREDENCIAL", host
    if "?" in base or "#" in base:
        return "PARAMETROS", host
    if _host_sensivel(host):
        return "AFILIADA", host
    caminho = p.path
    if "=" in caminho or ";" in caminho or "%" in caminho:
        return "PARAMETROS", host
    segs = [s for s in caminho.split("/") if s]
    if any(s.lower() in _SEGMENTOS_SENSIVEIS for s in segs):
        return "AFILIADA", host
    if len(segs) == 1 and _parece_codigo(segs[0]):
        return "CODIGO", host
    return None, host


def _host_seguro(host: str) -> str:
    return host if _RE_HOST.fullmatch(host or "") else "?"


@functools.lru_cache(maxsize=2048)
def _token(u: str) -> Optional[str]:
    """\u27e8url:host:h12\u27e9 quando u n\u00e3o pode ir em claro; None quando pode.
    Fun\u00e7\u00e3o pura: o cache (limitado) s\u00f3 evita reclassificar a mesma URL,
    que se repete entre os eventos de uma mesma oferta."""
    motivo, host = _classificar(u)
    if motivo is None:
        return None
    return f"\u27e8url:{_host_seguro(host)}:{h12(u)}\u27e9"


def _trocar(m: "re.Match") -> str:
    bruto = m.group(0)
    u = bruto.rstrip(_FIM_URL)
    if not u:
        return bruto
    tok = _token(u)
    if tok is None:
        return bruto
    _C["urls_protegidas"] += 1
    return tok + bruto[len(u):]


def mascarar_urls(texto: str) -> str:
    """Troca cada URL que não pode ir em claro por ⟨url:host:h12⟩; a
    pontuação final fica. Idempotente."""
    if "." not in texto and "://" not in texto:
        return texto                     # sem ponto nem esquema não há URL
    return _RE_URL.sub(_trocar, texto)


def representar_url(url) -> dict:
    """Forma segura de UMA URL para campo estruturado: {"url"} só quando
    ela pode ir em claro; senão {"h12", "host", "motivo"}, sem a URL."""
    if type(url) is not str:
        return {"motivo": "INVALIDA"}
    u = url.strip()
    motivo, host = _classificar(u)
    if motivo is None:
        return {"url": u}
    return {"h12": h12(u), "host": _host_seguro(host), "motivo": motivo}


# ─────────────────────────────────────────────────────────────────
# Saneamento — CÓPIA de primitivos, com custo e tamanho limitados.
# Subclasses passam pelos métodos da BASE: nenhum código do objeto roda.
# ─────────────────────────────────────────────────────────────────
class _Orc:
    __slots__ = ("nos", "chars", "cortado")

    def __init__(self, nos: int = _MAX_NOS, chars: int = _MAX_CHARS_TOTAL) -> None:
        self.nos = nos
        self.chars = chars
        self.cortado = False


def _orc_corr() -> _Orc:
    return _Orc(_MAX_NOS_CORR, _MAX_CHARS_CORR)


def _cortar_bytes(s: str, n: int) -> str:
    """Prefixo de s com até n bytes UTF-8, sem partir caractere."""
    if n <= 0:
        return ""
    return s.encode("utf-8", "replace")[:n].decode("utf-8", "ignore")


def _texto(s: str, orc: _Orc) -> str:
    teto = min(_MAX_CHARS_TEXTO, orc.chars)
    if len(s) > teto:
        s = s[:teto - 1] + _MARCA if teto > 1 else _MARCA
        orc.cortado = True
    if len(s) * 4 > _MAX_BYTES_TEXTO and len(s.encode("utf-8", "replace")) > _MAX_BYTES_TEXTO:
        s = _cortar_bytes(s, _MAX_BYTES_TEXTO - 3) + _MARCA
        orc.cortado = True
    orc.chars = max(0, orc.chars - len(s))
    return s


def _inteiro(x: int):
    return x if -_INT_LIMITE <= x <= _INT_LIMITE else f"<int:{x.bit_length()} bits>"


def _chave(k, orc: _Orc) -> str:
    orc.nos -= 1
    t = type(k)
    if t is str:
        c = k
    elif k is None:
        c = "null"
    elif t is bool:
        c = "true" if k else "false"
    elif t is int:
        c = str(_inteiro(k))
    elif t is float:
        c = repr(k) if math.isfinite(k) else _nao_finito(k)
    elif issubclass(t, str):
        c = str.__str__(k)
    elif issubclass(t, int):
        c = str(_inteiro(int.__int__(k)))
    else:
        c = f"<chave:{_nome(t)}>"
    if len(c) > _MAX_CHAVE:
        c = c[:_MAX_CHAVE - 1] + _MARCA
        orc.cortado = True
    orc.chars = max(0, orc.chars - len(c))
    return c


def _dict(v, orc: _Orc, nivel: int):
    n = dict.__len__(v)
    if nivel >= _PROFUNDIDADE:
        orc.cortado = True
        return f"<dict:{n}>"
    saida, vistos = {}, 0
    for k, x in dict.items(v):
        if vistos >= _MAX_ITENS or orc.nos <= 0:
            break
        saida[_chave(k, orc)] = _sanear(x, orc, nivel + 1)
        vistos += 1
    if n > vistos:
        orc.cortado = True
        saida[_MARCA] = f"+{n - vistos}"
    return saida


def _lista(v, orc: _Orc, nivel: int, base):
    n = base.__len__(v)
    if nivel >= _PROFUNDIDADE:
        orc.cortado = True
        return f"<lista:{n}>"
    saida = []
    for x in itertools.islice(base.__iter__(v), _MAX_ITENS):
        if orc.nos <= 0:
            break
        saida.append(_sanear(x, orc, nivel + 1))
    k = len(saida)
    if n > k:
        orc.cortado = True
        saida.append(f"{_MARCA}+{n - k}")
    return saida


def _conjunto(v, orc: _Orc, nivel: int, t):
    base = frozenset if issubclass(t, frozenset) else set
    n = base.__len__(v)
    if nivel < _PROFUNDIDADE and n <= _MAX_ITENS:
        itens = list(base.__iter__(v))
        if all(type(x) is str for x in itens) or all(type(x) is int for x in itens):
            return _lista(sorted(itens), orc, nivel, list)
    orc.cortado = True
    return f"<conjunto:{n}>"


def _sanear(v, orc: _Orc, nivel: int):
    orc.nos -= 1
    if orc.nos < 0:
        orc.cortado = True
        return _MARCA
    t = type(v)
    try:
        if v is None or t is bool:
            return v
        if t is int:
            return _inteiro(v)
        if t is float:
            return v if math.isfinite(v) else _nao_finito(v)
        if t is str:
            return _texto(v, orc)
        if t is dict:
            return _dict(v, orc, nivel)
        if t is list or t is tuple:
            return _lista(v, orc, nivel, t)
        if issubclass(t, str):
            return _texto(str.__str__(v), orc)
        if issubclass(t, int):
            return _inteiro(int.__int__(v))
        if issubclass(t, float):
            x = float.__float__(v)
            return x if math.isfinite(x) else _nao_finito(x)
        if issubclass(t, dict):
            return _dict(v, orc, nivel)
        if issubclass(t, tuple):
            campos = getattr(t, "_fields", None)       # NamedTuple: por campo
            if type(campos) is tuple and all(type(c) is str for c in campos):
                return _dict(dict(zip(campos, tuple.__iter__(v))), orc, nivel)
            return _lista(v, orc, nivel, tuple)
        if issubclass(t, list):
            return _lista(v, orc, nivel, list)
        if issubclass(t, (set, frozenset)):
            return _conjunto(v, orc, nivel, t)
        if issubclass(t, bytes):
            return f"<bytes:{bytes.__len__(v)}>"
        if issubclass(t, bytearray):
            return f"<bytes:{bytearray.__len__(v)}>"
        if t is memoryview:
            return f"<bytes:{v.nbytes}>"
        return f"<objeto:{_nome(t)}>"
    except Exception:                                  # noqa: BLE001
        orc.cortado = True
        return f"<erro:{_nome(t)}>"


def _raiz(v, orc: _Orc) -> dict:
    """corr/dados saneados: sempre um dict NOVO de primitivos."""
    if v is None:
        return {}
    r = _sanear(v, orc, 0)
    if type(r) is dict:
        return r
    orc.cortado = True
    return {}


# ─────────────────────────────────────────────────────────────────
# Máscara e ajuste — no despejo (fora dos locks quando há adiamento)
# ─────────────────────────────────────────────────────────────────
def _texto_final(s: str, marca: list) -> str:
    """Limites por string DEPOIS da máscara (o token pode ser mais longo
    que a URL): o anel nunca recebe texto que ele mesmo cortaria."""
    if len(s) > _MAX_CHARS_TEXTO:
        s = s[:_MAX_CHARS_TEXTO - 1] + _MARCA
        marca[0] = True
    if len(s) * 4 > _MAX_BYTES_TEXTO and len(s.encode("utf-8", "replace")) > _MAX_BYTES_TEXTO:
        s = _cortar_bytes(s, _MAX_BYTES_TEXTO - 3) + _MARCA
        marca[0] = True
    return s


def _proteger(v, marca: list):
    t = type(v)
    if t is str:
        return _texto_final(mascarar_urls(v), marca)
    if t is dict:
        saida = {}
        for k, x in v.items():
            k = mascarar_urls(k)
            if len(k) > _MAX_CHAVE:
                k = k[:_MAX_CHAVE - 1] + _MARCA
                marca[0] = True
            saida[k] = _proteger(x, marca)
        return saida
    if t is list:
        return [_proteger(x, marca) for x in v]
    return v


def _medir(x) -> int:
    return len(json.dumps(x, ensure_ascii=False, separators=(",", ":"),
                          allow_nan=False).encode("utf-8", "replace"))


def _tam_json(s: str) -> int:
    return len(json.dumps(s, ensure_ascii=False).encode("utf-8", "replace"))


def _cortar_json(s: str, alvo: int) -> str:
    """Prefixo de s + marca cujo JSON (com escapes) cabe em `alvo` bytes."""
    n = max(0, alvo - 8)
    for _ in range(12):
        c = _cortar_bytes(s, n) + _MARCA
        t = _tam_json(c)
        if t <= alvo:
            return c
        if n == 0:
            break
        n = max(0, n - (t - alvo) - 1)
    return _MARCA


def _folhas(v, saida: list) -> list:
    """(contêiner, chave ou índice, tamanho JSON) de cada texto."""
    if type(v) is dict:
        itens = v.items()
    elif type(v) is list:
        itens = enumerate(v)
    else:
        return saida
    for k, x in itens:
        if type(x) is str:
            saida.append((v, k, _tam_json(x)))
        elif type(x) is dict or type(x) is list:
            _folhas(x, saida)
    return saida


def _encurtar_listas(v) -> None:
    if type(v) is dict:
        for x in v.values():
            _encurtar_listas(x)
    elif type(v) is list:
        for x in v:
            _encurtar_listas(x)
        n = len(v)
        if n > 2:
            fica = n // 2
            del v[fica:]
            v.append(f"{_MARCA}+{n - fica}")


def _ajustar_dados(dados: dict) -> Tuple[dict, bool]:
    """Cabe no orçamento dos dados: corta os maiores textos, depois encurta
    listas; no limite, fica só a identificação do fato (TAMANHO)."""
    if _medir(dados) <= _ORCAMENTO_DADOS:
        return dados, False
    for _ in range(3):
        excesso = _medir(dados) - _ORCAMENTO_DADOS
        if excesso <= 0:
            return dados, True
        for cont, k, t in sorted(_folhas(dados, []), key=lambda f: -f[2]):
            if excesso <= 0 or t <= 80:
                break
            if cont is dados and k in catalogo.RESERVADAS_DADOS:
                continue
            novo = _cortar_json(cont[k], max(64, t - excesso - 16))
            excesso -= t - _tam_json(novo)
            cont[k] = novo
    for _ in range(4):
        if _medir(dados) <= _ORCAMENTO_DADOS:
            return dados, True
        _encurtar_listas(dados)
    if _medir(dados) <= _ORCAMENTO_DADOS:
        return dados, True
    chaves = sorted(k[:32] for k in dados if k not in catalogo.RESERVADAS_DADOS)[:20]
    return {"degradado": True, "erro_tipo": "TAMANHO", "local": dados.get("local"),
            "chaves": chaves}, True


def _ajustar_corr(corr: dict) -> Tuple[dict, bool]:
    """corr é só correlação: fica com os escalares curtos e, no limite,
    com os ids principais."""
    if _medir(corr) <= _ORCAMENTO_CORR:
        return corr, False
    corr = {k: x for k, x in corr.items()
            if x is None or type(x) in (int, float, bool)
            or (type(x) is str and len(x) <= 64)}
    if _medir(corr) > _ORCAMENTO_CORR:
        corr = {k: corr[k] for k in ("exec", "exec_pai", "chat", "msg", "post")
                if k in corr}
    return corr, True


# ─────────────────────────────────────────────────────────────────
# Emissão
# ─────────────────────────────────────────────────────────────────
def _rodar_coletor(coletor, minimo):
    """(corr, dados, erro). erro = nome da classe da falha, ou None.
    Nada levanta, exceto KeyboardInterrupt e SystemExit."""
    try:
        corr, dados = coletor()
        corr = {} if corr is None else corr
        dados = {} if dados is None else dados
        if isinstance(corr, dict) and isinstance(dados, dict):
            return corr, dados, None
        erro = "TypeError"
    except _PASSAM:
        raise
    except BaseException as e:                         # noqa: BLE001
        erro = _nome(type(e))
    corr = {}
    if minimo is not None:
        try:
            m = minimo()
            if isinstance(m, dict):
                corr = m
        except _PASSAM:
            raise
        except BaseException:                          # noqa: BLE001
            pass
    return corr, None, erro


def _capturar(tipo, local, corr, dados, erro) -> tuple:
    """Cópia saneada do fato, no instante dele (sob o lock, se houver)."""
    orc_c, orc = _orc_corr(), _Orc()
    corr = _raiz(corr, orc_c)
    if erro is None:
        dados = _raiz(dados, orc)
        for k in catalogo.RESERVADAS_DADOS:
            dados.pop(k, None)
    else:
        dados = {"degradado": True, "erro_tipo": erro}
    for k in catalogo.RESERVADAS_CORR:
        corr.pop(k, None)
    dados["local"] = _rotulo(local)
    dados["ts_fato"] = round(time.time(), 3)
    est = _EXEC.get()
    if est is not None:
        # Os ids da execução (chat, msg) entram em todo evento dela — até
        # no degradado; o que o coletor trouxe prevalece.
        corr = {**est.corr, **corr}
        corr["exec"] = est.id
        if est.pai is not None:
            corr["exec_pai"] = est.pai
        est.eventos += 1
        fim = catalogo.TIPOS.get(tipo) if type(tipo) is str else None
        if fim is not None:
            est.desfecho = fim
    return (_rotulo(tipo), _rotulo(local), corr, dados, orc.cortado, orc_c.cortado)


def _despachar(item: tuple) -> None:
    """Máscara, ajuste e anel. Blindado: falha vira último recurso."""
    tipo, local, corr, dados, cortado_d, cortado_c = item
    reg = getattr(_pacote, "_registro", None)
    if reg is None:
        return                       # desligado entre a captura e o despejo
    try:
        marca_d, marca_c = [cortado_d], [cortado_c]
        corr, dados = _proteger(corr, marca_c), _proteger(dados, marca_d)
        # Relógio e marca ficam FORA da medida e voltam depois, na folga:
        # a mesma carga corta sempre igual.
        ts = dados.pop("ts_fato", None)
        dados, cortou_d = _ajustar_dados(dados)
        corr, cortou_c = _ajustar_corr(corr)
        if marca_d[0] or marca_c[0] or cortou_d or cortou_c:
            dados["cortado"] = True
            _C["cortados"] += 1
        if ts is not None:
            dados["ts_fato"] = ts
        if reg.emitir(tipo, dados, corr):
            _C["emitidos"] += 1
        else:
            _C["recusados"] += 1
    except _PASSAM:
        raise
    except BaseException:                              # noqa: BLE001
        _ultimo_recurso(tipo, local)


def _ultimo_recurso(tipo, local) -> None:
    """A coleta falhou por dentro: tenta ao menos registrar que o fato
    existiu (degradado, sem detalhe). Nunca levanta."""
    _C["falhas_internas"] += 1
    try:
        reg = getattr(_pacote, "_registro", None)
        if reg is not None:
            reg.emitir(_rotulo(tipo), {"degradado": True, "erro_tipo": "INTERNO",
                                       "local": _rotulo(local)}, {})
    except _PASSAM:
        raise
    except BaseException:                              # noqa: BLE001
        pass


def _task_atual():
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


def _entregar(item: tuple) -> None:
    buf = _ADIAMENTO.get()
    if buf is not None and buf.aberto and buf.dono is _task_atual():
        if len(buf.itens) < _MAX_ADIADOS:
            buf.itens.append(item)
            _C["adiados"] += 1
            return
        _C["adiamento_estourado"] += 1   # estourou: emite já, nunca perde
    _despachar(item)


def emitir_de(tipo: str, coletor: Callable[[], Any], *, local: str,
              minimo: Optional[Callable[[], Any]] = None) -> None:
    """Emite `tipo` com o que `coletor()` devolver: (corr, dados).

    `local` é o rótulo do ponto (catálogo). `minimo()` devolve só os ids
    de correlação, usados se o coletor falhar. Nunca levanta (exceto
    KeyboardInterrupt/SystemExit), nunca espera, nunca faz I/O."""
    reg = getattr(_pacote, "_registro", None)
    if reg is None:
        return                                         # C1: nada roda
    try:
        _C["chamadas"] += 1
        corr, dados, erro = _rodar_coletor(coletor, minimo)
        if erro is not None:
            _C["falhas_coleta"] += 1
        _entregar(_capturar(tipo, local, corr, dados, erro))
    except _PASSAM:
        raise
    except BaseException:                              # noqa: BLE001
        _ultimo_recurso(tipo, local)


# ─────────────────────────────────────────────────────────────────
# Execução — exec, exec_pai e EXATAMENTE um execucao.fim
# ─────────────────────────────────────────────────────────────────
_EXEC: contextvars.ContextVar = contextvars.ContextVar("eventos_execucao", default=None)
_IDS = itertools.count(1)
_LOCAL_FIM = "eventos.execucao.fim"


class _Execucao:
    __slots__ = ("id", "pai", "tipo", "corr", "t0", "t_fila", "desfecho",
                 "erro_tipo", "transferida", "encerrada", "eventos")

    def __init__(self, ident: int, pai: Optional[int], tipo: str, corr: dict) -> None:
        self.id, self.pai, self.tipo, self.corr = ident, pai, tipo, corr
        self.t0 = time.monotonic()
        self.t_fila = None
        self.desfecho = self.erro_tipo = None
        self.transferida = self.encerrada = False
        self.eventos = 0


def _encerrar(est: _Execucao, resultado: Optional[str] = None,
              exc: Optional[BaseException] = None) -> None:
    """Emite o execucao.fim desta execução — uma única vez."""
    if est.encerrada:
        return
    est.encerrada = True
    if exc is not None and resultado == "ERRO":
        est.erro_tipo = _nome(type(exc))
    agora = time.monotonic()
    dados = {"resultado": resultado or est.desfecho or "SEM_DESFECHO",
             "tipo": est.tipo, "duracao_ms": round((agora - est.t0) * 1000, 1),
             "eventos": est.eventos}
    if est.t_fila is not None:
        dados["espera_ms"] = round((est.t_fila - est.t0) * 1000, 1)
    if est.erro_tipo:
        dados["erro_tipo"] = est.erro_tipo
    dados["local"] = _LOCAL_FIM
    dados["ts_fato"] = round(time.time(), 3)
    corr = dict(est.corr)
    corr["exec"] = est.id
    if est.pai is not None:
        corr["exec_pai"] = est.pai
    _C["fins"] += 1
    _entregar(("execucao.fim", _LOCAL_FIM, corr, dados, False, False))


def _resultado_de(exc: Optional[BaseException]) -> Optional[str]:
    if exc is None:
        return None
    return "CANCELADA" if isinstance(exc, asyncio.CancelledError) else "ERRO"


class execucao:
    """Abre uma execução: `with eventos.execucao(lambda: {"chat": c,
    "msg": m}):`. Dentro dela, todo evento leva `exec` (e `exec_pai`, se
    aberta dentro de outra). Na saída, se a execução não foi transferida
    a uma task (transferir_execucao), sai o execucao.fim — com o desfecho
    registrado, ERRO ou CANCELADA. Entrada e saída nunca levantam e
    nunca engolem a exceção de quem está dentro. Desligada: no-op."""
    __slots__ = ("_ids", "_tipo", "_est", "_token")

    def __init__(self, ids: Optional[Callable[[], Any]] = None, *,
                 tipo: str = "MENSAGEM") -> None:
        self._ids, self._tipo = ids, tipo
        self._est = self._token = None

    def __enter__(self) -> "execucao":
        if getattr(_pacote, "_registro", None) is None:
            return self
        try:
            corr = {}
            if self._ids is not None:
                try:
                    bruto = self._ids()
                    if isinstance(bruto, dict):
                        corr = _raiz(bruto, _orc_corr())
                        for k in catalogo.RESERVADAS_CORR:
                            corr.pop(k, None)
                except _PASSAM:
                    raise
                except BaseException:                  # noqa: BLE001
                    _C["falhas_coleta"] += 1
            pai = _EXEC.get()
            tipo = self._tipo if catalogo.valido("tipo_execucao", self._tipo) else "MENSAGEM"
            est = _Execucao(next(_IDS), pai.id if pai is not None else None, tipo, corr)
            self._token = _EXEC.set(est)
            self._est = est
            _C["execucoes"] += 1
        except _PASSAM:
            raise
        except BaseException:                          # noqa: BLE001
            _C["falhas_internas"] += 1
        return self

    def __exit__(self, tipo_exc, exc, tb) -> bool:
        est = self._est
        if est is None:
            return False
        try:
            try:
                _EXEC.reset(self._token)
            except (ValueError, RuntimeError):
                _C["falhas_internas"] += 1
            if not est.transferida:
                _encerrar(est, _resultado_de(exc), exc)
        except _PASSAM:
            raise
        except BaseException:                          # noqa: BLE001
            _C["falhas_internas"] += 1
        return False


def _fim_da_tarefa(est: _Execucao, tarefa) -> None:
    """Callback de término da task dona: garante o execucao.fim mesmo se
    ela for cancelada antes de rodar. Não consulta tarefa.exception() —
    isso calaria o aviso do asyncio sobre exceção não recuperada."""
    try:
        _encerrar(est, "CANCELADA" if tarefa.cancelled() else None)
    except _PASSAM:
        raise
    except BaseException:                              # noqa: BLE001
        _C["falhas_internas"] += 1


def transferir_execucao(tarefa) -> None:
    """A execução corrente passa a pertencer a `tarefa` (a task da fila,
    que herdou o contexto): o execucao.fim sai quando ela terminar."""
    est = None
    try:
        est = _EXEC.get()
        if est is None or est.encerrada:
            return
        est.transferida = True
        tarefa.add_done_callback(functools.partial(_fim_da_tarefa, est))
    except _PASSAM:
        raise
    except BaseException:                              # noqa: BLE001
        _C["falhas_internas"] += 1
        if est is not None:
            est.transferida = False    # sem callback, a saída do with encerra


def inicio_na_fila() -> None:
    """A execução saiu da espera (lane + orçamento): marca para espera_ms."""
    try:
        est = _EXEC.get()
        if est is not None and est.t_fila is None:
            est.t_fila = time.monotonic()
    except Exception:                                  # noqa: BLE001
        _C["falhas_internas"] += 1


def marcar_desfecho(resultado: str, erro: Optional[BaseException] = None) -> None:
    """Desfecho explícito da execução corrente (ex.: IGNORADA, ERRO)."""
    try:
        est = _EXEC.get()
        if est is None:
            return
        if not catalogo.valido("resultado_execucao", resultado):
            _C["falhas_internas"] += 1
            return
        est.desfecho = resultado
        if erro is not None:
            est.erro_tipo = _nome(type(erro))
    except Exception:                                  # noqa: BLE001
        _C["falhas_internas"] += 1


def exec_atual() -> Optional[int]:
    est = _EXEC.get()
    return est.id if est is not None else None


# ─────────────────────────────────────────────────────────────────
# Adiamento — captura sob o lock, emissão depois de soltar
# ─────────────────────────────────────────────────────────────────
_ADIAMENTO: contextvars.ContextVar = contextvars.ContextVar("eventos_adiamento", default=None)


class _Buffer:
    __slots__ = ("dono", "itens", "aberto")

    def __init__(self, dono) -> None:
        self.dono, self.itens, self.aberto = dono, [], True


class adiar:
    """`with eventos.adiar():` — dentro do escopo, a task dona só captura;
    os eventos saem, na ordem, ao sair do escopo (também em exceção ou
    cancelamento, que seguem iguais). Aninhado: só o de fora despeja."""
    __slots__ = ("_buf", "_token")

    def __init__(self) -> None:
        self._buf = self._token = None

    def __enter__(self) -> "adiar":
        if getattr(_pacote, "_registro", None) is None:
            return self
        try:
            atual = _ADIAMENTO.get()
            dono = _task_atual()
            if atual is not None and atual.aberto and atual.dono is dono:
                return self                            # aninhado
            buf = _Buffer(dono)
            self._token = _ADIAMENTO.set(buf)
            self._buf = buf
        except _PASSAM:
            raise
        except BaseException:                          # noqa: BLE001
            _C["falhas_internas"] += 1
        return self

    def __exit__(self, tipo_exc, exc, tb) -> bool:
        buf = self._buf
        if buf is None:
            return False
        try:
            buf.aberto = False
            try:
                _ADIAMENTO.reset(self._token)
            except (ValueError, RuntimeError):
                _C["falhas_internas"] += 1
            itens, buf.itens = buf.itens, []
            for item in itens:
                _despachar(item)
        except _PASSAM:
            raise
        except BaseException:                          # noqa: BLE001
            _C["falhas_internas"] += 1
        return False


# ─────────────────────────────────────────────────────────────────
# Prova de banco (vocabulário A.5) — sem acoplar o banco a este pacote
# ─────────────────────────────────────────────────────────────────
def prova_banco(res) -> str:
    """prova.banco a partir do resultado explícito de uma gravação
    (ResultadoGravacao: ok, etapa_falha, etapas_ok, efeitos_parciais).

    confirmado ......... SÓ com ok is True
    falhou_parcial ..... ok is False e houve escrita confirmada antes
                         (efeitos_parciais True ou etapas_ok não vazio)
    falhou_sem_escrita . ok is False e o resultado diz que nada foi escrito
    nao_verificavel .... sem resultado explícito ou ambíguo (inclui bool
                         solto e None). Nunca vira confirmado."""
    try:
        if res is None or type(res) is bool:
            return "nao_verificavel"
        ok = getattr(res, "ok")
        if ok is True:
            return "confirmado"
        if ok is not False:
            return "nao_verificavel"
        parcial = getattr(res, "efeitos_parciais", None)
        etapas = getattr(res, "etapas_ok", None)
        if parcial is True or (type(etapas) in (tuple, list) and len(etapas) > 0):
            return "falhou_parcial"
        if parcial is False or (type(etapas) in (tuple, list) and len(etapas) == 0):
            return "falhou_sem_escrita"
        return "nao_verificavel"
    except Exception:                                  # noqa: BLE001
        return "nao_verificavel"
