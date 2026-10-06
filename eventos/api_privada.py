"""
Proveniência [F1.1] — API PRIVADA do worker: SOMENTE LEITURA, SÓ MEMÓRIA.

Rotas (só GET; nem HEAD):
  /v1/saude    retrato do registro de eventos e do processo
  /v1/eventos  eventos por cursor "<boot_id>:<seq>", com espera longa

NUNCA: banco (nem database, nem sqlite3), Telegram, escrita, comando.
Transporte PUXADO: o Brain busca aqui; o worker nunca abre conexão para
ninguém. Brain fora não muda nada no worker.

REDE PRIVADA DUAL-STACK — FECHADA POR PADRÃO
  Uma escuta só: socket AF_INET6 em "[::]" com IPV6_V6ONLY=0, entregue ao
  aiohttp (SockSite). Atende IPv6 e IPv4 (mapeado) — a família que
  `<serviço>.railway.internal` entregar (ambiente novo da Railway resolve A
  e AAAA; o antigo, só AAAA). A família efetiva é lida de volta do socket
  e vai para o log e para /v1/saude. Sem IPv6 no kernel a API NÃO liga:
  nunca se cria socket AF_INET nem se escuta em 0.0.0.0.

NUNCA PÚBLICA — camadas, de fora para dentro
  1. porta própria: igual à pública (PORT) → não liga;
  2. porta de proxy TCP público (RAILWAY_TCP_APPLICATION_PORT) → não liga;
  3. requisição que passou pela borda pública da Railway (X-Railway-Edge,
     X-Real-IP, X-Forwarded-*, Forwarded…) ou com Host fora da rede
     privada (*.railway.internal, localhost, IP de loopback/privado) →
     404, ANTES até da assinatura: www.leoind.com.br nunca chega aqui,
     nem com uma "target port" apontada por engano para esta porta;
  4. assinatura HMAC; 5. taxa e teto de esperas (abaixo).

PROTEÇÃO DO WORKER — um Brain com defeito não pesa na publicação
  Requisição assinada passa por um balde de fichas (TAXA_POR_S, RAJADA):
  acima disso, 429. No máximo _MAX_ESPERAS esperas longas ao mesmo tempo;
  além delas a resposta sai na hora (sem esperar). Respostas ≤ 2 MiB.

ASSINATURA — toda requisição, ANTES de qualquer rota
  X-Foguetao-Ts:          segundos UNIX (só dígitos)
  X-Foguetao-Assinatura:  hex(HMAC-SHA256(segredo,
                              MÉTODO "\\n" caminho?query BRUTOS "\\n" ts))
  O caminho e a query entram EXATAMENTE como enviados, na ordem: qualquer
  parâmetro alterado, acrescentado ou reordenado invalida a assinatura.
  Janela ±60 s; comparação em tempo constante; falha → 401 genérico.
  Segredo com no mínimo 32 caracteres; nunca é logado nem devolvido.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import json
import socket
import time
from functools import partial
from typing import Callable, Optional

from aiohttp import web

from eventos.anel import Registro, ler_cursor
from logger import log_sys

__all__ = ["assinar", "criar_app", "iniciar", "encerrar", "ativa",
           "motivo_desligada", "SEGREDO_MIN", "JANELA_S",
           "MAX_RESPOSTA_BYTES", "TAXA_POR_S", "RAJADA"]

SEGREDO_MIN = 32
JANELA_S = 60
MAX_RESPOSTA_BYTES = 2 * 1024 * 1024
LIMITE_PADRAO, LIMITE_MAX = 500, 1000
ESPERA_MAX_S = 25.0
TAXA_POR_S = 10.0                # requisições assinadas por segundo…
RAJADA = 20                      # …com folga acumulada de até RAJADA
_MAX_ESPERAS = 4                 # esperas longas simultâneas
_PASSO_ESPERA_S = 0.5            # revê o anel mesmo sem aviso (outra thread)
_ENCERRAMENTO_S = 2.0            # requisição em voo ao desligar
_PORTA_MIN, _PORTA_MAX = 1025, 65535
CAB_TS = "X-Foguetao-Ts"
CAB_ASSINATURA = "X-Foguetao-Assinatura"
# Cabeçalhos que a borda pública da Railway sempre injeta (docs: Public
# Networking › Specs & Limits) e os genéricos de proxy: a rede privada
# não passa por proxy nenhum, então qualquer um deles = veio de fora.
_CABECALHOS_DE_BORDA = ("X-Railway-Edge", "X-Railway-Request-Id", "X-Real-IP",
                        "X-Forwarded-For", "X-Forwarded-Host",
                        "X-Forwarded-Proto", "X-Request-Start", "Forwarded")
_SUFIXO_PRIVADO = ".railway.internal"
_SEM_CACHE = {"Cache-Control": "no-store"}
_dumps = partial(json.dumps, ensure_ascii=False, separators=(",", ":"))

_runner: Optional[web.AppRunner] = None
_sock: Optional[socket.socket] = None
_parando = False
_esperas: set = set()            # esperas longas em curso (encerrar acorda)


# ── assinatura ─────────────────────────────────────────────────────
def assinar(segredo: str, metodo: str, caminho_bruto: str, ts: str) -> str:
    """Assinatura canônica de uma requisição (o Brain usa a mesma)."""
    msg = f"{metodo.upper()}\n{caminho_bruto}\n{ts}".encode("utf-8")
    return hmac.new(segredo.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def _assinatura_valida(segredo: str, metodo: str, caminho_bruto: str,
                       ts: str, assinatura: str, agora: float) -> bool:
    if not (ts and ts.isdigit() and len(ts) <= 12):
        return False
    if not (assinatura and len(assinatura) == 64):
        return False
    if abs(agora - int(ts)) > JANELA_S:
        return False
    esperada = assinar(segredo, metodo, caminho_bruto, ts)
    return hmac.compare_digest(esperada, assinatura.lower())


# ── rede privada: quem pode sequer ser atendido ────────────────────
def _host_privado(host: str) -> bool:
    """O Host endereça a rede privada? `*.railway.internal`, `localhost`
    ou IP literal de loopback/privado. Qualquer outro nome — o domínio
    público, `*.up.railway.app` — não."""
    h = (host or "").strip().lower()
    if not h or len(h) > 255:
        return False
    if h.startswith("["):                        # [ipv6]:porta
        fim = h.find("]")
        if fim < 0:
            return False
        nome = h[1:fim]
    else:
        nome = h.rsplit(":", 1)[0] if h.count(":") == 1 else h
    nome = nome.rstrip(".")
    if nome == "localhost" or nome.endswith(_SUFIXO_PRIVADO):
        return True
    try:
        ip = ipaddress.ip_address(nome)
    except ValueError:
        return False
    return ip.is_loopback or ip.is_private


def _da_rede_privada(cabecalhos) -> bool:
    """Requisição que pode ter vindo SÓ pela rede privada: nenhum
    cabeçalho de borda/proxy e Host privado."""
    if any(c in cabecalhos for c in _CABECALHOS_DE_BORDA):
        return False
    return _host_privado(cabecalhos.get("Host", ""))


# ── aplicação ──────────────────────────────────────────────────────
def _erro(status: int, codigo: str, **extra) -> web.Response:
    return web.json_response({"erro": codigo, **extra}, status=status,
                             headers=_SEM_CACHE, dumps=_dumps)


def _inteiro(valor, padrao: int, minimo: int, maximo: int):
    if valor is None:
        return padrao
    if not valor.isdigit() or len(valor) > 9:
        return None
    n = int(valor)
    return n if minimo <= n <= maximo else None


def _segundos(valor, padrao: float, minimo: float, maximo: float):
    if valor is None:
        return padrao
    if len(valor) > 12:
        return None
    try:
        n = float(valor)
    except ValueError:
        return None
    return n if minimo <= n <= maximo else None


def _resposta_eventos(res: dict) -> web.Response:
    """Monta o JSON com as cargas JÁ serializadas na emissão: o que foi
    emitido é exatamente o que sai (bytes imutáveis, sem reserializar)."""
    cabeca = _dumps(res["meta"]).encode("utf-8")
    corpo = (cabeca[:-1] + b',"eventos":[' + b",".join(res["cargas"]) + b"]}")
    return web.Response(body=corpo, content_type="application/json",
                        charset="utf-8", headers=_SEM_CACHE)


async def _esperar_novidade(reg: Registro, prazo: float) -> None:
    """Espera um evento novo do MESMO laço (set() direto, sem syscall),
    no máximo `prazo` segundos. Emissão de outra thread não acorda: a
    espera revê o anel a cada passo."""
    laco = asyncio.get_running_loop()
    novo = asyncio.Event()

    def ouvinte() -> None:
        try:
            if asyncio.get_running_loop() is laco:
                novo.set()
        except RuntimeError:
            pass

    reg.adicionar_ouvinte(ouvinte)
    _esperas.add(novo)
    try:
        await asyncio.wait_for(novo.wait(), prazo)
    except asyncio.TimeoutError:
        pass
    finally:
        _esperas.discard(novo)
        reg.remover_ouvinte(ouvinte)


def _acordar_esperas() -> None:
    """Acorda toda espera longa em curso (mesmo laço; sem syscall)."""
    for novo in tuple(_esperas):
        novo.set()


def criar_app(provedor: Callable[[], Optional[Registro]], segredo: str, *,
              saude_extra: Optional[Callable[[], dict]] = None,
              rede: Optional[dict] = None,
              relogio: Callable[[], float] = time.time) -> web.Application:
    """A aplicação, sem rede: quem liga o socket é iniciar(). Os testes
    servem esta mesma aplicação num servidor local de teste. `rede` é o
    retrato da escuta (porta, famílias) que /v1/saude devolve."""
    contadores = {"rede": 0, "recusadas": 0, "excesso": 0, "esperas": 0,
                  "pendentes_log": 0, "ultimo_log": float("-inf")}
    balde = {"fichas": float(RAJADA), "em": time.monotonic()}

    def _registrar_recusa(chave: str, agora: float) -> None:
        """Conta a recusa; avisa no log no máximo uma vez por minuto."""
        contadores[chave] += 1
        contadores["pendentes_log"] += 1
        if agora - contadores["ultimo_log"] >= 60:
            log_sys.warning(
                f"🔐 API privada: {contadores['pendentes_log']} requisição(ões) "
                f"recusada(s) | rede={contadores['rede']} "
                f"assinatura={contadores['recusadas']} "
                f"excesso={contadores['excesso']}")
            contadores["ultimo_log"] = agora
            contadores["pendentes_log"] = 0

    def _tem_ficha() -> bool:
        agora = time.monotonic()
        balde["fichas"] = min(float(RAJADA),
                              balde["fichas"] + (agora - balde["em"]) * TAXA_POR_S)
        balde["em"] = agora
        if balde["fichas"] < 1.0:
            return False
        balde["fichas"] -= 1.0
        return True

    @web.middleware
    async def proteger(request: web.Request, handler):
        agora = relogio()
        # 0) rede — só a rede privada. O que passou pela borda pública, ou
        #    vem endereçado a domínio público, recebe 404 como se nada
        #    existisse aqui: antes até da assinatura, sem revelar a API.
        if not _da_rede_privada(request.headers):
            _registrar_recusa("rede", agora)
            return _erro(404, "nao_encontrado")
        # 1) assinatura — sem ela nada mais roda
        if not _assinatura_valida(segredo, request.method, request.raw_path,
                                  request.headers.get(CAB_TS, ""),
                                  request.headers.get(CAB_ASSINATURA, ""),
                                  agora):
            _registrar_recusa("recusadas", agora)
            return _erro(401, "nao_autorizado")
        # 2) taxa — só quem assina gasta ficha: sem o segredo ninguém
        #    esgota o balde do Brain
        if not _tem_ficha():
            _registrar_recusa("excesso", agora)
            resposta = _erro(429, "excesso")
            resposta.headers["Retry-After"] = "1"
            return resposta
        resposta = await handler(request)
        resposta.headers["Cache-Control"] = "no-store"
        return resposta

    async def saude(request: web.Request) -> web.Response:
        reg = provedor()
        if reg is None:
            return _erro(503, "proveniencia_desligada")
        corpo = reg.saude()
        corpo["rede"] = rede
        corpo["recusadas_rede"] = contadores["rede"]
        corpo["recusadas_assinatura"] = contadores["recusadas"]
        corpo["recusadas_excesso"] = contadores["excesso"]
        corpo["esperas_ativas"] = contadores["esperas"]
        if saude_extra is not None:
            try:
                corpo["processo"] = dict(saude_extra())
            except Exception:
                corpo["processo"] = None
        return web.json_response(corpo, headers=_SEM_CACHE, dumps=_dumps)

    async def eventos(request: web.Request) -> web.Response:
        q = request.rel_url.query
        cursor = None
        if "cursor" in q:
            cursor = ler_cursor(q.get("cursor"))
            if cursor is None:
                return _erro(400, "parametro_invalido", campo="cursor")
        limite = _inteiro(q.get("limite"), LIMITE_PADRAO, 1, LIMITE_MAX)
        if limite is None:
            return _erro(400, "parametro_invalido", campo="limite")
        espera = _segundos(q.get("espera"), 0.0, 0.0, ESPERA_MAX_S)
        if espera is None:
            return _erro(400, "parametro_invalido", campo="espera")
        esperando = espera > 0 and contadores["esperas"] < _MAX_ESPERAS
        if not esperando:
            espera = 0.0                         # acima do teto: responde já
        laco = asyncio.get_running_loop()
        fim = laco.time() + espera
        if esperando:
            contadores["esperas"] += 1
        try:
            while True:
                reg = provedor()
                if reg is None:
                    return _erro(503, "proveniencia_desligada")
                res = reg.ler(cursor, limite, MAX_RESPOSTA_BYTES)
                restante = fim - laco.time()
                if (res["cargas"] or res["meta"]["continuidade"] != "continua"
                        or restante <= 0 or _parando):
                    return _resposta_eventos(res)
                await _esperar_novidade(reg, min(_PASSO_ESPERA_S, restante))
        finally:
            if esperando:
                contadores["esperas"] -= 1

    app = web.Application(middlewares=[proteger])
    app.router.add_get("/v1/saude", saude, allow_head=False)
    app.router.add_get("/v1/eventos", eventos, allow_head=False)
    return app


# ── rede: escuta privada dual-stack, fechada por padrão ────────────
def motivo_desligada(porta: int, segredo: str, porta_publica: int,
                     porta_tcp_publica: int = 0) -> str:
    """'' quando a configuração liga a API; senão o motivo (sem segredo).
    É a régua única: o main só instala o emissor quando ela devolve ''."""
    if porta == 0 and not segredo:
        return "não configurada (sem API_PRIVADA_PORTA e API_PRIVADA_SEGREDO)"
    if not isinstance(porta, int) or not _PORTA_MIN <= porta <= _PORTA_MAX:
        return "porta inválida"
    if porta == porta_publica:
        return "porta igual à pública"
    if porta_tcp_publica and porta == porta_tcp_publica:
        return "porta exposta por proxy TCP público (RAILWAY_TCP_APPLICATION_PORT)"
    if not isinstance(segredo, str) or len(segredo) < SEGREDO_MIN:
        return f"segredo ausente ou curto (mínimo {SEGREDO_MIN})"
    return ""


def _socket_privado(porta: int, *, criar=None) -> socket.socket:
    """Escuta dual-stack: AF_INET6 em '::' com IPV6_V6ONLY=0 — IPv6 e IPv4
    (mapeado) numa porta só. Qualquer falha fecha e levanta; quem chama
    desliga a API. NUNCA cria socket AF_INET (nada de 0.0.0.0).
    `criar` (padrão: socket.socket, resolvido na chamada) é a costura de
    teste — prova a configuração sem depender da rede do host."""
    s = (criar or socket.socket)(socket.AF_INET6, socket.SOCK_STREAM)
    try:
        s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("::", porta))
        s.listen(128)
        s.setblocking(False)
    except BaseException:
        s.close()
        raise
    return s


def _familias(sock) -> str:
    """Famílias que a escuta atende DE FATO (lidas do próprio socket)."""
    familia = getattr(sock, "family", None)
    if familia == socket.AF_INET6:
        try:
            so_v6 = sock.getsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY)
        except OSError:
            return "ipv6"
        return "ipv6" if so_v6 else "ipv4+ipv6"
    if familia == socket.AF_INET:
        return "ipv4"
    return str(familia)


def ativa() -> bool:
    return _runner is not None


async def iniciar(porta: int, segredo: str,
                  provedor: Callable[[], Optional[Registro]],
                  saude_extra: Optional[Callable[[], dict]] = None, *,
                  porta_publica: int, porta_tcp_publica: int = 0,
                  fabrica_socket: Optional[Callable[[int], socket.socket]] = None
                  ) -> bool:
    """Liga a API em [::]:porta (dual-stack, rede privada). Devolve False
    (e loga o motivo, nunca o segredo) se a configuração for inválida ou
    se a escuta privada não puder ser criada — e então nada abre.
    Idempotente. `fabrica_socket` (padrão: _socket_privado) é a costura
    de teste do caminho de sucesso."""
    global _runner, _sock
    if _runner is not None:
        return True
    motivo = motivo_desligada(porta, segredo, porta_publica, porta_tcp_publica)
    if motivo:
        log_sys.warning(f"🔐 API privada DESLIGADA | {motivo}")
        return False
    try:
        sock = (fabrica_socket or _socket_privado)(porta)
    except Exception as e:
        log_sys.warning(
            f"🔐 API privada DESLIGADA | escuta privada indisponível "
            f"({type(e).__name__}: {e}) — nunca abre IPv4 em 0.0.0.0")
        return False
    familias = _familias(sock)
    runner = web.AppRunner(
        criar_app(provedor, segredo, saude_extra=saude_extra,
                  rede={"porta": porta, "familias": familias}),
        access_log=None, shutdown_timeout=_ENCERRAMENTO_S)
    try:
        await runner.setup()
        await web.SockSite(runner, sock).start()
    except BaseException as e:
        # Falha OU cancelamento (sinal no meio do boot): nada fica aberto.
        try:
            await runner.cleanup()
        except BaseException:
            pass
        sock.close()
        if not isinstance(e, Exception):
            raise
        log_sys.warning(f"🔐 API privada DESLIGADA | {type(e).__name__}: {e}")
        return False
    _runner, _sock = runner, sock
    log_sys.info(
        f"🔐 API privada ativa | [::]:{porta} | rede privada {familias} | "
        f"rotas=/v1/saude,/v1/eventos")
    return True


async def encerrar() -> None:
    """Desliga a API e solta a porta. Idempotente. Esperas longas em
    curso respondem na hora."""
    global _runner, _sock, _parando
    if _runner is None:
        return
    _parando = True
    _acordar_esperas()
    try:
        await _runner.cleanup()
        log_sys.info("🔐 API privada encerrada")
    finally:
        _runner = None
        if _sock is not None:
            try:
                _sock.close()
            except Exception:
                pass
            _sock = None
        _parando = False
