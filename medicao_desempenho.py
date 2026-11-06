"""P0 — Medição de DESEMPENHO do worker. Só OBSERVA: nunca decide, nunca
espera, nunca derruba quem mede.

Folha do grafo de imports: só a stdlib e o `logger`. Sem I/O no import, sem
ContextVar (tests/test_eventos_tarefas.py, E03), sem task própria fora do
amostrador do loop — que main.py cria UMA vez por processo.

MARCADORES — uma linha cada, `chave=valor`, com `hora=HH:MM:SS.mmm` (os
logs da casa têm resolução de 1 s). Nenhum valor livre vai ao log: todo
valor passa por `_v`, que só deixa número, `-`, `?` e rótulo da lista
fechada de caracteres [A-Za-z0-9_.:,-] (URL, caminho, @, espaço, token
longo → `?`). Os `ms` são de time.monotonic().

  ⏱ [MIDIA_MS]   ms bytes resultado=ok|timeout|vazio|pequeno|erro tipo
                 — download_media da imagem a publicar (montagem_imagem)
  ⏱ [CONV_MS]    plat ms resultado=ok|ausente motivo <etapa>=ms:ok|falha…
                 — uma por chamada a plataforma.afilia (cache do core não
                 conta); etapas reais: ML expandir/descoberta/createlink,
                 Shopee expandir/api/api_recuperacao, Magalu expandir; head
                 = tentativa HEAD do expansor. Amazon: o total aqui; a
                 sonda do SiteStripe segue no [AMZ_SONDA] de sempre
  ⏱ [CONV_ETAPA] plat etapa ms resultado — etapa fora de uma conversão
                 (ex.: identificação da curadoria)
  ⏱ [LOCK_MS]    dono origem ident ident_n post post_n protegido
                 — ESPERA de cada lock do caminho de publicação e o tempo
                 com o lock mais externo seguro (execução protegida)
  ⏱ [FILA_MS]    chat id edit espera lane orcamento pendentes ativos
                 — da admissão (_enfileirar) ao início do pipeline
  ⏱ [LOOP_ATRASO] atraso — o event loop acordou atrasado ≥ LOOP_LIMIAR_MS
  ⏱ [LOOP_RESUMO] n p50 p90 p95 p99 max — a cada LOOP_RESUMO_S
  ⏱ [DB_LENTO]   op ms espera resultado — _db() acima de DB_LENTO_MS;
                 op = modulo.funcao de quem abriu (nunca SQL nem parâmetro)
  ⏱ [LINKS_ZERO] chat id links motivos=<motivo>:<n>,… — acompanha o
                 "🚫 Zero links convertidos"

Percentis: cada linha por evento é uma amostra (FILA, LOCK, MIDIA, CONV);
o loop já sai resumido. Nada aqui altera retorno, exceção, ordem, lock,
timeout ou regra de quem é medido.
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
import time
import weakref
from typing import Optional

from logger import log_sys

__all__ = [
    "agora", "midia", "link", "cronometro", "etapa", "motivo", "afiliou",
    "guardar_motivos", "tirar_motivos", "links_zero", "lock_medido",
    "admitida", "fila", "amostrar_loop", "origem_db", "db",
]

DB_LENTO_MS = 50.0
LOOP_INTERVALO_S = 1.0
LOOP_LIMIAR_MS = 100.0
LOOP_RESUMO_S = 600.0

_ROTULO = re.compile(r"[A-Za-z0-9_.:,-]{1,160}")


def agora() -> float:
    return time.monotonic()


def _ms(t0: float, t1: Optional[float] = None) -> int:
    return int(round(((time.monotonic() if t1 is None else t1) - t0) * 1000))


def _hora() -> str:
    t = time.time()
    return time.strftime("%H:%M:%S", time.localtime(t)) + f".{int((t % 1) * 1000):03d}"


def _v(x) -> str:
    """O ÚNICO caminho de um valor até o log: número, '-', '?' ou rótulo."""
    if x is None:
        return "-"
    if isinstance(x, bool):
        return "1" if x else "0"
    if isinstance(x, int):
        return str(x)
    if isinstance(x, float):
        return str(int(round(x)))
    s = str(x)
    if _ROTULO.fullmatch(s) and "http" not in s.lower():
        return s
    return "?"


def _emitir(marcador: str, *campos) -> None:
    try:
        corpo = " ".join(f"{k}={_v(v)}" for k, v in campos)
        log_sys.info(f"⏱ [{marcador}] hora={_hora()} {corpo}")
    except Exception:                                   # noqa: BLE001
        pass


def _task():
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


# ─────────────────────────────────────────────────────────────────
# 1. Download de mídia
# ─────────────────────────────────────────────────────────────────
def _tipo_midia(obj) -> str:
    """Pelo NOME da classe do Telethon e pelo mime — sem importar o Telethon."""
    try:
        m = getattr(obj, "media", obj)
        nome = type(m).__name__
        if nome == "MessageMediaPhoto":
            return "foto"
        if nome == "MessageMediaDocument":
            mime = (getattr(getattr(m, "document", None), "mime_type", "") or "").lower()
            if mime.startswith("video/"):
                return "video"
            if mime.startswith("image/"):
                return "imagem"
            return "documento"
        return "outro"
    except Exception:                                   # noqa: BLE001
        return "?"


def midia(t0: float, nbytes: int, resultado: str, media_obj) -> None:
    try:
        _emitir("MIDIA_MS", ("ms", _ms(t0)), ("bytes", int(nbytes or 0)),
                ("resultado", resultado), ("tipo", _tipo_midia(media_obj)))
    except Exception:                                   # noqa: BLE001
        pass


# ─────────────────────────────────────────────────────────────────
# 2. Conversão de links — ficha por task (a do gather de cada link)
# ─────────────────────────────────────────────────────────────────
class _Conv:
    __slots__ = ("motivo", "etapas")

    def __init__(self) -> None:
        self.motivo = None
        self.etapas = []

    def rotulo(self, plat) -> str:
        m = self.motivo or ("afiliacao_ausente" if plat not in (None, "none") else "desconhecido")
        return m if plat in (None, "none") else f"{plat}.{m}"


_CONV: dict = {}       # asyncio.Task -> _Conv (aberta por `link`, sempre fechada)


class link:
    """`with link() as c:` em volta da normalização de UM link, na task que
    a executa. Junta as etapas e o motivo da falha; fecha sempre."""
    __slots__ = ("_t", "_c", "_ant")

    def __enter__(self) -> _Conv:
        self._c = _Conv()
        self._t = _task()
        self._ant = None
        if self._t is not None:
            self._ant = _CONV.get(self._t)
            _CONV[self._t] = self._c
        return self._c

    def __exit__(self, *_exc) -> bool:
        try:
            if self._t is not None:
                if self._ant is None:
                    _CONV.pop(self._t, None)
                else:
                    _CONV[self._t] = self._ant
        except Exception:                               # noqa: BLE001
            pass
        return False


def _ficha() -> Optional[_Conv]:
    t = _task()
    return _CONV.get(t) if t is not None else None


def motivo(m: str) -> None:
    """Motivo de falha da conversão corrente. O PRIMEIRO vence: a camada
    mais funda (timeout do HTTP) chega antes da genérica (descoberta)."""
    try:
        c = _ficha()
        if c is not None and c.motivo is None:
            c.motivo = m
    except Exception:                                   # noqa: BLE001
        pass


def etapa(plat: str, nome: str, t0: float, ok: bool, so_na_ficha: bool = False) -> None:
    try:
        ms = _ms(t0)
        c = _ficha()
        if c is not None:
            usados = sum(1 for n, _, _ in c.etapas if n == nome or n.startswith(nome + "_"))
            c.etapas.append((nome if not usados else f"{nome}_{usados + 1}", ms, bool(ok)))
        elif not so_na_ficha:
            _emitir("CONV_ETAPA", ("plat", plat), ("etapa", nome), ("ms", ms),
                    ("resultado", "ok" if ok else "falha"))
    except Exception:                                   # noqa: BLE001
        pass


class cronometro:
    """`with cronometro(plat, etapa) as c:` — mede a etapa; `c.ok = False`
    quando ela devolve fracasso sem levantar. Exceção = falha, e passa."""
    __slots__ = ("plat", "nome", "ok", "_t0")

    def __init__(self, plat: str, nome: str) -> None:
        self.plat, self.nome, self.ok = plat, nome, True

    def __enter__(self) -> "cronometro":
        self._t0 = time.monotonic()
        return self

    def __exit__(self, tipo, _exc, _tb) -> bool:
        etapa(self.plat, self.nome, self._t0, self.ok and tipo is None)
        return False


def afiliou(plat: str, t0: float, ok: bool) -> None:
    """Fim de uma chamada a plataforma.afilia: a linha [CONV_MS]."""
    try:
        c = _ficha()
        campos = [("plat", plat), ("ms", _ms(t0)),
                  ("resultado", "ok" if ok else "ausente"),
                  ("motivo", None if ok or c is None else (c.motivo or "afiliacao_ausente"))]
        for nome, ms, eok in (c.etapas if c is not None else ()):
            campos.append((nome, f"{ms}:{'ok' if eok else 'falha'}"))
        _emitir("CONV_MS", *campos)
    except Exception:                                   # noqa: BLE001
        pass


# Motivos da última resolução de links de uma task (a do pipeline), entre
# resolver_e_afiliar e normalizar. Fraco: a task que morre leva a entrada.
_MOTIVOS = weakref.WeakKeyDictionary()


def guardar_motivos(contagem) -> None:
    try:
        t = _task()
        if t is not None:
            _MOTIVOS[t] = dict(contagem)
    except Exception:                                   # noqa: BLE001
        pass


def tirar_motivos() -> Optional[dict]:
    try:
        t = _task()
        return _MOTIVOS.pop(t, None) if t is not None else None
    except Exception:                                   # noqa: BLE001
        return None


def links_zero(chat, msg_id, n_links: int, motivos: Optional[dict]) -> None:
    try:
        if motivos:
            itens = ",".join(f"{_v(k)}:{int(n)}" for k, n in sorted(motivos.items()))
        else:
            itens = "desconhecido"
        _emitir("LINKS_ZERO", ("chat", chat), ("id", msg_id), ("links", n_links),
                ("motivos", itens))
    except Exception:                                   # noqa: BLE001
        pass


# ─────────────────────────────────────────────────────────────────
# 3. Locks do caminho de publicação — ESPERA × execução protegida
# ─────────────────────────────────────────────────────────────────
class _Ficha:
    __slots__ = ("dono", "espera", "n")

    def __init__(self, dono: str) -> None:
        self.dono = dono
        self.espera = {}
        self.n = {}


_FICHAS: dict = {}     # asyncio.Task -> _Ficha (do lock mais externo; sempre fechada)


class lock_medido:
    """`async with lock_medido("origem", lock):` — o MESMO lock, adquirido e
    liberado pelo próprio protocolo dele (__aenter__/__aexit__): ordem,
    liberação e finalidade intocadas. A contabilidade vem DEPOIS de adquirir
    e nunca levanta (um erro dela com o lock seguro seria deadlock). O
    primeiro lock da task é o dono da ficha e escreve a linha ao soltar."""
    __slots__ = ("_tipo", "_lock", "_dono", "_t_in", "_task")

    def __init__(self, tipo: str, lock) -> None:
        self._tipo, self._lock = tipo, lock
        self._dono, self._t_in, self._task = False, 0.0, None

    async def __aenter__(self):
        t0 = time.monotonic()
        r = await self._lock.__aenter__()
        try:
            t1 = time.monotonic()
            task = _task()
            f = _FICHAS.get(task) if task is not None else None
            if f is None and task is not None:
                f = _FICHAS[task] = _Ficha(self._tipo)
                self._dono, self._task, self._t_in = True, task, t1
            if f is not None:
                f.espera[self._tipo] = f.espera.get(self._tipo, 0.0) + (t1 - t0)
                f.n[self._tipo] = f.n.get(self._tipo, 0) + 1
        except Exception:                               # noqa: BLE001
            pass
        return r

    async def __aexit__(self, tipo, exc, tb):
        try:
            return await self._lock.__aexit__(tipo, exc, tb)
        finally:
            if self._dono:
                try:
                    f = _FICHAS.pop(self._task, None)
                    if f is not None:
                        def _esp(k):
                            return int(round(f.espera[k] * 1000)) if k in f.espera else None
                        _emitir("LOCK_MS", ("dono", f.dono), ("origem", _esp("origem")),
                                ("ident", _esp("identidade")), ("ident_n", f.n.get("identidade", 0)),
                                ("post", _esp("post")), ("post_n", f.n.get("post", 0)),
                                ("protegido", _ms(self._t_in)))
                except Exception:                       # noqa: BLE001
                    pass


# ─────────────────────────────────────────────────────────────────
# 4. Fila — da admissão ao início do pipeline
# ─────────────────────────────────────────────────────────────────
_ADMISSAO = weakref.WeakKeyDictionary()   # task da fila -> monotonic da admissão


def admitida(tarefa) -> None:
    try:
        _ADMISSAO[tarefa] = time.monotonic()
    except Exception:                                   # noqa: BLE001
        pass


def fila(event, is_edit: bool, t_lane: float, pendentes: int, ativos: int) -> None:
    """Dentro do orçamento, antes do pipeline. Sem admissão registrada
    (chamada direta), espera e lane saem '-'."""
    try:
        t = _task()
        t_adm = _ADMISSAO.pop(t, None) if t is not None else None
        agora_ = time.monotonic()
        _emitir("FILA_MS", ("chat", getattr(event, "chat_id", None)),
                ("id", getattr(getattr(event, "message", None), "id", None)),
                ("edit", bool(is_edit)),
                ("espera", _ms(t_adm, agora_) if t_adm is not None else None),
                ("lane", _ms(t_adm, t_lane) if t_adm is not None else None),
                ("orcamento", _ms(t_lane, agora_)),
                ("pendentes", int(pendentes)), ("ativos", int(ativos)))
    except Exception:                                   # noqa: BLE001
        pass


# ─────────────────────────────────────────────────────────────────
# 5. Event loop — amostrador leve, UMA instância por processo
# ─────────────────────────────────────────────────────────────────
_LOOP_ATIVO = False


def _percentil(ordenados, p: float) -> float:
    if not ordenados:
        return 0.0
    k = max(0, min(len(ordenados) - 1, int(-(-p * len(ordenados) // 100)) - 1))
    return ordenados[k]


async def amostrar_loop(intervalo: float = LOOP_INTERVALO_S,
                        limiar_ms: float = LOOP_LIMIAR_MS,
                        resumo_s: float = LOOP_RESUMO_S) -> None:
    """Dorme `intervalo` e mede quanto acordou atrasado. Segunda instância
    concorrente sai na hora. Cancelamento encerra limpo."""
    global _LOOP_ATIVO
    if _LOOP_ATIVO:
        return
    _LOOP_ATIVO = True
    try:
        amostras = []
        t_res = time.monotonic()
        while True:
            t0 = time.monotonic()
            await asyncio.sleep(intervalo)
            atraso = max(0.0, (time.monotonic() - t0 - intervalo) * 1000)
            amostras.append(atraso)
            if atraso >= limiar_ms:
                _emitir("LOOP_ATRASO", ("atraso", atraso))
            if time.monotonic() - t_res >= resumo_s:
                o = sorted(amostras)
                _emitir("LOOP_RESUMO", ("n", len(o)), ("p50", _percentil(o, 50)),
                        ("p90", _percentil(o, 90)), ("p95", _percentil(o, 95)),
                        ("p99", _percentil(o, 99)), ("max", o[-1] if o else 0.0))
                amostras.clear()
                t_res = time.monotonic()
    finally:
        _LOOP_ATIVO = False


# ─────────────────────────────────────────────────────────────────
# 6. SQLite — só a operação lenta vai ao log
# ─────────────────────────────────────────────────────────────────
def origem_db(profundidade: int = 3):
    """Código de quem abriu o _db(): (3) = chamador, a partir daqui →
    gerador _db → __enter__ do contextlib → chamador. Só o code object."""
    try:
        return sys._getframe(profundidade).f_code
    except Exception:                                   # noqa: BLE001
        return None


def db(codigo, t0: float, t1: Optional[float], ok: bool) -> None:
    try:
        fim = time.monotonic()
        total = (fim - t0) * 1000
        if total < DB_LENTO_MS:
            return
        if codigo is not None:
            op = f"{os.path.splitext(os.path.basename(codigo.co_filename))[0]}.{codigo.co_name}"
        else:
            op = "?"
        _emitir("DB_LENTO", ("op", op), ("ms", total),
                ("espera", _ms(t0, t1) if t1 is not None else None),
                ("resultado", "ok" if ok else "erro"))
    except Exception:                                   # noqa: BLE001
        pass
