"""
Proveniência [F1.1] — REGISTRO de eventos do processo: boot_id, seq e anel.

Responsabilidade ÚNICA: guardar em memória, com limite, os eventos que o
worker emite, numerados por ÉPOCA (boot_id) e SEQUÊNCIA (seq), e
devolvê-los por cursor "<boot_id>:<seq>".

GARANTIAS — contrato testado em tests/test_eventos_anel.py:
  G1 Sem I/O ........... nenhuma rede, disco, SQLite ou await. Só memória.
  G2 Nunca levanta ..... falha interna vira contador; quem emite nunca vê.
  G3 seq transacional .. o número só passa a existir DEPOIS que o evento
                         está no anel; falha antes disso não consome
                         número. Cada seq existente é um evento existente.
  G4 Contíguo .......... o anel tem seq consecutivos e crescentes. Descarte
                         por capacidade sai pela frente e é contado — o
                         evento existiu; não é buraco artificial.
  G5 Limitado .......... cada evento ≤ MAX_EVENTO_BYTES serializado — teto
                         RÍGIDO, checado no fim; o anel ≤ `capacidade`
                         eventos E ≤ `capacidade_bytes`.
  G6 Época ............. boot_id novo a cada processo; cursor de outra época
                         NUNCA é continuado (nova_epoca), e o fim não visto
                         da época anterior é declarado como lacuna.
  G7 Custo limitado .... converter um evento visita no máximo _MAX_NOS
                         valores e _MAX_CARACTERES de texto, seja qual for
                         a entrada: emitir() nunca trava o laço, nem com
                         dado patológico (lista de milhões, texto de MB).

O evento é serializado UMA vez, na emissão, e guardado como bytes
IMUTÁVEIS: o que foi emitido é o que a leitura devolve, mesmo que quem
chamou mexa depois no dict que passou.

NÃO faz:
  - transporte (a API privada lê daqui; o worker nunca envia nada)
  - decidir o que emitir (quem chama decide; a F1.2 liga o ciclo de vida)
  - persistir (o anel morre com o processo, por desenho)
"""
from __future__ import annotations

import json
import math
import secrets
import threading
import time
from collections import deque
from itertools import islice
from typing import Callable, Optional, Tuple

ENVELOPE_V = 1
MAX_TEXTO = 4096                 # caracteres por string em dados/corr
MAX_EVENTO_BYTES = 16 * 1024     # evento serializado, envelope incluso
_MAX_TIPO = 100                  # caracteres do rótulo do evento
_MAX_CHAVE = 200                 # caracteres por chave de dict
_MAX_CHAVE_ESGOTADO = 32         # … depois de esgotado o orçamento de texto
_MAX_ITENS = 200                 # itens por coleção (dict, lista, conjunto)
_MAX_NOS = 1000                  # chaves + valores visitados por dados/corr (G7)
_MAX_CARACTERES = MAX_EVENTO_BYTES  # texto total por dados/corr (G7): quem
                                 # esgota este orçamento já não caberia no
                                 # evento — o corte nunca muda um evento que
                                 # caberia inteiro
_MAX_CHAVES_MARCADOR = 50        # chaves listadas quando os dados excedem
_MAX_BYTES_CHAVES = 2048         # e o quanto essa lista pode ocupar
_PROFUNDIDADE = 4                # níveis de dict/lista percorridos
_INT_MAX = 2 ** 63               # inteiro maior vira texto descritivo
_MARCA_CORTE = "…[cortado]"
_SEP = (",", ":")
_HEX = frozenset("0123456789abcdef")


def _serializar(obj) -> bytes:
    """JSON compacto em UTF-8. Recebe só tipos nativos (_Corte garante);
    o que escapar levanta e quem chama trata (G2/G3). Surrogate solto vira
    '?': o evento é sempre UTF-8 válido."""
    return json.dumps(obj, ensure_ascii=False, separators=_SEP,
                      allow_nan=False).encode("utf-8", "replace")


class _Corte:
    """Converte dados/corr em JSON nativo com custo LIMITADO (G7).

    Determinística: a mesma entrada dá a mesma saída (dict e lista na
    ordem própria; conjunto ordenado antes de percorrer). Os cortes ficam
    marcados no próprio valor:
      texto > MAX_TEXTO ............ corta e acrescenta "…[cortado]"
      coleção > _MAX_ITENS ......... primeiros itens + "…+N itens"
      além de _PROFUNDIDADE ........ "…[dict de N itens]"
      orçamento de nós esgotado .... a coleção para ali + "…+N itens"
      orçamento de texto esgotado .. textos viram "…[cortado]"; chaves
                                     ficam com _MAX_CHAVE_ESGOTADO
      bytes ........................ "<N bytes>" (conteúdo binário não entra)
      float não finito, int enorme . texto
      outro tipo ................... str(valor), cortado — se str() levanta,
                                     o evento é recusado (G2/G3)
    """
    __slots__ = ("nos", "caracteres")

    def __init__(self) -> None:
        self.nos = _MAX_NOS
        self.caracteres = _MAX_CARACTERES

    def _texto(self, s: str, teto: int = MAX_TEXTO) -> str:
        n = len(s)
        if n <= teto and n <= self.caracteres:        # caminho comum: cabe
            self.caracteres -= n
            return s
        teto = min(teto, self.caracteres)
        if n > teto:
            s = s[:teto] + _MARCA_CORTE
        self.caracteres = max(0, self.caracteres - len(s))
        return s

    def _chave(self, k) -> str:
        """Chave como o JSON a escreveria (true, null, 1.5). Custa um nó e
        conta no orçamento de texto, mas NUNCA some: são as chaves que a
        investigação lê de um evento reduzido. Esgotado o texto, encurta
        para _MAX_CHAVE_ESGOTADO."""
        self.nos -= 1
        if isinstance(k, str):
            c = k
        elif k is None or isinstance(k, bool):
            c = json.dumps(k)
        elif isinstance(k, int):
            c = (str(int(k)) if -_INT_MAX <= k <= _INT_MAX
                 else f"<int de {k.bit_length()} bits>")
        elif isinstance(k, float):
            c = repr(float(k)) if math.isfinite(k) else str(k)
        else:
            c = f"<{type(k).__name__}>"
        teto = _MAX_CHAVE if self.caracteres > 0 else _MAX_CHAVE_ESGOTADO
        if len(c) > teto:
            c = c[:teto] + _MARCA_CORTE
        self.caracteres = max(0, self.caracteres - len(c))
        return c

    def valor(self, v, nivel: int = 0):
        self.nos -= 1
        if self.nos < 0:
            return _MARCA_CORTE
        t = type(v)
        # Caminho rápido pelos tipos exatos mais comuns; subclasses e o
        # resto caem na cadeia de isinstance logo abaixo (mesmo resultado).
        if t is str:
            return self._texto(v)
        if t is int:
            return v if -_INT_MAX <= v <= _INT_MAX else f"<int de {v.bit_length()} bits>"
        if t is dict:
            return self._dict(v, nivel)
        if t is list or t is tuple:
            return self._lista(v, nivel)
        if v is None or t is bool:
            return v
        if isinstance(v, bool):
            return bool(v)
        if isinstance(v, int):
            return int(v) if -_INT_MAX <= v <= _INT_MAX else f"<int de {v.bit_length()} bits>"
        if isinstance(v, float):
            return float(v) if math.isfinite(v) else str(v)
        if isinstance(v, str):
            return self._texto(v)
        if isinstance(v, (bytes, bytearray, memoryview)):
            return f"<{v.nbytes if isinstance(v, memoryview) else len(v)} bytes>"
        if isinstance(v, dict):
            return self._dict(v, nivel)
        if isinstance(v, (list, tuple)):
            return self._lista(v, nivel)
        if isinstance(v, (set, frozenset)):
            ordenado = self._ordenar(v, nivel)
            if ordenado is None:
                return f"…[conjunto de {len(v)} itens]"
            return [self.valor(x, nivel + 1) for x in ordenado]
        return self._texto(str(v))

    def _dict(self, v, nivel: int) -> object:
        if nivel >= _PROFUNDIDADE:
            return f"…[dict de {len(v)} itens]"
        saida, n = {}, 0
        for k, x in islice(v.items(), _MAX_ITENS):
            if self.nos <= 0:
                break
            saida[self._chave(k)] = self.valor(x, nivel + 1)
            n += 1
        if len(v) > n:
            saida["…"] = f"+{len(v) - n} itens"
        return saida

    def _lista(self, v, nivel: int) -> object:
        if nivel >= _PROFUNDIDADE:
            return f"…[lista de {len(v)} itens]"
        saida = []
        for x in islice(v, _MAX_ITENS):
            if self.nos <= 0:
                break
            saida.append(self.valor(x, nivel + 1))
        if len(v) > len(saida):
            saida.append(f"…+{len(v) - len(saida)} itens")
        return saida

    def _ordenar(self, v, nivel: int):
        """Conjunto pequeno e homogêneo (só texto ou só inteiro) em ordem
        canônica; senão None. Texto é cortado ANTES de ordenar: iguais
        depois do corte saem iguais, seja qual for a ordem de iteração."""
        if nivel >= _PROFUNDIDADE or len(v) > _MAX_ITENS or len(v) > self.nos:
            return None
        if all(isinstance(x, str) for x in v):
            return sorted(x[:MAX_TEXTO + 1] for x in v)
        if all(isinstance(x, int) and not isinstance(x, bool) for x in v):
            return sorted(v)
        return None


def _chaves(dados_convertidos) -> list:
    """Nomes das chaves (ordenados, limitados em quantidade e bytes) — o
    que resta de um evento grande demais, para a investigação saber o que
    havia. Recebe os dados JÁ convertidos: custo limitado (G7)."""
    if not isinstance(dados_convertidos, dict):
        return []
    saida, total = [], 0
    for k in sorted(dados_convertidos)[:_MAX_CHAVES_MARCADOR]:
        k = k[:_MAX_TIPO]
        total += len(_serializar(k)) + 1
        if total > _MAX_BYTES_CHAVES:
            break
        saida.append(k)
    return saida


def ler_cursor(texto: str) -> Optional[Tuple[str, int]]:
    """'<boot_id>:<seq>' → (boot_id, seq), ou None se malformado.
    boot_id: hexadecimal de 8 a 64 caracteres; seq: inteiro ≥ 0."""
    if not isinstance(texto, str) or texto.count(":") != 1:
        return None
    boot, _, seq = texto.partition(":")
    if not (8 <= len(boot) <= 64) or any(c not in "0123456789abcdef" for c in boot):
        return None
    if not seq.isdigit() or len(seq) > 18:
        return None
    return boot, int(seq)


class Registro:
    """Época (boot_id) + sequência (seq) + anel limitado. Um por processo
    (ver eventos.instalar); a API privada o lê, ninguém mais escreve."""

    def __init__(self, capacidade: int = 20000,
                 capacidade_bytes: int = 32 * 1024 * 1024, *,
                 boot_id: Optional[str] = None,
                 relogio: Callable[[], float] = time.time) -> None:
        boot_id = boot_id or secrets.token_hex(8)
        if not (8 <= len(boot_id) <= 64 and set(boot_id) <= _HEX):
            raise ValueError("boot_id: hexadecimal minúsculo de 8 a 64")
        self.boot_id = boot_id                # o cursor dele é sempre legível
        self._relogio = relogio
        self.iniciado_em = relogio()
        self._cap = max(1, int(capacidade))
        self._cap_bytes = max(MAX_EVENTO_BYTES, int(capacidade_bytes))
        self._anel: deque = deque()          # (seq, bytes) — despejo explícito
        self._bytes = 0
        self._ultimo = 0                      # último seq COMPROMETIDO (G3)
        self._lock = threading.Lock()
        self.descartados = 0                  # saíram pela frente (G4)…
        self.descartados_eventos = 0          # …porque o anel encheu em QUANTIDADE
        self.descartados_bytes = 0            # …porque o anel encheu em BYTES
        self.falhas = 0                       # emissões recusadas (G2)
        self.excedidos = 0                    # eventos reduzidos por tamanho (G5)
        self._ouvintes: list = []             # avisados após cada evento novo

    # ── emissão ────────────────────────────────────────────────────
    def _montar(self, seq: int, tipo, dados, corr) -> Tuple[bytes, bool]:
        """(evento serializado ≤ MAX_EVENTO_BYTES, excedeu?). Determinístico:
        os mesmos dados e corr produzem os mesmos `dados`/`corr` gravados.
        Sem efeito colateral — os contadores só mudam depois do append.

        Acima do limite, em degraus fixos (o envelope nunca muda):
          1. dados → {"_excedeu", "_bytes_dados", "_chaves"}
          2. corr  → {"_excedeu", "_bytes_corr"}
          3. dados → {"_excedeu", "_bytes_dados"}   (sem a lista de chaves)
        Se nem assim couber, levanta: o evento é recusado sem consumir seq
        e nenhum evento acima do teto jamais entra no anel (G5)."""
        corr_c = _Corte().valor(corr) if corr else {}
        dados_c = _Corte().valor(dados) if dados else {}
        ev = {"v": ENVELOPE_V, "tipo": str(tipo)[:_MAX_TIPO],
              "boot": self.boot_id, "seq": seq,
              "ts": round(self._relogio(), 6), "corr": corr_c, "dados": dados_c}
        carga = _serializar(ev)
        if len(carga) <= MAX_EVENTO_BYTES:
            return carga, False
        bytes_dados = len(_serializar(dados_c))
        ev["dados"] = {"_excedeu": True, "_bytes_dados": bytes_dados,
                       "_chaves": _chaves(dados_c)}
        carga = _serializar(ev)
        if len(carga) > MAX_EVENTO_BYTES:
            ev["corr"] = {"_excedeu": True,
                          "_bytes_corr": len(_serializar(corr_c))}
            carga = _serializar(ev)
        if len(carga) > MAX_EVENTO_BYTES:
            ev["dados"] = {"_excedeu": True, "_bytes_dados": bytes_dados}
            carga = _serializar(ev)
        if len(carga) > MAX_EVENTO_BYTES:
            raise ValueError("evento acima do teto mesmo reduzido")
        return carga, True

    def emitir(self, tipo, dados=None, corr=None) -> int:
        """Registra um evento e devolve o seq dele (0 = recusado).

        Síncrono, sem I/O (G1). Tudo numa seção crítica só: o seq
        candidato só é COMPROMETIDO depois do append (G3). Nunca levanta
        (G2)."""
        try:
            with self._lock:
                seq = self._ultimo + 1
                carga, excedeu = self._montar(seq, tipo, dados, corr)
                self._anel.append((seq, carga))
                self._bytes += len(carga)
                self._ultimo = seq                     # G3: só agora
                if excedeu:
                    self.excedidos += 1
                while (len(self._anel) > self._cap
                       or self._bytes > self._cap_bytes):
                    if len(self._anel) > self._cap:
                        self.descartados_eventos += 1
                    else:
                        self.descartados_bytes += 1
                    _, velho = self._anel.popleft()
                    self._bytes -= len(velho)
                    self.descartados += 1
        except Exception:
            try:
                with self._lock:
                    self.falhas += 1
            except Exception:
                pass
            return 0
        self._avisar()
        return seq

    # ── espera longa ───────────────────────────────────────────────
    def adicionar_ouvinte(self, fn: Callable[[], None]) -> None:
        self._ouvintes.append(fn)

    def remover_ouvinte(self, fn: Callable[[], None]) -> None:
        try:
            self._ouvintes.remove(fn)
        except ValueError:
            pass

    def _avisar(self) -> None:
        """Avisa quem espera evento novo. Sem ouvinte (o normal), custa
        uma checagem. Ouvinte com defeito nunca chega a quem emitiu."""
        if not self._ouvintes:
            return
        for fn in tuple(self._ouvintes):
            try:
                fn()
            except Exception:
                pass

    # ── leitura ────────────────────────────────────────────────────
    def ler(self, cursor: Optional[Tuple[str, int]], limite: int,
            max_bytes: int) -> dict:
        """Eventos depois do cursor, com a CONTINUIDADE declarada.

        continuidade:
          inicio .......... sem cursor: do mais antigo do anel
          continua ........ mesmo boot, nada perdido
          lacuna .......... mesmo boot, o anel já descartou parte
          nova_epoca ...... cursor de OUTRO boot: nunca é continuado; lê do
                            início desta época e declara perdido o fim (não
                            visto) da época anterior
          cursor_invalido . mesmo boot com seq à frente do último

        Devolve {"meta": {...}, "cargas": [bytes, ...]}. `meta["cursor"]`
        é o cursor a usar na próxima leitura."""
        limite = max(1, int(limite))
        with self._lock:
            primeiro = self._anel[0][0] if self._anel else self._ultimo + 1
            ultimo = self._ultimo
            lacunas: list = []
            epoca_anterior = None
            perdido_inicio = ({"boot": self.boot_id, "de": 1, "ate": primeiro - 1}
                              if primeiro > 1 else None)
            if cursor is None:
                continuidade, desde = "inicio", 0
                if perdido_inicio:
                    lacunas.append(perdido_inicio)
            else:
                boot_c, seq_c = cursor
                if boot_c != self.boot_id:
                    continuidade, desde = "nova_epoca", 0
                    epoca_anterior = {"boot": boot_c, "ultimo_seq_recebido": seq_c}
                    lacunas.append({"boot": boot_c, "de": seq_c + 1, "ate": None})
                    if perdido_inicio:
                        lacunas.append(perdido_inicio)
                elif seq_c > ultimo:
                    continuidade, desde = "cursor_invalido", 0
                    if perdido_inicio:
                        lacunas.append(perdido_inicio)
                elif seq_c + 1 < primeiro:
                    continuidade, desde = "lacuna", seq_c
                    lacunas.append({"boot": self.boot_id, "de": seq_c + 1,
                                    "ate": primeiro - 1})
                else:
                    continuidade, desde = "continua", seq_c
            n = len(self._anel)
            inicio = max(0, desde + 1 - primeiro)        # posição no anel
            fim = min(n, inicio + limite)
            if inicio >= n:
                candidatos: object = ()
            elif n - inicio < inicio:
                # Perto do fim — o caso normal do Brain em dia: anda do fim,
                # O(n - inicio), sem percorrer o anel inteiro.
                candidatos = list(islice(reversed(self._anel), n - fim, n - inicio))
                candidatos.reverse()
            else:
                candidatos = islice(self._anel, inicio, fim)
            cargas: list = []
            total = 0
            ultimo_entregue = max(desde, primeiro - 1)
            for seq, carga in candidatos:
                if cargas and total + len(carga) > max_bytes:
                    break
                cargas.append(carga)
                total += len(carga)
                ultimo_entregue = seq
        meta = {
            "v": ENVELOPE_V,
            "boot_id": self.boot_id,
            "boot_iniciado_em": self.iniciado_em,
            "continuidade": continuidade,
            "lacunas": lacunas,
            "epoca_anterior": epoca_anterior,
            "seq_primeiro": primeiro,
            "seq_ultimo": ultimo,
            "cursor": f"{self.boot_id}:{ultimo_entregue}",
            "quantidade": len(cargas),
        }
        return {"meta": meta, "cargas": cargas}

    def saude(self) -> dict:
        """Retrato do registro, só memória. Ocupação e limite em EVENTOS e
        em BYTES, lado a lado, e o descarte por causa: diz qual dos dois
        limites manda de fato."""
        with self._lock:
            primeiro = self._anel[0][0] if self._anel else self._ultimo + 1
            return {
                "v": ENVELOPE_V,
                "boot_id": self.boot_id,
                "boot_iniciado_em": self.iniciado_em,
                "seq_primeiro": primeiro,
                "seq_ultimo": self._ultimo,
                "eventos_anel": len(self._anel),
                "max_eventos_anel": self._cap,
                "bytes_anel": self._bytes,
                "max_bytes_anel": self._cap_bytes,
                "descartados": self.descartados,
                "descartados_por_eventos": self.descartados_eventos,
                "descartados_por_bytes": self.descartados_bytes,
                "falhas_emissao": self.falhas,
                "excedidos": self.excedidos,
                "no_ar_s": round(max(0.0, self._relogio() - self.iniciado_em), 3),
            }
