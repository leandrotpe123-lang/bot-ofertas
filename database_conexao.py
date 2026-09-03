"""Camada 7 — Banco / Conexão, mutex e schema.

Responsabilidade ÚNICA: a conexão SQLite do processo, o mutex que a
serializa, os PRAGMAs, o schema e o contexto de acesso `_db()`.

NÃO conhece regra de nenhum domínio: não sabe o que é um cupom, um
post ou um link. Fica ABAIXO dos módulos de domínio; não importa
nenhum deles.

══════════════════════════════════════════════════════════════════
CONTRATO INTERNO DA CAMADA — LEIA ANTES DE ALTERAR
══════════════════════════════════════════════════════════════════
_db tem prefixo `_`, mas NÃO é privado deste arquivo: é o ponto de
acesso usado por TODOS os módulos de domínio (database_links,
database_posts, database_cupons, database_manutencao) e também por
main.py, via a fachada `database`. O underscore foi PRESERVADO na
extração para não renomear nada fora de escopo — dívida registrada.

_db_conn e _db_lock são de fato internos: nenhum módulo os importa,
e importá-los seria BUG. `from ... import _db_conn` liga por VALOR e
capturaria o None anterior a _init_db(), nunca a conexão real.
Os domínios devem sempre usar `_db()`, que lê _db_conn na chamada.

_init_db mora aqui por restrição da linguagem, não por escolha: ele
declara `global _db_conn`, e esse global é o DESTE módulo. Movê-lo
para outro arquivo faria _db() enxergar None para sempre. Não separe.

══════════════════════════════════════════════════════════════════
ARMAZENAMENTO — ESTE MÓDULO É O ÚNICO QUE TOCA O FILESYSTEM
══════════════════════════════════════════════════════════════════
`config` DECLARA o caminho (_DB_PATH); este módulo o VALIDA e o
ABRE. A separação é deliberada: config é folha do grafo de imports
e é importado por plataformas, pipeline e saída — tocar disco lá
derrubaria o boot inteiro antes de existir qualquer log.

O banco NÃO é um arquivo só. `journal_mode=WAL`, ligado abaixo,
mantém `<db>-wal` e `<db>-shm` NO MESMO DIRETÓRIO do `.db`, e uma
transação confirmada pode viver apenas no `-wal` até o checkpoint.
Portanto o que precisa ser persistente é o DIRETÓRIO inteiro, não o
arquivo. Todo o WAL funciona dentro dele, sem caminho auxiliar em
outro lugar.

Duas naturezas de caminho, tratadas de forma diferente de propósito:

  MONTAGEM PERSISTENTE (config._DB_MONTAGENS_PERSISTENTES, ex. /data)
      o diretório TEM de existir e ser gravável. Ausência significa
      volume não montado — erro de infraestrutura. Falha explícita,
      com a causa nomeada no log.

  CAMINHO LOCAL (default "foguetao.db", ou qualquer outro)
      o diretório é criado se faltar. É o modo de desenvolvimento e
      preserva byte a byte o comportamento anterior a esta frente.

Por que NÃO cair para o disco efêmero quando o volume falta: o bot
subiria, publicaria links curtos e os perderia no deploy seguinte —
o redirect devolveria "Link não encontrado" para tudo que já foi
ao ar. Falha silenciosa com dano acumulado é pior que não subir.

Extraído de database sem qualquer alteração de comportamento; a
validação de armazenamento é a única adição desde então.
"""
from __future__ import annotations
import os
import sqlite3
from contextlib import contextmanager
from threading import Lock
from typing import Optional

from config import _DB_MONTAGENS_PERSISTENTES, _DB_PATH
from logger import log_db

_db_conn: Optional[sqlite3.Connection] = None
_db_lock  = Lock()


class ArmazenamentoIndisponivel(RuntimeError):
    """O armazenamento persistente esperado não está utilizável.

    Levantada ANTES de abrir a conexão, para que a causa apareça
    nomeada no log em vez de virar um `unable to open database file`
    genérico no meio do boot.
    """


def _base_persistente(diretorio: str) -> str:
    """Base de montagem persistente que contém o diretório, ou "".

    Responde apenas pela INTENÇÃO declarada — "este caminho deveria
    estar num volume" — e não pelo fato de estar. Quem responde pelo
    fato é _volume_montado.
    """
    alvo = os.path.abspath(diretorio)
    for base in _DB_MONTAGENS_PERSISTENTES:
        raiz = os.path.abspath(base)
        if alvo == raiz or alvo.startswith(raiz + os.sep):
            return raiz
    return ""


def _pontos_de_montagem() -> set:
    """Pontos de montagem vistos pelo kernel, via /proc/self/mountinfo.

    É a ÚNICA fonte confiável. Medido neste projeto: com um bind
    mount, os.path.ismount devolve False e o st_dev é idêntico ao do
    pai — os dois falham. O mountinfo acerta. Um falso negativo aqui
    derrubaria o boot de um serviço corretamente configurado, então a
    fonte tem de ser a autoritativa.

    Conjunto vazio quando /proc não existe (macOS, dev local): o
    chamador cai para os.path.ismount, suficiente fora de container.
    """
    try:
        with open("/proc/self/mountinfo", encoding="utf-8") as f:
            return {c[4] for c in (linha.split() for linha in f) if len(c) > 4}
    except OSError:
        return set()


def _volume_montado(diretorio: str, base: str) -> bool:
    """O volume está REALMENTE montado sob `base`?

    Sobe de `diretorio` até `base` inclusive — nunca além. Parar em
    `base` é essencial: "/" é sempre um ponto de montagem, e subir
    até lá faria qualquer caminho parecer persistente.

    Cobre o caso de o banco morar numa subpasta do volume
    (DB_PATH=/data/db/foguetao.db com o volume em /data).
    """
    pontos = _pontos_de_montagem()
    atual = os.path.abspath(diretorio)
    raiz = os.path.abspath(base)
    while True:
        if (atual in pontos) if pontos else os.path.ismount(atual):
            return True
        if atual == raiz:
            return False
        pai = os.path.dirname(atual)
        if pai == atual:
            return False
        atual = pai


def _preparar_diretorio(caminho: str) -> str:
    """Garante que o diretório do banco existe e é gravável.

    Devolve o caminho absoluto do banco. Levanta
    ArmazenamentoIndisponivel quando uma montagem persistente não
    está disponível — nunca contorna caindo para outro diretório.
    """
    absoluto = os.path.abspath(caminho)
    diretorio = os.path.dirname(absoluto) or "."
    base = _base_persistente(diretorio)

    if base:
        # A BASE tem de existir e ser um volume de verdade. Diretório
        # existir NÃO prova montagem: um /data criado pela imagem, por
        # um makedirs anterior ou por um deploy sem volume anexado é
        # diretório comum no disco efêmero. Subir sobre ele é a falha
        # silenciosa que esta frente existe para impedir — o bot
        # publicaria links curtos e os perderia no deploy seguinte.
        # Declarar persistência e não ter volume é erro de
        # infraestrutura: falha, não contorna.
        if not os.path.isdir(base):
            raise ArmazenamentoIndisponivel(
                f"volume persistente não montado: {base} não existe "
                f"(DB_PATH={caminho})"
            )
        if not _volume_montado(base, base):
            raise ArmazenamentoIndisponivel(
                f"{base} existe mas NÃO é um volume montado — o volume "
                f"persistente não está anexado ao serviço "
                f"(DB_PATH={caminho})"
            )

    # Com o volume comprovado, subpasta ausente DENTRO dele é caso
    # normal e pode ser criada: o que persiste é o volume, e tudo
    # abaixo dele herda a persistência. Fora de montagem persistente,
    # é o modo local e o diretório também é criado.
    if not os.path.isdir(diretorio):
        try:
            os.makedirs(diretorio, exist_ok=True)
        except OSError as e:
            raise ArmazenamentoIndisponivel(
                f"não foi possível criar {diretorio}: {e}"
            ) from e

    # Gravável importa mais que existente: o WAL precisa CRIAR
    # `<db>-wal` e `<db>-shm` ao lado do `.db`. Um diretório
    # somente-leitura deixaria o banco abrir e falhar na escrita.
    #
    # A verificação é uma SONDA REAL, não os.access: o processo roda
    # como root no container, e root ignora os bits de permissão —
    # os.access(W_OK) devolveria True num diretório 0555. Uma
    # montagem read-only, que é a falha real a detectar, recusa a
    # escrita mesmo para root. Só a tentativa prova.
    sonda = os.path.join(diretorio, ".foguetao_write_probe")
    try:
        with open(sonda, "w"):
            pass
    except OSError as e:
        raise ArmazenamentoIndisponivel(
            f"sem permissão de escrita em {diretorio} (DB_PATH={caminho}): {e}"
        ) from e
    finally:
        try:
            os.unlink(sonda)
        except OSError:
            pass

    return absoluto


def _init_db():
    global _db_conn
    caminho = _preparar_diretorio(_DB_PATH)
    persistente = bool(_base_persistente(os.path.dirname(caminho)))

    _db_conn = sqlite3.connect(
        caminho, check_same_thread=False, timeout=10, isolation_level=None)
    for p in [
        "PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL",
        "PRAGMA cache_size=-16000", "PRAGMA temp_store=MEMORY",
        "PRAGMA busy_timeout=10000",
    ]:
        _db_conn.execute(p)
    _db_conn.executescript("""
        CREATE TABLE IF NOT EXISTS links_cache(
            url_orig TEXT PRIMARY KEY, url_conv TEXT NOT NULL,
            url_canon TEXT, plat TEXT NOT NULL, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS short_links(
            code TEXT PRIMARY KEY, url TEXT NOT NULL, ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS post_estado(
            msg_id_dest INTEGER PRIMARY KEY,
            score INTEGER NOT NULL DEFAULT 0, texto TEXT NOT NULL DEFAULT '',
            plat TEXT NOT NULL DEFAULT '', lider TEXT DEFAULT '',
            janela_fim REAL DEFAULT 0, edit_count INTEGER DEFAULT 0,
            ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS oferta_index(
            identity TEXT PRIMARY KEY, msg_id_dest INTEGER NOT NULL,
            ts REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS cupom_idx(
            plat TEXT NOT NULL, codigo TEXT NOT NULL,
            identity TEXT NOT NULL, ts REAL NOT NULL,
            PRIMARY KEY(plat, codigo));
        CREATE TABLE IF NOT EXISTS origem_post(
            chat TEXT NOT NULL, msg_id INTEGER NOT NULL,
            dest INTEGER NOT NULL, ts REAL NOT NULL,
            PRIMARY KEY(chat, msg_id));
        CREATE INDEX IF NOT EXISTS idx_lc_plat     ON links_cache(plat);
        CREATE INDEX IF NOT EXISTS idx_lc_ts       ON links_cache(ts);
        CREATE INDEX IF NOT EXISTS idx_sl_code     ON short_links(code);
        CREATE INDEX IF NOT EXISTS idx_oi_dest     ON oferta_index(msg_id_dest);
        CREATE INDEX IF NOT EXISTS idx_ci_lookup   ON cupom_idx(plat,codigo,ts);
    """)
    for tabela, col, tipo in [
        ("links_cache",   "url_canon",        "TEXT"),
        ("post_estado",   "midia_chat",       "TEXT"),
        ("post_estado",   "score_versao",     "INTEGER"),
    ]:
        try:
            _db_conn.execute(f"ALTER TABLE {tabela} ADD COLUMN {col} {tipo}")
        except sqlite3.OperationalError:
            pass

    # O caminho EFETIVO e o modo de journal vão para o log do boot:
    # é a evidência operacional de que o estado está no volume, e a
    # primeira coisa a conferir quando um link curto some.
    try:
        journal = _db_conn.execute("PRAGMA journal_mode").fetchone()[0]
    except sqlite3.Error:
        journal = "?"
    log_db.info(
        f"🗄 DB ON | {caminho} | journal={journal} | "
        f"armazenamento={'persistente' if persistente else 'local'}"
    )

@contextmanager
def _db():
    with _db_lock:
        try:
            yield _db_conn
        except sqlite3.Error as e:
            log_db.error(f"❌ DB: {e}"); raise


def _fechar_db() -> None:
    """Fecha a conexao do processo. Idempotente. [E3.4]

    Mora aqui pela mesma restricao ja' documentada de _init_db: declara
    `global _db_conn`, e esse global e' DESTE modulo.

    O checkpoint TRUNCATE consolida o WAL no .db antes de fechar, para
    que o proximo boot nao herde um WAL grande. Falha no checkpoint NAO
    impede o close.
    """
    global _db_conn
    if _db_conn is None:
        return
    with _db_lock:
        if _db_conn is None:
            return
        try:
            _db_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception as e:
            log_db.warning(f"⚠️ checkpoint WAL: {e}")
        try:
            _db_conn.close()
        finally:
            _db_conn = None
    log_db.info("🗄 DB fechado")

                             
