"""
E5.0 — Harness de teste contra a ARVORE REAL.

REGRA DESTE HARNESS:
    Nenhum arquivo do projeto e vendorado, copiado ou reimplementado.
    Todos os modulos sob teste sao importados do repositorio real.

    O UNICO faking permitido aqui e de BIBLIOTECAS DE TERCEIROS que
    nao estao instalaveis neste ambiente (telethon, aiohttp). Elas sao
    a fronteira de I/O externa — nunca logica do Foguetao.

EXECUCAO:
    stdlib-only / standalone; compatibilidade pytest NAO validada nesta
    fase (o sandbox nao possui pytest e as funcoes de teste usam a
    assinatura test_xxx(r), que nao e a coleta padrao do pytest).

        python tests/test_e5_baseline.py
        python tests/test_e5_estrutura.py
        python tests/test_e5_frente.py

    Segue a convencao ja estabelecida em tests/test_uma_por_vez.py:
    roda sem nenhuma dependencia alem da biblioteca padrao. Nenhuma
    dependencia nova foi adicionada ao projeto.
"""
import os
import sys
import types
import tempfile

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ─────────────────────────────────────────────────────────────────
# 1. Fronteira de terceiros — telethon
# ─────────────────────────────────────────────────────────────────
def _instalar_telethon() -> None:
    if "telethon" in sys.modules:
        return

    telethon = types.ModuleType("telethon")
    errors = types.ModuleType("telethon.errors")
    tl = types.ModuleType("telethon.tl")
    tl_types = types.ModuleType("telethon.tl.types")
    sessions = types.ModuleType("telethon.sessions")
    utils_mod = types.ModuleType("telethon.utils")
    events = types.ModuleType("telethon.events")

    # FAKES da fronteira de terceiros. NAO sao as classes do Telethon:
    # sao substitutos que reproduzem exatamente a hierarquia e os
    # atributos que o Foguetao consome — heranca de Exception (para
    # `except FloodWaitError` funcionar) e o atributo `.seconds`, lido
    # por saida.py. Nada alem disso e reproduzido.
    class RPCError(Exception):
        pass

    class FloodWaitError(RPCError):
        def __init__(self, seconds: int = 0):
            super().__init__(f"flood wait {seconds}")
            self.seconds = seconds

    class MessageNotModifiedError(RPCError):
        pass

    class AuthKeyUnregisteredError(RPCError):
        pass

    class SessionPasswordNeededError(RPCError):
        pass

    for _n, _c in (("RPCError", RPCError),
                   ("FloodWaitError", FloodWaitError),
                   ("MessageNotModifiedError", MessageNotModifiedError),
                   ("AuthKeyUnregisteredError", AuthKeyUnregisteredError),
                   ("SessionPasswordNeededError", SessionPasswordNeededError)):
        setattr(errors, _n, _c)

    # FAKES de tipo: reproduzem o que ingestao.chave_midia consome via
    # isinstance() e os atributos .photo.id / .document.id. Nao sao os
    # tipos do Telethon.
    class _Media:
        pass

    class MessageMediaWebPage(_Media):
        pass

    class MessageMediaPhoto(_Media):
        def __init__(self, photo=None):
            self.photo = photo

    class MessageMediaDocument(_Media):
        def __init__(self, document=None):
            self.document = document

    class MessageEntityCode:
        def __init__(self, offset=0, length=0):
            self.offset, self.length = offset, length

    class MessageEntityPre:
        def __init__(self, offset=0, length=0, language=""):
            self.offset, self.length = offset, length

    for _n, _c in (("MessageMediaWebPage", MessageMediaWebPage),
                   ("MessageMediaPhoto", MessageMediaPhoto),
                   ("MessageMediaDocument", MessageMediaDocument),
                   ("MessageEntityCode", MessageEntityCode),
                   ("MessageEntityPre", MessageEntityPre)):
        setattr(tl_types, _n, _c)

    class StringSession:
        def __init__(self, *a, **k):
            pass

    sessions.StringSession = StringSession

    class TelegramClient:
        def __init__(self, *a, **k):
            pass

    telethon.TelegramClient = TelegramClient
    telethon.events = events
    telethon.errors = errors
    telethon.tl = tl
    tl.types = tl_types
    telethon.utils = utils_mod
    utils_mod.get_peer_id = lambda ent: getattr(ent, "id", 0)

    class NewMessage:
        def __init__(self, *a, **k):
            pass

    class MessageEdited:
        def __init__(self, *a, **k):
            pass

    events.NewMessage = NewMessage
    events.MessageEdited = MessageEdited

    sys.modules.update({
        "telethon": telethon, "telethon.errors": errors,
        "telethon.tl": tl, "telethon.tl.types": tl_types,
        "telethon.sessions": sessions, "telethon.utils": utils_mod,
        "telethon.events": events,
    })


# ─────────────────────────────────────────────────────────────────
# 2. Fronteira de terceiros — aiohttp
# ─────────────────────────────────────────────────────────────────
def _instalar_aiohttp() -> None:
    if "aiohttp" in sys.modules:
        return
    aiohttp = types.ModuleType("aiohttp")

    class ClientSession:
        def __init__(self, *a, **k):
            self.closed = False

        async def close(self):
            self.closed = True

    class TCPConnector:
        def __init__(self, *a, **k):
            pass

    class ClientTimeout:
        def __init__(self, *a, **k):
            pass

    class ClientError(Exception):
        pass

    web = types.ModuleType("aiohttp.web")
    for _n in ("Application", "AppRunner", "TCPSite", "Response", "Request",
               "HTTPFound"):
        setattr(web, _n, type(_n, (object,), {}))

    aiohttp.ClientSession = ClientSession
    aiohttp.TCPConnector = TCPConnector
    aiohttp.ClientTimeout = ClientTimeout
    aiohttp.ClientError = ClientError
    aiohttp.web = web
    sys.modules["aiohttp"] = aiohttp
    sys.modules["aiohttp.web"] = web


# ─────────────────────────────────────────────────────────────────
# 3. Bootstrap
# ─────────────────────────────────────────────────────────────────
_DB_TMP = None


def preparar() -> str:
    """Prepara o ambiente e devolve a raiz do repositorio real."""
    global _DB_TMP
    _instalar_telethon()
    _instalar_aiohttp()
    if _DB_TMP is None:
        _DB_TMP = tempfile.mkdtemp(prefix="foguetao_teste_")
        os.environ["DB_PATH"] = os.path.join(_DB_TMP, "teste.db")
    os.environ.setdefault("RAILWAY_PUBLIC_DOMAIN", "teste.local")
    os.environ.setdefault("API_ID", "1")
    os.environ.setdefault("API_HASH", "x")
    if RAIZ not in sys.path:
        sys.path.insert(0, RAIZ)
    return RAIZ


def caminho(rel: str) -> str:
    """Caminho absoluto de um arquivo DA ARVORE REAL."""
    return os.path.join(RAIZ, rel)


def ler_fonte(rel: str) -> str:
    with open(caminho(rel), encoding="utf-8") as f:
        return f.read()


# ─────────────────────────────────────────────────────────────────
# 4. Runner standalone (convencao da casa: sem dependencia nova)
# ─────────────────────────────────────────────────────────────────
class Resultado:
    def __init__(self) -> None:
        self.asserts = 0
        self.falhas = []

    def check(self, cond, nome, detalhe=""):
        self.asserts += 1
        if not cond:
            self.falhas.append(f"{nome} :: {detalhe}")
        return bool(cond)


def rodar(modulo_globals, titulo: str) -> int:
    """Executa toda funcao test_* do modulo. Devolve codigo de saida."""
    testes = [v for k, v in sorted(modulo_globals.items())
              if k.startswith("test_") and callable(v)]
    total_asserts = 0
    falhas_totais = []
    verdes = 0
    print("=" * 66)
    print(titulo)
    print("=" * 66)
    for t in testes:
        r = Resultado()
        try:
            t(r)
            erro = None
        except Exception as e:                     # noqa: BLE001
            erro = f"{type(e).__name__}: {e}"
        total_asserts += r.asserts
        if erro:
            falhas_totais.append(f"{t.__name__} :: EXCECAO {erro}")
            print(f"  FALHOU  {t.__name__:<44} excecao")
        elif r.falhas:
            falhas_totais.extend(f"{t.__name__} :: {f}" for f in r.falhas)
            print(f"  FALHOU  {t.__name__:<44} {len(r.falhas)}/{r.asserts}")
        else:
            verdes += 1
            print(f"  ok      {t.__name__:<44} {r.asserts} asserts")
    print("-" * 66)
    print(f"TESTES: {verdes}/{len(testes)} verdes | ASSERCOES: {total_asserts} "
          f"| FALHAS: {len(falhas_totais)}")
    if falhas_totais:
        print("-" * 66)
        for f in falhas_totais[:30]:
            print("   >", f)
    print("=" * 66)
    return 1 if falhas_totais else 0
