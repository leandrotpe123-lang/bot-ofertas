"""
Rede de regressão — CONGELAMENTO de identidade_canonica (Passo 1.0).

Fixa o comportamento ATUAL da identidade_canonica ANTES de unificá-la
(Passo 1.1). Os testes [MUDA_NO_1_1] falham DE PROPÓSITO quando a chave
canônica mudar; os demais seguem verdes.

BLINDAGEM (por teste, via _ambiente_isolado):
  1) BANCO: salva/redireciona pra um temp/_init_db()/restaura (config._DB_PATH,
     database._DB_PATH, database._db_conn). Schema criado com o MESMO init que
     main.py:86 usa. Nunca toca o foguetao.db.
  2) ESTADO DE MÓDULO: fotografa os globais de TODOS os módulos do projeto na
     cadeia (deduplicacao, globals, utils.*, pipeline.*) — exceto os tratados à
     parte (database/config pelo isolamento de DB; logger por ser infra de log).
     A checagem e a RESTAURAÇÃO rodam no finally — válidas MESMO SE O TESTE
     FALHAR antes do fim, pra não deixar vazamento silencioso entre testes.
     Vazamento num teste que PASSOU vira falha (superficar). Num teste que já
     falhou, o erro original vence, mas o estado é restaurado (sem contaminar)
     e o vazamento é avisado (warnings) — nada fica silencioso.

LIMITE HONESTO: database.py guarda estado em globais de módulo sem injeção.
Isolar exige mutar esses globais DURANTE o teste; a blindagem garante que não
sobra resíduo. Seguro com pytest SEQUENCIAL (o padrão). Execução PARALELA
(pytest-xdist) exigiria injeção real de conexão no database.

    pytest tests/test_identidade_canonica.py
    python tests/test_identidade_canonica.py
"""
import contextlib
import os
import shutil
import sys
import tempfile
import warnings

# Repo root no sys.path (idêntico ao golden_scenarios).
_d = os.path.dirname(os.path.abspath(__file__))
while _d != os.path.dirname(_d):
    if os.path.isdir(os.path.join(_d, "pipeline")) and \
       os.path.isdir(os.path.join(_d, "utils")):
        if _d not in sys.path:
            sys.path.insert(0, _d)
        break
    _d = os.path.dirname(_d)
_REPO_ROOT = _d

import config
import database as _dbmod
from pipeline.deduplicacao import identidade_canonica
from pipeline.normalizacao import MensagemNormalizada

_SENT = object()
# Tratados à parte: DB pelo isolamento; logger é infra de log, não estado de identidade.
_FORA_DA_VIGILANCIA = {"database", "config", "logger", "_bootstrap_stubs"}


def _modulos_vigiados():
    """Módulos do PROJETO carregados (sob o repo root), menos os tratados à parte."""
    out = {}
    for name, m in list(sys.modules.items()):
        if m is None or name in _FORA_DA_VIGILANCIA:
            continue
        f = getattr(m, "__file__", None)
        if isinstance(f, str) and f.startswith(_REPO_ROOT + os.sep):
            out[name] = m
    return out


def _snapshot(mods):
    """Para DETECÇÃO: mutáveis por conteúdo; o resto por id()."""
    s = {}
    for name, m in mods.items():
        for k, v in vars(m).items():
            if k.startswith("__"):
                continue
            key = f"{name}.{k}"
            try:
                if isinstance(v, dict):
                    s[key] = ("dict", tuple(sorted((str(a), repr(b)) for a, b in v.items())))
                elif isinstance(v, (set, frozenset)):
                    s[key] = ("set", tuple(sorted(repr(x) for x in v)))
                elif isinstance(v, (list, tuple)):
                    s[key] = ("seq", tuple(repr(x) for x in v))
                else:
                    s[key] = ("id", id(v))
            except Exception:
                s[key] = ("id", id(v))
    return s


def _diff(antes, depois):
    return {k: (antes.get(k), depois.get(k))
            for k in set(antes) | set(depois) if antes.get(k) != depois.get(k)}


def _capturar(mods):
    """Para RESTAURO: (objeto original, cópia rasa se container) por nome/módulo."""
    cap = {}
    for name, m in mods.items():
        nm = {}
        for k, v in vars(m).items():
            if k.startswith("__"):
                continue
            if isinstance(v, (dict, list, set)):
                try:
                    nm[k] = (v, type(v)(v))   # objeto original + cópia rasa do conteúdo
                except Exception:
                    nm[k] = (v, _SENT)
            else:
                nm[k] = (v, _SENT)
        cap[name] = (m, nm)
    return cap


def _restaurar(cap):
    """Desfaz rebindings, mutações in-place de containers e nomes novos."""
    for name, (m, nm) in cap.items():
        atual = vars(m)
        salvos = set(nm)
        for k in list(atual.keys()):              # remove nomes criados no teste
            if not k.startswith("__") and k not in salvos:
                try:
                    delattr(m, k)
                except Exception:
                    pass
        for k, (obj, copia) in nm.items():        # restaura os salvos
            if atual.get(k, _SENT) is not obj:     # rebindado -> devolve original
                try:
                    setattr(m, k, obj)
                except Exception:
                    pass
            if copia is not _SENT:                 # container -> restaura conteúdo
                try:
                    if isinstance(obj, dict):
                        obj.clear(); obj.update(copia)
                    elif isinstance(obj, set):
                        obj.clear(); obj.update(copia)
                    elif isinstance(obj, list):
                        obj[:] = copia
                except Exception:
                    pass


@contextlib.contextmanager
def _ambiente_isolado():
    """Isola o BANCO (temp) e VIGIA/RESTAURA o estado de módulo do projeto."""
    orig_cfg = config._DB_PATH
    orig_db = _dbmod._DB_PATH
    orig_conn = _dbmod._db_conn
    tmp_dir = tempfile.mkdtemp(prefix="golden_canonica_")
    tmp_db = os.path.join(tmp_dir, "isolado.db")
    config._DB_PATH = tmp_db
    _dbmod._DB_PATH = tmp_db
    _dbmod._db_conn = None
    _dbmod._init_db()  # MESMO init do fluxo real (main.py:86)
    assert "foguetao.db" not in (_dbmod._DB_PATH or ""), \
        "ISOLAMENTO FALHOU — abortando p/ nao tocar o banco real"

    mods = _modulos_vigiados()
    snap_antes = _snapshot(mods)
    cap = _capturar(mods)

    err = None
    vazou = {}
    try:
        yield tmp_db
    except BaseException as e:  # teste falhou por conta própria
        err = e
    finally:
        # checagem + restauro SEMPRE — válidos mesmo se o teste falhou
        try:
            vazou = _diff(snap_antes, _snapshot(_modulos_vigiados()))
        except Exception:
            vazou = {}
        try:
            _restaurar(cap)
        except Exception:
            pass
        try:
            if _dbmod._db_conn is not None:
                _dbmod._db_conn.close()
        except Exception:
            pass
        config._DB_PATH = orig_cfg
        _dbmod._DB_PATH = orig_db
        _dbmod._db_conn = orig_conn
        shutil.rmtree(tmp_dir, ignore_errors=True)

    if err is not None:
        if vazou:  # falha original vence, mas o vazamento não fica silencioso
            warnings.warn(f"teste falhou E vazou estado de modulo (restaurado): "
                          f"{sorted(vazou)[:5]}", stacklevel=2)
        raise err
    if vazou:
        raise AssertionError(f"VAZOU estado de modulo: {dict(list(vazou.items())[:5])}")

# Sob pytest: fixture autouse aplica o ambiente isolado a CADA teste.
# Standalone: o runner do __main__ faz o mesmo via context manager.
try:
    import pytest

    @pytest.fixture(autouse=True)
    def _ambiente():
        with _ambiente_isolado():
            yield
except ImportError:
    pass


def _norm(texto="", *, plat="shopee", cupom="", sku="", ids_globais=None,
          chave_campanha="", chaves_campanha=None, code_entities=None,
          tem_midia=False, tem_sinal_cashback=False, tem_host_campanha=False,
          mapa=None, chat="@grupo"):
    return MensagemNormalizada(
        msg_id=1, chat=chat, texto_limpo=texto, mapa=mapa or {}, preservar=[],
        plat=plat, cupom=cupom, sku=sku, tem_midia=tem_midia, media_obj=None,
        ids_globais=ids_globais or [], chave_campanha=chave_campanha,
        chaves_campanha=chaves_campanha or [], tem_host_campanha=tem_host_campanha,
        tem_sinal_cashback=tem_sinal_cashback, code_entities=code_entities or [],
    )


# ══════════ Identidades que NÃO mudam no 1.1 (continuam iguais) ══════════

def test_produto_unico():
    n = _norm("produto", plat="amazon", ids_globais=["B0XYZ"])
    assert identidade_canonica(n) == "amazon|B0XYZ"


def test_campanha_por_url():
    n = _norm("evento", plat="shopee", tem_host_campanha=True,
              chave_campanha="live.shopee.com.br/aggregation")
    assert identidade_canonica(n) == "shopee|camp|live.shopee.com.br/aggregation"


def test_campanha_por_keyword():
    n = _norm("Black Friday chegou", plat="shopee")
    assert identidade_canonica(n) == "shopee|camp|black friday"


def test_cashback():
    n = _norm("Cashback 30% na compra", plat="shopee", tem_sinal_cashback=True)
    assert identidade_canonica(n) == "shopee|cash|30"


def test_cupom_unico():
    n = _norm("cupom CURTEAI", plat="amazon", cupom="CURTEAI",
              code_entities=["CURTEAI"])
    assert identidade_canonica(n) == "amazon|cup|CURTEAI"


def test_dois_posts_mesmo_cupom_compartilham_canonica():
    # Comportamento STATEFUL do cupom_idx: o 2º post com o mesmo código
    # reusa a identidade do 1º (ambas as chamadas no MESMO banco temp do teste).
    a = _norm("cupom SHAREME", plat="amazon", cupom="SHAREME",
              code_entities=["SHAREME"])
    b = _norm("cupom SHAREME outro produto", plat="amazon", cupom="SHAREME",
              code_entities=["SHAREME"])
    c1 = identidade_canonica(a)
    c2 = identidade_canonica(b)
    assert c1 == c2 == "amazon|cup|SHAREME"


def test_fallback_url():
    n = _norm("texto sem oferta", plat="shopee", mapa={"a": "https://x.com/p"})
    assert identidade_canonica(n).startswith("shopee|url|")


def test_fallback_texto():
    n = _norm("texto puro sem nada", plat="shopee")
    assert identidade_canonica(n).startswith("shopee|txt|")


# ══════════ Identidades que MUDAM no 1.1 [MUDA_NO_1_1] ══════════

def test_MUDA_NO_1_1_lista_cupons_usa_cuplist():
    n = _norm("Cupons\nR$ 20 OFF em R$ 150: AAAA1\nR$ 50 OFF em R$ 300: BBBB2",
              plat="magalu", cupom="AAAA1", code_entities=["AAAA1", "BBBB2"])
    assert identidade_canonica(n).startswith("magalu|cuplist|")


def test_MUDA_NO_1_1_multi_produto_colapsa_no_min():
    n = _norm("dois produtos", plat="amazon", ids_globais=["B0BBB", "B0AAA"])
    assert identidade_canonica(n) == "amazon|B0AAA"   # min, nao o conjunto


if __name__ == "__main__":
    _ESTADO_ANTES = (config._DB_PATH, _dbmod._DB_PATH, _dbmod._db_conn)
    testes = [v for k, v in sorted(globals().items())
              if k.startswith("test_") and callable(v)]
    falhas = 0
    for t in testes:
        try:
            with _ambiente_isolado():  # DB isolado + vigilância de estado de módulo
                t()
            print(f"  ✅ {t.__name__}")
        except AssertionError as e:
            falhas += 1
            print(f"  ❌ {t.__name__}  →  {e}")
        except Exception as e:
            falhas += 1
            print(f"  💥 {t.__name__}  →  {type(e).__name__}: {e}")
    total = len(testes)
    print(f"\n{total - falhas}/{total} passaram"
          + (f" — {falhas} FALHA(S)" if falhas else " — tudo verde ✅"))
    _ESTADO_DEPOIS = (config._DB_PATH, _dbmod._DB_PATH, _dbmod._db_conn)
    print(f"globais de DB restaurados? {_ESTADO_ANTES == _ESTADO_DEPOIS}  "
          f"(_DB_PATH = {config._DB_PATH})")
    sys.exit(1 if falhas else 0)
         
