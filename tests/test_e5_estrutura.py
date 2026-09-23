"""
E5.0 — T19: INVARIANTE ESTRUTURAL (analise AST da ARVORE REAL).

Prova por ESTRUTURA, nao por execucao: "ponto unico de materializacao"
e propriedade do codigo-fonte, e um teste de runtime so provaria o
caminho que exercitou.

Le os arquivos .py do repositorio real com `ast`. Nao importa nada,
nao executa nada do pipeline.

ESTADO ESPERADO:
  test_T19_estado_atual_*   VERDE hoje   — documenta a HEAD antes da E5.0
  test_T19_alvo_*           VERMELHO hoje — e a ESPECIFICACAO da frente,
                                            fica verde quando a E5.0 existir

Roda stdlib-only / standalone; compatibilidade pytest nao validada
nesta fase:

    python tests/test_e5_estrutura.py
"""
import ast
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import ler_fonte, rodar, Resultado  # noqa: E402

# ── Nome da fachada de materializacao exigida pela frente ─────────
# Ponto unico de configuracao: se a implementacao escolher outro nome,
# muda-se AQUI e em lugar nenhum mais.
FACHADA = "materializar_imagem"
RESOLVER = "_resolver_imagem"

ARQ_MONTAGEM = "pipeline/montagem.py"
ARQ_MONTAGEM_IMG = "pipeline/montagem_imagem.py"
ARQ_PUBLICACAO = "pipeline/publicacao.py"
ARQ_APLICADORES = "pipeline/publicacao_aplicadores.py"
ARQ_SAIDA = "pipeline/saida.py"


# ── Apoio de AST ──────────────────────────────────────────────────
def arvore(rel: str) -> ast.Module:
    return ast.parse(ler_fonte(rel), filename=rel)


def _nome_chamado(no: ast.Call) -> str:
    f = no.func
    if isinstance(f, ast.Name):
        return f.id
    if isinstance(f, ast.Attribute):
        return f.attr
    return ""


def chamadas(rel: str, nome: str):
    """Todos os nos Call de `nome` no arquivo, com a linha."""
    return [(n.lineno, n) for n in ast.walk(arvore(rel))
            if isinstance(n, ast.Call) and _nome_chamado(n) == nome]


def funcao(rel: str, nome: str):
    for n in ast.walk(arvore(rel)):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                and n.name == nome:
            return n
    return None


def chamadas_dentro(no_funcao, nome: str):
    if no_funcao is None:
        return []
    return [n.lineno for n in ast.walk(no_funcao)
            if isinstance(n, ast.Call) and _nome_chamado(n) == nome]


def tem_def(rel: str, nome: str) -> bool:
    return funcao(rel, nome) is not None


# ══════════════════════════════════════════════════════════════════
# ESTADO ATUAL — verde hoje, documenta a HEAD
# ══════════════════════════════════════════════════════════════════
def test_T19_estado_atual_resolver_existe_e_e_unico(r: Resultado):
    """_resolver_imagem e definido uma unica vez, em montagem_imagem."""
    r.check(tem_def(ARQ_MONTAGEM_IMG, RESOLVER), "T19.atual.def",
            f"{RESOLVER} deve existir em {ARQ_MONTAGEM_IMG}")
    for rel in (ARQ_MONTAGEM, ARQ_PUBLICACAO, ARQ_APLICADORES, ARQ_SAIDA):
        r.check(not tem_def(rel, RESOLVER), "T19.atual.sem_duplicata",
                f"{RESOLVER} nao pode ser redefinido em {rel}")
    r.check(tem_def(ARQ_MONTAGEM_IMG, "preparar_imagem_tg"),
            "T19.atual.preparar", "preparar_imagem_tg deve existir")


def test_T19_estado_atual_aplicadores_e_saida_nao_materializam(r: Resultado):
    """Ja hoje: quem aplica NAO baixa. A E5.0 deve preservar isso."""
    for rel in (ARQ_APLICADORES, ARQ_SAIDA):
        r.check(chamadas(rel, RESOLVER) == [], "T19.atual.aplicador_limpo",
                f"{rel} nao pode chamar {RESOLVER}")
        r.check(chamadas(rel, FACHADA) == [], "T19.atual.aplicador_limpo2",
                f"{rel} nao pode chamar {FACHADA}")
        r.check(chamadas(rel, "download_media") == [],
                "T19.atual.sem_download", f"{rel} nao pode baixar midia")


def test_T19_estado_atual_download_so_em_montagem_imagem(r: Resultado):
    """download_media aparece SO em montagem_imagem, em toda a arvore."""
    achados = []
    for base, _dirs, arquivos in os.walk(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))):
        if any(p in base for p in ("/.git", "/tests", "/testes_ml")):
            continue
        for a in arquivos:
            if not a.endswith(".py"):
                continue
            caminho = os.path.join(base, a)
            rel = os.path.relpath(caminho, os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))))
            try:
                if chamadas(rel, "download_media"):
                    achados.append(rel)
            except (SyntaxError, OSError):
                pass
    r.check(achados == [ARQ_MONTAGEM_IMG], "T19.atual.download_unico",
            f"download_media em {achados}, esperado [{ARQ_MONTAGEM_IMG}]")


# ══════════════════════════════════════════════════════════════════
# ALVO E5.0 — VERMELHO hoje por desenho; e a especificacao da frente
# ══════════════════════════════════════════════════════════════════
def test_T19_estado_atual_relogio_capturado_uma_vez(r: Resultado):
    """T17d estrutural — `agora` e capturado UMA vez em _enviar_inner.
    Guarda: a E5.0 faz DUAS chamadas a decidir() e ambas devem usar o
    mesmo relogio; uma segunda captura viraria post duplicado."""
    inner = funcao(ARQ_PUBLICACAO, "_enviar_inner")
    r.check(inner is not None, "T19.8.existe")
    n = chamadas_dentro(inner, "time")
    r.check(len(n) == 1, "T19.8.uma_captura",
            f"time.time() chamado {len(n)}x em _enviar_inner (linhas {n})")


def test_T19_alvo_1_fachada_e_a_unica_a_chamar_resolver(r: Resultado):
    """T19.1 — _resolver_imagem e referenciado APENAS pela fachada."""
    r.check(tem_def(ARQ_MONTAGEM, FACHADA), "T19.1.fachada_existe",
            f"{FACHADA} deve ser definida em {ARQ_MONTAGEM}")
    fach = funcao(ARQ_MONTAGEM, FACHADA)
    r.check(len(chamadas_dentro(fach, RESOLVER)) == 1, "T19.1.fachada_chama",
            f"{FACHADA} deve chamar {RESOLVER} exatamente 1x")
    # em toda a arvore, so a fachada chama o resolver
    for rel in (ARQ_PUBLICACAO, ARQ_APLICADORES, ARQ_SAIDA):
        r.check(chamadas(rel, RESOLVER) == [], "T19.1.exclusividade",
                f"{rel} nao pode chamar {RESOLVER} diretamente")


def test_T19_alvo_2_montar_nao_materializa(r: Resultado):
    """T19.2 — montar() nao referencia o resolver nem a fachada."""
    mont = funcao(ARQ_MONTAGEM, "montar")
    r.check(mont is not None, "T19.2.montar_existe")
    r.check(chamadas_dentro(mont, RESOLVER) == [], "T19.2.sem_resolver",
            f"montar() ainda chama {RESOLVER} nas linhas "
            f"{chamadas_dentro(mont, RESOLVER)}")
    r.check(chamadas_dentro(mont, FACHADA) == [], "T19.2.sem_fachada",
            "montar() nao pode chamar a fachada")
    r.check(isinstance(mont, ast.AsyncFunctionDef), "T19.2.continua_async",
            "montar() deve continuar async (contrato do orchestrator)")


def test_T19_alvo_3_montar_produz_imagem_none(r: Resultado):
    """T19.3 — montar() constroi MensagemMontada com imagem=None literal."""
    mont = funcao(ARQ_MONTAGEM, "montar")
    if mont is None:
        r.check(False, "T19.3.montar_existe")
        return
    achou = False
    for n in ast.walk(mont):
        if isinstance(n, ast.Call) and _nome_chamado(n) == "MensagemMontada":
            for kw in n.keywords:
                if kw.arg == "imagem":
                    achou = True
                    r.check(isinstance(kw.value, ast.Constant)
                            and kw.value.value is None,
                            "T19.3.imagem_none",
                            "imagem= deve ser literal None em montar()")
    r.check(achou, "T19.3.kw_imagem", "montar() deve nomear imagem=")


def test_T19_alvo_5_exatamente_tres_sitios(r: Resultado):
    """T19.5 — 3 sitios sintaticos de INVOCACAO da materializacao,
    todos em publicacao.py: Portao A, Portao B RENASCER, Portao B final."""
    sitios = chamadas(ARQ_PUBLICACAO, FACHADA)
    r.check(len(sitios) == 3, "T19.5.tres_sitios",
            f"encontrados {len(sitios)} sitios de {FACHADA} em "
            f"{ARQ_PUBLICACAO} (linhas {[l for l, _ in sitios]}), esperado 3")
    # nenhum outro arquivo do pipeline pode invocar a fachada
    for rel in (ARQ_APLICADORES, ARQ_SAIDA, ARQ_MONTAGEM_IMG):
        r.check(chamadas(rel, FACHADA) == [], "T19.5.sem_espalhar",
                f"{rel} nao pode invocar {FACHADA}")


def test_T19_alvo_6_imagem_so_recebe_bytes_na_materializacao(r: Resultado):
    """T19.6 — em publicacao.py, `imagem=` so aparece alimentado pela
    fachada (via dataclasses.replace); nunca por outro caminho."""
    arv = arvore(ARQ_PUBLICACAO)
    kws = []
    for n in ast.walk(arv):
        if isinstance(n, ast.Call):
            for kw in n.keywords:
                if kw.arg == "imagem":
                    kws.append((n.lineno, _nome_chamado(n)))
    r.check(kws != [], "T19.6.existe_atribuicao",
            "publicacao.py deve reatribuir montada.imagem apos materializar")
    for linha, alvo in kws:
        r.check(alvo == "replace", "T19.6.via_replace",
                f"linha {linha}: imagem= em {alvo}(), esperado replace()")
    # atribuicao direta a atributo .imagem e proibida
    diretas = [n.lineno for n in ast.walk(arv)
               if isinstance(n, ast.Assign)
               for t in n.targets
               if isinstance(t, ast.Attribute) and t.attr == "imagem"]
    r.check(diretas == [], "T19.6.sem_atribuicao_direta",
            f"atribuicao direta a .imagem nas linhas {diretas}")


def test_T19_alvo_7_decidir_aceita_o_fato(r: Resultado):
    """T19.7 — decidir() expoe midia_candidata keyword-only com default
    None (aditivo: nenhum chamador antigo muda)."""
    dec = funcao("pipeline/decisao.py", "decidir")
    r.check(dec is not None, "T19.7.decidir_existe")
    if dec is None:
        return
    kwonly = [a.arg for a in dec.args.kwonlyargs]
    r.check("midia_candidata" in kwonly, "T19.7.parametro",
            f"decidir() deve ter midia_candidata keyword-only; tem {kwonly}")
    if "midia_candidata" in kwonly:
        i = kwonly.index("midia_candidata")
        padrao = dec.args.kw_defaults[i]
        r.check(isinstance(padrao, ast.Constant) and padrao.value is None,
                "T19.7.default_none", "midia_candidata deve ter default None")
    r.check("midia_key_aceita" in kwonly, "T19.7.e40_preservado",
            "midia_key_aceita (E4.0) deve permanecer")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "E5.0 — T19 ESTRUTURAL (AST) · arvore real"))
