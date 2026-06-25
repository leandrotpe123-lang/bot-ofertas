"""
Rede de regressão — cenários-ouro (Passo 0 da migração arquitetural).

Cada teste TRAVA um comportamento real que já quebrou (ou que NÃO pode
quebrar), na camada PURA de identidade/score. Rode antes de cada deploy:

    pytest tests/golden_scenarios.py
ou, sem pytest:
    python tests/golden_scenarios.py

Escopo deliberado: só funções PURAS e determinísticas
(extrair_todos_cupons, identidades, calcular_score). NÃO testa
identidade_canonica ainda — ela é stateful (grava em cupom_idx). Isso
entra após o Passo 1 (unificar a identidade em identidades(), pura por
construção). Essa dificuldade de testar a canônica é, ela mesma, prova
de que identidades() é a autoridade mais limpa.
"""
import os
import sys

# Garante o repo root no sys.path — roda igual via pytest ou standalone,
# de qualquer diretório. Sobe a árvore até achar pipeline/ e utils/.
_d = os.path.dirname(os.path.abspath(__file__))
while _d != os.path.dirname(_d):
    if os.path.isdir(os.path.join(_d, "pipeline")) and \
       os.path.isdir(os.path.join(_d, "utils")):
        if _d not in sys.path:
            sys.path.insert(0, _d)
        break
    _d = os.path.dirname(_d)

from utils.cupom import extrair_todos_cupons
from pipeline.deduplicacao import identidades, calcular_score
from pipeline.normalizacao import MensagemNormalizada


def _norm(texto="", *, plat="shopee", cupom="", sku="", ids_globais=None,
          chave_campanha="", chaves_campanha=None, code_entities=None,
          tem_midia=False, tem_sinal_cashback=False, tem_host_campanha=False,
          mapa=None, chat="@grupo"):
    """Constrói um MensagemNormalizada direto, bypassando ingestão/normalização.
    Isola a camada de identidade/score do resto do pipeline."""
    return MensagemNormalizada(
        msg_id=1, chat=chat, texto_limpo=texto, mapa=mapa or {}, preservar=[],
        plat=plat, cupom=cupom, sku=sku, tem_midia=tem_midia, media_obj=None,
        ids_globais=ids_globais or [], chave_campanha=chave_campanha,
        chaves_campanha=chaves_campanha or [], tem_host_campanha=tem_host_campanha,
        tem_sinal_cashback=tem_sinal_cashback, code_entities=code_entities or [],
    )


# Texto REAL de lista de cupom Magalu: os códigos vêm em LINHA SEPARADA
# (entidade monospace), NÃO colados depois de ":". Sem as entidades, o
# texto puro não entrega código nenhum — foi a causa-raiz da duplicação.
_TXT_CUPOM_ENTIDADE = (
    "🚨 Cupons Ativos Magalu APP\n"
    "R$20 OFF em R$150\nPELANDO20\n"
    "R$100 OFF em R$900\nPELANDO100\n"
    "R$300 OFF em R$3000\nCUPOMLIVE300"
)
_CODIGOS = ["PELANDO20", "PELANDO100", "CUPOMLIVE300"]


# ══════════════ GRUPO 1 — Extração de cupom (utils.cupom) ══════════════

def test_cupom_entidade_eh_lido_com_entities():
    """Códigos em entidade monospace DEVEM ser extraídos via code_entities."""
    got = extrair_todos_cupons(_TXT_CUPOM_ENTIDADE, _CODIGOS)
    assert set(got) == set(_CODIGOS), got


def test_cupom_entidade_sem_entities_eh_o_buraco():
    """GUARD: sem code_entities, o texto puro NÃO entrega os códigos. Trava
    a assimetria que causou a duplicação — se mudar, a rede avisa."""
    assert extrair_todos_cupons(_TXT_CUPOM_ENTIDADE, None) == []


def test_cupom_inline_continua_funcionando():
    """Formato inline 'OFF em R$Y: CODE' continua extraído sem entidades."""
    txt = "R$ 20 OFF em R$ 150: PELANDO20\nR$ 100 OFF em R$ 900: PELANDO100"
    assert set(extrair_todos_cupons(txt, None)) == {"PELANDO20", "PELANDO100"}


# ══════════════ GRUPO 2 — Conjunto de ofertas (identidades) ══════════════

def test_identidades_emite_cupom_de_entidade():
    """FIX 3: cupom que veio só como entidade entra no conjunto de ofertas."""
    n = _norm(_TXT_CUPOM_ENTIDADE, plat="magalu", cupom="PELANDO20",
              code_entities=_CODIGOS)
    ofs = identidades(n)
    for c in _CODIGOS:
        assert f"magalu|cup|{c}" in ofs, (c, ofs)


def test_identidades_emite_todas_chaves_campanha():
    """FIX 2: campanha multi-host emite TODAS as chaves, não só o min()."""
    n = _norm("evento black friday", plat="shopee", chave_campanha="c1",
              chaves_campanha=["c1", "c2", "c3"], tem_host_campanha=True)
    ofs = identidades(n)
    for k in ("c1", "c2", "c3"):
        assert f"shopee|camp|{k}" in ofs, (k, ofs)


def test_produto_com_cupom_emite_ambos():
    """Produto que carrega cupom emite as DUAS ofertas — base da união na
    evolução (FIX 1): se o produto evoluir, o cupom não pode sumir."""
    n = _norm("oferta de produto com cupom", plat="amazon", cupom="CURTEAI",
              ids_globais=["B0XYZ"], code_entities=["CURTEAI"])
    ofs = identidades(n)
    assert "amazon|B0XYZ" in ofs, ofs
    assert "amazon|cup|CURTEAI" in ofs, ofs


def test_dois_posts_mesmo_cupom_sao_mesma_familia():
    """Dois posts compartilhando código (um mais rico) DEVEM ter overlap —
    é o que faz o segundo evoluir o primeiro em vez de duplicar."""
    pobre = _norm(_TXT_CUPOM_ENTIDADE, plat="magalu", cupom="PELANDO20",
                  code_entities=_CODIGOS)
    rico = _norm(_TXT_CUPOM_ENTIDADE + "\nproduto", plat="magalu",
                 cupom="PELANDO20", code_entities=_CODIGOS, ids_globais=["SKU9"])
    assert set(identidades(pobre)) & set(identidades(rico))


def test_live_aggregation_e_session_NAO_convergem_HOJE():
    """CHARACTERIZATION (comportamento ATUAL, não necessariamente o desejado):
    vitrine (aggregation, sem ID) e live direta (session, com ID) hoje são
    famílias DIFERENTES. Se decidirmos colapsar, este teste muda DE PROPÓSITO."""
    agg = _norm("live shopee", plat="shopee", tem_host_campanha=True,
                chaves_campanha=["live.shopee.com.br/aggregation"])
    ses = _norm("live shopee", plat="shopee", tem_host_campanha=True,
                chaves_campanha=["live.shopee.com.br/live/4844302"])
    assert not (set(identidades(agg)) & set(identidades(ses)))


def test_identidades_deterministico():
    """Mesma entrada → mesmo conjunto, mesma ordem."""
    n = _norm(_TXT_CUPOM_ENTIDADE, plat="magalu", cupom="PELANDO20",
              code_entities=_CODIGOS)
    assert identidades(n) == identidades(n)


# ══════════════ GRUPO 3 — Score (calcular_score) ══════════════

def test_score_mais_cupons_nao_perde():
    """D3: post mais rico em cupons tem score >= post mais pobre."""
    pobre = _norm("R$ 100 off cupom", plat="magalu", cupom="A",
                  code_entities=["AAAA1"])
    rico = _norm("R$ 100 off cupons", plat="magalu", cupom="A",
                 code_entities=["AAAA1", "BBBB2", "CCCC3", "DDDD4"])
    assert calcular_score(rico) >= calcular_score(pobre)


def test_score_midia_normal_soma():
    """Mídia de grupo normal soma no score."""
    sem = _norm("texto", plat="amazon")
    com = _norm("texto", plat="amazon", tem_midia=True)
    assert calcular_score(com) >= calcular_score(sem)


if __name__ == "__main__":
    import sys
    testes = [v for k, v in sorted(globals().items())
              if k.startswith("test_") and callable(v)]
    falhas = 0
    for t in testes:
        try:
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
    sys.exit(1 if falhas else 0)
          
