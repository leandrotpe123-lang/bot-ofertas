"""
E5.0 — NIVEL 1 (BASELINE) contra a ARVORE REAL.

Captura o comportamento ATUAL da HEAD para que a implementacao da E5.0
seja provada equivalente. Deve ficar verde ANTES e DEPOIS da frente.

    T14  politica_midia: as 8 saidas + guarda E4.0 + ordem das regras
    T15  score / familia / identidade / vida: snapshot invariante
    T17b TEOREMA DE EQUIVALENCIA de decidir()
    T17c score independe da materializacao

Roda stdlib-only / standalone; compatibilidade pytest nao validada
nesta fase:

    python tests/test_e5_baseline.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar, Resultado  # noqa: E402

preparar()

from dataclasses import asdict                                    # noqa: E402
from pipeline import identidade                                   # noqa: E402
from pipeline.midia_politica import politica_midia, _regra_de_midia  # noqa: E402
from pipeline.score import (calcular_score, score_na_escala,      # noqa: E402
                            midia_ruim, V_LEGADO, V_CONTEUDO)
from pipeline.decisao import decidir                              # noqa: E402
from pipeline.vida_oferta import viva, estampar, VIDA_OFERTA_S    # noqa: E402
from pipeline.normalizacao import MensagemNormalizada             # noqa: E402
from pipeline.montagem import MensagemMontada                     # noqa: E402

# ── cache id->@username: preenchido na ingestao/boot em producao ──
identidade._CHAT_USERNAME.update({
    -1001: "promotom",             # midia RUIM (config._GRUPOS_IMG_RUIM)
    -1003: "fumotom",              # midia RUIM
    -1002: "fadapromos",           # midia BOA
    -1004: "ofertasconvertidas",   # midia BOA
})
RUIM_A, RUIM_B = "-1001", "-1003"
BOM_A, BOM_B = "-1002", "-1004"

BUF = b"x" * 4096          # bytes materializados validos
AGORA = 1_000_000.0
JANELA_VIVA = AGORA + 600
JANELA_MORTA = AGORA - 1


def norm(**kw) -> MensagemNormalizada:
    base = dict(msg_id=1, chat=BOM_A, texto_limpo="oferta boa aqui",
                mapa={}, preservar=[], plat="shopee", sku="",
                tem_midia=False, media_obj=None)
    base.update(kw)
    return MensagemNormalizada(**base)


def montada(texto="texto publicavel", imagem=None) -> MensagemMontada:
    return MensagemMontada(msg_id=1, chat=BOM_A, plat="shopee", sku="",
                           texto=texto, imagem=imagem, mapa={},
                           msg_id_origem=1)


def estado(**kw) -> dict:
    base = dict(msg_id_dest=555, score=10, texto="texto publicavel",
                lider=BOM_A, edit_count=0, janela_fim=JANELA_VIVA,
                ts=AGORA - 30, score_versao=V_CONTEUDO, plat="shopee")
    base.update(kw)
    return base


# ══════════════════════════════════════════════════════════════════
# T14 — politica_midia: as 8 saidas
# ══════════════════════════════════════════════════════════════════
def test_T14_politica_midia_oito_saidas(r: Resultado):
    # 1. sem imagem nova — vence qualquer estado
    for est in ({}, {"midia_chat": None}, {"midia_chat": ""},
                {"midia_chat": BOM_A}, {"midia_chat": RUIM_A}):
        r.check(politica_midia(None, BOM_A, est)
                == (False, "PRESERVA/sem_imagem_nova"), "T14.1", str(est))
    r.check(politica_midia(b"", BOM_A, {"midia_chat": ""})
            == (False, "PRESERVA/sem_imagem_nova"), "T14.1b", "buffer vazio")

    # 2. midia desconhecida (dono is None)
    r.check(politica_midia(BUF, BOM_A, {})
            == (False, "PRESERVA/midia_desconhecida"), "T14.2a")
    r.check(politica_midia(BUF, BOM_A, {"midia_chat": None})
            == (False, "PRESERVA/midia_desconhecida"), "T14.2b")

    # 3. post nasceu sem midia
    for chat in (BOM_A, RUIM_A):
        r.check(politica_midia(BUF, chat, {"midia_chat": ""})
                == (True, "TROCA/post_sem_midia"), "T14.3", chat)

    # 4. fonte editou
    for chat in (BOM_A, RUIM_A):
        r.check(politica_midia(BUF, chat, {"midia_chat": chat}, True)
                == (True, "TROCA/fonte_editou"), "T14.4", chat)
    r.check(politica_midia(BUF, BOM_A, {"midia_chat": BOM_A}, False)
            == (False, "PRESERVA/mesma_classe"), "T14.4c", "sem is_edit")

    # 5. nao rebaixa
    for ed in (False, True):
        r.check(politica_midia(BUF, RUIM_A, {"midia_chat": BOM_A}, ed)
                == (False, "PRESERVA/nao_rebaixa"), "T14.5", f"is_edit={ed}")

    # 6. upgrade
    for ed in (False, True):
        r.check(politica_midia(BUF, BOM_A, {"midia_chat": RUIM_A}, ed)
                == (True, "TROCA/upgrade"), "T14.6", f"is_edit={ed}")

    # 7. mesma classe
    r.check(politica_midia(BUF, BOM_A, {"midia_chat": BOM_B})
            == (False, "PRESERVA/mesma_classe"), "T14.7a")
    r.check(politica_midia(BUF, RUIM_A, {"midia_chat": RUIM_B})
            == (False, "PRESERVA/mesma_classe"), "T14.7b")

    # 8. guarda E4.0
    r.check(politica_midia(BUF, BOM_A, {"midia_chat": ""},
                           chave_nova="K", chave_aceita="K")
            == (False, "PRESERVA/midia_igual"), "T14.8a")
    r.check(politica_midia(BUF, BOM_A, {"midia_chat": ""},
                           chave_nova="K", chave_aceita="J")
            == (True, "TROCA/post_sem_midia"), "T14.8b")
    for kn, ka in (("K", ""), ("", "K"), ("", "")):
        r.check(politica_midia(BUF, BOM_A, {"midia_chat": ""},
                               chave_nova=kn, chave_aceita=ka)
                == (True, "TROCA/post_sem_midia"),
                "T14.8c", f"chave ausente nao inventa igualdade {kn!r}/{ka!r}")
    r.check(politica_midia(BUF, RUIM_A, {"midia_chat": BOM_A},
                           chave_nova="K", chave_aceita="K")
            == (False, "PRESERVA/nao_rebaixa"),
            "T14.8e", "guarda nao mascara PRESERVA")

    # ordem das regras: dono=='' precede is_edit
    r.check(_regra_de_midia(BUF, BOM_A, {"midia_chat": ""}, True)
            == (True, "TROCA/post_sem_midia"), "T14.ordem")


# ══════════════════════════════════════════════════════════════════
# T15 — score / vida: snapshot e invariancia
# ══════════════════════════════════════════════════════════════════
CORPUS_SCORE = [
    ("vazio", norm(texto_limpo="")),
    ("so_link", norm(texto_limpo="confira", mapa={"a": "b"})),
    ("preco", norm(texto_limpo="por R$ 99,90", mapa={"a": "b"})),
    ("cupom", norm(texto_limpo="use o cupom", mapa={"a": "b"}, cupons=["ABC"])),
    ("pct_off", norm(texto_limpo="50% off hoje R$ 10", mapa={"a": "b"})),
    ("rs_off", norm(texto_limpo="R$ 30 off acima de R$ 100", mapa={"a": "b"})),
    ("frete", norm(texto_limpo="frete gratis R$ 20", mapa={"a": "b"})),
    ("sku", norm(texto_limpo="R$ 50", mapa={"a": "b"}, sku="B01",
                 ids_globais=["B01"])),
    ("produto_rico", norm(texto_limpo="R$ 50 50% off",
                          mapa={"a": "1", "b": "2", "c": "3", "d": "4"},
                          cupons=["X", "Y", "Z"], sku="B01",
                          ids_globais=["B01"])),
    ("cupom_multi", norm(texto_limpo="R$30 OFF em R$299\nR$90 OFF em R$899\n"
                                     "20% OFF acima de R$79",
                         mapa={"a": "b"}, cupons=["A", "B"])),
]

SNAPSHOT_SCORE = {
    "vazio": 0, "so_link": 3, "preco": 5, "cupom": 5, "pct_off": 7,
    "rs_off": 8, "frete": 6, "sku": 6, "produto_rico": 20, "cupom_multi": 18,
}


def test_T15_score_snapshot(r: Resultado):
    for nome, n in CORPUS_SCORE:
        r.check(calcular_score(n) == SNAPSHOT_SCORE[nome], "T15.snapshot",
                f"{nome}: {calcular_score(n)} != {SNAPSHOT_SCORE[nome]}")


def test_T15_score_nao_depende_de_midia_nem_de_chat(r: Resultado):
    """[FASE 4] score e conteudo PURO: origem e midia nao participam."""
    for nome, n in CORPUS_SCORE:
        base = calcular_score(n)
        for chat in (RUIM_A, RUIM_B, BOM_A, BOM_B):
            for tm in (True, False):
                d = asdict(n)
                d.update(chat=chat, tem_midia=tm)
                r.check(calcular_score(MensagemNormalizada(**d)) == base,
                        "T15.puro", f"{nome} chat={chat} tem_midia={tm}")


def test_T15_midia_ruim_ponto_unico(r: Resultado):
    r.check(midia_ruim(RUIM_A) is True, "T15.ruim_a")
    r.check(midia_ruim(RUIM_B) is True, "T15.ruim_b")
    r.check(midia_ruim(BOM_A) is False, "T15.bom_a")
    r.check(midia_ruim(BOM_B) is False, "T15.bom_b")
    r.check(midia_ruim("-9999") is False, "T15.desconhecido")
    r.check(midia_ruim("nao-numerico") is False, "T15.nao_numerico")


def test_T15_vida_oferta_fronteira(r: Resultado):
    """Fronteira EXCLUSIVA: agora == fim significa MORTO."""
    fim = 1000.0
    r.check(viva(fim, fim - 0.001) is True, "T15.vida.vivo")
    r.check(viva(fim, fim) is False, "T15.vida.fronteira")
    r.check(viva(fim, fim + 0.001) is False, "T15.vida.morto")
    r.check(estampar(100.0) == 100.0 + VIDA_OFERTA_S, "T15.vida.estampar")
    r.check(VIDA_OFERTA_S == 1500, "T15.vida.constante", str(VIDA_OFERTA_S))


def test_T15_familia_e_identidade_no_db_real(r: Resultado):
    """Familia e identidade sobre SQLite REAL — nao pode mudar com a E5.0."""
    from database import _init_db
    from database_posts import (db_registrar_post, db_get_post,
                                db_overlap_posts, db_ofertas_de_post,
                                db_absorver_ofertas)
    from pipeline import familia

    _init_db()
    agora = time.time()
    fim = agora + VIDA_OFERTA_S

    db_registrar_post(9001, ["shopee|A", "shopee|B"], 10, "txt", "shopee",
                      BOM_A, fim, 0, midia_chat=BOM_A,
                      score_versao=V_CONTEUDO)
    est = db_get_post(9001)
    r.check(est is not None and est["score"] == 10, "T15.db.registrar")
    r.check(est["midia_chat"] == BOM_A, "T15.db.midia_chat")

    r.check(sorted(db_ofertas_de_post(9001)) == ["shopee|A", "shopee|B"],
            "T15.db.ofertas")
    r.check(db_overlap_posts(["shopee|A"]) == [(9001, 1)], "T15.db.overlap")
    r.check(db_overlap_posts(["shopee|A", "shopee|B"]) == [(9001, 2)],
            "T15.db.overlap2")
    r.check(db_overlap_posts(["shopee|ZZZ"]) == [], "T15.db.overlap_vazio")

    # familia.unir = UNIAO (so cresce)
    r.check(familia.unir(9001, ["shopee|C"])
            == ["shopee|A", "shopee|B", "shopee|C"], "T15.familia.unir")
    # unir NAO escreve
    r.check(sorted(db_ofertas_de_post(9001)) == ["shopee|A", "shopee|B"],
            "T15.familia.unir_nao_escreve")

    # absorver = aprende ancora orfa, NUNCA rouba
    r.check(db_absorver_ofertas(9001, ["shopee|D"]) == 1, "T15.familia.absorve")
    db_registrar_post(9002, ["shopee|E"], 5, "t2", "shopee", BOM_A, fim, 0,
                      midia_chat="", score_versao=V_CONTEUDO)
    r.check(db_absorver_ofertas(9001, ["shopee|E"]) == 0,
            "T15.familia.nao_rouba")
    r.check(db_overlap_posts(["shopee|E"]) == [(9002, 1)],
            "T15.familia.dono_preservado")

    # absorver NAO toca post_estado
    est2 = db_get_post(9001)
    for campo in ("score", "texto", "lider", "edit_count", "janela_fim",
                  "midia_chat"):
        r.check(est2[campo] == est[campo], "T15.familia.absorver_nao_muda_post",
                campo)

    # post morto some do overlap (vida e a autoridade)
    db_registrar_post(9003, ["shopee|M"], 1, "t3", "shopee", BOM_A,
                      agora - 1, 0, midia_chat="", score_versao=V_CONTEUDO)
    r.check(db_overlap_posts(["shopee|M"]) == [], "T15.familia.morto_nao_casa")


# ══════════════════════════════════════════════════════════════════
# CORPUS DE DECISAO — os 9 pontos de retorno
# ══════════════════════════════════════════════════════════════════
CASOS = [
    # (nome, norm, texto_montado, estado, is_edit, chave_aceita)
    ("R1_sem_estado", norm(), "t", None, False, ""),
    ("R2_janela_edit", norm(), "t", estado(janela_fim=JANELA_MORTA), True, ""),
    ("R3_janela_novo", norm(), "t", estado(janela_fim=JANELA_MORTA), False, ""),
    ("R4_renasce", norm(texto_limpo="a oferta voltou ao estoque"), "t",
     estado(), False, ""),
    ("R4_renasce_ruim", norm(chat=RUIM_A, texto_limpo="restock agora"), "t",
     estado(midia_chat=BOM_A), False, ""),
    ("R5_sync_txt_igual", norm(), "texto publicavel",
     estado(midia_chat=BOM_A), True, ""),
    ("R5_sync_txt_dif", norm(), "outro texto aqui",
     estado(midia_chat=BOM_A), True, ""),
    ("R5_sync_sem_midia", norm(), "texto publicavel",
     estado(midia_chat=""), True, ""),
    ("R5_sync_igual_k", norm(midia_key="K"), "texto publicavel",
     estado(midia_chat=""), True, "K"),
    ("R6_teto", norm(), "novo texto rico",
     estado(score=1, edit_count=2, midia_chat=RUIM_A), False, ""),
    ("R7_evolui", norm(), "novo texto rico",
     estado(score=1, midia_chat=RUIM_A), False, ""),
    ("R7_evolui_preserva", norm(chat=RUIM_A), "novo texto rico",
     estado(score=1, lider=RUIM_A, midia_chat=BOM_A), False, ""),
    ("R7_evolui_desconhecida", norm(), "novo texto rico",
     estado(score=1), False, ""),
    ("R8_dup", norm(), "texto publicavel",
     estado(score=5, texto="texto publicavel", midia_chat=RUIM_A), False, ""),
    ("R8_dup_igual_k", norm(midia_key="K"), "texto publicavel",
     estado(score=5, texto="texto publicavel", midia_chat=""), False, "K"),
    ("R9_igual_sim_baixo", norm(), "conteudo completamente distinto disso",
     estado(score=5, texto="texto publicavel", midia_chat=RUIM_A), False, ""),
    ("R9_nao_evolui", norm(), "zzz totalmente diferente",
     estado(score=99, midia_chat=RUIM_A), False, ""),
    ("R9_upgrade", norm(chat=BOM_A), "zzz totalmente diferente",
     estado(score=99, midia_chat=RUIM_A), False, ""),
]

SCORE_CANDIDATO = 5

INVARIANTES = ("acao", "motivo", "novo_score", "na_janela", "score_atual",
               "delta", "sim", "texto_igual")

ACOES_ESPERADAS = {
    "R1_sem_estado": ("PUBLICAR", "SEM_ESTADO"),
    "R2_janela_edit": ("IGNORAR", "JANELA_ENCERRADA"),
    "R3_janela_novo": ("PUBLICAR", "JANELA_ENCERRADA_NOVO_CICLO"),
    "R4_renasce": ("RENASCER", "RENASCIMENTO"),
    "R4_renasce_ruim": ("RENASCER", "RENASCIMENTO"),
    "R5_sync_txt_igual": ("SINCRONIZAR", "SINCRONIZACAO"),
    "R5_sync_txt_dif": ("SINCRONIZAR", "SINCRONIZACAO"),
    "R5_sync_sem_midia": ("SINCRONIZAR", "SINCRONIZACAO"),
    "R5_sync_igual_k": ("SINCRONIZAR", "SINCRONIZACAO"),
    "R6_teto": ("IGNORAR", "EVOLUCAO_LIMITE_ATINGIDO"),
    "R7_evolui": ("EVOLUIR", "EVOLUI"),
    "R7_evolui_preserva": ("EVOLUIR", "EVOLUI"),
    "R7_evolui_desconhecida": ("EVOLUIR", "EVOLUI"),
    "R8_dup": ("IGNORAR", "DUP_SILENCIOSO"),
    "R8_dup_igual_k": ("IGNORAR", "DUP_SILENCIOSO"),
    "R9_igual_sim_baixo": ("IGNORAR", "SCORE_NAO_EVOLUI"),
    "R9_nao_evolui": ("IGNORAR", "SCORE_NAO_EVOLUI"),
    "R9_upgrade": ("IGNORAR", "SCORE_NAO_EVOLUI"),
}


def test_T17b_corpus_cobre_os_nove_pontos(r: Resultado):
    vistos = set()
    for nome, n, txt, est, is_edit, ck in CASOS:
        d = decidir(n, montada(txt, BUF), SCORE_CANDIDATO, est, AGORA,
                    is_edit, midia_key_aceita=ck)
        esperado = ACOES_ESPERADAS[nome]
        r.check((d.acao, d.motivo) == esperado, "T17b.acao",
                f"{nome}: {(d.acao, d.motivo)} != {esperado}")
        vistos.add(d.motivo)
    for motivo in ("SEM_ESTADO", "JANELA_ENCERRADA",
                   "JANELA_ENCERRADA_NOVO_CICLO", "RENASCIMENTO",
                   "SINCRONIZACAO", "EVOLUCAO_LIMITE_ATINGIDO", "EVOLUI",
                   "DUP_SILENCIOSO", "SCORE_NAO_EVOLUI"):
        r.check(motivo in vistos, "T17b.cobertura", f"ponto {motivo} nao coberto")


def test_T17b_teorema_de_equivalencia(r: Resultado):
    """decidir(imagem=BUF) e decidir(imagem=None) so podem diferir nos
    4 campos de midia. E a referencia de decidir(midia_candidata=False)."""
    for nome, n, txt, est, is_edit, ck in CASOS:
        d_com = decidir(n, montada(txt, BUF), SCORE_CANDIDATO, est, AGORA,
                        is_edit, midia_key_aceita=ck)
        d_sem = decidir(n, montada(txt, None), SCORE_CANDIDATO, est, AGORA,
                        is_edit, midia_key_aceita=ck)
        for campo in INVARIANTES:
            r.check(getattr(d_com, campo) == getattr(d_sem, campo),
                    "T17b.invariante",
                    f"{nome}.{campo}: {getattr(d_com, campo)!r} != "
                    f"{getattr(d_sem, campo)!r}")
        r.check(d_sem.trocar_midia is False, "T17b.sem.trocar", nome)
        r.check(d_sem.permite_substituir is False, "T17b.sem.substituir", nome)
        r.check(d_sem.exigir_imagem is False, "T17b.sem.exigir", nome)
        if d_sem.motivo_midia:
            r.check(d_sem.motivo_midia == "PRESERVA/sem_imagem_nova",
                    "T17b.sem.motivo", f"{nome}: {d_sem.motivo_midia}")


def test_T17b_renascer_nao_passa_por_com_midia(r: Resultado):
    """RENASCER/PUBLICAR/JANELA_ENCERRADA nao recebem decisao de midia.
    E por isso que o Portao B nao pode depender de d.trocar_midia."""
    for nome in ("R1_sem_estado", "R2_janela_edit", "R3_janela_novo",
                 "R4_renasce", "R4_renasce_ruim"):
        caso = next(c for c in CASOS if c[0] == nome)
        _, n, txt, est, is_edit, ck = caso
        d = decidir(n, montada(txt, BUF), SCORE_CANDIDATO, est, AGORA,
                    is_edit, midia_key_aceita=ck)
        r.check(d.trocar_midia is False, "T17b.sem_com_midia.trocar", nome)
        r.check(d.motivo_midia == "", "T17b.sem_com_midia.motivo",
                f"{nome}: {d.motivo_midia!r}")
        r.check(d.permite_substituir is False,
                "T17b.sem_com_midia.substituir", nome)
        r.check(d.exigir_imagem is False, "T17b.sem_com_midia.exigir", nome)


# ══════════════════════════════════════════════════════════════════
# T17c — score independe da materializacao
# ══════════════════════════════════════════════════════════════════
def test_T17c_escala_independe_dos_bytes(r: Resultado):
    """score_na_escala le norm.tem_midia, NUNCA montada.imagem."""
    for chat in (RUIM_A, BOM_A):
        for tm in (True, False):
            r.check(score_na_escala(7, chat, tm, V_CONTEUDO) == 7,
                    "T17c.v2_conteudo_puro", f"{chat} tem_midia={tm}")
            esperado = 7 + (0 if not tm else (1 if midia_ruim(chat) else 3))
            r.check(score_na_escala(7, chat, tm, V_LEGADO) == esperado,
                    "T17c.v1_legado", f"{chat} tem_midia={tm}")


def test_T17c_decisao_de_score_nao_muda_com_bytes(r: Resultado):
    """O ramo de score (score_cmp vs score_atual) e identico com e sem bytes,
    inclusive na escala legado v1."""
    for versao in (V_CONTEUDO, V_LEGADO):
        for tm in (True, False):
            for chat in (RUIM_A, BOM_A):
                est = estado(score=5, score_versao=versao)
                n = norm(chat=chat, tem_midia=tm)
                d_com = decidir(n, montada("zzz dif", BUF), 5, est, AGORA, False)
                d_sem = decidir(n, montada("zzz dif", None), 5, est, AGORA, False)
                r.check(d_com.acao == d_sem.acao, "T17c.mesma_acao",
                        f"v{versao} tm={tm} {chat}")
                r.check(d_com.score_atual == d_sem.score_atual,
                        "T17c.mesmo_score_atual", f"v{versao} tm={tm}")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "E5.0 — NIVEL 1 (BASELINE) · arvore real"))
