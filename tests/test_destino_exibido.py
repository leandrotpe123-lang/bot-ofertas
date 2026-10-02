"""
DESTINO É FATO DO QUE O POST EXIBE (P1-2 da auditoria, post 24423).

01/10 12:00 — Promotom 110373 (12 códigos + `/sec/`, sem foto) abriu o
post 24423. 12:12 — Samuel 118773 (9 códigos, 8 listas) casou pelo código
em comum: PARCIAL → COMPOSICAO_PERDERIA_IDENTIDADE, texto preservado. Mas
`absorver` ensinou à família as 8 âncoras de DESTINO do Samuel, e
`tem_destino()`/`_acolhe()` liam a MEMÓRIA (oferta_index): o post passou a
"ter destino" sem nunca ter exibido uma lista. Daí em diante:
  · um mecanismo que AMPLIA o que o post exibe era barrado por
    MECANISMO_NAO_SUBSTITUI_DESTINO;
  · um destino diferente era recusado por FAMILIA_OUTRO_DESTINO.

Regra: oferta_index = memória para ENCONTRAR a família (continua);
post_exibida = fato estrutural do que o post MOSTRA (decide destino).

[ML lista, 02/10] O 24423 real (110373 SEM foto) agora EDITA para a versão
com listas, mantendo no ar os cupons só do Promotom (retencao_cupons): o
post passa a EXIBIR os destinos (test_01, último bloco). O estado "destino
só aprendido" continua existindo quando a retenção não cabe — o post COM
foto, cuja legenda (≤ 1024) não comporta o texto composto — e é por ele
que os testes abaixo exercitam a regra.

CORPUS REAL (t.me/s/<canal>, texto e códigos fiéis): Promotom 110373,
Samuel 118773. Os `meli.la` são bloqueados neste ambiente: a URL longa de
cada lista é sintética (mesma forma `lista.mercadolivre.com.br/<container>
?coupon_campaign_id=<id>` de test_familia_destino). Nenhum arquivo de
plataforma é tocado: o ML aqui é só DADO de teste.

  01  reprodução do 24423: destino só aprendido → tem_destino False
  02  o mesmo destino, EXIBIDO → tem_destino True
  03  _acolhe não trata destino aprendido como exibido (contrato direto)
  04  caminho real: destino diferente não é mais recusado por um destino
      que o post só aprendeu
  05  caminho real: mecanismo que AMPLIA o exibido evolui (não é mais
      MECANISMO_NAO_SUBSTITUI_DESTINO); com destino EXIBIDO continua barrado
  06  a memória continua: as âncoras aprendidas seguem em oferta_index e
      seguem encontrando a família
  07  post VIVO com post_exibida VAZIA e destino só na memória: continua
      sem destino — nenhum fallback para oferta_index (seria o 24423 de
      volta). Legado pré-Frente 8b não existe vivo: janela_fim é fixada
      no nascimento (25 min) e todo nascimento desde 4c6110b grava a
      exibida; o único vivo com exibida vazia é o nascido sem âncoras.

    python tests/test_destino_exibido.py
"""
import os
import sys
import types
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_familia_destino import (                              # noqa: E402
    FADA, MOTIVOS, PROMOTOM, SAMUEL, cenario, db_ofertas_de_post, lista, msg,
    publicar)
from _harness_e5 import rodar                                   # noqa: E402
from database import db_exibida                                 # noqa: E402
from database_conexao import _db                                # noqa: E402
from pipeline import familia                                    # noqa: E402
from pipeline.resolucao_identidade import eh_chave_destino      # noqa: E402

# ── 01/10 12:00 — Promotom 110373 (texto e códigos reais) ─────────
SEC_110373 = "https://mercadolivre.com/sec/2U6U32Q"
T_110373 = ("Cupom Mercado Livre\n\n"
            "8% OFF em R$ 150, Limite de R$ 300 OFF: PROMOAQU\n"
            "10% OFF em R$ 79, Limite de R$ 100 OFF: PROMOML\n"
            "10% OFF em R$ 149, Limite de R$ 200 OFF: DESCONTOSMELI\n"
            "15% OFF em R$ 49, Limite de R$ 50 OFF: MODAML\n"
            "15% OFF em R$ 59, Limite de R$ 50 OFF: BRINCAR\n"
            "15% OFF em R$ 79, Limite de R$ 60 OFF: TEMPROMO\n"
            "15% OFF em R$ 79, Limite de R$ 200 OFF: OFERTA\n"
            "20% OFF em R$ 19, Limite de R$ 80 OFF: OFERTAS\n"
            "20% OFF em R$ 79, Limite de R$ 50 OFF: ECONOMIAMELI\n"
            "22% OFF em R$ 1, Limite de R$ 500 OFF: PROMOCERTA\n"
            "25% OFF em R$ 1, Limite de R$ 500 OFF: OFFAQUI, ECONOMIA\n\n"
            f"-Resgate aqui: {SEC_110373}")
C_110373 = ["PROMOAQU", "PROMOML", "DESCONTOSMELI", "MODAML", "BRINCAR", "TEMPROMO",
            "OFERTA", "OFERTAS", "ECONOMIAMELI", "PROMOCERTA", "OFFAQUI", "ECONOMIA"]

# ── 01/10 12:12 — Samuel 118773 (texto e códigos reais) ───────────
T_118773 = ("🔥 Cupons Mercado Livre\n\n"
            "🎟 10% OFF em R$ 79, Limite de R$ 100 OFF: PROMOML\n\n"
            "🎟 15% OFF acima de R$ 79, limite R$ 60: TEMPROMO\n"
            "👉 Lista: https://meli.la/2SxES6P\n\n"
            "🎟 30% OFF, limite R$ 500: OPAECONOMIZEI\n"
            "👉 Lista: https://meli.la/2AnW4cU\n\n"
            "🎟 25% OFF, limite R$ 500: OFFAQUI\n"
            "👉 Lista: https://meli.la/2wo2vj6\n\n"
            "🎟 15% OFF em R$ 49, Limite de R$ 50 OFF: MODAML\n"
            "👉 Lista: https://meli.la/1GbyQqi\n\n"
            "🎟 15% OFF em R$ 59, Limite de R$ 50 OFF: BRINCAR\n"
            "👉 Lista: https://meli.la/1H3eMiY\n\n"
            "🎟 20% OFF em R$ 19, Limite de R$ 80 OFF: OFERTAS\n"
            "👉 Lista: https://meli.la/1adPLKK\n\n"
            "🎟 20% OFF em R$ 79, Limite de R$ 50 OFF: ECONOMIAMELI\n"
            "👉 Lista: https://meli.la/1RB11Mm\n\n"
            "🎟 22% OFF em R$ 1, Limite de R$ 500 OFF: PROMOCERTA\n"
            "👉 Lista: https://meli.la/2oz7gvp\n\n"
            "anúncio")
C_118773 = ["PROMOML", "TEMPROMO", "OPAECONOMIZEI", "OFFAQUI", "MODAML", "BRINCAR",
            "OFERTAS", "ECONOMIAMELI", "PROMOCERTA"]
# Uma lista por linha com "👉 Lista" (URL longa sintética: meli.la bloqueado).
L_118773 = [lista(f"_Container_cupom-{c.lower()}", 13800000 + i)
            for i, c in enumerate(C_118773[1:], start=1)]


def promotom_110373():
    return msg(PROMOTOM, T_110373, [SEC_110373], C_110373)


def samuel_118773():
    return msg(SAMUEL, T_118773, L_118773, C_118773)


def destinos(chaves):
    return {k for k in chaves if eh_chave_destino(k)}


def _com_foto(n):
    tag = f"m{n.msg_id}"
    return replace(n, tem_midia=True, media_obj=types.SimpleNamespace(tag=tag),
                   midia_key=f"k-{tag}")


def _24423(corpo_extra=None, foto=True):
    """Replay do 24423: 110373 abre, 118773 casa e é ignorado (PARCIAL).
    `foto=True`: o 110373 com foto — a legenda não comporta o texto com os
    cupons retidos, então o Samuel continua ignorado (destino só aprendido).
    `foto=False`: o 24423 real de hoje — a lista EDITA o post."""
    out = {}

    async def corpo(c):
        p = promotom_110373()
        await publicar(_com_foto(p) if foto else p, score=26)
        out["post"] = post = c_ids(c)[-1]
        MOTIVOS.clear()
        sam = samuel_118773()
        enr, _m = await publicar(sam, score=26)
        out["motivos_samuel"] = list(MOTIVOS)
        out["dest_samuel"] = destinos(enr.ofertas)
        out["exibe"] = set(db_exibida(post))
        out["memoria"] = set(db_ofertas_de_post(post))
        out["tem_destino"] = familia.tem_destino(post)
        out["destinos_do_post"] = familia.destinos_do_post(post)
        out["novos_apos_samuel"] = c.novos
        if corpo_extra:
            await corpo_extra(c, out)
    cli = cenario(corpo)
    return cli, out


_IDS = {}


def c_ids(c):
    """Ids de destino criados (o Cliente de test_familia_destino só conta)."""
    return _IDS.setdefault(id(c), [])


def _registrar_ids(c):
    envio, arquivo = c.send_message, c.send_file

    async def send_message(*a, **k):
        m = await envio(*a, **k)
        c_ids(c).append(m.id)
        return m

    async def send_file(*a, **k):
        m = await arquivo(*a, **k)
        c_ids(c).append(m.id)
        return m
    c.send_message, c.send_file = send_message, send_file


_cenario_real = cenario


def _zerar_banco():
    """Os códigos REAIS se repetem entre cenários: um post vivo de um
    cenário viraria família do seguinte."""
    with _db() as db:
        tabelas = [t for (t,) in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'")]
        for t in tabelas:
            db.execute(f"DELETE FROM {t}")


async def _baixar(media, file=None):
    """Mídia de teste: a foto existe (o Cliente base não baixa nada)."""
    if file is not None and getattr(media, "tag", None):
        file.write((media.tag + "|").encode().ljust(4096, b"x"))
        return "arquivo"
    return None


def cenario(corpo):                                             # noqa: F811
    async def com_ids(c):
        _registrar_ids(c)
        c.download_media = _baixar
        await corpo(c)
    _zerar_banco()
    return _cenario_real(com_ids)


# ══════════════════════════════════════════════════════════════════
def test_01_destino_so_aprendido_nao_e_destino_do_post(r):
    cli, out = _24423()
    r.check(out["motivos_samuel"] == ["COMPOSICAO_PERDERIA_IDENTIDADE"],
            "01.samuel_parcial_ignorado", str(out["motivos_samuel"]))
    r.check(len(out["dest_samuel"]) == 8, "01.samuel_declara_8_listas", str(out["dest_samuel"]))
    r.check(out["novos_apos_samuel"] == 1, "01.um_post", str(out["novos_apos_samuel"]))
    r.check(not destinos(out["exibe"]), "01.post_nao_exibe_destino", str(out["exibe"]))
    r.check(out["dest_samuel"] <= out["memoria"], "01.memoria_aprendeu_os_destinos",
            str(destinos(out["memoria"])))
    r.check(out["tem_destino"] is False, "01.tem_destino_False", str(out["tem_destino"]))
    r.check(out["destinos_do_post"] == set(), "01.destinos_do_post_vazio",
            str(out["destinos_do_post"]))

    # o 24423 real (110373 sem foto): a lista EDITA o post e mantém no ar
    # os cupons só do Promotom — o destino passa a ser EXIBIDO, de verdade
    _cli, real = _24423(foto=False)
    cups = {k.split("|")[2] for k in real["exibe"] if "|cup|" in k}
    r.check(real["motivos_samuel"] == ["DESTINO_PREVALECE"] and real["novos_apos_samuel"] == 1,
            "01.real_lista_edita_o_post", str(real["motivos_samuel"]))
    r.check(real["dest_samuel"] <= real["exibe"] and real["tem_destino"] is True
            and set(C_110373) <= cups, "01.real_exibe_listas_e_nenhum_cupom_some",
            str(sorted(real["exibe"]))[:300])


def test_02_mesmo_destino_exibido_e_destino_do_post(r):
    """O Samuel PRIMEIRO: o post exibe as 8 listas — destino real."""
    out = {}

    async def corpo(c):
        await publicar(samuel_118773(), score=26)
        post = c_ids(c)[-1]
        out["exibe"] = destinos(db_exibida(post))
        out["tem_destino"] = familia.tem_destino(post)
        out["destinos_do_post"] = familia.destinos_do_post(post)
    cenario(corpo)
    r.check(len(out["exibe"]) == 8, "02.post_exibe_as_listas", str(out["exibe"]))
    r.check(out["tem_destino"] is True, "02.tem_destino_True")
    r.check(out["destinos_do_post"] == out["exibe"], "02.destinos_do_post_e_o_exibido",
            str(out["destinos_do_post"]))


OUTRO_DESTINO = "mercadolivre|dest|mercadolivre:lista:_Container_outra-campanha:13899999"


def test_03_acolhe_nao_trata_aprendido_como_exibido(r):
    async def extra(c, out):
        post = out["post"]
        um_aprendido = sorted(out["dest_samuel"])[0]
        out["acolhe_outro"] = familia._acolhe(post, (OUTRO_DESTINO,))
        out["acolhe_aprendido"] = familia._acolhe(post, (um_aprendido,))
    _cli, out = _24423(extra)
    r.check(out["acolhe_outro"] is True, "03.post_sem_destino_exibido_acolhe_outro_destino")
    r.check(out["acolhe_aprendido"] is True, "03.acolhe_o_aprendido_tambem")

    # com destino EXIBIDO, a regra "um destino não entra na família de
    # outro" continua valendo
    out2 = {}

    async def corpo(c):
        await publicar(samuel_118773(), score=26)
        post = c_ids(c)[-1]
        out2["outro"] = familia._acolhe(post, (OUTRO_DESTINO,))
        out2["mesmo"] = familia._acolhe(post, (sorted(destinos(db_exibida(post)))[0],))
        out2["sem_destino"] = familia._acolhe(post, ())
    cenario(corpo)
    r.check(out2["outro"] is False, "03.exibido_recusa_outro_destino")
    r.check(out2["mesmo"] is True, "03.exibido_acolhe_o_mesmo")
    r.check(out2["sem_destino"] is True, "03.mecanismo_sempre_acolhido")


# Fada, lista DIFERENTE do Samuel, com código em comum com o post
T_FADA_OUTRA_LISTA = ("🔥 Cupom Mercado Livre\n\n"
                      "🎟 10% OFF acima de R$79, limite R$100: PROMOML\n\n"
                      "✅ Resgate aqui:\nhttps://meli.la/2XoB7wc")


def test_04_destino_diferente_nao_e_recusado_por_destino_aprendido(r):
    """Antes: a lista da Fada era recusada (FAMILIA_OUTRO_DESTINO) só
    porque o post APRENDEU as listas do Samuel, e virava post próprio.
    Agora o post (que não exibe destino) a acolhe pelo código, como
    acolheria sem o Samuel, e decide NO POST. [ML lista, 02/10] A lista
    da Fada é PARCIAL, mas o texto dela + os cupons retidos do Promotom
    cabem na legenda: a lista edita o post sem nenhum cupom sumir
    (DESTINO_PREVALECE) — antes, COMPOSICAO_PERDERIA_IDENTIDADE."""
    async def extra(c, out):
        MOTIVOS.clear()
        fada = msg(FADA, T_FADA_OUTRA_LISTA,
                   [lista("_Container_outra-campanha", 13899999)], ["PROMOML"])
        antes = c.novos
        await publicar(fada, score=26)
        out["fada_novo_post"] = c.novos > antes
        out["motivos_fada"] = list(MOTIVOS)
        out["exibe_depois"] = set(db_exibida(out["post"]))
    _cli, out = _24423(extra)
    # a família recusada pelo destino só aprendido abria um post novo
    r.check(out["fada_novo_post"] is False, "04.casa_o_post_pelo_codigo",
            f"novo={out['fada_novo_post']} motivos={out['motivos_fada']}")
    r.check(out["motivos_fada"] == ["DESTINO_PREVALECE"],
            "04.decidido_no_post_lista_edita", str(out["motivos_fada"]))
    cups = {k.split("|")[2] for k in out["exibe_depois"] if "|cup|" in k}
    r.check(set(C_110373) <= cups, "04.nenhum_cupom_exibido_some", str(sorted(cups)))


# Mecanismo (`/sec/`) que AMPLIA o que o post exibe: os 12 códigos do
# 110373 + o OPAECONOMIZEI do 118773 (linhas reais, composição montada).
T_AMPLIA = T_110373.replace(
    "\n\n-Resgate aqui:",
    "\n30% OFF, limite R$ 500: OPAECONOMIZEI\n\n-Resgate aqui:")
C_AMPLIA = C_110373 + ["OPAECONOMIZEI"]


def test_05_mecanismo_que_amplia_evolui(r):
    async def extra(c, out):
        MOTIVOS.clear()
        amplia = msg(FADA, T_AMPLIA, [SEC_110373], C_AMPLIA)
        await publicar(amplia, score=20)
        out["motivos_amplia"] = list(MOTIVOS)
        out["texto_ok"] = any(m == out["post"] and "OPAECONOMIZEI" in t
                              for m, t in c.edits)
    _cli, out = _24423(extra)
    r.check("MECANISMO_NAO_SUBSTITUI_DESTINO" not in out["motivos_amplia"],
            "05.nao_barrado_por_destino_so_aprendido", str(out["motivos_amplia"]))
    r.check(out["motivos_amplia"] == ["COMPOSICAO_AMPLIADA"], "05.amplia_evolui",
            str(out["motivos_amplia"]))
    r.check(out["texto_ok"], "05.post_exibe_o_codigo_novo")

    # destino EXIBIDO: o mecanismo continua não substituindo (DECISÃO 0)
    out2 = {}

    async def corpo(c):
        await publicar(samuel_118773(), score=26)
        MOTIVOS.clear()
        await publicar(msg(FADA, T_AMPLIA, [SEC_110373], C_AMPLIA), score=40)
        out2["motivos"] = list(MOTIVOS)
    cenario(corpo)
    r.check(out2["motivos"] == ["MECANISMO_NAO_SUBSTITUI_DESTINO"],
            "05.destino_exibido_continua_prevalecendo", str(out2["motivos"]))


def test_06_memoria_continua_encontrando_a_familia(r):
    async def extra(c, out):
        post = out["post"]
        um_aprendido = sorted(out["dest_samuel"])[0]
        out["overlap"] = familia.post_da_familia([um_aprendido])
        out["memoria_tem"] = um_aprendido in db_ofertas_de_post(post)
    _cli, out = _24423(extra)
    r.check(out["memoria_tem"], "06.ancora_aprendida_continua_na_memoria")
    r.check(out["overlap"] == out["post"], "06.memoria_continua_achando_o_post",
            str(out["overlap"]))


def test_07_exibida_vazia_sem_fallback_para_memoria(r):
    import time
    from database_posts import db_absorver_ofertas, db_registrar_post
    _zerar_banco()
    post, agora = 24999, time.time()
    destino = sorted(destinos(familia_destinos_samuel()))[0]
    db_registrar_post(post, [], 26, "texto qualquer", "mercadolivre", str(PROMOTOM),
                      agora + 900, 0, exibidas=[])
    db_absorver_ofertas(post, [destino])
    r.check(destino in db_ofertas_de_post(post) and not db_exibida(post),
            "07.so_memoria", f"{db_ofertas_de_post(post)} {db_exibida(post)}")
    r.check(familia.tem_destino(post) is False, "07.tem_destino_False")
    r.check(familia.destinos_do_post(post) == set(), "07.destinos_do_post_vazio")
    r.check(familia._acolhe(post, (OUTRO_DESTINO,)) is True, "07.acolhe_outro_destino")


def familia_destinos_samuel():
    from pipeline.enriquecimento import enriquecer
    return enriquecer(samuel_118773()).ofertas


if __name__ == "__main__":
    sys.exit(rodar(globals(), "DESTINO EXIBIDO · 24423"))
