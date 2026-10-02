"""
CANDIDATO REDUZ/PARCIAL NÃO TROCA A MÍDIA (24423, derivado da P1-2).

01/10 12:12 — Samuel 118773 (com foto, 9 códigos, 8 listas) casou o post
24423 do Promotom 110373 (sem foto, 12 códigos + `/sec/`). A composição
era PARCIAL: o texto ficou (COMPOSICAO_PERDERIA_IDENTIDADE) — mas a
política de mídia anexada à mesma decisão disse TROCA/post_sem_midia e a
foto do Samuel subiu num post cujo texto é do Promotom.

Regra: REDUZ/PARCIAL = o candidato NÃO representa o que o post exibe.
Ele não evolui o texto e não troca a imagem. A autoridade é
pipeline/decisao.py (a política de mídia é rebaixada ali, e _com_midia
rebaixa permite_substituir/exigir_imagem junto); nenhum aplicador muda.
IGUAL/AMPLIA/None, SINCRONIZAR (líder), RENASCER: política intacta.

CORPUS REAL: Promotom 110373 (sem foto), Samuel 118773 (com foto) e
Fada 17529 (com foto, só PROMOML) de 01/10. As URLs longas `meli.la` são
sintéticas (bloqueadas neste ambiente); o ML aqui é só DADO de teste.

  01  decidir(): PARCIAL e REDUZ com imagem → trocar_midia False, sem
      substituir, sem exigir imagem (post sem mídia e upgrade de classe)
  02  decidir(): IGUAL/None continuam TROCA (post sem mídia e upgrade);
      AMPLIA evolui com a imagem
  03  decidir(): fatos de destino — o caminho exato do 24423 (destino
      chega, PARCIAL) e MECANISMO_NAO_SUBSTITUI_DESTINO com PARCIAL não
      trocam; com IGUAL a mídia segue a política
  04  decidir(): SINCRONIZAR (edição do líder) com REDUZ mantém a política
  05  caminho real 24423: a foto do Samuel NÃO sobe no post do Promotom.
      [ML lista, 02/10] O texto agora evolui para a versão com listas
      (cupons só do Promotom retidos — retencao_cupons): é edição de
      TEXTO no lugar, sem mídia, sem apagar+reenviar
  06  caminho real REDUZ: Fada 17529 (só PROMOML, com foto) não troca
  07  caminho real IGUAL: imagem de classe melhor ainda faz upgrade
  08  caminho real AMPLIA: evolui texto e imagem
  09  LIVE: só-live com título do post forte (REDUZ sintético do
      container) não troca a imagem do post forte

    python tests/test_midia_composicao.py
"""
import asyncio
import os
import sys
import time
import types
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_destino_exibido import (                              # noqa: E402
    C_110373, C_118773, C_AMPLIA, L_118773, SEC_110373, T_110373, T_118773,
    T_AMPLIA, _zerar_banco)
from test_familia_destino import (                              # noqa: E402
    FADA, PROMOTOM, SAMUEL, _isolar_loop, msg, publicar)
from _harness_e5 import rodar                                   # noqa: E402
import client as modulo_client                                  # noqa: E402
from database import db_get_post                                # noqa: E402
from pipeline import publicacao                                 # noqa: E402
from pipeline.decisao import decidir                            # noqa: E402
from pipeline.midia_politica import (                           # noqa: E402
    PRESERVA_COMPOSICAO, TROCA_FONTE_EDITOU, TROCA_POST_SEM_MIDIA,
    TROCA_UPGRADE)
from pipeline.normalizacao import MensagemNormalizada           # noqa: E402
from pipeline.normalizacao_identidade import derivar_ancora_url  # noqa: E402
from pipeline.score import V_CONTEUDO                           # noqa: E402
from plataformas.shopee.links import _canonica_live             # noqa: E402

FUMOTOM = -1003775401737
from pipeline import identidade                                 # noqa: E402
identidade._CHAT_USERNAME.update({FUMOTOM: "fumotom"})

# ── 01/10 12:30 — Fada 17529 (texto e código reais) ───────────────
T_17529 = ("🔥 Cupom Mercado Livre\n\n"
           "🎟 10% OFF acima de R$79, limite R$100: PROMOML\n\n"
           "✅ Resgate aqui:\nhttps://meli.la/2XoB7wc\n\nMais cupons aqui 👇")


# ══════════════════════════════════════════════════════════════════
# PARTE 1 — decidir() puro
# ══════════════════════════════════════════════════════════════════
def _norm(chat, msg_id=900001, is_tema=""):
    n = msg(chat, T_118773, [], [])
    return replace(n, msg_id=msg_id, tem_midia=True, midia_key=f"k-{msg_id}")


def _estado(midia_chat, lider=PROMOTOM, lider_msg=110373, score=26):
    agora = time.time()
    return {"msg_id_dest": 24423, "score": score, "janela_fim": agora + 900,
            "edit_count": 0, "lider": str(lider), "lider_msg": lider_msg,
            "ts": agora - 60, "midia_chat": midia_chat, "texto": T_110373,
            "score_versao": V_CONTEUDO}


def _decidir(chat, estado, composicao, score=26, is_edit=False, msg_id=900001, **fatos):
    n = _norm(chat, msg_id=msg_id)
    montada = types.SimpleNamespace(imagem=None, texto=T_118773, msg_id=n.msg_id)
    return decidir(n, montada, score, dict(estado), time.time(), is_edit,
                   midia_candidata=True, composicao=composicao, **fatos)


SEM_MIDIA = ""


def test_01_reduz_parcial_nao_trocam_midia(r):
    for comp in ("PARCIAL", "REDUZ"):
        for rot, est in (("post_sem_midia", _estado(SEM_MIDIA)),
                         ("upgrade_de_classe", _estado(str(PROMOTOM)))):
            for sc in (10, 26, 60):
                d = _decidir(SAMUEL, est, comp, score=sc)
                r.check((d.acao, d.motivo) == ("IGNORAR", "COMPOSICAO_PERDERIA_IDENTIDADE"),
                        f"01.{comp}.{rot}.{sc}.texto_preservado", f"{d.acao} {d.motivo}")
                r.check(d.trocar_midia is False and d.motivo_midia == PRESERVA_COMPOSICAO,
                        f"01.{comp}.{rot}.{sc}.midia_preservada",
                        f"{d.trocar_midia} {d.motivo_midia}")
                r.check(not d.permite_substituir and not d.exigir_imagem,
                        f"01.{comp}.{rot}.{sc}.sem_substituir_sem_exigir")


def test_02_igual_none_amplia_seguem_a_politica(r):
    for comp in ("IGUAL", None):
        d = _decidir(SAMUEL, _estado(SEM_MIDIA), comp)
        r.check(d.acao == "IGNORAR" and d.trocar_midia
                and d.motivo_midia == TROCA_POST_SEM_MIDIA,
                f"02.{comp}.post_sem_midia_recebe", f"{d.acao} {d.motivo} {d.motivo_midia}")
        d = _decidir(FADA, _estado(str(PROMOTOM)), comp)
        r.check(d.trocar_midia and d.motivo_midia == TROCA_UPGRADE,
                f"02.{comp}.upgrade_de_classe", f"{d.motivo_midia}")
    d = _decidir(SAMUEL, _estado(SEM_MIDIA), "AMPLIA", score=10)
    r.check((d.acao, d.motivo) == ("EVOLUIR", "COMPOSICAO_AMPLIADA")
            and d.trocar_midia and d.exigir_imagem and d.permite_substituir,
            "02.AMPLIA.evolui_com_imagem",
            f"{d.acao} {d.motivo} {d.trocar_midia} {d.exigir_imagem}")


def test_03_fatos_de_destino(r):
    # o caminho EXATO do 24423: o candidato declara destino, o post não
    d = _decidir(SAMUEL, _estado(SEM_MIDIA), "PARCIAL",
                 destino_candidato=True, destino_post=False)
    r.check((d.motivo, d.trocar_midia, d.motivo_midia)
            == ("COMPOSICAO_PERDERIA_IDENTIDADE", False, PRESERVA_COMPOSICAO),
            "03.24423_exato", f"{d.motivo} {d.trocar_midia} {d.motivo_midia}")
    for comp, troca in (("PARCIAL", False), ("REDUZ", False), ("IGUAL", True), (None, True)):
        d = _decidir(SAMUEL, _estado(SEM_MIDIA), comp,
                     destino_candidato=False, destino_post=True)
        r.check(d.motivo == "MECANISMO_NAO_SUBSTITUI_DESTINO" and d.trocar_midia is troca,
                f"03.mecanismo.{comp}", f"{d.motivo} {d.trocar_midia} {d.motivo_midia}")


def test_04_sincronizar_do_lider_mantem_a_politica(r):
    est = _estado(str(PROMOTOM), lider=PROMOTOM, lider_msg=110373)
    d = _decidir(PROMOTOM, est, "REDUZ", is_edit=True, msg_id=110373)
    r.check(d.acao == "SINCRONIZAR", "04.lider_sincroniza", f"{d.acao} {d.motivo}")
    r.check(d.trocar_midia and d.motivo_midia == TROCA_FONTE_EDITOU,
            "04.politica_do_lider_intacta", f"{d.trocar_midia} {d.motivo_midia}")


# ══════════════════════════════════════════════════════════════════
# PARTE 2 — caminho real (enriquecer → montar → enviar), imagem rastreada
# ══════════════════════════════════════════════════════════════════
class _Msg:
    def __init__(self, i, com_midia=False):
        self.id, self.media, self.photo = i, (object() if com_midia else None), None


_ID = {"n": 61000}


def _tag(file):
    if file is None:
        return None
    try:
        return bytes(file.getvalue()[:24]).split(b"|", 1)[0].decode()
    except Exception:                                   # noqa: BLE001
        return "?"


class Cliente:
    """Telegram falso: cada imagem carrega a TAG da mensagem de origem."""

    def __init__(self):
        self.criados, self.edits = [], []

    async def send_message(self, dest, texto, parse_mode=None, link_preview=None):
        _ID["n"] += 1
        self.criados.append((_ID["n"], None))
        return _Msg(_ID["n"])

    async def send_file(self, dest, img, caption=None, parse_mode=None,
                        force_document=False):
        _ID["n"] += 1
        self.criados.append((_ID["n"], _tag(img)))
        return _Msg(_ID["n"], com_midia=True)

    async def edit_message(self, dest, msg_id, texto, parse_mode=None, file=None):
        self.edits.append((msg_id, texto, _tag(file)))
        return _Msg(msg_id, com_midia=file is not None)

    async def delete_messages(self, dest, msg_id):
        return True

    async def download_media(self, media, file=None):
        if file is not None:
            file.write((getattr(media, "tag", "?") + "|").encode().ljust(4096, b"x"))
        return "arquivo"

    def imagens_em(self, mid):
        return ([t for i, t in self.criados if i == mid and t]
                + [t for i, _x, t in self.edits if i == mid and t])

    def ultimo(self):
        return self.criados[-1][0]


MOTIVOS_MIDIA = []
_log_real = publicacao._log_decisao


def _espiao(d, *a, **k):
    MOTIVOS_MIDIA.append((d.motivo, d.motivo_midia))
    return _log_real(d, *a, **k)


def cenario(corpo):
    async def _run():
        _isolar_loop()
        c = Cliente()
        modulo_client.client = c
        MOTIVOS_MIDIA.clear()
        publicacao._log_decisao = _espiao
        try:
            await corpo(c)
        finally:
            publicacao._log_decisao = _log_real
        return c
    _zerar_banco()
    return asyncio.run(_run())


def com_foto(n):
    tag = f"m{n.msg_id}"
    return replace(n, tem_midia=True, media_obj=types.SimpleNamespace(tag=tag),
                   midia_key=f"k-{tag}")


def promotom_110373(foto=False):
    n = msg(PROMOTOM, T_110373, [SEC_110373], C_110373)
    n = replace(n, msg_id=110373)
    return com_foto(n) if foto else n


def samuel_118773():
    return com_foto(replace(msg(SAMUEL, T_118773, L_118773, C_118773), msg_id=118773))


def test_05_24423_foto_do_samuel_nao_sobe(r):
    out = {}

    async def corpo(c):
        await publicar(promotom_110373(), score=26)
        out["post"] = c.ultimo()
        MOTIVOS_MIDIA.clear()
        await publicar(samuel_118773(), score=26)
        out["decisao"] = list(MOTIVOS_MIDIA)
        out["midia_chat"] = (db_get_post(out["post"]) or {}).get("midia_chat")
    c = cenario(corpo)
    r.check(out["decisao"] == [("DESTINO_PREVALECE", PRESERVA_COMPOSICAO)],
            "05.decisao", str(out["decisao"]))
    r.check(c.imagens_em(out["post"]) == [], "05.post_continua_sem_a_foto_do_samuel",
            str(c.imagens_em(out["post"])))
    edits = [(i, t) for i, t, img in c.edits if i == out["post"] and not img]
    r.check(len(c.edits) == 1 and len(edits) == 1
            and "OPAECONOMIZEI" in edits[0][1] and "DESCONTOSMELI" in edits[0][1],
            "05.so_o_texto_editado_no_lugar_nada_some", str(c.edits)[:300])
    r.check(out["midia_chat"] == "", "05.midia_chat_intacto", repr(out["midia_chat"]))


def test_06_reduz_real_nao_troca(r):
    out = {}

    async def corpo(c):
        await publicar(promotom_110373(), score=26)
        out["post"] = c.ultimo()
        MOTIVOS_MIDIA.clear()
        fada = com_foto(replace(msg(FADA, T_17529, [SEC_110373], ["PROMOML"]), msg_id=17529))
        await publicar(fada, score=26)
        out["decisao"] = list(MOTIVOS_MIDIA)
    c = cenario(corpo)
    r.check(out["decisao"] == [("COMPOSICAO_PERDERIA_IDENTIDADE", PRESERVA_COMPOSICAO)],
            "06.reduz_ignorado_e_preservado", str(out["decisao"]))
    r.check(c.imagens_em(out["post"]) == [] and not c.edits, "06.sem_troca",
            f"{c.imagens_em(out['post'])} {c.edits}")


def test_07_igual_com_imagem_melhor_faz_upgrade(r):
    """Promotom (mídia ruim) no ar COM foto; a Fada repete os mesmos 12
    códigos com foto de classe melhor: IGUAL → upgrade de mídia."""
    out = {}

    async def corpo(c):
        await publicar(promotom_110373(foto=True), score=26)
        out["post"] = c.ultimo()
        MOTIVOS_MIDIA.clear()
        fada = com_foto(replace(msg(FADA, T_110373, [SEC_110373], C_110373), msg_id=17530))
        await publicar(fada, score=26)
        out["decisao"] = list(MOTIVOS_MIDIA)
    c = cenario(corpo)
    r.check(len(out["decisao"]) == 1 and out["decisao"][0][1] == TROCA_UPGRADE,
            "07.igual_upgrade", str(out["decisao"]))
    r.check(c.imagens_em(out["post"])[-1:] == ["m17530"], "07.imagem_da_fada_no_post",
            str(c.imagens_em(out["post"])))


def test_08_amplia_evolui_texto_e_imagem(r):
    out = {}

    async def corpo(c):
        await publicar(promotom_110373(), score=26)
        out["post"] = c.ultimo()
        MOTIVOS_MIDIA.clear()
        amplia = com_foto(replace(msg(FADA, T_AMPLIA, [SEC_110373], C_AMPLIA), msg_id=17531))
        await publicar(amplia, score=20)
        out["decisao"] = list(MOTIVOS_MIDIA)
    c = cenario(corpo)
    r.check(out["decisao"] == [("COMPOSICAO_AMPLIADA", TROCA_POST_SEM_MIDIA)],
            "08.amplia", str(out["decisao"]))
    r.check(c.imagens_em(out["post"]) == ["m17531"]
            and any(m == out["post"] and "OPAECONOMIZEI" in (t or "") for m, t, _f in c.edits),
            "08.texto_e_imagem_do_amplia", f"{c.imagens_em(out['post'])}")


# ── LIVE: REDUZ sintético do container (publicacao) ────────────────
LIVE_7199917 = ("https://live.shopee.com.br/universal-link/share?from=live"
                "&session=7199917&utm_medium=affiliates",)
AOC32_118688 = ("🔥 Smart TV 32 Polegadas HD 32S5155/78G Roku TV AOC\n\n💵 R$ 699\n"
                "🎟 Resgate todos os cupons na Live (sacola laranja) no APP aqui:\n"
                "https://s.shopee.com.br/9AP2eWq9OA\n\nanúncio")
AOC32_35169 = ("Smart TV 32 Polegadas HD 32S5155/78G Roku TV AOC\n\nR$ 699\n"
               "-Resgate todos os cupons na Live (sacola laranja) no APP aqui:\n"
               "https://s.shopee.com.br/50ZTgk4qQ4\n\n-Anúncio")


def _live(chat, texto, msg_id, pids=(), foto=False):
    pids, tag = list(pids), f"m{msg_id}"
    return MensagemNormalizada(
        msg_id=msg_id, chat=str(chat), texto_limpo=texto, texto_analise=texto,
        mapa={"http://o": "http://a"}, preservar=[], plat="shopee",
        sku=(pids[0] if pids else ""), tem_midia=foto,
        media_obj=(types.SimpleNamespace(tag=tag) if foto else None),
        ids_globais=pids, idents=[("shopee", p, "produto") for p in pids],
        cupons=[], midia_key=(f"k-{tag}" if foto else ""),
        ancora_url=derivar_ancora_url([_canonica_live(u) for u in LIVE_7199917]))


def test_09_live_so_container_nao_troca_imagem_do_forte(r):
    """Forte (Samuel, produto + live, sem foto) no ar; a cópia só-live da
    fumotom com o mesmo título casa pelo título (doutrina LIVE intacta),
    não reescreve o texto (REDUZ sintético) — e não troca a imagem."""
    out = {}

    async def corpo(c):
        await publicar(_live(SAMUEL, AOC32_118688, 118688, pids=["7199917.32001"]), score=5)
        out["post"] = c.ultimo()
        MOTIVOS_MIDIA.clear()
        so_live = _live(FUMOTOM, AOC32_35169, 35169, foto=True)
        await publicar(so_live, score=40)
        out["decisao"] = list(MOTIVOS_MIDIA)
        out["texto"] = (db_get_post(out["post"]) or {}).get("texto", "")
    c = cenario(corpo)
    r.check(out["decisao"] == [("COMPOSICAO_PERDERIA_IDENTIDADE", PRESERVA_COMPOSICAO)],
            "09.casa_pelo_titulo_e_preserva", str(out["decisao"]))
    r.check(c.imagens_em(out["post"]) == [], "09.sem_imagem_da_copia_so_live",
            str(c.imagens_em(out["post"])))
    r.check("🔥" in out["texto"], "09.texto_do_forte_intacto", out["texto"][:60])


if __name__ == "__main__":
    sys.exit(rodar(globals(), "MÍDIA × COMPOSIÇÃO · REDUZ/PARCIAL não troca a imagem"))
