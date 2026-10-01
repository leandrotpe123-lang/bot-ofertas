"""
MÍDIA × LÍDER — TROCA/fonte_editou é a edição do LÍDER, não do canal.

Incidente 01/10 00:03 (post 24393, cupons 30/90 da Fada 17525): a Fada
editou OUTRA mensagem — a 17526, "Novo Cupom Shopee Tech" — para pôr a
foto. A regra comparava só o canal (`is_edit and chat_novo ==
midia_chat`) e a foto do Tech entrou no post dos cupons 30/90.

Regra: `is_edit and chat_novo == midia_chat and (lider_msg is None or
msg_novo == lider_msg)` — `lider_msg` é a MESMA autoridade da Frente
LIVE (`estado["lider_msg"]`, lida em decisao). None = post legado.

  01  política pura: líder troca; outra mensagem do mesmo canal não;
      lider_msg None = regra antiga; sem msg_novo e com líder = não troca
  02  as outras saídas da política e a guarda de delta: intocadas
  03  decidir(): o msg_id da mensagem chega à política (replay 17526)

    python tests/test_midia_lider.py
"""
import os
import sys
import time
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar  # noqa: E402

preparar()

try:
    import yarl  # noqa: F401
except ImportError:
    _yarl = types.ModuleType("yarl")

    class _URL(str):
        def __new__(cls, valor="", **_kw):
            return str.__new__(cls, valor)
    _yarl.URL = _URL
    sys.modules["yarl"] = _yarl

from pipeline.decisao import decidir                            # noqa: E402
from pipeline.midia_politica import (                           # noqa: E402
    PRESERVA_DESCONHECIDA, PRESERVA_MESMA_CLASSE, PRESERVA_MIDIA_IGUAL,
    PRESERVA_SEM_IMAGEM, TROCA_FONTE_EDITOU, TROCA_POST_SEM_MIDIA,
    politica_midia)
from pipeline.montagem import MensagemMontada                   # noqa: E402
from pipeline.normalizacao import MensagemNormalizada           # noqa: E402

FADA = "-1002050488946"
SAMUEL = "-1001768101197"
BUF = b"\xff\xd8imagem"


def test_01_politica_lider(r):
    est = {"midia_chat": FADA, "lider_msg": 17525}
    r.check(politica_midia(BUF, FADA, est, True, msg_novo=17525)
            == (True, TROCA_FONTE_EDITOU), "01.lider_editou_troca")
    r.check(politica_midia(BUF, FADA, est, True, msg_novo=17526)
            == (False, PRESERVA_MESMA_CLASSE), "01.outra_mensagem_nao_troca",
            str(politica_midia(BUF, FADA, est, True, msg_novo=17526)))
    r.check(politica_midia(BUF, FADA, est, True)[1] != TROCA_FONTE_EDITOU,
            "01.sem_msg_novo_nao_e_lider")
    leg = {"midia_chat": FADA}
    r.check(politica_midia(BUF, FADA, leg, True, msg_novo=17526)
            == (True, TROCA_FONTE_EDITOU), "01.legado_regra_por_canal")
    r.check(politica_midia(BUF, FADA, {"midia_chat": FADA, "lider_msg": None},
                           True, msg_novo=1) == (True, TROCA_FONTE_EDITOU),
            "01.lider_none_regra_por_canal")
    r.check(politica_midia(BUF, FADA, est, False, msg_novo=17525)[1]
            != TROCA_FONTE_EDITOU, "01.mensagem_nova_nao_e_edicao")
    r.check(politica_midia(BUF, SAMUEL, est, True, msg_novo=17525)[1]
            != TROCA_FONTE_EDITOU, "01.outro_canal_nao_e_fonte")


def test_02_demais_saidas_intocadas(r):
    est = {"midia_chat": FADA, "lider_msg": 17525}
    r.check(politica_midia(None, FADA, est, True, msg_novo=17525)
            == (False, PRESERVA_SEM_IMAGEM), "02.sem_imagem")
    r.check(politica_midia(BUF, FADA, {"lider_msg": 1}, True, msg_novo=1)
            == (False, PRESERVA_DESCONHECIDA), "02.desconhecida")
    r.check(politica_midia(BUF, FADA, {"midia_chat": "", "lider_msg": 1}, True,
                           msg_novo=2) == (True, TROCA_POST_SEM_MIDIA),
            "02.post_sem_midia")
    r.check(politica_midia(BUF, FADA, est, True, chave_nova="k", chave_aceita="k",
                           msg_novo=17525) == (False, PRESERVA_MIDIA_IGUAL),
            "02.guarda_de_delta")


def _norm(chat, msg_id):
    return MensagemNormalizada(
        msg_id=msg_id, chat=chat, texto_limpo="🔥 Novo Cupom Shopee Tech",
        texto_analise="🔥 Novo Cupom Shopee Tech", mapa={}, preservar=[],
        plat="shopee", sku="", tem_midia=True, media_obj=None, ids_globais=[],
        idents=[], cupons=[])


def test_03_decidir_repassa_msg_id(r):
    agora = time.time()
    est = {"score": 10, "texto": "🚨 Novos Cupons Shopee\nR$30/R$90", "lider": FADA,
           "lider_msg": 17525, "midia_chat": FADA, "edit_count": 1,
           "janela_fim": agora + 1200, "ts": agora - 200}
    for msg_id, esperado in ((17526, PRESERVA_MESMA_CLASSE),
                             (17525, TROCA_FONTE_EDITOU)):
        n = _norm(FADA, msg_id)
        m = MensagemMontada(msg_id, FADA, "shopee", "", n.texto_limpo, BUF, {}, msg_id)
        d = decidir(n, m, 7, est, agora, True)
        r.check(d.motivo_midia == esperado, f"03.msg_{msg_id}",
                f"{d.acao} {d.motivo} {d.motivo_midia}")
    leg = dict(est, lider_msg=None)
    n = _norm(FADA, 17526)
    m = MensagemMontada(17526, FADA, "shopee", "", n.texto_limpo, BUF, {}, 17526)
    r.check(decidir(n, m, 7, leg, agora, True).motivo_midia == TROCA_FONTE_EDITOU,
            "03.post_legado_regra_por_canal")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "MÍDIA × LÍDER · TROCA/fonte_editou"))
