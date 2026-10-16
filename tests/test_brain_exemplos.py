"""
Brain — exemplos do dono (decisão de 16/10): só casos marcados por ele,
nunca do holdout, nunca o próprio caso; 3–5 mais parecidos. Hermético.

    python tests/test_brain_exemplos.py
"""
import contextlib
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _harness_e5 import preparar, rodar                         # noqa: E402

RAIZ = preparar()
sys.path.insert(0, os.path.join(RAIZ, "brain"))
from brain import cliente as CL, contexto as C, divisao, exemplos as E, juiz as J  # noqa: E402

H = 3600.0


def ts(iso):
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp()


def post(pid, iso, texto, links=(), codigos=(), estado="vivo", grupo=""):
    return C.Post(f"canal:{pid}", "canal", ts(iso), texto, tuple(links), tuple(codigos),
                  grupo=grupo, estado=estado)


# ══════════════════════════════════════════════════════════════════
# Exemplos do dono
# ══════════════════════════════════════════════════════════════════
def _caso(cid, fonte="dono", plat="shopee", texto="Cupons Shopee"):
    return {"id": cid, "deve": "UM_POST", "rotulo": "MESMA_CAMPANHA", "fonte_rotulo": fonte,
            "alvo": {"plataforma": plat, "grupo": "fada", "texto": texto, "delta_min": 0.0},
            "candidatos": [{"grupo": "promotom", "texto": "par", "delta_min": -3.0}]}


def test_20_exemplos_so_do_dono_e_nunca_do_holdout(r):
    ids = [f"SN{i:02d}" for i in range(1, 41)]
    casos = [_caso(i) for i in ids] + [_caso("F99", fonte="claude")]
    ex = E.do_conjunto(casos)
    r.check(all(not divisao.em_holdout({"id": e.id}) for e in ex), "20.nenhum_do_holdout")
    r.check("F99" not in [e.id for e in ex], "20.so_rotulo_do_dono")
    r.check(len(ex) == sum(1 for i in ids if not divisao.holdout(i)), "20.todos_os_do_dev")


def test_21_escolhe_3_a_5_mais_parecidos_e_nunca_o_proprio_caso(r):
    casos = [_caso(f"D{i}", texto=t) for i, t in enumerate(
        ["Cupons Shopee VIP 50%", "Cupom VIP 50% Shopee ainda ativo", "Air fryer Mondial", "Cupons Shopee",
         "Fone JBL", "Cupom Shopee VIP"])]
    ex = [e for e in (E.Exemplo(d["id"], "UM_POST", "", "shopee", "", d["alvo"]["texto"], "", "", 0.0)
                      for d in casos)]
    esc = E.escolher("Cupons Shopee VIP 50% OFF", "shopee", ex, excluir=frozenset({"D0"}))
    r.check(3 <= len(esc) <= 5 and "D0" not in [e.id for e in esc], "21.sem_o_proprio_caso")
    r.check(esc[0].id in ("D1", "D5"), "21.mais_parecido_primeiro", [e.id for e in esc])
    r.check(len(E.escolher("x", "shopee", ex, k=9)) == 5 and len(E.escolher("x", "shopee", ex, k=1)) == 3,
            "21.k_entre_3_e_5")
    mesmo = E.escolher("Air fryer Mondial", "shopee", ex, excluir_textos=frozenset({"Air fryer Mondial"}))
    r.check("D2" not in [e.id for e in mesmo], "21.mesmo_alvo_em_outro_caso_fora")


if __name__ == "__main__":
    sys.exit(rodar(globals(), "Brain · exemplos do dono (só dev)"))
