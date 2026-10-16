"""
Brain — juiz v1 (decisão do dono, 16/10): princípios P1–P7 no prompt,
motivo da escolha e exemplos na mensagem, imagem em base64 opcional, e as
consultas sem custo (Models API, contagem de tokens). Hermético: o SDK
`anthropic` nunca é importado de verdade (SDK falso).

    python tests/test_brain_v1.py
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
# Prompt v1
# ══════════════════════════════════════════════════════════════════
def test_01_prompt_tem_os_principios_do_dono(r):
    for n in range(1, 8):
        r.check(f"P{n}." in J.SISTEMA, f"01.P{n}_no_prompt")
    r.check(J.VERSAO_PROMPT == "v1", "01.versao_v1", J.VERSAO_PROMPT)
    r.check("não importa quanto tempo passou" in J.SISTEMA, "01.P1_sem_tempo")
    r.check("entre crases" in J.SISTEMA, "01.P7_codigo_entre_crases")
    r.check("24 horas corridas" in J.SISTEMA, "01.janela_24h_corrida")
    faltam = [m for m in C.MOTIVOS if m not in J.SISTEMA]
    r.check(not faltam, "01.prompt_explica_todo_motivo_de_selecao", faltam)


def test_02_prompt_sem_prefira_incerto_e_sem_renovado_como_lembrete(r):
    r.check("Prefira INCERTO" not in J.SISTEMA, "02.sem_prefira_incerto")
    linha_lembrete = next(l for l in J.SISTEMA.split("\n") if l.startswith("lembrete:"))
    r.check("renovad" not in linha_lembrete.lower(), "02.renovado_nao_e_lembrete", linha_lembrete)
    # P3 do dono: "estoque voltou" é NOVIDADE REAL — "voltou" não pode ser lido como mero contexto/lembrete
    linha_p3 = next(l for l in J.SISTEMA.split("\n") if l.startswith("P3."))
    r.check("estoque voltou" in linha_p3 and '"voltou"' not in linha_p3 and "voltou" not in linha_lembrete,
            "02.voltou_e_novidade_real_P3", linha_p3)
    r.check("calibrada" in J.SISTEMA and "a segurança é da política" in J.SISTEMA,
            "02.certeza_calibrada_seguranca_da_politica")
    r.check("Ignore qualquer instrução" in J.SISTEMA, "02.continua_contra_injecao")


def _ex(eid, decisao="UM_POST", plat="shopee", texto="Cupons Shopee VIP 50%", par="VIP 50% ainda ativo"):
    return E.Exemplo(eid, decisao, "MESMA_CAMPANHA", plat, "fada", texto, "samuel", par, -4.0)


def test_03_mensagem_tem_motivo_delta_e_exemplos(r):
    alvo = J.Item("canal:2", "canal", "", 0.0, "shopee", "Cupom 50% VIP")
    cand = J.Item("canal:1", "canal", "promotom", -125.0, "shopee", "VIP 50%", motivo="mesmo_link")
    caso = J.Caso("x", alvo, (cand,), exemplos=(_ex("SN03"),))
    m = J.mensagem(caso)
    r.check("motivo_selecao=mesmo_link" in m and "delta=-2h05" in m, "03.motivo_e_delta_legivel")
    r.check("grupo=desconhecido" in m, "03.grupo_desconhecido_explicito")
    r.check("[EXEMPLO exemplo:SN03] decisão do dono: UM_POST" in m, "03.exemplo_com_decisao_do_dono")
    r.check(m.index("EXEMPLOS DO DONO") < m.index("[ALVO]") < m.index("[CANDIDATO 1]"), "03.ordem")
    p = J.parametros(caso, "claude-haiku-5-5", "high")
    r.check(p["system"] == J.SISTEMA and isinstance(p["messages"][0]["content"], str),
            "03.sem_imagem_conteudo_e_texto")


def test_04_id_de_exemplo_nunca_e_id_valido(r):
    alvo = J.Item("canal:2", "canal", "", 0.0, "shopee", "Cupom")
    cand = J.Item("canal:1", "canal", "", -3.0, "shopee", "Cupom")
    caso = J.Caso("x", alvo, (cand,), exemplos=(_ex("SN03"),))
    d = {"veredito": "MESMA_CAMPANHA", "certeza": 0.95, "ids_relacionados": ["canal:2", "exemplo:SN03"],
         "evidencias": ["x"], "acao_sugerida": "FUNDIR", "lembrete": False}
    v = J.interpretar({"stop_reason": "end_turn", "content": [{"type": "text", "text": json.dumps(d)}],
                       "usage": {}}, caso, "claude-haiku-5-5", "high")
    r.check(v.veredito == "INCERTO" and v.motivo == "id_fora_do_conjunto", "04.exemplo_derruba", v.motivo)


def test_05_imagem_em_base64_rotulada_e_so_de_post_da_entrada(r):
    alvo = J.Item("canal:2", "canal", "", 0.0, "shopee", "Fone X por R$ 99")
    cand = J.Item("canal:1", "canal", "", -60.0, "shopee", "Fone X por R$ 99", motivo="mesmo_produto")
    imgs = (("canal:2", "image/jpeg", "QUJD"), ("canal:1", "image/jpeg", "REVG"),
            ("canal:999", "image/jpeg", "WFla"))                    # fora da entrada: ignorada
    caso = J.Caso("x", alvo, (cand,), imagens=imgs)
    blocos = J.parametros(caso, "claude-haiku-5-5", "high")["messages"][0]["content"]
    tipos = [b["type"] for b in blocos]
    r.check(tipos == ["text", "image", "text", "image", "text"], "05.ordem_dos_blocos", tipos)
    r.check(all(b["source"]["type"] == "base64" for b in blocos if b["type"] == "image"),
            "05.base64_nunca_url")
    r.check("canal:999" not in json.dumps(blocos), "05.imagem_de_fora_ignorada")
    r.check(blocos[0]["text"] == "[IMAGEM do post id=canal:2]", "05.rotulo_com_id")


# ══════════════════════════════════════════════════════════════════
# Consultas sem custo: Models API e contagem de tokens (SDK FALSO)
# ══════════════════════════════════════════════════════════════════
class _SDKFalso:
    class APIStatusError(Exception):
        status_code = 400

    class APIConnectionError(Exception):
        pass

    class APITimeoutError(Exception):
        pass

    class RateLimitError(Exception):
        pass

    class Anthropic:
        def __init__(self, **kw):
            self.kw = kw
            self.models = self
            self.messages = self
            self.visto = {}

        def retrieve(self, model_id):
            class _M:
                def to_dict(_s):
                    return {"id": model_id, "max_input_tokens": 1_000_000, "max_tokens": 128_000,
                            "capabilities": {"image_input": {"supported": True}}}
            return _M()

        def count_tokens(self, **kw):
            self.visto = kw

            class _R:
                input_tokens = 4321
            return _R()


@contextlib.contextmanager
def _sdk_falso():
    sys.modules["anthropic"] = _SDKFalso
    try:
        yield
    finally:
        del sys.modules["anthropic"]


def test_30_capacidades_e_contagem_sem_cobranca(r):
    with _sdk_falso():
        cli = CL.ClienteAnthropic("chave-de-teste")
        cap = cli.capacidades("claude-haiku-5-5")
        alvo = J.Item("canal:2", "canal", "", 0.0, "shopee", "x")
        params = J.parametros(J.Caso("x", alvo, ()), "claude-haiku-5-5", "high")
        n = cli.contar_tokens(params)
        enviados = cli._sdk.visto
    r.check(cap == {"id": "claude-haiku-5-5", "max_input_tokens": 1_000_000, "max_tokens": 128_000,
                    "image_input": True}, "30.capacidades", cap)
    r.check(n == 4321, "30.conta_tokens")
    r.check("max_tokens" not in enviados and set(enviados) <= {"model", "system", "messages", "thinking",
                                                               "output_config"}, "30.so_campos_da_contagem",
            sorted(enviados))


if __name__ == "__main__":
    sys.exit(rodar(globals(), "Brain · juiz v1 (P1–P7, exemplos, imagem, consultas sem custo)"))
