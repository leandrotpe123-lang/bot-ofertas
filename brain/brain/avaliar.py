"""Bancada de teste do juiz — o PORTÃO antes de qualquer deploy do Brain.

    cd brain
    python -m brain.avaliar orcamento --cambio 5.04        # sem rede, sem chave
    python -m brain.avaliar piloto --confirmar             # 10 chamadas Haiku, <= US$ 0,50
    python -m brain.avaliar dev --confirmar                # iteração: Haiku high, só DEV x3
    python -m brain.avaliar final --confirmar              # Haiku+Sonnet x3 efforts; Opus difícil

Regras:
  · só roda chamada real com ANTHROPIC_API_KEY de TESTE no ambiente E com
    --confirmar; a chave nunca é lida, impressa ou gravada aqui (o SDK lê);
  · teto de US$ medido pelo usage: a bancada PARA antes de passar dele; o
    teto TOTAL (--teto-total-usd, padrão 20) soma todas as rodadas gravadas;
  · cada caso 3× (o Haiku/Sonnet 5.5 não aceitam temperature: a
    estabilidade é medida, não fixada);
  · HOLDOUT: metade dos casos, escolhida por hash do id; caso que inspirou
    exemplo do prompt fica sempre no DEV. A iteração (comando dev) só vê o
    DEV; o holdout só entra no final;
  · resultado em brain/teste/resultados/<carimbo>_<comando>/
    (chamadas.jsonl + tabela.md + tabela.json).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from brain import config, juiz, politica, precos

# brain/ é a raiz do serviço (Root Directory na Railway); o pacote é brain/brain/.
AQUI = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CASOS = os.path.join(AQUI, "teste", "casos.json")
REPETICOES = 3

# Tokens de saída presumidos por effort (pensamento + JSON), só para o
# ORÇAMENTO antes do piloto. O piloto mede e o orçamento é refeito com o
# usage real antes do dev e do final (nunca gastar no chute).
SAIDA_PRESUMIDA = {"low": 450, "medium": 1000, "high": 1800}
CHARS_POR_TOKEN = 2.8          # PT-BR com emoji; conservador (tokenizador novo)


def holdout(caso_id: str) -> bool:
    return int(hashlib.sha1(caso_id.encode()).hexdigest(), 16) % 2 == 0


def em_holdout(d: dict) -> bool:
    """Caso que inspirou um exemplo do prompt nunca é holdout."""
    return not d.get("inspirou_prompt") and holdout(d["id"])


def carregar(caminho: str = CASOS):
    with open(caminho, encoding="utf-8") as f:
        dados = json.load(f)
    casos = []
    for d in dados["casos"]:
        alvo = juiz.Item(**d["alvo"])
        cands = tuple(juiz.Item(**c) for c in d["candidatos"])
        casos.append((juiz.Caso(d["id"], alvo, cands), d))
    return casos


def tokens_entrada(caso: juiz.Caso) -> int:
    return int((len(juiz.SISTEMA) + len(juiz.mensagem(caso))) / CHARS_POR_TOKEN) + 300


RODADAS_DEV = 3                # rodadas de iteração do prompt previstas no orçamento
OPUS_DIFICIL_MAX = 30          # teto de casos do subconjunto difícil (Opus)
HAIKU, SONNET, OPUS = "claude-haiku-5-5", "claude-sonnet-5-5", "claude-opus-5-5"


def fases_por_comando(casos) -> dict:
    """Fases (decisão do dono, 14/10): iterar no Haiku só com o DEV; medir
    Haiku e Sonnet em high/medium/low com todos os casos; Opus só no
    subconjunto difícil, se sobrar orçamento."""
    dev = [c for c, d in casos if not em_holdout(d)]
    todos = [c for c, d in casos]
    return {
        "piloto": [("piloto_haiku_high", HAIKU, "high", dev[:10], 1)],
        "dev": [("dev_haiku_high", HAIKU, "high", dev, REPETICOES)],
        "final": [(f"final_{m}_{e}", m, e, todos, REPETICOES)
                  for m in (HAIKU, SONNET) for e in ("high", "medium", "low")],
    }


def _usd(modelo, effort, lista, reps) -> float:
    return sum(precos.estimar_usd(modelo, tokens_entrada(c), SAIDA_PRESUMIDA[effort])
               for c in lista) * reps


def orcamento(casos, cambio: float) -> list:
    """(fase, modelo, effort, chamadas, US$, R$) — presumido, ANTES de gastar."""
    f = fases_por_comando(casos)
    linhas = []
    for nome, m, e, lista, reps in f["piloto"]:
        linhas.append((nome, m, e, len(lista) * reps, _usd(m, e, lista, reps)))
    for nome, m, e, lista, reps in f["dev"]:
        linhas.append((f"{nome}_x{RODADAS_DEV}_rodadas", m, e, len(lista) * reps * RODADAS_DEV,
                       _usd(m, e, lista, reps) * RODADAS_DEV))
    for nome, m, e, lista, reps in f["final"]:
        linhas.append((nome, m, e, len(lista) * reps, _usd(m, e, lista, reps)))
    todos = [c for c, _ in casos]
    caros = sorted(todos, key=tokens_entrada, reverse=True)[:OPUS_DIFICIL_MAX]
    linhas.append((f"opus_dificil_ate_{OPUS_DIFICIL_MAX}_casos", OPUS, "high",
                   len(caros) * REPETICOES, _usd(OPUS, "high", caros, REPETICOES)))
    linhas.append(("TOTAL", "", "", sum(l[3] for l in linhas), sum(l[4] for l in linhas)))
    return [(n, m, e, k, u, u * cambio) for n, m, e, k, u in linhas]


def gasto_acumulado(pasta: str) -> float:
    """Soma do custo MEDIDO de todas as rodadas já gravadas — o teto total
    vale entre comandos (piloto + dev + final), não só dentro de um."""
    total = 0.0
    if not os.path.isdir(pasta):
        return 0.0
    for sub in sorted(os.listdir(pasta)):
        arq = os.path.join(pasta, sub, "chamadas.jsonl")
        if os.path.exists(arq):
            with open(arq, encoding="utf-8") as f:
                total += sum(json.loads(l).get("custo_usd", 0.0) for l in f if l.strip())
    return total


def _chave_presente() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


def contexto_do_caso(d: dict) -> "politica.Contexto":
    """Contexto da política para o caso, em modo ACAO (mede o que FARIA)."""
    cands = d["candidatos"]
    delta = min(abs(c["delta_min"] - d["alvo"]["delta_min"]) for c in cands) if cands else 0.0
    ctx = dict(d.get("contexto") or {})
    return politica.Contexto(tipo=d.get("tipo", "par_canal"), delta_s=delta * 60.0,
                             modo="ACAO", **ctx)


class _V:
    """Veredito mínimo para a política (a config entra como validada: a
    bancada mede o que a política FARIA se esta config passasse)."""
    def __init__(self, ch):
        self.veredito, self.certeza = ch["veredito"], float(ch.get("certeza") or 0.0)
        self.lembrete = bool(ch.get("lembrete"))


def resultado(ch: dict, d: dict) -> str:
    """certo | falso_destrutivo | falso_reinjetar | alerta_falso | perdeu —
    pelo que a política faria com este veredito neste caso."""
    faria, _ = politica._base(_V(ch), contexto_do_caso(d))
    um_post = d["deve"] == "UM_POST"
    if d.get("tipo", "par_canal") == "par_canal":
        if faria == "PEDIR_FUNDIR":
            return "certo" if um_post else "falso_destrutivo"
        if faria == "ALERTAR" and not um_post:
            return "alerta_falso"          # ruído na DM do estágio ALERTA
        return "perdeu" if um_post else "certo"
    if faria == "PEDIR_REINJETAR":
        return "falso_reinjetar" if um_post else "certo"
    if faria == "ALERTAR" and not um_post:
        return "certo"
    return "certo" if um_post else "perdeu"


def avaliar_resultados(chamadas, meta):
    """Métricas por configuração (modelo, effort). `meta`: id → caso do
    casos.json. Duas réguas:
      · veredito — acerto = veredito ∈ aceitos; falso_mesma_campanha =
        MESMA_CAMPANHA num caso cujo rótulo não é (exceto lembrete aceito);
      · resultado — o que a política faria: falso_destrutivo (pediria
        fundir onde o certo são dois posts) e falso_reinjetar são o PORTÃO."""
    por_cfg = defaultdict(list)
    for ch in chamadas:
        por_cfg[(ch["modelo"], ch["effort"])].append(ch)
    tabela = {}
    for cfg, lista in por_cfg.items():
        por_caso = defaultdict(list)
        for ch in lista:
            por_caso[ch["caso"]].append(ch)
        res = {"n_chamadas": len(lista)}
        for parte, filtro in (("todos", lambda d: True), ("holdout", em_holdout),
                              ("dono", lambda d: d.get("fonte_rotulo") == "dono")):
            m = defaultdict(int)
            for cid, chs in por_caso.items():
                d = meta[cid]
                if not filtro(d):
                    continue
                rot, aceitos = d["rotulo"], set(d.get("aceitos") or [d["rotulo"]])
                m["casos"] += 1
                m["casos_estaveis"] += len({c["veredito"] for c in chs}) == 1
                for ch in chs:
                    m["chamadas"] += 1
                    m[f"n_{d['deve']}"] += 1
                    m["acerto_veredito"] += ch["veredito"] in aceitos
                    m["incerto"] += ch["veredito"] == "INCERTO"
                    if (ch["veredito"] == "MESMA_CAMPANHA" and rot != "MESMA_CAMPANHA"
                            and "MESMA_CAMPANHA" not in aceitos):
                        m["falso_mesma_campanha"] += 1
                    r = resultado(ch, d)
                    m[f"r_{r}"] += 1
                    if r == "certo":
                        m[f"certo_{d['deve']}"] += 1
            res[parte] = dict(m)
        lat = sorted(ch["latencia_ms"] for ch in lista if ch.get("latencia_ms"))
        if lat:
            res["lat_p50_ms"] = lat[len(lat) // 2]
            res["lat_p95_ms"] = lat[min(len(lat) - 1, int(len(lat) * 0.95))]
        res["usd_por_chamada"] = statistics.mean(ch["custo_usd"] for ch in lista)
        res["tokens_in_med"] = statistics.mean(ch["usage"].get("input_tokens", 0) for ch in lista)
        res["tokens_out_med"] = statistics.mean(ch["usage"].get("output_tokens", 0) for ch in lista)
        tabela[cfg] = res
    return tabela


def passa_no_portao(res: dict) -> bool:
    """Portão do dono: ZERO falso destrutivo, ZERO falso reinjetar e ZERO
    falso MESMA_CAMPANHA — em todos os casos, não só no holdout."""
    t = res.get("todos", {})
    return (t.get("chamadas", 0) > 0 and t.get("r_falso_destrutivo", 0) == 0
            and t.get("r_falso_reinjetar", 0) == 0 and t.get("falso_mesma_campanha", 0) == 0)


def subconjunto_dificil(chamadas, meta, limite=30):
    """Casos onde Haiku ou Sonnet (high) erraram, deram INCERTO ou variaram
    entre as repetições — é onde o Opus é medido."""
    por = defaultdict(list)
    for ch in chamadas:
        if ch["effort"] == "high" and ch["modelo"] in ("claude-haiku-5-5", "claude-sonnet-5-5"):
            por[ch["caso"]].append(ch["veredito"])
    def aceitos(cid):
        d = meta[cid]
        return set(d.get("aceitos") or [d["rotulo"]])
    duros = [cid for cid, vs in por.items()
             if len(set(vs)) > 1 or any(v not in aceitos(cid) for v in vs)]
    return sorted(duros)[:limite]


def cascata_offline(chamadas, principal, referencia, certeza_min=config.CASCATA_CERTEZA_MIN):
    """Cascata calculada das chamadas já feitas (sem gastar de novo):
    a referência entra onde o principal deu INCERTO ou certeza baixa."""
    p = {(c["caso"], c["rep"]): c for c in chamadas if (c["modelo"], c["effort"]) == principal}
    r = {(c["caso"], c["rep"]): c for c in chamadas if (c["modelo"], c["effort"]) == referencia}
    out = []
    for k, cp in p.items():
        sobe = cp["veredito"] == "INCERTO" or cp["certeza"] < certeza_min
        if sobe and k in r:
            cr = dict(r[k])
            cr["custo_usd"] = cp["custo_usd"] + r[k]["custo_usd"]
            cr["modelo"], cr["effort"] = "cascata", f"{principal[0]}→{referencia[0]}"
            out.append(cr)
        else:
            c2 = dict(cp)
            c2["modelo"], c2["effort"] = "cascata", f"{principal[0]}→{referencia[0]}"
            out.append(c2)
    return out


def rodar(casos, fases, teto_usd: float, saida: str, cliente=None, dormir=time.sleep,
          paralelo: int = 1) -> float:
    """Roda as fases e grava cada chamada (uma linha, gravada na hora) em
    <saida>/chamadas.jsonl. RETOMÁVEL: o que já está gravado nessa pasta
    (fase, caso, repetição) não é refeito — o contêiner pode reciclar no
    meio de uma rodada longa. Com `paralelo` > 1, o teto continua valendo:
    cada chamada RESERVA o custo presumido antes de sair."""
    from brain.cliente import ClienteAnthropic, Juiz, Tetos
    os.makedirs(saida, exist_ok=True)
    arq = os.path.join(saida, "chamadas.jsonl")
    feitos = set()
    if os.path.exists(arq):
        with open(arq, encoding="utf-8") as f:
            for l in f:
                if l.strip():
                    d = json.loads(l)
                    feitos.add((d["fase"], d["caso"], d["rep"]))
    tarefas = [(nome, modelo, effort, rep, caso)
               for nome, modelo, effort, lista, reps in fases
               for rep in range(reps) for caso in lista
               if (nome, caso.id, rep) not in feitos]
    cli = cliente or ClienteAnthropic()
    trava = threading.Lock()
    estado = {"gasto": 0.0, "reservado": 0.0, "parou": False}
    local = threading.local()

    def juiz():
        if not hasattr(local, "j"):
            local.j = Juiz(cli, tetos=Tetos(chamadas_dia=10**9, usd_dia=teto_usd), dormir=dormir)
        return local.j

    with open(arq, "a", encoding="utf-8", buffering=1) as f:
        def uma(t):
            nome, modelo, effort, rep, caso = t
            est = precos.estimar_usd(modelo, tokens_entrada(caso), SAIDA_PRESUMIDA[effort])
            with trava:
                if estado["parou"] or estado["gasto"] + estado["reservado"] + est > teto_usd:
                    estado["parou"] = True
                    return
                estado["reservado"] += est
            try:
                v = juiz().julgar(caso, modelo, effort)
            finally:
                with trava:
                    estado["reservado"] -= est
            with trava:
                estado["gasto"] += v.custo_usd
                f.write(json.dumps({
                    "fase": nome, "caso": caso.id, "rep": rep, "modelo": v.modelo,
                    "effort": v.effort, "veredito": v.veredito, "certeza": v.certeza,
                    "ids": list(v.ids), "acao_sugerida": v.acao_sugerida,
                    "lembrete": v.lembrete, "motivo": v.motivo,
                    "stop_reason": v.stop_reason, "latencia_ms": v.latencia_ms,
                    "usage": v.usage, "custo_usd": v.custo_usd,
                    "versao_prompt": v.versao_prompt}, ensure_ascii=False) + "\n")
                f.flush()

        if paralelo <= 1:
            for t in tarefas:
                if estado["parou"]:
                    break
                uma(t)
        else:
            with ThreadPoolExecutor(max_workers=paralelo) as ex:
                list(ex.map(uma, tarefas))
    if estado["parou"]:
        print(f"PARADA: teto de US$ {teto_usd:.2f} (gasto {estado['gasto']:.4f})")
    return estado["gasto"]


def escrever_tabela(saida: str, meta: dict, cambio: float) -> dict:
    feitas = [json.loads(l) for l in open(os.path.join(saida, "chamadas.jsonl"), encoding="utf-8")]
    extra = cascata_offline(feitas, config.MODELO_PRINCIPAL, config.MODELO_REFERENCIA)
    tab = avaliar_resultados(feitas + extra, meta)
    with open(os.path.join(saida, "tabela.json"), "w", encoding="utf-8") as f:
        json.dump({f"{m}|{e}": dict(v, passa=passa_no_portao(v)) for (m, e), v in tab.items()},
                  f, ensure_ascii=False, indent=1)
    linhas = ["| config | PORTÃO | falso destrutivo | falso reinjetar | falso MESMA | "
              "acerto veredito (holdout) | um post certo | dois posts certo | INCERTO | "
              "estáveis | p50 ms | p95 ms | US$/chamada" + (" | R$/chamada" if cambio else "") + " |",
              "|" + "---|" * (14 if cambio else 13)]
    for (m, e), v in sorted(tab.items()):
        t, h = v.get("todos", {}), v.get("holdout", {})
        linhas.append(
            f"| {m} {e} | {'PASSA' if passa_no_portao(v) else 'reprova'} | "
            f"{t.get('r_falso_destrutivo', 0)} | {t.get('r_falso_reinjetar', 0)} | "
            f"{t.get('falso_mesma_campanha', 0)} | "
            f"{h.get('acerto_veredito', 0)}/{h.get('chamadas', 0)} | "
            f"{t.get('certo_UM_POST', 0)}/{t.get('n_UM_POST', 0)} | "
            f"{t.get('certo_DOIS_POSTS', 0)}/{t.get('n_DOIS_POSTS', 0)} | "
            f"{t.get('incerto', 0)}/{t.get('chamadas', 0)} | "
            f"{t.get('casos_estaveis', 0)}/{t.get('casos', 0)} | "
            f"{v.get('lat_p50_ms', 0):.0f} | {v.get('lat_p95_ms', 0):.0f} | "
            f"{v['usd_por_chamada']:.5f}" + (f" | {v['usd_por_chamada'] * cambio:.5f}" if cambio else "") + " |")
    with open(os.path.join(saida, "tabela.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(linhas) + "\n")
    return tab


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="cd brain && python -m brain.avaliar")
    ap.add_argument("comando", choices=["orcamento", "piloto", "dev", "final"])
    ap.add_argument("--teto-usd", type=float, default=None,
                    help="teto DESTE comando (piloto: no máximo 0,50)")
    ap.add_argument("--teto-total-usd", type=float, default=20.0,
                    help="teto da bancada inteira, somando todas as rodadas gravadas")
    ap.add_argument("--cambio", type=float, default=0.0,
                    help="R$ por US$ (informar com a data e a fonte)")
    ap.add_argument("--confirmar", action="store_true")
    ap.add_argument("--paralelo", type=int, default=4,
                    help="chamadas simultâneas (padrão 4)")
    ap.add_argument("--retomar", default=None,
                    help="pasta de uma rodada interrompida DESTE comando: continua de onde parou")
    ap.add_argument("--resultados", default=os.path.join(AQUI, "teste", "resultados"),
                    help=argparse.SUPPRESS)
    a = ap.parse_args(argv)
    casos = carregar()
    pasta = a.resultados
    if a.comando == "orcamento":
        print(f"{len(casos)} casos ({sum(em_holdout(d) for _, d in casos)} no holdout; "
              f"dev = {sum(not em_holdout(d) for _, d in casos)})")
        for n, m, e, k, u, brl in orcamento(casos, a.cambio or 0.0):
            print(f"{n:40} {m:18} {e:6} chamadas={k:5}  US$ {u:7.2f}"
                  + (f"  R$ {brl:7.2f}" if a.cambio else ""))
        print(f"já gasto (medido, rodadas gravadas): US$ {gasto_acumulado(pasta):.4f}")
        return 0
    if not _chave_presente():
        print("ANTHROPIC_API_KEY ausente: configure a chave de TESTE no ambiente.")
        return 2
    if not a.confirmar:
        print("Recusado: chamada real exige --confirmar (o orçamento vem antes).")
        return 2
    print("chave: presente (não exibida)")
    ja = gasto_acumulado(pasta)
    teto = a.teto_total_usd - ja
    if a.teto_usd is not None:
        teto = min(teto, a.teto_usd)
    if a.comando == "piloto":
        teto = min(teto, 0.50)
    if teto <= 0:
        print(f"PARADA: teto total de US$ {a.teto_total_usd:.2f} já atingido (gasto US$ {ja:.4f}).")
        return 3
    if a.retomar:
        saida = os.path.abspath(a.retomar)
        if not os.path.basename(saida).endswith("_" + a.comando) or not os.path.isdir(saida):
            print(f"Recusado: --retomar precisa ser uma pasta existente de '{a.comando}'.")
            return 2
    else:
        saida = os.path.join(pasta, time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "_" + a.comando)
    gasto = rodar(casos, fases_por_comando(casos)[a.comando], teto, saida, paralelo=a.paralelo)
    meta = {c.id: d for c, d in casos}
    if a.comando == "final" and gasto < teto:
        feitas = [json.loads(l) for l in open(os.path.join(saida, "chamadas.jsonl"), encoding="utf-8")]
        duros = set(subconjunto_dificil(feitas, meta, OPUS_DIFICIL_MAX))
        lista = [c for c, _ in casos if c.id in duros]
        if lista:
            gasto += rodar(casos, [("final_opus_high_dificil", OPUS, "high", lista, REPETICOES)],
                           teto - gasto, saida, paralelo=a.paralelo)
    if os.path.exists(os.path.join(saida, "chamadas.jsonl")):
        escrever_tabela(saida, meta, a.cambio)
    print(f"gasto medido nesta rodada: US$ {gasto:.4f}; acumulado: US$ {ja + gasto:.4f} → {saida}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
