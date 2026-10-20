"""
CUPOM SEM CÓDIGO — LANÇAMENTO × LIBERAÇÃO (regra pura, três faixas).

Responsabilidade ÚNICA: dado um candidato `cupb` e os posts `cupb` vivos da
mesma plataforma, responder se a mensagem é o MESMO LANÇAMENTO de um post
vivo (junta), uma LIBERAÇÃO NOVA (post novo) ou um caso INCERTO (fica o
comportamento de hoje e a dúvida é registrada).

Por que existe — os dois erros medidos em 07–14/10/2026 (logs da Railway +
prévia pública; ver o PR da frente CUPOM-SEM-CÓDIGO):
  · FALSO NEGATIVO: a mesma campanha chega com nomes diferentes por fonte e
    vira dois posts (posts 28391 × 28392: `proximo` × `lojas-oficiais`).
  · FALSO POSITIVO: a série horária da mesma fonte, com o mesmo texto, parece
    a mesma oferta e a liberação nova some (Promotom 111092 contra o post
    27046, Δ 55 min, SCORE_NAO_EVOLUI; 111103 contra o 27126, Δ 59,7 min,
    DUP_SILENCIOSO).

Princípio: identidade do lançamento = plataforma + benefício + momento. O
nome declarado só SEPARA (nomes diferentes = campanhas diferentes; nome
ausente é compatível). Mensagem nova da mesma fonte já ligada ao post vivo
é liberação nova, salvo lembrete.

NÃO faz: banco, Telegram, log, relógio, decisão de texto (score/decisao),
família por overlap (familia) — e NÃO muda chave nenhuma: as âncoras de
hoje continuam as de sempre. Quem lê o banco e registra é
familia.lancamento_cupom; quem aplica (só em modo ATIVO) é publicacao.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, Optional

from pipeline.assunto import beneficio_do_cupom
from pipeline.assunto_oferta import assinatura_do_beneficio, nome_declarado

__all__ = ["Perfil", "PostVivo", "Decisao", "perfil", "candidato",
           "pode_mudar", "classificar", "MANTER", "JUNTA", "NOVA", "INCERTO",
           "FAIXA"]

MANTER, JUNTA, NOVA, INCERTO = "MANTER", "JUNTA", "NOVA", "INCERTO"


# ─────────────────────────────────────────────────────────────────
# NOME — só para COMPARAR lançamentos (a chave `cupb|<nome>` não muda)
#
# Palavras de tempo, chamada ou função que hoje viram "nome" e separam a
# mesma campanha. Cada uma tem o caso real que a justifica; nenhuma entra
# sem caso (decisão do dono, 14/10).
# ─────────────────────────────────────────────────────────────────
_NOME_FORA = frozenset({
    # "Próximo Cupom 20% OFF Mercado Livre" (Promotom 111295) → `proximo`,
    # e "Próximo Cupom 20% OFF em Lojas Oficiais" (Samuel 121007, editada)
    # → `proximo-lojas-oficiais`: a mesma campanha da Fada 17799.
    "proximo",
    # "Cupons Shopee VIP, resgate agora e use meia-noite!" (Promotom
    # 111082) → `vip-use`; "Cupons Mercado Livre e Shopee, resgate agora e
    # use meia-noite!" (Promotom 111088) → `shopee-use`.
    "use",
    # "Ainda ativo! Cupom Shopee VIP" (Fada 17712) → `ainda-vip`;
    # "Cupom 50% OFF Shopee VIP ainda Ativo!!" (Samuel 120532) → `vip-ainda`.
    "ainda",
    # "Resgatem todos! 5 Cupons 30% OFF para o 10.10" (Fada 17687)
    # → `resgatem-todos`.
    "resgatem", "todos",
    # "Shopee Figurinhas, ganhe cupons de até R$ 100 OFF" (Promotom 110945)
    # → `figurinhas-ganhe`.
    "ganhe",
    # "Cupons liberados para quem é VIP Shopee!" (Fada 17674) → `quem-vip`.
    "quem",
    # "Cupons Shopee Contas Selecionadas - 40% OFF Sem Mínimo!" (Promotom
    # 111161) → `contas-selecionadas-sem`.
    "sem",
})

# Nome de OUTRA plataforma nunca é nome (decisão do dono, 14/10). Caso:
# Promotom 111088, post de Mercado Livre que virou `shopee-use`.
_PALAVRAS_PLATAFORMA = {
    "shopee": frozenset({"shopee"}),
    "mercadolivre": frozenset({"mercado", "livre", "meli", "mercadolivre"}),
    "amazon": frozenset({"amazon"}),
    "magalu": frozenset({"magalu"}),
    "netshoes": frozenset({"netshoes"}),
}

# ─────────────────────────────────────────────────────────────────
# LEMBRETE — mensagem nova da MESMA fonte que NÃO é liberação nova.
# Medido nas 5 fontes, 07–14/10: com código, a mesma fonte relembrou o
# mesmo cupom dentro da hora; sem código, nenhum caso — mas o vocabulário é
# o mesmo, então a regra de mesma fonte não vale para lembrete (vai a
# INCERTO, que é o comportamento de hoje).
# ─────────────────────────────────────────────────────────────────
_RE_LEMBRETE = re.compile(
    r"\b(?:"
    # "Cupom Shopee Esgotando" (Fada 17749, BEN3FICIO13AF, 49,6 min depois
    # da 17745); "Cuponzão Shopee Esgotando!" (Fada 17759, 18,4 min depois
    # da 17757).
    r"esgotando"
    # "Cupom Shopee Voltando!" (Promotom 111176, 0F1C141S35, 57,6 min
    # depois da 111172); "Cupom Shopee Voltando" (Fada 17594, 17814).
    r"|voltando|voltou"
    # "Ainda ativo! Cupom Shopee VIP" (Fada 17712); "… VIP ainda Ativo!!"
    # (Samuel 120494, 120532, 120812).
    r"|ainda\s+ativ[oa]s?"
    # "Lembrando! Cupom Shopee 40/199" (Samuel 120344; Fada 17685).
    r"|lembrando"
    # "Última chance! Shopee Figurinhas" (Samuel 119837; Fada 17613).
    r"|ultima\s+chance"
    # "Cupom Shopee Renovado!" (Fada 17773; Samuel 120795).
    r"|renovad[oa]s?"
    r")\b")
_ESCOPO_LEMBRETE = 200

# ─────────────────────────────────────────────────────────────────
# RENOVADOS — o anúncio "cupons renovados" de fontes diferentes, sem
# benefício declarado dos dois lados, no mesmo instante, é UM lançamento
# (regra determinística estreita aprovada pelo dono em 15/10). Casos:
# 14/10 14:59 UTC Promotom 111375 "Cupons Shopee Renovados" × Fada 17831
# "Renovados! Dia de Cupons Shopee" (posts 29047 × 29048) e 14/10 23:00
# Fada 17838 × Promotom 111400 (29160 × 29161, duplicata apagada à mão).
# Medido em 07–15/10: só esses dois mudam; os demais pares já estavam
# juntos e a repetição da MESMA fonte (Fada 17778 → 17781, 6,7 min) não
# entra (mesma fonte decide antes).
# ─────────────────────────────────────────────────────────────────
_RE_RENOVADO = re.compile(r"\brenovad[oa]s?\b")


_RE_NAO_ASCII = re.compile(r"[^\x00-\x7f]")


def _sem_acento(s: str) -> str:
    """NFD sem as marcas "Mn" — o mesmo resultado de filtrar caractere a
    caractere, mas a varredura é do regex: ASCII nunca é "Mn", então só o
    não-ASCII (acento, emoji) chega a unicodedata.category (D.5)."""
    nfd = unicodedata.normalize("NFD", s)
    if nfd.isascii():
        return nfd
    return _RE_NAO_ASCII.sub(
        lambda m: "" if unicodedata.category(m.group()) == "Mn" else m.group(),
        nfd)


def _sem_palavras(w: str, fora: frozenset) -> str:
    """O pedaço `w` (entre espaços) sem as palavras de `fora`. A palavra é
    a sequência de letras [a-z] depois de tirar acento e caixa — a mesma
    que nome_declarado lê —, então emoji ou pontuação colados não a
    escondem: "🔥Ainda" (Fada 17712) perde o "ainda"."""
    base = _sem_acento(w.lower())
    sobra = re.sub(r"[a-z]+", lambda m: "" if m.group() in fora
                   else m.group(), base)
    return w if sobra == base else sobra


def _titulo_limpo(texto: str, plat: str) -> str:
    """O texto com as palavras de _NOME_FORA e de outras plataformas
    retiradas da 1ª linha não vazia — a linha que nome_declarado lê. Tirar
    antes (e não da saída) evita que a sobra passe de 3 palavras e o nome
    suma. Nenhum espaço duplo novo nasce (ele encerra o título em
    _linha_titulo): o pedaço que fica vazio sai inteiro."""
    fora = _NOME_FORA | frozenset().union(
        *(v for k, v in _PALAVRAS_PLATAFORMA.items() if k != plat))
    linhas = (texto or "").split("\n")
    for i, linha in enumerate(linhas):
        if linha.strip():
            pedacos = []
            for w in linha.split(" "):
                limpo = _sem_palavras(w, fora)
                if limpo or not w:
                    pedacos.append(limpo)
            linhas[i] = " ".join(pedacos)
            break
    return "\n".join(linhas)


def _radical(nome: str) -> str:
    """Plural e gênero não separam campanha: "Contas Selecionadas"
    (Promotom 111161) × "Contas Selecionados" (Samuel 120479), o mesmo
    cupom de 40%. "Black Friday" × "Black Week" continuam diferentes."""
    out = []
    for w in nome.split("-"):
        if not w:
            continue
        w = re.sub(r"s$", "", w)
        w = re.sub(r"[ao]$", "", w)
        out.append(w)
    return "-".join(out)


# ─────────────────────────────────────────────────────────────────
# PERFIL — o que a regra compara de cada mensagem ou post
# ─────────────────────────────────────────────────────────────────
_FALTA = object()


def _nome_de(texto: str, plat: str) -> str:
    return _radical(nome_declarado(_titulo_limpo(texto, plat), plat))


def _pct_de(texto: str, plat: str) -> str:
    for parte in beneficio_do_cupom(texto).split("+"):
        if parte.startswith("pct:"):
            return parte.split(":", 1)[1]
    return ""


def _valores_de(texto: str, plat: str) -> frozenset:
    return frozenset(a for a in assinatura_do_beneficio(texto)
                     if a.startswith("v:"))


def _lembrete_de(texto: str, plat: str) -> bool:
    return bool(_RE_LEMBRETE.search(
        _sem_acento(texto[:_ESCOPO_LEMBRETE].lower())))


def _renovado_de(texto: str, plat: str) -> bool:
    return bool(_RE_RENOVADO.search(
        _sem_acento(texto[:_ESCOPO_LEMBRETE].lower())))


class Perfil:
    """O que a regra compara de uma mensagem ou post:
      nome      nome declarado, sem tempo/chamada, radical ("" = sem nome)
      pct       percentual do benefício ("" = sem %)
      valores   assinaturas de valor ("v:30-299")
      lembrete  vocabulário de lembrete no começo do texto
      renovado  marcador "renovad…" no começo do texto (regra RENOVADOS)

    Vem dos valores (Perfil(nome, pct, valores, lembrete)) ou do texto
    (perfil()). Do texto, cada campo é calculado só quando a decisão o lê
    e fica guardado (D.5): mesma fonte só lê `lembrete`; com os dois
    percentuais, o valor nem é calculado. O resultado é o mesmo."""
    __slots__ = ("_texto", "_plat", "_campos")
    _CALCULO = {"nome": _nome_de, "pct": _pct_de, "valores": _valores_de,
                "lembrete": _lembrete_de, "renovado": _renovado_de}

    def __init__(self, nome=_FALTA, pct=_FALTA, valores=_FALTA,
                 lembrete=_FALTA, renovado=_FALTA, *, texto: str = "",
                 plat: str = ""):
        self._texto, self._plat = texto, plat
        self._campos = {"nome": nome, "pct": pct, "valores": valores,
                        "lembrete": lembrete, "renovado": renovado}

    def _campo(self, nome):
        v = self._campos[nome]
        if v is _FALTA:
            v = self._campos[nome] = self._CALCULO[nome](self._texto,
                                                         self._plat)
        return v

    nome = property(lambda self: self._campo("nome"))
    pct = property(lambda self: self._campo("pct"))
    valores = property(lambda self: self._campo("valores"))
    lembrete = property(lambda self: self._campo("lembrete"))
    renovado = property(lambda self: self._campo("renovado"))

    @property
    def tem_beneficio(self) -> bool:
        return bool(self.pct or self.valores)

    def __eq__(self, outro):
        if not isinstance(outro, Perfil):
            return NotImplemented
        return all(self._campo(k) == outro._campo(k) for k in self._CALCULO)

    __hash__ = None

    def __repr__(self):
        return (f"Perfil(nome={self.nome!r}, pct={self.pct!r}, "
                f"valores={set(self.valores) or '{}'}, lembrete={self.lembrete})")


@lru_cache(maxsize=1024)
def perfil(texto: str, plat: str) -> Perfil:
    """Perfil de lançamento de um texto (candidato ou post publicado).

    Memoizado: é função pura de (texto, plataforma) e o texto de um post
    vivo não muda entre uma mensagem e outra — sem o cache, cada mensagem
    `cupb` refazia o perfil de todos os posts vivos (D.5: p99 de 1,3 ms
    com 4 posts). Os campos calculados ficam no objeto guardado; 1024
    textos limitam a memória."""
    return Perfil(texto=texto or "", plat=plat)


def candidato(ofertas: Iterable[str]) -> str:
    """Plataforma do candidato quando TODAS as âncoras são cupom sem
    código (`<plat>|cupb|…`) da mesma plataforma; senão "" (fora da regra:
    código, produto, destino, cashback, url)."""
    plat = ""
    for k in ofertas or ():
        partes = (k or "").split("|", 2)
        if len(partes) != 3 or partes[1] != "cupb" or not partes[2]:
            return ""
        if plat and partes[0] != plat:
            return ""
        plat = partes[0]
    return plat


# ─────────────────────────────────────────────────────────────────
# DECISÃO
# ─────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class PostVivo:
    msg_id: int
    perfil: Perfil
    nascimento: float         # estampa do ciclo (janela_fim − vida)
    fontes: frozenset         # chats das origens ligadas ao post


@dataclass(frozen=True)
class Decisao:
    acao: str                 # MANTER | JUNTA | NOVA | INCERTO
    post: Optional[int]       # post envolvido (vivo de hoje ou o de junção)
    motivo: str
    delta_s: Optional[float]  # do nascimento do post envolvido até agora
    mesma_fonte: bool = False


# JUNTA da faixa (RAJADA < Δ ≤ SUPERIOR, fontes diferentes, benefício
# igual). MONOTÔNICA em relação à regra sem ela: só transforma INCERTO em
# JUNTA. Com família o resultado é o de antes (INCERTO faixa_cinzenta); sem
# família ela só conta quando não há JUNTA da rajada.
FAIXA = "mesmo_beneficio_na_faixa"

# INCERTO que merece registro (calibração da sombra). Os demais INCERTO
# coincidem com o que hoje já acontece e não viram linha.
_INCERTO_COM_FAMILIA = frozenset({
    "faixa_cinzenta", "lembrete_mesma_fonte", "mesma_fonte_na_rajada",
    "acima_do_superior"})
_INCERTO_SEM_FAMILIA = frozenset({
    "faixa_cinzenta", "sem_beneficio", "valor_nome_x_sem_nome",
    "beneficio_incomparavel"})


def _valor_compativel(x: str, y: str) -> bool:
    """Duas assinaturas de valor (`v:<desconto>-<mínimo>` ou `v:<desconto>`)
    são o mesmo benefício quando o desconto é o mesmo e o mínimo é o mesmo
    ou só um lado o declara. Caso real (14/10 22:00 UTC, marcado errado
    pelo dono): Fada 17836 "Cupom R$60 OFF" (v:60) × Promotom 111397
    "R$ 60 OFF em R$ 499" (v:60-499), o mesmo relâmpago de meia-noite — a
    Fada só não escreveu o mínimo. Mínimos declarados diferentes seguem
    diferentes (regra do dono, 15/10)."""
    dx, _, mx = x[2:].partition("-")
    dy, _, my = y[2:].partition("-")
    return dx == dy and (mx == my or not mx or not my)


def _beneficio(a: Perfil, b: Perfil) -> str:
    """igual | diferente | sem_beneficio | valor_nome_x_sem_nome |
    incomparavel."""
    if not a.tem_beneficio or not b.tem_beneficio:
        return "sem_beneficio"
    if a.pct and b.pct:
        return "igual" if a.pct == b.pct else "diferente"
    if a.valores and b.valores:
        if not any(_valor_compativel(x, y) for x in a.valores for y in b.valores):
            return "diferente"
        if bool(a.nome) != bool(b.nome):
            # Nome × sem nome com o mesmo VALOR: tests/test_cupom_sem_codigo
            # 08 fixa dois posts (Tech v:30-299 × genérico v:30-299). A
            # variante aprovada (B) só junta nome × sem nome por percentual.
            return "valor_nome_x_sem_nome"
        return "igual"
    return "incomparavel"


def _relacao(c: Perfil, chat: str, p: PostVivo, agora: float,
             rajada_s: float, superior_s: float):
    """(ação, motivo, Δ) entre o candidato e UM post vivo."""
    delta = agora - p.nascimento
    if chat in p.fontes:
        if c.lembrete:
            return INCERTO, "lembrete_mesma_fonte", delta
        if delta < rajada_s:
            return INCERTO, "mesma_fonte_na_rajada", delta
        return NOVA, "mesma_fonte", delta
    if c.nome and p.perfil.nome and c.nome != p.perfil.nome:
        return NOVA, "nome_conflitante", delta
    b = _beneficio(c, p.perfil)
    if b == "diferente":
        return NOVA, "beneficio_diferente", delta
    if delta > superior_s:
        # P1 do dono (16/10): outro grupo publicando a mesma oferta que ainda
        # vale é duplicata, não importa o tempo. Entre fontes diferentes o
        # tempo sozinho nunca abre post novo — só benefício ou nome diferente
        # (acima). Caso real, 18/10 22:37 UTC: Samuel 121892 "Próximo Cupom
        # 20% OFF" × post 29716 (Promotom, 20%), Δ 36,6 min — NOVA aqui era
        # um post a mais com o mesmo cupom. Fica o comportamento de hoje.
        return INCERTO, "acima_do_superior", delta
    if delta > rajada_s:
        if b == "igual":
            # P1 do dono (20/10): outro grupo, MESMO benefício, entre a
            # RAJADA e o SUPERIOR é o mesmo cupom. Caso real, 19/10 22:14
            # UTC: Samuel 122157 × post 30102 (Fada), Δ 13,1 min, virou o
            # 3º post (30104). Só vale sem JUNTA da rajada (classificar).
            return JUNTA, FAIXA, delta
        return INCERTO, "faixa_cinzenta", delta
    if b == "igual":
        return JUNTA, "mesmo_lancamento", delta
    if (b == "sem_beneficio" and not c.tem_beneficio
            and not p.perfil.tem_beneficio and c.renovado and p.perfil.renovado):
        # fontes diferentes (mesma fonte decidiu acima), sem nome em
        # conflito (idem), Δ ≤ RAJADA (idem) — ver RENOVADOS
        return JUNTA, "renovados", delta
    return INCERTO, ("sem_beneficio" if b == "sem_beneficio" else
                     "valor_nome_x_sem_nome" if b == "valor_nome_x_sem_nome"
                     else "beneficio_incomparavel"), delta


def pode_mudar(chat: str, agora: float, nascimento: float, fontes: frozenset,
               superior_s: float) -> bool:
    """SEM família, só um post de OUTRA fonte nascido há ≤ SUPERIOR pode
    virar JUNTA ou INCERTO registrável. Para os demais, _relacao dá NOVA
    (benefício ou nome diferente), INCERTO não registrável sem família
    (acima do SUPERIOR) ou o desfecho de mesma fonte (NOVA ou INCERTO de
    mesma fonte) — e nada disso muda a decisão sem família. Quem lê o
    banco usa isto para nem calcular o perfil desses posts (D.5); o
    resultado de classificar é o mesmo com ou sem eles."""
    return chat not in fontes and agora - nascimento <= superior_s


def classificar(c: Perfil, chat: str, agora: float, vivos: list,
                familia_hoje: Optional[int], rajada_s: float,
                superior_s: float) -> Decisao:
    """Decisão da regra nova para uma MENSAGEM NOVA (nunca edição).

    `familia_hoje` é o post que a família de hoje escolheu (None = post
    novo). MANTER e INCERTO = comportamento de hoje; JUNTA e NOVA = desvio
    (registrado na sombra; aplicado só em modo ATIVO)."""
    por_id = {p.msg_id: p for p in vivos}
    if familia_hoje is not None:
        p = por_id.get(familia_hoje)
        if p is None:
            return Decisao(MANTER, familia_hoje, "familia_fora_da_regra", None)
        acao, motivo, delta = _relacao(c, chat, p, agora, rajada_s, superior_s)
        mesma = chat in p.fontes
        if acao == JUNTA and motivo == FAIXA:
            # Com família o post já é o dela: a faixa fica como era.
            acao, motivo = INCERTO, "faixa_cinzenta"
        if acao == NOVA:
            return Decisao(NOVA, familia_hoje, motivo, delta, mesma)
        if acao == INCERTO and motivo in _INCERTO_COM_FAMILIA:
            return Decisao(INCERTO, familia_hoje, motivo, delta, mesma)
        return Decisao(MANTER, familia_hoje, motivo, delta, mesma)
    juntas, faixa, incertos = [], [], []
    for p in vivos:
        acao, motivo, delta = _relacao(c, chat, p, agora, rajada_s, superior_s)
        if acao == JUNTA and motivo == FAIXA:
            faixa.append((delta, p.msg_id, motivo))
        elif acao == JUNTA:
            juntas.append((delta, p.msg_id, motivo))
        elif acao == INCERTO and motivo in _INCERTO_SEM_FAMILIA:
            # Δ ≤ SUPERIOR: acima dele o INCERTO (acima_do_superior) não
            # está em _INCERTO_SEM_FAMILIA e não muda a decisão sem família.
            incertos.append((delta, p.msg_id, motivo))
    if len(juntas) == 1:
        delta, mid, motivo = juntas[0]
        return Decisao(JUNTA, mid, motivo, delta)
    if len(juntas) > 1:
        delta, mid, _motivo = min(juntas)
        return Decisao(INCERTO, mid, "ambiguo", delta)
    # Só sem JUNTA da rajada (acima): a da faixa nunca desfaz nem troca uma
    # JUNTA da rajada — ela só transforma o INCERTO de antes em JUNTA.
    if len(faixa) == 1:
        delta, mid, motivo = faixa[0]
        return Decisao(JUNTA, mid, motivo, delta)
    if len(faixa) > 1:
        delta, mid, _motivo = min(faixa)
        return Decisao(INCERTO, mid, "ambiguo", delta)
    if incertos:
        delta, mid, motivo = min(incertos)
        return Decisao(INCERTO, mid, motivo, delta)
    return Decisao(MANTER, None, "sem_lancamento_vivo", None)
