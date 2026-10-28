"""
CAMPANHA POR PÁGINA DE RESGATE — regra pura, só em SOMBRA nesta frente.

Responsabilidade ÚNICA: dado um candidato SEM âncora forte e os posts vivos
da mesma plataforma, também sem âncora forte, responder se ele é a MESMA
campanha de um deles porque os dois levam à MESMA página reconhecida de
resgate de cupons — e para qual canal o post iria. Não lê banco, não loga,
não publica: quem lê o estado e registra é pipeline.campanha_resgate_sombra.

Por que existe — o caso real (28/10/2026 00:00 BRT, logs da Railway + prévia
pública dos canais):
  · Promotom 112024 "Cupons Shopee / -Resgate aqui: 1 … 2 …" → post 31943,
    identidade `shopee|cupb|geral` (o título tem "Cupons": natureza cupom),
    nos dois canais;
  · Fada 18042 "🚨 Começou! Esquenta 11.11 Shopee / ✅ Resgate todos os
    cupons aqui: …" 44 s depois → post NOVO 31945, identidade
    `shopee|url|https://shopee.com.br/m/cupom-de-desconto` (o título não
    tem "cupom": natureza evento), só no principal;
  · os dois publicaram o MESMO link (s.shopee.com.br/AAHdNLxGSX): a Shopee
    dá um link por destino (cache por destino em plataformas/shopee), e o
    link 1 do Promotom já tinha virado AAHdNLxGSX no post 30602 (21/10).
Nenhuma chave em comum → nenhuma família → duas publicações. O dono apagou
o 31943 (com a cópia) e encaminhou o 31945 à mão para o canal de cupons.

A RELAÇÃO (conservadora — nunca "mesmo link = mesma campanha"):
  1. só entre mensagens/posts SEM âncora forte (produto, código, destino têm
     autoridade própria) da MESMA plataforma — de 21 a 28/10, 186 posts de
     PRODUTO do fumotom abriam com "-Resgate todos os cupons desta página:"
     e esta mesma página como 1º link (ex.: 42674 → post 31947, 1 min
     depois do caso real); é este filtro que os deixa fora;
  2. a página de resgate é a canônica que a normalização já deriva
     (norm.ancora_url) e precisa estar DECLARADA pela plataforma como
     página de resgate de cupons (Plataforma.paginas_resgate) — página
     genérica (carteira, carrinho, campanha sazonal, live) não serve;
  3. a página é o destino PRINCIPAL dos dois lados: o 1º link do candidato
     (o da âncora) e o 1º link publicado do post vivo são o MESMO link
     publicado (só vale para plataforma que dá um link publicado por
     destino — contrato da capacidade). Página só como link secundário não
     conta: post de produto com "resgate todos os cupons" no rodapé, ou um
     cupom de campanha própria que cita a página geral (26/10 03:02: Fada
     17998 "Novos Cupons Shopee Tech, R$100 OFF em R$999" tinha a página
     como 2º link, ao lado do 31451 do Promotom — campanhas distintas);
  4. fonte diferente e dentro do LIMITE SUPERIOR do lançamento
     (cupom_lancamento.pode_mudar — a mesma régua da regra de cupom);
  5. nome e benefício compatíveis (cupom_lancamento.compativeis): nome ou
     benefício em conflito NUNCA junta — fica INCERTO (comportamento de hoje);
  6. exatamente UM post assim: JUNTA; mais de um: INCERTO (ambíguo).
Com família já escolhida pelo pipeline, a regra não troca de post: se a
família já exibe a página, é o mesmo post; se não, INCERTO.

CANAL DE CUPONS (separado da natureza): o post que leva a uma página
reconhecida de resgate de cupons é elegível ao canal de cupons mesmo quando
o título não diz "cupom" (natureza evento). Junção: vale o canal do post.

NÃO faz: banco, Telegram, log, relógio, mudança de chave ou de natureza.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from pipeline import cupom_lancamento
from pipeline.resolucao_identidade import eh_chave_forte

__all__ = ["Candidato", "PostVivo", "Decisao", "avaliar", "sem_ancora_forte",
           "chave_resgate", "MANTER", "JUNTA", "INCERTO", "PRINCIPAL",
           "PRINCIPAL_E_CUPONS"]

MANTER, JUNTA, INCERTO = "MANTER", "JUNTA", "INCERTO"
PRINCIPAL, PRINCIPAL_E_CUPONS = "principal", "principal+cupons"


def sem_ancora_forte(chaves: Iterable[str]) -> bool:
    """Nenhuma âncora forte (produto, cupom com código, destino) e pelo
    menos uma chave."""
    chaves = list(chaves or ())
    return bool(chaves) and not any(eh_chave_forte(k) for k in chaves)


def chave_resgate(plat: str, pagina: str) -> str:
    """Chave de encontro PROPOSTA (fraca) — só aparece no registro da sombra;
    nenhuma família a conhece nesta frente."""
    return f"{plat}|resgate|{pagina}"


@dataclass(frozen=True)
class Candidato:
    plat: str
    chat: str
    chaves: tuple             # âncoras atuais (enr.ofertas)
    pagina: str               # página de resgate reconhecida ("" = nenhuma)
    publicada: str            # link publicado dessa página ("" = sem)
    perfil: cupom_lancamento.Perfil
    familia_hoje: Optional[int]
    espelho_hoje: bool        # o post de hoje iria ao canal de cupons?


@dataclass(frozen=True)
class PostVivo:
    msg_id: int
    plat: str
    chaves: frozenset         # o que o post possui ou exibe
    primeiro_link: str        # 1º link do texto PUBLICADO
    perfil: cupom_lancamento.Perfil
    nascimento: float
    fontes: frozenset
    espelhado: bool           # tem cópia no canal de cupons


@dataclass(frozen=True)
class Decisao:
    acao: str                 # MANTER | JUNTA | INCERTO
    post: Optional[int]
    motivo: str
    delta_s: Optional[float]
    destino_hoje: str
    destino_previsto: str
    outros: tuple = ()        # posts que também têm a página como principal


def _destino(espelho: bool) -> str:
    return PRINCIPAL_E_CUPONS if espelho else PRINCIPAL


def avaliar(c: Candidato, vivos: list, agora: float, superior_s: float) -> Decisao:
    """A decisão da relação para o candidato `c` diante dos posts `vivos`
    (os dados chegam prontos; nada aqui consulta estado)."""
    hoje = _destino(c.espelho_hoje)
    if not sem_ancora_forte(c.chaves):
        return Decisao(MANTER, None, "fora:ancora_forte", None, hoje, hoje)
    if not c.pagina or not c.publicada:
        return Decisao(MANTER, None, "fora:sem_pagina_de_resgate", None, hoje, hoje)
    previsto_novo = PRINCIPAL_E_CUPONS          # página de resgate de cupons
    exibem = [p for p in vivos
              if p.plat == c.plat and sem_ancora_forte(p.chaves)
              and p.primeiro_link == c.publicada]
    if c.familia_hoje is not None:
        fam = next((p for p in exibem if p.msg_id == c.familia_hoje), None)
        outros = tuple(p.msg_id for p in exibem if p.msg_id != c.familia_hoje)
        if fam is not None:
            return Decisao(MANTER, fam.msg_id, "ja_no_mesmo_post",
                           agora - fam.nascimento, hoje, hoje, outros)
        if outros:
            return Decisao(INCERTO, outros[0], "familia_de_hoje_diferente", None,
                           hoje, hoje, outros)
        return Decisao(MANTER, None, "sem_par", None, hoje, hoje)
    if not exibem:
        return Decisao(MANTER, None, "sem_par", None, hoje, previsto_novo)
    validos, motivos = [], []
    for p in exibem:
        delta = agora - p.nascimento
        if not cupom_lancamento.pode_mudar(c.chat, agora, p.nascimento, p.fontes,
                                           superior_s):
            motivos.append(("mesma_fonte" if c.chat in p.fontes
                            else "acima_do_superior", p, delta))
            continue
        rel = cupom_lancamento.compativeis(c.perfil, p.perfil)
        if rel == "conflito":
            motivos.append(("nome_ou_beneficio_em_conflito", p, delta))
            continue
        validos.append((rel, p, delta))
    todos = tuple(p.msg_id for p in exibem)
    if len(validos) == 1:
        rel, p, delta = validos[0]
        return Decisao(JUNTA, p.msg_id, f"mesma_pagina_de_resgate:{rel}", delta,
                       hoje, _destino(p.espelhado), todos)
    if len(validos) > 1:
        return Decisao(INCERTO, validos[0][1].msg_id, "ambiguo", None,
                       hoje, previsto_novo, todos)
    motivo, p, delta = motivos[0]
    return Decisao(INCERTO, p.msg_id, motivo, delta, hoje, previsto_novo, todos)
