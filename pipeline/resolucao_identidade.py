"""
Pipeline — Resolução de FORÇA das evidências de identidade.

Responsabilidade única: dado o conjunto de evidências que o pipeline
JÁ derivou, decidir QUAIS delas identificam a oferta e em que ordem de
precedência. É a política — o que muda quando surge uma mecânica nova.

Consumido por: pipeline.identidade_oferta.

NÃO faz:
  - derivar evidência (responsabilidade da normalização, do assunto e
    dos adaptadores de plataforma)
  - formatar ou emitir a chave final (responsabilidade de
    identidade_oferta, que rotula a espécie)
  - deduplicação, família, score, mídia ou publicação
  - I/O, estado ou log

─────────────────────────────────────────────────────────────────────
PUREZA DO RESOLVEDOR

Este módulo NÃO conhece marketplace, mecânica nem vocabulário:
não sabe o que é "Shopee", "cashback", "moedas", "live", "aggregation"
ou "session". Ele recebe `Evidencias` — proposições já decididas por
quem tem autoridade para decidi-las — e aplica precedência sobre elas.

É essa ignorância que torna o módulo extensível: uma mecânica nova
entra declarando suas evidências na camada que a conhece (adaptador
ou derivador), sem acrescentar uma linha aqui.

─────────────────────────────────────────────────────────────────────
FASE 1 — EXTRAÇÃO, NÃO EVOLUÇÃO

A política aqui é BYTE-EQUIVALENTE à que vivia em
identidade_oferta.ancoras(). Nada foi acrescentado, removido ou
"melhorado". Os comentários doutrinários foram trazidos junto porque
explicam POR QUE cada precedência existe; alterá-los sem medição seria
perder a justificativa.

Qualquer mudança de política é frente própria, com corpus, adversarial
e regressão — nunca dentro de uma extração.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple


# ─────────────────────────────────────────────────────────────────
# CONTRATO DE ENTRADA — evidências já derivadas
# ─────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Evidencias:
    """Fotografia das evidências de identidade de UMA mensagem.

    Todos os campos chegam PRONTOS. O resolvedor não recalcula, não
    consulta texto e não conhece a origem de nenhum deles — apenas
    pesa. Quem derivou cada evidência é responsável por ela.
    """
    plataforma:      str
    # natureza: a oferta É a campanha de cupom? (derivado por
    # pipeline.natureza — gate R1×R2 do MB)
    entidade_cupom:  bool
    # códigos de cupom declarados pela fonte (utils.cupom)
    codigos:         Tuple[str, ...] = ()
    # tema textual da campanha, já resolvido (assunto_oferta)
    tema_campanha:   str = ""
    # produtos identificados pelos adaptadores: (plataforma, id, tipo)
    produtos:        Tuple[Tuple[str, str, str], ...] = ()
    # o post tem produto identificado? (espelha ids_globais)
    tem_produto:     bool = False
    # chaves de campanha derivadas dos hosts declarados pelos
    # adaptadores (registry.compor_capacidade)
    chaves_campanha: Tuple[str, ...] = ()
    # natureza cashback já decidida (assunto_especie + sinal do
    # adaptador). O resolvedor não sabe o que a palavra significa.
    natureza_cash:   bool = False
    # percentual do benefício, já extraído por assunto_oferta
    # (beneficio_do_cupom). Chega como string OPACA — o resolvedor não
    # sabe de onde veio nem o que "100" significa; só compara.
    percentual:      str = ""
    # link canônico da oferta, já normalizado pelo adaptador
    link_canonico:   str = ""
    # fingerprint terminal do texto, já calculado
    fingerprint:     str = ""


@dataclass(frozen=True)
class Entidade:
    """Uma entidade de identidade resolvida.

    `papel` nomeia a natureza da evidência que identificou; `chave` é
    a string byte-idêntica à historicamente emitida. identidade_oferta
    traduz `papel` na espécie da Ancora — a tradução vive lá porque a
    espécie é vocabulário do consumidor, não do resolvedor.
    """
    papel: str
    chave: str


# ─────────────────────────────────────────────────────────────────
# RESOLUÇÃO
# ─────────────────────────────────────────────────────────────────
def resolver(ev: Evidencias) -> List[Entidade]:
    """Aplica a precedência de evidências e devolve as entidades.

    Nunca devolve lista vazia: a cauda garante uma entidade terminal.
    """
    saida: List[Entidade] = []
    vistos = set()

    def _add(papel: str, chave: str) -> None:
        if chave not in vistos:
            vistos.add(chave)
            saida.append(Entidade(papel, chave))

    plat = ev.plataforma

    # ══ C3 — AUTORIDADE DO ASSUNTO ══
    # Quando o CUPOM é o assunto do post (classificador endurecido no
    # C2), a oferta É o cupom — o produto no link é apenas
    # VEÍCULO/ilustração e NÃO ancora. Sem isso, o mesmo cupom
    # relâmpago ilustrado com produtos diferentes por grupos
    # diferentes viraria N ofertas distintas.
    #
    # A âncora é EXCLUSIVA (só o cupom), nunca aditiva: se o produto
    # ilustrativo também ancorasse, um post-vitrine capturaria a
    # oferta legítima daquele produto quando ela chegasse.
    #
    # Segurança: o C2 garante que "Echo Dot R$249 (cupom ECHO10)" NÃO
    # é post de cupom (cupom como complemento) — logo produtos
    # distintos que compartilham um código genérico seguem em famílias
    # separadas. Gate R1×R2 (MB ratificado): com produto identificado,
    # o cupom só ancora exclusivo se o post caracterizar CLARAMENTE
    # benefício de loja; na dúvida, prevalece o produto (cupom vira
    # atributo). R2+ (emenda ratificada): 2+ códigos e nenhum preço de
    # item → a lista de códigos É a oferta; o produto é vitrine.
    if ev.entidade_cupom:
        if ev.codigos:
            # COM código: o código É a identidade (elemento mais
            # estável disponível — INV-E3; troca de código na vida da
            # campanha é responsabilidade do Motor de Estado, Fase 3).
            for cod in ev.codigos:
                _add("cupom", f"{plat}|cup|{cod.upper()}")
        else:
            # [F-C4] SEM código: a identidade é o TEMA da campanha
            # (INV-E2/E3: benefício/percentual/limite são ESTADO e
            # nunca entram na chave). Sem tema → "geral" (bucket do
            # ciclo, R5). cupb| NÃO é código: não entra no cupom_idx
            # (que só indexa códigos reais) — garantido por
            # construção, pois o efeito de cupom só roda sobre os
            # códigos, aqui vazios.
            _add("cupom-beneficio", f"{plat}|cupb|{ev.tema_campanha}")
        return saida

        for plat_link, pid, _tipo in ev.produtos:
        _add("produto", f"{plat_link}|{pid}")

    for k in ev.chaves_campanha:
        _add("campanha", f"{plat}|camp|{k}")

    if not ev.tem_produto:
        for cod in ev.codigos:
            _add("cupom", f"{plat}|cup|{cod.upper()}")

        if ev.natureza_cash and ev.percentual and not ev.tem_produto:
            # [INV-E2 REFINADO] Cashback SEM código e SEM produto: não
            # existe identificador mais forte, e o TEMA é instável.
            #
            # Medido em 18 mensagens reais: a mesma campanha recebe
            # temas diferentes conforme o grupo escreva ou não a
            # palavra-enfeite ("Moedas" aparece em 61% das mensagens;
            # a natureza cashback, em 100%). O tema errava nos DOIS
            # sentidos ao mesmo tempo — separava 100% de 100% e
            # juntava 60% com 100%.
            #
            # A chave NÃO é o percentual: é plataforma + natureza +
            # percentual. O percentual sozinho nunca identifica, e sem
            # natureza provada esta via nem é alcançada. A natureza
            # também não vira chave literal — "cash" puro colapsaria
            # campanhas distintas (medido: 5 falsos merges).
            #
            # Isto REFINA o INV-E2, não o revoga: percentual continua
            # sendo ESTADO onde existe identificador estável (código,
            # produto) ou onde o benefício é acessório da oferta. Aqui
            # ele é o único discriminante da mecânica.
            #
            # Deliberadamente NÃO participa: URL/contexto do link. A
            # expansão depende de rede; um timeout devolve o link
            # encurtado, único por mensagem, e a identidade viraria
            # refém do relógio. Contexto fica registrado para frente
            # própria, com dado de produção dos dois lados.
            _add("cashback", f"{plat}|cash|{ev.percentual}")

    if saida:
        return saida

    # ── Cauda: sem oferta estruturada ────────────────────────────
    # MB §11.9 (ratificado): evento é FALLBACK, não espécie própria —
    # âncora por palavra-chave colidia entre eventos distintos da
    # mesma mecânica. Ordem: link primeiro, texto como terminal.
    # Nunca vazio. A chave já chega derivada da URL afiliada LONGA
    # canônica; esta camada apenas a pesa.
    if ev.link_canonico:
        return [Entidade("fallback", f"{plat}|url|{ev.link_canonico}")]
    return [Entidade("fallback", f"{plat}|txt|{ev.fingerprint}")]
  
