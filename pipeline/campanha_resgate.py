"""
CAMPANHA POR PÁGINA DE RESGATE — regra pura (SOMBRA por padrão; ATIVO por
config.CAMPANHA_RESGATE_MODO — ver COM CÓDIGO DE CUPOM, abaixo).

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
     publicado (ver O LINK PUBLICADO NA REGRA). Página só como link
     secundário não conta: post de produto com "resgate todos os cupons" no
     rodapé, ou um cupom de campanha própria que cita a página geral (26/10
     03:02: Fada 17998 "Novos Cupons Shopee Tech, R$100 OFF em R$999" tinha
     a página como 2º link, ao lado do 31451 do Promotom — campanhas
     distintas);
  4. fonte diferente e dentro do LIMITE SUPERIOR do lançamento
     (cupom_lancamento.pode_mudar — a mesma régua da regra de cupom);
  5. nome e benefício compatíveis (cupom_lancamento.compativeis): nome ou
     benefício em conflito NUNCA junta — fica INCERTO (comportamento de hoje);
  6. exatamente UM post assim: JUNTA; mais de um: INCERTO (ambíguo).
Com família já escolhida pelo pipeline, a regra não troca de post: se a
família já tem a página como principal, é o mesmo post; se não, INCERTO.

O LINK PUBLICADO NA REGRA (tensão DECLARADA com o contrato da normalização:
"a URL de publicação JAMAIS participa" da identidade —
normalizacao_identidade.derivar_ancora_url). A regra não cria nem muda
identidade; o link publicado entra SÓ como teste de igualdade, porque o
post `cupb` não guarda a canônica do seu 1º link (só o texto publicado):
  · o candidato é reconhecido pela CANÔNICA (ancora_url) — nunca pelo link;
  · o link publicado do candidato é o que a afiliação deu para essa mesma
    canônica (mapa e canônicas vêm da mesma Afiliacao);
  · igualdade de link publicado ⇒ mesmo destino: um link curto leva a um
    destino só. É a parte que dá CERTEZA (nenhuma junção falsa por ela);
  · mesmo destino ⇒ mesmo link publicado só vale enquanto a plataforma der
    um link por destino (contrato da capacidade; na Shopee, o cache por
    destino em afiliacao.afilia). Se isso quebrar, a regra só deixa de
    juntar — o lado conservador.
Cada decisão registra em `prova` em que se apoiou: "canonica_persistida"
(o post guarda `<plat>|url|<página>`) ou "so_link_publicado". A alternativa
sem esta tensão — persistir a canônica do 1º link do post — está em
docs/arquitetura/campanha-resgate.md.

PENDENTE DE APROVAÇÃO: JUNTA com nome/benefício só "compatível" (benefício
ausente ou incomparável de um lado — onde a regra de cupom de hoje fica em
INCERTO) sai com `pendente=True`. Só "igual" (o mesmo benefício explícito)
não é pendente. Ativar as pendentes exige aprovação separada do dono: é a
flag config.CAMPANHA_RESGATE_MODO = "ATIVO" (os dois casos reais de 28/10
são "compatível"); o campo continua na linha como registro.

CANAL DE CUPONS — QUATRO COISAS SEPARADAS:
  · destino_elegivel: a POLÍTICA proposta — o post cujo link principal é a
    página reconhecida de resgate de cupons é elegível ao canal de cupons
    mesmo quando o título não diz "cupom" (natureza evento). Elegível ao
    CANAL não quer dizer cupom elegível à conta de afiliado do dono
    (CLAUDE.md §9.1);
  · vinculo: o que o BANCO guarda (espelho_post) para o post que a campanha
    usaria — "nova" (post novo: ainda não há cópia), "presente" (há vínculo
    para uma mensagem), "ausente" (o worker marcou AUSENTE depois de achar a
    cópia apagada: espelho_cupons nunca a recria) ou "sem_vinculo";
  · presenca: o que se SABE da cópia no Telegram — "nao_verificada" (há
    vínculo, mas a sombra não consulta o Telegram, e o worker só descobre
    uma cópia apagada à mão quando tenta editá-la), "ausente_confirmada"
    (o vínculo AUSENTE só é gravado com a leitura por id confirmando),
    "a_criar" (post novo) ou "-" (sem vínculo, nada rastreado);
  · destino_afirmavel: o destino que a evidência SUSTENTA — elegível + nova
    → principal+cupons (é a publicação que envia aos dois); elegível +
    presente → principal, com a condicional explícita
    se_copia_no_ar=principal+cupons (nunca como certeza); elegível + ausente
    → principal (exclusão manual preservada); elegível + sem_vinculo →
    principal (não se presume criação); não elegível → principal.
O vínculo é lido pelo adaptador, com leitura estrita, só para o post que
importa (família de hoje ou alvo da junção); a regra só combina (finalizar).

INDETERMINADO não sai daqui: é do adaptador, quando uma LEITURA do estado
falha (a regra nunca decide sobre estado incompleto).

COM CÓDIGO DE CUPOM (`com_codigo=True`; o adaptador só passa em ATIVO).
Caso real (28/10/2026 11:59 UTC, logs + prévia): Fada 18049 "Cupons
Renovados! Esquenta Shopee / Resgate todos os cupons aqui: <página>" → post
32138, que na edição seguinte ganhou o código 3SQU3NT411 (R$150 OFF em
R$999); Promotom 112034 "Cupons Shopee / Resgate aqui: <página>" 18 s depois
→ post NOVO 32139 (CUPOM_INCERTO sem_beneficio). O dono apagou o 32139 às
12:08. Com "sem âncora forte" dos dois lados, o 32138 (com código) nem é par.
  · produto e destino declarado continuam fora dos dois lados
    (ancora_estrutural): os 186+ posts de produto do fumotom que abrem com a
    página seguem fora;
  · código de cupom deixa de excluir, mas a junção NUNCA pode esconder um
    código (codigo_preservado). Quem escolhe o texto depois é a decisão de
    evolução de sempre (pipeline.decisao), e relacao_composicao é None
    quando um lado só tem chave fraca — aí decide o SCORE. Então:
      – só o post tem código: junta só se o candidato NÃO vence pelo score
        (o texto com o código fica: SCORE_NAO_EVOLUI/DUP_SILENCIOSO); se o
        candidato tem parte dos códigos, é REDUZ e nunca substitui;
      – só o candidato tem código: junta só se ele VENCE pelo score e o post
        ainda tem orçamento de edição (o texto com o código entra: EVOLUI);
        se o post tem parte dos códigos, é AMPLIA e basta o orçamento;
      – códigos diferentes nos dois lados: nunca (codigos_distintos);
      – score ou orçamento não informados (PostVivo.score_maior/orcamento
        None): não junta;
  · com `com_codigo=False` (SOMBRA) a regra é exatamente a de antes.

NÃO faz: banco, Telegram, log, relógio, mudança de chave ou de natureza. A
única consulta é estática: a capacidade `paginas_resgate` das plataformas.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from plataformas import registry
from pipeline import cupom_lancamento
from pipeline.resolucao_identidade import (eh_chave_destino, eh_chave_forte,
                                           eh_chave_produto)

__all__ = ["Candidato", "PostVivo", "Decisao", "avaliar", "finalizar",
           "destino_afirmavel", "sem_ancora_forte", "chave_resgate",
           "ancora_estrutural", "entra_na_regra", "codigo_preservado",
           "pagina_de_resgate", "so_pagina_de_resgate", "MANTER",
           "JUNTA", "INCERTO", "INDETERMINADO", "PRINCIPAL", "PRINCIPAL_E_CUPONS",
           "NOVA", "PRESENTE", "AUSENTE", "SEM_VINCULO", "NAO_VERIFICADA",
           "AUSENTE_CONFIRMADA", "A_CRIAR"]

MANTER, JUNTA, INCERTO = "MANTER", "JUNTA", "INCERTO"
INDETERMINADO = "INDETERMINADO"           # do adaptador: leitura do estado falhou
PRINCIPAL, PRINCIPAL_E_CUPONS = "principal", "principal+cupons"
# vínculo da cópia no BANCO (espelho_post) e o que se sabe dela no TELEGRAM
NOVA, PRESENTE, AUSENTE, SEM_VINCULO = "nova", "presente", "ausente", "sem_vinculo"
NAO_VERIFICADA, AUSENTE_CONFIRMADA, A_CRIAR = "nao_verificada", "ausente_confirmada", "a_criar"


def sem_ancora_forte(chaves: Iterable[str]) -> bool:
    """Nenhuma âncora forte (produto, cupom com código, destino) e pelo
    menos uma chave."""
    chaves = list(chaves or ())
    return bool(chaves) and not any(eh_chave_forte(k) for k in chaves)


def _codigo(chave: str) -> bool:
    """Cupom COM código: `<plat>|cup|<código>`."""
    partes = (chave or "").split("|", 2)
    return len(partes) == 3 and partes[1] == "cup" and eh_chave_forte(chave)


def ancora_estrutural(chaves: Iterable[str]) -> bool:
    """Alguma âncora forte que não é código de cupom (produto, destino
    declarado): essa oferta tem autoridade própria e nunca entra na regra."""
    return any(eh_chave_forte(k) and not _codigo(k) for k in chaves or ())


def entra_na_regra(chaves: Iterable[str], com_codigo: bool = False) -> bool:
    """As chaves podem entrar na regra? Sem `com_codigo`: sem âncora forte
    nenhuma (sem_ancora_forte). Com: só não pode haver âncora estrutural."""
    chaves = list(chaves or ())
    if not com_codigo:
        return sem_ancora_forte(chaves)
    return bool(chaves) and not ancora_estrutural(chaves)


def _codigos(chaves) -> frozenset:
    return frozenset(k for k in chaves or () if _codigo(k))


def codigo_preservado(chaves_candidato: Iterable[str], p: "PostVivo") -> str:
    """"" quando juntar o candidato ao post `p` não esconde código de cupom
    nenhum na decisão de evolução que roda depois; senão o motivo (ver COM
    CÓDIGO DE CUPOM no cabeçalho)."""
    cc, cp = _codigos(chaves_candidato), _codigos(p.chaves)
    if cc == cp:
        return ""
    if cc < cp:                    # o post exibe código que o candidato não traz
        if cc or p.score_maior is False:
            return ""              # REDUZ, ou o score não deixa substituir
        return "codigo_do_post_sairia"
    if cp < cc:                    # o candidato traz código que o post não exibe
        if p.orcamento and (cp or p.score_maior):
            return ""              # AMPLIA, ou o score faz o texto evoluir
        return "codigo_novo_ficaria_oculto"
    return "codigos_distintos"


def pagina_de_resgate(plat: str, canonica: str) -> str:
    """`canonica` quando a plataforma `plat` a declara página de resgate
    de cupons (Plataforma.paginas_resgate); senão ""."""
    if not plat or not canonica:
        return ""
    donos = registry.compor_capacidade("paginas_resgate").get(canonica)
    return canonica if donos and plat in donos else ""


def so_pagina_de_resgate(chaves: Iterable[str]) -> bool:
    """As chaves exibidas são de uma campanha cujo destino é a página de
    resgate declarada: `<plat>|url|<página>` e nada de produto ou destino
    declarado. (A chave `url` nasce da 1ª canônica — a página é o destino
    PRINCIPAL por construção.)"""
    chaves = list(chaves or ())
    if any(eh_chave_produto(k) or eh_chave_destino(k) for k in chaves):
        return False
    for k in chaves:
        partes = k.split("|", 2)
        if (len(partes) == 3 and partes[1] == "url"
                and pagina_de_resgate(partes[0], partes[2])):
            return True
    return False


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
    cupons_hoje: bool         # hoje a mensagem fica também no canal de cupons?


@dataclass(frozen=True)
class PostVivo:
    msg_id: int
    plat: str
    chaves: frozenset         # o que o post possui ou exibe
    primeiro_link: str        # 1º link do texto PUBLICADO
    perfil: cupom_lancamento.Perfil
    nascimento: float
    fontes: frozenset
    # só para COM CÓDIGO DE CUPOM (o adaptador preenche em ATIVO), na régua
    # da decisão de evolução: o score do candidato, na escala do post, é
    # MAIOR que o do texto no ar? o post ainda tem orçamento de edição?
    score_maior: Optional[bool] = None
    orcamento: Optional[bool] = None


@dataclass(frozen=True)
class Decisao:
    acao: str                 # MANTER | JUNTA | INCERTO (| INDETERMINADO, do adaptador)
    post: Optional[int]
    motivo: str
    delta_s: Optional[float]
    destino_hoje: str         # o que a evidência sustenta hoje
    destino_elegivel: str     # pela política proposta (página declarada)
    outros: tuple = ()        # posts que também têm a página como principal
    pendente: bool = False    # JUNTA só "compatível": exige aprovação separada
    prova: str = "-"          # canonica_persistida | so_link_publicado
    vinculo: str = "-"        # nova | presente | ausente | sem_vinculo (finalizar)
    presenca: str = "-"       # nao_verificada | ausente_confirmada | a_criar | -
    destino_afirmavel: str = "-"   # o que a evidência sustenta (finalizar)
    se_copia_no_ar: str = "-"      # condicional explícita (vínculo não verificado)


def _destino(cupons: bool) -> str:
    return PRINCIPAL_E_CUPONS if cupons else PRINCIPAL


_PRESENCA = {NOVA: A_CRIAR, PRESENTE: NAO_VERIFICADA,
             AUSENTE: AUSENTE_CONFIRMADA, SEM_VINCULO: "-"}


def destino_afirmavel(elegivel: str, vinculo: str) -> tuple:
    """(destino que a evidência sustenta, condicional). Só o post NOVO
    elegível vai aos dois canais sem condição (é a publicação que envia).
    Vínculo presente: a cópia PODE estar no ar — a sombra não confere —,
    então só o principal é afirmado e o canal de cupons vira condicional.
    AUSENTE nunca volta; sem vínculo não se presume criação."""
    if elegivel != PRINCIPAL_E_CUPONS:
        return PRINCIPAL, "-"
    if vinculo == NOVA:
        return PRINCIPAL_E_CUPONS, "-"
    if vinculo == PRESENTE:
        return PRINCIPAL, PRINCIPAL_E_CUPONS
    return PRINCIPAL, "-"


def finalizar(d: Decisao, vinculo: str) -> Decisao:
    """A decisão com o vínculo da cópia do post que a campanha usaria."""
    afirmavel, condicional = destino_afirmavel(d.destino_elegivel, vinculo)
    return Decisao(d.acao, d.post, d.motivo, d.delta_s, d.destino_hoje,
                   d.destino_elegivel, d.outros, d.pendente, d.prova, vinculo,
                   _PRESENCA.get(vinculo, "-"), afirmavel, condicional)


def _prova(c: Candidato, p: PostVivo) -> str:
    """Em que a igualdade de página com o post `p` se apoia."""
    if f"{c.plat}|url|{c.pagina}" in p.chaves:
        return "canonica_persistida"
    return "so_link_publicado"


def avaliar(c: Candidato, vivos: list, agora: float, superior_s: float,
            com_codigo: bool = False) -> Decisao:
    """A decisão da relação para o candidato `c` diante dos posts `vivos`
    (os dados chegam prontos e COMPLETOS; nada aqui consulta estado).
    `com_codigo`: ver COM CÓDIGO DE CUPOM no cabeçalho."""
    hoje = _destino(c.cupons_hoje)
    if not entra_na_regra(c.chaves, com_codigo):
        return Decisao(MANTER, None, "fora:ancora_forte", None, hoje, hoje)
    if not c.pagina or not c.publicada:
        return Decisao(MANTER, None, "fora:sem_pagina_de_resgate", None, hoje, hoje)
    # política proposta: a página de resgate de cupons como link principal
    # torna a campanha elegível ao canal de cupons (o estado da cópia vem
    # depois, em finalizar)
    elegivel = PRINCIPAL_E_CUPONS
    exibem = [p for p in vivos
              if p.plat == c.plat and entra_na_regra(p.chaves, com_codigo)
              and p.primeiro_link == c.publicada]
    if c.familia_hoje is not None:
        fam = next((p for p in exibem if p.msg_id == c.familia_hoje), None)
        outros = tuple(p.msg_id for p in exibem if p.msg_id != c.familia_hoje)
        if fam is not None:
            return Decisao(MANTER, fam.msg_id, "ja_no_mesmo_post",
                           agora - fam.nascimento, hoje, elegivel, outros,
                           prova=_prova(c, fam))
        if outros:
            return Decisao(INCERTO, outros[0], "familia_de_hoje_diferente", None,
                           hoje, elegivel, outros)
        return Decisao(MANTER, None, "sem_par", None, hoje, elegivel)
    if not exibem:
        return Decisao(MANTER, None, "sem_par", None, hoje, elegivel)
    validos, motivos = [], []
    for p in exibem:
        delta = agora - p.nascimento
        if not cupom_lancamento.pode_mudar(c.chat, agora, p.nascimento, p.fontes,
                                           superior_s):
            motivos.append(("mesma_fonte" if c.chat in p.fontes
                            else "acima_do_superior", p, delta))
            continue
        esconde = codigo_preservado(c.chaves, p) if com_codigo else ""
        if esconde:
            motivos.append((esconde, p, delta))
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
                       hoje, elegivel, todos,
                       pendente=(rel != "igual"), prova=_prova(c, p))
    if len(validos) > 1:
        return Decisao(INCERTO, validos[0][1].msg_id, "ambiguo", None,
                       hoje, elegivel, todos)
    motivo, p, delta = motivos[0]
    return Decisao(INCERTO, p.msg_id, motivo, delta, hoje, elegivel, todos)
