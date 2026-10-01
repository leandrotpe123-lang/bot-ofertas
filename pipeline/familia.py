"""
Camada — Família de ofertas.

Responsabilidade ÚNICA: dado um conjunto de ofertas, responder
  (a) qual post lógico as acolhe, e
  (b) qual passa a ser a composição da família.

A regra "a família só cresce" (união) vive aqui, uma única vez.

NÃO faz:
  - decidir publicar/editar/ignorar     (pipeline.decisao)
  - travar o post                       (pipeline.exclusao)
  - aplicar o efeito no destino         (pipeline.publicacao)
  - falar com o Telegram                (pipeline.saida)

Contrato público:
  post_da_familia(ofertas, dest_fix, destinos, titulo) -> int | None
  eh_chave_container(chave) / mesmo_titulo(a, b) / so_container(ofertas)
  tem_destino(msg_id_dest)             -> bool
  unir(msg_id_dest, ofertas)           -> list[str]
  absorver(msg_id_dest, ofertas)       -> int
  compartilhadas(msg_id_dest, ofertas) -> list[str]
  fortes(chaves) / estrutura(chaves)   -> frozenset[str]
  relacao_composicao(candidato, alvo)  -> IGUAL|AMPLIA|REDUZ|PARCIAL|None
  plano_fusao(escrito, comp, scores)   -> Plano(fusoes, conflitos)
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from database import (db_absorver_ofertas, db_exibida, db_get_post,
                      db_ofertas_de_post, db_overlap_posts,
                      db_posts_vivos_com_prefixo)
from logger import log_out
from plataformas import registry
from pipeline.resolucao_identidade import (chave_cupom_geral,
                                           eh_chave_assinatura,
                                           eh_chave_destino, eh_chave_forte,
                                           prefixo_cupom_sem_codigo)
from utils.textos import _RE_EMJ_NORM, _RUIDO_NORM, _rm_acentos
from utils.urls import _netloc

__all__ = ["post_da_familia", "eh_chave_container", "mesmo_titulo",
           "so_container",
           "unir", "absorver", "compartilhadas", "tem_destino", "fortes", "estrutura", "relacao_composicao",
           "plano_fusao", "Plano"]


def _escolher_post(candidatos: list) -> int:
    """Post da família: maior sobreposição; empate → desempate estável
    (maior score → ts mais recente → maior msg_id_dest). `candidatos`
    já vem ordenado por sobreposição desc de db_overlap_posts."""
    max_n = candidatos[0][1]
    empatados = [mid for mid, n in candidatos if n == max_n]
    if len(empatados) == 1:
        return empatados[0]
    def _chave(mid: int):
        p = db_get_post(mid) or {}
        return (p.get("score", 0), p.get("ts", 0.0), mid)
    return max(empatados, key=_chave)


def compartilhadas(msg_id_dest: int, ofertas: list) -> list:
    """Interseção entre as ofertas do post e as da mensagem (uso: log)."""
    return sorted(set(ofertas) & set(db_ofertas_de_post(msg_id_dest)))


def unir(msg_id_dest: int, ofertas: list) -> list:
    """UNIÃO DA FAMÍLIA — regra de negócio: as ofertas registradas passam
    a ser a UNIÃO das do post existente com as da mensagem (X + Y). Lê o
    post ANTES de remover/regravar; a família só cresce, então a união é
    sempre superconjunto do post e nada legítimo se perde. Sem isto, o
    registro gravaria só as ofertas da mensagem e descartaria as
    exclusivas do post — quebrando a conectividade e duplicando a
    família."""
    return sorted(set(db_ofertas_de_post(msg_id_dest)) | set(ofertas))

def absorver(msg_id_dest: int, ofertas: list) -> int:
    """APRENDIZADO DE ÂNCORA — a família passa a reconhecer estas formas
    de encontrar a mesma oferta, sem que nada do post mude.

    Complementa `unir`, que compõe a família para quem VAI REGRAVAR o
    post. Aqui não há regravação: a decisão textual foi IGNORAR e o
    texto vencedor, o score, o líder, o edit_count, a janela e a mídia
    permanecem exatamente como estavam. Descobrir uma nova forma de
    reconhecer a oferta não dá a essa mensagem o direito de substituir
    o texto vencedor.

    Devolve quantas âncoras foram aprendidas (0 = nada novo).
    """
    novas = db_absorver_ofertas(msg_id_dest, ofertas)
    if novas:
        log_out.info(
            f"🧬 [ANCORA_ABSORVIDA] post:{msg_id_dest} aprendeu {novas} "
            f"âncora(s) | candidato={sorted(ofertas)} "
            f"| familia={sorted(db_ofertas_de_post(msg_id_dest))}")
    return novas


def destinos_do_post(msg_id_dest: int) -> set:
    """Destinos declarados que a família do post já reconhece."""
    return {k for k in db_ofertas_de_post(msg_id_dest) if eh_chave_destino(k)}


def tem_destino(msg_id_dest: int) -> bool:
    """FATO para a decisão: a família do post tem destino declarado?"""
    return bool(destinos_do_post(msg_id_dest))


def _acolhe(msg_id_dest: int, destinos: tuple) -> bool:
    """UM DESTINO NÃO ENTRA NA FAMÍLIA DE OUTRO DESTINO.

    Candidato sem destino declarado (só mecanismo: cupom, `/sec/`) é
    acolhido por qualquer família com que compartilhe âncora — é assim
    que o `/sec/` encontra a lista que traz o mesmo código.

    Candidato COM destino é acolhido por família sem destino (a lista
    encontra o `/sec/` que chegou antes) ou pela família do MESMO
    destino. Família de OUTRO destino não o acolhe, ainda que os dois
    compartilhem o código: duas campanhas diferentes com o mesmo cupom
    não colapsam.
    """
    if not destinos:
        return True
    do_post = destinos_do_post(msg_id_dest)
    return not do_post or not do_post.isdisjoint(destinos)


# ─────────────────────────────────────────────────────────────────
# CONTAINER (incidente 30/09, post 24262)
#
# Controle Xbox, Caixa AIWA e Caixa Philco vieram só com links da MESMA
# live Shopee (session 7187289): nenhum link carregava produto e as
# três mensagens ganharam a mesma âncora fraca
# `shopee|url|https://live.shopee.com.br/live/7187289`. A família as
# juntou num post só — e daí vieram SYNC de outro produto e mídia de
# outro produto no post do Controle.
#
# A URL de um CONTAINER (host declarado pela plataforma em
# `hosts_container`) identifica a transmissão, não o produto. Quando é
# TUDO o que o candidato compartilha com um post, o TÍTULO é o
# discriminador: título equivalente casa; diferente ou ausente, não
# casa (o candidato não contamina o post; vira post próprio).
# Âncora forte (produto, cupom, destino) ou outra fraca continuam
# decidindo como sempre — esta regra é só do container.
# ─────────────────────────────────────────────────────────────────
# TÍTULO = discriminador FRACO, só para âncora de container. Nunca
# identidade: não sobrescreve âncora forte, não é usado quando há
# qualquer outra âncora em comum. Determinístico e conservador — na
# dúvida, SEPARA (contaminar um post com outro produto é pior que abrir
# um post a mais). Tokens = 1ª linha, sem palavras vazias:
#   · sem token de um lado            → não casa (live sem título);
#   · algum lado CURTO (1–2 tokens)   → casa só se os conjuntos forem
#     IGUAIS ("Controle Xbox" × "Controle para Xbox" casa;
#     "Controle Xbox" × "Controle Xbox Preto" separa);
#   · ALTERNATIVA DE COR do mesmo anúncio ("Carbon Black ou Pulse
#     Red", "Preto/Branco", "nas cores Preta e Cinza") → as cores
#     oferecidas saem da comparação dos DOIS lados (ver
#     `_alternativas_de_cor`); o resto segue as regras abaixo;
#   · ambos LONGOS (≥ 3 tokens)       → um CONTÉM o outro (cada lado com
#     token próprio é variante e separa), o acréscimo não traz
#     DISTINTIVO — quantidade, sufixo de modelo, cor ou qualquer token
#     com dígito (capacidade, tamanho, modelo) — e o menor cobre ≥ 50%
#     do maior (genérico × específico separa).
# Números fazem parte do título (15 × 16, 50 × 55 polegadas): a
# tokenização é própria — `_alma` descarta tokens curtos como "15".
_TITULO_CURTO = 2
_TITULO_COBERTURA = 0.5
_TITULO_VAZIAS = frozenset({"para", "com", "por", "dos", "das", "nos", "nas",
                            "uma", "uns", "umas", "the", "and", "for",
                            "de", "da", "do", "em", "e", "a", "o"})
_TITULO_DISTINTIVOS = frozenset({
    # quantidade
    "kit", "kits", "combo", "combos", "pack", "pacote", "conjunto", "jogo",
    "par", "pares",
    # sufixo de modelo
    "pro", "max", "plus", "mini", "ultra", "lite", "air", "slim", "neo",
    "turbo", "fe",
    # cor
    "preto", "preta", "branco", "branca", "cinza", "azul", "vermelho",
    "vermelha", "verde", "amarelo", "amarela", "rosa", "roxo", "roxa",
    "laranja", "marrom", "bege", "prata", "dourado", "dourada", "grafite",
    "lilas", "black", "white", "gray", "grey", "blue", "red", "green",
    "pink", "silver", "gold"})
_TITULO_CORES = frozenset({
    "preto", "preta", "branco", "branca", "cinza", "azul", "vermelho",
    "vermelha", "verde", "amarelo", "amarela", "rosa", "roxo", "roxa",
    "laranja", "marrom", "bege", "prata", "dourado", "dourada", "grafite",
    "lilas", "black", "white", "gray", "grey", "blue", "red", "green",
    "pink", "silver", "gold"})

# ALTERNATIVA DE VARIANTE DE COR ("Carbon Black ou Pulse Red",
# "Preto/Branco", "nas cores Preta e Cinza"): o MESMO anúncio oferecendo
# mais de uma cor. Reconhecida só como CLÁUSULA FINAL introduzida por um
# marcador, em que TODA opção é composta apenas de cor e de qualificador
# de nome de cor (Pulse Red, Carbon Black, Robot White…) e tem ao menos
# uma cor de fato. Qualquer outra coisa numa opção (produto, número,
# modelo, "Pro", "iPhone 16") invalida a cláusula e o título é comparado
# inteiro. "ou" sozinho não autoriza nada.
_TITULO_QUALIFICADORES_COR = frozenset({
    "carbon", "pulse", "robot", "shock", "midnight", "cosmic", "deep",
    "electric", "velocity", "starlight", "volcanic", "nova", "sterling",
    "glacier", "astral", "stellar", "mystic", "ice", "escuro", "escura",
    "claro", "clara", "fosco", "fosca", "metalico", "metalica", "neon",
    "pastel", "matte", "space", "night", "sky"})
_RE_ALTERNATIVA = re.compile(
    r"\s(?:ou|nas\s+cores|na\s+cor|cores)\s|\s*/\s*")
_RE_OPCOES = re.compile(r"\s(?:ou|e)\s|,|/")


def eh_chave_container(chave: str) -> bool:
    """Âncora de fallback `plat|url|<url>` cuja URL é de CONTAINER."""
    partes = (chave or "").split("|", 2)
    if len(partes) != 3 or partes[1] != "url" or not partes[2]:
        return False
    host = _netloc(partes[2])
    return bool(host) and any(
        host == h or host.endswith("." + h)
        for h in registry.compor_capacidade("hosts_container"))


def _tokens_titulo(linha: str) -> frozenset:
    """Tokens da linha-título: mesma limpeza de `utils.textos._alma`
    (acentos, URLs, emojis, preço, ruído promocional), mas preservando
    todo token com dígito — o número distingue modelo, tamanho e
    capacidade."""
    t = _rm_acentos(linha.lower())
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"r\$\s*[\d.,]+", " ", t)
    t = re.sub(r"\b\d+%", " ", t)
    t = _RE_EMJ_NORM.sub(" ", t)
    t = re.sub(r"[^\w\s]", " ", t)
    return frozenset(w for w in t.split()
                     if w not in _RUIDO_NORM
                     and (len(w) > 2 or any(ch.isdigit() for ch in w)))


def _linha_titulo(texto: str) -> str:
    for linha in (texto or "").split("\n"):
        if linha.strip():
            return linha
    return ""


def _titulo(texto: str) -> frozenset:
    """Tokens da 1ª linha não vazia."""
    return _tokens_titulo(_linha_titulo(texto))


def _eh_opcao_de_cor(opcao: str) -> bool:
    toks = [w for w in re.sub(r"[^\w\s]", " ", opcao).split() if w]
    return (bool(toks) and any(w in _TITULO_CORES for w in toks)
            and all(w in _TITULO_CORES or w in _TITULO_QUALIFICADORES_COR
                    for w in toks))


def _alternativas_de_cor(texto: str) -> tuple:
    """(base, cores): a linha-título sem a cláusula FINAL de alternativas
    de cor e os tokens dessa cláusula — incluindo a cor que fecha a base
    imediatamente antes do marcador (a 1ª opção: "Carbon Black ou Pulse
    Red" → cores {carbon, black, pulse, red}). Sem cláusula válida:
    (linha inteira, vazio). Marcador com opção que NÃO é cor ("ou iPhone
    16", "ou Pulse", "ou Mesa Branca"): (linha inteira, None) — alternativa
    de outra coisa, o título só casa se for idêntico."""
    linha = _linha_titulo(texto)
    t = " " + _rm_acentos(linha.lower()).strip() + " "
    m = _RE_ALTERNATIVA.search(t)
    if not m:
        return linha, frozenset()
    base, resto = t[:m.start()], t[m.end():]
    opcoes = [o.strip() for o in _RE_OPCOES.split(" " + resto + " ") if o.strip()]
    if not opcoes or not all(_eh_opcao_de_cor(o) for o in opcoes):
        return linha, None
    cores = set()
    for o in opcoes:
        cores |= set(o.split())
    # a 1ª opção: a sequência de cor/qualificador que fecha a base
    palavras = re.sub(r"[^\w\s]", " ", base).split()
    while palavras and (palavras[-1] in _TITULO_CORES
                        or palavras[-1] in _TITULO_QUALIFICADORES_COR):
        cores.add(palavras.pop())
    return " ".join(palavras), frozenset(cores)


def mesmo_titulo(a: str, b: str) -> bool:
    """Os dois textos anunciam, com CLAREZA, o mesmo produto pelo título?
    Ausente, ambíguo ou limítrofe → False (não casa)."""
    base_a, cores_a = _alternativas_de_cor(a)
    base_b, cores_b = _alternativas_de_cor(b)
    if cores_a is None or cores_b is None:
        # alternativa que não é de cor: "ou" não autoriza nada
        ta = _tokens_titulo(base_a) - _TITULO_VAZIAS
        tb = _tokens_titulo(base_b) - _TITULO_VAZIAS
        return bool(ta) and ta == tb
    cores = cores_a | cores_b
    ta = _tokens_titulo(base_a) - _TITULO_VAZIAS - cores
    tb = _tokens_titulo(base_b) - _TITULO_VAZIAS - cores
    if not ta or not tb:
        return False
    if min(len(ta), len(tb)) <= _TITULO_CURTO:
        return ta == tb
    menor, maior = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    if not menor <= maior:
        return False
    extra = maior - menor
    if extra & _TITULO_DISTINTIVOS or any(ch.isdigit() for w in extra for ch in w):
        return False
    return len(menor) / len(maior) >= _TITULO_COBERTURA


def _so_container(mid: int, ofertas: set) -> bool:
    """O candidato `mid` compartilha com a mensagem SOMENTE âncoras de
    container (pela posse ou pela exibição)?"""
    comuns = ofertas & (set(db_ofertas_de_post(mid)) | db_exibida(mid))
    return bool(comuns) and all(eh_chave_container(k) for k in comuns)


def so_container(ofertas) -> bool:
    """A mensagem só tem âncora(s) de container (nenhuma outra)?"""
    ofertas = list(ofertas or ())
    return bool(ofertas) and all(eh_chave_container(k) for k in ofertas)


def _adotar_por_container(container: str, ofertas: list, titulo: str) -> list:
    """Mensagem com identidade FORTE que não achou família pelo forte, mas
    veio da mesma live de um post que só era conhecido pela live: com
    título CLARAMENTE equivalente e sem outra identidade forte no post, o
    post é a família — e o forte passa a ser a referência estrutural da
    oferta (evolução/absorção seguem as regras de sempre)."""
    ofs = set(ofertas)
    aceitos = []
    for mid, n in db_overlap_posts([container]):
        chaves = set(db_ofertas_de_post(mid)) | db_exibida(mid)
        if fortes(chaves) - ofs:
            continue                      # outro forte: título não decide
        if mesmo_titulo(titulo, (db_get_post(mid) or {}).get("texto", "")):
            aceitos.append((mid, n))
    if len(aceitos) > 1:
        # Mais de um post só-live plausível: o forte não escolhe no escuro.
        log_out.info(
            f"🧬 [LIVE_ADOCAO_AMBIGUA] {sorted(ofs)} — posts "
            f"{[m for m, _ in aceitos]} da live {container} passam pelo "
            f"título; nenhum é adotado")
        return []
    if aceitos:
        log_out.info(
            f"🧬 [LIVE_ADOTA_FORTE] {sorted(ofs)} adota post:{aceitos[0][0]} "
            f"da mesma live {container} — título equivalente, sem forte "
            f"conflitante")
    return aceitos


def _filtrar_container(candidatos: list, ofertas: list, titulo: str) -> list:
    if not any(eh_chave_container(k) for k in ofertas):
        return candidatos                 # caminho comum: zero consulta
    ofs = set(ofertas)
    aceitos, por_titulo = [], []
    for mid, n in candidatos:
        if not _so_container(mid, ofs):
            aceitos.append((mid, n))
            continue
        texto_post = (db_get_post(mid) or {}).get("texto", "")
        if mesmo_titulo(titulo, texto_post):
            por_titulo.append((mid, n))
            continue
        motivo = ("LIVE_SEM_IDENTIDADE" if not (_titulo(titulo)
                                                and _titulo(texto_post))
                  else "FAMILIA_TITULO_DIVERGENTE")
        log_out.info(
            f"🧬 [{motivo}] post:{mid} recusado — só compartilha container "
            f"{sorted(k for k in ofs if eh_chave_container(k))} e o título "
            f"não prova o mesmo produto (candidato="
            f"{(titulo or '').strip().splitlines()[0][:60] if (titulo or '').strip() else '-'!r})")
    if len(por_titulo) > 1:
        # Mais de um post da mesma live com título equivalente (ex.: dois
        # produtos fortes diferentes com o mesmo nome): ambíguo — o
        # título não escolhe entre eles.
        log_out.info(
            f"🧬 [LIVE_AMBIGUA] posts {[m for m, _ in por_titulo]} passam pelo "
            f"título — nenhum é aceito só pelo container")
        por_titulo = []
    return aceitos + por_titulo


# ─────────────────────────────────────────────────────────────────
# CUPOM SEM NOME — GENÉRICO × ASSINATURA (madrugada Shopee)
#
# Toda noite às 23:59 o Promotom posta "Cupons Shopee" seco — sem nome e
# sem benefício (`cupb|geral`) — e logo depois Fada/Samuel postam a
# MESMA rodada com os valores ("R$30 OFF em R$299", `cupb|v:30-299`).
# As chaves não se tocam: o seco ficava parado e o rico virava outro
# post (testes 1356/1357 de 01/10 → posts 24418 e 24419).
#
# O genérico não identifica campanha nenhuma; a assinatura não traz
# nome. Quando NÃO houver mais nada que os diferencie, os dois se
# encontram por ADOÇÃO DE CANDIDATO ÚNICO — o mesmo idioma da Frente
# LIVE (`_adotar_por_container`):
#   · assinatura sem família → adota o ÚNICO post vivo que é SÓ genérico
#     da plataforma (nada além de `cupb|geral`);
#   · genérico sem família   → adota o ÚNICO post vivo que é SÓ
#     assinatura da plataforma (nada além de `cupb|<x>:<y>`).
# Mais de um plausível → ninguém adota (na dúvida, não funde). Nome
# declarado, cupom com código, destino, produto, url — qualquer outra
# âncora no candidato ou no post — tira o caso desta regra. A adoção só
# escolhe a FAMÍLIA; quem decide o texto é a decisão de sempre (o mais
# rico evolui pelo score; o seco que chega depois só ensina a âncora).
# Um post adotado deixa de ser "só genérico": a regra não se repete
# sobre ele — uma segunda assinatura diferente vira post próprio.
# ─────────────────────────────────────────────────────────────────
def _so_cupom_sem_nome(chaves, plat: str):
    """"geral" | "assinatura" | None — a natureza das chaves quando TODAS
    são cupom sem nome da plataforma, de um tipo só."""
    chaves = set(chaves or ())
    if not chaves:
        return None
    if chaves == {chave_cupom_geral(plat)}:
        return "geral"
    prefixo = prefixo_cupom_sem_codigo(plat)
    if all(k.startswith(prefixo) and eh_chave_assinatura(k) for k in chaves):
        return "assinatura"
    return None


def _adotar_cupom_sem_nome(ofertas: list) -> list:
    """Candidato só-genérico ou só-assinatura sem família: o ÚNICO post
    vivo do tipo complementar, da mesma plataforma, é a família."""
    ofs = set(ofertas or ())
    plat = next(iter(ofs)).split("|", 1)[0] if ofs else ""
    tipo = _so_cupom_sem_nome(ofs, plat)
    if tipo is None:
        return []
    procura = "assinatura" if tipo == "geral" else "geral"
    aceitos = []
    for mid in db_posts_vivos_com_prefixo(prefixo_cupom_sem_codigo(plat)):
        chaves = set(db_ofertas_de_post(mid)) | db_exibida(mid)
        if _so_cupom_sem_nome(chaves, plat) == procura:
            aceitos.append((mid, 0))
    if len(aceitos) > 1:
        log_out.info(
            f"🧬 [CUPOM_ADOCAO_AMBIGUA] {sorted(ofs)} — posts "
            f"{[m for m, _ in aceitos]} só-{procura} vivos; nenhum é adotado")
        return []
    if aceitos:
        log_out.info(
            f"🧬 [CUPOM_ADOTA] {sorted(ofs)} ({tipo}) adota post:{aceitos[0][0]}"
            f" só-{procura} — único cupom sem nome vivo da plataforma")
    return aceitos


def post_da_familia(ofertas: list, dest_fix=None, destinos: tuple = (),
                    titulo: str = "", container: str = ""):
    """Post vivo que acolhe estas ofertas, ou None se não houver.
    `dest_fix` fixa o alvo pelo vínculo de Origem (I2) e curto-circuita
    a busca por sobreposição (e a regra de destino: é a mesma origem).

    `destinos` são os destinos declarados do candidato (espécie
    "destino"). Vazio — toda mensagem sem destino declarado, e toda
    plataforma que não os declara — é exatamente o comportamento de
    antes: nenhum candidato é recusado."""
    candidatos = ([(dest_fix, 0)] if dest_fix
                  else db_overlap_posts(ofertas))
    if candidatos and destinos and not dest_fix:
        recusados = [mid for mid, _n in candidatos
                     if not _acolhe(mid, destinos)]
        if recusados:
            log_out.info(
                f"🧬 [FAMILIA_OUTRO_DESTINO] candidato={sorted(destinos)} "
                f"recusado por post(s) {recusados} — destinos distintos "
                f"não colapsam, mesmo com cupom em comum")
            candidatos = [(mid, n) for mid, n in candidatos
                          if mid not in recusados]
    if candidatos and not dest_fix:
        # [Container] antes de QUALQUER escolha: o candidato recusado aqui
        # nunca chega a DUP/SYNC/evolução/upgrade de mídia daquele post.
        candidatos = _filtrar_container(candidatos, ofertas, titulo)
    if (not candidatos and not dest_fix and container
            and not so_container(ofertas) and eh_chave_container(container)):
        # [Container] forte novo de uma oferta antes só conhecida pela live.
        candidatos = _adotar_por_container(container, ofertas, titulo)
    if not candidatos and not dest_fix and not destinos:
        # [Cupom sem nome] genérico × assinatura: candidato único.
        candidatos = _adotar_cupom_sem_nome(ofertas)
    if len(candidatos) > 1:
        log_out.debug(
            f"🧬 [FAMILIA_MULTI] {len(candidatos)} posts em sobreposição "
            f"p/ ofertas={ofertas} — escolhendo o melhor candidato")
    if not candidatos:
        return None
    msg_id_rel = _escolher_post(candidatos)
    log_out.info(f"🔎 [OVERLAP_MATCH] post:{msg_id_rel} casou por ofertas_compartilhadas={compartilhadas(msg_id_rel, ofertas)} | candidato={sorted(ofertas)} | candidatos={[c[0] for c in candidatos]} fix={dest_fix or '-'} post_ofertas={db_ofertas_de_post(msg_id_rel)}")
    return msg_id_rel


# ─────────────────────────────────────────────────────────────────
# [Frente 8] CONSOLIDAÇÃO ESTRUTURAL — regra PURA (sem banco, sem relógio)
#
# Quem decide se uma escrita cria duplicidade é pipeline.convergencia;
# as regras que ela aplica moram aqui, puras e testáveis.
#
# VOCABULÁRIO
#   fortes(post)     âncoras FORTES que o post EXIBE: produto exato,
#                    cupom com código, destino declarado. Nunca o que
#                    foi só aprendido, nunca a imagem, nunca campanha/
#                    cashback/url/texto.
#   estrutura(post)  as fortes que dizem O QUE a oferta é. Com destino
#                    declarado, o código é ATRIBUTO (Frente 7B): duas
#                    listas diferentes com o mesmo código não são a
#                    mesma oferta; o `/sec/` sem destino continua contido
#                    na lista que traz o código.
#   duplicidade      estrutura(X) ∩ fortes(Y) ≠ ∅ entre dois posts vivos.
#
# P1 e P2 são ofertas DIFERENTES (mesma loja não prova nada). O que se
# funde são POSTS: um post sai quando TUDO o que ele representa continua
# exibido por sobreviventes reais. Nada é inventado, nada some.
# ─────────────────────────────────────────────────────────────────
IGUAL, AMPLIA, REDUZ, PARCIAL = "IGUAL", "AMPLIA", "REDUZ", "PARCIAL"


def fortes(chaves) -> frozenset:
    """Âncoras fortes de um conjunto de chaves."""
    return frozenset(k for k in (chaves or ()) if eh_chave_forte(k))


def estrutura(chaves) -> frozenset:
    """Identidade estrutural: as fortes, sem os códigos quando há destino
    declarado (neste caso o código é atributo da lista)."""
    f = fortes(chaves)
    if any(eh_chave_destino(k) for k in f):
        return frozenset(k for k in f if eh_chave_destino(k)
                         or k.split("|", 2)[1:2] != ["cup"])
    return f


def relacao_composicao(candidato, alvo):
    """Composição do candidato em relação ao que o alvo EXIBE:
    IGUAL | AMPLIA | REDUZ | PARCIAL — ou None quando um dos lados não
    tem composição forte (legado, só fracas): comportamento de sempre,
    decidido por score.

    Mesma régua da consolidação (cobertura de ESTRUTURA por FORTES):
      perde = alguma estrutura do alvo não seria mais exibida;
      ganha = o candidato traz estrutura que o alvo não exibe.
    Para produtos e cupons sem destino é a comparação de conjuntos
    (⊋ / ⊊ / parcial). Em lista (destino declarado) o código é
    atributo (Frente 7B): a mesma lista com outro código é IGUAL e o
    score decide; a lista que chega sobre o `/sec/` do mesmo código
    AMPLIA."""
    c, a = fortes(candidato), fortes(alvo)
    if not c or not a:
        return None
    perde = not estrutura(a) <= c
    ganha = not estrutura(c) <= a
    if perde and ganha:
        return PARCIAL
    if perde:
        return REDUZ
    if ganha:
        return AMPLIA
    return IGUAL


@dataclass
class Plano:
    """Resultado da consolidação: quem sai, para quem vai cada âncora e
    que duplicidade sobra sem solução real."""
    fusoes: list = field(default_factory=list)     # [(perdedor, principal, {chave: dono})]
    conflitos: list = field(default_factory=list)  # [(x, y, frozenset(compartilhadas))]

    def envolve(self, mid) -> bool:
        return any(mid in (x, y) for x, y, _ in self.conflitos)


def _duplica(ex, fx, ey, fy) -> frozenset:
    return (ex & fy) | (ey & fx)


def plano_fusao(escrito: int, composicoes: dict, scores: dict | None = None):
    """Decide a consolidação de um grupo de posts vivos.

    `composicoes` = {msg_id: chaves EXIBIDAS}; `scores` = {msg_id: score
    do texto publicado}. `escrito` é o post que acabou (ou vai acabar) de
    exibir conteúdo novo. PURA.

    Retirada GULOSA e DETERMINÍSTICA, do menos ao mais prioritário:
      prioridade de FICAR = (|fortes|, score, é o escrito, −msg_id)
        · estrutura primeiro (quem exibe mais não perde para quem exibe
          menos — nenhum produto some);
        · score só desempata composições do mesmo tamanho (autoridade de
          texto entre conteúdos que representam a mesma coisa).
      um post SAI quando toda a sua estrutura continua exibida pelos que
      ficam (cobertura real, inclusive por mais de um sobrevivente).
    Cada âncora forte do que sai vai para um sobrevivente que a EXIBE;
    a origem vai para o principal (o que mais o cobre).
    O que sobra duplicado sem ninguém redundante é CONFLITO — nunca
    resolvido apagando conteúdo exclusivo."""
    scores = scores or {}
    f = {m: fortes(c) for m, c in (composicoes or {}).items()}
    f = {m: s for m, s in f.items() if s}
    e = {m: estrutura(s) for m, s in f.items()}
    plano = Plano()
    if escrito not in f:
        return plano

    def prio(m):
        return (len(f[m]), scores.get(m, 0), m == escrito, -m)

    vivos = set(f)
    principal = {}
    for m in sorted(f, key=prio):
        outros = vivos - {m}
        if not outros:
            continue
        exibido = frozenset().union(*(f[x] for x in outros))
        if e[m] <= exibido:
            principal[m] = max(outros, key=lambda x: (len(e[m] & f[x]), prio(x)))
            vivos.discard(m)

    def final(m):
        seen = set()
        while m in principal and m not in seen:
            seen.add(m)
            m = principal[m]
        return m

    for m in sorted(principal, key=prio):
        p = final(m)
        donos = {}
        for k in f[m]:
            if k in f[p]:
                donos[k] = p
            else:
                quem = sorted(x for x in vivos if k in f[x])
                donos[k] = quem[0] if quem else p
        plano.fusoes.append((m, p, donos))

    for x in sorted(vivos):
        for y in sorted(vivos):
            if x < y:
                d = _duplica(e[x], f[x], e[y], f[y])
                if d:
                    plano.conflitos.append((x, y, d))
    return plano
