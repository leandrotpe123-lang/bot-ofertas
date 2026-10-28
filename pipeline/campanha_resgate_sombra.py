"""
CAMPANHA POR PÁGINA DE RESGATE — adaptador (SOMBRA por padrão; ATIVO por
config.CAMPANHA_RESGATE_MODO).

Responsabilidade ÚNICA: para uma mensagem NOVA, reunir do estado os fatos
que a regra pura (pipeline.campanha_resgate) precisa, pedir a decisão e
REGISTRAR o que ela faria: 🎟 [RESGATE_SOMBRA]. Não publica, não edita, não
apaga, não funde e não escreve no banco. Em SOMBRA não muda a família que o
pipeline escolheu — `registrar` devolve sempre None e `familia` devolve a
família de hoje. Qualquer erro vira uma linha e mais nada (a publicação
segue igual).

ATIVO (familia, mutexes): a JUNTA vira a família da mensagem — só quando
hoje ela abriria post NOVO (família None) e a regra de cupom sem código não
decidiu nada (lancamento_cupom não devolveu JUNTA nem NOVA). A regra nunca
troca uma família já escolhida, nunca contraria a regra de cupom e nunca
junta sobre leitura que falhou (INDETERMINADO). O texto que fica no post é
o de sempre: a decisão de evolução (pipeline.decisao) roda depois, sob o
lock do post — o mais rico evolui, o empate fica com o texto que já está
no ar, e a guarda de código da regra (codigo_preservado) só deixa juntar
quando essa decisão não pode tirar um código do ar. A linha
ganha `modo=ATIVO aplicada=sim|nao:<porquê>|-`. A trava sintética
`<plat>|resgate*|<página>` (mutexes) serializa as mensagens da mesma
campanha. Em ATIVO a regra também olha post com código de cupom
(regra: COM CÓDIGO DE CUPOM).

Chamada por publicacao._enviar_inner (familia) logo depois da regra de
cupom sem código, sob os mesmos locks (ORIGEM → IDENTIDADE), só para
mensagem nova.
Custo: nada para quem tem âncora forte ou não leva a uma página de resgate
declarada (a imensa maioria); o resto lê os posts vivos da plataforma.

LEITURA ESTRITA: toda leitura do estado usa estrito=True (database_posts e
database_espelho — esta, do #33): a falha levanta em vez de virar lista
vazia ou None. Estado que não pôde ser lido INTEIRO (falha, ou post da
lista de vivos que some na leitura seguinte) dá acao=INDETERMINADO
motivo=erro_leitura:<etapa>:<Classe> — nunca MANTER, JUNTA ou INCERTO como
se o estado estivesse completo.

VÍNCULO × PRESENÇA × DESTINO AFIRMÁVEL (ver pipeline.campanha_resgate): o
vínculo do espelho (espelho_post) é lido só para o post que importa — a
família de hoje (etapa espelho_familia) e o alvo da junção (espelho_post).
A sombra NÃO consulta o Telegram: vínculo presente sai como
presenca=nao_verificada, destino_afirmavel=principal e
se_copia_no_ar=principal+cupons (condicional, nunca certeza — o worker só
descobre uma cópia apagada à mão quando tenta editá-la). Vínculo AUSENTE:
presenca=ausente_confirmada, destino_afirmavel=principal (a cópia nunca
volta). Leitura que falha: INDETERMINADO.

LINHA (só quando a regra junta, duvida, mudaria o destino afirmável, vê
outro post com a mesma página, acha a cópia AUSENTE ou não consegue ler; ids,
chaves, página canônica, nomes e benefícios — nada de texto nem link
publicado):
  🎟 [RESGATE_SOMBRA] msg=(chat,id) acao= post: motivo= Δ= pagina=
     hoje=post:<id>|post_novo ident_atual=[…] ident_proposta=[…]
     destino_hoje= destino_elegivel= vinculo= presenca= destino_afirmavel=
     se_copia_no_ar= prova= aprovacao=pendente|- nome=(cand|post)
     beneficio=(cand|post) [modo=ATIVO aplicada=sim|nao:<porquê>|- — só em ATIVO]
     outros=[posts que também têm a página como destino principal]
"""
from __future__ import annotations

import contextlib
import time
from typing import Optional

import config
from database import (db_espelho_get, db_exibida, db_get_post,
                      db_ofertas_de_post, db_origens_do_post,
                      db_posts_vivos_com_prefixo)
from logger import log_out
from pipeline import campanha_resgate as regra
from pipeline import cupom_lancamento
from pipeline.espelho_cupons import _AUSENTE    # o valor do vínculo AUSENTE
from pipeline.ingestao import links_de
from pipeline.resolucao_identidade import eh_identidade_cupom
from pipeline.score import V_LEGADO, score_na_escala
from pipeline.vida_oferta import VIDA_OFERTA_S

__all__ = ["registrar", "familia", "mutexes", "modo", "avaliar",
           "pagina_de_resgate", "SOMBRA", "ATIVO"]

# Espécies fracas: as únicas em que a regra olha (âncora forte tem dono).
_FRACAS = ("cupb", "url", "camp", "cash", "txt")
# Em ATIVO, também o post com código de cupom (regra: COM CÓDIGO DE CUPOM).
_COM_CODIGO = _FRACAS + ("cup",)

SOMBRA, ATIVO = "SOMBRA", "ATIVO"


def modo() -> str:
    """config.CAMPANHA_RESGATE_MODO. Qualquer valor diferente de "ATIVO"
    vale como SOMBRA (o lado seguro)."""
    return ATIVO if getattr(config, "CAMPANHA_RESGATE_MODO", SOMBRA) == ATIVO else SOMBRA


class LeituraFalhou(Exception):
    """Uma leitura do estado não pôde ser feita inteira."""
    def __init__(self, etapa: str, classe: str):
        super().__init__(f"{etapa}:{classe}")
        self.etapa, self.classe = etapa, classe


def _ler(etapa: str, leitura, *args):
    try:
        return leitura(*args, estrito=True)
    except Exception as e:                                 # noqa: BLE001
        raise LeituraFalhou(etapa, type(e).__name__) from e


# A dona é a regra (o canal de cupons também a usa): mesma função.
pagina_de_resgate = regra.pagina_de_resgate


def _publicada_da_ancora(norm) -> str:
    """O link publicado da página da âncora: a 1ª entrada do mapa é a da 1ª
    canônica (normalizacao.resolver_e_afiliar preenche os dois juntos, e
    ancora_url é a 1ª canônica)."""
    return next(iter((getattr(norm, "mapa", None) or {}).values()), "")


def _vinculo(mid: int, etapa: str) -> str:
    """O vínculo da cópia do post `mid` no banco (leitura estrita: erro de
    consulta NUNCA vira "sem vínculo"). Não diz se a mensagem está no ar."""
    esp = _ler(etapa, db_espelho_get, mid)
    if esp is None:
        return regra.SEM_VINCULO
    return regra.AUSENTE if esp == _AUSENTE else regra.PRESENTE


def _vivos(plat: str, especies: tuple = _FRACAS, norm=None,
           score: Optional[int] = None) -> list:
    """Os posts vivos da plataforma nas `especies`, já lidos. Com `score`
    (ATIVO), cada um leva score_maior/orcamento na régua de
    decisao.decidir: o score do candidato projetado na escala do post contra
    o do texto no ar, e edit_count < _MAX_EDITS."""
    ids = []
    for especie in especies:
        for mid in _ler("vivos", db_posts_vivos_com_prefixo, f"{plat}|{especie}|"):
            if mid not in ids:
                ids.append(mid)
    vivos = []
    for mid in ids:
        est = _ler("post", db_get_post, mid)
        if not est:             # estava na lista de vivos: a leitura ficou incompleta
            raise LeituraFalhou("post", "Ausente")
        score_maior = orcamento = None
        if score is not None and norm is not None:
            score_maior = score_na_escala(
                score, norm.chat, norm.tem_midia,
                est.get("score_versao") or V_LEGADO) > (est.get("score") or 0)
            orcamento = (est.get("edit_count", 0) or 0) < config._MAX_EDITS
        vivos.append(regra.PostVivo(
            msg_id=mid, plat=est.get("plat") or plat,
            chaves=(frozenset(_ler("exibida", db_exibida, mid))
                    | frozenset(_ler("ofertas", db_ofertas_de_post, mid))),
            primeiro_link=next(iter(links_de(est.get("texto") or "")), ""),
            perfil=cupom_lancamento.perfil(est.get("texto") or "", plat),
            nascimento=float(est.get("janela_fim") or 0.0) - VIDA_OFERTA_S,
            fontes=frozenset(str(o) for o, _m in _ler("origens", db_origens_do_post, mid)),
            score_maior=score_maior, orcamento=orcamento))
    return vivos


def avaliar(norm, ofertas, familia_hoje: Optional[int],
            agora: Optional[float] = None, score: Optional[int] = None):
    """(candidato, posts vivos, decisão) para a mensagem `norm`, ou None
    quando ela está fora da regra (âncora forte ou nenhuma página de
    resgate declarada). Só LÊ o estado; quem decide é a regra pura. Leitura
    que falha: decisão INDETERMINADO (e posts vivos vazios). `score` (o do
    candidato, só em ATIVO) alimenta a guarda de código (codigo_preservado);
    sem ele, nenhuma junção que envolva código."""
    plat = getattr(norm, "plat", "") or ""
    chaves = tuple(ofertas or ())
    ativo = modo() == ATIVO
    if not regra.entra_na_regra(chaves, ativo):
        return None
    pagina = pagina_de_resgate(plat, getattr(norm, "ancora_url", "") or "")
    if not pagina:
        return None
    agora = time.time() if agora is None else agora
    base = dict(plat=plat, chat=str(getattr(norm, "chat", "")), chaves=chaves,
                pagina=pagina, publicada=_publicada_da_ancora(norm),
                perfil=cupom_lancamento.perfil(getattr(norm, "texto_analise", "") or "", plat),
                familia_hoje=familia_hoje)
    vivos, d = [], None
    try:
        if familia_hoje is not None:
            vinculo_familia = _vinculo(familia_hoje, "espelho_familia")
            # sem conferir o Telegram, só o principal é afirmável hoje
            cupons_hoje = False
        else:                       # post novo: o roteamento de hoje decide
            vinculo_familia = regra.NOVA
            cupons_hoje = eh_identidade_cupom(chaves) or (
                ativo and regra.so_pagina_de_resgate(chaves))
        c = regra.Candidato(cupons_hoje=cupons_hoje, **base)
        vivos = _vivos(plat, _COM_CODIGO, norm, score) if ativo else _vivos(plat)
        d = regra.avaliar(c, vivos, agora, float(config.CUPOM_LIMITE_SUPERIOR_S),
                          com_codigo=ativo)
        # a cópia do post que a campanha usaria: o alvo da junção, ou a
        # família de hoje / o post novo (nada muda de post)
        vinculo = (_vinculo(d.post, "espelho_post") if d.acao == regra.JUNTA
                   else vinculo_familia)
    except LeituraFalhou as f:
        c = regra.Candidato(cupons_hoje=False, **base)
        return c, vivos, regra.Decisao(
            regra.INDETERMINADO, d.post if d is not None else None,
            f"erro_leitura:{f.etapa}:{f.classe}", None, "indeterminado",
            "indeterminado", vinculo="nao_lido", presenca="-",
            destino_afirmavel="indeterminado")
    return c, vivos, regra.finalizar(d, vinculo)


def _par(a, b) -> str:
    return f"({a or '-'}|{b or '-'})"


def _beneficio(perfil) -> str:
    return perfil.pct or ",".join(sorted(perfil.valores))


def registrar(norm, ofertas, familia_hoje: Optional[int],
              agora: Optional[float] = None) -> None:
    """Registra o que a regra faria para a mensagem `norm`. Nunca levanta e
    nunca muda nada; sempre devolve None."""
    _avaliar_e_registrar(norm, ofertas, familia_hoje, agora)
    return None


def familia(norm, ofertas, familia_hoje: Optional[int],
            via_lancamento: bool = False, agora: Optional[float] = None,
            *, score: Optional[int] = None):
    """(família a usar, veio_da_campanha) para a mensagem NOVA `norm`.
    SOMBRA: registrar() e (familia_hoje, False), sempre. ATIVO: (post da
    JUNTA, True) só quando a junção se aplica (ver _aplicacao); senão
    (familia_hoje, False). `score`: o do candidato (enr.score). Nunca
    levanta: erro = comportamento de hoje."""
    if modo() != ATIVO:
        if agora is None:
            registrar(norm, ofertas, familia_hoje)
        else:
            registrar(norm, ofertas, familia_hoje, agora)
        return familia_hoje, False
    d, aplicada = _avaliar_e_registrar(norm, ofertas, familia_hoje, agora,
                                       ativo=True, via_lancamento=via_lancamento,
                                       score=score)
    if aplicada:
        return d.post, True
    return familia_hoje, False


def mutexes(norm, ofertas) -> list:
    """[só ATIVO] A trava sintética da campanha, `<plat>|resgate*|<página>`,
    para toda mensagem que pode entrar na regra (página de resgate declarada
    como destino principal e nenhuma âncora estrutural): duas mensagens da
    mesma campanha quase simultâneas passam a ser serializadas e a segunda
    vê o post da primeira. Não é chave real (a espécie `resgate*` não
    existe): entra SÓ no conjunto de locks de identidade de
    publicacao.enviar — nunca em ofertas, família, banco ou decisão.
    SOMBRA: [] (nem os locks mudam). Nunca levanta."""
    try:
        if norm is None or modo() != ATIVO:
            return []
        plat = getattr(norm, "plat", "") or ""
        pagina = pagina_de_resgate(plat, getattr(norm, "ancora_url", "") or "")
        if not pagina or not regra.entra_na_regra(ofertas, True):
            return []
        return [f"{plat}|resgate*|{pagina}"]
    except Exception:                                      # noqa: BLE001
        return []


def _aplicacao(d, familia_hoje: Optional[int], via_lancamento: bool) -> str:
    """Em ATIVO: "sim" quando a JUNTA vira a família; senão o porquê
    ("-" = a decisão não é JUNTA)."""
    if d.acao != regra.JUNTA:
        return "-"
    if familia_hoje is not None:        # nunca troca uma família escolhida
        return "nao:familia_de_hoje"
    if via_lancamento:                  # a regra de cupom já decidiu
        return "nao:regra_de_cupom"
    return "sim"


def _avaliar_e_registrar(norm, ofertas, familia_hoje: Optional[int],
                         agora: Optional[float], *, ativo: bool = False,
                         via_lancamento: bool = False,
                         score: Optional[int] = None):
    """(decisão, aplicada) — (None, False) fora da regra ou com erro. A
    decisão não depende do registro: a linha que falha não muda nada.
    Nunca levanta."""
    try:
        r = (avaliar(norm, ofertas, familia_hoje, agora, score) if ativo
             else avaliar(norm, ofertas, familia_hoje, agora))
        if r is None:
            return None, False
        c, vivos, d = r
        aplicada = _aplicacao(d, familia_hoje, via_lancamento) if ativo else ""
    except Exception as e:                                 # noqa: BLE001
        _erro(e)
        return None, False
    try:
        _linha(norm, c, vivos, d, familia_hoje, aplicada)
    except Exception as e:                                 # noqa: BLE001
        _erro(e)
    return d, aplicada == "sim"


def _erro(e: Exception) -> None:
    # nem o registro do erro pode escapar para a publicação
    with contextlib.suppress(Exception):
        log_out.error(f"❌ [RESGATE_SOMBRA] {type(e).__name__}")


def _linha(norm, c, vivos, d, familia_hoje: Optional[int], aplicada: str) -> None:
    """A linha 🎟 [RESGATE_SOMBRA] (silêncio quando nada mudaria e não há o
    que observar). `aplicada` vazia = SOMBRA: a linha do #34, byte a byte."""
    if (d.acao == regra.MANTER and d.destino_afirmavel == d.destino_hoje
            and not d.outros and d.vinculo != regra.AUSENTE):
        return
    p = next((v for v in vivos if v.msg_id == d.post), None)
    delta = f"{d.delta_s / 60:.1f}min" if d.delta_s is not None else "-"
    hoje = f"post:{familia_hoje}" if familia_hoje is not None else "post_novo"
    proposta = sorted(set(c.chaves) | {regra.chave_resgate(c.plat, c.pagina)})
    nivel = log_out.warning if d.acao == regra.INDETERMINADO else log_out.info
    ativo = f" modo={ATIVO} aplicada={aplicada}" if aplicada else ""
    nivel(
        f"🎟 [RESGATE_SOMBRA] msg=({c.chat},{getattr(norm, 'msg_id', '')})"
        f" acao={d.acao} post:{d.post if d.post is not None else '-'}"
        f" motivo={d.motivo} Δ={delta} pagina={c.pagina} hoje={hoje}"
        f" ident_atual={sorted(c.chaves)} ident_proposta={proposta}"
        f" destino_hoje={d.destino_hoje} destino_elegivel={d.destino_elegivel}"
        f" vinculo={d.vinculo} presenca={d.presenca}"
        f" destino_afirmavel={d.destino_afirmavel} se_copia_no_ar={d.se_copia_no_ar}"
        f" prova={d.prova} aprovacao={'pendente' if d.pendente else '-'}"
        f" nome={_par(c.perfil.nome, p.perfil.nome if p else '')}"
        f" beneficio={_par(_beneficio(c.perfil), _beneficio(p.perfil) if p else '')}"
        f"{ativo} outros={list(d.outros)}")
