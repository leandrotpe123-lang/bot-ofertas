"""
Plataforma Mercado Livre — composição.

Raiz de composição do pacote: reúne as capacidades dos módulos e
monta a instância `PLATAFORMA`, descoberta pelo Auto Discovery no
boot. Não contém lógica.

═══════════════════════════════════════════════════════════════════
MAPA DE RESPONSABILIDADES
═══════════════════════════════════════════════════════════════════
afiliado.py   identidade e validação de atribuição (folha)
links.py      reconhecimento, classificação, elegibilidade — puro
sessao.py     estado e validade da credencial — fonte única
cliente.py    HTTP com o createLink
afiliacao.py  fluxo do caso de uso e cache

Direção de dependência (acíclica, unidirecional):

  __init__ → afiliacao → links    → afiliado
                       → sessao
                       → cliente  → sessao
                                  → afiliado

`links` não conhece rede. `sessao` não conhece HTTP. `cliente` não
conhece cache nem elegibilidade. Nada abaixo de `afiliacao` conhece
o core.

═══════════════════════════════════════════════════════════════════
ESCOPO DESTA FRENTE — PRODUTO E LISTAGEM DIRETA
═══════════════════════════════════════════════════════════════════
Elegível para o createLink — os dois comprovados em teste real
contra o endpoint:

    produto    /p/MLB<díg>, /up/MLBU<díg>, MLB-<díg>-..._JM
    listagem   lista.mercadolivre.com.br/_Container_<slug>
                 ?coupon_campaign_id=<id>

Reconhecido e classificado, mas NÃO enviado ao createLink:

    /social/<slug>              vitrine de afiliado — o SERVIDOR
                                recusa com error_code 111
    /social/<slug>/lists/<uuid> lista de afiliado — idem
    /sec/XXXX                   link curto de afiliado — não converte

A recusa de vitrine e de /sec/ não é escolha de escopo: é o
comportamento do Programa de Afiliados. Esses casos devolvem
AUSENTE e seguem o fallback existente do core.

EM ABERTO, fora deste pacote: a rota /social/<slug> → listagem.
Há indício de que a vitrine carrega um <a> apontando para o
_Container_ correspondente, mas isso ainda não foi confirmado no
HTML SERVIDO — enquanto não for, não vira código.

═══════════════════════════════════════════════════════════════════
AUTENTICAÇÃO
═══════════════════════════════════════════════════════════════════
Credencial de sessão provisionada externamente, em
ML_SESSION_COOKIE e ML_CSRF_TOKEN.

Não há mecanismo oficial, documentado e estável de renovação
automática da sessão do Programa de Afiliados que esteja comprovado
e que possamos usar no Foguetão. O pacote não contém login,
navegador, proxy nem qualquer tentativa de contornar autenticação.
Expiração é detectada, registrada e alertada; a renovação é manual.

═══════════════════════════════════════════════════════════════════
INVARIANTES
═══════════════════════════════════════════════════════════════════
INV-ML-1  `links` é síncrono, puro e sem I/O.
INV-ML-2  Nada abaixo de `afiliacao` conhece cache ou publicação.
INV-ML-3  URL resolvida pelo cache nunca chega à rede.
INV-ML-4  URL inelegível nunca chega ao `cliente`.
INV-ML-5  Nenhum link é publicado sem tag confirmada pelo servidor.
INV-ML-6  Qualquer falha resulta em AUSENTE; nunca link parcial.
INV-ML-7  Credencial recusada não gera retry nem alerta repetido.
INV-ML-8  Cookie, CSRF e segredos nunca aparecem em log.
"""
from __future__ import annotations

from plataformas.contrato import CONTRACT_VERSION, Plataforma

from .afiliacao import afilia
from .afiliado import IDENTIFICADOR
from .links import (
    ENCURTADORES,
    ENCURTADORES_FORCA_GET,
    extrai_identidade,
    reconhece,
)


PLATAFORMA = Plataforma(
    identificador=IDENTIFICADOR,
    versao_contrato=CONTRACT_VERSION,
    reconhece=reconhece,
    extrai_identidade=extrai_identidade,
    afilia=afilia,
    encurtadores=ENCURTADORES,
    encurtadores_forca_get=ENCURTADORES_FORCA_GET,
)


# Superfície pública: o caminho idiomático é sempre via registry.
__all__ = ["PLATAFORMA"]
