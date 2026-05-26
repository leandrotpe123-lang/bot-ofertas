"""
Package plataformas — Catálogo Vivo de Plugins de Plataforma.

Este __init__.py é o gatilho do mecanismo de Auto Discovery da casa.
Ao ser carregado pela primeira vez (no boot do processo), varre o
próprio diretório da package em busca de módulos que exponham uma
instância PLATAFORMA do contrato e os registra automaticamente no
catálogo do registry.

═══════════════════════════════════════════════════════════════════
PRINCÍPIO INEGOCIÁVEL
═══════════════════════════════════════════════════════════════════
Adicionar uma plataforma nova deve exigir EXCLUSIVAMENTE:
  1) criar um arquivo dentro de plataformas/;
  2) declarar a constante PLATAFORMA (instância de Plataforma);
  3) obedecer o contrato existente.

Nada além disso. Sem edição em registry, em main, em bootstrap, em
pipeline, em classificação, em utils ou em qualquer outro ponto do
core. Este é o critério de Zero-Touch Extension definido pelos MBs
da casa.

═══════════════════════════════════════════════════════════════════
REGRA ESTRUTURAL PERMANENTE
═══════════════════════════════════════════════════════════════════
Módulos fora desta package nunca devem importar uma plataforma
concreta pelo nome. O único caminho idiomático para consumir uma
plataforma é via registry:

    from plataformas import registry
    plataforma = registry.resolver(url)             # por URL
    plataforma = registry.acessar(identificador)    # por identificador

Capacidades agregadas são compostas iterando
registry.plataformas_registradas() e consultando cada plataforma
pelo seu identificador nominal. Qualquer import direto do tipo
`from plataformas.amazon import ...` em código fora desta package
deve ser tratado como vazamento arquitetural.

═══════════════════════════════════════════════════════════════════
DESCOBERTA AUTOMÁTICA — REGRAS
═══════════════════════════════════════════════════════════════════
Candidato a plugin é qualquer arquivo .py na package que:
  - não comece por '_' (convenção de privado/interno);
  - não seja 'registry.py' nem 'contrato.py'.

Plugin é qualquer candidato cujo módulo, uma vez importado, expõe
um atributo PLATAFORMA que é instância de Plataforma. Módulos
candidatos que não sejam plugin são silenciosamente ignorados
(podem ser helpers internos da package).

═══════════════════════════════════════════════════════════════════
POLÍTICA DE ERRO
═══════════════════════════════════════════════════════════════════
REGISTRY_ENV=dev (padrão):
  qualquer falha de import ou de cadastro interrompe o boot com
  erro explícito identificando o plugin culpado. Defeito de
  contrato deve ser visível.

REGISTRY_ENV=prod:
  falhas são logadas e isoladas; a descoberta prossegue com os
  demais plugins. Preserva continuidade operacional ao custo de
  silenciar defeitos.

═══════════════════════════════════════════════════════════════════
DETERMINISMO
═══════════════════════════════════════════════════════════════════
A ordem de registro é alfabética por nome de módulo, reprodutível
entre sistemas operacionais. Como o contrato exige reconhecimento
mutuamente exclusivo entre plataformas (4ª invariante do contrato),
a ordem é semanticamente irrelevante — mas a previsibilidade do log
e do teste vale o custo zero.

═══════════════════════════════════════════════════════════════════
EXECUÇÃO
═══════════════════════════════════════════════════════════════════
A descoberta acontece exatamente uma vez por processo, no primeiro
import desta package. Python cacheia __init__.py — não há
reexecução. Para testes, o trabalho real está em _descobrir_plugins,
que recebe path e nome de pacote como parâmetros e pode ser
invocada com diretórios mock.

═══════════════════════════════════════════════════════════════════
API PÚBLICA DA PACKAGE
═══════════════════════════════════════════════════════════════════
Apenas dois símbolos são considerados públicos para consumidores
externos:
  - registry  : o catálogo
  - contrato  : os tipos do contrato (uso por anotações de tipo)
"""
from __future__ import annotations

import importlib
import os
import pkgutil
from typing import List, Tuple

from logger import log_sys

from plataformas import registry
from plataformas.contrato import Plataforma


# ── Módulos estruturais que NÃO são plugins ───────────────────────
# Excluídos por nome explícito. Se forem renomeados ou movidos para
# um subpacote, esta lista precisa acompanhar. A alternativa de
# isolar o core da package em plataformas/_core/ é desejável no
# futuro mas não bloqueia esta fundação.
_MODULOS_ESTRUTURAIS = frozenset({"registry", "contrato"})


def _eh_candidato_a_plugin(nome_modulo: str) -> bool:
    """
    Verdadeiro se o nome do módulo é candidato a plugin: arquivo .py
    público (não começa por '_') e não é módulo estrutural da package.
    """
    if nome_modulo.startswith("_"):
        return False
    if nome_modulo in _MODULOS_ESTRUTURAIS:
        return False
    return True


def _descobrir_plugins(
    pacote_path,
    pacote_nome: str,
    modo_producao: bool,
) -> Tuple[List[str], List[str], List[Tuple[str, str]]]:
    """
    Varre o diretório do pacote e tenta registrar cada módulo
    candidato como plugin.

    Devolve a tripla (registrados, ignorados, falhos):
      - registrados : identificadores efetivamente cadastrados no
                      registry, em ordem alfabética;
      - ignorados   : nomes de módulos candidatos importados que
                      não expõem PLATAFORMA válida — não são
                      plugins (podem ser helpers internos);
      - falhos      : pares (nome_modulo, motivo) para módulos
                      cujo import ou cadastro foi recusado.

    Em modo desenvolvimento, qualquer falha re-lança a exceção
    original após o log, interrompendo o boot. Em modo produção, a
    falha é apenas reportada e a descoberta prossegue com os
    demais plugins.

    A função é parametrizada para ser testável com diretórios mock
    e instâncias de registry simuladas — o __init__.py invoca-a
    apenas com os defaults reais.
    """
    registrados: List[str] = []
    ignorados: List[str] = []
    falhos: List[Tuple[str, str]] = []

    # Coleta primeiro, ordena depois — determinismo entre sistemas.
    nomes_candidatos = sorted(
        nome
        for _, nome, ehpkg in pkgutil.iter_modules(pacote_path)
        if not ehpkg and _eh_candidato_a_plugin(nome)
    )

    for nome in nomes_candidatos:
        caminho = f"{pacote_nome}.{nome}"

        # 1) Tentativa de import — falha aqui significa código quebrado
        #    no plugin (sintaxe, dependência ausente, exceção em
        #    top-level). É erro estrutural do plugin.
        try:
            modulo = importlib.import_module(caminho)
        except Exception as exc:
            motivo = f"import falhou: {type(exc).__name__}: {exc}"
            falhos.append((nome, motivo))
            log_sys.error(f"❌ Plugin '{nome}' — {motivo}")
            if not modo_producao:
                raise
            continue

        # 2) Verificação de conformidade — o módulo expõe PLATAFORMA?
        #    Se não, não é plugin. Pode ser helper interno da package
        #    (cenário legítimo). Log em debug, não warning.
        plataforma_obj = getattr(modulo, "PLATAFORMA", None)
        if not isinstance(plataforma_obj, Plataforma):
            ignorados.append(nome)
            log_sys.debug(
                f"⏭ Módulo '{nome}' sem PLATAFORMA válida — ignorado"
            )
            continue

        # 3) Cadastro — falha aqui é defeito de contrato detectado
        #    pelo registry (versão incompatível, identificador
        #    duplicado, capacidade obrigatória ausente).
        try:
            registry.cadastrar(plataforma_obj)
        except Exception as exc:
            motivo = f"cadastro recusado: {type(exc).__name__}: {exc}"
            falhos.append((nome, motivo))
            log_sys.error(f"❌ Plugin '{nome}' — {motivo}")
            if not modo_producao:
                raise
            continue

        registrados.append(plataforma_obj.identificador)

    return registrados, ignorados, falhos


# ── Execução da descoberta ────────────────────────────────────────
# Roda exatamente uma vez por processo, no primeiro import da
# package. Python cacheia __init__.py — não há reexecução.

_MODO_PRODUCAO = (
    os.environ.get("REGISTRY_ENV", "dev").lower() == "prod"
)

_registrados, _ignorados, _falhos = _descobrir_plugins(
    pacote_path=__path__,
    pacote_nome=__name__,
    modo_producao=_MODO_PRODUCAO,
)

log_sys.info(
    f"🧩 Auto Discovery | registrados={len(_registrados)} "
    f"ignorados={len(_ignorados)} falhos={len(_falhos)} "
    f"plataformas={_registrados}"
)


# ── API pública da package ────────────────────────────────────────
# Plataformas concretas NÃO são reexportadas. O caminho idiomático
# para acessá-las é sempre via registry.resolver / registry.acessar.
__all__ = ["registry", "contrato"]
  
