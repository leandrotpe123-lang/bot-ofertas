"""Camada 7 — Banco de dados SQLite. Toda persistência passa por aqui."""
#
# Implementação: database_conexao (conexão única, mutex, schema),
# database_links (links_cache + short_links), database_posts
# (post_estado + oferta_index + origem_post), database_cupons
# (cupom_idx) e database_manutencao (retenção e diagnóstico).
#
# Este arquivo é FACHADA: sem lógica, sem SQL, sem estado.
#
# Reexporta as 20 FUNÇÕES. _db_conn e _db_lock NÃO são reexportados:
# nenhum consumidor os acessa, e reexportá-los seria bug — o import
# liga por VALOR e capturaria o None anterior a _init_db().
from __future__ import annotations

from database_conexao import _db, _fechar_db, _init_db
from database_cupons import (db_cupom_idx_buscar,
                             db_cupom_idx_registrar,
                             db_cupom_idx_registrar_inedito)
from database_links import (
    db_get_link,
    db_get_short,
    db_set_link,
    db_set_short,
)
from database_manutencao import _db_count_links, db_limpar
from database_posts import (
    db_absorver_ofertas,
    db_get_post,
    db_ofertas_de_post,
    db_origem_get,
    db_origem_set,
    db_overlap_posts,
    db_registrar_post,
    db_remover_post,
)
