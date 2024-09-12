# dialects/tibero/dictionary.py
# Copyright (C) 2005-2024 the Tibero authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors
from sqlalchemy import Table, MetaData, table, Column, CHAR, VARCHAR

from .types import DATE
from .types import LONG
from .types import NUMBER
from .types import RAW
from .types import VARCHAR2

# constants
DB_LINK_PLACEHOLDER = "__$sa_dblink$__"
# tables
dual = table("dual")
dictionary_meta = MetaData()

# TODO 아래 Note의 내용이 티베로 데이터베이스에서도 적용되는지 확인하기
# NOTE: all the dictionary_meta are aliases because oracle does not like
# using the full table@dblink for every column in query, and complains with
# ORA-00960: ambiguous column naming in select list
all_tables = Table(
    "all_tables" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("table_name", VARCHAR(128), nullable=False),
    Column("tablespace_name", VARCHAR(128)),
    Column("pct_free", NUMBER),
    Column("ini_trans", NUMBER),
    Column("logging", VARCHAR(3)),
    Column("num_rows", NUMBER),
    Column("blocks", NUMBER),
    Column("avg_row_len", NUMBER),
    Column("degree", NUMBER),
    Column("sample_size", NUMBER),
    Column("last_analyzed", DATE),
    Column("partitioned", VARCHAR(3)),
    Column("buffer_pool", VARCHAR(7)),
    Column("row_movement", VARCHAR(8)),
    Column("duration", VARCHAR(11)),
    Column("compression", VARCHAR(3)),
    Column("compress_for", VARCHAR(12)),
    Column("dropped", VARCHAR(3)),
    Column("read_only", VARCHAR(3)),
    Column("temporary", VARCHAR(3)),
    Column("max_extents", NUMBER),
    Column("iot_type", VARCHAR(12)),
    Column("initial_extent", NUMBER),
    Column("next_extent", NUMBER),
    Column("min_extents", NUMBER),
    Column("is_virtual", VARCHAR(1)),
    Column("inmemory", VARCHAR(8)),
    Column("inmemory_priority", VARCHAR(8)),
    Column("inmemory_distribute", VARCHAR(15)),
    Column("inmemory_compression", VARCHAR(14)),
    Column("inmemory_duplicate", VARCHAR(13)),
).alias("a_tables")

all_views = Table(
    "all_views" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("view_name", VARCHAR(128), nullable=False),
    Column("text", LONG),
).alias("a_views")


all_sequences = Table(
    "all_sequences" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("sequence_owner", VARCHAR(128), nullable=False),
    Column("sequence_name", VARCHAR(128), nullable=False),
    Column("min_value", NUMBER),
    Column("max_value", NUMBER),
    Column("increment_by", NUMBER, nullable=False),
    Column("cycle_flag", VARCHAR(1)),
    Column("order_flag", VARCHAR(1)),
    Column("if_avail", VARCHAR(1)),
    Column("cache_size", NUMBER, nullable=False),
    Column("last_number", NUMBER, nullable=False),
    Column("session_flag", VARCHAR(1)),
    Column("scale_flag", VARCHAR(1)),
    Column("extend_flag", VARCHAR(1)),

).alias("a_sequences")

all_users = Table(
    "all_users" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("username", VARCHAR(128), nullable=False),
    Column("user_id", NUMBER, nullable=False),
    Column("created", DATE, nullable=False),
).alias("a_users")

all_mviews = Table(
    "all_mviews" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("mview_name", VARCHAR(128), nullable=False),
    Column("container_name", VARCHAR(128), nullable=False),
    Column("query", LONG),
    Column("query_len", NUMBER),
    Column("updatable", CHAR(1)),
    Column("update_log", VARCHAR(0)),
    Column("master_rollback_seg", VARCHAR(0)),
    Column("master_link", VARCHAR(0)),
    Column("rewrite_enabled", VARCHAR(1)),
    Column("rewrite_capability", CHAR(7)),
    Column("refresh_mode", VARCHAR(6)),
    Column("refresh_method", VARCHAR(1)),
    Column("build_mode", VARCHAR(9)),
    Column("fast_refreshable", VARCHAR(3)),
    Column("last_refresh_type", VARCHAR(8)),
    Column("last_refresh_date", DATE),
    Column("last_refresh_end_time", DATE),
    Column("staleness", VARCHAR(9)),
    Column("after_fast_refresh", CHAR(2)),
    Column("unknown_prebuilt", CHAR(1)),
    Column("unknown_plsql_func", CHAR(1)),
    Column("unknown_external_table", CHAR(1)),
    Column("unknown_consider_fresh", CHAR(1)),
    Column("unknown_import", CHAR(1)),
    Column("unknown_trusted_fd", CHAR(1)),
    Column("compile_state", CHAR(5)),
    Column("use_no_index", CHAR(1)),
    Column("stale_since", VARCHAR(0)),
    Column("interval", VARCHAR(2000)),
    Column("reduced_precision", VARCHAR(1)),
    Column("refresh_key", VARCHAR(11)),
).alias("a_mviews")

all_tab_identity_cols = Table(
    "all_tab_identity_cols" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("table_name", VARCHAR(128), nullable=False),
    Column("column_name", VARCHAR(128), nullable=False),
    Column("generation_type", VARCHAR(18)),
    Column("sequence_name", VARCHAR(128), nullable=False),
    Column("identity_options", VARCHAR(65532)),
).alias("a_tab_identity_cols")

all_tab_cols = Table(
    "all_tab_cols" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("table_name", VARCHAR(128), nullable=False),
    Column("column_name", VARCHAR(128), nullable=False),
    Column("data_type", VARCHAR(128)),
    Column("data_type_owner", VARCHAR(128)),
    Column("data_length", NUMBER, nullable=False),
    Column("data_precision", NUMBER),
    Column("data_scale", NUMBER),
    Column("nullable", VARCHAR(1)),
    Column("column_id", NUMBER),
    Column("data_default", LONG),
    Column("default_length", NUMBER),
    Column("num_nulls", NUMBER),
    Column("char_col_decl_length", NUMBER),
    Column("char_length", NUMBER),
    Column("char_used", VARCHAR(1)),
    Column("hidden_column", VARCHAR(1)),
    Column("virtual_column", VARCHAR(1)),
    Column("segment_column_id", NUMBER),
    Column("internal_column_id", NUMBER, nullable=False),
    Column("qualified_col_name", VARCHAR(4000)),
    Column("OBJ_ID", NUMBER),
).alias("a_tab_cols")

all_tab_comments = Table(
    "all_tab_comments" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("table_name", VARCHAR(128), nullable=False),
    Column("table_type", VARCHAR(9)),
    Column("comments", VARCHAR(4000)),
).alias("a_tab_comments")

all_col_comments = Table(
    "all_col_comments" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("table_name", VARCHAR(128), nullable=False),
    Column("column_name", VARCHAR(128), nullable=False),
    Column("comments", VARCHAR(4000)),
).alias("a_col_comments")

all_mview_comments = Table(
    "all_mview_comments" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("mview_name", VARCHAR(128), nullable=False),
    Column("comments", VARCHAR(4000)),
).alias("a_mview_comments")

all_ind_columns = Table(
    "all_ind_columns" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("index_owner", VARCHAR(128), nullable=False),
    Column("index_name", VARCHAR(128), nullable=False),
    Column("table_owner", VARCHAR(128), nullable=False),
    Column("table_name", VARCHAR(128), nullable=False),
    Column("column_name", VARCHAR(128)),
    Column("column_position", NUMBER, nullable=False),
    Column("column_length", NUMBER, nullable=False),
    Column("descend", VARCHAR(4)),
).alias("a_ind_columns")

all_indexes = Table(
    "all_indexes" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("index_name", VARCHAR(128), nullable=False),
    Column("index_type", VARCHAR(26)),
    Column("table_owner", VARCHAR(128), nullable=False),
    Column("table_name", VARCHAR(128), nullable=False),
    Column("table_type", CHAR(9)),
    Column("uniqueness", VARCHAR(9)),
    Column("compression", VARCHAR(8)),
    Column("prefix_length", NUMBER),
    Column("tablespace_name", VARCHAR(128)),
    Column("ini_trans", NUMBER),
    Column("pct_free", NUMBER),
    Column("initial_extent", NUMBER),
    Column("logging", VARCHAR(3)),
    Column("blevel", NUMBER),
    Column("leaf_blocks", NUMBER),
    Column("distinct_keys", NUMBER),
    Column("clustering_factor", NUMBER),
    Column("status", VARCHAR(8)),
    Column("num_rows", NUMBER),
    Column("last_analyzed", DATE),
    Column("partitioned", VARCHAR(3)),
    Column("buffer_pool", VARCHAR(7)),
    Column("generated_by_system", VARCHAR(1)),  # Tibero만 이 칼럼을 가지고 있습니다.
    Column("referential", VARCHAR(3)),          # Tibero만 이 칼럼을 가지고 있습니다.
    Column("max_extents", NUMBER),
    Column("include_column", NUMBER),
    Column("min_extents", NUMBER),
    Column("pct_threshold", NUMBER),
    Column("visibility", VARCHAR(9)),
    Column("ityp_owner", VARCHAR(30)),
    Column("ityp_name", VARCHAR(30)),
    Column("parameters", VARCHAR(1000)),
    Column("domidx_status", VARCHAR(3)),
    Column("domidx_opstatus", VARCHAR(12)),
    Column("domidx_management", VARCHAR(14)),
).alias("a_indexes")

all_ind_expressions = Table(
    "all_ind_expressions" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("index_owner", VARCHAR(128), nullable=False),
    Column("index_name", VARCHAR(128), nullable=False),
    Column("table_owner", VARCHAR(128), nullable=False),
    Column("table_name", VARCHAR(128), nullable=False),
    Column("column_position", NUMBER, nullable=False),
    Column("column_expression", LONG),
).alias("a_ind_expressions")

all_constraints = Table(
    "all_constraints" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128)),
    Column("constraint_name", VARCHAR(128)),
    Column("con_type", VARCHAR(22)),            # Tibero만 이 칼럼을 가지고 있습니다. constraint_type을 쉽게 보기 위한 column
    Column("constraint_type", VARCHAR(1)),
    Column("table_name", VARCHAR(128)),
    Column("search_condition", VARCHAR(65532)),
    Column("r_owner", VARCHAR(128)),
    Column("r_constraint_name", VARCHAR(128)),
    Column("delete_rule", VARCHAR(9)),
    Column("status", VARCHAR(8)),
    Column("deferrable", VARCHAR(14)),
    Column("deferred", VARCHAR(9)),
    Column("index_owner", VARCHAR(128)),
    Column("index_name", VARCHAR(128)),
).alias("a_constraints")

all_cons_columns = Table(
    "all_cons_columns" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("constraint_name", VARCHAR(128), nullable=False),
    Column("table_name", VARCHAR(128), nullable=False),
    Column("column_name", VARCHAR(128)),
    Column("position", NUMBER),
).alias("a_cons_columns")

# TODO figure out if it's still relevant, since there is no mention from here
# https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/ALL_DB_LINKS.html
# original note:
# using user_db_links here since all_db_links appears
# to have more restricted permissions.
# https://docs.oracle.com/cd/B28359_01/server.111/b28310/ds_admin005.htm
# will need to hear from more users if we are doing
# the right thing here.  See [ticket:2619]
all_db_links = Table(
    "all_db_links" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("db_link", VARCHAR(128), nullable=False),
    Column("username", VARCHAR(128)),
    Column("host", VARCHAR(128)),
    Column("created", DATE, nullable=False),
).alias("a_db_links")

all_synonyms = Table(
    "all_synonyms" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128)),
    Column("synonym_name", VARCHAR(128)),
    Column("org_object_owner", VARCHAR(128)),
    Column("org_object_name", VARCHAR(257)),
).alias("a_synonyms")

all_objects = Table(
    "all_objects" + DB_LINK_PLACEHOLDER,
    dictionary_meta,
    Column("owner", VARCHAR(128), nullable=False),
    Column("object_name", VARCHAR(128), nullable=False),
    Column("subobject_name", VARCHAR(128)),
    Column("object_id", NUMBER, nullable=False),
    Column("object_type", VARCHAR(23)),
    Column("object_type_no", NUMBER, nullable=False),
    Column("created", DATE, nullable=False),
    Column("last_ddl_time", DATE, nullable=False),
    Column("timestamp", VARCHAR(19)),
    Column("status", VARCHAR(7)),
    Column("temporary", VARCHAR(1)),
).alias("a_objects")
