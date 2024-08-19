from sqlalchemy.sql import sqltypes
from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements

class Requirements(SuiteRequirements):
    @property
    def deferrable_or_no_constraints(self):
        """Target database must support deferrable constraints."""

        return exclusions.open()

    @property
    def check_constraints(self):
        """Target database must support check constraints."""

        return exclusions.open()

    @property
    def enforces_check_constraints(self):
        """Target database must also enforce check constraints."""

        return exclusions.open()

    @property
    def named_constraints(self):
        """target database must support names for constraints."""

        return exclusions.open()

    @property
    def implicitly_named_constraints(self):
        """target database must apply names to unnamed constraints."""

        return exclusions.open()

    @property
    def foreign_keys(self):
        """Target database must support foreign keys."""

        return exclusions.open()

    @property
    def foreign_keys_reflect_as_index(self):
        return exclusions.closed()

    @property
    def unique_index_reflect_as_unique_constraints(self):
        return exclusions.closed()

    @property
    def unique_constraints_reflect_as_index(self):
        return exclusions.open()

    @property
    def foreign_key_constraint_name_reflection(self):
        return exclusions.open()

    @property
    def table_ddl_if_exists(self):
        """target platform supports IF NOT EXISTS / IF EXISTS for tables."""

        return exclusions.closed()

    @property
    def index_ddl_if_exists(self):
        """target platform supports IF NOT EXISTS / IF EXISTS for indexes."""

        return exclusions.closed()

    @property
    def on_update_cascade(self):
        """target database must support ON UPDATE..CASCADE behavior in
        foreign keys."""

        return exclusions.closed()

    @property
    def non_updating_cascade(self):
        """target database must *not* support ON UPDATE..CASCADE behavior in
        foreign keys."""

        return exclusions.open()

    @property
    def recursive_fk_cascade(self):
        """target database must support ON DELETE CASCADE on a self-referential
        foreign key"""

        return exclusions.open()

    @property
    def deferrable_fks(self):
        """target database must support deferrable fks"""

        return exclusions.open()

    @property
    def foreign_key_constraint_option_reflection_ondelete(self):
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_ondelete_restrict(self):
        return exclusions.closed()

    @property
    def fk_constraint_option_reflection_ondelete_noaction(self):
        return exclusions.closed()

    @property
    def foreign_key_constraint_option_reflection_onupdate(self):
        return exclusions.closed()

    @property
    def fk_constraint_option_reflection_onupdate_restrict(self):
        return exclusions.closed()

    @property
    def comment_reflection(self):
        return exclusions.open()

    @property
    def comment_reflection_full_unicode(self):
        return exclusions.open()

    @property
    def constraint_comment_reflection(self):
        return exclusions.closed()

    @property
    def unbounded_varchar(self):
        """Target database must support VARCHAR with no length"""

        return exclusions.closed()

    @property
    def boolean_col_expressions(self):
        """Target database must support boolean expressions as columns"""
        # TODO: 티베로에서 제공되는 기능인지 확인하기, 일단 테스트를 하도록 설정했습니다.
        return exclusions.open()

    @property
    def non_native_boolean_unconstrained(self):
        """target database is not native boolean and allows arbitrary integers
        in it's "bool" column"""

        return exclusions.open()

    @property
    def qmark_paramstyle(self):
        return exclusions.open()

    @property
    def named_paramstyle(self):
        return exclusions.closed()

    @property
    def format_paramstyle(self):
        return exclusions.closed()

    @property
    def pyformat_paramstyle(self):
        return exclusions.closed()

    @property
    def no_quoting_special_bind_names(self):
        """Target database will quote bound parameter names, doesn't support
        EXPANDING"""

        return exclusions.closed()

    @property
    def temporary_tables(self):
        """target database supports temporary tables"""
        return exclusions.open()

    @property
    def temp_table_reflection(self):
        return self.temporary_tables

    @property
    def temp_table_reflect_indexes(self):
        return exclusions.open()

    @property
    def reflectable_autoincrement(self):
        """Target database must support tables that can automatically generate
        PKs assuming they were reflected.

        this is essentially all the DBs in "identity" plus PostgreSQL, which
        has SERIAL support.  Oracle requires the Sequence
        to be explicitly added, including if the table was reflected.
        """
        return exclusions.closed()

    @property
    def non_broken_binary(self):
        """target DBAPI must work fully with binary values"""

        return exclusions.open()

    @property
    def binary_comparisons(self):
        """target database/driver can allow BLOB/BINARY fields to be compared
        against a bound parameter value.
        """
        return exclusions.closed()

    @property
    def binary_literals(self):
        """target backend supports simple binary literals, e.g. an
        expression like::

            SELECT CAST('foo' AS BINARY)

        Where ``BINARY`` is the type emitted from :class:`.LargeBinary`,
        e.g. it could be ``BLOB`` or similar.

        Basically fails on Oracle.

        """
        return exclusions.closed()

    @property
    def tuple_in(self):
        # TODO: 티베로에서 제공하는 기능인지 확인하기
        return exclusions.open()

    @property
    def tuple_in_w_empty(self):
        # TODO: 티베로에서 제공하는 기능인지 확인하기
        return exclusions.open()

    @property
    def independent_cursors(self):
        """Target must support simultaneous, independent database cursors
        on a single connection."""

        # TODO: 티베로 pyodbc에서 제공되는 기능인지 확인하기
        return exclusions.open()

    @property
    def cursor_works_post_rollback(self):
        """Driver quirk where the cursor.fetchall() will work even if
        the connection has been rolled back.

        This generally refers to buffered cursors but also seems to work
        with cx_oracle, for example.

        """

        # TODO: 티베로 pyodbc에서 제공되는 기능인지 확인하기
        return exclusions.open()

    @property
    def select_star_mixed(self):
        r"""target supports expressions like "SELECT x, y, \*, z FROM table"

        apparently MySQL / MariaDB, Oracle doesn't handle this.

        We only need a few backends so just cover SQLite / PG

        """
        return exclusions.closed()

    @property
    def independent_connections(self):
        """
        Target must support simultaneous, independent database connections.
        """

        return exclusions.open()

    @property
    def independent_readonly_connections(self):
        """
        Target must support simultaneous, independent database connections
        that will be used in a readonly fashion.

        """
        return exclusions.open()

    @property
    def predictable_gc(self):
        """target platform must remove all cycles unconditionally when
        gc.collect() is called, as well as clean out unreferenced subclasses.

        """
        return exclusions.closed()

    @property
    def memory_process_intensive(self):
        """Driver is able to handle the memory tests which run in a subprocess
        and iterate through hundreds of connections

        """
        return exclusions.closed()

    @property
    def updateable_autoincrement_pks(self):
        """Target must support UPDATE on autoincrement/integer primary key."""

        return exclusions.open()

    @property
    def isolation_level(self):
        return exclusions.open()

    @property
    def autocommit(self):
        """target dialect supports 'AUTOCOMMIT' as an isolation_level"""

        return exclusions.open()

    @property
    def row_triggers(self):
        """Target must support standard statement-running EACH ROW triggers."""

        return exclusions.closed()

    @property
    def sequences_as_server_defaults(self):
        """Target database must support SEQUENCE as a server side default."""

        return exclusions.open()

    @property
    def sql_expressions_inserted_as_primary_key(self):
        return exclusions.open()

    # 아마 안될텐데 일단 테스트
    @property
    def computed_columns_on_update_returning(self):
        return exclusions.open()

    # todo : 다른 returning 테스트 해보고
    @property
    def returning_star(self):
        """backend supports ``RETURNING *``"""

        return exclusions.close()

    @property
    def correlated_outer_joins(self):
        """Target must support an outer join to a subquery which
        correlates to the parent."""

        return exclusions.open()

    # 아마 안될텐데 되는지 혹시 몰라서 해봄 update 되면 해보기로
    @property
    def multi_table_update(self):
        return exclusions.closed()

    @property
    def update_from(self):
        """Target must support UPDATE..FROM syntax"""

        return exclusions.open()

    # 아마 안될텐데 되는지 혹시 몰라서 해봄 update 되면 해보기로
    @property
    def update_from_using_alias(self):
        """Target must support UPDATE..FROM syntax against an alias"""

        return exclusions.closed()

    @property
    def delete_using(self):
        """Target must support DELETE FROM..FROM or DELETE..USING syntax"""
        return exclusions.open()

    @property
    def delete_using_alias(self):
        """Target must support DELETE FROM against an alias"""
        return exclusions.open()

    @property
    def update_where_target_in_subquery(self):
        """Target must support UPDATE (or DELETE) where the same table is
        present in a subquery in the WHERE clause.

        This is an ANSI-standard syntax that apparently MySQL can't handle,
        such as::

            UPDATE documents SET flag=1 WHERE documents.title IN
                (SELECT max(documents.title) AS title
                    FROM documents GROUP BY documents.user_id
                )

        """
        return exclusions.fails("not supported")

    # 테스트가 없음
    @property
    def savepoints(self):
        """Target database must support savepoints."""

        return exclusions.closed()

    @property
    def compat_savepoints(self):
        """Target database must support savepoints, or a compat
        recipe e.g. for sqlite will be used"""

        return exclusions.open()

    @property
    def savepoints_w_release(self):
        return exclusions.open()

    @property
    def schemas(self):
        """Target database must support external schemas, and have one
        named 'test_schema'."""

        return exclusions.open()


    @property
    def schema_create_delete(self):
        """target database supports schema create and dropped with
        'CREATE SCHEMA' and 'DROP SCHEMA'"""
        return exclusions.closed()

    @property
    def cross_schema_fk_reflection(self):
        """target system must support reflection of inter-schema foreign
        keys"""
        return exclusions.closed()

    @property
    def implicit_default_schema(self):
        """target system has a strong concept of 'default' schema that can
        be referred to implicitly.

        basically, PostgreSQL.

        TODO: what does this mean?  all the backends have a "default"
        schema

        """
        return exclusions.closed()

    @property
    def default_schema_name_switch(self):
        return exclusions.open()

    @property
    def unique_constraint_reflection(self):
        return exclusions.fails("not supported")

    @property
    def unique_constraint_reflection_no_index_overlap(self):
        return exclusions.closed()

    @property
    def check_constraint_reflection(self):
        return exclusions.open()

    @property
    def indexes_with_expressions(self):
        return exclusions.open()

    @property
    def reflect_indexes_with_expressions(self):
        return exclusions.open()

    @property
    def reflect_indexes_with_ascdesc_as_expression(self):
        return exclusions.open()

    @property
    def temp_table_names(self):
        """target dialect supports listing of temporary table names"""

        return exclusions.open()


    @property
    def has_temp_table(self):
        """target dialect supports checking a single temp table name

        unfortunately this is not the same as temp_table_names

        """

        # SQLite file db "works", but there's some kind of issue when
        # run in the full test suite that causes it not to work
        return exclusions.open()

    @property
    def temporary_views(self):
        """target database supports temporary views"""
        return exclusions.closed()


    @property
    def table_value_constructor(self):
        return exclusions.closed()

    @property
    def update_nowait(self):
        """Target database must support SELECT...FOR UPDATE NOWAIT"""
        return exclusions.open()


    @property
    def subqueries(self):
        """Target database must support subqueries."""
        return exclusions.open()

    @property
    def ctes(self):
        """Target database supports CTEs"""
        return exclusions.closed()


    # oracle은 cte 구문이 지원은 되나 update delete는 지원되지 않는다고 한다. tibero는 테스트 결과 지원이 되는 구문이긴 하므로 일단 테스트
    @property
    def ctes_with_update_delete(self):
        """target database supports CTES that ride on top of a normal UPDATE
        or DELETE statement which refers to the CTE in a correlated subquery.

        """
        return exclusions.open()


    @property
    def ctes_on_dml(self):
        """target database supports CTES which consist of INSERT, UPDATE
        or DELETE *within* the CTE, e.g. WITH x AS (UPDATE....)"""

        return exclusions.open()

    @property
    def mod_operator_as_percent_sign(self):
        """target database must use a plain percent '%' as the 'modulus'
        operator."""

        return exclusions.open()

    @property
    def intersect(self):
        """Target database must support INTERSECT or equivalent."""

        return exclusions.open()


    @property
    def except_(self):
        """Target database must support EXCEPT or equivalent (i.e. MINUS)."""
        return exclusions.open()

    @property
    def dupe_order_by_ok(self):
        """target db won't choke if ORDER BY specifies the same expression
        more than once

        """

        return exclusions.open()

    @property
    def order_by_col_from_union(self):
        """target database supports ordering by a column from a SELECT
        inside of a UNION

        E.g.  (SELECT id, ...) UNION (SELECT id, ...) ORDER BY id

        Fails on SQL Server and oracle.

        Previously on Oracle, prior to #8221, the ROW_NUMBER subquerying
        applied to queries allowed the test at
        suite/test_select.py ->
        CompoundSelectTest.test_limit_offset_selectable_in_unions
        to pass, because of the implicit subquerying thus creating a query
        that was more in line with the syntax
        illustrated at
        https://stackoverflow.com/a/6036814/34549.  However, Oracle doesn't
        support the above (SELECT ..) UNION (SELECT ..) ORDER BY syntax
        at all.  So those tests are now not supported w/ Oracle as of
        #8221.

        """
        return exclusions.open()

    @property
    def parens_in_union_contained_select_w_limit_offset(self):
        """Target database must support parenthesized SELECT in UNION
        when LIMIT/OFFSET is specifically present.

        E.g. (SELECT ... LIMIT ..) UNION (SELECT .. OFFSET ..)

        This is known to fail on SQLite.

        """
        return exclusions.open()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        """Target database must support parenthesized SELECT in UNION
        when OFFSET/LIMIT is specifically not present.

        E.g. (SELECT ...) UNION (SELECT ..)

        This is known to fail on SQLite.  It also fails on Oracle
        because without LIMIT/OFFSET, there is currently no step that
        creates an additional subquery.

        """
        return exclusions.fails("fails on Tibero because without LIMIT/OFFSET")

    @property
    def sql_expression_limit_offset(self):
        return exclusions.open()

    @property
    def window_functions(self):
        return exclusions.open()

    @property
    def two_phase_transactions(self):
        """Target database must support two-phase transactions."""
        return exclusions.open()

    @property
    def two_phase_recovery(self):
        return exclusions.closed("recovery not functional")

    @property
    def views(self):
        """Target database must support VIEWs."""
        return exclusions.open()

    @property
    def empty_strings_varchar(self):
        """
        target database can persist/return an empty string with a varchar.
        """

        return exclusions.fails("tibero converts empty strings to a blank space")

    @property
    def empty_strings_text(self):
        """target database can persist/return an empty string with an
        unbounded text."""

        return exclusions.fails("tibero converts empty strings to a blank space")

    @property
    def string_type_isnt_subtype(self):
        """target dialect does not have a dialect-specific subtype for String.

        This is used for a special type expression test which wants to
        test the compiler with a subclass of String, where we don't want
        the dialect changing that type when we grab the 'impl'.

        """

        def go(config):
            return (
                sqltypes.String().dialect_impl(config.db.dialect).__class__
                is sqltypes.String
            )

        return exclusions.only_if(go)

    @property
    def empty_inserts_executemany(self):
        return self.empty_inserts

    @property
    def provisioned_upsert(self):
        """backend includes upsert() in its provisioning.py"""
        return exclusions.closed()

    @property
    def expressions_against_unbounded_text(self):
        """target database supports use of an unbounded textual field in a
        WHERE clause."""

        return exclusions.fails("inconsistent datatypes: expected - got CLOB")

    @property
    def unicode_connections(self):
        """
        Target driver must support some encoding of Unicode across the wire.

        """
        return exclusions.open()

    @property
    def unicode_data_no_special_types(self):
        """Target database/dialect can receive / deliver / compare data with
        non-ASCII characters in plain VARCHAR, TEXT columns, without the need
        for special "national" datatypes like NVARCHAR or similar.

        """
        return exclusions.open()

    @property
    def unicode_ddl(self):
        """Target driver must support some degree of non-ascii symbol names."""

        return exclusions.open()

    @property
    def symbol_names_w_double_quote(self):
        """Target driver can create tables with a name like 'some " table'"""

        return exclusions.fails("Tibero and Oracle do not support tables with a name like 'some \" table'")

    @property
    def arraysize(self):
        return exclusions.open()

    @property
    def emulated_lastrowid(self):
        """target dialect retrieves cursor.lastrowid or an equivalent
        after an insert() construct executes.
        """
        return exclusions.fails()

    @property
    def database_discards_null_for_autoincrement(self):
        """target database autoincrements a primary key and populates
        .lastrowid even if NULL is explicitly passed for the column.

        """
        return exclusions.succeeds_if(
            lambda config: (
                config.db.dialect.insert_null_pk_still_autoincrements
            )
        )

    @property
    def emulated_lastrowid_even_with_sequences(self):
        """target dialect retrieves cursor.lastrowid or an equivalent
        after an insert() construct executes, even if the table has a
        Sequence on it.
        """
        return exclusions.fails_on_everything_except(
            "mysql",
            "mariadb",
            "sqlite+pysqlite",
            "sqlite+pysqlcipher",
        )

    @property
    def dbapi_lastrowid(self):
        """target backend includes a 'lastrowid' accessor on the DBAPI
        cursor object.

        """
        return exclusions.skip_if("mssql+pymssql", "crashes on pymssql") + exclusions.only_on(
            [
                "mysql",
                "mariadb",
                "sqlite+pysqlite",
                "sqlite+aiosqlite",
                "sqlite+pysqlcipher",
                "mssql",
            ]
        )

    @property
    def nullsordering(self):
        """Target backends that support nulls ordering."""
        return exclusions.open()

    @property
    def reflects_pk_names(self):
        """Target driver reflects the name of primary key constraints."""

        return exclusions.open()

    @property
    def nested_aggregates(self):
        """target database can select an aggregate from a subquery that's
        also using an aggregate"""

        return exclusions.closed()

    @property
    def tuple_valued_builtin_functions(self):
        return exclusions.closed()

    @property
    def array_type(self):
        return exclusions.closed()

    @property
    def json_type(self):
        return exclusions.closed()

    @property
    def json_index_supplementary_unicode_element(self):
        # TODO: json관련 코드인데 제가 (전영배) 알기로는 오라클 dialect는 아직 json을
        #       지원안하는 것으로 알고 있습니다. 그런데 원본 코드에서는 마치 오라클도 같이
        #       테스트해야하는 것처럼 되어 있습니다.
        return exclusions.open()

    @property
    def legacy_unconditional_json_extract(self):
        """Backend has a JSON_EXTRACT or similar function that returns a
        valid JSON string in all cases.

        Used to test a legacy feature and is not needed.

        """
        return self.json_type

    @property
    def sqlite_memory(self):
        return exclusions.closed()


    @property
    def sqlite_partial_indexes(self):
        return exclusions.closed()

    @property
    def reflects_json_type(self):
        return exclusions.closed()

    @property
    def json_array_indexes(self):
        return self.json_type

    @property
    def datetime_interval(self):
        """target dialect supports rendering of a datetime.timedelta as a
        literal string, e.g. via the TypeEngine.literal_processor() method.
        Added for Oracle and Postgresql as of now.
        """
        return exclusions.open()

    @property
    def datetime_literals(self):
        """target dialect supports rendering of a date, time, or datetime as a
        literal string, e.g. via the TypeEngine.literal_processor() method.

        """
        return exclusions.open()

    @property
    def datetime(self):
        """target dialect supports representation of Python
        datetime.datetime() objects."""

        return exclusions.open()

    @property
    def date_implicit_bound(self):
        """target dialect when given a date object will bind it such
        that the database server knows the object is a date, and not
        a plain string.

        """

        # mariadbconnector works.  pyodbc we dont know, not supported in
        # testing.
        # TODO: 티베로에서는 어떻게 동작하는지 테스트해보고 결정해봐야 할것 같습니다.
        return exclusions.open()

    @property
    def time_implicit_bound(self):
        """target dialect when given a time object will bind it such
        that the database server knows the object is a time, and not
        a plain string.

        """

        # this may have worked with mariadbconnector at some point, but
        # this now seems to not be the case.   Since no other mysql driver
        # supports these tests, that's fine
        # TODO: 티베로에서는 어떻게 동작하는지 테스트해보고 결정해봐야 할것 같습니다.
        return exclusions.open()

    @property
    def datetime_implicit_bound(self):
        """target dialect when given a datetime object will bind it such
        that the database server knows the object is a date, and not
        a plain string.

        """

        # mariadbconnector works.  pyodbc we dont know, not supported in
        # testing.
        # TODO: 티베로에서는 어떻게 동작하는지 테스트해보고 결정해봐야 할것 같습니다.
        return exclusions.open()

    @property
    def datetime_timezone(self):
        return exclusions.closed()

    @property
    def time_timezone(self):
        return exclusions.closed()

    @property
    def datetime_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects."""

        return exclusions.closed()

    @property
    def timestamp_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects but only
        if TIMESTAMP is used."""

        return exclusions.open()

    @property
    def timestamp_microseconds_implicit_bound(self):
        # TODO: 테스트 코드 확인하고 왜 실패해야 정상인지 확인하기
        return self.timestamp_microseconds + exclusions.fails()

    @property
    def datetime_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1900) values."""

        return exclusions.closed()

    @property
    def date(self):
        """target dialect supports representation of Python
        datetime.date() objects."""

        return exclusions.open()

    @property
    def date_coerces_from_datetime(self):
        """target dialect accepts a datetime object as the target
        of a date column."""

        # does not work as of pyodbc 4.0.22
        # TODO: tibero와 pyodbc 5.x 에서는 어떻게 동작하는지 확인이 필요합니다.
        return exclusions.open()

    @property
    def date_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1900) values."""

        return exclusions.closed()

    @property
    def time(self):
        """target dialect supports representation of Python
        datetime.time() objects."""

        return exclusions.closed()

    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.closed()

    @property
    def precision_numerics_general(self):
        """target backend has general support for moderately high-precision
        numerics."""
        return exclusions.open()

    @property
    def precision_numerics_enotation_small(self):
        """target backend supports Decimal() objects using E notation
        to represent very small values."""
        # NOTE: this exclusion isn't used in current tests.
        return exclusions.open()

    @property
    def precision_numerics_many_significant_digits(self):
        """target backend supports values with many digits on both sides,
        such as 319438950232418390.273596, 87673.594069654243

        """

        return exclusions.open()

    @property
    def cast_precision_numerics_many_significant_digits(self):
        """same as precision_numerics_many_significant_digits but within the
        context of a CAST statement (hello MySQL)

        """
        return self.precision_numerics_many_significant_digits

    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        # TODO: 오라클은 드라이버가 지원안한다고 하나 티베로도 같은지 확인해봐야 합니다.
        return exclusions.open()

    @property
    def numeric_received_as_decimal_untyped(self):
        return exclusions.open()

    @property
    def literal_float_coercion(self):
        return exclusions.open()

    @property
    def infinity_floats(self):
        return exclusions.fails("Tibero does not support infinity")

    @property
    def float_or_double_precision_behaves_generically(self):
        return exclusions.closed()

    @property
    def precision_generic_float_type(self):
        """target backend will return native floating point numbers with at
        least seven decimal places when using the generic Float type."""

        return exclusions.open()

    @property
    def implicit_decimal_binds(self):
        """target backend will return a selected Decimal as a Decimal, not
        a string.

        e.g.::

            expr = decimal.Decimal("15.7563")

            value = e.scalar(
                select(literal(expr))
            )

            assert value == expr

        See :ticket:`4036`

        """

        return exclusions.open()

    @property
    def fetch_null_from_numeric(self):
        return exclusions.open()

    @property
    def float_is_numeric(self):
        return exclusions.fails_if(["tibero"])

    @property
    def duplicate_key_raises_integrity_error(self):
        return exclusions.open()

    @property
    def hstore(self):
        return exclusions.closed()

    @property
    def citext(self):
        return exclusions.closed()

    @property
    def btree_gist(self):
        return exclusions.closed()

    @property
    def range_types(self):
        return exclusions.closed()

    @property
    def multirange_types(self):
        return exclusions.closed()

    @property
    def async_dialect(self):
        """dialect makes use of await_() to invoke operations on the DBAPI."""

        return self.asyncio + exclusions.only_on(
            exclusions.LambdaPredicate(
                lambda config: config.db.dialect.is_async,
                "Async dialect required",
            )
        )

    def _has_tibero_test_dblink(self, key):
        def check(config):
            assert config.db.dialect.name == "tibero"
            name = config.file_config.get("sqla_testing", key)
            if not name:
                return False
            with config.db.connect() as conn:
                links = config.db.dialect._list_dblinks(conn)
                return config.db.dialect.normalize_name(name) in links

        return exclusions.only_on(["tibero"]) + exclusions.only_if(
            check,
            f"{key} option not specified in config or dblink not found in db",
        )

    @property
    def oracle_test_dblink(self):
        #TODO: oracle_test_dblink propery는 dialect/oracle/test_reflection.py에서 사용됩니다.
        #      tibero용 테스트를 만들때 사용할 이름을 oracle_test_dblink에서 tibero_test_dblink로 바꿔주시리길 바랍니다.
        return self._has_tibero_test_dblink("oracle_db_link")

    @property
    def oracle_test_dblink2(self):
        #TODO: oracle_test_dblink propery는 dialect/oracle/test_reflection.py에서 사용됩니다.
        #      tibero용 테스트를 만들때 사용할 이름을 oracle_test_dblink2에서 tibero_test_dblink2로 바꿔주시리길 바랍니다.
        return self._has_tibero_test_dblink("oracle_db_link2")

    @property
    def postgresql_test_dblink(self):
        return exclusions.closed()

    @property
    def postgresql_jsonb(self):
        return exclusions.closed()

    @property
    def native_hstore(self):
        return exclusions.closed()

    @property
    def psycopg2_compatibility(self):
        return exclusions.closed()

    @property
    def any_psycopg_compatibility(self):
        return exclusions.closed()

    @property
    def psycopg_only_compatibility(self):
        return exclusions.closed()

    @property
    def psycopg_or_pg8000_compatibility(self):
        return exclusions.closed()

    @property
    def percent_schema_names(self):
        return exclusions.open()

    @property
    def order_by_label_with_expression(self):
        return exclusions.open()

    def get_order_by_collation(self, config):
        raise NotImplementedError()

    @property
    def skip_mysql_on_windows(self):
        """Catchall for a large variety of MySQL on Windows failures"""

        return exclusions.closed()

    @property
    def mssql_freetds(self):
        return exclusions.closed()

    @property
    def ad_hoc_engines(self):
        return exclusions.closed()

    @property
    def no_asyncio(self):
        def go(config):
            return config.db.dialect.is_async

        return exclusions.skip_if(go)

    @property
    def no_mssql_freetds(self):
        return exclusions.closed()

    @property
    def pyodbc_fast_executemany(self):
        # TODO: 원래 코드를 보면 mssql pyodbc에만 테스트하라고하는 것 같지만 pyodbc관련 기능인 것 같아 일단
        #       티베로에서도 테스트하도록 만들었습니다.
        return exclusions.open()

    @property
    def selectone(self):
        """target driver must support the literal statement 'select 1'"""
        return exclusions.closed()

    @property
    def mysql_for_update(self):
        return exclusions.closed()

    @property
    def mysql_fsp(self):
        return exclusions.closed()

    @property
    def mysql_notnull_generated_columns(self):
        return exclusions.closed()

    @property
    def mysql_fully_case_sensitive(self):
        return exclusions.closed()

    @property
    def mysql_zero_date(self):
        return exclusions.closed()

    @property
    def mysql_non_strict(self):
        return exclusions.closed()

    @property
    def mysql_ngram_fulltext(self):
        return exclusions.closed()

    def _mysql_80(self, config):
        return exclusions.closed()

    def _mariadb_102(self, config):
        return exclusions.closed()

    def _mariadb_105(self, config):
        return exclusions.closed()

    def _mysql_and_check_constraints_exist(self, config):
        # 1. we have mysql / mariadb and
        # 2. it enforces check constraints
        return exclusions.closed()

    def _mysql_check_constraints_exist(self, config):
        # 1. we dont have mysql / mariadb or
        # 2. we have mysql / mariadb that enforces check constraints
        return exclusions.closed()

    def _mysql_check_constraints_dont_exist(self, config):
        # 1. we have mysql / mariadb and
        # 2. they dont enforce check constraints
        return exclusions.closed()

    def _mysql_not_mariadb_102(self, config):
        return exclusions.closed()

    def _mysql_not_mariadb_103(self, config):
        return exclusions.closed()

    def _mysql_not_mariadb_103_not_mysql8031(self, config):
        return exclusions.closed()

    def _mysql_not_mariadb_104(self, config):
        return exclusions.closed()

    def _mysql_not_mariadb_104_not_mysql8031(self, config):
        return exclusions.closed()

    def _has_mysql_on_windows(self, config):
        return exclusions.closed()

    def _has_mysql_fully_case_sensitive(self, config):
        return exclusions.closed()

    @property
    def postgresql_utf8_server_encoding(self):
        return exclusions.closed()

    @property
    def cxoracle6_or_greater(self):
        return exclusions.skip_if("tibero", "tibero does not use support cxoracle features")

    # @property
    # def fail_on_oracledb_thin(self):
    #     def go(config):
    #         if against(config, "oracle+oracledb"):
    #             with config.db.connect() as conn:
    #                 return config.db.dialect.is_thin_mode(conn)
    #         return False
    #
    #     return fails_if(go)
    #
    @property
    def computed_columns(self):
        return exclusions.open()

    @property
    def python_profiling_backend(self):
        return exclusions.closed()

    @property
    def computed_columns_stored(self):
        # TODO: oracle과 tibero는 stored/virtual column을 지원하는 것으로 알고있습니다. 확인이 필요합니다.
        return exclusions.closed()

    @property
    def computed_columns_virtual(self):
        # TODO: oracle과 tibero는 stored/virtual column을 지원하는 것으로 알고있습니다. 확인이 필요합니다.
        return exclusions.open()

    @property
    def computed_columns_default_persisted(self):
        # TODO: oracle과 tibero는 stored/virtual column을 지원하는 것으로 알고있습니다. 확인이 필요합니다.
        return exclusions.closed()

    @property
    def computed_columns_reflect_persisted(self):
        # TODO: oracle과 tibero는 stored/virtual column을 지원하는 것으로 알고있습니다. 확인이 필요합니다.
        return exclusions.closed()

    @property
    def regexp_match(self):
        return exclusions.open()

    @property
    def regexp_replace(self):
        return exclusions.open()

    @property
    def supports_distinct_on(self):
        """If a backend supports the DISTINCT ON in a select"""
        return exclusions.closed()

    @property
    def supports_for_update_of(self):
        # TODO: 티베로에서 제공하는 기능인지 확인하기
        return exclusions.open()

    @property
    def sequences_in_other_clauses(self):
        """sequences allowed in WHERE, GROUP BY, HAVING, etc."""
        return exclusions.closed()

    @property
    def supports_lastrowid_for_expressions(self):
        """cursor.lastrowid works if an explicit SQL expression was used."""
        return exclusions.closed()

    @property
    def supports_sequence_for_autoincrement_column(self):
        """for mssql, autoincrement means IDENTITY, not sequence"""
        # TODO: 티베로에서 제공하는 기능인지 확인 필요.
        return exclusions.open()

    @property
    def supports_autoincrement_w_composite_pk(self):
        """integer autoincrement works for tables with composite primary
        keys"""
        # TODO: 티베로에서 제공하는 기능인지 확인 필요.
        return exclusions.open()

    @property
    def identity_columns(self):
        # TODO: 티베로에서 제공하는 기능인지 확인 필요.
        return exclusions.open()

    @property
    def multiple_identity_columns(self):
        return exclusions.closed()

    @property
    def identity_columns_standard(self):
        return self.identity_columns

    @property
    def index_reflects_included_columns(self):
        return exclusions.closed()

    @property
    def fetch_first(self):
        # TODO: 티베로에서 제공하는 기능인지 확인 필요.
        return exclusions.open()

    @property
    def fetch_percent(self):
        # TODO: 티베로에서 제공하는 기능인지 확인 필요.
        return exclusions.open()

    @property
    def fetch_ties(self):
        # TODO: 티베로에서 제공하는 기능인지 확인 필요.
        return exclusions.open()

    @property
    def fetch_no_order_by(self):
        # TODO: 티베로에서 제공하는 기능인지 확인 필요.
        return exclusions.open()

    @property
    def fetch_offset_with_options(self):
        # use together with fetch_first
        # TODO: 티베로에서 제공하는 기능인지 확인 필요.
        return exclusions.open()

    @property
    def fetch_expression(self):
        # use together with fetch_first
        # TODO: 티베로에서 제공하는 기능인지 확인 필요.
        return exclusions.open()

    @property
    def autoincrement_without_sequence(self):
        return exclusions.closed()

    @property
    def reflect_tables_no_columns(self):
        # so far sqlite, mariadb, mysql don't support this
        return exclusions.closed()

    @property
    def json_deserializer_binary(self):
        "indicates if the json_deserializer function is called with bytes"
        return exclusions.closed()

    @property
    def mssql_filestream(self):
        "returns if mssql supports filestream"
        return exclusions.closed()

    @property
    def reflect_table_options(self):
        return exclusions.open()

    @property
    def materialized_views(self):
        """Target database must support MATERIALIZED VIEWs."""
        return exclusions.open()

    @property
    def materialized_views_reflect_pk(self):
        return exclusions.open()

    @property
    def uuid_data_type(self):
        """Return databases that support the UUID datatype."""
        return exclusions.closed()

    @property
    def has_json_each(self):
        return exclusions.closed()

    @property
    def rowcount_always_cached(self):
        """Indicates that ``cursor.rowcount`` is always accessed,
        usually in an ``ExecutionContext.post_exec``.
        """
        return exclusions.closed()

    @property
    def rowcount_always_cached_on_insert(self):
        """Indicates that ``cursor.rowcount`` is always accessed in an insert
        statement.
        """
        return exclusions.closed()
