# tibero/base.py

"""
Support for the TmaxData Tibero database.
"""

from collections import defaultdict
from functools import lru_cache
from functools import wraps
import re


from sqlalchemy import util
from sqlalchemy import exc
from sqlalchemy import sql
from sqlalchemy import schema as sa_schema

from sqlalchemy import Computed

from sqlalchemy.engine import default
from sqlalchemy.engine import reflection
from sqlalchemy.engine import ObjectKind
from sqlalchemy.engine import ObjectScope
from sqlalchemy.engine.reflection import ReflectionDefaults

from sqlalchemy.sql import sqltypes
from sqlalchemy.sql import compiler
from sqlalchemy.sql import expression
from sqlalchemy.sql import visitors
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import select
from sqlalchemy.sql import bindparam
from sqlalchemy.sql import and_
from sqlalchemy.sql import or_
from sqlalchemy.sql import func
from sqlalchemy.sql import null
from sqlalchemy.sql.visitors import InternalTraversal

from sqlalchemy.types import INTEGER
from sqlalchemy.types import DOUBLE_PRECISION
from sqlalchemy.types import REAL


from . import types, dictionary

# TODO: 여기 있는 모든 키워드가 티베로에서 지원되는지 확인하기
#       지원안되는 게 몇 개 있더라도 뺴면 안됩니다. 나중에 지원될 수 있기 때문입니다.
RESERVED_WORDS = set(
    "SHARE RAW DROP BETWEEN FROM DESC OPTION PRIOR LONG THEN "
    "DEFAULT ALTER IS INTO MINUS INTEGER NUMBER GRANT IDENTIFIED "
    "ALL TO ORDER ON FLOAT DATE HAVING CLUSTER NOWAIT RESOURCE "
    "ANY TABLE INDEX FOR UPDATE WHERE CHECK SMALLINT WITH DELETE "
    "BY ASC REVOKE LIKE SIZE RENAME NOCOMPRESS NULL GROUP VALUES "
    "AS IN VIEW EXCLUSIVE COMPRESS SYNONYM SELECT INSERT EXISTS "
    "NOT TRIGGER ELSE CREATE INTERSECT PCTFREE DISTINCT USER "
    "CONNECT SET MODE OF UNIQUE VARCHAR2 VARCHAR LOCK OR CHAR "
    "DECIMAL UNION PUBLIC AND START UID COMMENT CURRENT LEVEL".split()
)

NO_ARG_FNS = set(
    "UID CURRENT_DATE SYSDATE USER CURRENT_TIME CURRENT_TIMESTAMP".split()
)

colspecs = {
    sqltypes.Boolean: types._TiberoBoolean,
    sqltypes.Interval: types.INTERVAL,
    sqltypes.DateTime: types.DATE,
    sqltypes.Date: types._TiberoDate,
}

# TODO: 여기 있는 모든 타입들이 티베로에서 지원되는지 확인하기
# Oracle에서 VARCHAR 는 자동으로 VARCHAR2로 변환이 되는데
# Tibero에서는 VARCHAR2가 자동으로 VARCHAR로 변환이 됩니다.
# Oracle에서 NVARCHAR 라는 타입은 없으나 NVARCHAR2 라는
# 타입을 가지고 있습니다. Tibero에서는 VARCHAR2가 자동으로
# VARCHAR로 변환이 됩니다.
ischema_names = {
    "VARCHAR": sqltypes.VARCHAR,
    "NVARCHAR": sqltypes.NVARCHAR,
    "CHAR": sqltypes.CHAR,
    "NCHAR": sqltypes.NCHAR,
    "DATE": types.DATE,
    "NUMBER": types.NUMBER,
    "BLOB": sqltypes.BLOB,
    "BFILE": types.BFILE,
    "CLOB": sqltypes.CLOB,
    "NCLOB": types.NCLOB,
    "TIMESTAMP": types.TIMESTAMP,
    "TIMESTAMP WITH TIME ZONE": types.TIMESTAMP,
    "TIMESTAMP WITH LOCAL TIME ZONE": types.TIMESTAMP,
    "INTERVAL DAY TO SECOND": types.INTERVAL,
    "RAW": types.RAW,
    "FLOAT": types.FLOAT,
    "DOUBLE PRECISION": sqltypes.DOUBLE_PRECISION,
    "REAL": sqltypes.REAL,
    "LONG": types.LONG,
    "BINARY_DOUBLE": types.BINARY_DOUBLE,
    "BINARY_FLOAT": types.BINARY_FLOAT,
    "ROWID": types.ROWID,
}


class TiberoTypeCompiler(compiler.GenericTypeCompiler):
    # Note:
    # Tibero DATE == DATETIME
    # Tibero does not allow milliseconds in DATE
    # Tibero does not support TIME columns

    def visit_datetime(self, type_, **kw):
        return self.visit_DATE(type_, **kw)

    def visit_float(self, type_, **kw):
        return self.visit_FLOAT(type_, **kw)

    def visit_double(self, type_, **kw):
        return self.visit_DOUBLE_PRECISION(type_, **kw)

    def visit_unicode(self, type_, **kw):
        if self.dialect._use_nchar_for_unicode:
            return self.visit_NVARCHAR2(type_, **kw)
        else:
            return self.visit_VARCHAR2(type_, **kw)

    def visit_INTERVAL(self, type_, **kw):
        return "INTERVAL DAY%s TO SECOND%s" % (
            type_.day_precision is not None
            and "(%d)" % type_.day_precision
            or "",
            type_.second_precision is not None
            and "(%d)" % type_.second_precision
            or "",
        )

    def visit_LONG(self, type_, **kw):
        return "LONG"

    def visit_TIMESTAMP(self, type_, **kw):
        if getattr(type_, "local_timezone", False):
            return "TIMESTAMP WITH LOCAL TIME ZONE"
        elif type_.timezone:
            return "TIMESTAMP WITH TIME ZONE"
        else:
            return "TIMESTAMP"

    def visit_DOUBLE_PRECISION(self, type_, **kw):
        return self._generate_numeric(type_, "DOUBLE PRECISION", **kw)

    def visit_BINARY_DOUBLE(self, type_, **kw):
        return self._generate_numeric(type_, "BINARY_DOUBLE", **kw)

    def visit_BINARY_FLOAT(self, type_, **kw):
        return self._generate_numeric(type_, "BINARY_FLOAT", **kw)

    def visit_FLOAT(self, type_, **kw):
        kw["_requires_binary_precision"] = True
        return self._generate_numeric(type_, "FLOAT", **kw)

    def visit_NUMBER(self, type_, **kw):
        return self._generate_numeric(type_, "NUMBER", **kw)

    def _generate_numeric(
            self,
            type_,
            name,
            precision=None,
            scale=None,
            _requires_binary_precision=False,
            **kw,
    ):
        if precision is None:
            precision = getattr(type_, "precision", None)

        if _requires_binary_precision:
            binary_precision = getattr(type_, "binary_precision", None)

            if precision and binary_precision is None:
                # https://www.oracletutorial.com/oracle-basics/oracle-float/
                estimated_binary_precision = int(precision / 0.30103)
                raise exc.ArgumentError(
                    "Tibero FLOAT types use 'binary precision', which does "
                    "not convert cleanly from decimal 'precision'.  Please "
                    "specify "
                    f"this type with a separate Tibero variant, such as "
                    f"{type_.__class__.__name__}(precision={precision})."
                    f"with_variant(tibero.FLOAT"
                    f"(binary_precision="
                    f"{estimated_binary_precision}), 'tibero'), so that the "
                    "Tibero specific 'binary_precision' may be specified "
                    "accurately."
                )
            else:
                precision = binary_precision

        if scale is None:
            scale = getattr(type_, "scale", None)

        if precision is None:
            return name
        elif scale is None:
            n = "%(name)s(%(precision)s)"
            return n % {"name": name, "precision": precision}
        else:
            n = "%(name)s(%(precision)s, %(scale)s)"
            return n % {"name": name, "precision": precision, "scale": scale}

    def visit_string(self, type_, **kw):
        return self.visit_VARCHAR2(type_, **kw)

    def visit_VARCHAR2(self, type_, **kw):
        return self._visit_varchar(type_, "", "2")

    def visit_NVARCHAR2(self, type_, **kw):
        return self._visit_varchar(type_, "N", "2")

    visit_NVARCHAR = visit_NVARCHAR2

    def visit_VARCHAR(self, type_, **kw):
        return self._visit_varchar(type_, "", "")

    def _visit_varchar(self, type_, n, num):
        if not type_.length:
            return "%(n)sVARCHAR%(two)s" % {"two": num, "n": n}
        elif not n and self.dialect._supports_char_length:
            varchar = "VARCHAR%(two)s(%(length)s CHAR)"
            return varchar % {"length": type_.length, "two": num}
        else:
            varchar = "%(n)sVARCHAR%(two)s(%(length)s)"
            return varchar % {"length": type_.length, "two": num, "n": n}

    def visit_text(self, type_, **kw):
        return self.visit_CLOB(type_, **kw)

    def visit_unicode_text(self, type_, **kw):
        if self.dialect._use_nchar_for_unicode:
            return self.visit_NCLOB(type_, **kw)
        else:
            return self.visit_CLOB(type_, **kw)

    def visit_large_binary(self, type_, **kw):
        return self.visit_BLOB(type_, **kw)

    def visit_big_integer(self, type_, **kw):
        return self.visit_NUMBER(type_, precision=19, **kw)

    def visit_boolean(self, type_, **kw):
        return self.visit_SMALLINT(type_, **kw)

    def visit_RAW(self, type_, **kw):
        if type_.length:
            return "RAW(%(length)s)" % {"length": type_.length}
        else:
            return "RAW"

    def visit_ROWID(self, type_, **kw):
        return "ROWID"


class TiberoCompiler(compiler.SQLCompiler):
    """Tibero compiler modifies the lexical structure of Select
    statements to work under non-ANSI configured Tibero databases, if
    the use_ansi flag is False.
    """

    compound_keywords = util.update_copy(
        compiler.SQLCompiler.compound_keywords,
        {expression.CompoundSelect.EXCEPT: "MINUS"},
    )

    def __init__(self, *args, **kwargs):
        self.__wheres = {}
        super().__init__(*args, **kwargs)

    def visit_mod_binary(self, binary, operator, **kw):
        return "mod(%s, %s)" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw),
        )

    def visit_now_func(self, fn, **kw):
        return "CURRENT_TIMESTAMP"

    def visit_char_length_func(self, fn, **kw):
        return "LENGTH" + self.function_argspec(fn, **kw)

    def visit_match_op_binary(self, binary, operator, **kw):
        return "CONTAINS (%s, %s)" % (
            self.process(binary.left),
            self.process(binary.right),
        )

    def visit_true(self, expr, **kw):
        return "1"

    def visit_false(self, expr, **kw):
        return "0"

    def get_cte_preamble(self, recursive):
        return "WITH"

    def get_select_hint_text(self, byfroms):
        return " ".join("/*+ %s */" % text for table, text in byfroms.items())

    def function_argspec(self, fn, **kw):
        if len(fn.clauses) > 0 or fn.name.upper() not in NO_ARG_FNS:
            return compiler.SQLCompiler.function_argspec(self, fn, **kw)
        else:
            return ""

    def visit_function(self, func, **kw):
        text = super().visit_function(func, **kw)
        if kw.get("asfrom", False):
            text = "TABLE (%s)" % text
        return text

    def visit_table_valued_column(self, element, **kw):
        text = super().visit_table_valued_column(element, **kw)
        text = text + ".COLUMN_VALUE"
        return text

    def default_from(self):
        """Called when a ``SELECT`` statement has no froms,
        and no ``FROM`` clause is to be appended.

        The Tibero compiler tacks a "FROM DUAL" to the statement.
        """

        return " FROM DUAL"

    def visit_join(self, join, from_linter=None, **kwargs):
        if self.dialect.use_ansi:
            return compiler.SQLCompiler.visit_join(
                self, join, from_linter=from_linter, **kwargs
            )
        else:
            if from_linter:
                from_linter.edges.add((join.left, join.right))

            kwargs["asfrom"] = True
            if isinstance(join.right, expression.FromGrouping):
                right = join.right.element
            else:
                right = join.right
            return (
                    self.process(join.left, from_linter=from_linter, **kwargs)
                    + ", "
                    + self.process(right, from_linter=from_linter, **kwargs)
            )

    def _get_nonansi_join_whereclause(self, froms):
        clauses = []

        def visit_join(join):
            if join.isouter:
                # https://docs.oracle.com/database/121/SQLRF/queries006.htm#SQLRF52354
                # "apply the outer join operator (+) to all columns of B in
                # the join condition in the WHERE clause" - that is,
                # unconditionally regardless of operator or the other side
                def visit_binary(binary):
                    if isinstance(
                            binary.left, expression.ColumnClause
                    ) and join.right.is_derived_from(binary.left.table):
                        binary.left = _OuterJoinColumn(binary.left)
                    elif isinstance(
                            binary.right, expression.ColumnClause
                    ) and join.right.is_derived_from(binary.right.table):
                        binary.right = _OuterJoinColumn(binary.right)

                clauses.append(
                    visitors.cloned_traverse(
                        join.onclause, {}, {"binary": visit_binary}
                    )
                )
            else:
                clauses.append(join.onclause)

            for j in join.left, join.right:
                if isinstance(j, expression.Join):
                    visit_join(j)
                elif isinstance(j, expression.FromGrouping):
                    visit_join(j.element)

        for f in froms:
            if isinstance(f, expression.Join):
                visit_join(f)

        if not clauses:
            return None
        else:
            return sql.and_(*clauses)

    def visit_outer_join_column(self, vc, **kw):
        return self.process(vc.column, **kw) + "(+)"

    def visit_sequence(self, seq, **kw):
        return self.preparer.format_sequence(seq) + ".nextval"

    def get_render_as_alias_suffix(self, alias_name_text):
        """Tibero doesn't like ``FROM table AS alias``"""

        return " " + alias_name_text

    def returning_clause(
            self, stmt, returning_cols, *, populate_result_map, **kw
    ):
        columns = []
        binds = []

        for i, column in enumerate(
                expression._select_iterables(returning_cols)
        ):
            if (
                    self.isupdate
                    and isinstance(column, sa_schema.Column)
                    and isinstance(column.server_default, Computed)
                    and not self.dialect._supports_update_returning_computed_cols
            ):
                #  TODO: Tibero도 지원안되는지 확인해보기
                util.warn(
                    "Computed columns don't work with Tibero UPDATE "
                    "statements that use RETURNING; the value of the column "
                    "*before* the UPDATE takes place is returned.   It is "
                    "advised to not use RETURNING with an Tibero computed "
                    "column.  Consider setting implicit_returning to False on "
                    "the Table object in order to avoid implicit RETURNING "
                    "clauses from being generated for this Table."
                )
            if column.type._has_column_expression:
                col_expr = column.type.column_expression(column)
            else:
                col_expr = column

            outparam = sql.outparam("ret_%d" % i, type_=column.type)
            self.binds[outparam.key] = outparam
            binds.append(
                self.bindparam_string(self._truncate_bindparam(outparam))
            )

            # has_out_parameters would in a normal case be set to True
            # as a result of the compiler visiting an outparam() object.
            # in this case, the above outparam() objects are not being
            # visited.   Ensure the statement itself didn't have other
            # outparam() objects independently.
            # technically, this could be supported, but as it would be
            # a very strange use case without a clear rationale, disallow it
            if self.has_out_parameters:
                raise exc.InvalidRequestError(
                    "Using explicit outparam() objects with "
                    "UpdateBase.returning() in the same Core DML statement "
                    "is not supported in the Tibero dialect."
                )

            self._tibero_returning = True

            columns.append(self.process(col_expr, within_columns_clause=False))
            if populate_result_map:
                self._add_to_result_map(
                    getattr(col_expr, "name", col_expr._anon_name_label),
                    getattr(col_expr, "name", col_expr._anon_name_label),
                    (
                        column,
                        getattr(column, "name", None),
                        getattr(column, "key", None),
                    ),
                    column.type,
                )

        return "RETURNING " + ", ".join(columns) + " INTO " + ", ".join(binds)

    def _row_limit_clause(self, select, **kw):
        """Tibero7 supports OFFSET/FETCH operators
        Use it instead subquery with row_number
        """

        if (
                select._fetch_clause is not None
                or not self.dialect._supports_offset_fetch
        ):
            return super()._row_limit_clause(
                select, use_literal_execute_for_simple_int=True, **kw
            )
        else:
            return self.fetch_clause(
                select,
                fetch_clause=self._get_limit_or_fetch(select),
                use_literal_execute_for_simple_int=True,
                **kw,
            )

    def _get_limit_or_fetch(self, select):
        if select._fetch_clause is None:
            return select._limit_clause
        else:
            return select._fetch_clause

    def translate_select_structure(self, select_stmt, **kwargs):
        select = select_stmt

        if not getattr(select, "_tibero_visit", None):
            if not self.dialect.use_ansi:
                froms = self._display_froms_for_select(
                    select, kwargs.get("asfrom", False)
                )
                whereclause = self._get_nonansi_join_whereclause(froms)
                if whereclause is not None:
                    select = select.where(whereclause)
                    select._tibero_visit = True

            # if fetch is used this is not needed
            if (
                    select._has_row_limiting_clause
                    and not self.dialect._supports_offset_fetch
                    and select._fetch_clause is None
            ):
                limit_clause = select._limit_clause
                offset_clause = select._offset_clause

                if select._simple_int_clause(limit_clause):
                    limit_clause = limit_clause.render_literal_execute()

                if select._simple_int_clause(offset_clause):
                    offset_clause = offset_clause.render_literal_execute()

                # currently using form at:
                # https://blogs.oracle.com/oraclemagazine/\
                # on-rownum-and-limiting-results

                orig_select = select
                select = select._generate()
                select._tibero_visit = True

                # add expressions to accommodate FOR UPDATE OF
                for_update = select._for_update_arg
                if for_update is not None and for_update.of:
                    for_update = for_update._clone()
                    for_update._copy_internals()

                    for elem in for_update.of:
                        if not select.selected_columns.contains_column(elem):
                            select = select.add_columns(elem)

                # Wrap the middle select and add the hint
                inner_subquery = select.alias()
                limitselect = sql.select(
                    *[
                        c
                        for c in inner_subquery.c
                        if orig_select.selected_columns.corresponding_column(c)
                           is not None
                    ]
                )

                if (
                        limit_clause is not None
                        and self.dialect.optimize_limits
                        and select._simple_int_clause(limit_clause)
                ):
                    limitselect = limitselect.prefix_with(
                        expression.text(
                            "/*+ FIRST_ROWS(%s) */"
                            % self.process(limit_clause, **kwargs)
                        )
                    )

                limitselect._tibero_visit = True
                limitselect._is_wrapper = True

                # add expressions to accommodate FOR UPDATE OF
                if for_update is not None and for_update.of:
                    adapter = sql_util.ClauseAdapter(inner_subquery)
                    for_update.of = [
                        adapter.traverse(elem) for elem in for_update.of
                    ]

                # If needed, add the limiting clause
                if limit_clause is not None:
                    if select._simple_int_clause(limit_clause) and (
                            offset_clause is None
                            or select._simple_int_clause(offset_clause)
                    ):
                        max_row = limit_clause

                        if offset_clause is not None:
                            max_row = max_row + offset_clause

                    else:
                        max_row = limit_clause

                        if offset_clause is not None:
                            max_row = max_row + offset_clause
                    limitselect = limitselect.where(
                        sql.literal_column("ROWNUM") <= max_row
                    )

                # If needed, add the ora_rn, and wrap again with offset.
                if offset_clause is None:
                    limitselect._for_update_arg = for_update
                    select = limitselect
                else:
                    limitselect = limitselect.add_columns(
                        sql.literal_column("ROWNUM").label("ora_rn")
                    )
                    limitselect._tibero_visit = True
                    limitselect._is_wrapper = True

                    if for_update is not None and for_update.of:
                        limitselect_cols = limitselect.selected_columns
                        for elem in for_update.of:
                            if (
                                    limitselect_cols.corresponding_column(elem)
                                    is None
                            ):
                                limitselect = limitselect.add_columns(elem)

                    limit_subquery = limitselect.alias()
                    origselect_cols = orig_select.selected_columns
                    offsetselect = sql.select(
                        *[
                            c
                            for c in limit_subquery.c
                            if origselect_cols.corresponding_column(c)
                               is not None
                        ]
                    )

                    offsetselect._tibero_visit = True
                    offsetselect._is_wrapper = True

                    if for_update is not None and for_update.of:
                        adapter = sql_util.ClauseAdapter(limit_subquery)
                        for_update.of = [
                            adapter.traverse(elem) for elem in for_update.of
                        ]

                    offsetselect = offsetselect.where(
                        sql.literal_column("ora_rn") > offset_clause
                    )

                    offsetselect._for_update_arg = for_update
                    select = offsetselect

        return select

    def limit_clause(self, select, **kw):
        return ""

    def visit_empty_set_expr(self, type_, **kw):
        return "SELECT 1 FROM DUAL WHERE 1!=1"

    def for_update_clause(self, select, **kw):
        if self.is_subquery():
            return ""

        tmp = " FOR UPDATE"

        if select._for_update_arg.of:
            tmp += " OF " + ", ".join(
                self.process(elem, **kw) for elem in select._for_update_arg.of
            )

        if select._for_update_arg.nowait:
            tmp += " NOWAIT"
        if select._for_update_arg.skip_locked:
            tmp += " SKIP LOCKED"

        return tmp

    def visit_is_distinct_from_binary(self, binary, operator, **kw):
        return "DECODE(%s, %s, 0, 1) = 1" % (
            self.process(binary.left),
            self.process(binary.right),
        )

    def visit_is_not_distinct_from_binary(self, binary, operator, **kw):
        return "DECODE(%s, %s, 0, 1) = 0" % (
            self.process(binary.left),
            self.process(binary.right),
        )

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        string = self.process(binary.left, **kw)
        pattern = self.process(binary.right, **kw)
        flags = binary.modifiers["flags"]
        if flags is None:
            return "REGEXP_LIKE(%s, %s)" % (string, pattern)
        else:
            return "REGEXP_LIKE(%s, %s, %s)" % (
                string,
                pattern,
                self.render_literal_value(flags, sqltypes.STRINGTYPE),
            )

    def visit_not_regexp_match_op_binary(self, binary, operator, **kw):
        return "NOT %s" % self.visit_regexp_match_op_binary(
            binary, operator, **kw
        )

    def visit_regexp_replace_op_binary(self, binary, operator, **kw):
        string = self.process(binary.left, **kw)
        pattern_replace = self.process(binary.right, **kw)
        flags = binary.modifiers["flags"]
        if flags is None:
            return "REGEXP_REPLACE(%s, %s)" % (
                string,
                pattern_replace,
            )
        else:
            return "REGEXP_REPLACE(%s, %s, %s)" % (
                string,
                pattern_replace,
                self.render_literal_value(flags, sqltypes.STRINGTYPE),
            )

    def visit_aggregate_strings_func(self, fn, **kw):
        return "LISTAGG%s" % self.function_argspec(fn, **kw)


class TiberoDDLCompiler(compiler.DDLCompiler):
    def define_constraint_cascades(self, constraint):
        text = ""
        if constraint.ondelete is not None:
            text += " ON DELETE %s" % constraint.ondelete

        # oracle and tibero have no ON UPDATE CASCADE -
        # its only available via triggers
        # https://asktom.oracle.com/tkyte/update_cascade/index.html
        if constraint.onupdate is not None:
            util.warn(
                "Tibero does not contain native UPDATE CASCADE "
                "functionality - onupdates will not be rendered for foreign "
                "keys.  Consider using deferrable=True, initially='deferred' "
                "or triggers."
            )

        return text

    def visit_drop_table_comment(self, drop, **kw):
        return "COMMENT ON TABLE %s IS ''" % self.preparer.format_table(
            drop.element
        )

    def visit_create_index(self, create, **kw):
        index = create.element
        self._verify_index_table(index)
        preparer = self.preparer
        text = "CREATE "
        if index.unique:
            text += "UNIQUE "
        # TODO: 테스트를 위해 tibero에서 잠시 oracle로 바꿨습니다. string을 바꾸지 않고 테스트할 수 있는 방법을 찾으세요.
        # if index.dialect_options["tibero"]["bitmap"]:
        #     text += "BITMAP "

        if index.dialect_options["oracle"]["bitmap"]:
            text += "BITMAP "

        text += "INDEX %s ON %s (%s)" % (
            self._prepared_index_name(index, include_schema=True),
            preparer.format_table(index.table, use_schema=True),
            ", ".join(
                self.sql_compiler.process(
                    expr, include_table=False, literal_binds=True
                )
                for expr in index.expressions
            ),
        )

        # TODO: 테스트를 위해 tibero에서 잠시 oracle로 바꿨습니다. string을 바꾸지 않고 테스트할 수 있는 방법을 찾으세요.
        # if index.dialect_options["tibero"]["compress"] is not False:
        #     if index.dialect_options["tibero"]["compress"] is True:
        #         text += " COMPRESS"
        #     else:
        #         text += " COMPRESS %d" % (
        #             index.dialect_options["tibero"]["compress"]
        #         )
        if index.dialect_options["oracle"]["compress"] is not False:
            if index.dialect_options["oracle"]["compress"] is True:
                text += " COMPRESS"
            else:
                text += " COMPRESS %d" % (
                    index.dialect_options["oracle"]["compress"]
                )

        return text

    def post_create_table(self, table):
        table_opts = []

        # TODO: 테스트를 위해 tibero에서 잠시 oracle로 바꿨습니다. string을 바꾸지 않고 테스트할 수 있는 방법을 찾으세요.
        # opts = table.dialect_options["tibero"]
        opts = table.dialect_options["oracle"]

        if opts["on_commit"]:
            on_commit_options = opts["on_commit"].replace("_", " ").upper()
            table_opts.append("\n ON COMMIT %s" % on_commit_options)

        if opts["compress"]:
            if opts["compress"] is True:
                table_opts.append("\n COMPRESS")
            else:
                table_opts.append("\n COMPRESS FOR %s" % (opts["compress"]))

        return "".join(table_opts)

    def get_identity_options(self, identity_options):
        text = super().get_identity_options(identity_options)
        text = text.replace("NO MINVALUE", "NOMINVALUE")
        text = text.replace("NO MAXVALUE", "NOMAXVALUE")
        text = text.replace("NO CYCLE", "NOCYCLE")
        # TODO: 이 코드는 sqlalchemy 2.1에서 부터 지원됩니다. sqlalchemy 2.1 버전을
        #       위해 코멘트를 나중에 해제하십시오.
        # options = identity_options.dialect_options["tibero"]
        # if options.get("order") is not None:
        #     text += " ORDER" if options["order"] else " NOORDER"

        if identity_options.order is not None:
            text += " ORDER" if identity_options.order else " NOORDER"

        return text.strip()

    def visit_computed_column(self, generated, **kw):
        text = "GENERATED ALWAYS AS (%s)" % self.sql_compiler.process(
            generated.sqltext, include_table=False, literal_binds=True
        )
        if generated.persisted is True:
            raise exc.CompileError(
                "Tibero computed columns do not support 'stored' persistence; "
                "set the 'persisted' flag to None or False for Tibero support."
            )
        elif generated.persisted is False:
            text += " VIRTUAL"
        return text

    def visit_identity_column(self, identity, **kw):
        if identity.always is None:
            kind = ""
        else:
            kind = "ALWAYS" if identity.always else "BY DEFAULT"
        text = "GENERATED %s" % kind
        # TODO: 이 코드는 sqlalchemy 2.1에서 부터 지원됩니다. sqlalchemy 2.1 버전을
        #       위해 코멘트를 나중에 해제하십시오.
        # if identity.dialect_options["tibero"].get("on_null"):
        #     text += " ON NULL"
        if identity.on_null:
            text += " ON NULL"

        text += " AS IDENTITY"
        options = self.get_identity_options(identity)
        if options:
            text += " (%s)" % options
        return text


class TiberoIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = {x.lower() for x in RESERVED_WORDS}
    illegal_initial_characters = {str(dig) for dig in range(0, 10)}.union(
        ["_", "$"]
    )

    def _bindparam_requires_quotes(self, value):
        """Return True if the given identifier requires quoting."""
        lc_value = value.lower()
        return (
                lc_value in self.reserved_words
                or value[0] in self.illegal_initial_characters
                or not self.legal_characters.match(str(value))
        )

    def format_savepoint(self, savepoint):
        name = savepoint.ident.lstrip("_")
        return super().format_savepoint(savepoint, name)


class TiberoExecutionContext(default.DefaultExecutionContext):
    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
            "SELECT "
            + self.identifier_preparer.format_sequence(seq)
            + ".nextval FROM DUAL",
            type_,
        )

    def pre_exec(self):
        if self.statement and "_tibero_dblink" in self.execution_options:
            self.statement = self.statement.replace(
                dictionary.DB_LINK_PLACEHOLDER,
                self.execution_options["_tibero_dblink"],
            )


class TiberoDialect(default.DefaultDialect):
    name = "oracle"
    supports_statement_cache = True
    supports_alter = True
    max_identifier_length = 128

    _supports_offset_fetch = True

    insert_returning = True
    update_returning = True
    delete_returning = True

    div_is_floordiv = False

    supports_simple_order_by_label = False
    cte_follows_insert = True
    returns_native_bytes = True

    supports_sequences = True
    sequences_optional = False
    postfetch_lastrowid = False

    default_paramstyle = "named"
    colspecs = colspecs
    ischema_names = ischema_names
    requires_name_normalize = True

    supports_comments = True

    supports_default_values = False
    supports_default_metavalue = True
    supports_empty_insert = False
    supports_identity_columns = True

    statement_compiler = TiberoCompiler
    ddl_compiler = TiberoDDLCompiler
    type_compiler_cls = TiberoTypeCompiler
    preparer = TiberoIdentifierPreparer
    execution_ctx_cls = TiberoExecutionContext

    reflection_options = ("tibero_resolve_synonyms",)

    _use_nchar_for_unicode = False

    construct_arguments = [
        (
            sa_schema.Table,
            {"resolve_synonyms": False, "on_commit": None, "compress": False},
        ),
        (sa_schema.Index, {"bitmap": False, "compress": False}),
        (sa_schema.Sequence, {"order": None}),
        (sa_schema.Identity, {"order": None, "on_null": None}),
    ]

    @util.deprecated_params(
        use_binds_for_limits=(
                "1.4",
                "The ``use_binds_for_limits`` Tibero dialect parameter is "
                "deprecated. The dialect now renders LIMIT /OFFSET integers "
                "inline in all cases using a post-compilation hook, so that the "
                "value is still represented by a 'bound parameter' on the Core "
                "Expression side.",
        )
    )
    def __init__(
            self,
            use_ansi=True,
            optimize_limits=False,
            use_binds_for_limits=None,
            use_nchar_for_unicode=False,
            exclude_tablespaces=("SYSTEM", "SYSSUB"),
            enable_offset_fetch=True,
            **kwargs,
    ):
        default.DefaultDialect.__init__(self, **kwargs)
        self._use_nchar_for_unicode = use_nchar_for_unicode
        self.use_ansi = use_ansi
        self.optimize_limits = optimize_limits
        self.exclude_tablespaces = exclude_tablespaces

        # TODO: tibero 7 패치셋 version에 따라 지원 여부 확인하기
        #       enable_offset_fetch는 oracle version에 따라 지원 여부가 다른 것 같은데
        #       tibero 7에서는 어떤지 확인하기
        self.enable_offset_fetch = self._supports_offset_fetch = enable_offset_fetch

    def initialize(self, connection):
        super().initialize(connection)

        # TODO: tibero 7 패치셋 version에 따라 지원 여부 확인하기
        self.supports_identity_columns = True
        # TODO: tibero 7 패치셋 version에 따라 지원 여부 확인하기
        self._supports_offset_fetch = self.enable_offset_fetch

    @property
    def _supports_table_compression(self):
        # TODO: tibero 7 패치셋 version에 따라 지원 여부 확인하기
        return True

    @property
    def _supports_table_compress_for(self):
        # TODO: tibero 7 패치셋 version에 따라 지원 여부 확인하기
        return True

    @property
    def _supports_char_length(self):
        # TODO: tibero 7 패치셋 version에 따라 지원 여부 확인하기
        return True

    @property
    def _supports_update_returning_computed_cols(self):
        return True

    @property
    def _supports_except_all(self):
        return False

    def do_release_savepoint(self, connection, name):
        # Like Oracle, Tibero does not support RELEASE SAVEPOINT
        pass

    def _check_max_identifier_length(self, connection):
        # use the default which is defined in max_identifier_length field
        return None

    def get_isolation_level_values(self, dbapi_connection):
        return ["READ COMMITTED", "SERIALIZABLE"]

    def get_default_isolation_level(self, dbapi_conn):
        try:
            return self.get_isolation_level(dbapi_conn)
        except NotImplementedError:
            raise
        except:
            return "READ COMMITTED"

    def _execute_reflection(
            self, connection, query, dblink, returns_long, params=None
    ):

        # TODO: schema_translate_map이 무엇인지 어떻게 작동하는지 알아보기
        #       sqlalchemy tibero dialect를 통해서 dblink가 작동하는지 확인하기
        if dblink and not dblink.startswith("@"):
            dblink = f"@{dblink}"
        execution_options = {
            # handle db links
            "_tibero_dblink": dblink or "",
            # override any schema translate map
            "schema_translate_map": None,
        }

        # TODO: 이 if-statement가 티베로에서도 필요한지 확인하기
        if dblink and returns_long:
            # Oracle seems to error with
            # "ORA-00997: illegal use of LONG datatype" when returning
            # LONG columns via a dblink in a query with bind params
            # This type seems to be very hard to cast into something else
            # so it seems easier to just use bind param in this case
            def visit_bindparam(bindparam):
                bindparam.literal_execute = True

            query = visitors.cloned_traverse(
                query, {}, {"bindparam": visit_bindparam}
            )
        return connection.execute(
            query, params, execution_options=execution_options
        )

    @util.memoized_property
    def _has_table_query(self):
        # materialized views are returned by all_tables
        tables = (
            select(
                dictionary.all_tables.c.table_name,
                dictionary.all_tables.c.owner,
            )
            .union_all(
                select(
                    dictionary.all_views.c.view_name.label("table_name"),
                    dictionary.all_views.c.owner,
                )
            )
            .subquery("tables_and_views")
        )

        query = select(tables.c.table_name).where(
            tables.c.table_name == bindparam("table_name"),
            tables.c.owner == bindparam("owner"),
        )
        return query

    @reflection.cache
    def has_table(
            self, connection, table_name, schema=None, dblink=None, **kw
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link."""
        self._ensure_has_table_connection(connection)

        if not schema:
            schema = self.default_schema_name

        params = {
            "table_name": self.denormalize_name(table_name),
            "owner": self.denormalize_schema_name(schema),
        }
        cursor = self._execute_reflection(
            connection,
            self._has_table_query,
            dblink,
            returns_long=False,
            params=params,
        )
        return bool(cursor.scalar())

    @reflection.cache
    def has_sequence(
            self, connection, sequence_name, schema=None, dblink=None, **kw
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link."""
        if not schema:
            schema = self.default_schema_name

        query = select(dictionary.all_sequences.c.sequence_name).where(
            dictionary.all_sequences.c.sequence_name
            == self.denormalize_schema_name(sequence_name),
            dictionary.all_sequences.c.sequence_owner
            == self.denormalize_schema_name(schema),
        )

        cursor = self._execute_reflection(
            connection, query, dblink, returns_long=False
        )
        return bool(cursor.scalar())

    def _get_default_schema_name(self, connection):
        return self.normalize_name(
            connection.exec_driver_sql(
                "select sys_context( 'userenv', 'current_schema' ) from dual"
            ).scalar()
        )

    def denormalize_schema_name(self, name):
        # look for quoted_name
        force = getattr(name, "quote", None)
        if force is None and name == "public":
            # look for case insensitive, no quoting specified, "public"
            return "PUBLIC"
        return super().denormalize_name(name)

    @reflection.flexi_cache(
        ("schema", InternalTraversal.dp_string),
        ("filter_names", InternalTraversal.dp_string_list),
        ("dblink", InternalTraversal.dp_string),
    )
    def _get_synonyms(self, connection, schema, filter_names, dblink, **kw):
        owner = self.denormalize_schema_name(
            schema or self.default_schema_name
        )

        has_filter_names, params = self._prepare_filter_names(filter_names)
        query = select(
            dictionary.all_synonyms.c.synonym_name,
            dictionary.all_synonyms.c.org_object_name,
            dictionary.all_synonyms.c.org_object_owner,
        ).where(dictionary.all_synonyms.c.owner == owner)
        if has_filter_names:
            query = query.where(
                dictionary.all_synonyms.c.synonym_name.in_(
                    params["filter_names"]
                )
            )
        result = self._execute_reflection(
            connection, query, dblink, returns_long=False
        ).mappings()
        return result.all()

    @lru_cache()
    def _all_objects_query(
            self, owner, scope, kind, has_filter_names, has_mat_views
    ):
        query = (
            select(dictionary.all_objects.c.object_name)
            .where(dictionary.all_objects.c.owner == owner)
        )

        # NOTE: materialized views are listed in all_objects twice;
        # once as MATERIALIZE VIEW and once as TABLE
        if kind is ObjectKind.ANY:
            # materilaized view are listed also as tables so there is no
            # need to add them to the in_.
            query = query.where(
                dictionary.all_objects.c.object_type.in_(("TABLE", "VIEW"))
            )
        else:
            object_type = []
            if ObjectKind.VIEW in kind:
                object_type.append("VIEW")
            if (
                    ObjectKind.MATERIALIZED_VIEW in kind
                    and ObjectKind.TABLE not in kind
            ):
                # materilaized view are listed also as tables so there is no
                # need to add them to the in_ if also selecting tables.
                object_type.append("MATERIALIZED VIEW")
            if ObjectKind.TABLE in kind:
                object_type.append("TABLE")
                if has_mat_views and ObjectKind.MATERIALIZED_VIEW not in kind:
                    # materialized view are listed also as tables,
                    # so they need to be filtered out
                    # EXCEPT ALL / MINUS profiles as faster than using
                    # NOT EXISTS or NOT IN with a subquery, but it's in
                    # general faster to get the mat view names and exclude
                    # them only when needed
                    query = query.where(
                        dictionary.all_objects.c.object_name.not_in(
                            bindparam("mat_views")
                        )
                    )
            query = query.where(
                dictionary.all_objects.c.object_type.in_(object_type)
            )

        # handles scope
        if scope is ObjectScope.DEFAULT:
            query = query.where(dictionary.all_objects.c.temporary == "N")
        elif scope is ObjectScope.TEMPORARY:
            query = query.where(dictionary.all_objects.c.temporary == "Y")

        if has_filter_names:
            query = query.where(
                dictionary.all_objects.c.object_name.in_(
                    bindparam("filter_names")
                )
            )
        return query

    @reflection.flexi_cache(
        ("schema", InternalTraversal.dp_string),
        ("scope", InternalTraversal.dp_plain_obj),
        ("kind", InternalTraversal.dp_plain_obj),
        ("filter_names", InternalTraversal.dp_string_list),
        ("dblink", InternalTraversal.dp_string),
    )
    def _get_all_objects(
            self, connection, schema, scope, kind, filter_names, dblink, **kw
    ):
        owner = self.denormalize_schema_name(
            schema or self.default_schema_name
        )

        has_filter_names, params = self._prepare_filter_names(filter_names)
        has_mat_views = False
        if (
                ObjectKind.TABLE in kind
                and ObjectKind.MATERIALIZED_VIEW not in kind
        ):
            # see note in _all_objects_query
            mat_views = self.get_materialized_view_names(
                connection, schema, dblink, _normalize=False, **kw
            )
            if mat_views:
                params["mat_views"] = mat_views
                has_mat_views = True

        query = self._all_objects_query(
            owner, scope, kind, has_filter_names, has_mat_views
        )

        result = self._execute_reflection(
            connection, query, dblink, returns_long=False, params=params
        ).scalars()

        return result.all()

    def _handle_synonyms_decorator(fn):
        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            return self._handle_synonyms(fn, *args, **kwargs)

        return wrapper

    def _handle_synonyms(self, fn, connection, *args, **kwargs):
        if not kwargs.get("tibero_resolve_synonyms", False):
            return fn(self, connection, *args, **kwargs)

        original_kw = kwargs.copy()
        schema = kwargs.pop("schema", None)
        result = self._get_synonyms(
            connection,
            schema=schema,
            filter_names=kwargs.pop("filter_names", None),
            dblink=kwargs.pop("dblink", None),
            info_cache=kwargs.get("info_cache", None),
        )

        dblinks_owners = defaultdict(dict)
        for row in result:
            # TODO: 바로 아래 라인이 문제없이 작동하는지 테스트 필요
            remote_table_name, db_link = row["db_link"].split("@")
            db_link = '@' + db_link

            key = db_link, row["org_object_owner"]
            tn = self.normalize_name(row["org_object_name"])
            dblinks_owners[key][tn] = row["synonym_name"]

        if not dblinks_owners:
            # No synonym, do the plain thing
            return fn(self, connection, *args, **original_kw)

        data = {}
        for (dblink, table_owner), mapping in dblinks_owners.items():
            call_kw = {
                **original_kw,
                "schema": table_owner,
                "dblink": self.normalize_name(dblink),
                "filter_names": mapping.keys(),
            }
            call_result = fn(self, connection, *args, **call_kw)
            for (_, tn), value in call_result:
                synonym_name = self.normalize_name(mapping[tn])
                data[(schema, synonym_name)] = value
        return data.items()

    @reflection.cache
    def get_schema_names(self, connection, dblink=None, **kw):
        """Supported kw arguments are: ``dblink`` to reflect via a db link."""
        query = select(dictionary.all_users.c.username).order_by(
            dictionary.all_users.c.username
        )
        result = self._execute_reflection(
            connection, query, dblink, returns_long=False
        ).scalars()
        return [self.normalize_name(row) for row in result]

    @reflection.cache
    def get_table_names(self, connection, schema=None, dblink=None, **kw):
        """Supported kw arguments are: ``dblink`` to reflect via a db link."""
        # note that table_names() isn't loading DBLINKed or synonym'ed tables
        if schema is None:
            schema = self.default_schema_name

        den_schema = self.denormalize_schema_name(schema)
        if kw.get("tibero_resolve_synonyms", False):
            # TODO: 애초에 synonym과 연결된 테이블이 다 all_tables안에 있을거 같은데
            #       all_synonyms도 탐색할 이유가 있는지 의문이 있습니다. 만약
            #       테이블 이름 + 테이블과 연결된 synonym 이름을 보여준다면 이해가 됩니다.
            #
            # TODO: 쿼리가 너무 복잡한 것 같습니다. 쉽게 이해할 수 있는 쿼리가 있는지 고려해야 합니다.
            tables = (
                select(
                    dictionary.all_tables.c.table_name,
                    dictionary.all_tables.c.owner,
                    dictionary.all_tables.c.iot_type,
                    dictionary.all_tables.c.duration,
                    dictionary.all_tables.c.tablespace_name,
                )
                .union_all(
                    select(
                        dictionary.all_synonyms.c.synonym_name.label(
                            "table_name"
                        ),
                        dictionary.all_synonyms.c.owner,
                        dictionary.all_tables.c.iot_type,
                        dictionary.all_tables.c.duration,
                        dictionary.all_tables.c.tablespace_name,
                    )
                    .select_from(dictionary.all_tables)
                    .join(
                        dictionary.all_synonyms,
                        and_(
                            dictionary.all_tables.c.table_name
                            == dictionary.all_synonyms.c.table_name,
                            dictionary.all_tables.c.owner
                            == func.coalesce(
                                dictionary.all_synonyms.c.table_owner,
                                dictionary.all_synonyms.c.owner,
                            ),
                        ),
                    )
                )
                .subquery("available_tables")
            )
        else:
            tables = dictionary.all_tables

        query = select(tables.c.table_name)
        if self.exclude_tablespaces:
            query = query.where(
                func.coalesce(
                    tables.c.tablespace_name, "no tablespace"
                ).not_in(self.exclude_tablespaces)
            )
        query = query.where(
            tables.c.owner == den_schema,
            tables.c.iot_type.is_(null()),
            tables.c.duration.is_(null()),
        )

        # remove materialized views
        mat_query = select(
            dictionary.all_mviews.c.mview_name.label("table_name")
        ).where(dictionary.all_mviews.c.owner == den_schema)

        query = (
            query.except_all(mat_query)
            if self._supports_except_all
            else query.except_(mat_query)
        )

        result = self._execute_reflection(
            connection, query, dblink, returns_long=False
        ).scalars()
        return [self.normalize_name(row) for row in result]

    @reflection.cache
    def get_temp_table_names(self, connection, **kw):
        schema = self.denormalize_name(self.default_schema_name)

        sql_str = "SELECT table_name FROM all_tables WHERE "
        if self.exclude_tablespaces:
            sql_str += (
                "nvl(tablespace_name, 'no tablespace') "
                "NOT IN (%s) AND "
                % (", ".join(["'%s'" % ts for ts in self.exclude_tablespaces]))
            )
        sql_str += (
            "OWNER = :owner "
            "AND DURATION IS NOT NULL"
        )

        cursor = connection.execute(sql.text(sql_str), dict(owner=schema))
        return [self.normalize_name(row[0]) for row in cursor]

    # TODO: 아래 코드는 이상이 없어보이나 다음의 에러를 발생시킵니다. 이유를 찾기 바랍니다.
    #       (pyodbc.Error) ('ERREX', '[ERREX]  Values are from incompatible
    #       data types. (-11022) (SQLExecDirectW)')
    # @reflection.cache
    # def get_temp_table_names(self, connection, dblink=None, **kw):
    #     """Supported kw arguments are: ``dblink`` to reflect via a db link."""
    #     schema = self.denormalize_schema_name(self.default_schema_name)
    #
    #     query = select(dictionary.all_tables.c.table_name)
    #     if self.exclude_tablespaces:
    #         query = query.where(
    #             func.coalesce(
    #                 dictionary.all_tables.c.tablespace_name, "no tablespace"
    #             ).not_in(self.exclude_tablespaces)
    #         )
    #
    #     # TODO: iot_name이 왜 조건에 필요한지 모르겠습니다. temp table을 생성할 때
    #     #       index organized table로 만들 수 없는 것으로 알고 있습니다.
    #     #       temp table를 찾는 조건에 iot_name은 불필요해보입니다.
    #     #       또한 duration 칼럼이 아닌 temporary ='Y' 을 사용해도 괜찮아 보입니다.
    #     query = query.where(
    #         dictionary.all_tables.c.owner == schema,
    #         dictionary.all_tables.c.iot_type.is_(null()),
    #         dictionary.all_tables.c.duration.is_not(null()),
    #     )
    #
    #     result = self._execute_reflection(
    #         connection, query, dblink, returns_long=False
    #     ).scalars()
    #     return [self.normalize_name(row) for row in result]

    @reflection.cache
    def get_materialized_view_names(
            self, connection, schema=None, dblink=None, _normalize=True, **kw
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link."""
        if not schema:
            schema = self.default_schema_name

        query = select(dictionary.all_mviews.c.mview_name).where(
            dictionary.all_mviews.c.owner
            == self.denormalize_schema_name(schema)
        )
        result = self._execute_reflection(
            connection, query, dblink, returns_long=False
        ).scalars()
        if _normalize:
            return [self.normalize_name(row) for row in result]
        else:
            return result.all()

    @reflection.cache
    def get_view_names(self, connection, schema=None, dblink=None, **kw):
        """Supported kw arguments are: ``dblink`` to reflect via a db link."""
        if not schema:
            schema = self.default_schema_name

        query = select(dictionary.all_views.c.view_name).where(
            dictionary.all_views.c.owner
            == self.denormalize_schema_name(schema)
        )
        result = self._execute_reflection(
            connection, query, dblink, returns_long=False
        ).scalars()
        return [self.normalize_name(row) for row in result]

    @reflection.cache
    def get_sequence_names(self, connection, schema=None, dblink=None, **kw):
        """Supported kw arguments are: ``dblink`` to reflect via a db link."""
        if not schema:
            schema = self.default_schema_name
        query = select(dictionary.all_sequences.c.sequence_name).where(
            dictionary.all_sequences.c.sequence_owner
            == self.denormalize_schema_name(schema)
        )

        result = self._execute_reflection(
            connection, query, dblink, returns_long=False
        ).scalars()
        return [self.normalize_name(row) for row in result]

    def _value_or_raise(self, data, table, schema):
        table = self.normalize_name(str(table))
        try:
            return dict(data)[(schema, table)]
        except KeyError:
            raise exc.NoSuchTableError(
                f"{schema}.{table}" if schema else table
            ) from None

    def _prepare_filter_names(self, filter_names):
        if filter_names:
            fn = [self.denormalize_name(name) for name in filter_names]
            return True, {"filter_names": fn}
        else:
            return False, {}

    @reflection.cache
    def get_table_options(self, connection, table_name, schema=None, **kw):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        data = self.get_multi_table_options(
            connection,
            schema=schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    @lru_cache()
    def _table_options_query(
            self, owner, scope, kind, has_filter_names, has_mat_views
    ):
        query = select(
            dictionary.all_tables.c.table_name,
            (
                dictionary.all_tables.c.compression
                if self._supports_table_compression
                else sql.null().label("compression")
            ),
            (
                dictionary.all_tables.c.compress_for
                if self._supports_table_compress_for
                else sql.null().label("compress_for")
            ),
        ).where(dictionary.all_tables.c.owner == owner)
        if has_filter_names:
            query = query.where(
                dictionary.all_tables.c.table_name.in_(
                    bindparam("filter_names")
                )
            )
        if scope is ObjectScope.DEFAULT:
            query = query.where(dictionary.all_tables.c.duration.is_(null()))
        elif scope is ObjectScope.TEMPORARY:
            query = query.where(
                dictionary.all_tables.c.duration.is_not(null())
            )

        if (
                has_mat_views
                and ObjectKind.TABLE in kind
                and ObjectKind.MATERIALIZED_VIEW not in kind
        ):
            # cant use EXCEPT ALL / MINUS here because we don't have an
            # excludable row vs. the query above
            # outerjoin + where null works better on oracle 21 but 11 does
            # not like it at all. this is the next best thing

            query = query.where(
                dictionary.all_tables.c.table_name.not_in(
                    bindparam("mat_views")
                )
            )
        elif (
                ObjectKind.TABLE not in kind
                and ObjectKind.MATERIALIZED_VIEW in kind
        ):
            query = query.where(
                dictionary.all_tables.c.table_name.in_(bindparam("mat_views"))
            )
        return query

    @_handle_synonyms_decorator
    def get_multi_table_options(
            self,
            connection,
            *,
            schema,
            filter_names,
            scope,
            kind,
            dblink=None,
            **kw,
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        owner = self.denormalize_schema_name(
            schema or self.default_schema_name
        )

        has_filter_names, params = self._prepare_filter_names(filter_names)
        has_mat_views = False

        if (
                ObjectKind.TABLE in kind
                and ObjectKind.MATERIALIZED_VIEW not in kind
        ):
            # see note in _table_options_query
            mat_views = self.get_materialized_view_names(
                connection, schema, dblink, _normalize=False, **kw
            )
            if mat_views:
                params["mat_views"] = mat_views
                has_mat_views = True
        elif (
                ObjectKind.TABLE not in kind
                and ObjectKind.MATERIALIZED_VIEW in kind
        ):
            mat_views = self.get_materialized_view_names(
                connection, schema, dblink, _normalize=False, **kw
            )
            params["mat_views"] = mat_views

        options = {}
        default = ReflectionDefaults.table_options

        if ObjectKind.TABLE in kind or ObjectKind.MATERIALIZED_VIEW in kind:
            query = self._table_options_query(
                owner, scope, kind, has_filter_names, has_mat_views
            )
            result = self._execute_reflection(
                connection, query, dblink, returns_long=False, params=params
            )

            for table, compression, compress_for in result:
                if compression == "ENABLED":
                    data = {"tibero_compress": compress_for}
                else:
                    data = default()
                options[(schema, self.normalize_name(table))] = data
        if ObjectKind.VIEW in kind and ObjectScope.DEFAULT in scope:
            # add the views (no temporary views)
            for view in self.get_view_names(connection, schema, dblink, **kw):
                if not filter_names or view in filter_names:
                    options[(schema, view)] = default()

        return options.items()

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """

        data = self.get_multi_columns(
            connection,
            schema=schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    def _run_batches(
            self, connection, query, dblink, returns_long, mappings, all_objects
    ):
        each_batch = 500
        batches = list(all_objects)
        while batches:
            batch = batches[0:each_batch]
            batches[0:each_batch] = []

            result = self._execute_reflection(
                connection,
                query,
                dblink,
                returns_long=returns_long,
                params={"all_objects": batch},
            )
            if mappings:
                yield from result.mappings()
            else:
                yield from result

    @lru_cache()
    def _column_query(self, owner):
        all_cols = dictionary.all_tab_cols
        all_comments = dictionary.all_col_comments
        all_ids = dictionary.all_tab_identity_cols

        # 오라클 코드에서는 all_tab_cols에 default_on_null 칼럼이 있지만
        # 티베로에는 없습니다. 그래서 임의의 숫자 99999를 사용했습니다.
        # 나중에 티베로에 칼럼들이 추가된다면 임의의 숫자를 올바른 숫자로 바꿔주세요.
        if self.server_version_info >= (999999,):
            add_cols = (
                all_cols.c.default_on_null,
                sql.case(
                    (all_ids.c.table_name.is_(None), sql.null()),
                    else_=all_ids.c.generation_type
                          + ","
                          + all_ids.c.identity_options,
                ).label("identity_options"),
            )
            join_identity_cols = True
        else:
            add_cols = (
                sql.null().label("default_on_null"),
                sql.null().label("identity_options"),
            )
            join_identity_cols = False

        # NOTE: on oracle cannot create tables/views without columns and
        # a table cannot have all column hidden:
        # ORA-54039: table must have at least one column that is not invisible
        # all_tab_cols returns data for tables/views/mat-views.
        # all_tab_cols does not return recycled tables

        query = (
            select(
                all_cols.c.table_name,
                all_cols.c.column_name,
                all_cols.c.data_type,
                all_cols.c.char_length,
                all_cols.c.data_precision,
                all_cols.c.data_scale,
                all_cols.c.nullable,
                all_cols.c.data_default,
                all_comments.c.comments,
                all_cols.c.virtual_column,
                *add_cols,
            ).select_from(all_cols)
            # NOTE: all_col_comments has a row for each column even if no
            # comment is present, so a join could be performed, but there
            # seems to be no difference compared to an outer join
            .outerjoin(
                all_comments,
                and_(
                    all_cols.c.table_name == all_comments.c.table_name,
                    all_cols.c.column_name == all_comments.c.column_name,
                    all_cols.c.owner == all_comments.c.owner,
                ),
            )
        )
        if join_identity_cols:
            query = query.outerjoin(
                all_ids,
                and_(
                    all_cols.c.table_name == all_ids.c.table_name,
                    all_cols.c.column_name == all_ids.c.column_name,
                    all_cols.c.owner == all_ids.c.owner,
                ),
            )

        # Oracle은 hidden_column을 지원하고 값으로는 YES 또는 NO입니다. 반면에
        # Tibero는 hidden_column을 지원하지 않으면 값으로는 N만 가지는 것 같습니다.
        query = query.where(
            all_cols.c.table_name.in_(bindparam("all_objects")),
            all_cols.c.hidden_column == "N",
            all_cols.c.owner == owner,
        ).order_by(all_cols.c.table_name, all_cols.c.column_id)
        return query

    @_handle_synonyms_decorator
    def get_multi_columns(
            self,
            connection,
            *,
            schema,
            filter_names,
            scope,
            kind,
            dblink=None,
            **kw,
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        owner = self.denormalize_schema_name(
            schema or self.default_schema_name
        )
        query = self._column_query(owner)

        if (
                filter_names
                and kind is ObjectKind.ANY
                and scope is ObjectScope.ANY
        ):
            all_objects = [self.denormalize_name(n) for n in filter_names]
        else:
            all_objects = self._get_all_objects(
                connection, schema, scope, kind, filter_names, dblink, **kw
            )

        columns = defaultdict(list)

        # all_tab_cols.data_default is LONG
        result = self._run_batches(
            connection,
            query,
            dblink,
            returns_long=True,
            mappings=True,
            all_objects=all_objects,
        )

        def maybe_int(value):
            if isinstance(value, float) and value.is_integer():
                return int(value)
            else:
                return value

        remove_size = re.compile(r"\(\d+\)")

        for row_dict in result:
            table_name = self.normalize_name(row_dict["table_name"])
            orig_colname = row_dict["column_name"]
            colname = self.normalize_name(orig_colname)
            coltype = row_dict["data_type"]
            precision = maybe_int(row_dict["data_precision"])

            if coltype == "NUMBER":
                scale = maybe_int(row_dict["data_scale"])
                if precision is None and scale == 0:
                    coltype = INTEGER()
                else:
                    coltype = types.NUMBER(precision, scale)
            elif coltype == "FLOAT":
                # https://docs.oracle.com/cd/B14117_01/server.101/b10758/sqlqr06.htm
                if precision == 126:
                    # The DOUBLE PRECISION datatype is a floating-point
                    # number with binary precision 126.
                    coltype = DOUBLE_PRECISION()
                elif precision == 63:
                    # The REAL datatype is a floating-point number with a
                    # binary precision of 63, or 18 decimal.
                    coltype = REAL()
                else:
                    # non standard precision
                    coltype = types.FLOAT(binary_precision=precision)

            elif coltype in ("VARCHAR", "NVARCHAR", "CHAR", "NCHAR"):
                # Oracle에서 VARCHAR 는 자동으로 VARCHAR2로 변환이 되는데
                # Tibero에서는 VARCHAR2가 자동으로 VARCHAR로 변환이 됩니다.
                # Oracle에서 NVARCHAR 라는 타입은 없으나 NVARCHAR2 라는
                # 타입을 가지고 있습니다. Tibero에서는 VARCHAR2가 자동으로
                # VARCHAR로 변환이 됩니다.
                char_length = maybe_int(row_dict["char_length"])
                coltype = self.ischema_names.get(coltype)(char_length)
            elif "WITH TIME ZONE" in coltype:
                coltype = types.TIMESTAMP(timezone=True)
            elif "WITH LOCAL TIME ZONE" in coltype:
                coltype = types.TIMESTAMP(local_timezone=True)
            else:
                coltype = re.sub(remove_size, "", coltype)
                try:
                    coltype = self.ischema_names[coltype]
                except KeyError:
                    util.warn(
                        "Did not recognize type '%s' of column '%s'"
                        % (coltype, colname)
                    )
                    coltype = sqltypes.NULLTYPE

            default = row_dict["data_default"]
            if row_dict["virtual_column"] == "Y":
                computed = dict(sqltext=default)
                default = None
            else:
                computed = None

            identity_options = row_dict["identity_options"]
            if identity_options is not None:
                identity = self._parse_identity_options(
                    identity_options, row_dict["default_on_null"]
                )
                default = None
            else:
                identity = None

            cdict = {
                "name": colname,
                "type": coltype,
                "nullable": row_dict["nullable"] == "Y",
                "default": default,
                "comment": row_dict["comments"],
            }
            if orig_colname.lower() == orig_colname:
                cdict["quote"] = True
            if computed is not None:
                cdict["computed"] = computed
            if identity is not None:
                cdict["identity"] = identity

            columns[(schema, table_name)].append(cdict)

        # NOTE: default not needed since all tables have columns
        # default = ReflectionDefaults.columns
        # return (
        #     (key, value if value else default())
        #     for key, value in columns.items()
        # )
        return columns.items()

    def _parse_identity_options(self, identity_options, default_on_null):
        # identity_options is a string that starts with 'ALWAYS,' or
        # 'BY DEFAULT,' and continues with
        # START WITH: 1, INCREMENT BY: 1, MAX_VALUE: 123, MIN_VALUE: 1,
        # CYCLE_FLAG: N, CACHE_SIZE: 1, ORDER_FLAG: N, SCALE_FLAG: N,
        # EXTEND_FLAG: N, SESSION_FLAG: N, KEEP_VALUE: N
        #
        # Oracle Dialect의 원작성자가 적은 위 코멘트에 추가로 작성하겠습니다.
        # sqlplus같은 도구를 사용해 ALL_TAB_IDENTITY_COLS 테이블의 IDENTITY_OPTIONS
        # 칼럼을 조회하면 'ALWAYS' 또는 'BY DEFAULT'로 시작하지 않습니다. _column_query() 메서드에서
        # 제공되는 쿼리에서 'ALWAYS' 또는 'BY DEFAULT'로 시작되도록 하는 로직이 있습니다.
        parts = [p.strip() for p in identity_options.split(",")]
        identity = {
            "always": parts[0] == "ALWAYS",
            "tibero_on_null": default_on_null == "YES",
        }

        for part in parts[1:]:
            option, value = part.split(":")
            value = value.strip()

            if "START WITH" in option:
                identity["start"] = int(value)
            elif "INCREMENT BY" in option:
                identity["increment"] = int(value)
            elif "MAX_VALUE" in option:
                identity["maxvalue"] = int(value)
            elif "MIN_VALUE" in option:
                identity["minvalue"] = int(value)
            elif "CYCLE_FLAG" in option:
                identity["cycle"] = value == "Y"
            elif "CACHE_SIZE" in option:
                identity["cache"] = int(value)
            elif "ORDER_FLAG" in option:
                identity["tibero_order"] = value == "Y"
        return identity

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        data = self.get_multi_table_comment(
            connection,
            schema=schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    @lru_cache()
    def _comment_query(self, owner, scope, kind, has_filter_names):
        # NOTE: all_tab_comments / all_mview_comments have a row for all
        # object even if they don't have comments
        queries = []
        if ObjectKind.TABLE in kind or ObjectKind.VIEW in kind:
            # all_tab_comments returns also plain views
            tbl_view = select(
                dictionary.all_tab_comments.c.table_name,
                dictionary.all_tab_comments.c.comments,
            ).where(
                dictionary.all_tab_comments.c.owner == owner,
                dictionary.all_tab_comments.c.table_name.not_like("BIN$%"),
            )
            if ObjectKind.VIEW not in kind:
                tbl_view = tbl_view.where(
                    dictionary.all_tab_comments.c.table_type == "TABLE"
                )
            elif ObjectKind.TABLE not in kind:
                tbl_view = tbl_view.where(
                    dictionary.all_tab_comments.c.table_type == "VIEW"
                )
            queries.append(tbl_view)
        if ObjectKind.MATERIALIZED_VIEW in kind:
            mat_view = select(
                dictionary.all_mview_comments.c.mview_name.label("table_name"),
                dictionary.all_mview_comments.c.comments,
            ).where(
                dictionary.all_mview_comments.c.owner == owner,
                dictionary.all_mview_comments.c.mview_name.not_like("BIN$%"),
            )
            queries.append(mat_view)
        if len(queries) == 1:
            query = queries[0]
        else:
            union = sql.union_all(*queries).subquery("tables_and_views")
            query = select(union.c.table_name, union.c.comments)

        name_col = query.selected_columns.table_name

        if scope in (ObjectScope.DEFAULT, ObjectScope.TEMPORARY):
            temp = "Y" if scope is ObjectScope.TEMPORARY else "N"
            # need distinct since materialized view are listed also
            # as tables in all_objects
            query = query.distinct().join(
                dictionary.all_objects,
                and_(
                    dictionary.all_objects.c.owner == owner,
                    dictionary.all_objects.c.object_name == name_col,
                    dictionary.all_objects.c.temporary == temp,
                ),
            )
        if has_filter_names:
            query = query.where(name_col.in_(bindparam("filter_names")))
        return query

    @_handle_synonyms_decorator
    def get_multi_table_comment(
            self,
            connection,
            *,
            schema,
            filter_names,
            scope,
            kind,
            dblink=None,
            **kw,
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        owner = self.denormalize_schema_name(
            schema or self.default_schema_name
        )
        has_filter_names, params = self._prepare_filter_names(filter_names)
        query = self._comment_query(owner, scope, kind, has_filter_names)

        result = self._execute_reflection(
            connection, query, dblink, returns_long=False, params=params
        )
        default = ReflectionDefaults.table_comment

        # 아래의 내용은 티베로에는 적용되지 않으나 문제없이 작동되고
        # 업데이트된 oracle dialect 코드를 보고 tibero dialect 또한 업데이트할 때
        # 쉽게 업데이트하기 위해 코드를 남겨두었습니다.
        # materialized views by default seem to have a comment like
        # "snapshot table for snapshot owner.mat_view_name"
        ignore_mat_view = "snapshot table for snapshot "
        return (
            (
                (schema, self.normalize_name(table)),
                (
                    {"text": comment}
                    if comment is not None
                       and not comment.startswith(ignore_mat_view)
                    else default()
                ),
            )
            for table, comment in result
        )

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        data = self.get_multi_indexes(
            connection,
            schema=schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    @lru_cache()
    def _index_query(self, owner):

        # HACK
        # CASE 문을 사용하여 정규식에 맞는 경우 'SYS'를 붙임
        # oracle의 dialect._index_query()는 index_name과 index position에 따라 row
        # order를 결정합니다. 그런데 티베로는 index 이름 짓는 규칙이 오라클이랑 달라서 테스트에서
        # 실패하는 문제가 발생합니다. 우회방안으로 sql query 실행시 index name을 오라클이랑 비슷하게 변경하고
        # 변경된 index name으로 순서를 결정하도록 했습니다. 이 order가 중요할 수 있으니 최대한 sqlalchemy의
        # 행동과 따라하기 위해 변경했으나 솔직히 이 순서가 중요한 것 같지는 않습니다.
        # 테스트 스위트에서 정답지를 변경하거나 테스트의 동작을 수정할 수도 있지만, Tibero Dialect에서
        # 코드를 수정하기로 한 이유는, Oracle과 비교해 차이가 발생할 경우 가능한 서버에 가까운 쪽에서 수정하는
        # 것이 클라이언트 쪽에서 발생할 수 있는 불일치를 최소화할 수 있다고 판단했기 때문입니다.
        index_name = sql.case(
            (
                sql.text("REGEXP_LIKE(a_ind_columns.index_name, '^_.*CON\\d+$')"),
                sql.literal_column("'SYS'") + dictionary.all_ind_columns.c.index_name,
            ),
            else_= dictionary.all_ind_columns.c.index_name
        ).label("index_name")

        return (
            select(
                dictionary.all_ind_columns.c.table_name,
                index_name,
                dictionary.all_ind_columns.c.column_name,
                dictionary.all_indexes.c.index_type,
                dictionary.all_indexes.c.uniqueness,
                dictionary.all_indexes.c.compression,
                dictionary.all_indexes.c.prefix_length,
                dictionary.all_ind_columns.c.descend,
                dictionary.all_ind_expressions.c.column_expression,
            )
            .select_from(dictionary.all_ind_columns)
            .join(
                dictionary.all_indexes,
                sql.and_(
                    dictionary.all_ind_columns.c.index_name
                    == dictionary.all_indexes.c.index_name,
                    dictionary.all_ind_columns.c.index_owner
                    == dictionary.all_indexes.c.owner,
                ),
            )
            .outerjoin(
                # NOTE: this adds about 20% to the query time. Using a
                # case expression with a scalar subquery only when needed
                # with the assumption that most indexes are not expression
                # would be faster but oracle does not like that with
                # LONG datatype. It errors with:
                # ORA-00997: illegal use of LONG datatype
                dictionary.all_ind_expressions,
                sql.and_(
                    dictionary.all_ind_expressions.c.index_name
                    == dictionary.all_ind_columns.c.index_name,
                    dictionary.all_ind_expressions.c.index_owner
                    == dictionary.all_ind_columns.c.index_owner,
                    dictionary.all_ind_expressions.c.column_position
                    == dictionary.all_ind_columns.c.column_position,
                ),
            )
            .where(
                dictionary.all_indexes.c.table_owner == owner,
                dictionary.all_indexes.c.table_name.in_(
                    bindparam("all_objects")
                ),
            )
            .order_by(
                sql.literal_column("index_name"),
                dictionary.all_ind_columns.c.column_position,
            )
        )

    @reflection.flexi_cache(
        ("schema", InternalTraversal.dp_string),
        ("dblink", InternalTraversal.dp_string),
        ("all_objects", InternalTraversal.dp_string_list),
    )
    def _get_indexes_rows(self, connection, schema, dblink, all_objects, **kw):
        owner = self.denormalize_schema_name(
            schema or self.default_schema_name
        )

        query = self._index_query(owner)

        # NOTE: get_multi_indexes()는 SQLAlchemy에서는 primary
        #       key index를 제외한 index들만 반환하는 것이 spec인 듯 합니다.
        #       문서를 보았을 때 그런 말은 없으나 oracle dialect는 그렇게 구현
        #       되어 있습니다.
        pks = set()
        for row_dict in self._get_all_constraint_rows(connection, schema, dblink, all_objects, **kw):
            if row_dict["constraint_type"] != "P":
                continue

            # mview의 index 중에 i_snap$으로 시작하는 index가 있습니다.
            # oracle에서는 특정 설정없이 mview를 생성하면 새로운 index가
            # 생성되고 all_constraints view에서 index는 constraint_name과
            # index_name이 같은 string은 가집니다. 반면에 tibero에서는 원본 테이블의
            # index에 링크된 i_snap$ index를 사용하는 것을 확인했습니다. 이는 곧,
            # i_snap$ 인덱스의 경우 all_constraints view에서 constraint_name은
            # 원본 index 이름을 가지고 index_name이 i_snap$인 것을 의미합니다.
            # self._index_query()는 constraint_name이 아닌 index_name만을 반환하므로
            # i_snap$이 primary key index라면 pks에 추가해줘야 합니다.
            index_name = row_dict.get("index_name")
            constraint_name = row_dict["constraint_name"]
            if index_name and index_name != constraint_name:
                pks.add(index_name)
            else:
                pks.add(constraint_name)

        # all_ind_expressions.column_expression is LONG
        result = self._run_batches(
            connection,
            query,
            dblink,
            returns_long=True,
            mappings=True,
            all_objects=all_objects,
        )

        return [
            row_dict
            for row_dict in result
            if row_dict["index_name"] not in pks
        ]

    @_handle_synonyms_decorator
    def get_multi_indexes(
            self,
            connection,
            *,
            schema,
            filter_names,
            scope,
            kind,
            dblink=None,
            **kw,
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        all_objects = self._get_all_objects(
            connection, schema, scope, kind, filter_names, dblink, **kw
        )

        uniqueness = {"NONUNIQUE": False, "UNIQUE": True}
        enabled = {"DISABLED": False, "ENABLED": True}
        is_bitmap = {"BITMAP", "FUNCTION-BASED BITMAP"}

        indexes = defaultdict(dict)

        for row_dict in self._get_indexes_rows(
                connection, schema, dblink, all_objects, **kw
        ):
            index_name = self.normalize_name(row_dict["index_name"])
            table_name = self.normalize_name(row_dict["table_name"])
            table_indexes = indexes[(schema, table_name)]

            if index_name not in table_indexes:
                table_indexes[index_name] = index_dict = {
                    "name": index_name,
                    "column_names": [],
                    "dialect_options": {},
                    "unique": uniqueness.get(row_dict["uniqueness"], False),
                }
                do = index_dict["dialect_options"]
                if row_dict["index_type"] in is_bitmap:
                    do["tibero_bitmap"] = True
                if enabled.get(row_dict["compression"], False):
                    do["tibero_compress"] = row_dict["prefix_length"]

            else:
                index_dict = table_indexes[index_name]

            expr = row_dict["column_expression"]
            if expr is not None:
                index_dict["column_names"].append(None)
                if "expressions" in index_dict:
                    index_dict["expressions"].append(expr)
                else:
                    index_dict["expressions"] = index_dict["column_names"][:-1]
                    index_dict["expressions"].append(expr)

                if row_dict["descend"].lower() != "asc":
                    assert row_dict["descend"].lower() == "desc"
                    cs = index_dict.setdefault("column_sorting", {})
                    cs[expr] = ("desc",)
            else:
                assert row_dict["descend"].lower() == "asc"
                cn = self.normalize_name(row_dict["column_name"])
                index_dict["column_names"].append(cn)
                if "expressions" in index_dict:
                    index_dict["expressions"].append(cn)

        default = ReflectionDefaults.indexes

        return (
            (key, list(indexes[key].values()) if key in indexes else default())
            for key in (
            (schema, self.normalize_name(obj_name))
            for obj_name in all_objects
        )
        )

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        data = self.get_multi_pk_constraint(
            connection,
            schema=schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    @lru_cache()
    def _constraint_query(self, owner):
        local = dictionary.all_cons_columns.alias("local")
        remote = dictionary.all_cons_columns.alias("remote")
        # HACK
        # CASE 문을 사용하여 정규식에 맞는 경우 'SYS'를 붙임
        # oracle의 dialect._constraint_query()는 constraint_name과 position에 따라 row
        # order를 결정합니다. 그런데 티베로는 constraint_name 이름 짓는 규칙이 오라클이랑 달라서 테스트에서
        # 실패하는 문제가 발생합니다. 우회방안으로 sql query 실행시 constraint_name을 오라클이랑 비슷하게 변경하고
        # 변경된 constraint_name으로 순서를 결정하도록 했습니다. 이 order가 중요할 수 있으니 최대한
        # sqlalchemy의 행동과 따라하기 위해 변경했으나 솔직히 이 순서가 중요한 것 같지는 않습니다.
        # 테스트 스위트에서 정답지를 변경하거나 테스트의 동작을 수정할 수도 있지만, Tibero Dialect에서
        # 코드를 수정하기로 한 이유는, Oracle과 비교해 차이가 발생할 경우 가능한 서버에 가까운 쪽에서 수정하는
        # 것이 클라이언트 쪽에서 발생할 수 있는 불일치를 최소화할 수 있다고 판단했기 때문입니다.
        constraint_name = sql.case(
            (
                sql.text("REGEXP_LIKE(a_constraints.constraint_name, '^_.*CON\\d+$')"),
                sql.literal_column("'SYS'") + dictionary.all_constraints.c.constraint_name
            ),
            else_=dictionary.all_constraints.c.constraint_name
        ).label("constraint_name")
        index_name = sql.case(
            (
                sql.text("REGEXP_LIKE(a_constraints.index_name, '^_.*CON\\d+$')"),
                sql.literal_column("'SYS'") + dictionary.all_constraints.c.index_name
            ),
            else_=dictionary.all_constraints.c.index_name
        ).label("index_name")

        return (
            select(
                dictionary.all_constraints.c.table_name,
                dictionary.all_constraints.c.constraint_type,
                constraint_name,
                local.c.column_name.label("local_column"),
                remote.c.table_name.label("remote_table"),
                remote.c.column_name.label("remote_column"),
                remote.c.owner.label("remote_owner"),
                dictionary.all_constraints.c.search_condition,
                dictionary.all_constraints.c.delete_rule,
                index_name,
            )
            .select_from(dictionary.all_constraints)
            .join(
                local,
                and_(
                    local.c.owner == dictionary.all_constraints.c.owner,
                    dictionary.all_constraints.c.constraint_name
                    == local.c.constraint_name,
                ),
            )
            .outerjoin(
                remote,
                and_(
                    dictionary.all_constraints.c.r_owner == remote.c.owner,
                    dictionary.all_constraints.c.r_constraint_name
                    == remote.c.constraint_name,
                    or_(
                        remote.c.position.is_(sql.null()),
                        local.c.position == remote.c.position,
                    ),
                ),
            )
            .where(
                dictionary.all_constraints.c.owner == owner,
                dictionary.all_constraints.c.table_name.in_(
                    bindparam("all_objects")
                ),
                dictionary.all_constraints.c.constraint_type.in_(
                    ("R", "P", "U", "C")
                ),
            )
            .order_by(
                sql.literal_column("constraint_name"), local.c.position
            )
        )

    @reflection.flexi_cache(
        ("schema", InternalTraversal.dp_string),
        ("dblink", InternalTraversal.dp_string),
        ("all_objects", InternalTraversal.dp_string_list),
    )
    def _get_all_constraint_rows(
            self, connection, schema, dblink, all_objects, **kw
    ):
        owner = self.denormalize_schema_name(
            schema or self.default_schema_name
        )
        query = self._constraint_query(owner)

        # since the result is cached a list must be created
        values = list(
            self._run_batches(
                connection,
                query,
                dblink,
                returns_long=False,
                mappings=True,
                all_objects=all_objects,
            )
        )
        return values

    @_handle_synonyms_decorator
    def get_multi_pk_constraint(
            self,
            connection,
            *,
            scope,
            schema,
            filter_names,
            kind,
            dblink=None,
            **kw,
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        all_objects = self._get_all_objects(
            connection, schema, scope, kind, filter_names, dblink, **kw
        )

        primary_keys = defaultdict(dict)
        default = ReflectionDefaults.pk_constraint

        for row_dict in self._get_all_constraint_rows(
                connection, schema, dblink, all_objects, **kw
        ):
            if row_dict["constraint_type"] != "P":
                continue
            table_name = self.normalize_name(row_dict["table_name"])
            constraint_name = self.normalize_name(row_dict["constraint_name"])
            column_name = self.normalize_name(row_dict["local_column"])

            table_pk = primary_keys[(schema, table_name)]
            if not table_pk:
                table_pk["name"] = constraint_name
                table_pk["constrained_columns"] = [column_name]
            else:
                table_pk["constrained_columns"].append(column_name)

        return (
            (key, primary_keys[key] if key in primary_keys else default())
            for key in (
            (schema, self.normalize_name(obj_name))
            for obj_name in all_objects
        )
        )

    @reflection.cache
    def get_foreign_keys(
            self,
            connection,
            table_name,
            schema=None,
            **kw,
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        data = self.get_multi_foreign_keys(
            connection,
            schema=schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    @_handle_synonyms_decorator
    def get_multi_foreign_keys(
            self,
            connection,
            *,
            scope,
            schema,
            filter_names,
            kind,
            dblink=None,
            **kw,
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        all_objects = self._get_all_objects(
            connection, schema, scope, kind, filter_names, dblink, **kw
        )

        resolve_synonyms = kw.get("tibero_resolve_synonyms", False)

        owner = self.denormalize_schema_name(
            schema or self.default_schema_name
        )

        all_remote_owners = set()
        fkeys = defaultdict(dict)

        for row_dict in self._get_all_constraint_rows(
                connection, schema, dblink, all_objects, **kw
        ):
            if row_dict["constraint_type"] != "R":
                continue

            table_name = self.normalize_name(row_dict["table_name"])
            constraint_name = self.normalize_name(row_dict["constraint_name"])
            table_fkey = fkeys[(schema, table_name)]

            assert constraint_name is not None

            local_column = self.normalize_name(row_dict["local_column"])
            remote_table = self.normalize_name(row_dict["remote_table"])
            remote_column = self.normalize_name(row_dict["remote_column"])
            remote_owner_orig = row_dict["remote_owner"]
            remote_owner = self.normalize_name(remote_owner_orig)
            if remote_owner_orig is not None:
                all_remote_owners.add(remote_owner_orig)

            if remote_table is None:
                # ticket 363
                if dblink and not dblink.startswith("@"):
                    dblink = f"@{dblink}"
                util.warn(
                    "Got 'None' querying 'table_name' from "
                    f"all_cons_columns{dblink or ''} - does the user have "
                    "proper rights to the table?"
                )
                continue

            if constraint_name not in table_fkey:
                table_fkey[constraint_name] = fkey = {
                    "name": constraint_name,
                    "constrained_columns": [],
                    "referred_schema": None,
                    "referred_table": remote_table,
                    "referred_columns": [],
                    "options": {},
                }

                if resolve_synonyms:
                    # will be removed below
                    fkey["_ref_schema"] = remote_owner

                if schema is not None or remote_owner_orig != owner:
                    fkey["referred_schema"] = remote_owner

                delete_rule = row_dict["delete_rule"]
                if delete_rule != "NO ACTION":
                    fkey["options"]["ondelete"] = delete_rule

            else:
                fkey = table_fkey[constraint_name]

            fkey["constrained_columns"].append(local_column)
            fkey["referred_columns"].append(remote_column)

        if resolve_synonyms and all_remote_owners:
            query = select(
                dictionary.all_synonyms.c.owner,
                dictionary.all_synonyms.c.table_name,
                dictionary.all_synonyms.c.table_owner,
                dictionary.all_synonyms.c.synonym_name,
            ).where(dictionary.all_synonyms.c.owner.in_(all_remote_owners))

            result = self._execute_reflection(
                connection, query, dblink, returns_long=False
            ).mappings()

            remote_owners_lut = {}
            for row in result:
                synonym_owner = self.normalize_name(row["owner"])
                table_name = self.normalize_name(row["table_name"])

                remote_owners_lut[(synonym_owner, table_name)] = (
                    row["table_owner"],
                    row["synonym_name"],
                )

            empty = (None, None)
            for table_fkeys in fkeys.values():
                for table_fkey in table_fkeys.values():
                    key = (
                        table_fkey.pop("_ref_schema"),
                        table_fkey["referred_table"],
                    )
                    remote_owner, syn_name = remote_owners_lut.get(key, empty)
                    if syn_name:
                        sn = self.normalize_name(syn_name)
                        table_fkey["referred_table"] = sn
                        if schema is not None or remote_owner != owner:
                            ro = self.normalize_name(remote_owner)
                            table_fkey["referred_schema"] = ro
                        else:
                            table_fkey["referred_schema"] = None
        default = ReflectionDefaults.foreign_keys

        return (
            (key, list(fkeys[key].values()) if key in fkeys else default())
            for key in (
            (schema, self.normalize_name(obj_name))
            for obj_name in all_objects
        )
        )

    @reflection.cache
    def get_unique_constraints(
            self, connection, table_name, schema=None, **kw
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        data = self.get_multi_unique_constraints(
            connection,
            schema=schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    @_handle_synonyms_decorator
    def get_multi_unique_constraints(
            self,
            connection,
            *,
            scope,
            schema,
            filter_names,
            kind,
            dblink=None,
            **kw,
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        all_objects = self._get_all_objects(
            connection, schema, scope, kind, filter_names, dblink, **kw
        )

        unique_cons = defaultdict(dict)

        index_names = {
            row_dict["index_name"]
            for row_dict in self._get_indexes_rows(
                connection, schema, dblink, all_objects, **kw
            )
        }

        for row_dict in self._get_all_constraint_rows(
                connection, schema, dblink, all_objects, **kw
        ):
            if row_dict["constraint_type"] != "U":
                continue
            table_name = self.normalize_name(row_dict["table_name"])
            constraint_name_orig = row_dict["constraint_name"]
            constraint_name = self.normalize_name(constraint_name_orig)
            column_name = self.normalize_name(row_dict["local_column"])
            table_uc = unique_cons[(schema, table_name)]

            assert constraint_name is not None

            if constraint_name not in table_uc:
                table_uc[constraint_name] = uc = {
                    "name": constraint_name,
                    "column_names": [],
                    "duplicates_index": (
                        constraint_name
                        if constraint_name_orig in index_names
                        else None
                    ),
                }
            else:
                uc = table_uc[constraint_name]

            uc["column_names"].append(column_name)

        default = ReflectionDefaults.unique_constraints

        return (
            (
                key,
                (
                    list(unique_cons[key].values())
                    if key in unique_cons
                    else default()
                ),
            )
            for key in (
            (schema, self.normalize_name(obj_name))
            for obj_name in all_objects
        )
        )

    @reflection.cache
    def get_view_definition(
            self,
            connection,
            view_name,
            schema=None,
            dblink=None,
            **kw,
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        if kw.get("tibero_resolve_synonyms", False):
            synonyms = self._get_synonyms(
                connection, schema, filter_names=[view_name], dblink=dblink
            )
            if synonyms:
                assert len(synonyms) == 1
                row_dict = synonyms[0]
                dblink = self.normalize_name(row_dict["db_link"])
                schema = row_dict["table_owner"]
                view_name = row_dict["table_name"]

        name = self.denormalize_name(view_name)
        owner = self.denormalize_schema_name(
            schema or self.default_schema_name
        )
        query = (
            select(dictionary.all_views.c.text)
            .where(
                dictionary.all_views.c.view_name == name,
                dictionary.all_views.c.owner == owner,
            )
            .union_all(
                select(dictionary.all_mviews.c.query).where(
                    dictionary.all_mviews.c.mview_name == name,
                    dictionary.all_mviews.c.owner == owner,
                )
            )
        )

        rp = self._execute_reflection(
            connection, query, dblink, returns_long=False
        ).scalar()
        if rp is None:
            raise exc.NoSuchTableError(
                f"{schema}.{view_name}" if schema else view_name
            )
        else:
            return rp

    @reflection.cache
    def get_check_constraints(
            self, connection, table_name, schema=None, include_all=False, **kw
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``oracle_resolve_synonyms`` to resolve names to synonyms
        """
        data = self.get_multi_check_constraints(
            connection,
            schema=schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            include_all=include_all,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    @_handle_synonyms_decorator
    def get_multi_check_constraints(
            self,
            connection,
            *,
            schema,
            filter_names,
            dblink=None,
            scope,
            kind,
            include_all=False,
            **kw,
    ):
        """Supported kw arguments are: ``dblink`` to reflect via a db link;
        ``tibero_resolve_synonyms`` to resolve names to synonyms
        """
        all_objects = self._get_all_objects(
            connection, schema, scope, kind, filter_names, dblink, **kw
        )

        not_null = re.compile(r"..+?. IS NOT NULL$")

        check_constraints = defaultdict(list)

        for row_dict in self._get_all_constraint_rows(
                connection, schema, dblink, all_objects, **kw
        ):
            if row_dict["constraint_type"] != "C":
                continue
            table_name = self.normalize_name(row_dict["table_name"])
            constraint_name = self.normalize_name(row_dict["constraint_name"])
            search_condition = row_dict["search_condition"]

            table_checks = check_constraints[(schema, table_name)]
            if constraint_name is not None and (
                    include_all or not not_null.match(search_condition)
            ):
                table_checks.append(
                    {"name": constraint_name, "sqltext": search_condition}
                )

        default = ReflectionDefaults.check_constraints

        return (
            (
                key,
                (
                    check_constraints[key]
                    if key in check_constraints
                    else default()
                ),
            )
            for key in (
            (schema, self.normalize_name(obj_name))
            for obj_name in all_objects
        )
        )

    def _list_dblinks(self, connection, dblink=None):
        query = select(dictionary.all_db_links.c.db_link)
        links = self._execute_reflection(
            connection, query, dblink, returns_long=False
        ).scalars()
        return [self.normalize_name(link) for link in links]


class _OuterJoinColumn(sql.ClauseElement):
    __visit_name__ = "outer_join_column"

    def __init__(self, column):
        self.column = column
