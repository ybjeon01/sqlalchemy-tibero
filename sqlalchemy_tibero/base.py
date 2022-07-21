# tibero/base.py
# Copyright (C) 2007-2019 the SQLAlchemy authors and contributors <see AUTHORS file>
# Copyright (C) 2007 Paul Johnston, paj@pajhome.org.uk
# Portions derived from jet2sql.py by Matt Keranen, mksql@yahoo.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Support for the TmaxSoft Tibero database.

"""
import warnings
from itertools import groupby
import re
import os

from sqlalchemy import types, exc, pool, Computed, sql, util
from sqlalchemy import schema as sa_schema
from sqlalchemy.sql import compiler, expression, sqltypes, visitors
from sqlalchemy.sql import util as sql_util
from sqlalchemy.engine import default, reflection
from sqlalchemy.types import BLOB, CHAR, CLOB, FLOAT, INTEGER, NCHAR, NVARCHAR, TIMESTAMP, VARCHAR
from sqlalchemy.util import compat

import pyodbc

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
    "UID CURRENT_DATE SYSDATE USER " "CURRENT_TIME CURRENT_TIMESTAMP".split()
)

class RAW(sqltypes._Binary):
    __visit_name__ = "RAW"

TiberoRaw = RAW

class NCLOB(sqltypes.Text):
    __visit_name__ = "NCLOB"


class VARCHAR2(VARCHAR):
    __visit_name__ = "VARCHAR2"


NVARCHAR2 = NVARCHAR


class NUMBER(sqltypes.Numeric, sqltypes.Integer):
    __visit_name__ = "NUMBER"

    def __init__(self, precision=None, scale=None, asdecimal=None):
        if asdecimal is None:
            asdecimal = bool(scale and scale > 0)

        super(NUMBER, self).__init__(
            precision=precision, scale=scale, asdecimal=asdecimal
        )

    def adapt(self, impltype):
        ret = super(NUMBER, self).adapt(impltype)
        # leave a hint for the DBAPI handler
        ret._is_tibero_number = True
        return ret

    @property
    def _type_affinity(self):
        if bool(self.scale and self.scale > 0):
            return sqltypes.Numeric
        else:
            return sqltypes.Integer


class DOUBLE_PRECISION(sqltypes.Float):
    __visit_name__ = "DOUBLE_PRECISION"


class BINARY_DOUBLE(sqltypes.Float):
    __visit_name__ = "BINARY_DOUBLE"


class BINARY_FLOAT(sqltypes.Float):
    __visit_name__ = "BINARY_FLOAT"


class BFILE(sqltypes.LargeBinary):
    __visit_name__ = "BFILE"


class LONG(sqltypes.Text):
    __visit_name__ = "LONG"


class DATE(sqltypes.DateTime):
    """Provide the tibero DATE type.

    This type has no special Python behavior, except that it subclasses
    :class:`_types.DateTime`; this is to suit the fact that the Tibero
    ``DATE`` type supports a time value.

    .. versionadded:: 0.9.4

    """

    __visit_name__ = "DATE"

    def _compare_type_affinity(self, other):
        return other._type_affinity in (sqltypes.DateTime, sqltypes.Date)


class INTERVAL(sqltypes.NativeForEmulated, sqltypes._AbstractInterval):
    __visit_name__ = "INTERVAL"

    def __init__(self, day_precision=None, second_precision=None):
        """Construct an INTERVAL.

        Note that only DAY TO SECOND intervals are currently supported.
        This is due to a lack of support for YEAR TO MONTH intervals
        within available DBAPIs.

        :param day_precision: the day precision value.  this is the number of
          digits to store for the day field.  Defaults to "2"
        :param second_precision: the second precision value.  this is the
          number of digits to store for the fractional seconds field.
          Defaults to "6".

        """
        self.day_precision = day_precision
        self.second_precision = second_precision

    @classmethod
    def _adapt_from_generic_interval(cls, interval):
        return INTERVAL(
            day_precision=interval.day_precision,
            second_precision=interval.second_precision,
        )

    @property
    def _type_affinity(self):
        return sqltypes.Interval

    def as_generic(self, allow_nulltype=False):
        return sqltypes.Interval(
            native=True,
            second_precision=self.second_precision,
            day_precision=self.day_precision,
        )


class ROWID(sqltypes.TypeEngine):
    """Tibero ROWID type.

    When used in a cast() or similar, generates ROWID.

    """

    __visit_name__ = "ROWID"


class _TiberoBoolean(sqltypes.Boolean):
    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER


colspecs = {
    sqltypes.Boolean: _TiberoBoolean,
    sqltypes.Interval: INTERVAL,
    sqltypes.DateTime: DATE,
}

ischema_names = {
    "VARCHAR2": VARCHAR,
    "NVARCHAR2": NVARCHAR,
    "VARCHAR": VARCHAR,
    "NVARCHAR": NVARCHAR,
    "CHAR": CHAR,
    "NCHAR": NCHAR,
    "DATE": DATE,
    "DATETIME": DATE,
    "NUMBER": NUMBER,
    "BLOB": BLOB,
    "BFILE": BFILE,
    "CLOB": CLOB,
    "NCLOB": NCLOB,
    "TIMESTAMP": TIMESTAMP,
    "TIMESTAMP WITH TIME ZONE": TIMESTAMP,
    "INTERVAL DAY TO SECOND": INTERVAL,
    "RAW": RAW,
    "FLOAT": FLOAT,
    "DOUBLE PRECISION": DOUBLE_PRECISION,
    "LONG": LONG,
    "BINARY_DOUBLE": BINARY_DOUBLE,
    "BINARY_FLOAT": BINARY_FLOAT,
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
        if type_.timezone:
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
        # don't support conversion between decimal/binary
        # precision yet
        kw["no_precision"] = True
        return self._generate_numeric(type_, "FLOAT", **kw)

    def visit_NUMBER(self, type_, **kw):
        return self._generate_numeric(type_, "NUMBER", **kw)

    def _generate_numeric(
        self, type_, name, precision=None, scale=None, no_precision=False, **kw
    ):
        if precision is None:
            precision = type_.precision

        if scale is None:
            scale = getattr(type_, "scale", None)

        if no_precision or precision is None:
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
        return self.visit_NCLOB(type_, **kw)

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
        super(TiberoCompiler, self).__init__(*args, **kwargs)

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
        text = super(TiberoCompiler, self).visit_function(func, **kw)
        if kw.get("asfrom", False):
            text = "TABLE (%s)" % func
        return text

    def visit_table_valued_column(self, element, **kw):
        text = super(TiberoCompiler, self).visit_table_valued_column(
            element, **kw
        )
        text = "COLUMN_VALUE " + text
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
                # https://docs.tibero.com/database/121/SQLRF/queries006.htm#SQLRF52354
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

    def returning_clause(self, stmt, returning_cols):
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

            # ensure the ExecutionContext.get_out_parameters() method is
            # *not* called; the cx_Tibero dialect wants to handle these
            # parameters separately
            self.has_out_parameters = False

            columns.append(self.process(col_expr, within_columns_clause=False))

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
                and select._fetch_clause is None
            ):
                limit_clause = select._limit_clause
                offset_clause = select._offset_clause

                if select._simple_int_clause(limit_clause):
                    limit_clause = limit_clause.render_literal_execute()

                if select._simple_int_clause(offset_clause):
                    offset_clause = offset_clause.render_literal_execute()

                # currently using form at:
                # https://blogs.tibero.com/tiberomagazine/\
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

    def visit_empty_set_expr(self, type_):
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

    def _get_regexp_args(self, binary, kw):
        string = self.process(binary.left, **kw)
        pattern = self.process(binary.right, **kw)
        flags = binary.modifiers["flags"]
        if flags is not None:
            flags = self.process(flags, **kw)
        return string, pattern, flags

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        string, pattern, flags = self._get_regexp_args(binary, kw)
        if flags is None:
            return "REGEXP_LIKE(%s, %s)" % (string, pattern)
        else:
            return "REGEXP_LIKE(%s, %s, %s)" % (string, pattern, flags)

    def visit_not_regexp_match_op_binary(self, binary, operator, **kw):
        return "NOT %s" % self.visit_regexp_match_op_binary(
            binary, operator, **kw
        )

    def visit_regexp_replace_op_binary(self, binary, operator, **kw):
        string, pattern, flags = self._get_regexp_args(binary, kw)
        replacement = self.process(binary.modifiers["replacement"], **kw)
        if flags is None:
            return "REGEXP_REPLACE(%s, %s, %s)" % (
                string,
                pattern,
                replacement,
            )
        else:
            return "REGEXP_REPLACE(%s, %s, %s, %s)" % (
                string,
                pattern,
                replacement,
                flags,
            )


class TiberoDDLCompiler(compiler.DDLCompiler):
    def define_constraint_cascades(self, constraint):
        text = ""
        if constraint.ondelete is not None:
            text += " ON DELETE %s" % constraint.ondelete

        # tibero has no ON UPDATE CASCADE -
        # its only available via triggers
        # http://asktom.tibero.com/tkyte/update_cascade/index.html
        if constraint.onupdate is not None:
            util.warn(
                "Tibero does not contain native UPDATE CASCADE "
                "functionality - onupdates will not be rendered for foreign "
                "keys.  Consider using deferrable=True, initially='deferred' "
                "or triggers."
            )

        return text

    def visit_drop_table_comment(self, drop):
        return "COMMENT ON TABLE %s IS ''" % self.preparer.format_table(
            drop.element
        )

    def visit_create_index(self, create):
        index = create.element
        self._verify_index_table(index)
        preparer = self.preparer
        text = "CREATE "
        if index.unique:
            text += "UNIQUE "
        if index.dialect_options["tibero"]["bitmap"]:
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
        if index.dialect_options["tibero"]["compress"] is not False:
            if index.dialect_options["tibero"]["compress"] is True:
                text += " COMPRESS"
            else:
                text += " COMPRESS %d" % (
                    index.dialect_options["tibero"]["compress"]
                )
        return text

    def post_create_table(self, table):
        table_opts = []
        opts = table.dialect_options["tibero"]

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
        text = super(TiberoDDLCompiler, self).get_identity_options(
            identity_options
        )
        text = text.replace("NO MINVALUE", "NOMINVALUE")
        text = text.replace("NO MAXVALUE", "NOMAXVALUE")
        text = text.replace("NO CYCLE", "NOCYCLE")
        text = text.replace("NO ORDER", "NOORDER")
        return text

    def visit_computed_column(self, generated):
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
            or not self.legal_characters.match(util.text_type(value))
        )

    def format_savepoint(self, savepoint):
        name = savepoint.ident.lstrip("_")
        return super(TiberoIdentifierPreparer, self).format_savepoint(
            savepoint, name
        )


class TiberoExecutionContext(default.DefaultExecutionContext):
    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
            "SELECT "
            + self.identifier_preparer.format_sequence(seq)
            + ".nextval FROM DUAL",
            type_,
        )


class TiberoDialect(default.DefaultDialect):
    name = "tibero"
    #driver = "Tibero"

    bind_typing = 2
    supports_statement_cache = True
    supports_alter = True
    supports_unicode_statements = False
    supports_unicode_binds = False
    max_identifier_length = 128

    insert_returning = True
    update_returning = True
    delete_returning = True

    div_is_floordiv = False

    supports_simple_order_by_label = False
    cte_follows_insert = True

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
    type_compiler = TiberoTypeCompiler
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

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        #print("XXXX" + url + "XXXX")
        connectors = ["Driver=Tibero 6 ODBC Driver"]
        user = opts.get("username", None)
        if user:
            connectors.append("USER=%s" % user)
            connectors.append("PASSWORD=%s" % opts.get("password", ""))
        return [[";".join(connectors)], {}]

    def __init__(
        self,
        use_ansi=True,
        optimize_limits=False,
        use_binds_for_limits=None,
        use_nchar_for_unicode=False,
        exclude_tablespaces=("SYSTEM", "SYSSUB"),
        auto_convert_lobs=True,
        coerce_to_decimal=True,
        arraysize=50,
        encoding_errors=None,
        threaded=None,
        **kwargs
    ):
        default.DefaultDialect.__init__(self, **kwargs)
        self._use_nchar_for_unicode = use_nchar_for_unicode
        self.use_ansi = use_ansi
        self.optimize_limits = optimize_limits
        self.exclude_tablespaces = exclude_tablespaces
        os.environ.update([
            # Tibero takes client-side character set encoding from the environment.
            ('TB_NLS_LANG', 'UTF8'),
            # This prevents unicode from getting mangled by getting encoded into the
            # potentially non-unicode database character set.
            ('TBCLI_WCHAR_TYPE', 'UCS2'),
            #('ORA_NCHAR_LITERAL_REPLACE', 'TRUE'),
        ])
        self.arraysize = arraysize
        self.encoding_errors = encoding_errors
        if encoding_errors:
            self._cursor_var_unicode_kwargs = {
                "encodingErrors": encoding_errors
            }
        if threaded is not None:
            self._cx_oracle_threaded = threaded
        self.auto_convert_lobs = auto_convert_lobs
        self.coerce_to_decimal = coerce_to_decimal
        self.include_set_input_sizes = {
            NCLOB,
            CLOB,
            NCHAR,
            BLOB,
            TIMESTAMP,
        }
        self._paramval = lambda value: value.getvalue()

    def initialize(self, connection):
        super(TiberoDialect, self).initialize(connection)
        connection.maxwrite=32767

    @property
    def _supports_table_compression(self):
        return True

    @property
    def _supports_table_compress_for(self):
        return True

    @property
    def _supports_char_length(self):
        return True

    @property
    def _supports_update_returning_computed_cols(self):
        return False

    def do_release_savepoint(self, connection, name):
        pass

    def _check_max_identifier_length(self, connection):
        return 30

    def _check_unicode_returns(self, connection):
        additional_tests = [
            expression.cast(
                expression.literal_column("'test nvarchar2 returns'"),
                sqltypes.NVARCHAR(60),
            )
        ]
        return super(TiberoDialect, self)._check_unicode_returns(
            connection, additional_tests
        )

    _isolation_lookup = ["READ COMMITTED", "SERIALIZABLE"]

    def get_default_isolation_level(self, dbapi_conn):
        try:
            return self.get_isolation_level(dbapi_conn)
        except NotImplementedError:
            raise
        except:
            return "READ COMMITTED"

    def has_table(self, connection, table_name, schema=None):
        #self._ensure_has_table_connection(connection)

        if not schema:
            schema = self.default_schema_name
        text = "SELECT table_name FROM all_tables WHERE table_name = :name AND owner = :schema_name"
        params = dict(name=self.denormalize_name(table_name), schema_name=self.denormalize_name(schema),)
        cursor = connection.execute(sql.text(text), params)
        return cursor.first() is not None

    def has_sequence(self, connection, sequence_name, schema=None):
        if not schema:
            schema = self.default_schema_name
        cursor = connection.execute(
            sql.text(
                "SELECT sequence_name FROM all_sequences "
                "WHERE sequence_name = :name AND "
                "sequence_owner = :schema_name"
            ),
            dict(
                name=self.denormalize_name(sequence_name),
                schema_name=self.denormalize_name(schema),
            ),
        )
        return cursor.first() is not None

    def _get_default_schema_name(self, connection):
        s = "SELECT sys_context('userenv','current_schema') FROM dual"
        cursor = connection.execute(sql.text(s))
        return cursor.fetchone()[0]

    def _resolve_synonym(
        self,
        connection,
        desired_owner=None,
        desired_synonym=None,
        desired_table=None,
    ):
        """search for a local synonym matching the given desired owner/name.

        if desired_owner is None, attempts to locate a distinct owner.

        returns the actual name, owner, dblink name, and synonym name if
        found.
        """

        q = (
            "SELECT owner, table_owner, table_name, db_link, "
            "synonym_name FROM all_synonyms WHERE "
        )
        clauses = []
        params = {}
        if desired_synonym:
            clauses.append("synonym_name = :synonym_name")
            params["synonym_name"] = desired_synonym
        if desired_owner:
            clauses.append("owner = :desired_owner")
            params["desired_owner"] = desired_owner
        if desired_table:
            clauses.append("table_name = :tname")
            params["tname"] = desired_table

        q += " AND ".join(clauses)

        result = connection.execution_options(future_result=True).execute(
            sql.text(q), params
        )
        if desired_owner:
            row = result.mappings().first()
            if row:
                return (
                    row["table_name"],
                    row["table_owner"],
                    row["db_link"],
                    row["synonym_name"],
                )
            else:
                return None, None, None, None
        else:
            rows = result.mappings().all()
            if len(rows) > 1:
                raise AssertionError(
                    "There are multiple tables visible to the schema, you "
                    "must specify owner"
                )
            elif len(rows) == 1:
                row = rows[0]
                return (
                    row["table_name"],
                    row["table_owner"],
                    row["db_link"],
                    row["synonym_name"],
                )
            else:
                return None, None, None, None

    @reflection.cache
    def _prepare_reflection_args(
        self,
        connection,
        table_name,
        schema=None,
        resolve_synonyms=False,
        dblink="",
        **kw
    ):

        if resolve_synonyms:
            actual_name, owner, dblink, synonym = self._resolve_synonym(
                connection,
                desired_owner=self.denormalize_name(schema),
                desired_synonym=self.denormalize_name(table_name),
            )
        else:
            actual_name, owner, dblink, synonym = None, None, None, None
        if not actual_name:
            actual_name = self.denormalize_name(table_name)

        if dblink:
            # using user_db_links here since all_db_links appears
            # to have more restricted permissions.
            # http://docs.tibero.com/cd/B28359_01/server.111/b28310/ds_admin005.htm
            # will need to hear from more users if we are doing
            # the right thing here.  See [ticket:2619]
            owner = connection.scalar(
                sql.text(
                    "SELECT username FROM user_db_links " "WHERE db_link=:link"
                ),
                dict(link=dblink),
            )
            dblink = "@" + dblink
        elif not owner:
            owner = self.denormalize_name(schema or self.default_schema_name)

        return (actual_name, owner, dblink or "", synonym)

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        s = "SELECT username FROM all_users ORDER BY username"
        cursor = connection.execute(sql.text(s))
        return [self.normalize_name(row[0]) for row in cursor]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        schema = self.denormalize_name(schema or self.default_schema_name)

        # note that table_names() isn't loading DBLINKed or synonym'ed tables
        if schema is None:
            schema = self.default_schema_name

        sql_str = "SELECT table_name FROM all_tables WHERE "
        if self.exclude_tablespaces:
            sql_str += (
                "nvl(tablespace_name, 'no tablespace') "
                "NOT IN (%s) AND "
                % (", ".join(["'%s'" % ts for ts in self.exclude_tablespaces]))
            )
        sql_str += (
            "OWNER = :owner "
            "AND DURATION IS NULL"
        )

        cursor = connection.execute(sql.text(sql_str), dict(owner=schema))
        return [self.denormalize_name(row[0]) for row in cursor]

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

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        schema = self.denormalize_name(schema or self.default_schema_name)
        s = sql.text("SELECT view_name FROM all_views WHERE owner = :owner")
        cursor = connection.execute(
            s, dict(owner=self.denormalize_name(schema))
        )
        return [self.normalize_name(row[0]) for row in cursor]

    @reflection.cache
    def get_sequence_names(self, connection, schema=None, **kw):
        if not schema:
            schema = self.default_schema_name
        cursor = connection.execute(
            sql.text(
                "SELECT sequence_name FROM all_sequences "
                "WHERE sequence_owner = :schema_name"
            ),
            dict(schema_name=self.denormalize_name(schema)),
        )
        return [self.normalize_name(row[0]) for row in cursor]

    @reflection.cache
    def get_table_options(self, connection, table_name, schema=None, **kw):
        options = {}

        resolve_synonyms = kw.get("tibero_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        params = {"table_name": table_name}

        columns = ["table_name"]
        if self._supports_table_compression:
            columns.append("compression")
        if self._supports_table_compress_for:
            columns.append("compress_for")

        text = (
            "SELECT %(columns)s "
            "FROM ALL_TABLES%(dblink)s "
            "WHERE table_name = :table_name"
        )

        if schema is not None:
            params["owner"] = schema
            text += " AND owner = :owner "
        text = text % {"dblink": dblink, "columns": ", ".join(columns)}

        result = connection.execute(sql.text(text), params)

        enabled = dict(DISABLED=False, ENABLED=True)

        row = result.first()
        if row:
            if enabled.get(row.compression, False):
                options["tibero_compress"] = row.compress_for

        return options

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        kw arguments can be:
            tibero_resolve_synonyms
            dblink
        """
        resolve_synonyms = kw.get("tibero_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )
        columns = []
        if self._supports_char_length:
            char_length_col = "char_length"
        else:
            char_length_col = "data_length"

        identity_cols = ", NULL as default_on_null, NULL as identity_options"

        params = {"table_name": table_name}
        text = """
            SELECT
                col.column_name,
                col.data_type,
                col.%(char_length_col)s,
                col.data_precision,
                col.data_scale,
                col.nullable,
                col.data_default,
                com.comments,
                col.virtual_column
                %(identity_cols)s
            FROM all_tab_cols%(dblink)s col
            LEFT JOIN all_col_comments%(dblink)s com
            ON col.table_name = com.table_name
            AND col.column_name = com.column_name
            AND col.owner = com.owner
            WHERE col.table_name = :table_name
            AND col.hidden_column = 'N'
        """
        if schema is not None:
            params["owner"] = schema
            text += " AND col.owner = :owner "
        text += " ORDER BY col.column_id"
        text = text % {
            "dblink": dblink,
            "char_length_col": char_length_col,
            "identity_cols": identity_cols,
        }

        c = connection.execute(sql.text(text), params)

        for row in c:
            colname = self.normalize_name(row[0])
            orig_colname = row[0]
            coltype = row[1]
            length = row[2]
            precision = row[3]
            scale = row[4]
            nullable = row[5] == "Y"
            default = row[6]
            comment = row[7]
            generated = row[8]
            default_on_nul = row[9]
            identity_options = row[10]

            if coltype == "NUMBER":
                if precision is None and scale == 0:
                    coltype = INTEGER()
                else:
                    coltype = NUMBER(precision, scale)
            elif coltype == "FLOAT":
                # TODO: support "precision" here as "binary_precision"
                coltype = FLOAT()
            elif coltype in ("VARCHAR", "NVARCHAR", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"):
                coltype = self.ischema_names.get(coltype)(length)
            elif "WITH TIME ZONE" in coltype:
                coltype = TIMESTAMP(timezone=True)
            else:
                coltype = re.sub(r"\(\d+\)", "", coltype)
                try:
                    coltype = self.ischema_names[coltype]
                except KeyError:
                    util.warn(
                        "Did not recognize type '%s' of column '%s'"
                        % (coltype, colname)
                    )
                    coltype = sqltypes.NULLTYPE

            if generated == "YES":
                computed = dict(sqltext=default)
                default = None
            else:
                computed = None

            if identity_options is not None:
                identity = self._parse_identity_options(
                    identity_options, default_on_nul
                )
                default = None
            else:
                identity = None

            cdict = {
                "name": colname,
                "type": coltype,
                "nullable": nullable,
                "default": default,
                "autoincrement": "auto",
                "comment": comment,
            }
            if orig_colname.lower() == orig_colname:
                cdict["quote"] = True
            if computed is not None:
                cdict["computed"] = computed
            if identity is not None:
                cdict["identity"] = identity

            columns.append(cdict)
        return columns

    def _parse_identity_options(self, identity_options, default_on_nul):
        # identity_options is a string that starts with 'ALWAYS,' or
        # 'BY DEFAULT,' and continues with
        # START WITH: 1, INCREMENT BY: 1, MAX_VALUE: 123, MIN_VALUE: 1,
        # CYCLE_FLAG: N, CACHE_SIZE: 1, ORDER_FLAG: N, SCALE_FLAG: N,
        # EXTEND_FLAG: N, SESSION_FLAG: N, KEEP_VALUE: N
        parts = [p.strip() for p in identity_options.split(",")]
        identity = {
            "always": parts[0] == "ALWAYS",
            "on_null": default_on_nul == "YES",
        }

        for part in parts[1:]:
            option, value = part.split(":")
            value = value.strip()

            if "START WITH" in option:
                identity["start"] = compat.long_type(value)
            elif "INCREMENT BY" in option:
                identity["increment"] = compat.long_type(value)
            elif "MAX_VALUE" in option:
                identity["maxvalue"] = compat.long_type(value)
            elif "MIN_VALUE" in option:
                identity["minvalue"] = compat.long_type(value)
            elif "CYCLE_FLAG" in option:
                identity["cycle"] = value == "Y"
            elif "CACHE_SIZE" in option:
                identity["cache"] = compat.long_type(value)
            elif "ORDER_FLAG" in option:
                identity["order"] = value == "Y"
        return identity

    @reflection.cache
    def get_table_comment(
        self,
        connection,
        table_name,
        schema=None,
        resolve_synonyms=False,
        dblink="",
        **kw
    ):

        info_cache = kw.get("info_cache")
        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        if not schema:
            schema = self.default_schema_name

        COMMENT_SQL = """
            SELECT comments
            FROM all_tab_comments
            WHERE table_name = :table_name AND owner = :schema_name
        """

        c = connection.execute(
            sql.text(COMMENT_SQL),
            dict(table_name=table_name, schema_name=schema),
        )
        return {"text": c.scalar()}

    @reflection.cache
    def get_indexes(
        self,
        connection,
        table_name,
        schema=None,
        resolve_synonyms=False,
        dblink="",
        **kw
    ):

        info_cache = kw.get("info_cache")
        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )
        indexes = []

        params = {"table_name": table_name}
        text = (
            "SELECT a.index_name, a.column_name, "
            "\nb.index_type, b.uniqueness, 'DISABLED' as compression, 1 as prefix_length "
            "\nFROM ALL_IND_COLUMNS%(dblink)s a, "
            "\nALL_INDEXES%(dblink)s b "
            "\nWHERE "
            "\na.index_name = b.index_name "
            "\nAND a.table_owner = b.table_owner "
            "\nAND a.table_name = b.table_name "
            "\nAND a.table_name = :table_name "
        )

        if schema is not None:
            params["schema"] = schema
            text += "AND a.table_owner = :schema "

        text += "ORDER BY a.index_name, a.column_position"

        text = text % {"dblink": dblink}

        q = sql.text(text)
        rp = connection.execute(q, params)
        indexes = []
        last_index_name = None
        pk_constraint = self.get_pk_constraint(
            connection,
            table_name,
            schema,
            resolve_synonyms=resolve_synonyms,
            dblink=dblink,
            info_cache=kw.get("info_cache"),
        )

        uniqueness = dict(NONUNIQUE=False, UNIQUE=True)
        enabled = dict(DISABLED=False, ENABLED=True)

        tibero_sys_col = re.compile(r"SYS_NC\d+\$", re.IGNORECASE)

        index = None
        for rset in rp:
            index_name_normalized = self.normalize_name(rset.index_name)

            # skip primary key index.  This is refined as of
            # [ticket:5421].  Note that ALL_INDEXES.GENERATED will by "Y"
            # if the name of this index was generated by Tibero, however
            # if a named primary key constraint was created then this flag
            # is false.
            if (
                pk_constraint
                and index_name_normalized == pk_constraint["name"]
            ):
                continue

            if rset.index_name != last_index_name:
                index = dict(
                    name=index_name_normalized,
                    column_names=[],
                    dialect_options={},
                )
                indexes.append(index)
            index["unique"] = uniqueness.get(rset.uniqueness, False)

            if rset.index_type in ("BITMAP", "FUNCTION-BASED BITMAP"):
                index["dialect_options"]["tibero_bitmap"] = True
            if enabled.get(rset.compression, False):
                index["dialect_options"][
                    "tibero_compress"
                ] = rset.prefix_length

            # filter out Tibero SYS_NC names.  could also do an outer join
            # to the all_tab_columns table and check for real col names there.
            if not tibero_sys_col.match(rset.column_name):
                index["column_names"].append(
                    self.normalize_name(rset.column_name)
                )
            last_index_name = rset.index_name

        return indexes

    @reflection.cache
    def _get_constraint_data(
        self, connection, table_name, schema=None, dblink="", **kw
    ):

        params = {"table_name": table_name}

        text = (
            "SELECT"
            "\nac.constraint_name,"  # 0
            "\nac.constraint_type,"  # 1
            "\nloc.column_name AS local_column,"  # 2
            "\nrem.table_name AS remote_table,"  # 3
            "\nrem.column_name AS remote_column,"  # 4
            "\nrem.owner AS remote_owner,"  # 5
            "\nloc.position as loc_pos,"  # 6
            "\nrem.position as rem_pos,"  # 7
            "\nac.search_condition,"  # 8
            "\nac.delete_rule"  # 9
            "\nFROM all_constraints%(dblink)s ac,"
            "\nall_cons_columns%(dblink)s loc,"
            "\nall_cons_columns%(dblink)s rem"
            "\nWHERE ac.table_name = :table_name"
            "\nAND ac.constraint_type IN ('R','P', 'U', 'C')"
        )

        if schema is not None:
            params["owner"] = schema
            text += "\nAND ac.owner = :owner"

        text += (
            "\nAND ac.owner = loc.owner"
            "\nAND ac.constraint_name = loc.constraint_name"
            "\nAND ac.r_owner = rem.owner(+)"
            "\nAND ac.r_constraint_name = rem.constraint_name(+)"
            "\nAND (rem.position IS NULL or loc.position=rem.position)"
            "\nORDER BY ac.constraint_name, loc.position"
        )

        text = text % {"dblink": dblink}
        rp = connection.execute(sql.text(text), params)
        constraint_data = rp.fetchall()
        return constraint_data

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        resolve_synonyms = kw.get("tibero_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )
        pkeys = []
        constraint_name = None
        constraint_data = self._get_constraint_data(
            connection,
            table_name,
            schema,
            dblink,
            info_cache=kw.get("info_cache"),
        )

        for row in constraint_data:
            (
                cons_name,
                cons_type,
                local_column,
                remote_table,
                remote_column,
                remote_owner,
            ) = row[0:2] + tuple([self.normalize_name(x) for x in row[2:6]])
            if cons_type == "P":
                if constraint_name is None:
                    constraint_name = self.normalize_name(cons_name)
                pkeys.append(local_column)
        return {"constrained_columns": pkeys, "name": constraint_name}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """

        kw arguments can be:

            tibero_resolve_synonyms

            dblink

        """
        requested_schema = schema  # to check later on
        resolve_synonyms = kw.get("tibero_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        constraint_data = self._get_constraint_data(
            connection,
            table_name,
            schema,
            dblink,
            info_cache=kw.get("info_cache"),
        )

        def fkey_rec():
            return {
                "name": None,
                "constrained_columns": [],
                "referred_schema": None,
                "referred_table": None,
                "referred_columns": [],
                "options": {},
            }

        fkeys = util.defaultdict(fkey_rec)

        for row in constraint_data:
            (
                cons_name,
                cons_type,
                local_column,
                remote_table,
                remote_column,
                remote_owner,
            ) = row[0:2] + tuple([self.normalize_name(x) for x in row[2:6]])

            cons_name = self.normalize_name(cons_name)

            if cons_type == "R":
                if remote_table is None:
                    # ticket 363
                    util.warn(
                        (
                            "Got 'None' querying 'table_name' from "
                            "all_cons_columns%(dblink)s - does the user have "
                            "proper rights to the table?"
                        )
                        % {"dblink": dblink}
                    )
                    continue

                rec = fkeys[cons_name]
                rec["name"] = cons_name
                local_cols, remote_cols = (
                    rec["constrained_columns"],
                    rec["referred_columns"],
                )

                if not rec["referred_table"]:
                    if resolve_synonyms:
                        (
                            ref_remote_name,
                            ref_remote_owner,
                            ref_dblink,
                            ref_synonym,
                        ) = self._resolve_synonym(
                            connection,
                            desired_owner=self.denormalize_name(remote_owner),
                            desired_table=self.denormalize_name(remote_table),
                        )
                        if ref_synonym:
                            remote_table = self.normalize_name(ref_synonym)
                            remote_owner = self.normalize_name(
                                ref_remote_owner
                            )

                    rec["referred_table"] = remote_table

                    if (
                        requested_schema is not None
                        or self.denormalize_name(remote_owner) != schema
                    ):
                        rec["referred_schema"] = remote_owner

                    if row[9] != "NO ACTION":
                        rec["options"]["ondelete"] = row[9]

                local_cols.append(local_column)
                remote_cols.append(remote_column)

        return list(fkeys.values())

    @reflection.cache
    def get_unique_constraints(
        self, connection, table_name, schema=None, **kw
    ):
        resolve_synonyms = kw.get("tibero_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        constraint_data = self._get_constraint_data(
            connection,
            table_name,
            schema,
            dblink,
            info_cache=kw.get("info_cache"),
        )

        unique_keys = filter(lambda x: x[1] == "U", constraint_data)
        uniques_group = groupby(unique_keys, lambda x: x[0])

        index_names = {
            ix["name"]
            for ix in self.get_indexes(connection, table_name, schema=schema)
        }
        return [
            {
                "name": name,
                "column_names": cols,
                "duplicates_index": name if name in index_names else None,
            }
            for name, cols in [
                [
                    self.normalize_name(i[0]),
                    [self.normalize_name(x[2]) for x in i[1]],
                ]
                for i in uniques_group
            ]
        ]

    @reflection.cache
    def get_view_definition(
        self,
        connection,
        view_name,
        schema=None,
        resolve_synonyms=False,
        dblink="",
        **kw
    ):
        info_cache = kw.get("info_cache")
        (view_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            view_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        params = {"view_name": view_name}
        text = "SELECT text FROM all_views WHERE view_name=:view_name"

        if schema is not None:
            text += " AND owner = :schema"
            params["schema"] = schema

        rp = connection.execute(sql.text(text), params).scalar()
        if rp:
            if util.py2k:
                rp = rp.decode(self.encoding)
            return rp
        else:
            return None

    @reflection.cache
    def get_check_constraints(
        self, connection, table_name, schema=None, include_all=False, **kw
    ):
        resolve_synonyms = kw.get("tibero_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        constraint_data = self._get_constraint_data(
            connection,
            table_name,
            schema,
            dblink,
            info_cache=kw.get("info_cache"),
        )

        check_constraints = filter(lambda x: x[1] == "C", constraint_data)

        return [
            {"name": self.normalize_name(cons[0]), "sqltext": cons[8]}
            for cons in check_constraints
            if include_all or not re.match(r"..+?. IS NOT NULL$", cons[8])
        ]


class _OuterJoinColumn(sql.ClauseElement):
    __visit_name__ = "outer_join_column"

    def __init__(self, column):
        self.column = column
