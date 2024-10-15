import datetime
import decimal
import os
import random

from sqlalchemy import bindparam
from sqlalchemy import cast
from sqlalchemy import CHAR
from sqlalchemy import DATE
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Double
from sqlalchemy import DOUBLE_PRECISION
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import FLOAT
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import LargeBinary
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import NCHAR
from sqlalchemy import Numeric
from sqlalchemy import NVARCHAR
from sqlalchemy import select
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy import TIMESTAMP
from sqlalchemy import TypeDecorator
from sqlalchemy import types as sqltypes
from sqlalchemy import Unicode
from sqlalchemy import UnicodeText
from sqlalchemy import VARCHAR

from sqlalchemy.sql import column
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.util import b
from sqlalchemy.util.concurrency import await_fallback

from sqlalchemy_tibero import base as tibero
from sqlalchemy_tibero import pyodbc


def exec_sql(conn, sql, *args, **kwargs):
    return conn.exec_driver_sql(sql, *args, **kwargs)


class DialectTypesTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = tibero.TiberoDialect()

    def test_no_clobs_for_string_params(self):
        """test that simple string params get a DBAPI type of
        VARCHAR, not CLOB. This is to prevent setinputsizes
        from setting up cx_oracle.CLOBs on
        string-based bind params [ticket:793]."""

        class FakeDBAPI:
            def __getattr__(self, attr):
                return attr

        dialect = tibero.TiberoDialect()
        dbapi = FakeDBAPI()

        b = bindparam("foo", "hello world!")
        eq_(b.type.dialect_impl(dialect).get_dbapi_type(dbapi), "STRING")

        b = bindparam("foo", "hello world!")
        eq_(b.type.dialect_impl(dialect).get_dbapi_type(dbapi), "STRING")

    def test_long(self):
        self.assert_compile(tibero.LONG(), "LONG")

    @testing.combinations(
        (Date(), pyodbc._PYODBCTiberoDate),
        (tibero.TiberoRaw(), pyodbc._TiberoRaw),
        (String(), String),
        (VARCHAR(), pyodbc._TiberoString),
        (DATE(), pyodbc._PYODBCTiberoDate),
        (tibero.DATE(), tibero.DATE),
        (String(50), pyodbc._TiberoString),
        (Unicode(), pyodbc._TiberoUnicodeStringCHAR),
        (Text(), pyodbc._TiberoText),
        (UnicodeText(), pyodbc._TiberoUnicodeTextCLOB),
        (CHAR(), pyodbc._TiberoChar),
        (NCHAR(), pyodbc._TiberoNChar),
        (NVARCHAR(), pyodbc._TiberoUnicodeStringNCHAR),
        (tibero.RAW(50), pyodbc._TiberoRaw),
        argnames="start, test",
    )
    @testing.combinations(pyodbc, argnames="module")
    def test_type_adapt(self, start, test, module):
        dialect = module.dialect()

        assert isinstance(start.dialect_impl(dialect), test), (
            "wanted %r got %r" % (test, start.dialect_impl(dialect))
        )

    @testing.combinations(
        (String(), String),
        (VARCHAR(), pyodbc._TiberoString),
        (String(50), pyodbc._TiberoString),
        (Unicode(), pyodbc._TiberoUnicodeStringNCHAR),
        (Text(), pyodbc._TiberoText),
        (UnicodeText(), pyodbc._TiberoUnicodeTextNCLOB),
        (NCHAR(), pyodbc._TiberoNChar),
        (NVARCHAR(), pyodbc._TiberoUnicodeStringNCHAR),
        argnames="start, test",
    )
    @testing.combinations(pyodbc, argnames="module")
    def test_type_adapt_nchar(self, start, test, module):
        dialect = module.dialect(use_nchar_for_unicode=True)

        a = start.dialect_impl(dialect)
        assert isinstance(start.dialect_impl(dialect), test), (
            "wanted %r got %r" % (test, start.dialect_impl(dialect))
        )

    def test_raw_compile(self):
        self.assert_compile(tibero.RAW(), "RAW")
        self.assert_compile(tibero.RAW(35), "RAW(35)")

    def test_char_length(self):
        self.assert_compile(VARCHAR(50), "VARCHAR(50 CHAR)")

        # 원본 코드에는 oracle 8를 테스트하는 코드가 있습니다. 하지만 Tibero Dialect는
        # 7부터 지원하기 때문에 이 코드가 필요없습니다. 원본 코드이 이러한 코드가 있다는 것을
        # 알리기 위해 삭제하지 않고 주석처리했습니다.
        # tibero8dialect = tibero.dialect()
        # tibero8dialect.server_version_info = (6, 0)
        # self.assert_compile(VARCHAR(50), "VARCHAR(50)", dialect=tibero8dialect)

        self.assert_compile(NVARCHAR(50), "NVARCHAR2(50)")
        self.assert_compile(CHAR(50), "CHAR(50)")

    @testing.combinations(
        (String(50), "VARCHAR2(50 CHAR)"),
        (Unicode(50), "VARCHAR2(50 CHAR)"),
        (NVARCHAR(50), "NVARCHAR2(50)"),
        (VARCHAR(50), "VARCHAR(50 CHAR)"),
        (tibero.NVARCHAR2(50), "NVARCHAR2(50)"),
        (tibero.VARCHAR2(50), "VARCHAR2(50 CHAR)"),
        (String(), "VARCHAR2"),
        (Unicode(), "VARCHAR2"),
        (NVARCHAR(), "NVARCHAR2"),
        (VARCHAR(), "VARCHAR"),
        (tibero.NVARCHAR2(), "NVARCHAR2"),
        (tibero.VARCHAR2(), "VARCHAR2"),
    )
    def test_varchar_types(self, typ, exp):
        dialect = tibero.dialect()
        self.assert_compile(typ, exp, dialect=dialect)

    @testing.combinations(
        (String(50), "VARCHAR2(50 CHAR)"),
        (Unicode(50), "NVARCHAR2(50)"),
        (NVARCHAR(50), "NVARCHAR2(50)"),
        (VARCHAR(50), "VARCHAR(50 CHAR)"),
        (tibero.NVARCHAR2(50), "NVARCHAR2(50)"),
        (tibero.VARCHAR2(50), "VARCHAR2(50 CHAR)"),
        (String(), "VARCHAR2"),
        (Unicode(), "NVARCHAR2"),
        (NVARCHAR(), "NVARCHAR2"),
        (VARCHAR(), "VARCHAR"),
        (tibero.NVARCHAR2(), "NVARCHAR2"),
        (tibero.VARCHAR2(), "VARCHAR2"),
    )
    def test_varchar_use_nchar_types(self, typ, exp):
        dialect = tibero.dialect(use_nchar_for_unicode=True)
        self.assert_compile(typ, exp, dialect=dialect)

    @testing.combinations(
        (tibero.INTERVAL(), "INTERVAL DAY TO SECOND"),
        (tibero.INTERVAL(day_precision=3), "INTERVAL DAY(3) TO SECOND"),
        (tibero.INTERVAL(second_precision=5), "INTERVAL DAY TO SECOND(5)"),
        (
            tibero.INTERVAL(day_precision=2, second_precision=5),
            "INTERVAL DAY(2) TO SECOND(5)",
        ),
        (
            sqltypes.Interval(day_precision=9, second_precision=3),
            "INTERVAL DAY(9) TO SECOND(3)",
        ),
    )
    def test_interval(self, type_, expected):
        self.assert_compile(type_, expected)

    def test_interval_coercion_literal(self):
        expr = column("bar", tibero.INTERVAL) == datetime.timedelta(days=1)
        eq_(expr.right.type._type_affinity, sqltypes.Interval)

    @testing.combinations(
        ("sa", sqltypes.Float(), "FLOAT"),
        ("sa", sqltypes.Double(), "DOUBLE PRECISION"),
        ("sa", sqltypes.FLOAT(), "FLOAT"),
        ("sa", sqltypes.REAL(), "REAL"),
        ("sa", sqltypes.DOUBLE(), "DOUBLE"),
        ("sa", sqltypes.DOUBLE_PRECISION(), "DOUBLE PRECISION"),
        ("tibero", tibero.FLOAT(), "FLOAT"),
        ("tibero", tibero.DOUBLE_PRECISION(), "DOUBLE PRECISION"),
        ("tibero", tibero.REAL(), "REAL"),
        ("tibero", tibero.BINARY_DOUBLE(), "BINARY_DOUBLE"),
        ("tibero", tibero.BINARY_FLOAT(), "BINARY_FLOAT"),
        id_="ira",
    )
    def test_float_type_compile(self, type_, sql_text):
        self.assert_compile(type_, sql_text)

    @testing.combinations(
        (
            text("select :parameter from dual").bindparams(
                parameter=datetime.timedelta(days=2)
            ),
            "select NUMTODSINTERVAL(172800.0, 'SECOND') from dual",
        ),
        (
            text("SELECT :parameter from dual").bindparams(
                parameter=datetime.timedelta(days=1, minutes=3, seconds=4)
            ),
            "SELECT NUMTODSINTERVAL(86584.0, 'SECOND') from dual",
        ),
        (
            text("select :parameter - :parameter2 from dual").bindparams(
                parameter=datetime.timedelta(days=1, minutes=3, seconds=4),
                parameter2=datetime.timedelta(days=0, minutes=1, seconds=4),
            ),
            (
                "select NUMTODSINTERVAL(86584.0, 'SECOND') - "
                "NUMTODSINTERVAL(64.0, 'SECOND') from dual"
            ),
        ),
    )
    def test_interval_literal_processor(self, type_, expected):
        self.assert_compile(type_, expected, literal_binds=True)


class TypesTest(fixtures.TestBase):
    # 테스트할 때는 TiberoDialect의 name이 "oracle"이기 때문에 남겨두었습니다.
    __only_on__ = "oracle"
    __dialect__ = tibero.TiberoDialect()
    __backend__ = True

    @testing.combinations((CHAR,), (NCHAR,), argnames="char_type")
    def test_fixed_char(self, metadata, connection, char_type):
        m = metadata
        t = Table(
            "t1",
            m,
            Column("id", Integer, primary_key=True),
            Column("data", char_type(30), nullable=False),
        )
        v1, v2, v3 = "value 1", "value 2", "value 3"

        t.create(connection)
        connection.execute(
            t.insert(),
            [
                dict(id=1, data=v1),
                dict(id=2, data=v2),
                dict(id=3, data=v3),
            ],
        )

        eq_(
            connection.execute(t.select().where(t.c.data == v2)).fetchall(),
            [(2, "value 2                       ")],
        )

        m2 = MetaData()
        t2 = Table("t1", m2, autoload_with=connection)
        is_(type(t2.c.data.type), char_type)
        eq_(
            connection.execute(t2.select().where(t2.c.data == v2)).fetchall(),
            [(2, "value 2                       ")],
        )

    @testing.requires.insert_returning
    def test_int_not_float(self, metadata, connection):
        m = metadata
        t1 = Table("t1", m, Column("foo", Integer))
        t1.create(connection)
        r = connection.execute(t1.insert().values(foo=5).returning(t1.c.foo))
        x = r.scalar()
        assert x == 5
        assert isinstance(x, int)

        x = connection.scalar(t1.select())
        assert x == 5
        assert isinstance(x, int)

    # coerce_to_decimal은 oracle driver의 outputtypehandler와 같은 수준의
    # api가 pyodbc에서 제공되어야 지원가능합니다. 따라서 tibero_pyodbc에서는
    # coerce_to_decimal parameter를 제거했고 아래의 테스트는 실행될 수 없습니다.
    # 함수 이름을 남긴 이유는 oracle dialect에 이러한 테스트가 있는 걸 알려주기 위함입니다.
    @testing.requires.insert_returning
    def _test_int_not_float_no_coerce_decimal(self, metadata):
        pass

    def test_integer_truediv(self, connection):
        """test #4926"""

        stmt = select(literal(1, Integer) / literal(2, Integer))
        eq_(connection.scalar(stmt), decimal.Decimal("0.5"))

    def test_rowid(self, metadata, connection):
        t = Table("t1", metadata, Column("x", Integer))

        t.create(connection)
        connection.execute(t.insert(), {"x": 5})
        s1 = select(t).subquery()
        s2 = select(column("rowid")).select_from(s1)
        rowid = connection.scalar(s2)

        rowid_col = column("rowid", tibero.ROWID)
        s3 = select(t.c.x, rowid_col).where(
            rowid_col == cast(rowid, tibero.ROWID)
        )
        eq_(connection.execute(s3).fetchall(), [(5, rowid)])

    def test_interval(self, metadata, connection):
        interval_table = Table(
            "intervaltable",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("day_interval", tibero.INTERVAL(day_precision=3)),
        )
        metadata.create_all(connection)

        connection.execute(
            interval_table.insert(),
            dict(day_interval=datetime.timedelta(days=35, seconds=5743)),
        )

        row = connection.execute(interval_table.select()).first()
        eq_(
            row._mapping["day_interval"],
            datetime.timedelta(days=35, seconds=5743),
        )

    def test_interval_literal_processor(self, connection):
        stmt = text("select :parameter - :parameter2 from dual")
        result = connection.execute(
            stmt.bindparams(
                bindparam(
                    "parameter",
                    datetime.timedelta(days=1, minutes=3, seconds=4),
                    literal_execute=True,
                ),
                bindparam(
                    "parameter2",
                    datetime.timedelta(days=0, minutes=1, seconds=4),
                    literal_execute=True,
                ),
            )
        ).one()
        eq_(result[0], datetime.timedelta(days=1, seconds=120))

    def test_no_decimal_float_precision(self):
        with expect_raises_message(
            exc.ArgumentError,
            "Tibero FLOAT types use 'binary precision', which does not "
            "convert cleanly from decimal 'precision'.  Please specify this "
            "type with a separate Tibero variant, such as "
            r"FLOAT\(precision=5\).with_variant\(tibero.FLOAT\("
            r"binary_precision=16\), 'tibero'\), so that the Tibero "
            "specific 'binary_precision' may be specified accurately.",
        ):
            FLOAT(5).compile(dialect=tibero.dialect())

    # 티베로는 타입 정보를 오라클만큼 정확히 저장하지 않습니다. 예를 들어 Float, Double모두
    # 데이터베이스에서는 Number로 저장되어 autoreloading을 통해 원래 타입을 복원하는 것이
    # 불가능합니다. 아래 코드는 수정되었으니 원본 코드를 보고 싶으면 oracle dialect test 코드를
    # 참고해주세요.
    def test_numerics(self, metadata, connection):
        m = metadata
        t1 = Table(
            "t1",
            m,
            Column("intcol", Integer),
            Column("numericcol", Numeric(precision=9, scale=2)),
            Column("floatcol1", Float()),
            Column("floatcol2", FLOAT()),
            Column("doubleprec1", DOUBLE_PRECISION),
            Column("doubleprec2", Double()),
            Column("numbercol1", tibero.NUMBER(9)),
            Column("numbercol2", tibero.NUMBER(9, 3)),
            Column("numbercol3", tibero.NUMBER),
        )
        t1.create(connection)
        connection.execute(
            t1.insert(),
            dict(
                intcol=1,
                numericcol=5.2,
                floatcol1=6.5,
                floatcol2=8.5,
                doubleprec1=9.5,
                doubleprec2=14.5,
                numbercol1=12,
                numbercol2=14.85,
                numbercol3=15.76,
            ),
        )

        m2 = MetaData()

        # 티베로 데이터베이스의 한계로 autoload_with는 완벽하게 데이터 타입
        # 을 t1처럼 똑같이 가져올 수 없습니다.
        t2 = Table("t1", m2, autoload_with=connection)

        for row in (
            connection.execute(t1.select()).first(),
            connection.execute(t2.select()).first(),
        ):
            for i, (val, type_) in enumerate(
                (
                    (1, int),
                    (decimal.Decimal("5.2"), decimal.Decimal),
                    (6.5, float),
                    (8.5, float),
                    (9.5, float),
                    (14.5, float),
                    (12.0, float),
                    (decimal.Decimal("14.85"), decimal.Decimal),
                    (15.76, float),
                )
            ):
                eq_(row[i], val)
                assert isinstance(row[i], type_), "%r is not %r" % (
                    row[i],
                    type_,
                )

    # # Note: tbodbc에서 BINARY_DOUBLE_INFINITY를 넣는 방법을 찾는 것이 우선으로 보입니다.
    # def test_numeric_infinity_float(self, metadata, connection):
    #     m = metadata
    #     t1 = Table(
    #         "t1",
    #         m,
    #         Column("intcol", Integer),
    #         Column("numericcol", tibero.BINARY_DOUBLE(asdecimal=False)),
    #     )
    #     t1.create(connection)
    #     connection.execute(
    #         t1.insert(),
    #         [
    #             dict(intcol=1, numericcol=float("inf")),
    #             dict(intcol=2, numericcol=float("-inf")),
    #         ],
    #     )
    #
    #     connection.commit()
    #
    #     eq_(
    #         connection.execute(
    #             select(t1.c.numericcol).order_by(t1.c.intcol)
    #         ).fetchall(),
    #         [(float("inf"),), (float("-inf"),)],
    #     )
    #
    #     eq_(
    #         exec_sql(
    #             connection, "select numericcol from t1 order by intcol"
    #         ).fetchall(),
    #         [(float("inf"),), (float("-inf"),)],
    #     )

    # # Note: tbodbc에서 BINARY_DOUBLE_INFINITY를 넣는 방법을 찾는 것이 우선으로 보입니다.
    # def test_numeric_infinity_decimal(self, metadata, connection):
    #     m = metadata
    #     t1 = Table(
    #         "t1",
    #         m,
    #         Column("intcol", Integer),
    #         Column("numericcol", tibero.BINARY_DOUBLE(asdecimal=True)),
    #     )
    #     t1.create(connection)
    #     connection.execute(
    #         t1.insert(),
    #         [
    #             dict(intcol=1, numericcol=decimal.Decimal("Infinity")),
    #             dict(intcol=2, numericcol=decimal.Decimal("-Infinity")),
    #         ],
    #     )
    #
    #     eq_(
    #         connection.execute(
    #             select(t1.c.numericcol).order_by(t1.c.intcol)
    #         ).fetchall(),
    #         [(decimal.Decimal("Infinity"),), (decimal.Decimal("-Infinity"),)],
    #     )
    #
    #     eq_(
    #         exec_sql(
    #             connection, "select numericcol from t1 order by intcol"
    #         ).fetchall(),
    #         [(decimal.Decimal("Infinity"),), (decimal.Decimal("-Infinity"),)],
    #     )

    # Note: tbodbc에서 BINARY_DOUBLE_NAN를 넣는 방법을 찾는 것이 우선으로 보입니다.
    # def test_numeric_nan_float(self, metadata, connection):
    #     m = metadata
    #     t1 = Table(
    #         "t1",
    #         m,
    #         Column("intcol", Integer),
    #         Column("numericcol", tibero.BINARY_DOUBLE(asdecimal=False)),
    #     )
    #     t1.create(connection)
    #     connection.execute(
    #         t1.insert(),
    #         [
    #             dict(intcol=1, numericcol=float("nan")),
    #             dict(intcol=2, numericcol=float("-nan")),
    #         ],
    #     )
    #
    #     eq_(
    #         [
    #             tuple(str(col) for col in row)
    #             for row in connection.execute(
    #                 select(t1.c.numericcol).order_by(t1.c.intcol)
    #             )
    #         ],
    #         [("nan",), ("nan",)],
    #     )
    #
    #     eq_(
    #         [
    #             tuple(str(col) for col in row)
    #             for row in exec_sql(
    #                 connection, "select numericcol from t1 order by intcol"
    #             )
    #         ],
    #         [("nan",), ("nan",)],
    #     )

    # Note: tbodbc에서 BINARY_DOUBLE_NAN를 넣는 방법을 찾는 것이 우선으로 보입니다.
    # needs https://github.com/oracle/python-cx_Oracle/
    # issues/184#issuecomment-391399292
    def _dont_test_numeric_nan_decimal(self, metadata, connection):
        m = metadata
        t1 = Table(
            "t1",
            m,
            Column("intcol", Integer),
            Column("numericcol", tibero.BINARY_DOUBLE(asdecimal=True)),
        )
        t1.create()
        t1.insert().execute(
            [
                dict(intcol=1, numericcol=decimal.Decimal("NaN")),
                dict(intcol=2, numericcol=decimal.Decimal("-NaN")),
            ]
        )

        eq_(
            select(t1.c.numericcol).order_by(t1.c.intcol).execute().fetchall(),
            [(decimal.Decimal("NaN"),), (decimal.Decimal("NaN"),)],
        )

        eq_(
            exec_sql(
                connection, "select numericcol from t1 order by intcol"
            ).fetchall(),
            [(decimal.Decimal("NaN"),), (decimal.Decimal("NaN"),)],
        )

    # pyodbc와 oracle driver 차이로 인해 생기는 오류가 너무 많습니다.
    # 많은 수정이 필요합니다.
    # def test_numerics_broken_inspection(self, metadata, connection):
    #     """Numeric scenarios where Oracle type info is 'broken',
    #     returning us precision, scale of the form (0, 0) or (0, -127).
    #     We convert to Decimal and let int()/float() processors take over.
    #
    #     """
    #
    #     # this test requires cx_oracle 5
    #
    #     foo = Table(
    #         "foo",
    #         metadata,
    #         Column("idata", Integer),
    #         Column("ndata", Numeric(20, 2)),
    #         Column("ndata2", Numeric(20, 2)),
    #         Column("nidata", Numeric(5, 0)),
    #         Column("fdata", Float()),
    #     )
    #     foo.create(connection)
    #
    #     connection.execute(
    #         foo.insert(),
    #         {
    #             "idata": 5,
    #             "ndata": decimal.Decimal("45.6"),
    #             "ndata2": decimal.Decimal("45.0"),
    #             "nidata": decimal.Decimal("53"),
    #             "fdata": 45.68392,
    #         },
    #     )
    #     # TODO: remove this line
    #     connection.commit()
    #
    #     stmt = "SELECT idata, ndata, ndata2, nidata, fdata FROM foo"
    #
    #     row = exec_sql(connection, stmt).fetchall()[0]
    #
    #     # oracle, tibero 상관없이 pyodbc의 경우 Number without scale은 Decimal
    #     # 타입의 값을 반환합니다. 주석처리된 코드는 원본 오라클 코드입니다. 비교를 위해 남겨
    #     # 두었습니다.
    #     # 또한 sqlalchemy를 이용해 integer type의 column을 생성했어도 테이블 class를
    #     # 사용하지 않고 raw query string으로 조회하면 integer가 아닌 decimal로 받을 수
    #     # 밖에 없습니다.
    #     # eq_(
    #     #     [type(x) for x in row],
    #     #     [int, decimal.Decimal, decimal.Decimal, int, float],
    #     # )
    #     eq_(
    #         [type(x) for x in row],
    #         [decimal.Decimal, decimal.Decimal, decimal.Decimal, decimal.Decimal, float],
    #     )
    #
    #     eq_(
    #         row,
    #         (
    #             5,
    #             decimal.Decimal("45.6"),
    #             decimal.Decimal("45"),
    #             53,
    #             45.683920000000001,
    #         ),
    #     )
    #
    #     # with a nested subquery,
    #     # both Numeric values that don't have decimal places, regardless
    #     # of their originating type, come back as ints with no useful
    #     # typing information beyond "numeric".  So native handler
    #     # must convert to int.
    #     # this means our Decimal converters need to run no matter what.
    #     # totally sucks.
    #
    #     stmt = """
    #     SELECT
    #         (SELECT (SELECT idata FROM foo) FROM DUAL) AS idata,
    #         (SELECT CAST((SELECT ndata FROM foo) AS NUMERIC(20, 2)) FROM DUAL)
    #         AS ndata,
    #         (SELECT CAST((SELECT ndata2 FROM foo) AS NUMERIC(20, 2)) FROM DUAL)
    #         AS ndata2,
    #         (SELECT CAST((SELECT nidata FROM foo) AS NUMERIC(5, 0)) FROM DUAL)
    #         AS nidata,
    #         (SELECT CAST((SELECT fdata FROM foo) AS FLOAT) FROM DUAL) AS fdata
    #     FROM dual
    #     """
    #     row = exec_sql(connection, stmt).fetchall()[0]
    #
    #     # 아래 코드는 원본 오라클 코드입니다.
    #     # oracle의 경우 ndata2의 타입이 decimal이 아닌 int였고 fdata의 타입이 float가 아닌
    #     # decimal이 나왔습니다. castin으로는 numeric이고 float인데 특이합니다.
    #     # eq_(
    #     #     [type(x) for x in row],
    #     #     [int, decimal.Decimal, int, int, decimal.Decimal],
    #     # )
    #     # eq_(
    #     #     row,
    #     #     (5, decimal.Decimal("45.6"), 45, 53, decimal.Decimal("45.68392")),
    #     # )
    #
    #     eq_(
    #         [type(x) for x in row],
    #         [decimal.Decimal, decimal.Decimal, decimal.Decimal, decimal.Decimal, float],
    #     )
    #     eq_(
    #         row,
    #         (5, decimal.Decimal("45.6"), 45, 53, 45.68392),
    #     )
    #
    #     row = connection.execute(
    #         text(stmt).columns(
    #             idata=Integer(),
    #             ndata=Numeric(20, 2),
    #             ndata2=Numeric(20, 2),
    #             nidata=Numeric(5, 0),
    #             fdata=Float(),
    #         )
    #     ).fetchall()[0]
    #     eq_(
    #         [type(x) for x in row],
    #         [int, decimal.Decimal, decimal.Decimal, decimal.Decimal, float],
    #     )
    #     eq_(
    #         row,
    #         (
    #             5,
    #             decimal.Decimal("45.6"),
    #             decimal.Decimal("45"),
    #             decimal.Decimal("53"),
    #             45.683920000000001,
    #         ),
    #     )
    #
    #     stmt = """
    #     SELECT
    #             anon_1.idata AS anon_1_idata,
    #             anon_1.ndata AS anon_1_ndata,
    #             anon_1.ndata2 AS anon_1_ndata2,
    #             anon_1.nidata AS anon_1_nidata,
    #             anon_1.fdata AS anon_1_fdata
    #     FROM (SELECT idata, ndata, ndata2, nidata, fdata
    #     FROM (
    #         SELECT
    #             (SELECT (SELECT idata FROM foo) FROM DUAL) AS idata,
    #             (SELECT CAST((SELECT ndata FROM foo) AS NUMERIC(20, 2))
    #             FROM DUAL) AS ndata,
    #             (SELECT CAST((SELECT ndata2 FROM foo) AS NUMERIC(20, 2))
    #             FROM DUAL) AS ndata2,
    #             (SELECT CAST((SELECT nidata FROM foo) AS NUMERIC(5, 0))
    #             FROM DUAL) AS nidata,
    #             (SELECT CAST((SELECT fdata FROM foo) AS FLOAT) FROM DUAL)
    #             AS fdata
    #         FROM dual
    #     )
    #     WHERE ROWNUM >= 0) anon_1
    #     """
    #     row = exec_sql(connection, stmt).fetchall()[0]
    #     # 아래 코드는 원본 오라클 코드입니다.
    #     # oracle의 경우 ndata2의 타입이 decimal이 아닌 int였고 fdata의 타입이 float가 아닌
    #     # decimal이 나왔습니다. castin으로는 numeric이고 float인데 특이합니다.
    #     # eq_(
    #     #     [type(x) for x in row],
    #     #     [int, decimal.Decimal, int, int, decimal.Decimal],
    #     # )
    #     # eq_(
    #     #     row,
    #     #     (5, decimal.Decimal("45.6"), 45, 53, decimal.Decimal("45.68392")),
    #     # )
    #     eq_(
    #         [type(x) for x in row],
    #         [decimal.Decimal, decimal.Decimal, decimal.Decimal, decimal.Decimal, float],
    #     )
    #     eq_(
    #         row,
    #         (5, decimal.Decimal("45.6"), 45, 53, 45.68392),
    #     )
    #
    #     row = connection.execute(
    #         text(stmt).columns(
    #             anon_1_idata=Integer(),
    #             anon_1_ndata=Numeric(20, 2),
    #             anon_1_ndata2=Numeric(20, 2),
    #             anon_1_nidata=Numeric(5, 0),
    #             anon_1_fdata=Float(),
    #         )
    #     ).fetchall()[0]
    #     eq_(
    #         [type(x) for x in row],
    #         [int, decimal.Decimal, decimal.Decimal, decimal.Decimal, float],
    #     )
    #     eq_(
    #         row,
    #         (
    #             5,
    #             decimal.Decimal("45.6"),
    #             decimal.Decimal("45"),
    #             decimal.Decimal("53"),
    #             45.683920000000001,
    #         ),
    #     )
    #
    #     row = connection.execute(
    #         text(stmt).columns(
    #             anon_1_idata=Integer(),
    #             anon_1_ndata=Numeric(20, 2, asdecimal=False),
    #             anon_1_ndata2=Numeric(20, 2, asdecimal=False),
    #             anon_1_nidata=Numeric(5, 0, asdecimal=False),
    #             anon_1_fdata=Float(asdecimal=True),
    #         )
    #     ).fetchall()[0]
    #     eq_(
    #         [type(x) for x in row], [int, float, float, float, decimal.Decimal]
    #     )
    #     eq_(row, (5, 45.6, 45, 53, decimal.Decimal("45.68392")))

    # Oracle driver의 connection에는 outputtypehandler를 설정해줄 수 있습니다.
    # 이 기능은 데이터베이스로부터 각 칼럼의 데이터를 가져올 때 어떤 type으로 가져올 건지
    # 설정할 수 있 수 있습니다. Oracle dialect의 coerce_to_decimal은 이 기능을
    # 사용하는데 pyodbc에는 oracle driver에 견줄만한 기능을 제공하지 않습니다. 따라서
    # 아래 테스트 내용을 지웠습니다. 함수 이름을 남긴 이유는 oracle dialect에 이러한
    # 테스트가 있는 걸 알려주기 위함입니다.
    def _test_numeric_no_coerce_decimal_mode(self):
        pass

    # Oracle driver의 connection에는 outputtypehandler를 설정해줄 수 있습니다.
    # 이 기능은 데이터베이스로부터 각 칼럼의 데이터를 가져올 때 어떤 type으로 가져올 건지
    # 설정할 수 있 수 있습니다. Oracle dialect의 coerce_to_decimal은 이 기능을
    # 사용하는데 pyodbc에는 oracle driver에 견줄만한 기능을 제공하지 않습니다. 따라서
    # 아래 테스트 내용을 지웠습니다. 함수 이름을 남긴 이유는 oracle dialect에 이러한
    # 테스트가 있는 걸 알려주기 위함입니다.
    def _test_numeric_coerce_decimal_mode(self, connection):
        pass

    @testing.combinations(
        (
            "Max 32-bit Number",
            "SELECT CAST(2147483647 AS NUMBER(19,0)) FROM dual",
        ),
        (
            "Min 32-bit Number",
            "SELECT CAST(-2147483648 AS NUMBER(19,0)) FROM dual",
        ),
        (
            "32-bit Integer Overflow",
            "SELECT CAST(2147483648 AS NUMBER(19,0)) FROM dual",
        ),
        (
            "32-bit Integer Underflow",
            "SELECT CAST(-2147483649 AS NUMBER(19,0)) FROM dual",
        ),
        (
            "Max Number with Precision 19",
            "SELECT CAST(9999999999999999999 AS NUMBER(19,0)) FROM dual",
        ),
        (
            "Min Number with Precision 19",
            "SELECT CAST(-9999999999999999999 AS NUMBER(19,0)) FROM dual",
        ),
    )
    # 테스트를 위해 tibero dialect의 name이 oracle로 의도적으로 설정하기 때문에 oracle+pyodbc입니다.
    @testing.only_on(
        ["oracle+pyodbc"],
        "cx_oracle/oracledb specific feature",
    )
    def test_raw_numerics(self, title, stmt):
        with testing.db.connect() as conn:
            # get a brand new connection that definitely is not
            # in the pool to avoid any outputtypehandlers
            cx_oracle_raw = testing.db.pool._creator()
            cursor = cx_oracle_raw.cursor()
            cursor.execute(stmt)
            cx_oracle_result = cursor.fetchone()[0]
            cursor.close()

            sqla_result = conn.exec_driver_sql(stmt).scalar()

            eq_(sqla_result, cx_oracle_result)

    def test_reflect_dates(self, metadata, connection):
        Table(
            "date_types",
            metadata,
            Column("d1", sqltypes.DATE),
            Column("d2", tibero.DATE),
            Column("d3", TIMESTAMP),
            Column("d4", TIMESTAMP(timezone=True)),
            Column("d5", tibero.INTERVAL(second_precision=5)),
            Column("d6", tibero.TIMESTAMP(local_timezone=True)),
        )
        metadata.create_all(connection)
        m = MetaData()
        t1 = Table("date_types", m, autoload_with=connection)
        assert isinstance(t1.c.d1.type, tibero.DATE)
        assert isinstance(t1.c.d1.type, DateTime)
        assert isinstance(t1.c.d2.type, tibero.DATE)
        assert isinstance(t1.c.d2.type, DateTime)
        assert isinstance(t1.c.d3.type, tibero.TIMESTAMP)
        assert not t1.c.d3.type.timezone
        assert isinstance(t1.c.d4.type, tibero.TIMESTAMP)
        assert t1.c.d4.type.timezone
        assert isinstance(t1.c.d6.type, tibero.TIMESTAMP)
        assert t1.c.d6.type.local_timezone
        assert isinstance(t1.c.d5.type, tibero.INTERVAL)

    def _dont_test_reflect_all_types_schema(self):
        types_table = Table(
            "all_types",
            MetaData(),
            Column("owner", String(30), primary_key=True),
            Column("type_name", String(30), primary_key=True),
            autoload_with=testing.db,
            oracle_resolve_synonyms=True,
        )
        for row in types_table.select().execute().fetchall():
            [row[k] for k in row.keys()]

    def test_raw_roundtrip(self, metadata, connection):
        raw_table = Table(
            "raw",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", tibero.RAW(35)),
        )
        metadata.create_all(connection)
        connection.execute(raw_table.insert(), dict(id=1, data=b("ABCDEF")))
        eq_(connection.execute(raw_table.select()).first(), (1, b("ABCDEF")))

    def test_reflect_nvarchar(self, metadata, connection):
        Table(
            "tnv",
            metadata,
            Column("nv_data", sqltypes.NVARCHAR(255)),
            Column("c_data", sqltypes.NCHAR(20)),
        )
        metadata.create_all(connection)
        m2 = MetaData()
        t2 = Table("tnv", m2, autoload_with=connection)
        assert isinstance(t2.c.nv_data.type, sqltypes.NVARCHAR)
        assert isinstance(t2.c.c_data.type, sqltypes.NCHAR)

        # 테스트를 위해 tibero dialect의 name이 oracle로 의도적으로 설정하기 때문에 oracle+pyodbc입니다.
        if testing.against("oracle+pyodbc"):
            assert isinstance(
                t2.c.nv_data.type.dialect_impl(connection.dialect),
                pyodbc._TiberoUnicodeStringNCHAR,
            )

            assert isinstance(
                t2.c.c_data.type.dialect_impl(connection.dialect),
                pyodbc._TiberoNChar,
            )

        data = "m’a réveillé."
        connection.execute(t2.insert(), dict(nv_data=data, c_data=data))
        nv_data, c_data = connection.execute(t2.select()).first()
        eq_(nv_data, data)
        eq_(c_data, data + (" " * 7))  # char is space padded
        assert isinstance(nv_data, str)
        assert isinstance(c_data, str)

    def test_reflect_unicode_no_nvarchar(self, metadata, connection):
        Table("tnv", metadata, Column("data", sqltypes.Unicode(255)))
        metadata.create_all(connection)
        m2 = MetaData()
        t2 = Table("tnv", m2, autoload_with=connection)
        assert isinstance(t2.c.data.type, sqltypes.VARCHAR)

        # 테스트를 위해 tibero dialect의 name이 oracle로 의도적으로 설정하기 때문에 oracle+pyodbc입니다.
        if testing.against("oracle+pyodbc"):
            assert isinstance(
                t2.c.data.type.dialect_impl(connection.dialect),
                pyodbc._TiberoString,
            )

        data = "m’a réveillé."
        connection.execute(t2.insert(), {"data": data})
        res = connection.execute(t2.select()).first().data
        eq_(res, data)
        assert isinstance(res, str)

    def test_char_length(self, metadata, connection):
        t1 = Table(
            "t1",
            metadata,
            Column("c1", VARCHAR(50)),
            Column("c2", NVARCHAR(250)),
            Column("c3", CHAR(200)),
            Column("c4", NCHAR(180)),
        )
        t1.create(connection)
        m2 = MetaData()
        t2 = Table("t1", m2, autoload_with=connection)
        eq_(t2.c.c1.type.length, 50)
        eq_(t2.c.c2.type.length, 250)
        eq_(t2.c.c3.type.length, 200)
        eq_(t2.c.c4.type.length, 180)

    def test_long_type(self, metadata, connection):
        t = Table("t", metadata, Column("data", tibero.LONG))
        metadata.create_all(connection)
        connection.execute(t.insert(), dict(data="xyz"))
        eq_(connection.scalar(select(t.c.data)), "xyz")

    def test_longstring(self, metadata, connection):
        exec_sql(
            connection,
            """
        CREATE TABLE Z_TEST
        (
          ID        NUMERIC(22) PRIMARY KEY,
          ADD_USER  VARCHAR2(20)  NOT NULL
        )
        """,
        )
        try:
            t = Table("z_test", metadata, autoload_with=connection)
            connection.execute(t.insert(), dict(id=1.0, add_user="foobar"))
            assert connection.execute(t.select()).fetchall() == [(1, "foobar")]
        finally:
            exec_sql(connection, "DROP TABLE Z_TEST")


class LOBFetchTest(fixtures.TablesTest):
    __only_on__ = "oracle"
    __backend__ = True

    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "z_test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", Text),
            Column("bindata", LargeBinary),
        )

        Table(
            "binary_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", LargeBinary),
        )

    @classmethod
    def insert_data(cls, connection):
        cls.data = data = [
            dict(
                id=i,
                data="this is text %d" % i,
                bindata=b("this is binary %d" % i),
            )
            for i in range(1, 20)
        ]

        connection.execute(cls.tables.z_test.insert(), data)

        binary_table = cls.tables.binary_table
        fname = os.path.join(os.path.dirname(__file__), "binary_data_one.dat")
        with open(fname, "rb") as file_:
            cls.stream = stream = file_.read(12000)

        for i in range(1, 11):
            connection.execute(binary_table.insert(), dict(id=i, data=stream))

    # auto_convert_lobs=False일 때만 쓰이는 함수입니다. 즉 pyodbc를 사용하는 tibero dialect
    # 에서는 사용되지 않는 함수입니다. 이러한 함수가 오라클에서 사용한다는 것을 보이기 위해 남겨두었습니다.
    def _read_lob(self, engine, row):
        if engine.dialect.is_async:
            data = await_fallback(row._mapping["data"].read())
            bindata = await_fallback(row._mapping["bindata"].read())
        else:
            data = row._mapping["data"].read()
            bindata = row._mapping["bindata"].read()
        return data, bindata

    # tibero가 사용하는 pyodbc는 binary data에 대해 항상 bytes 타입의 데이터를 받아옵니다.
    # 반대로 oracle dialect에서 auto_convert_lobs=False로 설정하면 oracle driver로부터
    # bytes type이아닌 stream object로 받는 것이 가능합니다. pyodbc는 stream object를
    # 반환하는 것이 불가능해서 auto_convert_lobs 파라미터를 Tibero_pyodbc로부터 제거했고 아래
    # 테스트를 제외시켰습니다. 함수를 남긴 이유는 다음과 같은 테스트가 오라클에 있는다는 것을 알려주기
    # 위함입니다.
    def _test_lobs_without_convert(self):
        pass

    # Tibero_pyodbc의 기본 동작은 oracle dialect의 auto_convert_lobs=True를 설정한 것과
    # 같습니다.
    def test_lobs_with_convert(self, connection):
        t = self.tables.z_test
        row = connection.execute(t.select().where(t.c.id == 1)).first()
        eq_(row._mapping["data"], "this is text 1")
        eq_(row._mapping["bindata"], b("this is binary 1"))

    # Tibero_pyodbc의 기본 동작은 oracle dialect의 auto_convert_lobs=True를 설정한 것과
    # 같습니다.
    def test_lobs_with_convert_raw(self, connection):
        row = exec_sql(connection, "select data, bindata from z_test").first()
        eq_(row._mapping["data"], "this is text 1")
        eq_(row._mapping["bindata"], b("this is binary 1"))

    # tibero가 사용하는 pyodbc는 binary data에 대해 항상 bytes 타입의 데이터를 받아옵니다.
    # 반대로 oracle dialect에서 auto_convert_lobs=False로 설정하면 oracle driver로부터
    # bytes type이아닌 stream object로 받는 것이 가능합니다. pyodbc는 stream object를
    # 반환하는 것이 불가능해서 auto_convert_lobs 파라미터를 Tibero_pyodbc로부터 제거했고 아래
    # 테스트를 제외시켰습니다. 함수를 남긴 이유는 다음과 같은 테스트가 오라클에 있는다는 것을 알려주기
    # 위함입니다.
    def _test_lobs_without_convert_many_rows(self):
        pass

    # tibero가 사용하는 pyodbc는 binary data에 대해 항상 bytes 타입의 데이터를 받아옵니다.
    # 반대로 oracle dialect에서 auto_convert_lobs=False로 설정하면 oracle driver로부터
    # bytes type이아닌 stream object로 받는 것이 가능합니다. pyodbc는 stream object를
    # 반환하는 것이 불가능해서 auto_convert_lobs 파라미터를 Tibero_pyodbc로부터 제거했고 아래
    # 테스트를 제외시켰습니다. 함수를 남긴 이유는 다음과 같은 테스트가 오라클에 있는다는 것을 알려주기
    # 위함입니다.
    def _test_lobs_with_convert_many_rows(self):
        pass

    # test_lobs_with_convert_many_rows()의 내용과 거의 동일합니다. 차이점은
    # tibero dialect에 auto_convert_lobs parameter를 추가 안한 것과 같습니다.
    def test_lobs_with_many_rows(self, connection):
        result = exec_sql(
            connection,
            "select id, data, bindata from z_test order by id",
        )
        results = result.fetchall()

        eq_(
            [
                dict(
                    id=row._mapping["id"],
                    data=row._mapping["data"],
                    bindata=row._mapping["bindata"],
                )
                for row in results
            ],
            self.data,
        )

    # TODO: random하게 y column의 데이터가 잘못 insert되는 것 같습니다. 원인을
    #       찾아야 합니다. '🐍' 문자가 없으면 모든 케이스에서 성공합니다.
    #       datasize가 10같이 작은 수에서는 성공하는 것을 확인했습니다. 그러나 100, 250,
    #       550 같이 큰 수에서는 높은 확률로 실패합니다.
    @testing.combinations(
        (UnicodeText(),), (Text(),), (LargeBinary(),), argnames="datatype"
    )
    @testing.combinations((10,), (100,), (250,), argnames="datasize")
    @testing.combinations(
        ("x,y,z"), ("y"), ("y,x,z"), ("x,z,y"), argnames="retcols"
    )
    def test_insert_returning_w_lobs(
        self, datatype, datasize, retcols, metadata, connection
    ):
        long_text = Table(
            "long_text",
            metadata,
            Column("x", Integer),
            Column("y", datatype),
            Column("z", Integer),
        )
        long_text.create(connection)

        if isinstance(datatype, UnicodeText):
            word_seed = "ab🐍’«cdefg"
        else:
            word_seed = "abcdef"

        some_text = " ".join(
            "".join(random.choice(word_seed) for j in range(150))
            for i in range(datasize)
        )
        if isinstance(datatype, LargeBinary):
            some_text = some_text.encode("ascii")

        data = {"x": 5, "y": some_text, "z": 10}
        return_columns = [long_text.c[col] for col in retcols.split(",")]
        expected = tuple(data[col] for col in retcols.split(","))
        result = connection.execute(
            long_text.insert().returning(*return_columns),
            data,
        )
        a = result.fetchall()
        eq_(a, [expected])

    def test_insert_returning_w_unicode(self, metadata, connection):
        long_text = Table(
            "long_text",
            metadata,
            Column("x", Integer),
            Column("y", Unicode(255)),
        )
        long_text.create(connection)

        word_seed = "ab🐍’«cdefg"

        some_text = " ".join(
            "".join(random.choice(word_seed) for j in range(10))
            for i in range(15)
        )

        data = {"x": 5, "y": some_text}
        result = connection.execute(
            long_text.insert().returning(long_text.c.y),
            data,
        )

        eq_(result.fetchall(), [(some_text,)])

    def test_large_stream(self, connection):
        binary_table = self.tables.binary_table
        result = connection.execute(
            binary_table.select().order_by(binary_table.c.id)
        ).fetchall()
        eq_(result, [(i, self.stream) for i in range(1, 11)])

    # arraysize는 원래 oracle driver의 cursor.var를 통해 구현되었으나
    # pyodbc에서 cursor.arraysize를 통해 비슷하게 구현했습니다.
    def test_large_stream_single_arraysize(self):
        binary_table = self.tables.binary_table
        eng = testing_engine(options={"arraysize": 1})
        with eng.connect() as conn:
            result = conn.execute(
                binary_table.select().order_by(binary_table.c.id)
            ).fetchall()
            eq_(result, [(i, self.stream) for i in range(1, 11)])


class EuroNumericTest(fixtures.TestBase):
    """
    test the numeric output_type_handler when using non-US locale for NLS_LANG.
    """

    __only_on__ = "oracle+pyodbc"
    __backend__ = True

    def setup_test(self):
        connect = testing.db.pool._creator

        def _creator():
            conn = connect()
            cursor = conn.cursor()
            cursor.execute("ALTER SESSION SET NLS_TERRITORY='GERMANY'")
            cursor.close()
            return conn

        self.engine = testing_engine(options={"creator": _creator})

    def teardown_test(self):
        self.engine.dispose()

    # oracle driver의 outputtypehandler와 같은 수준의
    # api가 pyodbc에서 제공되어야 지원가능합니다. 따라서 pyodbc 한계로 인해
    # 아래의 테스트는 실행될 수 없습니다. 이는 곧 raw query를 실행하면 column type
    # 이 oracle가 다를 수 있음을 의미합니다. 테스트 이름을 남긴 이유는 oracle에
    # 이러한 테스트가 있음을 알리기 위함입니다.
    def _test_detection(self):
        pass

    # oracle driver의 outputtypehandler와 같은 수준의
    # api가 pyodbc에서 제공되어야 지원가능합니다. 따라서 pyodbc 한계로 인해
    # 아래의 테스트는 실행될 수 없습니다. 이는 곧 raw query를 실행하면 column type
    # 이 oracle가 다를 수 있음을 의미합니다. 테스트 이름을 남긴 이유는 oracle에
    # 이러한 테스트가 있음을 알리기 위함입니다.
    def _test_output_type_handler(self, stmt, expected, kw):
        pass

class SetInputSizesTest(fixtures.TestBase):
    __only_on__ = "oracle+pyodbc"
    __backend__ = True

    # oracle driver는 do_set_input_sizes()를 재정의했습니다. 하지만
    # tibero는 SQLALchemy에서 제공해주는 PyODBCConnector()의
    # do_set_input_sizes()를 그대로 사용하기 때문에 테스트할 필요가 없습니다.
    def test_setinputsizes(
        self, metadata, datatype, value, sis_value_text, set_nchar_flag
    ):
        pass

    # oracle driver는 do_set_input_sizes()를 재정의했습니다. 하지만
    # tibero는 SQLALchemy에서 제공해주는 PyODBCConnector()의
    # do_set_input_sizes()를 그대로 사용하기 때문에 테스트할 필요가 없습니다.
    def test_event_no_native_float(self, metadata):
        pass
