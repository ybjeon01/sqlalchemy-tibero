# sqlalchemy_tibero/pyodbc.py
# Copyright (C) 2024-2024 the Tibero authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
import datetime
import os
import decimal

import pyodbc

from sqlalchemy import util
from sqlalchemy import func
from sqlalchemy.engine import interfaces
from sqlalchemy import processors
from sqlalchemy import exc
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy.sql import sqltypes

from . import base as tibero
from .base import TiberoExecutionContext, TiberoDialect, TiberoCompiler

# 1. SQLAlchemy는 자체적인 풀링 메커니즘을 가지고 있기 때문에, PyODBC의 풀링 기능을 비활성화하는
#    것이 더 나은 경우가 많습니다. 이 동작은 PyODBC 모듈 수준에서 전역적으로 비활성화할 수 있으며,
#    첫 번째 연결이 만들어지기 전에만 비활성화할 수 있습니다. 이 내용은
#    lib/sqlalchemy/dialects/mssql/pyodbc.py에서 발견했습니다.
# 2. 풀링 기능을 비활성화해야 test/test_suite.py::WeCanSetDefaultSchemaWEventsTest
#    테스트가 성공가능합니다.
pyodbc.pooling = False


class _TiberoInteger(sqltypes.Integer):
    def get_dbapi_type(self, dbapi):
        # see https://github.com/oracle/python-cx_Oracle/issues/
        # 208#issuecomment-409715955
        return int

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return int(value)
            else:
                return value

        return process


class _TiberoNumeric(sqltypes.Numeric):
    is_number = False

    def bind_processor(self, dialect):
        if self.scale == 0:
            return None
        elif self.asdecimal:
            processor = processors.to_decimal_processor_factory(
                decimal.Decimal, self._effective_decimal_return_scale
            )

            def process(value):
                if isinstance(value, (int, float)):
                    return processor(value)
                elif value is not None and value.is_infinite():
                    return float(value)
                else:
                    return value

            return process
        else:
            return processors.to_float

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            if coltype is float:
                return processors.to_decimal_processor_factory(
                    decimal.Decimal, self._effective_decimal_return_scale
                )
            else:
                # pyodbc에서 반환한 값의 타입이 decimal인 경우 데이터 변환없이 그대로 유저에게 전달
                return None
        else:
            return processors.to_float


class _TiberoBinaryFloat(_TiberoNumeric):
    def get_dbapi_type(self, dbapi):
        return dbapi.NATIVE_FLOAT


class _TiberoBINARY_FLOAT(_TiberoBinaryFloat, tibero.BINARY_FLOAT):
    pass


class _TiberoBINARY_DOUBLE(_TiberoBinaryFloat, tibero.BINARY_DOUBLE):
    pass


class _TiberoNUMBER(_TiberoNumeric):
    is_number = True


class _PYODBCTiberoDate(sqltypes.Date):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return value.date()
            else:
                return value

        return process

    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_TYPE_DATE


class _PYODBCTiberoDateTime(tibero.DATE):
    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_TYPE_DATE


# TODO: the names used across CHAR / VARCHAR / NCHAR / NVARCHAR
# here are inconsistent and not very good
class _TiberoChar(sqltypes.CHAR):
    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_CHAR


class _TiberoNChar(sqltypes.NCHAR):
    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_WCHAR


class _TiberoUnicodeStringNCHAR(tibero.NVARCHAR2):
    pass

    # 아래 코드를 주석처리한 이유는 시간이 부족해 bind parameter에 어떤 타입을
    # 사용해야 할지 자세히 조사를 못했기 때문입니다.
    # def get_dbapi_type(self, dbapi):
    #     return dbapi.NCHAR


class _TiberoUnicodeStringCHAR(sqltypes.Unicode):
    pass

    # 아래 코드를 주석처리한 이유는 시간이 부족해 bind parameter에 어떤 타입을
    # 사용해야 할지 자세히 조사를 못했기 때문입니다.
    # def get_dbapi_type(self, dbapi):
    #     return dbapi.LONG_STRING


class _TiberoUnicodeTextNCLOB(tibero.NCLOB):
    pass

    # 아래 코드를 주석처리한 이유는 시간이 부족해 bind parameter에 어떤 타입을
    # 사용해야 할지 자세히 조사를 못했기 때문입니다.
    # def get_dbapi_type(self, dbapi):
    #     # previously, this was dbapi.NCLOB.
    #     # DB_TYPE_NVARCHAR will instead be passed to setinputsizes()
    #     # when this datatype is used.
    #     return dbapi.DB_TYPE_NVARCHAR


class _TiberoUnicodeTextCLOB(sqltypes.UnicodeText):
    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_WLONGVARCHAR


class _TiberoText(sqltypes.Text):
    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_WLONGVARCHAR


class _TiberoLong(tibero.LONG):
    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_WLONGVARCHAR


class _TiberoString(sqltypes.String):
    # get_dbapi_type을 명시안한 이유는 pyodbc가 자동으로 varchar 또는 longvarchar를 선택하기
    # 위함입니다.
    pass


class _TiberoEnum(sqltypes.Enum):
    def bind_processor(self, dialect):
        enum_proc = sqltypes.Enum.bind_processor(self, dialect)

        def process(value):
            raw_str = enum_proc(value)
            return raw_str

        return process


class _TiberoBinary(sqltypes.LargeBinary):
    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_LONGVARBINARY

    def bind_processor(self, dialect):
        # 위의 dbapi.SQL_BINARY 타입으로 인해 pyodbc 내부에서 binary로 변환해줍니다.
        # 따라서 sqltypes.LargeBinary에 정의된 bind_processor()가 필요없습니다.
        return None

    def result_processor(self, dialect, coltype):
        # pyodbc에서 이미 bytes로 전송해주기 때문에 별도의 processor가 필요없습니다.
        # 따라서 sqltypes.LargeBinary에 이미 정의되어 있는 result_processor가
        # 필요없습니다.
        # https://github.com/mkleehammer/pyodbc/wiki/Data-Types
        return None


class _TiberoInterval(tibero.INTERVAL):
    def bind_processor(self, dialect):
        def process(value: datetime.timedelta) -> str:
            # timedelta에서 days, seconds, microseconds 추출
            days = value.days
            seconds = value.seconds

            # 시, 분, 초로 변환
            h = seconds // 3600
            m = (seconds % 3600) // 60
            s = seconds % 60
            ms = value.microseconds

            # 마이크로초를 소수점 이하 6자리로 변환
            return f"{days} {h}:{m}:{s}.{ms}"

        return process

    def bind_expression(self, bindparam):
        return func.TO_DSINTERVAL(bindparam)

    @staticmethod
    def _add_pyodbc_output_converter(conn):
        def handler(dto: bytes):
            interval_str = dto.decode()
            days, time_str = interval_str.split()

            days = int(days)
            hours, minutes, rest = time_str.split(":")
            hours = int(hours)
            minutes = int(minutes)

            if "." in rest:
                seconds, microseconds = map(int, rest.split("."))
            else:
                seconds = int(rest)
                microseconds = 0

            # timedelta 객체 생성
            return datetime.timedelta(
                days=days,
                hours=hours,
                minutes=minutes,
                seconds=seconds,
                microseconds=microseconds,
            )

	def binary_float_handler(dto: bytes):
            """
            BINARY_FLOAT (4 bytes, Big Endian IEEE 754) to Python float
            """
            return dto

        def binary_double_handler(dto: bytes):
            """
            BINARY_DOUBLE (8 bytes, Big Endian IEEE 754) to Python double
            """
            return dto

        conn.add_output_converter(pyodbc.SQL_INTERVAL_DAY_TO_SECOND, handler)
        conn.add_output_converter(56, binary_float_handler)
        conn.add_output_converter(57, binary_double_handler)

    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_INTERVAL_DAY_TO_SECOND


class _TiberoRaw(tibero.RAW):
    pass


class _TiberoRowid(tibero.ROWID):
    pass

    # 아래 코드를 주석처리한 이유는 시간이 부족해 bind parameter에 어떤 타입을
    # 사용해야 할지 자세히 조사를 못했기 때문입니다.
    # def get_dbapi_type(self, dbapi):
    #     return dbapi.ROWID


class TiberoCompiler_pyodbc(TiberoCompiler):
    pass


class TiberoExecutionContext_pyodbc(TiberoExecutionContext):
    def create_cursor(self):
        c = self._dbapi_connection.cursor()
        if self.dialect.arraysize:
            c.arraysize = self.dialect.arraysize
        return c


class TiberoDialect_pyodbc(PyODBCConnector, TiberoDialect):
    # 아래 속성들을 보면 DefaultDialect에서 이미 같은 값으로 설정이 되어있기 때문에 생략해도 문제없는 코드가 있습니다. 하지만
    # OracleDialect_cx_oracle의 패턴을 따랐습니다.
    # 예를 들어, supports_statement_cache, supports_sane_rowcount들은 이미 DefaultDialect에서 True로 설정되어 있습니다.
    # 또한, execution_ctx_cls, statement_compiler 또한 TiberoDialect에 설정된 깂을 그대로 사용하지만
    # OracleDialect_cx_oracle의 패턴을 따랐습니다.

    supports_statement_cache = True
    execution_ctx_cls = TiberoExecutionContext_pyodbc
    statement_compiler = TiberoCompiler_pyodbc

    # Tibero pyodbc에서는 pyodbc execute()는 select, insert, update,
    # delete문에 대해 cursor.rowcount가 정상적으로 작동하는 것을 확인했습니다.
    supports_sane_rowcount = True
    # Tibero pyodbc에서는 executemany()를 실행할 때 select, insert, update,
    # delete문에 대해 cursor.rowcount가 정상적으로 작동하지 않는 것을 확인했습니다.
    # 따라서 오라클과 다르게 아래 설정을 False로 수정했습니다.
    supports_sane_multi_rowcount = False

    # OracleDialect_cx_oracle에서는 아래 4개의 항목 모두 True
    # TODO: insert_executemany_returning은
    #       test/orm/test_composites.py::PointTest::test_bulk_insert
    #       을 통해 작동한다는 것을 알았습니다. 그리고
    #       insert_executemany_returning_sort_by_parameter_order은
    #       ReturningTest::test_insert_w_floats을 통해 작동한다는 것을 알았습니다.
    #       하지만 나머지 2개는 아직 확인을 못했기 때문에 False로 두었습니다.
    insert_executemany_returning = True
    insert_executemany_returning_sort_by_parameter_order = False
    update_executemany_returning = False
    delete_executemany_returning = False

    # TAG: 1.4 2.0 차이
    # 1.4에는 다음 내용이 없음
    # bind_typing = interfaces.BindTyping.SETINPUTSIZES

    pyodbc_driver_name = "Tibero"

    colspecs = util.update_copy(
        TiberoDialect.colspecs,
        {
            sqltypes.Numeric: _TiberoNumeric,
            sqltypes.Float: _TiberoNumeric,
            tibero.BINARY_FLOAT: _TiberoBINARY_FLOAT,
            tibero.BINARY_DOUBLE: _TiberoBINARY_DOUBLE,
            sqltypes.Integer: _TiberoInteger,
            tibero.NUMBER: _TiberoNUMBER,
            sqltypes.Date: _PYODBCTiberoDate,
            sqltypes.DateTime: _PYODBCTiberoDateTime,
            sqltypes.LargeBinary: _TiberoBinary,
            sqltypes.Boolean: tibero._TiberoBoolean,
            sqltypes.Interval: _TiberoInterval,
            tibero.INTERVAL: _TiberoInterval,
            sqltypes.Text: _TiberoText,
            sqltypes.String: _TiberoString,
            sqltypes.UnicodeText: _TiberoUnicodeTextCLOB,
            sqltypes.CHAR: _TiberoChar,
            sqltypes.NCHAR: _TiberoNChar,
            sqltypes.Enum: _TiberoEnum,
            tibero.LONG: _TiberoLong,
            tibero.RAW: _TiberoRaw,
            sqltypes.Unicode: _TiberoUnicodeStringCHAR,
            sqltypes.NVARCHAR: _TiberoUnicodeStringNCHAR,
            tibero.NCLOB: _TiberoUnicodeTextNCLOB,
            tibero.ROWID: _TiberoRowid,
        },
    )
    #####################
    #### New Section ####
    #####################
    # 이 부분부터는 oracle_cx와 oracledb class와 완전 다르게 동작하는 부분을 추가한 섹션입니다.

    # https://docs.sqlalchemy.org/en/20/core/connections.html#engine-insertmanyvalues
    # https://docs.sqlalchemy.org/en/20/core/internals.html#sqlalchemy.engine.default.DefaultDialect.supports_multivalues_insert
    # https://docs.sqlalchemy.org/en/20/core/internals.html#sqlalchemy.engine.default.DefaultDialect.use_insertmanyvalues
    # https://docs.sqlalchemy.org/en/20/core/internals.html#sqlalchemy.engine.default.DefaultDialect.insert_executemany_returning
    # https://docs.sqlalchemy.org/en/20/core/internals.html#sqlalchemy.engine.default.DefaultDialect.insert_returning
    #
    # oracledb와 cx_Oracle의 execute_many가 이미 multivalues_insert처럼 최적화하는 기능을
    # 제공하므로, supports_multivalues_insert나 use_insertmanyvalues를 True로 설정할
    # 필요가 없습니다. 다만, execute_many에 사용되는 쿼리와 multivalues_insert 플래그의 영향을
    # 받아 생성되는 SQL 쿼리는 동일하지 않습니다.
    #
    # multivalues_insert는
    # "insert into table(a, b, c) values (...), (...), (...)" 처럼
    # "(...)" 부분을 하나의 query string안에 여러번 넣을 것을 의미합니다. string의 길이는
    # 길지만 한번의 통신으로 모든 값들을 보낼 수 있습니다. execute_many에서는
    # "insert into table(a, b, c) values (?, ?, ?)"같이 짧은 query와
    # 여러 parameter를 사용해 한번의 통신을 한 것을 의미합니다.
    supports_multivalues_insert = True
    use_insertmanyvalues = True

    # TAG: 1.4 2.0 차이
    # 1.4에는 다음 옵션 미존재
    # insertmanyvalues_implicit_sentinel = (
    #     InsertmanyvaluesSentinelOpts.AUTOINCREMENT
    # )

    #############################
    #### End Of  New Section ####
    #############################

    def __init__(
        self,
        arraysize=50,
        char_encoding="UTF-8",
        wchar_encoding="UTF-8",
        **kwargs,
    ):
        self.char_encoding = char_encoding
        self.wchar_encoding = wchar_encoding

        # This prevents unicode from getting mangled by getting encoded into the
        # potentially non-unicode database character set.
        os.environ.setdefault("TBCLI_WCHAR_TYPE", "UCS2")
        # Tibero takes client-side character set encoding from the environment.
        os.environ.setdefault("TB_NLS_LANG", "UTF8")

        # tbodbc에 returning into를 가능하게 하는 환경 변수
        os.environ.setdefault("TBCLI_COMPAT_ALCHEMY", "YES")

        TiberoDialect.__init__(self, **kwargs)
        # arraysize는 원래 oracle driver의 cursor.var를 통해 구현되었으나
        # pyodbc에서 cursor.arraysize를 통해 비슷하게 구현했습니다.
        self.arraysize = arraysize
        if self._use_nchar_for_unicode:
            self.colspecs = self.colspecs.copy()
            self.colspecs[sqltypes.Unicode] = _TiberoUnicodeStringNCHAR
            self.colspecs[sqltypes.UnicodeText] = _TiberoUnicodeTextNCLOB

    def get_isolation_level(
        self, dbapi_connection
    ):
        # general idea of transaction id, have to start one, etc.
        # https://stackoverflow.com/questions/10711204/how-to-check-isoloation-level

        # how to decode xid cols from v$transaction to match
        # https://asktom.oracle.com/pls/apex/f?p=100:11:0::::P11_QUESTION_ID:9532779900346079444

        # Tibero v$transaction document
        # https://technet.tmax.co.kr/upload/download/online/tibero/pver-20220224-000002/index.html

        # this is the only way to ensure a transaction is started without
        # actually running DML. There's no way to see the configured
        # isolation level without getting it from v$transaction which
        # means transaction has to be started.
        cursor = dbapi_connection.cursor()
        try:
            # 임시 함수 get_trans_id_34218484 생성 / 34218484는 임의의 숫자입니다.
            # oracle은 out parameter를 지원하지만 pyodbc에서는 불가능한 것 같습니다.
            # 이로 인해 편법으로 임시 함수를 만들고 select function() from dual 형식으로
            # 사용 했습니다.

            # 티베로에는 local_transaction_id 함수가 존재하나 문서가 없습니다. 언제든 스펙이
            # 바뀔 수 있다는 문제가 있습니다.
            cursor.execute("""
                CREATE FUNCTION get_trans_id_34218484 RETURN VARCHAR IS
                    trans_id VARCHAR(100);
                BEGIN
                    trans_id := dbms_transaction.local_transaction_id(TRUE);
                    RETURN trans_id;
                END;
            """)

            cursor.execute("SELECT get_trans_id_34218484 FROM dual")
            trans_id = cursor.fetchone()[0]
            xidusn, xidslot, xidsqn = trans_id.split(".", 2)
            # 티베로의 여러 view를 보면 (xidusn, xidslot, xidsqn) 또는 (usn, slot, wrap)
            # 칼럼명을 씁니다. 이를 보아 티베로 테이블 칼럼 이름의 통일성이 없는 문제가 있습니다.
            # flag의 내용이 oracle이랑 많이 다릅니다. 다른 연구원에게 물어서 대략적으로 transaction
            # level을 찾는 것을 알아냈지만 확실하지 않습니다. 문서도 없어서 아래의 코드는 언젠가 깨질 수도
            # 있습니다.
            cursor.execute(
                """
                SELECT CASE flag
                WHEN 0 THEN 'SERIALIZABLE'
                ELSE 'READ COMMITTED' END AS isolation_level
                FROM v$transaction WHERE
                usn = ? AND slot = ? AND wrap = ?
                """,
                (xidusn, xidslot, xidsqn),
            )
            row = cursor.fetchone()
            if row is None:
                raise exc.InvalidRequestError(
                    "could not retrieve isolation level"
                )
            result = row[0]
        finally:
            cursor.execute("DROP FUNCTION GET_TRANS_ID_34218484")
            cursor.close()

        return result

    def set_isolation_level(
        self,
        dbapi_connection,
        level,
    ) -> None:
        if level == "AUTOCOMMIT":
            dbapi_connection.autocommit = True
        else:
            supported_levels = self.get_isolation_level_values(
                dbapi_connection
            )
            assert (
                level in supported_levels
            ), f"{level} is an unsupported isolation level"

            dbapi_connection.autocommit = False
            cursor = dbapi_connection.cursor()
            cursor.execute(f"ALTER SESSION SET ISOLATION_LEVEL={level}")
            cursor.commit()

    def on_connect(self):
        super_ = super().on_connect()

        def on_connect(conn):
            if super_ is not None:
                super_(conn)

            _TiberoInterval._add_pyodbc_output_converter(conn)

            # declare Unicode encoding for pyodbc as per
            #   https://github.com/mkleehammer/pyodbc/wiki/Unicode
            conn.setdecoding(pyodbc.SQL_CHAR, encoding=self.char_encoding)
            conn.setdecoding(pyodbc.SQL_WCHAR, encoding=self.wchar_encoding)

        return on_connect

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username="user")
        opts.update(url.query)

        keys = opts

        query = url.query

        connect_args = {}
        for param in ("ansi", "unicode_results", "autocommit"):
            if param in keys:
                connect_args[param] = util.asbool(keys.pop(param))

        if "odbc_connect" in keys:
            connectors = [util.unquote_plus(keys.pop("odbc_connect"))]
        else:

            def check_quote(token):
                if ";" in str(token) or str(token).startswith("{"):
                    token = "{%s}" % token.replace("}", "}}")
                return token

            keys = dict((k, check_quote(v)) for k, v in keys.items())

            dsn_connection = "dsn" in keys or (
                "host" in keys and "database" not in keys
            )
            if dsn_connection:
                connectors = [
                    "dsn=%s" % (keys.pop("host", "") or keys.pop("dsn", ""))
                ]
            else:
                connectors = []
                driver = keys.pop("driver", self.pyodbc_driver_name)
                if driver is None and keys:
                    # note if keys is empty, this is a totally blank URL
                    util.warn(
                        "No driver name specified; "
                        "this is expected by PyODBC when using "
                        "DSN-less connections"
                    )
                else:
                    connectors.append("DRIVER={%s}" % driver)

                connectors.extend(
                    [
                        "Server=%s" % (keys.pop("host", "")),
                        "Port=%s" % (keys.pop("port", "")),
                        "Database=%s" % keys.pop("database", ""),
                    ]
                )

            user = keys.pop("user", None)
            if user:
                connectors.append("UID=%s" % user)
                pwd = keys.pop("password", "")
                if pwd:
                    connectors.append("PWD=%s" % pwd)
            else:
                authentication = keys.pop("authentication", None)
                if authentication:
                    connectors.append("Authentication=%s" % authentication)
                else:
                    connectors.append("Trusted_Connection=Yes")

            # if set to 'Yes', the ODBC layer will try to automagically
            # convert textual data from your database encoding to your
            # client encoding.  This should obviously be set to 'No' if
            # you query a cp1253 encoded database from a latin1 client...
            if "odbc_autotranslate" in keys:
                connectors.append(
                    "AutoTranslate=%s" % keys.pop("odbc_autotranslate")
                )

            connectors.extend(["%s=%s" % (k, v) for k, v in keys.items()])

        return [[";".join(connectors)], connect_args]


dialect = TiberoDialect_pyodbc
