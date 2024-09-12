# tibero/pyodbc.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Support for TmaxSoft Tibero via pyodbc.

pyodbc is available at:
    http://pypi.python.org/pypi/pyodbc/

Connecting
^^^^^^^^^^
Examples of pyodbc connection string URLs:
* ``tibero+pyodbc://mydsn`` - connects using the specified DSN named ``mydsn``.
"""

import os
import decimal

import pyodbc

from sqlalchemy import util
from sqlalchemy.engine import interfaces
from sqlalchemy.engine import processors
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.engine.interfaces import IsolationLevel
from sqlalchemy import exc
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.sqltypes import BLOB
from sqlalchemy.sql.sqltypes import CLOB
from sqlalchemy.sql.sqltypes import NCHAR
from sqlalchemy.sql.sqltypes import TIMESTAMP


from .types import NCLOB
from . import types
from .base import TiberoExecutionContext, TiberoDialect, TiberoCompiler


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
        return None


class _TiberoUUID(sqltypes.Uuid):
    pass
    # def get_dbapi_type(self, dbapi):
    #     return dbapi.STRING


class _TiberoBinaryFloat(_TiberoNumeric):
    pass

    # def get_dbapi_type(self, dbapi):
    #     return dbapi.NATIVE_FLOAT


class _TiberoBINARY_FLOAT(_TiberoBinaryFloat, types.BINARY_FLOAT):
    pass


class _TiberoBINARY_DOUBLE(_TiberoBinaryFloat, types.BINARY_DOUBLE):
    pass


class _TiberoNUMBER(_TiberoNumeric):
    is_number = True


class _PYODBCTiberoDate(types._TiberoDate):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return value.date()
            else:
                return value

        return process


class _PYODBCTiberoTIMESTAMP(
    types._TiberoDateLiteralRender, sqltypes.TIMESTAMP
):
    def literal_processor(self, dialect):
        return self._literal_processor_datetime(dialect)

    def get_dbapi_type(self, dbapi):
        return dbapi.SQL_TYPE_TIMESTAMP


class _LOBDataType:
    pass


# TODO: the names used across CHAR / VARCHAR / NCHAR / NVARCHAR
# here are inconsistent and not very good
class _TiberoChar(sqltypes.CHAR):
    pass
    # def get_dbapi_type(self, dbapi):
    #     return dbapi.FIXED_CHAR


class _TiberoNChar(sqltypes.NCHAR):
    pass
    # def get_dbapi_type(self, dbapi):
    #     return dbapi.FIXED_NCHAR


class _TiberoUnicodeStringNCHAR(types.NVARCHAR2):
    pass
    # def get_dbapi_type(self, dbapi):
    #     return dbapi.NCHAR


class _TiberoUnicodeStringCHAR(sqltypes.Unicode):
    pass
    # def get_dbapi_type(self, dbapi):
    #     return dbapi.LONG_STRING


class _TiberoUnicodeTextNCLOB(_LOBDataType, types.NCLOB):
    pass
    # def get_dbapi_type(self, dbapi):
    #     # previously, this was dbapi.NCLOB.
    #     # DB_TYPE_NVARCHAR will instead be passed to setinputsizes()
    #     # when this datatype is used.
    #     return dbapi.DB_TYPE_NVARCHAR


class _TiberoUnicodeTextCLOB(_LOBDataType, sqltypes.UnicodeText):
    pass
    # def get_dbapi_type(self, dbapi):
    #     # previously, this was dbapi.CLOB.
    #     # DB_TYPE_NVARCHAR will instead be passed to setinputsizes()
    #     # when this datatype is used.
    #     return dbapi.DB_TYPE_NVARCHAR


class _TiberoText(_LOBDataType, sqltypes.Text):
    pass
    # def get_dbapi_type(self, dbapi):
    #     # previously, this was dbapi.CLOB.
    #     # DB_TYPE_NVARCHAR will instead be passed to setinputsizes()
    #     # when this datatype is used.
    #     return dbapi.DB_TYPE_NVARCHAR


class _TiberoLong(_LOBDataType, types.LONG):
    pass
    # def get_dbapi_type(self, dbapi):
    #     return dbapi.LONG_STRING


class _TiberoString(sqltypes.String):
    pass


class _TiberoEnum(sqltypes.Enum):
    def bind_processor(self, dialect):
        enum_proc = sqltypes.Enum.bind_processor(self, dialect)

        def process(value):
            raw_str = enum_proc(value)
            return raw_str

        return process


class _TiberoBinary(_LOBDataType, sqltypes.LargeBinary):
    # def get_dbapi_type(self, dbapi):
    #     # previously, this was dbapi.BLOB.
    #     # DB_TYPE_RAW will instead be passed to setinputsizes()
    #     # when this datatype is used.
    #     return dbapi.DB_TYPE_RAW

    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        if not dialect.auto_convert_lobs:
            return None
        else:
            return super().result_processor(dialect, coltype)


class _TiberoInterval(types.INTERVAL):
    pass
    # def get_dbapi_type(self, dbapi):
    #     return dbapi.INTERVAL


class _TiberoRaw(types.RAW):
    pass


class _TiberoRowid(types.ROWID):
    pass
    # def get_dbapi_type(self, dbapi):
    #     return dbapi.ROWID


class TiberoCompiler_pyodbc(TiberoCompiler):
    pass


class TiberoExecutionContext_pyodbc(TiberoExecutionContext):
    pass


class TiberoDialect_pyodbc(PyODBCConnector, TiberoDialect):
    # 아래 속성들을 보면 DefaultDialect에서 이미 같은 값으로 설정이 되어있기 때문에 생략해도 문제없는 코드가 있습니다. 하지만
    # OracleDialect_cx_oracle의 패턴을 따랐습니다.
    # 예를 들어, supports_statement_cache, supports_sane_rowcount들은 이미 DefaultDialect에서 True로 설정되어 있습니다.
    # 또한, execution_ctx_cls, statement_compiler 또한 TiberoDialect에 설정된 깂을 그대로 사용하지만
    # OracleDialect_cx_oracle의 패턴을 따랐습니다.

    supports_statement_cache = True
    execution_ctx_cls = TiberoExecutionContext_pyodbc
    statement_compiler = TiberoCompiler_pyodbc

    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    # OracleDialect_cx_oracle에서는 아래 4개의 항목 모두 True
    insert_executemany_returning = False
    insert_executemany_returning_sort_by_parameter_order = False
    update_executemany_returning = False
    delete_executemany_returning = False

    insert_returning = False  # OracleDialect에서는 True
    update_returning = False  # OracleDialect에서는 True
    delete_returning = False  # OracleDialect에서는 True

    bind_typing = interfaces.BindTyping.SETINPUTSIZES

    pyodbc_driver_name = "Tibero"

    colspecs = util.update_copy(
        TiberoDialect.colspecs,
        {
            sqltypes.TIMESTAMP: _PYODBCTiberoTIMESTAMP,
            sqltypes.Numeric: _TiberoNumeric,
            sqltypes.Float: _TiberoNumeric,
            types.BINARY_FLOAT: _TiberoBINARY_FLOAT,
            types.BINARY_DOUBLE: _TiberoBINARY_DOUBLE,
            sqltypes.Integer: _TiberoInteger,
            types.NUMBER: _TiberoNUMBER,
            sqltypes.Date: _PYODBCTiberoDate,
            sqltypes.LargeBinary: _TiberoBinary,
            sqltypes.Boolean: types._TiberoBoolean,
            sqltypes.Interval: _TiberoInterval,
            types.INTERVAL: _TiberoInterval,
            sqltypes.Text: _TiberoText,
            sqltypes.String: _TiberoString,
            sqltypes.UnicodeText: _TiberoUnicodeTextCLOB,
            sqltypes.CHAR: _TiberoChar,
            sqltypes.NCHAR: _TiberoNChar,
            sqltypes.Enum: _TiberoEnum,
            types.LONG: _TiberoLong,
            types.RAW: _TiberoRaw,
            sqltypes.Unicode: _TiberoUnicodeStringCHAR,
            sqltypes.NVARCHAR: _TiberoUnicodeStringNCHAR,
            sqltypes.Uuid: _TiberoUUID,
            types.NCLOB: _TiberoUnicodeTextNCLOB,
            types.ROWID: _TiberoRowid,
        },
    )

    def __init__(
        self,
        auto_convert_lobs=True,
        coerce_to_decimal=True,
        arraysize=50,
        encoding_errors=None,
        threaded=None,
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

        TiberoDialect.__init__(self, **kwargs)
        self.arraysize = arraysize
        self.encoding_errors = encoding_errors
        if encoding_errors:
            self._cursor_var_unicode_kwargs = {
                "encodingErrors": encoding_errors
            }
        if threaded is not None:
            self._cx_tibero_threaded = threaded
        self.auto_convert_lobs = auto_convert_lobs
        self.coerce_to_decimal = coerce_to_decimal
        self.include_set_input_sizes = {
            NCLOB,
            CLOB,
            NCHAR,
            BLOB,
            TIMESTAMP,
        }

    def get_isolation_level(
        self, dbapi_connection: DBAPIConnection
    ) -> IsolationLevel:
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
        dbapi_connection: interfaces.DBAPIConnection,
        level: IsolationLevel,
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

            # declare Unicode encoding for pyodbc as per
            #   https://github.com/mkleehammer/pyodbc/wiki/Unicode
            conn.setdecoding(pyodbc.SQL_CHAR, encoding=self.char_encoding)
            conn.setdecoding(pyodbc.SQL_WCHAR, encoding=self.wchar_encoding)

        return on_connect
