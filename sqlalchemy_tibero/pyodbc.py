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
from typing import cast

import pyodbc

from sqlalchemy.engine import interfaces
from sqlalchemy import exc
from sqlalchemy.engine.interfaces import DBAPIConnection, IsolationLevel
from .base import TiberoExecutionContext, TiberoDialect
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy.types import BLOB
from sqlalchemy.types import CLOB
from sqlalchemy.types import NCHAR
from sqlalchemy.types import TIMESTAMP

from .types import NCLOB


class TiberoExecutionContext_pyodbc(TiberoExecutionContext):
    pass


class TiberoDialect_pyodbc(PyODBCConnector, TiberoDialect):
    pyodbc_driver_name = "Tibero"
    execution_ctx_cls = TiberoExecutionContext_pyodbc
    colspecs = TiberoDialect.colspecs
    supports_statement_cache = True
    bind_typing = 2

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

    def initialize(self, connection):
        pyodbc_conn = cast(pyodbc.Connection, connection.connection.dbapi_connection)
        pyodbc_conn.setdecoding(pyodbc.SQL_CHAR, self.char_encoding)
        pyodbc_conn.setdecoding(pyodbc.SQL_WCHAR, self.wchar_encoding)

        super().initialize(connection)

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
            # 티베로의 여러 view를 보면 (xidusn, xidslot, xidsqn) 또는 (usn, slot, wrap) 칼럼명을 씁니다.
            # 이를 보아 이름의 통일성이 없는 문제가 있습니다.
            cursor.execute(
                """
                SELECT CASE flag
                WHEN 0 THEN 'SERIALIZABLE'
                ELSE 'READ COMMITTED' END AS isolation_level
                FROM v$transaction WHERE
                usn = ? AND slot = ? AND wrap = ?
                """, (xidusn, xidslot, xidsqn)
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
            supported_levels = self.get_isolation_level_values(dbapi_connection)
            assert level in supported_levels, f"{level} is an unsupported isolation level"

            dbapi_connection.autocommit = False
            cursor = dbapi_connection.cursor()
            cursor.execute(f"ALTER SESSION SET ISOLATION_LEVEL={level}")
            cursor.commit()
