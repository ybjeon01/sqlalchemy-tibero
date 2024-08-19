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
        # TODO: 네트워크통해서 현재 isolation level을 가져오기
        #       티베로 jdbc 코드에서는 set_isolation_level() 메서드를 사용함으로서
        #       get_isolation_level() 메서드를 구현하는데 내가 (전영배)가 보기에는 올바른
        #       방법이 아닙니다. 사용자가 get_isolation_level()를 통해 현재
        #       isolation level을 볼려고 하는데 set_isolation_level을 통해 상태가 변경되면
        #       예상치 못한 결과가 발생할 수 있습니다.
        return "READ COMMITTED"

    def set_isolation_level(
        self,
        dbapi_connection: interfaces.DBAPIConnection,
        level: IsolationLevel,
    ) -> None:
        if level == "AUTOCOMMIT":
            dbapi_connection.autocommit = True

        supported_levels = self.get_isolation_level_values(dbapi_connection)
        assert level in supported_levels, f"{level} is an unsupported isolation level"

        dbapi_connection.autocommit = False
        cursor = dbapi_connection.cursor()
        cursor.execute(f"ALTER SESSION SET ISOLATION_LEVEL={level}")
        cursor.commit()
