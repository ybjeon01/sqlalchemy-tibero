from .base import DOUBLE_PRECISION
from .base import REAL
from sqlalchemy.sql.sqltypes import BLOB
from sqlalchemy.sql.sqltypes import CHAR
from sqlalchemy.sql.sqltypes import CLOB
from sqlalchemy.sql.sqltypes import NCHAR
from sqlalchemy.sql.sqltypes import NVARCHAR
from sqlalchemy.sql.sqltypes import VARCHAR

from . import base  # noqa
from . import pyodbc
from .types import BFILE
from .types import BINARY_DOUBLE
from .types import BINARY_FLOAT
from .types import DATE
from .types import FLOAT
from .types import INTERVAL
from .types import LONG
from .types import NCLOB
from .types import NUMBER
from .types import NVARCHAR2
from .types import RAW
from .types import ROWID
from .types import TIMESTAMP
from .types import VARCHAR2

# TODO: 비동기 프로그램밍 지원하기
# Alias oracledb also as oracledb_async
# oracledb_async = type(
#     "oracledb_async", (ModuleType,), {"dialect": oracledb.dialect_async}
# )


base.dialect = dialect = pyodbc.TiberoDialect_pyodbc

# TODO: 티베로에서 지원안되는 타입들이 있는지 확인해보기
__all__ = (
    "VARCHAR",
    "NVARCHAR",
    "CHAR",
    "NCHAR",
    "DATE",
    "NUMBER",
    "BLOB",
    "BFILE",
    "CLOB",
    "NCLOB",
    "TIMESTAMP",
    "RAW",
    "FLOAT",
    "DOUBLE_PRECISION",
    "BINARY_DOUBLE",
    "BINARY_FLOAT",
    "LONG",
    "dialect",
    "INTERVAL",
    "VARCHAR2",
    "NVARCHAR2",
    "ROWID",
    "REAL",
)

__version__ = "2.0.0"

# TODO: 내가 (전영배)가 알기로는 setup.py에 다음의 라인을 추가하면 자동으로 register되는 것으로 알고 있습니다.
#         entry_points = {
#             "sqlalchemy.dialects": [
#                 "tibero.pyodbc = sqlalchemy_tibero.pyodbc:TiberoDialect_pyodbc",
#             ]
#         },
#       하지만 자동으로 등록이 되지 않아 아래왜 같이 트릭을 썼습니다. 자세히 이유 분석 및 해결이 필요합니다.
from sqlalchemy.dialects import registry as _registry
_registry.register("tibero", "sqlalchemy_tibero.pyodbc", "TiberoDialect_pyodbc")