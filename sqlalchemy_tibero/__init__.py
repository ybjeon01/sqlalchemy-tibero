from . import base  # noqa
from . import pyodbc  # noqa
from .base import BFILE
from .base import BINARY_DOUBLE
from .base import BINARY_FLOAT
from .base import BLOB
from .base import CHAR
from .base import CLOB
from .base import DATE
from .base import DOUBLE_PRECISION
from .base import FLOAT
from .base import INTERVAL
from .base import LONG
from .base import NCHAR
from .base import NCLOB
from .base import NUMBER
from .base import NVARCHAR
from .base import NVARCHAR2
from .base import RAW
from .base import ROWID
from .base import TIMESTAMP
from .base import VARCHAR
from .base import VARCHAR2


base.dialect = dialect = pyodbc.dialect

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
)

__version__ = "1.4.55"

# TODO: 내가 (전영배)가 알기로는 setup.py에 다음의 라인을 추가하면 자동으로 register되는 것으로 알고 있습니다.
#         entry_points = {
#             "sqlalchemy.dialects": [
#                 "tibero.pyodbc = sqlalchemy_tibero.pyodbc:TiberoDialect_pyodbc",
#             ]
#         },
#       하지만 자동으로 등록이 되지 않아 아래왜 같이 트릭을 썼습니다. 자세히 이유 분석 및 해결이 필요합니다.
from sqlalchemy.dialects import registry as _registry

_registry.register(
    "tibero", "sqlalchemy_tibero.pyodbc", "TiberoDialect_pyodbc"
)
