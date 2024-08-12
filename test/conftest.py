from sqlalchemy.dialects import registry
import pytest

registry.register(
    "tibero.pyodbc", "sqlalchemy_tibero.pyodbc", "TiberoDialect_pyodbc"
)

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa
