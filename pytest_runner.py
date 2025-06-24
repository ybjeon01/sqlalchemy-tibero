# run_tests.py
import pytest
import os

# 테스트하기 위해선 tibero라는 이름을 oracle로 변경해야 합니다.
from sqlalchemy_tibero import dialect
from sqlalchemy_tibero import base

dialect.name = "oracle"
base.OracleDialect = base.TiberoDialect


# os.environ["TB_HOME"] = "/media/tibero/work/tibero/repo/tibero7/tibero7_room2_0"
# os.environ["TBCLI_LOG_LVL"] = "TRACE"
# os.environ["TBCLI_LOG_DIR"] = "/tmp"

def main():
    # result = pytest.main(["--db", "tibero", "test/test_suite.py::UnicodeVarcharTest"])
    # result = pytest.main(["--db", "tibero", "test/test_suite.py::UnicodeVarcharTest::test_empty_strings_varchar"])
    # result = pytest.main(
    #    ["--db", "tibero", "test/test_suite.py::ComponentReflectionTest::test_comments_unicode_full"])
    # result = pytest.main(["--db", "tibero", "test/test_suite.py::ComponentReflectionTest::test_get_multi_table_comment"])
    # result = pytest.main(["--db", "tibero", "test/test_suite.py::AutocommitIsolationTest::test_autocommit_on"])
    result = pytest.main(["--db", "tibero", "test/test_suite.py::ComputedReflectionTest::test_computed_col_default_not_set"])
    # result = pytest.main(["--db", "tibero"])

    if result == 0:
        print("All tests passed.")
    else:
        print(f"Tests failed with code: {result}")

if __name__ == "__main__":
    main()
