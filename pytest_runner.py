# run_tests.py
import pytest
import os

os.environ["TB_HOME"] = "/media/tibero/data/tibero/tibero7/repos/develop"
# os.environ["TBCLI_LOG_LVL"] = "TRACE"
# os.environ["TBCLI_LOG_DIR"] = "/tmp"

def main():
    result = pytest.main(["--db", "tibero"])

    if result == 0:
        print("All tests passed.")
    else:
        print(f"Tests failed with code: {result}")

if __name__ == "__main__":
    main()
