# Tibero Dialect와 함께 SQLAlchemy 소스 코드 내 테스트 실행하기

이 가이드는 SQLAlchemy 소스 코드에 포함된 테스트 디렉토리에서 Tibero Dialect를 사용하여 테스트를
실행하는 방법을 설명합니다. SQLAlchemy의 공식 테스트 스위트가 아니라, 소스 코드에 있는
`test` 폴더안 테스트들을 Tibero 환경에서 수행하는 절차를 다룹니다. 이 문서에는 unixODBC 설치와 ODBC 설정
파일 구성, 그리고 Tibero ODBC 드라이버를 등록하는 방법도 포함됩니다.

- **Note**: pypi를 통해 sqlalchemy를 설치하면 테스트 스위트에 여전히 접근가능하지만 test 폴더는 생략되어 있습니다.

---

## 1. `unixODBC` 설치 및 설정

`unixODBC`를 설치한 후, `odbc.ini` 및 `odbcinst.ini` 파일을 설정하고 Tibero ODBC 드라이버를 등록합니다.

## 1.1 `unixODBC` 설치

```bash
# debian 계열 리눅스 배포판에서 설치 방법
sudo apt install unixodbc
```

## 1.2 전역 ODBC 설정

전역 ODBC 설정 파일은 시스템 전체에서 사용할 수 있으며, `/etc/odbc.ini`와 `/etc/odbcinst.ini`
파일을 수정하여 구성할 수 있습니다. 예시 파일은 아래와 같습니다.

- **Note**: 홈 디렉토리에서 `~/.odbc.ini` 및 `~/.odbcinst.ini` 파일을 수정할 수도 있으나 이 설명서에서는
개인 환경에 맞는 설정 방법을 생략했습니다.

### `/etc/odbc.ini`

```ini
[Tibero7]
Description = Tibero7 ODBC Datasource
Driver      = Tibero7Driver
SID         = tibero
User        = tibero
Password    = tmax
```

- **Driver**: `odbcinst.ini` 파일에 등록된 Tibero 드라이버 이름과 일치해야 합니다.
- **SID**: 데이터베이스 이름(Tibero 데이터베이스의 SID)입니다. 사용자 환경에 맞게 고쳐주십시오.
`tbdsn.tbr`를 확인해주세요.
- **User** 및 **Password**: 연결할 때 사용할 Tibero 데이터베이스의 사용자명과 비밀번호입니다.
사용자 환경에 맞게 고쳐주십시오.

### `/etc/odbcinst.ini`

```ini
[ODBC]
Trace=yes
TraceFile=/tmp/unixodbc.trace

[Tibero7Driver]
Description=ODBC Driver for Tibero 7
Driver=/media/tibero/data/tibero/tibero7/repos/develop/client/lib/libtbodbc.so
```

- **Trace**: ODBC 드라이버의 트레이스 기능을 활성화합니다. 문제가 발생했을 때 디버깅 용도로 사용됩니다.
- **TraceFile**: ODBC 드라이버의 트레이스 기능이 활성화시 저장할 파일 위치입니다.
- **Description**: Tibero ODBC 드라이버에 대한 설명입니다. 사용자가 원하는 문자열을 아무것나 입력해도 됩니다.
- **Driver**: Tibero ODBC 드라이버의 경로입니다. 환경에 따라 이 경로는 다를 수 있으므로, 실제 Tibero 클라이언트가 설치된 경로로 수정해야 합니다.

## 2. SQLAlchemy 및 Tibero Dialect 저장소 복제

## 2.1 SQLAlchemy 공식 저장소를 복제

SQLAlchemy 공식 저장소를 복제합니다.

```bash
# 최신 커밋만 다운로드
git clone --depth 1 https://github.com/sqlalchemy/sqlalchemy.git
```

## 2.2 Tibero Dialect 테스트 브랜치 복제

```bash
# 최신 커밋만 다운로드
git clone --depth 1 https://github.com/ybjeon01/sqlalchemy-tibero.git -b test-branch
```

- **Warning**: PyPI에서 제공하는 Tibero Dialect를 설치하면 안 되며, 테스트용 코드가 포함된 브랜치를 사용해야 합니다.

---

## 3. SQLAlchemy 환경 설정

1. SQLAlchemy 저장소 디렉토리로 이동합니다.

    ```bash
    cd sqlalchemy
    ```

2. 가상 환경을 설정하고 활성화합니다.

    ```bash
    virtualenv .venv
    source .venv/bin/activate
    ```

3. 복제한 Tibero Dialect를 설치합니다.

    ```bash
    pip install -e ../sqlalchemy-tibero[pyodbc]
    ```

4. `pytest`를 설치합니다.

    ```bash
    pip install pytest
    ```

---

## 4. 데이터베이스 설정 변경

1. 현재 디렉토리 (sqlalchemy)에 있는 `setup.cfg` 파일을 열어 `[db]` 섹션에서 기본 설정을
변경합니다. **SQLite 메모리 DB 대신 Tibero DB**를 사용하도록 설정합니다.
    ```ini
    [db]
    # 포멧은 다음과 같습니다 <username>:<password>@<odbc.ini에 명시된 data source name>
    # 위의 odbc.ini 예시를 그대로 이용한다면 username과 password를 명시해줬기 때문에
    # "tibero:tmax"부분을 삭제하셔도 됩니다. 
    default = tibero+pyodbc://tibero:tmax@Tibero7
    ```

2. Tibero에 맞는 테스트 요구사항을 적용하기 위해, `sqlalchemy/test/requirements.py`
파일을 Tibero Dialect에서 제공하는 파일로 교체합니다.

    ```bash
    cp ../sqlalchemy-tibero/sqlalchemy_tibero/requirements.py test/requirements.py
    ```

---

## 5. 테스트 실행 방법

Tibero Dialect를 테스트하려면 환경 변수를 설정해야 합니다. 이를 위해 `pytest_runner.py` 파일을 사용하여
자동으로 설정할 수 있습니다.

1. Tibero Dialect 저장소에서 `pytest_runner.py` 파일을 복사합니다.

    ```bash
    cp ../sqlalchemy-tibero/pytest_runner.py .
    ```

2. `pytest_runner.py` 파일을 열고 `TB_HOME` 변수를 설정하여 Tibero ODBC 드라이버가 데이터
소스 파일(`tbdsn.tbr`)을 찾을 수 있도록 합니다.

3. `pytest_runner.py` 파일 내에서 아래 코드를 수정하여 Tibero Dialect를 사용하도록 변경합니다.

    ```python
    result = pytest.main(["--db", "default"])
    ```

---

## 6. 특정 테스트 실행하기

아래 4가지 방법 중 필요한 것을 선택해서 설정해주세요.

1. **특정 파일**의 테스트를 실행하려면, 파일 경로를 추가합니다.

    ```python
    result = pytest.main(["--db", "default", "test/orm/test_ac_relationships.py"])
    ```

2. **특정 디렉토리**의 모든 테스트를 실행하려면, 디렉토리 경로를 추가합니다.

    ```python
    result = pytest.main(["--db", "default", "test/orm"])
    ```

3. **특정 클래스**의 테스트를 실행하려면, 클래스명을 추가합니다.

    ```python
    result = pytest.main(["--db", "default", "test/orm/test_ac_relationships.py::AliasedClassRelationshipTest"])
    ```

4. **특정 메서드**만 실행하려면, 메서드명을 지정합니다.

    ```python
    result = pytest.main(["--db", "default", "test/orm/test_ac_relationships.py::AliasedClassRelationshipTest::test_join_one"])
    ```

---

## 7. 테스트 실행

설정이 완료되면, pytest_runner.py 파일을 실행하여 Tibero Dialect와 함께 SQLAlchemy 소스 코드의
테스트를 진행할 수 있습니다.

```bash
python pytest_runner.py
````

이 스크립트는 필요한 환경 변수를 자동으로 설정하고, Tibero 데이터베이스 환경에서
모든 테스트를 실행합니다. 실행 결과에 따라, 필요에 따라 테스트 파일이나 디렉토리,
특정 클래스 또는 메서드를 지정하여 원하는 테스트만 실행할 수 있습니다.


## 8. pyproject.toml 파일을 통한 pytest 설정 변경 (선택 사항)

SQLAlchemy의 pytest 설정은 pyproject.toml 파일 내에서 관리됩니다. 따라서, 테스트 환경을
더 세밀하게 조정하거나 설정을 변경하고 싶다면, 이 파일을 수정할 수 있습니다. 예를 들어, 추가 테스트
옵션, 경고 필터링, 테스트 파일 규칙 등을 설정할 수 있습니다.

아래는 현재 디렉토리 (sqlalchemy) pyproject.toml 파일에서 실제로 사용되는 pytest 설정 부분입니다.

```toml
[tool.pytest.ini_options]
addopts = "--tb native -v -r sfxX --maxfail=250 -p warnings -p logging --strict-markers"
norecursedirs = "examples build doc lib"
python_files = "test_*.py"
minversion = "6.2"
filterwarnings = [
    # NOTE: 추가적인 SQLAlchemy 특정 필터는
    # sqlalchemy/testing/warnings.py에 있습니다. SQLAlchemy 모듈은 이곳에
    # 명시될 수 없으며 pytest가 이를 즉시 로드하기 때문에
    # coverage 및 sys.path 조정에 문제가 발생할 수 있습니다.
    "error::DeprecationWarning:test",
    "error::DeprecationWarning:sqlalchemy",

    # sqlite3 관련 경고: test/dialect/test_sqlite.py->test_native_datetime,
    # 이 테스트는 파이썬 3.12에서 deprecated 된 핸들러가 동작하는지 확인합니다.
    "ignore:The default (date)?(time)?(stamp)? (adapter|converter):DeprecationWarning",
]
markers = [
    "memory_intensive: 메모리 / CPU 집약적인 테스트",
    "mypy: mypy 통합 / 플러그인 테스트",
    "timing_intensive: 시간에 민감한 테스트 (경쟁 상태에 취약)",
    "backend: 모든 백엔드에서 실행되어야 하는 테스트; 주로 다이얼렉트에 민감한 테스트",
    "sparse_backend: 일부 백엔드에서 실행되지만, 반드시 모두는 아닌 테스트",
]
```

- **`addopts`**: `pytest`에 추가적인 명령줄 옵션을 제공합니다. 예를 들어, `-v`는 자세한 출력을 활성화하고,
`--maxfail=250`은 최대 250개의 실패가 발생하면 테스트를 중지하도록 설정합니다.

- **`norecursedirs`**: 테스트에서 제외할 디렉토리 목록입니다.

- **`python_files`**: 테스트 파일의 이름 패턴을 정의합니다. 여기서는 `test_*.py`로 설정되어 있어,
`test_`로 시작하는 모든 `.py` 파일이 테스트로 인식됩니다.

- **`filterwarnings`**: 테스트 중 무시하거나 에러로 처리할 경고를 정의합니다. 예를 들어, 특정 경고를
에러로 처리하거나 특정 모듈에서 발생하는 경고를 무시하도록 설정할 수 있습니다.

- **`markers`**: 테스트에 사용할 마커를 정의합니다. 이는 테스트를 카테고리화하거나 특정 조건에서만 실행할
수 있도록 도와줍니다.

---

## 9. tox.ini 파일

`tox.ini`는 테스트 환경을 자동화하고 가상 환경을 관리하는 데 사용되는 파일입니다.
이를 통해 다양한 파이썬 버전과 환경에서 일관된 테스트를 수행할 수 있습니다.
다만, 이 가이드에서는 `tox.ini` 파일의 구체적인 사용 방법에 대해서는 다루지
않습니다. tox를 사용하여 테스트 환경을 구성하는 방법에 대해서는
[tox](https://tox.wiki/en) 공식 문서 참고해주시기 바랍니다.
