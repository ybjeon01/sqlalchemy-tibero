# sqlalchemy-tibero 패키지 빌드 및 배포 방법

이 가이드는 sqlalchemy-tibero 패키지를 빌드하고, PyPI 테스트 서버와 공식 PyPI 서버에 배포하는 방법에
대해 설명합니다.

## 1. 이전 배포 파일 삭제하기

새로운 빌드를 위해 이전에 생성된 배포 파일을 삭제합니다. 다음 명령어를 실행하여 dist/ 디렉토리를 정리합니다.
만약 dist 폴더가 존재하지 않는다면, 2단계로 넘어가 주시기 바랍니다.

```bash
cd <path to sqlalchemy-tibero repo>
rm -rf dist/*
```

## 2. 패키지 빌드하기

패키지를 배포하기 위해 먼저 소스 배포판(`sdist`)과 휠 배포판(`bdist_wheel`)을 생성해야 합니다. 

### 1단계: `setuptools` 설치

`setuptools`이 설치되어 있지 않다면 다음 명령어로 설치합니다:

```bash
pip install setuptools
```

### 2단계: 패키지 빌드

다음 명령어를 실행하여 패키지를 빌드합니다. 이 명령어는 `setup.py` 파일이 있는 sqlalchemy-tibero
repo 루트 디렉토리에서 실행해야 합니다:

```bash
python setup.py sdist bdist_wheel
```

이 명령어를 실행하면 `dist/`라는 디렉토리가 생성되고, 그 안에 `.tar.gz`(소스 배포판)와 `.whl`(휠 배포판) 파일이 생성됩니다.

## 3. 패키지 배포하기

### 1단계: `twine` 설치

패키지를 PyPI에 업로드하려면 `twine`이라는 도구를 사용해야 합니다. `twine`이 설치되어 있지 않다면, 다음 명령어로 설치합니다:

```bash
pip install twine
```

### 2단계: 패키지 메타데이터 확인

`twine`의 `check` 명령어를 사용하여 패키지의 메타데이터를 검증합니다. 다음 명령어를 실행합니다:

```bash
twine check dist/* # dist directory안에 있는 두개의 패키지를 검사합니다.
```

### 3단계: PyPI 테스트 서버에 배포

공식 PyPI에 배포하기 전에 테스트 서버에서 먼저 테스트하는 것이 좋습니다. 아래 명령어로 패키지를 테스트 PyPI에 업로드합니다:

```bash
twine upload -r testpypi dist/* # dist directory안에 있는 두개의 패키지를 업로드합니다.
```

이때 PyPI 테스트 서버의 사용자 API Token 입력하라는 메시지가 표시됩니다. 올바른 API Token을 입력하면 패키지가 PyPI
테스트 서버로 업로드됩니다.

### 4단계: 패키지 설치 테스트

패키지를 테스트 PyPI에서 설치해볼 수 있습니다. 다음 명령어를 사용하여 테스트 서버에서 패키지를 설치합니다:

```bash
python -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple sqlalchemy-tibero[pyodbc]
```

`--extra-index-url`을 넣어준 이유는 `sqlalchemy`와 `pyodbc` 패키지를 공식 PyPI 서버로부터 다운로드받기 위함입니다.


### 5단계: 공식 PyPI 서버에 배포

테스트가 완료되면, 이제 패키지를 공식 PyPI 서버에 업로드할 수 있습니다. 아래 명령어를 사용합니다:

```bash
twine upload dist/* # dist directory안에 있는 두개의 패키지를 업로드합니다.
```

이때 공식 PyPI의 사용자 API Token 입력하라는 메시지가 표시됩니다. 올바른 API Token을 입력하면 패키지가 PyPI
테스트 서버로 업로드됩니다.

### 6. 배포 후 확인

배포가 완료되면 [PyPI](https://pypi.org/) 웹사이트에 접속하여 sqlchemy-tibero 패키지가 제대로 등록되었는지 확인합니다. 
