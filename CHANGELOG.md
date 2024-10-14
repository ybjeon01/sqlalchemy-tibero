
# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [2.0.0a13] - 2024-10-14

### Added
- setinputsizes 사용할때 올바른 타입으로 연결, 77b227141a
  - 지금까지는 setupinputsizes 자체를 사용안했습니다.
- test/test_types.py 파일 추가, cda729d8a7
### Changed
- 데이터베이스에서 메타 정보를 얻을 때 Number(38, 0)이 sqlalchemy에서 Integer로 사용되도록 결정, d1254515c2
- pyodbc에서 사용가능한 oracle dialect 옵션만 남기기, a647342e88
### Fixed
- 문자 수 기준 char, varchar 지원안되는 버그 해결, 1af2a337fa
- executemany()를 사용시 cursor.rowcount가 잘못된 정보를 가지고 있는 문제 우회, 066444a7b1 
- requirements.py 파일 수정, 01c0a6d119 
  - pyodbc 사용으로 인해 oracle에서 실패한 것이 성공해서 수정했습니다.
- pyodbc 모듈에서 dialect라는 public property 추가, 7487b468ff
  - 테스트할 때 쓰이기에 추가했습니다.

## [2.0.0a12] - 2024-10-07

### Added
- pyodbc에서 지원하지 않는 datetime.timedelta 우회 지원,  961d5c1b9c
- multi value insert 지원하기, ca35abcee5
- returning into 기능 추가, 12d3557541
### Changed
- pyodbc 제한으로 제공되지 않는 coerce_to_decimal 파라미터 제거, 7058b091a9
### Fixed
- Numeric type의 as_decimal parameter 버그 수정, 7516cf6090

## [2.0.0a11] - 2024-09-25

### Added
- 테스트 문서 추가 및 원본 문서 수정, 6e768155db
  - sqlalchemy repo의 test directory를 사용해서 tibero dialect를
    테스트하는 문서 추가 (test suite가 아님)
  - 잘못 표기된 지원되는 python, pyodbc, sqlalchemy version을 수정
### Changed
### Fixed

## [2.0.0a10] - 2024-09-23

### Added
- Tibero Compiler용 테스트 파일 추가, 43205e7020
- TiberoDialect에 새로운 연산자 추가, 7d0623269c
### Changed
- 오래된 링크 업데이트, db4427bd66
### Fixed

## [2.0.0a9] - 2024-09-13

### Added
### Changed
- README.md에 test-branch에 대한 설명 추가, 35b710b5cd
### Fixed
- setup.cfg에 잘못 명시된 long_description file 포멧 변경, 4bfb3b482d

## [2.0.0a8] - 2024-09-12

### Added
### Changed
### Fixed
- pypi test server에 잘못된 2.0.0a7이 올라가 있습니다. 어쩔 수 없이 a8로 올립니다.

## [2.0.0a7] - 2024-09-12

### Added
- 설치 가이드 문서 추가, 23a340292d
- 개발을 위한 설치 도구를 setup.cfg에 명시, 54b15639df
### Changed
- Tibero Dialect에 맞는 README.md 파일 생성, a97788d52f
- Tibero Dialect에 맞게 작성되지 않은 문서 지우기, 62eab6ed92
- AUTHORS과 LICENSE 파일 업데이트, b2cfc0f820
### Fixed

## [2.0.0a6] - 2024-09-12

### Added
- ruff 도구 설정 추가, 57b8ed77a1

### Changed
- 앞으로 ruff tool을 쓰기 위해서 flake8 설정 지우기, ae32b7e1ea
- setuptools에서 설정을 setup.py에 넣는 것이 아닌 setup.cfg에 넣기, b1d91dbbdb
- .gitignore를 python project에 맞게 수정, 04fe626899

### Fixed
- PYODBCTiberoTIMESTAMP에 대해 올바른 dbapi 타입 반환, cc071ee459
- Tibero Dialect의 returning clause 비활성화, 81c2b0d82e
- (sql에서 필드 선택이 아닌) 클래스안의 정수형 필드를 select할 때 Decimal로 반환되는 버그 우회, b378a08c75
- Tibero Dialect 자체적으로 index 이름을 오라클과 비슷하게 반환하도록 수정, f5536ee
  - Oracle Dialect의 행동과 똑같이 하도록 하기 위함
  - sqlalchemy test suite에 있는 oracle 테스트 정답지를 그대로 사용하기 위함
- 없앴던 profile_file 설정 다시 추가, 8ea95a08b9
  - test suite를 사용하기 위해 필요한 것으로 추정

## [2.0.0a5] - 2024-08-29

### Added
 
### Changed
- 티베로의 경우만 sqlalchemy 테스트의 정답지가 틀린 것을 수정, cc071ee459

### Fixed

## [2.0.0a4] - 2024-08-29

### Added
 
### Changed

### Fixed
- 테스트할 때 누락된 oracle test 실행하도록 수정, c0b92133e8
- connection당 pyodbc conn.setdecoding가 실행되도록 수정, 62394f8333
- connection.execute()할 때 올바른 python 타입을 반환못했던 문제 해결, cf90f62946

## [2.0.0a3] - 2024-08-29

### Added
 
### Changed

### Fixed
- virtual column에 대해 get_columns()이 올바른 값을 주지 못했던 문제 해결, c23babad25

## [2.0.0a2] - 2024-08-20

### Added
 
### Changed
- get_isolation_level() 메서드 구현 완료
- AUTHORS에 작성자 이름 변경

### Fixed
- set_isolation_level() 메서드 인자가 AUTOCOMMIT일 때 예외가 발생하던 문제 해결 완료
