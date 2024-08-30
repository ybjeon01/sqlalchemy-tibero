
# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).
 
## [2.0.0a2] - 2024-08-20

### Added
 
### Changed
- get_isolation_level() 메서드 구현 완료
- AUTHORS에 작성자 이름 변경

### Fixed
- set_isolation_level() 메서드 인자가 AUTOCOMMIT일 때 예외가 발생하던 문제 해결 완료

## [2.0.0a3] - 2024-08-29

### Added
 
### Changed

### Fixed
- virtual column에 대해 get_columns()이 올바른 값을 주지 못했던 문제 해결, c23babad25

## [2.0.0a4] - 2024-08-29

### Added
 
### Changed

### Fixed
- 테스트할 때 누락된 oracle test 실행하도록 수정, c0b92133e8
- connection당 pyodbc conn.setdecoding가 실행되도록 수정, 62394f8333
- connection.execute()할 때 올바른 python 타입을 반환못했던 문제 해결, cf90f62946


## [2.0.0a5] - 2024-08-29

### Added
 
### Changed
- 티베로의 경우만 sqlalchemy 테스트의 정답지가 틀린 것을 수정, cc071ee459

### Fixed
