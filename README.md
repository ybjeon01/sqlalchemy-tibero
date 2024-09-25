# SQLAlchemy Tibero Dialect

The SQLAlchemy Tibero Dialect is a SQLAlchemy extension designed to enable
interaction between SQLAlchemy and Tibero databases

---

## Installation Guide

For detailed installation instructions, please refer to the
`sqlalchemy-tibero_install_guide.md` file located in the same directory.

You can view the korean version here: [sqlalchemy-tibero_install_guide.md](./sqlalchemy-tibero_install_guide.md)

- **Note**: This branch is for testing purposes and differs from
the code available on PyPI.

---

## How to test with sqlalchemy test directory

Following document describes the procedure for executing the tests located
in the test folder of the source code in a Tibero environment,
instead of testing the official SQLAlchemy test suite.

You can find the document (korean version) here:
[how-to-test-with-sqlalchemy-test-directory.md](./how-to-test-with-sqlalchemy-test-directory.md)

---

## TODO

- [ ] Use GitHub workflow (run pre-commit and pytest, pypi upload)
- [ ] Use pre-commit (ruff check --diff, and ruff format --diff)
- [ ] Pass all tests in SQLAlchemy test suite and most of sqlalchemy/tests
- [ ] Create documentation on how to test the SQLAlchemy Tibero Dialect
- [ ] Create documentation on SQLAlchemy Tibero Dialect