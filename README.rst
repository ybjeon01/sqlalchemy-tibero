sqlalchemy-tibero
=================

A TmaxSoft Tibero dialect for SQLAlchemy.

Objectives
----------

This dialect is mainly intended to offer
pandas users an easy way to save a DataFrame into an
Tibero database via ``to_sql``.

Pre-requisites
--------------

This dialect requires SQLAlchemy and pyodbc. They are both specified as requirements so ``pip`` will install
them if they are not already in place. To install, just::

    pip install sqlalchemy-tibero

Getting Started
---------------

Create an `ODBC DSN (Data Source Name)`_ that points to your Tibero database.
(Tip: For best results, enable `ExtendedAnsiSQL`_.)
Then, in your Python app, you can connect to the database via::

    from sqlalchemy import create_engine
    engine = create_engine("tibero+pyodbc://@your_dsn")

The SQLAlchemy Project
======================

SQLAlchemy-tibero is part of the `SQLAlchemy Project <https://www.sqlalchemy.org>`_ and
adheres to the same standards and conventions as the core project.

Development / Bug reporting / Pull requests
-------------------------------------------

Please refer to the
`SQLAlchemy Community Guide <https://www.sqlalchemy.org/develop.html>`_ for
guidelines on coding and participating in this project.

Code of Conduct
_______________

Above all, SQLAlchemy places great emphasis on polite, thoughtful, and
constructive communication between users and developers.
Please see our current Code of Conduct at
`Code of Conduct <https://www.sqlalchemy.org/codeofconduct.html>`_.

License
=======

SQLAlchemy-tibero is distributed under the `MIT license
<https://opensource.org/licenses/MIT>`_.
