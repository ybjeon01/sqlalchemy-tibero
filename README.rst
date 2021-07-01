sqlalchemy-tibero
=================

.. image:: https://img.shields.io/pypi/dm/sqlalchemy-tibero.svg
        :target: https://pypi.org/project/sqlalchemy-tibero/

A TmaxSoft Tibero dialect for SQLAlchemy.

Objectives
----------

This dialect is mainly intended to offer
pandas users an easy way to save a DataFrame into an
Tibero database via ``to_sql``.

Pre-requisites
--------------

- If you already have TmaxSoft Office (or standalone TmaxSoft Tibero) installed then install a version
  of Python with the same "bitness". For example, if you have 32-bit Office then you should install
  32-bit Python.

- If you do not already have TmaxSoft Office (or standalone TmaxSoft Tibero) installed then install
  the version of the TmaxSoft Tibero Database Engine Redistributable with the same "bitness" as the
  version of Python you will be using. For example, if you will be running 64-bit Python then you
  should install the 64-bit version of the Tibero Database Engine.

Special case: If you will be running 32-bit Python and you will **only** be working with .mdb files
then you can use the older 32-bit ``TmaxSoft Tibero Driver (*.mdb)`` that ships with Windows.

Co-requisites
-------------

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

For other ways of connecting see the `Getting Connected`_ page in the Wiki.

.. _ODBC DSN (Data Source Name): https://support.microsoft.com/en-ca/help/966849/what-is-a-dsn-data-source-name
.. _ExtendedAnsiSQL: https://github.com/sqlalchemy/sqlalchemy-tibero/wiki/%5Btip%5D-use-ExtendedAnsiSQL
.. _Getting Connected: https://github.com/sqlalchemy/sqlalchemy-tibero/wiki/Getting-Connected

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
