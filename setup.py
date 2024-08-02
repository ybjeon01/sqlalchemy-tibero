import os
import re

from setuptools import setup, find_packages

v = open(
    os.path.join(os.path.dirname(__file__), "sqlalchemy_tibero", "__init__.py")
)
VERSION = re.compile(r'.*__version__ = "(.*?)"', re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), "README.rst")


setup(
    name="sqlalchemy-tibero",
    version=VERSION,
    description="SQLAlchemy Tibero Dialect",
    long_description=open(readme).read(),
    url="https://github.com/cpyang/sqlalchemy-tibero",
    author="Conrad Yang",
    author_email="conrad.yang@tmaxsoft.com",
    license="MIT",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent",
    ],
    keywords="SQLAlchemy Microsoft Tibero",
    project_urls={
        "Documentation": "https://github.com/cpyang/sqlalchemy-tibero/wiki",
        "Source": "https://github.com/cpyang/sqlalchemy-tibero",
        "Tracker": "https://github.com/cpyang/sqlalchemy-tibero/issues",
    },
    packages=find_packages(include=["sqlalchemy_tibero"]),
    include_package_data=True,
    install_requires=[
        "SQLAlchemy",
        "pyodbc",
    ],
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects": [
            "tibero.pyodbc = sqlalchemy_tibero.pyodbc:TiberoDialect_pyodbc",
        ]
    },
)
