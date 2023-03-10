#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import pytest

import os
import pathlib
import tempfile

import tll.logger
tll.logger.init()

from tll.channel import Context

version = tuple([int(x) for x in pytest.__version__.split('.')[:2]])

if version < (3, 9):
    @pytest.fixture
    def tmp_path():
        with tempfile.TemporaryDirectory() as tmp:
            yield pathlib.Path(tmp)

@pytest.fixture(scope='session')
def odbcini(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp('odbc')
    db = f'{tmp_path}/test.db'
    #db = '/tmp/test.db'
    open(tmp_path / "odbc.ini", "w").write(f"""
[testdb]
Description=Test SQLite database
Driver=SQLite3
Database={db}
Trace=Yes
TraceFile={tmp_path}/sqlite.log
""")
    os.environ["ODBCINI"] = str(tmp_path / "odbc.ini")
    return {'ini': os.environ["ODBCINI"], 'db': db}

@pytest.fixture
def context():
    ctx = Context()
    ctx.load(os.path.join(os.environ.get("BUILD_DIR", "build"), "tll-odbc"))
    return ctx
