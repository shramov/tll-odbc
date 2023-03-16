#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import pytest

import os
import pathlib
import tempfile

import tll.logger
tll.logger.init()

from tll.channel import Context

@pytest.fixture
def odbcini(tmp_path):
    db = f'{tmp_path}/test.db'
    kw = {
        'settings.description': 'Test SQLite database',
        'driver': 'SQLite3',
        'database': db,
        'settings.trace': 'Yes',
        'settings.tracefile': f'{tmp_path}/sqlite.log',
    }
    return kw

@pytest.fixture
def context():
    ctx = Context()
    ctx.load(os.path.join(os.environ.get("BUILD_DIR", "build"), "tll-odbc"))
    return ctx
