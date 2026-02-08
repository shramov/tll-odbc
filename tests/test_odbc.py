#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import pytest

import datetime
from decimal import Decimal
import pyodbc

from tll.error import TLLError
from tll.test_util import Accum
from tll.chrono import TimePoint

@pytest.fixture
def db(odbcini):
    r = pyodbc.connect(**odbcini)
    yield r
    r.close()

SCHEME = '''yamls://
- name: Data
  id: 10
  fields:
    - {name: f0, type: int8}
    - {name: f1, type: double}
    - {name: f2, type: string}
'''

@pytest.mark.parametrize("t,value",
        [('int8', -123),
        ('int16', -12312),
        ('int32', -123123123),
        ('int64', -123123123123123),
        ('uint8', 231),
        ('uint16', 54321),
        ('uint32', 2345678901),
        ('double', 123.123),
        ('decimal128', Decimal('123.456')),
        ('string, options.sql.column-type: "VARCHAR(8)"', 'string'),
        ('byte32, options.type: string', 'string'),
        ])
def test_field(context, db, odbcini, t, value):
    if db.getinfo(pyodbc.SQL_DBMS_NAME) == 'SQLite' and t == 'decimal128':
        pytest.skip("Decimal128 not supported on SQLite3")

    dbname = "Data"
    scheme = f'''yamls://
    - name: {dbname}
      id: 10
      fields:
        - {{name: f0, type: {t}}}
    '''

    with db.cursor() as c:
        c.execute('DROP TABLE IF EXISTS "Data"')

    c = Accum('odbc://;name=odbc;create-mode=checked', scheme=scheme, dump='scheme', context=context, **odbcini)
    c.open()
    c.post({'f0': value}, name=dbname, seq=100)
    #c.post({'f0': value}, name='Data', seq=2)

    if t not in ('int8', 'uint32'):
        assert [tuple(r) for r in db.cursor().execute(f'SELECT * FROM "Data"')] == [(100, value)]

    c.post({'message': 10}, name='Query', type=c.Type.Control)

    c.process()
    assert [(m.type, m.msgid, m.seq) for m in c.result] == [(c.Type.Data, 10, 100)]

    assert c.unpack(c.result[-1]).as_dict() == {'f0': value}

@pytest.mark.parametrize("query,result",
        [([], list(range(10))),
        ([{'field': 'f0', 'op': 'EQ', 'value': {'i': 1000}}], [1]),
        ([{'field': 'f0', 'op': 'LT', 'value': {'i': 1000}}], [0]),
        ([{'field': 'f0', 'op': 'LE', 'value': {'i': 1000}}], [0, 1]),
        ([{'field': 'f0', 'op': 'GT', 'value': {'i': 8000}}], [9]),
        ([{'field': 'f0', 'op': 'GE', 'value': {'i': 8000}}], [8, 9]),
        ([{'field': 'f0', 'op': 'GT', 'value': {'i': 1000}}, {'field': 'f1', 'op': 'LE', 'value': {'f': 500}}], [2, 3, 4]),
        ([{'field': 'f0', 'op': 'GT', 'value': {'i': 5000}}, {'field': 'f1', 'op': 'LE', 'value': {'f': 500}}], []),
        ([{'field': 'f2', 'op': 'EQ', 'value': {'s': '2'}}], [2]),
        ])
def test_query(context, db, odbcini, query, result):
    scheme = '''yamls://
    - name: Query
      id: 10
      fields:
        - {name: f0, type: int64}
        - {name: f1, type: double}
        - {name: f2, type: string}
    '''

    with db.cursor() as c:
        c.execute(f'DROP TABLE IF EXISTS "Query"')

    i = context.Channel('odbc://;name=insert;create-mode=checked', scheme=scheme, dir='w', **odbcini)
    i.open()
    for x in range(10):
        i.post({'f0': 1000 * x, 'f1': 100.5 * x, 'f2': str(x)}, name=f'Query', seq=x)

    s = Accum('odbc://;name=select', scheme=scheme, dump='scheme', context=context, **odbcini)
    s.open()
    s.post({'message': 10, 'expression': query}, name='Query', type=s.Type.Control)

    for _ in range(100):
        s.process()

    assert [(m.type, m.msgid, m.seq) for m in s.result] == [(s.Type.Data, 10, x) for x in result] + [(s.Type.Control, 50, 0)]
    for m, r in zip(s.result, result):
        assert s.unpack(m).as_dict() == {'f0': 1000 * r, 'f1': 100.5 * r, 'f2': str(r)}

def test_function(context, db, odbcini):
    scheme = '''yamls://
    - name: Input
      options.sql.table: TestFunction
      options.sql.output: Output
      options.sql.template: function
      options.sql.with-seq: no
      id: 10
      fields:
        - {name: a, type: int32}
        - {name: b, type: double}
    - name: Output
      options.sql.with-seq: no
      options.sql.template: none
      id: 20
      fields:
        - {name: a, type: double}
        - {name: b, type: int32}
    '''
    if db.getinfo(pyodbc.SQL_DBMS_NAME) == 'SQLite':
        pytest.skip("Functions not supported in SQLite3")
    with db.cursor() as c:
        c.execute(f'DROP FUNCTION IF EXISTS "TestFunction"')
        c.execute('''
CREATE FUNCTION "TestFunction" (a INTEGER, b DOUBLE PRECISION) RETURNS TABLE(a DOUBLE PRECISION, b INTEGER)
AS $$ SELECT b, a $$
LANGUAGE SQL
''')

    c = Accum('odbc://;name=select', scheme=scheme, dump='scheme', context=context, **odbcini)
    c.open()
    c.post({'a': 10, 'b': 123.45}, name='Input')
    for _ in range(10):
        c.process()
    assert [(m.type, m.msgid) for m in c.result] == [(c.Type.Data, 20), (c.Type.Control, 50)]
    assert c.unpack(c.result[0]).as_dict() == {'a': 123.45, 'b': 10}

def test_procedure(context, db, odbcini):
    scheme = '''yamls://
    - name: Output
      options.sql.template: none
      options.sql.create: yes
      id: 20
      fields:
        - {name: a, type: double}
        - {name: b, type: int32}
    - name: Input
      options.sql.table: TestProcedure
      options.sql.template: procedure
      id: 10
      fields:
        - {name: a, type: int32}
        - {name: b, type: double}
    '''
    if db.getinfo(pyodbc.SQL_DBMS_NAME) == 'SQLite':
        pytest.skip("Procedures not supported in SQLite3")
    with db.cursor() as c:
        c.execute(f'DROP TABLE IF EXISTS "Output"')

    c = Accum('odbc://;name=select;create-mode=checked', scheme=scheme, dump='scheme', context=context, **odbcini)
    c.open()

    with db.cursor() as cur:
        cur.execute(f'DROP PROCEDURE IF EXISTS "TestProcedure"')
        cur.execute('''
CREATE PROCEDURE "TestProcedure" (seq BIGINT, a INTEGER, b DOUBLE PRECISION)
LANGUAGE SQL
AS $$
INSERT INTO "Output" VALUES(seq, b, a);
INSERT INTO "Output" VALUES(seq * 2, b * 2, a * 2);
$$
''')

    c.post({'a': 10, 'b': 123.45}, name='Input', seq=100)

    c.post({'message': 20}, name='Query', type=c.Type.Control)
    for _ in range(10):
        c.process()
    assert [(m.type, m.msgid, m.seq) for m in c.result] == [(c.Type.Data, 20, 100), (c.Type.Data, 20, 200), (c.Type.Control, 50, 0)]
    assert c.unpack(c.result[0]).as_dict() == {'a': 123.45, 'b': 10}
    assert c.unpack(c.result[1]).as_dict() == {'a': 246.9, 'b': 20}

@pytest.mark.parametrize("mode", ["no", "checked", "always"])
def test_create(context, db, odbcini, mode):
    dbname = "Data"
    scheme = f'''yamls://
    - name: {dbname}
      id: 10
      fields:
        - {{name: f0, type: int32}}
    '''

    def drop(db):
        with db.cursor() as c:
            c.execute(f'DROP TABLE IF EXISTS "{dbname}"')
    def create(db):
        with db.cursor() as c:
            c.execute(f'CREATE TABLE "{dbname}" ("_tll_seq" INTEGER NOT NULL, "f0" INTEGER NOT NULL)')
    drop(db)

    c = Accum(f'odbc://;name=odbc;create-mode={mode}', scheme=scheme, dump='scheme', context=context, create='no', **odbcini)
    if mode != 'checked':
        create(db)

    if mode == 'always':
        with pytest.raises(TLLError): c.open()
        drop(db)
        c.close()

    c.open()
    c.post({'f0': 123}, name=dbname, seq=100)

    assert [tuple(r) for r in db.cursor().execute(f'SELECT * FROM "{dbname}"')] == [(100, 123)]

def test_raw(context, db, odbcini):
    dbname = "Data"
    scheme = f'''yamls://
    - name: Insert
      options.sql.table: {dbname}
      id: 10
      fields:
        - {{name: f0, type: int32}}
        - {{name: f1, type: double}}
    - name: Select
      options.sql.table: {dbname}
      options.sql.with-seq: no
      options.sql.query: 'SELECT "_tll_seq", "f0", "f1" FROM "{dbname}" WHERE "f0" < ?'
      options.sql.output: Insert
      id: 20
      fields:
        - {{name: f0, type: int32}}
    '''

    with db.cursor() as c:
        c.execute('DROP TABLE IF EXISTS "Data"')

    c = Accum(f'odbc://;name=odbc;create-mode=checked', scheme=scheme, dump='scheme', context=context, **odbcini)

    c.open()
    c.post({'f0': 10, 'f1': 12.34}, name='Insert', seq=100)
    c.post({'f0': 20, 'f1': 56.78}, name='Insert', seq=200)

    assert [tuple(r) for r in db.cursor().execute(f'SELECT * FROM "{dbname}"')] == [(100, 10, 12.34), (200, 20, 56.78)]

    c.post({'f0': 0}, name='Select')
    c.process()

    assert [(m.type, m.msgid) for m in c.result] == [(c.Type.Control, c.scheme_control.messages.EndOfData.msgid)]

    c.result = []

    c.post({'f0': 15}, name='Select')
    c.process()
    c.process()
    assert [(m.type, m.msgid) for m in c.result] == [(c.Type.Data, c.scheme.messages.Insert.msgid), (c.Type.Control, c.scheme_control.messages.EndOfData.msgid)]

    assert c.unpack(c.result[0]).as_dict() == {'f0': 10, 'f1': 12.34}

def test_none(context, db, odbcini):
    scheme = '''yamls://
    - name: Ignore
      options.sql.template: none
      id: 20
      fields:
        - {name: f0, type: int32}
    '''

    c = Accum(f'odbc://;name=odbc;create-mode=no', scheme=scheme, dump='yes', context=context, **odbcini)

    c.open()
    c.post({'f0': 30}, name='Ignore')

def test_null(context, db, odbcini):
    dbname = "Data"
    scheme = '''yamls://
    - name: Data
      options.sql.with-seq: no
      id: 10
      fields:
        - {name: pmap, type: uint8, options.pmap: yes}
        - {name: f0, type: int32}
        - {name: f1, type: double, options.optional: yes}
        - {name: f2, type: int64, options.optional: yes}
    '''

    with db.cursor() as c:
        c.execute(f'DROP TABLE IF EXISTS "{dbname}"')
        c.execute(f'CREATE TABLE "{dbname}" (f0 INTEGER, f1 DOUBLE PRECISION, f2 VARCHAR(255))')
        c.execute(f'INSERT INTO "{dbname}" VALUES (10, NULL, NULL), (NULL, 123.456, NULL), (NULL, NULL, 1234)')

    c = Accum(f'odbc://;name=odbc;create-mode=no', scheme=scheme, dump='scheme', context=context, **odbcini)

    c.open()

    c.post({'message': 10}, name='Query', type=c.Type.Control)

    for _ in range(4):
        c.process()
    assert [c.unpack(m).as_dict() for m in c.result[:-1]] == [{'f0': 10}, {'f0': 0, 'f1': 123.456}, {'f0': 0, 'f2': 1234}]
    assert [(m.type, m.msgid) for m in c.result[-1:]] == [(c.Type.Control, c.scheme_control.messages.EndOfData.msgid)]

def test_null_insert(context, db, odbcini):
    dbname = "Data"
    scheme = '''yamls://
    - name: Data
      options.sql.with-seq: no
      options.sql.template: insert
      id: 10
      fields:
        - {name: pmap, type: uint8, options.pmap: yes}
        - {name: f0, type: int32}
        - {name: f1, type: double, options.optional: yes}
        - {name: f2, type: byte16, options.type: string, options.optional: yes}
    '''

    with db.cursor() as c:
        c.execute(f'DROP TABLE IF EXISTS "{dbname}"')

    c = Accum(f'odbc://;name=odbc;create-mode=checked', scheme=scheme, dump='scheme', context=context, **odbcini)

    c.open()

    c.post({'f0': 10}, name='Data')
    c.post({'f1': 123.456}, name='Data')
    c.post({'f2': "string"}, name='Data')

    with db.cursor() as c:
        r = list(c.execute(f'SELECT f0,f1,f2 FROM "{dbname}"'))
        assert [tuple(x) for x in r] == [(10, None, None), (0, 123.456, None), (0, None, "string")]

@pytest.mark.parametrize("t,prec,value",
        [('uint16', 'day', '2024-01-02'),
        ('int64', 's', '2000-01-02T03:04:05'),
        ('int64', 'ms', '2000-01-02T03:04:05.678'),
        ('int64', 'us', '2000-01-02T03:04:05.678901'),
        ('int64', 'ns', '2000-01-02T03:04:05.678901234'),
        ('double', 's', '2000-01-02T03:04:05.678'),
        ])
def test_timestamp(context, db, odbcini, t, prec, value):
    value = TimePoint.from_str(value)
    if (name := db.getinfo(pyodbc.SQL_DBMS_NAME)) == 'SQLite':
        # SQLite3 stores only 3 digits
        value = TimePoint(value, 'ms', type=int)
    elif name == 'PostgreSQL':
        # PostgreSQL stores only 6 digits
        value = TimePoint(value, 'us', type=int)

    dbname = "Data"
    scheme = f'''yamls://
    - name: {dbname}
      options.sql.template: insert
      id: 10
      fields:
        - {{name: f0, type: {t}, options.type: time_point, options.resolution: {prec}}}
    '''

    with db.cursor() as c:
        c.execute('DROP TABLE IF EXISTS "Data"')

    c = Accum('odbc://;name=odbc;create-mode=checked', scheme=scheme, dump='scheme', context=context, **odbcini)
    c.open()
    c.post({'f0': value}, name=dbname, seq=100)

    if t != 'double':
        dt = datetime.datetime.utcfromtimestamp(value.seconds)
        assert [tuple(r) for r in db.cursor().execute(f'SELECT * FROM "Data"')] == [(100, dt)]

    c.post({'message': 10}, name='Query', type=c.Type.Control)

    c.process()
    assert [(m.type, m.msgid, m.seq) for m in c.result] == [(c.Type.Data, 10, 100)]

    r = c.unpack(c.result[-1]).f0

    if t == 'double':
        assert r.seconds == pytest.approx(value.seconds, abs=0.001)
    else:
        assert r == value

def test_default_template(context, db, odbcini):
    scheme = '''yamls://
    - name: Data
      options.sql.template: insert
      id: 10
      fields:
        - {name: f0, type: int32}
    - name: List
      id: 20
      fields:
        - {name: f0, type: '*int32'}
    '''

    with db.cursor() as c:
        c.execute('DROP TABLE IF EXISTS "Data"')

    c = Accum('odbc://;name=odbc;create-mode=checked;default-template=none', scheme=scheme, dump='yes', context=context, **odbcini)
    c.open()
    c.post({'f0': 1000}, name='Data', seq=100)

    assert [tuple(r) for r in db.cursor().execute(f'SELECT * FROM "Data"')] == [(100, 1000)]
