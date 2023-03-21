#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import pytest

from decimal import Decimal
import pyodbc

from tll.test_util import Accum

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
        ('string', 'string'),
        ('byte32, options.type: string', 'string'),
        ])
def test_field(context, odbcini, t, value):
    if odbcini.get('driver', '') == 'SQLite3' and t == 'decimal128':
        pytest.skip("Decimal128 not supported on SQLite3")

    dbname = "Data"
    scheme = f'''yamls://
    - name: {dbname}
      id: 10
      fields:
        - {{name: f0, type: {t}}}
    '''

    db = pyodbc.connect(**odbcini) #{k.split('.')[-1]: v for k,v in odbcini.items()}
    with db.cursor() as c:
        c.execute('DROP TABLE IF EXISTS "Data"')

    c = Accum('odbc://;name=odbc', scheme=scheme, dump='scheme', context=context, **odbcini)
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
def test_query(context, odbcini, query, result):
    scheme = '''yamls://
    - name: Query
      id: 10
      fields:
        - {name: f0, type: int64}
        - {name: f1, type: double}
        - {name: f2, type: string}
    '''

    db = pyodbc.connect(**odbcini) #{k.split('.')[-1]: v for k,v in odbcini.items()}
    with db.cursor() as c:
        c.execute(f'DROP TABLE IF EXISTS "Query"')

    i = context.Channel('odbc://;name=insert', scheme=scheme, dir='w', **odbcini)
    i.open()
    for x in range(10):
        i.post({'f0': 1000 * x, 'f1': 100.5 * x, 'f2': str(x)}, name=f'Query', seq=x)

    s = Accum('odbc://;name=select', scheme=scheme, dump='scheme', context=context, **odbcini)
    s.open()
    s.post({'message': 10, 'expression': query}, name='Query', type=s.Type.Control)

    for _ in range(100):
        s.process()

    assert [(m.type, m.msgid, m.seq) for m in s.result] == [(s.Type.Data, 10, x) for x in result]
    for m, r in zip(s.result, result):
        assert s.unpack(m).as_dict() == {'f0': 1000 * r, 'f1': 100.5 * r, 'f2': str(r)}

def test_function(context, odbcini):
    scheme = '''yamls://
    - name: Input
      options.sql.function: TestFunction
      options.sql.function-output: Output
      id: 10
      fields:
        - {name: a, type: int32}
        - {name: b, type: double}
    - name: Output
      id: 20
      fields:
        - {name: a, type: double}
        - {name: b, type: int32}
    '''
    if odbcini.get('driver', '') == 'SQLite3':
        pytest.skip("Functions not supported in SQLite3")
    db = pyodbc.connect(**odbcini) #{k.split('.')[-1]: v for k,v in odbcini.items()}
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
    assert [(m.type, m.msgid) for m in c.result] == [(c.Type.Data, 20)]
    assert c.unpack(c.result[0]).as_dict() == {'a': 123.45, 'b': 10}
