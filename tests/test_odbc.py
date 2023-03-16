#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import pytest

import sqlite3

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
        ('string', 'string'),
        ('byte32, options.type: string', 'string'),
        ])
def test_field(context, odbcini, t, value):
    dbname = f"Data_{t.split(',')[0]}"
    scheme = f'''yamls://
    - name: {dbname}
      id: 10
      fields:
        - {{name: f0, type: {t}}}
    '''
    i = context.Channel('odbc://testdb;name=insert', scheme=scheme, dir='w')
    i.open()
    i.post({'f0': value}, name=dbname, seq=100)
    #c.post({'f0': value}, name='Data', seq=2)

    if t not in ('int8', 'uint32'):
        db = sqlite3.connect(odbcini['database'])
        assert list(db.cursor().execute(f'SELECT * FROM `{dbname}`')) == [(100, value)]

    s = Accum('odbc://testdb;name=select', scheme=scheme, dump='scheme', context=context)
    s.open()
    s.post({'message': 10}, name='Query', type=i.Type.Control)

    s.process()
    assert [(m.type, m.msgid, m.seq) for m in s.result] == [(s.Type.Data, 10, 100)]

    assert s.unpack(s.result[-1]).as_dict() == {'f0': value}

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

    i = context.Channel('odbc://testdb;name=insert', scheme=scheme, dir='w')
    i.open()

    db = sqlite3.connect(odbcini['db'])
    if list(db.cursor().execute(f'SELECT * FROM `Query`')) == []:
        for x in range(10):
            i.post({'f0': 1000 * x, 'f1': 100.5 * x, 'f2': str(x)}, name=f'Query', seq=x)

    s = Accum('odbc://testdb;name=select', scheme=scheme, dump='scheme', context=context)
    s.open()
    s.post({'message': 10, 'expression': query}, name='Query', type=s.Type.Control)

    for _ in range(100):
        s.process()

    assert [(m.type, m.msgid, m.seq) for m in s.result] == [(s.Type.Data, 10, x) for x in result]
    for m, r in zip(s.result, result):
        assert s.unpack(m).as_dict() == {'f0': 1000 * r, 'f1': 100.5 * r, 'f2': str(r)}
