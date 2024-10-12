#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import pytest

import time

from tll.test_util import Accum

SCHEME = '''yamls://
- name: Data
  id: 10
  fields:
    - {name: f0, type: int8}
'''

def test_timestamp(context):
    s = Accum('direct://', name='server', context=context, scheme=SCHEME, dump='yes')
    c = Accum('db-heartbeat+direct://', name='client', context=context, master=s, timeout='50ms', message='Data')

    s.open()
    c.open()

    assert c.state == c.State.Active

    time.sleep(0.05)
    c.children[-1].process()
    c.children[-1].process()

    assert [m.msgid for m in s.result] == [10]
    assert s.unpack(s.result[0]).as_dict() == {'f0': 0}

    s.result = []
    for _ in range(4):
        time.sleep(0.025)
        s.post({'f0': 10}, name='Data')
        c.children[-1].process()
    assert [m.msgid for m in s.result] == []
