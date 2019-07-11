import os
from multiprocessing import Queue

import pytest

from reader import is_log_file, Reader


@pytest.fixture()
def make_temp_log(tmpdir):
    queries = '{"timestamp": 1525208713, "event_type": "create", "ids": [4, 5, 7, 1, 10, 8, 3, 2],"query_string": ' \
              '"id=12&lzbnn=lzbnn&id=11&yfxxu=yfxxu&ffgtx=ffgtx&id=13&id=17&id=14&id=20&tmghs=tmghs"}\n' + \
              '{"timestamp": 1525139146, "event_type": "delete", "ids": [2, 10, 7, 1, 9, 8, 5], "query_string": ' \
              '"id=9&culyr=culyr&id=2&id=10&thvgj=thvgj&id=7&lbyjz=lbyjz&id=1&tdiob=tdiob&wryji=wryji"}\n' + \
              '{"timestamp": 1525164213, "event_type": "delete", "ids": [7, 1, 2, 6, 3, 5, 4], "query_string": ' \
              '"id=6&rjtdw=rjtdw&id=7&id=5&id=3&id=4&id=1&id=2"}'
    fn = tmpdir.join('1.log')
    fn.write(queries)
    return os.path.join(fn.dirname, fn.basename)


@pytest.mark.parametrize('filename, expected', (
        ('10.log', True),
        ('10.txt.log', False),
        ('abc.log', False),
        ('10.txt', False),
        ('10.log.txt', False),
))
def test_is_log_file(filename, expected):
    assert is_log_file(filename) == expected


def test_reader_day_time():
    assert Reader(None, None).get_day_time(1514765912) == 1514764800


@pytest.mark.parametrize('query_string, expected', (
        ('id=1&id=2&id=2&ida=0&asd=asd', {1, 2}),
        ('id=1&id=2&id=3&ida=0&asd=asd', {1, 2, 3}),
        ('btksf=btksf&id=3&id=7&id=9&dabbq=dabbq&wtcbd=wtcbd', {3, 7, 9}),
))
def test_reader_get_id_from_query(query_string, expected):
    assert Reader(None, None).get_id_from_query(query_string) == expected


def test_reader(make_temp_log):
    q = Queue()
    r = Reader(make_temp_log, q)
    r.start()
    expected = {'valid': {1525132800: {'create': 0, 'update': 0, 'delete': 1}},
                'non_valid': {1525132800: {'create': 1, 'update': 0, 'delete': 1}}}
    assert q.get() == expected
