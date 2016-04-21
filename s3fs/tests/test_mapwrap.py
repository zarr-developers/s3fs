from s3fs.tests.test_s3fs import s3, test_bucket_name
from s3fs.mapwrap import MapWrap

def test_simple(s3):
    root = test_bucket_name+'/mapping'
    mw = MapWrap(s3, root)
    assert not mw

    assert list(mw) == list(mw.keys()) == []
    assert list(mw.values()) == []
    assert list(mw.items()) == []

    mw['x'] = b'123'
    assert list(mw) == list(mw.keys()) == ['x']
    assert list(mw.values()) == [b'123']
    assert list(mw.items()) == [('x', b'123')]
    assert mw['x'] == b'123'
    assert bool(mw)

    assert s3.walk(root) == [test_bucket_name+'/mapping/x']
    mw['x'] = b'000'
    assert mw['x'] == b'000'

    mw['y'] = b'456'
    assert mw['y'] == b'456'
    assert set(mw) == {'x', 'y'}

    mw.clear()
    assert list(mw) == []
