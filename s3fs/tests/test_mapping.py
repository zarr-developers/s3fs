from s3fs.tests.test_s3fs import s3, test_bucket_name
from s3fs.mapping import S3Map

root = test_bucket_name+'/mapping'


def test_simple(s3):
    mw = S3Map(s3, root)
    assert not mw

    assert list(mw) == list(mw.keys()) == []
    assert list(mw.values()) == []
    assert list(mw.items()) == []


def test_with_data(s3):
    mw = S3Map(s3, root)
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


def test_complex_keys(s3):
    mw = S3Map(s3, root)
    mw[1] = b'hello'
    assert mw[1] == b'hello'
    del mw[1]

    mw[1, 2] = b'world'
    assert mw[1, 2] == b'world'
    del mw[1, 2]

    mw['x', 1, 2] = b'hello world'
    assert mw['x', 1, 2] == b'hello world'
    print(list(mw))

    assert ('x', 1, 2) in mw


def test_pickle(s3):
    d = S3Map(s3, root)
    d['x'] = b'1'

    import pickle
    d2 = pickle.loads(pickle.dumps(d))

    assert d2['x'] == b'1'
