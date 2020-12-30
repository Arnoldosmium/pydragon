from streamer import streams, Stream


def test_cartesian():
    assert streams.cartesian_product_stream(range(10), range(5)) \
        .collect_as_set() == {(x, y) for x in range(10) for y in range(5)}

    assert streams.cartesian_product_stream(iter([]), Stream(range(10))) \
        .collect_as_set() == set()

    assert streams.cartesian_power(3, list(range(4))) \
        .collect_as_set() == {(x, y, z) for x in range(4) for y in range(4) for z in range(4)}

    assert streams.cartesian_product_stream(range(10), streams.constant_of(None, 3)) \
        .group_by(lambda x: x) \
        .map_values(len) \
        .collect_dict() == {(i, None): 3 for i in range(10)}


def test_simple_generators():
    assert streams.constant_of(1, 100).collect_as_list() == [1] * 100
    assert streams.generate(lambda: 1).limit(10).collect_as_list() == [1] * 10
    assert streams.iterate(1, lambda x: x * 2).limit(10).collect_as_list() == [1 << x for x in range(10)]
