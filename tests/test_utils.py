import pytest

from ansq.utils import truncate


@pytest.mark.parametrize(
    "text, limit, expected",
    (
        pytest.param(
            b"0123456789", 11, b"0123456789", id="no trunc, greater than limit"
        ),
        pytest.param(b"0123456789", 10, b"0123456789", id="no trunc, equal limit"),
        pytest.param(b"0123456789", 9, b"012345678...", id="trunc"),
        pytest.param(b"0123456789", 1, b"0...", id="minimal trunc"),
        pytest.param(b"", 10, b"", id="empty string"),
    ),
)
def test_truncate(text, limit, expected):
    assert truncate(text, limit) == expected


@pytest.mark.parametrize(
    "text, limit",
    (
        pytest.param(b"0123456789", 0, id="zero"),
        pytest.param(b"0123456789", -1, id="negative"),
    ),
)
def test_truncate_raises_value_error(text, limit):
    with pytest.raises(ValueError, match=r"^limit must be greater than 0$"):
        assert truncate(text, limit)
