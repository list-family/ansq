import pytest

from ansq.utils import truncate_text


@pytest.mark.parametrize(
    "text, limit, expected",
    (
        pytest.param("0123456789", 11, "0123456789", id="no trunc, greater than limit"),
        pytest.param("0123456789", 10, "0123456789", id="no trunc, equal limit"),
        pytest.param("0123456789", 9, "012345678...", id="trunc"),
        pytest.param("0123456789", 1, "0...", id="minimal trunc"),
        pytest.param("", 10, "", id="empty string"),
    ),
)
def test_truncate_text(text, limit, expected):
    assert truncate_text(text, limit) == expected


@pytest.mark.parametrize(
    "text, limit",
    (
        pytest.param("0123456789", 0, id="zero"),
        pytest.param("0123456789", -1, id="negative"),
    ),
)
def test_truncate_text_raises_value_error(text, limit):
    with pytest.raises(ValueError, match=r"^limit must be greater than 0$"):
        assert truncate_text(text, limit)
