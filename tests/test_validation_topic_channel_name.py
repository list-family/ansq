import pytest

from ansq.utils import validate_topic_channel_name


@pytest.mark.parametrize(
    "name",
    (
        "test_topic_name",
        "test_channel_name1",
        "123456",
        "test_topic_name#ephemeral",
        "__-.-__",
        "TEST_TOPIC_12123-name_",
        "123123#ephemeral",
        "TEST_TOPIC_CHANNEL123123-name_#ephemeral",
    ),
)
def test_validation_topic_channel_name(name: str):
    assert validate_topic_channel_name(name) is None


@pytest.mark.parametrize(
    "name",
    (
        "",
        ".",
        "1",
        "#ephemeral",
        "VERY_BIG_NAME_VERY_BIG_NAME_VERY_BIG_NAME_VERY_BIG_NAME_VERY_BIG_",
        "maybe try this",
        "or_this_one?",
        "-how-about-this-\name",
    ),
)
def test_validation_topic_channel_name_with_exception(name: str):
    with pytest.raises(AssertionError):
        validate_topic_channel_name(name)
