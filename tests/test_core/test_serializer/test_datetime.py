from datetime import datetime, timedelta, timezone

from artigraph.core.serializer.datetime import datetime_serializer, timedelta_serializer


def test_datetime_serializer():
    """Test that the datetime serializer works."""
    now = datetime.now(timezone.utc)
    assert datetime_serializer.serialize(now) == now.isoformat().encode()
    assert datetime_serializer.deserialize(now.isoformat().encode()) == now


def test_timedelta_serializer():
    """Test that the timedelta serializer works."""
    delta = timedelta(days=1, hours=2, minutes=3, seconds=4, milliseconds=5)
    assert timedelta_serializer.serialize(delta) == b"93784.005"
    assert timedelta_serializer.deserialize(b"93784.005") == delta
