from datetime import datetime, timedelta

from artigraph.serializer import Serializer


class DatetimeSerializer(Serializer):
    """Serializer for datetime.datetime."""

    name = "artigraph-datetime"
    types = (datetime,)

    def serialize(self, value: datetime) -> bytes:
        """Serialize a datetime.datetime to an ISO 8601 string."""
        return value.isoformat().encode()

    def deserialize(self, value: bytes) -> datetime:
        """Deserialize an ISO 8601 string to a datetime.datetime."""
        return datetime.fromisoformat(value.decode())


class TimeDeltaSerializer(Serializer):
    """Serializer for datetime.timedelta."""

    name = "artigraph-timedelta"
    types = (timedelta,)

    def serialize(self, value: timedelta) -> bytes:
        """Serialize a datetime.timedelta to a string."""
        return str(value.total_seconds()).encode()

    def deserialize(self, value: bytes) -> timedelta:
        """Deserialize a string to a datetime.timedelta."""
        return timedelta(seconds=float(value.decode()))


datetime_serializer = DatetimeSerializer().register()
"""A serializer for datetime.datetime."""

timedelta_serializer = TimeDeltaSerializer().register()
"""A serializer for datetime.timedelta."""
