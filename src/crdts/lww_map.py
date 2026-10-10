
from typing import Any


class LWWMap:
    """A Last-Writer-Wins Map for replicated data."""

    def __init__(self, replica_id: str) -> None:
        if not isinstance(replica_id, str) or not replica_id.strip():
            raise ValueError("replica_id cannot be empty")

        self.replica_id = replica_id
        self._clock = 0
        self._entries: dict[str, dict[str, Any]] = {}

    def _next_timestamp(self) -> int:
        """Generate the next local logical timestamp."""
        self._clock += 1
        return self._clock

    def put(self, key: str, value: str) -> None:
        """Insert or update a key."""
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")

        if not isinstance(value, str):
            raise ValueError("value must be a string")

        self._entries[key] = {
            "timestamp": self._next_timestamp(),
            "replica_id": self.replica_id,
            "value": value,
            "deleted": False,
        }

    def remove(self, key: str) -> None:
        """Delete a key using a versioned tombstone."""
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")

        self._entries[key] = {
            "timestamp": self._next_timestamp(),
            "replica_id": self.replica_id,
            "value": None,
            "deleted": True,
        }

    def get(self, key: str) -> str | None:
        """Get a visible value, or None if absent/deleted."""
        entry = self._entries.get(key)

        if entry is None or entry["deleted"]:
            return None

        return entry["value"]

    @property
    def items(self) -> dict[str, str]:
        """Return visible key-value pairs."""
        return {
            key: entry["value"]
            for key, entry in self._entries.items()
            if not entry["deleted"]
        }

    @property
    def state(self) -> dict[str, Any]:
        """Return a serialisable snapshot."""
        return {
            "entries": {
                key: dict(entry)
                for key, entry in self._entries.items()
            }
        }

    def merge_state(self, incoming_state: dict[str, Any]) -> None:
        """Merge entries using timestamp and replica ID ordering."""
        if not isinstance(incoming_state, dict):
            raise ValueError("incoming_state must be a dictionary")

        incoming_entries = incoming_state.get("entries")

        if not isinstance(incoming_entries, dict):
            raise ValueError("'entries' must be a dictionary")

        parsed: dict[str, dict[str, Any]] = {}

        # Validate the complete snapshot before changing local state.
        for key, entry in incoming_entries.items():
            if not isinstance(key, str) or not key:
                raise ValueError("invalid key")

            if not isinstance(entry, dict):
                raise ValueError("each entry must be a dictionary")

            timestamp = entry.get("timestamp")
            replica_id = entry.get("replica_id")
            value = entry.get("value")
            deleted = entry.get("deleted")

            if (
                not isinstance(timestamp, int)
                or isinstance(timestamp, bool)
                or timestamp < 1
            ):
                raise ValueError("timestamp must be a positive integer")

            if not isinstance(replica_id, str) or not replica_id.strip():
                raise ValueError("invalid replica ID")

            if not isinstance(deleted, bool):
                raise ValueError("deleted must be a boolean")

            if deleted:
                if value is not None:
                    raise ValueError("deleted entries must have no value")
            elif not isinstance(value, str):
                raise ValueError("visible entries must have string values")

            parsed[key] = {
                "timestamp": timestamp,
                "replica_id": replica_id,
                "value": value,
                "deleted": deleted,
            }

        # A later local write must follow all observed timestamps.
        if parsed:
            self._clock = max(
                self._clock,
                max(entry["timestamp"] for entry in parsed.values()),
            )

        for key, incoming in parsed.items():
            current = self._entries.get(key)

            if current is None:
                self._entries[key] = dict(incoming)
                continue

            incoming_version = (
                incoming["timestamp"],
                incoming["replica_id"],
            )
            current_version = (
                current["timestamp"],
                current["replica_id"],
            )

            if incoming_version > current_version:
                self._entries[key] = dict(incoming)

    def merge(self, other: "LWWMap") -> None:
        """Merge another LWW-Map's snapshot."""
        self.merge_state(other.state)
