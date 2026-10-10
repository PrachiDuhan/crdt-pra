
from uuid import uuid4


class ORSet:
    """An Observed-Remove Set for replicated data."""

    def __init__(self, replica_id: str) -> None:
        if not isinstance(replica_id, str) or not replica_id.strip():
            raise ValueError("replica_id cannot be empty")

        self.replica_id = replica_id
        self._adds: dict[str, set[str]] = {}
        self._removed_tags: set[str] = set()

    def add(self, value: str) -> None:
        """Add a value with a unique tag."""
        if not isinstance(value, str) or not value:
            raise ValueError("value must be a non-empty string")

        tag = f"{self.replica_id}:{uuid4().hex}"
        self._adds.setdefault(value, set()).add(tag)

    def remove(self, value: str) -> None:
        """Remove the addition tags currently observed for a value."""
        if not isinstance(value, str) or not value:
            raise ValueError("value must be a non-empty string")

        observed_tags = self._adds.get(value, set())
        self._removed_tags.update(observed_tags)

    @property
    def values(self) -> set[str]:
        """Return the currently visible set of values."""
        return {
            value
            for value, tags in self._adds.items()
            if tags - self._removed_tags
        }

    @property
    def state(self) -> dict:
        """Return a serialisable snapshot of this replica's state."""
        return {
            "adds": {
                value: sorted(tags)
                for value, tags in self._adds.items()
            },
            "removed_tags": sorted(self._removed_tags),
        }

    def merge_state(self, incoming_state: dict) -> None:
        """Merge a received snapshot using set union."""
        if not isinstance(incoming_state, dict):
            raise ValueError("incoming_state must be a dictionary")

        incoming_adds = incoming_state.get("adds", {})
        incoming_removed = incoming_state.get("removed_tags", [])

        if not isinstance(incoming_adds, dict):
            raise ValueError("'adds' must be a dictionary")

        if not isinstance(incoming_removed, (list, tuple, set)):
            raise ValueError("'removed_tags' must be a collection")

        # Validate the whole snapshot before changing local state.
        parsed_adds: dict[str, set[str]] = {}

        for value, tags in incoming_adds.items():
            if not isinstance(value, str) or not value:
                raise ValueError("invalid value in incoming state")

            if not isinstance(tags, (list, tuple, set)):
                raise ValueError("each value's tags must be a collection")

            if any(not isinstance(tag, str) or not tag for tag in tags):
                raise ValueError("invalid addition tag")

            parsed_adds[value] = set(tags)

        if any(
            not isinstance(tag, str) or not tag
            for tag in incoming_removed
        ):
            raise ValueError("invalid removed tag")

        # Merge by union; repeat messages do not duplicate additions.
        for value, tags in parsed_adds.items():
            self._adds.setdefault(value, set()).update(tags)

        self._removed_tags.update(incoming_removed)

    def merge(self, other: "ORSet") -> None:
        """Merge another OR-Set's state."""
        self.merge_state(other.state)
