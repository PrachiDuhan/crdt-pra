class GCounter:
    """A grow-only counter for a replicated system."""

    def __init__(self, replica_id: str) -> None:
        if not isinstance(replica_id, str) or not replica_id.strip():
            raise ValueError("replica_id cannot be empty")

        self.replica_id = replica_id
        self._counts: dict[str, int] = {}

    def increment(self, amount: int = 1) -> None:
        """Increase this replica's own counter component."""
        if (
            not isinstance(amount, int)
            or isinstance(amount, bool)
            or amount < 1
        ):
            raise ValueError("amount must be a positive integer")

        current = self._counts.get(self.replica_id, 0)
        self._counts[self.replica_id] = current + amount

    @property
    def value(self) -> int:
        """Return the total count across all replicas."""
        return sum(self._counts.values())

    @property
    def state(self) -> dict[str, int]:
        """Return a copy of the internal state."""
        return dict(self._counts)

    def merge_state(self, incoming_state: dict[str, int]) -> None:
        """Validate and merge a received snapshot using component maxima."""
        if not isinstance(incoming_state, dict):
            raise ValueError("incoming_state must be a dictionary")

        # Validate the complete state before mutating local state.
        parsed: dict[str, int] = {}
        for replica_id, count in incoming_state.items():
            if not isinstance(replica_id, str) or not replica_id.strip():
                raise ValueError("invalid replica ID")
            if (
                not isinstance(count, int)
                or isinstance(count, bool)
                or count < 0
            ):
                raise ValueError("counter values must be non-negative integers")
            parsed[replica_id] = count

        for replica_id, count in parsed.items():
            current = self._counts.get(replica_id, 0)
            self._counts[replica_id] = max(current, count)

    def merge(self, other: "GCounter") -> None:
        """Merge another counter's state."""
        self.merge_state(other.state)
