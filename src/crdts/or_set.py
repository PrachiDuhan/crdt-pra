from threading import Lock
from time import time_ns
from uuid import uuid4


# The prototype simulates multiple replicas inside one Python process. A shared
# high-water mark gives sequential local add() calls a strict order even on OSes
# whose wall clock has coarser resolution than one nanosecond. In a real
# multi-process deployment, independent offline events still need a deterministic
# tie-breaker because their physical order cannot always be known.
_order_clock_lock = Lock()
_last_order_timestamp_ns = 0


def _next_order_timestamp_ns(observed_clock: int = 0) -> int:
    """Return a strictly increasing timestamp for this process's add calls."""
    global _last_order_timestamp_ns
    with _order_clock_lock:
        timestamp = max(
            time_ns(),
            _last_order_timestamp_ns + 1,
            observed_clock + 1,
        )
        _last_order_timestamp_ns = timestamp
        return timestamp


class ORSet:
    """An Observed-Remove Set with replicated, deterministic addition order."""

    def __init__(self, replica_id: str) -> None:
        if not isinstance(replica_id, str) or not replica_id.strip():
            raise ValueError("replica_id cannot be empty")

        self.replica_id = replica_id
        self._adds: dict[str, set[str]] = {}
        self._removed_tags: set[str] = set()
        # Each tag has immutable logical ordering metadata: (counter, origin ID).
        self._add_order: dict[str, tuple[int, str]] = {}
        self._clock = 0

    @staticmethod
    def _tag_origin(tag: str) -> str:
        """Extract a stable origin for legacy tags lacking ordering metadata."""
        origin, separator, _ = tag.rpartition(":")
        return origin if separator and origin else tag

    def add(self, value: str) -> None:
        """Add a value with a unique tag and a Lamport-style logical order."""
        if not isinstance(value, str) or not value:
            raise ValueError("value must be a non-empty string")

        # Use wall-clock nanoseconds to order independent offline additions,
        # while advancing beyond observed timestamps to preserve causal order.
        self._clock = _next_order_timestamp_ns(self._clock)
        tag = f"{self.replica_id}:{uuid4().hex}"
        self._adds.setdefault(value, set()).add(tag)
        self._add_order[tag] = (self._clock, self.replica_id)

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
    def ordered_values(self) -> list[str]:
        """Return visible values in deterministic replicated addition order.

        When the same value has multiple active addition tags, its earliest
        active tag determines its position. Concurrent additions are ordered by
        logical counter, origin replica ID, then unique tag.
        """
        positions: list[tuple[tuple[int, str, str], str]] = []
        for value, tags in self._adds.items():
            active_tags = tags - self._removed_tags
            if not active_tags:
                continue

            def key_for(tag: str) -> tuple[int, str, str]:
                counter, origin = self._add_order.get(
                    tag, (0, self._tag_origin(tag))
                )
                return counter, origin, tag

            first_key = min(key_for(tag) for tag in active_tags)
            positions.append((first_key, value))

        positions.sort(key=lambda item: (item[0], item[1]))
        return [value for _, value in positions]

    @property
    def state(self) -> dict:
        """Return a serialisable snapshot, including addition ordering metadata."""
        return {
            "adds": {
                value: sorted(tags)
                for value, tags in sorted(self._adds.items())
            },
            "removed_tags": sorted(self._removed_tags),
            "add_order": {
                tag: {"timestamp_ns": counter, "replica_id": origin}
                for tag, (counter, origin) in sorted(self._add_order.items())
            },
        }

    def merge_state(self, incoming_state: dict) -> None:
        """Merge a full snapshot or delta, preserving deterministic task order.

        Older saved states without ``add_order`` remain readable. Their order
        cannot be reconstructed, so legacy tags receive a stable fallback order
        until authoritative metadata arrives from an upgraded replica.
        """
        if not isinstance(incoming_state, dict):
            raise ValueError("incoming_state must be a dictionary")

        incoming_adds = incoming_state.get("adds", {})
        incoming_removed = incoming_state.get("removed_tags", [])
        incoming_order = incoming_state.get("add_order", {})

        if not isinstance(incoming_adds, dict):
            raise ValueError("'adds' must be a dictionary")
        if not isinstance(incoming_removed, (list, tuple, set)):
            raise ValueError("'removed_tags' must be a collection")
        if not isinstance(incoming_order, dict):
            raise ValueError("'add_order' must be a dictionary")

        # Validate the entire payload before mutating this OR-Set.
        parsed_adds: dict[str, set[str]] = {}
        incoming_tags: set[str] = set()

        for value, tags in incoming_adds.items():
            if not isinstance(value, str) or not value:
                raise ValueError("invalid value in incoming state")
            if not isinstance(tags, (list, tuple, set)):
                raise ValueError("each value's tags must be a collection")
            if any(not isinstance(tag, str) or not tag for tag in tags):
                raise ValueError("invalid addition tag")
            parsed_tags = set(tags)
            parsed_adds[value] = parsed_tags
            incoming_tags.update(parsed_tags)

        if any(not isinstance(tag, str) or not tag for tag in incoming_removed):
            raise ValueError("invalid removed tag")

        parsed_order: dict[str, tuple[int, str]] = {}
        for tag, metadata in incoming_order.items():
            if not isinstance(tag, str) or not tag:
                raise ValueError("invalid tag in ordering metadata")
            if tag not in incoming_tags:
                raise ValueError("ordering metadata refers to an unknown addition tag")
            if not isinstance(metadata, dict):
                raise ValueError("addition ordering metadata must be an object")

            # Read timestamp_ns from new snapshots, and counter from snapshots
            # written by the first ordering patch.
            counter = metadata.get("timestamp_ns", metadata.get("counter"))
            origin = metadata.get("replica_id")
            if (
                not isinstance(counter, int)
                or isinstance(counter, bool)
                or counter < 0
            ):
                raise ValueError("invalid addition ordering counter")
            if not isinstance(origin, str) or not origin.strip():
                raise ValueError("invalid addition ordering replica ID")
            parsed_order[tag] = (counter, origin)

        # Prepare merged order information. If a legacy fallback (counter 0)
        # later meets real metadata for the same origin, upgrade the fallback.
        merged_order = dict(self._add_order)
        for tags in parsed_adds.values():
            for tag in tags:
                current = merged_order.get(tag)
                incoming = parsed_order.get(tag)
                if incoming is None:
                    incoming = current or (0, self._tag_origin(tag))

                if current is None:
                    merged_order[tag] = incoming
                elif current == incoming:
                    pass
                # Upgrade an old small Lamport counter to a wall-clock
                # timestamp when both metadata records refer to the same tag.
                elif current[0] < 1_000_000_000_000 <= incoming[0] and current[1] == incoming[1]:
                    merged_order[tag] = incoming
                elif incoming[0] < 1_000_000_000_000 <= current[0] and current[1] == incoming[1]:
                    pass
                elif current[0] == 0 and incoming[0] > 0 and current[1] == incoming[1]:
                    merged_order[tag] = incoming
                elif incoming[0] == 0 and current[0] > 0 and current[1] == incoming[1]:
                    pass
                else:
                    raise ValueError("conflicting ordering metadata for addition tag")

        for value, tags in parsed_adds.items():
            self._adds.setdefault(value, set()).update(tags)
        self._removed_tags.update(incoming_removed)
        self._add_order = merged_order
        if self._add_order:
            self._clock = max(
                self._clock,
                max(counter for counter, _ in self._add_order.values()),
            )

    def merge(self, other: "ORSet") -> None:
        """Merge another OR-Set's state."""
        self.merge_state(other.state)
