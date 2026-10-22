"""In-memory network simulator for the PS-39 replicated-store prototype."""

import json
import random
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from replicas.replica import Replica


@dataclass(frozen=True)
class Message:
    """A serialised snapshot or delta sent from one replica to another."""

    sender: str
    receiver: str
    payload: str

    @property
    def size_bytes(self) -> int:
        """UTF-8 bytes in the serialised payload (not wire-protocol overhead)."""
        return len(self.payload.encode("utf-8"))


class NetworkSimulator:
    """Simulate messages among independent in-memory replicas."""

    def __init__(
        self,
        replica_ids: list[str],
        sync_mode: str = "full",
    ) -> None:
        if len(replica_ids) < 2:
            raise ValueError("At least two replicas are required")
        if any(
            not isinstance(rid, str) or not rid.strip()
            for rid in replica_ids
        ):
            raise ValueError("Replica IDs must be non-empty strings")
        if len(set(replica_ids)) != len(replica_ids):
            raise ValueError("Replica IDs must be unique")
        if sync_mode not in {"full", "delta"}:
            raise ValueError("sync_mode must be 'full' or 'delta'")

        self.sync_mode = sync_mode
        self.replica_ids = list(replica_ids)
        self.replicas = {rid: Replica(rid) for rid in self.replica_ids}

        # Each pair tracks state successfully delivered from sender to receiver.
        self.peer_known_states: dict[tuple[str, str], Replica] = {}
        if self.sync_mode == "delta":
            for sender in self.replica_ids:
                for receiver in self.replica_ids:
                    if sender != receiver:
                        known_id = f"__known__{sender}__{receiver}"
                        self.peer_known_states[(sender, receiver)] = Replica(
                            known_id
                        )

        # Replicas in different groups cannot communicate.
        self._groups = {rid: 0 for rid in self.replica_ids}
        self.queue: list[Message] = []

        self.messages_sent = 0
        self.messages_delivered = 0
        # Count payload bytes at send/queue time, including messages later lost.
        self.bytes_transmitted = 0
        # Counts payload bytes delivered to receivers, including duplicates.
        self.bytes_delivered = 0
        self.merge_latencies_ms: list[float] = []

    def set_partitions(self, groups: list[list[str]]) -> None:
        """Place replicas in disconnected network groups."""
        flattened = [rid for group in groups for rid in group]
        if (
            len(flattened) != len(self.replica_ids)
            or set(flattened) != set(self.replica_ids)
            or any(not group for group in groups)
        ):
            raise ValueError(
                "Partition groups must contain every replica exactly once"
            )

        self._groups = {
            rid: group_index
            for group_index, group in enumerate(groups)
            for rid in group
        }

    def heal_partitions(self) -> None:
        """Reconnect all replicas."""
        self._groups = {rid: 0 for rid in self.replica_ids}

    def _build_delta_snapshot(
        self,
        sender: str,
        receiver: str,
    ) -> dict[str, Any]:
        """Build a delta against state previously delivered on this pair."""
        local = self.replicas[sender].snapshot()
        known = self.peer_known_states[(sender, receiver)].snapshot()

        # Counter: transmit only components whose value increased.
        counter_delta = {
            rid: count
            for rid, count in local["counter"].items()
            if count > known["counter"].get(rid, 0)
        }

        # OR-Set: transmit addition tags and removal tags not yet delivered.
        local_adds = local["task_set"]["adds"]
        known_adds = known["task_set"]["adds"]
        adds_delta: dict[str, list[str]] = {}

        for value, tags in local_adds.items():
            unseen = set(tags) - set(known_adds.get(value, []))
            if unseen:
                adds_delta[value] = sorted(unseen)

        # Ordering metadata travels with every previously undelivered add tag.
        local_order = local["task_set"].get("add_order", {})
        add_order_delta = {
            tag: local_order[tag]
            for tags in adds_delta.values()
            for tag in tags
        }

        removed_delta = sorted(
            set(local["task_set"]["removed_tags"])
            - set(known["task_set"]["removed_tags"])
        )

        # LWW-Map: transmit keys whose version is newer than the delivered one.
        local_entries = local["settings"]["entries"]
        known_entries = known["settings"]["entries"]
        entries_delta: dict[str, Any] = {}

        for key, entry in local_entries.items():
            previous = known_entries.get(key)
            if previous is None:
                entries_delta[key] = entry
                continue

            incoming_version = (entry["timestamp"], entry["replica_id"])
            previous_version = (
                previous["timestamp"],
                previous["replica_id"],
            )
            if incoming_version > previous_version:
                entries_delta[key] = entry

        return {
            "replica_id": sender,
            "counter": counter_delta,
            "task_set": {
                "adds": adds_delta,
                "removed_tags": removed_delta,
                "add_order": add_order_delta,
            },
            "settings": {"entries": entries_delta},
        }

    def send_snapshot(self, sender: str, receiver: str) -> bool:
        """Queue a full snapshot or delta if the link is currently available."""
        if sender not in self.replicas or receiver not in self.replicas:
            raise ValueError("Unknown replica ID")
        if sender == receiver:
            raise ValueError("A replica cannot send a message to itself")
        if self._groups[sender] != self._groups[receiver]:
            return False

        if self.sync_mode == "delta":
            state = self._build_delta_snapshot(sender, receiver)
            is_empty = (
                not state["counter"]
                and not state["task_set"]["adds"]
                and not state["task_set"]["removed_tags"]
                and not state["task_set"].get("add_order", {})
                and not state["settings"]["entries"]
            )
            if is_empty:
                return False
        else:
            # Full-state mode deliberately sends even an empty state.
            state = self.replicas[sender].snapshot()

        payload = json.dumps(state, sort_keys=True, separators=(",", ":"))
        message = Message(sender, receiver, payload)
        self.queue.append(message)
        self.messages_sent += 1
        self.bytes_transmitted += message.size_bytes
        return True

    def broadcast_round(self) -> int:
        """Queue one message from each replica to every reachable peer."""
        queued_before = len(self.queue)
        for sender in self.replica_ids:
            for receiver in self.replica_ids:
                if sender != receiver:
                    self.send_snapshot(sender, receiver)
        return len(self.queue) - queued_before

    def duplicate_message(self, index: int) -> None:
        """Inject a duplicate copy of a queued packet into the network."""
        if not 0 <= index < len(self.queue):
            raise IndexError("Message index is out of range")
        # A network duplicate is not a new sender transmission; it can be
        # delivered more than once, and each arrival is counted in bytes_delivered.
        self.queue.append(self.queue[index])

    def deliver(self, index: int = 0) -> bool:
        """Deliver one queued message if its link is currently available."""
        if not 0 <= index < len(self.queue):
            raise IndexError("Message index is out of range")

        message = self.queue[index]
        if self._groups[message.sender] != self._groups[message.receiver]:
            return False

        self.queue.pop(index)
        incoming_state = json.loads(message.payload)

        start = perf_counter()
        self.replicas[message.receiver].merge_state(incoming_state)
        elapsed_ms = (perf_counter() - start) * 1000
        # Keep the merge-latency definition comparable across modes: time only
        # the target replica's state merge, not sender/receiver bookkeeping.
        self.merge_latencies_ms.append(elapsed_ms)

        if self.sync_mode == "delta":
            self.peer_known_states[(message.sender, message.receiver)].merge_state(
                incoming_state
            )

        self.messages_delivered += 1
        self.bytes_delivered += message.size_bytes
        return True

    def deliver_all(
        self,
        random_order: bool = False,
        seed: int | None = None,
    ) -> int:
        """Deliver currently reachable messages, optionally in random order."""
        rng = random.Random(seed)
        delivered = 0
        while self.queue:
            eligible = [
                index
                for index, message in enumerate(self.queue)
                if self._groups[message.sender] == self._groups[message.receiver]
            ]
            if not eligible:
                break
            index = rng.choice(eligible) if random_order else eligible[0]
            if self.deliver(index):
                delivered += 1
        return delivered

    def all_replicas_converged(self) -> bool:
        """Return whether all replicas have identical logical CRDT state."""
        states = []
        for replica in self.replicas.values():
            state = replica.snapshot()
            state.pop("replica_id")
            states.append(state)
        return all(state == states[0] for state in states[1:])

    @property
    def metrics(self) -> dict[str, float | int]:
        """Return transmission, delivery, and merge timing metrics."""
        count = len(self.merge_latencies_ms)
        return {
            "messages_sent": self.messages_sent,
            "messages_delivered": self.messages_delivered,
            "bytes_transmitted": self.bytes_transmitted,
            "bytes_delivered": self.bytes_delivered,
            "average_merge_latency_ms": (
                sum(self.merge_latencies_ms) / count if count else 0.0
            ),
        }
