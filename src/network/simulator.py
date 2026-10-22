"""In-memory network simulator for the PS-39 replicated-store prototype."""

import json
import random
from dataclasses import dataclass
from time import perf_counter
from typing import Any
<<<<<<< HEAD
=======
from uuid import uuid4
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256

from replicas.replica import Replica


@dataclass(frozen=True)
class Message:
<<<<<<< HEAD
    """A serialised snapshot or delta sent from one replica to another."""
=======
    """A serialised snapshot, delta, or acknowledgement sent between replicas."""
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256

    sender: str
    receiver: str
    payload: str
<<<<<<< HEAD
=======
    msg_id: str = ""
    msg_type: str = "data"  # "data" or "ack"
    ack_for: str = ""       # ID of the data message being acknowledged
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256

    @property
    def size_bytes(self) -> int:
        """UTF-8 bytes in the serialised payload (not wire-protocol overhead)."""
        return len(self.payload.encode("utf-8"))


<<<<<<< HEAD
class NetworkSimulator:
    """Simulate messages among independent in-memory replicas."""
=======
@dataclass
class PendingDelta:
    """An outstanding unacknowledged delta sent from sender to receiver."""

    msg_id: str
    sender: str
    receiver: str
    delta_state: dict[str, Any]
    payload: str


class NetworkSimulator:
    """Simulate messages and acknowledgements among independent in-memory replicas."""
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256

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

<<<<<<< HEAD
        # Each pair tracks state successfully delivered from sender to receiver.
        self.peer_known_states: dict[tuple[str, str], Replica] = {}
=======
        # Each pair tracks state confirmed by an ACK delivered from receiver to sender.
        self.peer_known_states: dict[tuple[str, str], Replica] = {}
        # Tracks the outstanding unacknowledged delta per (sender, receiver) pair.
        self.pending_deltas: dict[tuple[str, str], PendingDelta] = {}

>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
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

<<<<<<< HEAD
=======
        # Data message metrics
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
        self.messages_sent = 0
        self.messages_delivered = 0
        # Count payload bytes at send/queue time, including messages later lost.
        self.bytes_transmitted = 0
        # Counts payload bytes delivered to receivers, including duplicates.
        self.bytes_delivered = 0
        self.merge_latencies_ms: list[float] = []

<<<<<<< HEAD
=======
        # ACK metrics (separately accounted)
        self.ack_messages_sent = 0
        self.ack_messages_delivered = 0
        self.ack_bytes_transmitted = 0
        self.ack_bytes_delivered = 0

        self._msg_seq = 0

    def _next_message_id(self, sender: str, receiver: str) -> str:
        """Generate a unique identifier for a data message."""
        self._msg_seq += 1
        return f"msg-{sender}-{receiver}-{self._msg_seq}-{uuid4().hex[:8]}"

>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
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
<<<<<<< HEAD
        """Build a delta against state previously delivered on this pair."""
=======
        """Build a delta against state confirmed via ACKs on this pair."""
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
        local = self.replicas[sender].snapshot()
        known = self.peer_known_states[(sender, receiver)].snapshot()

        # Counter: transmit only components whose value increased.
        counter_delta = {
            rid: count
            for rid, count in local["counter"].items()
            if count > known["counter"].get(rid, 0)
        }

<<<<<<< HEAD
        # OR-Set: transmit addition tags and removal tags not yet delivered.
=======
        # OR-Set: transmit addition tags and removal tags not yet acknowledged.
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
        local_adds = local["task_set"]["adds"]
        known_adds = known["task_set"]["adds"]
        adds_delta: dict[str, list[str]] = {}

        for value, tags in local_adds.items():
            unseen = set(tags) - set(known_adds.get(value, []))
            if unseen:
                adds_delta[value] = sorted(unseen)

<<<<<<< HEAD
        # Ordering metadata travels with every previously undelivered add tag.
=======
        # Ordering metadata travels with every previously unacknowledged add tag.
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
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

<<<<<<< HEAD
        # LWW-Map: transmit keys whose version is newer than the delivered one.
=======
        # LWW-Map: transmit keys whose version is newer than the acknowledged one.
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
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
<<<<<<< HEAD
=======
            pair = (sender, receiver)
            pending = self.pending_deltas.get(pair)
            if pending is not None:
                # Retry logic: an unacknowledged delta exists.
                # Avoid queuing duplicate retry packets when one is already in the queue.
                already_queued = any(
                    m.msg_id == pending.msg_id and m.msg_type == "data"
                    for m in self.queue
                )
                if already_queued:
                    return False

                # Resend the exact same logical delta with original ID and payload.
                message = Message(
                    sender=sender,
                    receiver=receiver,
                    payload=pending.payload,
                    msg_id=pending.msg_id,
                    msg_type="data",
                )
                self.queue.append(message)
                self.messages_sent += 1
                self.bytes_transmitted += message.size_bytes
                return True

            # No outstanding delta: compute new delta against acknowledged state.
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
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
<<<<<<< HEAD
        else:
            # Full-state mode deliberately sends even an empty state.
            state = self.replicas[sender].snapshot()

        payload = json.dumps(state, sort_keys=True, separators=(",", ":"))
        message = Message(sender, receiver, payload)
=======

            msg_id = self._next_message_id(sender, receiver)
            payload = json.dumps(state, sort_keys=True, separators=(",", ":"))
            self.pending_deltas[pair] = PendingDelta(
                msg_id=msg_id,
                sender=sender,
                receiver=receiver,
                delta_state=state,
                payload=payload,
            )
            message = Message(
                sender=sender,
                receiver=receiver,
                payload=payload,
                msg_id=msg_id,
                msg_type="data",
            )
        else:
            # Full-state mode deliberately sends even an empty state.
            state = self.replicas[sender].snapshot()
            payload = json.dumps(state, sort_keys=True, separators=(",", ":"))
            msg_id = self._next_message_id(sender, receiver)
            message = Message(
                sender=sender,
                receiver=receiver,
                payload=payload,
                msg_id=msg_id,
                msg_type="data",
            )

>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
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
<<<<<<< HEAD
        # delivered more than once, and each arrival is counted in bytes_delivered.
        self.queue.append(self.queue[index])

=======
        # delivered more than once, and each arrival is counted in delivered metrics.
        self.queue.append(self.queue[index])

    def _queue_ack(self, data_message: Message) -> None:
        """Generate and enqueue an ACK for a delivered data message."""
        ack_payload = json.dumps(
            {
                "type": "ack",
                "ack_for": data_message.msg_id,
                "sender": data_message.receiver,
                "receiver": data_message.sender,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        ack = Message(
            sender=data_message.receiver,
            receiver=data_message.sender,
            payload=ack_payload,
            msg_id=f"ack-{data_message.msg_id}",
            msg_type="ack",
            ack_for=data_message.msg_id,
        )
        self.queue.append(ack)
        self.ack_messages_sent += 1
        self.ack_bytes_transmitted += ack.size_bytes

    def _process_ack(self, ack_message: Message) -> bool:
        """Process an ACK delivered back to the data sender."""
        orig_sender = ack_message.receiver
        orig_receiver = ack_message.sender
        pair = (orig_sender, orig_receiver)

        pending = self.pending_deltas.get(pair)
        if pending is None:
            # Stale, duplicate, or unknown ACK
            return False

        if pending.msg_id != ack_message.ack_for:
            # Mismatched or older ACK
            return False

        # Advance peer_known_states ONLY with the acknowledged delta state
        self.peer_known_states[pair].merge_state(pending.delta_state)
        del self.pending_deltas[pair]
        return True

>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
    def deliver(self, index: int = 0) -> bool:
        """Deliver one queued message if its link is currently available."""
        if not 0 <= index < len(self.queue):
            raise IndexError("Message index is out of range")

        message = self.queue[index]
        if self._groups[message.sender] != self._groups[message.receiver]:
            return False

        self.queue.pop(index)
<<<<<<< HEAD
=======

        if message.msg_type == "ack":
            self.ack_messages_delivered += 1
            self.ack_bytes_delivered += message.size_bytes
            self._process_ack(message)
            return True

        # Data message delivery
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
        incoming_state = json.loads(message.payload)

        start = perf_counter()
        self.replicas[message.receiver].merge_state(incoming_state)
        elapsed_ms = (perf_counter() - start) * 1000
        # Keep the merge-latency definition comparable across modes: time only
        # the target replica's state merge, not sender/receiver bookkeeping.
        self.merge_latencies_ms.append(elapsed_ms)

<<<<<<< HEAD
        if self.sync_mode == "delta":
            self.peer_known_states[(message.sender, message.receiver)].merge_state(
                incoming_state
            )

        self.messages_delivered += 1
        self.bytes_delivered += message.size_bytes
=======
        self.messages_delivered += 1
        self.bytes_delivered += message.size_bytes

        # In delta mode, queue an explicit ACK back to the sender through the network
        if self.sync_mode == "delta":
            self._queue_ack(message)

>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
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

<<<<<<< HEAD
=======
    def drop_message(self, index: int = 0) -> Message:
        """Remove a queued message without delivering it."""
        if not 0 <= index < len(self.queue):
            raise IndexError("Message index is out of range")
        return self.queue.pop(index)

    def drop_first_ack(self) -> Message | None:
        """Find and remove the first queued ACK message."""
        for index, msg in enumerate(self.queue):
            if msg.msg_type == "ack":
                return self.queue.pop(index)
        return None

    def drop_ack(self, ack_for: str) -> bool:
        """Find and drop an ACK for a specific data message ID."""
        for index, msg in enumerate(self.queue):
            if msg.msg_type == "ack" and msg.ack_for == ack_for:
                self.queue.pop(index)
                return True
        return False

    def drop_random_messages(
        self,
        p_data: float = 0.0,
        p_ack: float = 0.0,
        rng: random.Random | None = None,
    ) -> int:
        """Drop queued messages probabilistically according to type."""
        if rng is None:
            rng = random.Random()
        survived: list[Message] = []
        dropped = 0
        for msg in self.queue:
            p_loss = p_ack if msg.msg_type == "ack" else p_data
            if rng.random() < p_loss:
                dropped += 1
            else:
                survived.append(msg)
        self.queue = survived
        return dropped

>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
    def all_replicas_converged(self) -> bool:
        """Return whether all replicas have identical logical CRDT state."""
        states = []
        for replica in self.replicas.values():
            state = replica.snapshot()
            state.pop("replica_id")
            states.append(state)
        return all(state == states[0] for state in states[1:])

<<<<<<< HEAD
=======
    def all_deltas_acknowledged(self) -> bool:
        """Return whether there are no outstanding unacknowledged deltas."""
        return len(self.pending_deltas) == 0

    def has_pending_delta(self, sender: str, receiver: str) -> bool:
        """Return whether there is an unacknowledged delta for a peer pair."""
        return (sender, receiver) in self.pending_deltas

    def pending_delta_count(self) -> int:
        """Return the number of unacknowledged deltas across all pairs."""
        return len(self.pending_deltas)

    def reset_peer_knowledge(self, replica_id: str | None = None) -> None:
        """Reset acknowledged knowledge and clear pending deltas for a replica."""
        if replica_id is None:
            pairs_to_reset = list(self.peer_known_states.keys())
        else:
            pairs_to_reset = [
                (s, r) for (s, r) in self.peer_known_states
                if s == replica_id or r == replica_id
            ]

        for s, r in pairs_to_reset:
            known_id = f"__known__{s}__{r}"
            self.peer_known_states[(s, r)] = Replica(known_id)
            self.pending_deltas.pop((s, r), None)

    def reset_metrics(self) -> None:
        """Reset all metric counters."""
        self.messages_sent = 0
        self.messages_delivered = 0
        self.bytes_transmitted = 0
        self.bytes_delivered = 0
        self.merge_latencies_ms.clear()
        self.ack_messages_sent = 0
        self.ack_messages_delivered = 0
        self.ack_bytes_transmitted = 0
        self.ack_bytes_delivered = 0

>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
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
<<<<<<< HEAD
=======
            "ack_messages_sent": self.ack_messages_sent,
            "ack_messages_delivered": self.ack_messages_delivered,
            "ack_bytes_transmitted": self.ack_bytes_transmitted,
            "ack_bytes_delivered": self.ack_bytes_delivered,
>>>>>>> 5255a852cd4600efefc4c5419c382424dc5ee256
        }
