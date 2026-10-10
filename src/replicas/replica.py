from copy import deepcopy
from typing import Any

from crdts.g_counter import GCounter
from crdts.lww_map import LWWMap
from crdts.or_set import ORSet


class Replica:
    """One independent copy of our replicated data store."""

    def __init__(self, replica_id: str) -> None:
        if not isinstance(replica_id, str) or not replica_id.strip():
            raise ValueError("replica_id cannot be empty")

        self.replica_id = replica_id
        self.counter = GCounter(replica_id)
        self.task_set = ORSet(replica_id)
        self.settings = LWWMap(replica_id)

    def snapshot(self) -> dict[str, Any]:
        """Export this replica's current state."""
        return {
            "replica_id": self.replica_id,
            "counter": self.counter.state,
            "task_set": self.task_set.state,
            "settings": self.settings.state,
        }

    def merge_state(self, incoming_state: dict[str, Any]) -> None:
        """Atomically validate and merge a full or partial CRDT snapshot."""
        if not isinstance(incoming_state, dict):
            raise ValueError("incoming_state must be a dictionary")

        required_keys = {"replica_id", "counter", "task_set", "settings"}
        if not required_keys.issubset(incoming_state):
            raise ValueError("snapshot is missing required fields")

        incoming_id = incoming_state["replica_id"]
        if not isinstance(incoming_id, str) or not incoming_id.strip():
            raise ValueError("invalid source replica ID")

        if incoming_id == self.replica_id:
            return

        # Merge into copies first so a malformed later component cannot leave
        # the receiver partially updated after an earlier component succeeded.
        new_counter = deepcopy(self.counter)
        new_task_set = deepcopy(self.task_set)
        new_settings = deepcopy(self.settings)

        new_counter.merge_state(incoming_state["counter"])
        new_task_set.merge_state(incoming_state["task_set"])
        new_settings.merge_state(incoming_state["settings"])

        self.counter = new_counter
        self.task_set = new_task_set
        self.settings = new_settings

    def restore_snapshot(self, saved_state: dict[str, Any]) -> None:
        """Restore this replica from its own saved snapshot."""
        if not isinstance(saved_state, dict):
            raise ValueError("saved_state must be a dictionary")

        required_keys = {
            "replica_id",
            "counter",
            "task_set",
            "settings",
        }

        if not required_keys.issubset(saved_state):
            raise ValueError("Saved snapshot is missing required fields")

        if saved_state["replica_id"] != self.replica_id:
            raise ValueError("Saved snapshot belongs to another replica")

        # Restore into temporary CRDTs before replacing the current state.
        new_counter = GCounter(self.replica_id)
        new_task_set = ORSet(self.replica_id)
        new_settings = LWWMap(self.replica_id)

        new_counter.merge_state(saved_state["counter"])
        new_task_set.merge_state(saved_state["task_set"])
        new_settings.merge_state(saved_state["settings"])

        self.counter = new_counter
        self.task_set = new_task_set
        self.settings = new_settings
