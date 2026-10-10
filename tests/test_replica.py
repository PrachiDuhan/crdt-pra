
import json

from replicas.replica import Replica


def test_replicas_exchange_all_three_crdts():
    alice = Replica("alice")
    bob = Replica("bob")

    # Independent updates on Alice.
    alice.counter.increment(2)
    alice.task_set.add("Buy milk")
    alice.settings.put("theme", "blue")

    # Independent updates on Bob.
    bob.counter.increment(3)
    bob.task_set.add("Study Python")
    bob.settings.put("language", "English")

    # Exchange snapshots.
    alice.merge_state(bob.snapshot())
    bob.merge_state(alice.snapshot())

    assert alice.counter.value == 5
    assert bob.counter.value == 5

    assert alice.task_set.values == {
        "Buy milk",
        "Study Python",
    }
    assert bob.task_set.values == alice.task_set.values

    assert alice.settings.items == {
        "theme": "blue",
        "language": "English",
    }
    assert bob.settings.items == alice.settings.items


def test_repeated_snapshot_does_not_change_state():
    alice = Replica("alice")
    bob = Replica("bob")

    alice.counter.increment(2)
    bob.counter.increment(3)
    bob.task_set.add("Bread")
    bob.settings.put("theme", "dark")

    incoming = bob.snapshot()

    alice.merge_state(incoming)
    first_result = alice.snapshot()

    alice.merge_state(incoming)

    assert alice.snapshot() == first_result


def test_snapshot_can_be_serialised_to_json():
    alice = Replica("alice")
    alice.counter.increment()
    alice.task_set.add("Milk")
    alice.settings.put("theme", "blue")

    encoded = json.dumps(alice.snapshot())
    decoded = json.loads(encoded)

    bob = Replica("bob")
    bob.merge_state(decoded)

    assert bob.counter.value == 1
    assert bob.task_set.values == {"Milk"}
    assert bob.settings.items == {"theme": "blue"}


def test_malformed_snapshot_does_not_partially_mutate_replica():
    import pytest

    alice = Replica("alice")
    alice.counter.increment(2)
    alice.task_set.add("existing")
    alice.settings.put("theme", "blue")
    before = alice.snapshot()

    incoming = Replica("bob").snapshot()
    incoming["counter"] = {"bob": 9}
    incoming["task_set"] = {"adds": {"milk": ["tag-1"]}, "removed_tags": None}

    with pytest.raises(ValueError):
        alice.merge_state(incoming)

    assert alice.snapshot() == before
