
from crdts.g_counter import GCounter


def test_increment():
    alice = GCounter("alice")
    alice.increment()
    alice.increment()

    assert alice.value == 2
    assert alice.state == {"alice": 2}


def test_merge_independent_replicas():
    alice = GCounter("alice")
    bob = GCounter("bob")

    alice.increment(2)
    bob.increment(3)

    alice.merge(bob)
    bob.merge(alice)

    assert alice.value == 5
    assert bob.value == 5
    assert alice.state == bob.state


def test_duplicate_merge_does_not_double_count():
    alice = GCounter("alice")
    bob = GCounter("bob")

    alice.increment(2)
    bob.increment(3)

    alice.merge(bob)
    alice.merge(bob)

    assert alice.value == 5


def test_merge_does_not_change_other_replica():
    alice = GCounter("alice")
    bob = GCounter("bob")

    alice.increment(2)
    bob.increment(3)

    alice.merge(bob)

    assert bob.state == {"bob": 3}
    assert alice.state == {"alice": 2, "bob": 3}

def test_merge_received_snapshot():
    alice = GCounter("alice")
    bob = GCounter("bob")

    alice.increment(2)
    bob.increment(3)

    # Simulate Alice receiving Bob's state.
    alice.merge_state(bob.state)

    assert alice.state == {"alice": 2, "bob": 3}
    assert alice.value == 5

    # Receiving the same snapshot again must not double-count.
    alice.merge_state(bob.state)

    assert alice.value == 5

def test_invalid_snapshot_does_not_partially_mutate_counter():
    import pytest

    alice = GCounter("alice")
    alice.increment(2)
    before = alice.state

    with pytest.raises(ValueError):
        alice.merge_state({"bob": 5, "invalid": -1})

    assert alice.state == before
