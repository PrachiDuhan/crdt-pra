
from crdts.or_set import ORSet


def test_add_and_remove():
    alice = ORSet("alice")

    alice.add("Milk")
    assert alice.values == {"Milk"}

    alice.remove("Milk")
    assert alice.values == set()


def test_replicas_merge_independent_items():
    alice = ORSet("alice")
    bob = ORSet("bob")

    alice.add("Milk")
    bob.add("Bread")

    alice.merge_state(bob.state)
    bob.merge_state(alice.state)

    assert alice.values == {"Milk", "Bread"}
    assert bob.values == alice.values


def test_duplicate_merge_does_not_duplicate_values():
    alice = ORSet("alice")
    bob = ORSet("bob")

    alice.add("Milk")
    bob.add("Bread")

    alice.merge_state(bob.state)
    alice.merge_state(bob.state)

    assert alice.values == {"Milk", "Bread"}


def test_concurrent_add_survives_an_unobserved_remove():
    alice = ORSet("alice")
    bob = ORSet("bob")

    # Bob observes Alice's original addition.
    alice.add("Milk")
    bob.merge_state(alice.state)

    # Alice removes the original tag.
    # Bob, unaware of this removal, adds Milk with a new tag.
    alice.remove("Milk")
    bob.add("Milk")

    # Alice receives Bob's state, including the new addition.
    alice.merge_state(bob.state)

    assert alice.values == {"Milk"}

    # Bob receives Alice's merged state.
    bob.merge_state(alice.state)

    assert bob.values == {"Milk"}
    assert bob.state == alice.state
