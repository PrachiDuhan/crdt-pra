
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



def test_ordered_values_preserve_local_addition_order():
    replica = ORSet("alice")
    expected = ["dipanshu", "prachi", "ayush", "khushal"]

    for name in expected:
        replica.add(name)

    assert replica.ordered_values == expected


def test_ordered_values_preserve_causal_order_after_merge():
    alice = ORSet("alice")
    bob = ORSet("bob")

    alice.add("dipanshu")
    bob.merge_state(alice.state)
    bob.add("prachi")
    alice.merge_state(bob.state)
    bob.merge_state(alice.state)

    assert alice.ordered_values == ["dipanshu", "prachi"]
    assert bob.ordered_values == alice.ordered_values
    assert bob.state == alice.state


def test_concurrent_additions_have_deterministic_order():
    alice = ORSet("alice")
    bob = ORSet("bob")
    alice.add("from alice")
    bob.add("from bob")

    alice.merge_state(bob.state)
    bob.merge_state(alice.state)

    assert alice.ordered_values == ["from alice", "from bob"]
    assert bob.ordered_values == alice.ordered_values
    assert bob.state == alice.state


def test_readded_value_uses_new_active_addition_order():
    replica = ORSet("alice")
    replica.add("dipanshu")
    replica.add("prachi")
    replica.remove("dipanshu")
    replica.add("dipanshu")

    assert replica.ordered_values == ["prachi", "dipanshu"]

def test_sequential_additions_on_different_replicas_keep_process_order():
    """Fast sequential calls stay ordered even if the OS clock has coarse resolution."""
    first = ORSet("r4")
    second = ORSet("r2")

    first.add("prachi")
    second.add("ayush")

    first.merge_state(second.state)
    second.merge_state(first.state)

    assert first.ordered_values == ["prachi", "ayush"]
    assert second.ordered_values == ["prachi", "ayush"]
