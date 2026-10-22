
from crdts.lww_map import LWWMap


def test_put_and_get():
    alice = LWWMap("alice")
    alice.put("theme", "blue")

    assert alice.get("theme") == "blue"
    assert alice.items == {"theme": "blue"}


def test_merge_independent_keys():
    alice = LWWMap("alice")
    bob = LWWMap("bob")

    alice.put("theme", "blue")
    bob.put("language", "English")

    alice.merge_state(bob.state)
    bob.merge_state(alice.state)

    assert alice.items == {
        "theme": "blue",
        "language": "English",
    }
    assert alice.state == bob.state


def test_conflicting_writes_converge_deterministically():
    alice = LWWMap("alice")
    bob = LWWMap("bob")

    alice.put("theme", "blue")
    bob.put("theme", "red")

    alice.merge_state(bob.state)
    bob.merge_state(alice.state)

    # Equal timestamps: "bob" wins the replica-ID tie-break.
    assert alice.get("theme") == "red"
    assert bob.get("theme") == "red"
    assert alice.state == bob.state


def test_duplicate_merge_does_not_change_state():
    alice = LWWMap("alice")
    bob = LWWMap("bob")

    alice.put("theme", "blue")
    bob.put("language", "English")

    alice.merge_state(bob.state)
    first_result = alice.state

    alice.merge_state(bob.state)

    assert alice.state == first_result


def test_deletion_tombstone_propagates():
    alice = LWWMap("alice")
    bob = LWWMap("bob")

    alice.put("task", "draft")
    bob.merge_state(alice.state)

    alice.remove("task")
    bob.merge_state(alice.state)

    assert alice.get("task") is None
    assert bob.get("task") is None
    assert "task" not in bob.items
    assert alice.state == bob.state


def test_local_write_after_merge_gets_later_timestamp():
    alice = LWWMap("alice")
    bob = LWWMap("bob")

    alice.put("status", "old")
    bob.merge_state(alice.state)

    bob.put("status", "new")
    alice.merge_state(bob.state)

    assert alice.get("status") == "new"

    bob.merge_state(alice.state)
    assert bob.get("status") == "new"
    assert alice.state == bob.state
