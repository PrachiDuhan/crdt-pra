import json

import pytest
from network.simulator import NetworkSimulator
from replicas.replica import Replica
from storage.local_store import load_replica, save_replica


def test_replica_survives_save_and_restore(tmp_path):
    original = Replica("r1")

    original.counter.increment(4)
    original.task_set.add("Emergency supplies")
    original.task_set.add("First aid kit")
    original.task_set.remove("First aid kit")
    original.settings.put("theme", "blue")

    expected_state = original.snapshot()

    save_replica(original, tmp_path)
    restored = load_replica("r1", tmp_path)

    assert restored.snapshot() == expected_state

    # Verify the restored replica can continue working.
    restored.counter.increment(2)
    restored.task_set.add("Water")
    restored.settings.put("theme", "dark")

    assert restored.counter.value == 6
    assert restored.task_set.values == {
        "Emergency supplies",
        "Water",
    }
    assert restored.settings.get("theme") == "dark"


def test_loading_wrong_replica_identity_is_rejected(tmp_path):
    original = Replica("r1")
    save_replica(original, tmp_path)

    source = json.loads(
        (tmp_path / "r1.json").read_text(encoding="utf-8")
    )

    # Simulate a file whose outer identity disagrees with its snapshot.
    source["replica_id"] = "r2"
    (tmp_path / "r2.json").write_text(
        json.dumps(source),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_replica("r2", tmp_path)


def test_missing_saved_replica_is_reported(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_replica("r1", tmp_path)


def test_corrupt_saved_json_is_rejected(tmp_path):
    (tmp_path / "r1.json").write_text(
        "{not valid JSON",
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_replica("r1", tmp_path)


@pytest.mark.parametrize("mode", ["full", "delta"])
def test_restored_replica_resynchronises_with_peers(tmp_path, mode):
    replica_ids = ["r1", "r2", "r3", "r4", "r5"]

    # Prepare and persist r1's local state.
    original = Replica("r1")
    original.counter.increment(4)
    original.task_set.add("PS39-PERSIST-TEST")
    original.settings.put("theme", "blue")
    save_replica(original, tmp_path)

    # Create a fresh network and restore only r1.
    network = NetworkSimulator(replica_ids, sync_mode=mode)
    network.replicas["r1"] = load_replica("r1", tmp_path)

    # In delta mode, discard stale sender-receiver knowledge
    # involving the restored replica.
    if mode == "delta":
        for sender in replica_ids:
            for receiver in replica_ids:
                if (
                    sender != receiver
                    and (sender == "r1" or receiver == "r1")
                ):
                    known_id = f"__known__{sender}__{receiver}"
                    network.peer_known_states[
                        (sender, receiver)
                    ] = Replica(known_id)

    # Other replicas make independent changes.
    network.replicas["r2"].counter.increment(3)
    network.replicas["r3"].task_set.add("offline-note")
    network.replicas["r4"].settings.put("language", "English")

    # Exchange updates until the replicas converge.
    for round_number in range(20):
        network.broadcast_round()
        network.deliver_all(
            random_order=True,
            seed=1200 + round_number,
        )

        if network.all_replicas_converged():
            break

    assert network.all_replicas_converged()

    # Verify that all restored and new data survived.
    for replica in network.replicas.values():
        assert replica.counter.value == 7
        assert replica.task_set.values == {
            "PS39-PERSIST-TEST",
            "offline-note",
        }
        assert replica.settings.items == {
            "theme": "blue",
            "language": "English",
        }



def test_task_order_survives_save_and_restore(tmp_path):
    original = Replica("ordered-replica")
    expected = ["dipanshu", "prachi", "ayush", "khushal"]
    for name in expected:
        original.task_set.add(name)

    save_replica(original, tmp_path)
    restored = load_replica("ordered-replica", tmp_path)

    assert restored.task_set.ordered_values == expected
    assert restored.snapshot() == original.snapshot()
