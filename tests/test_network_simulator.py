import random

from network.simulator import NetworkSimulator


def make_network(sync_mode="full"):
    return NetworkSimulator(
        ["r1", "r2", "r3", "r4", "r5"],
        sync_mode=sync_mode,
    )


def test_creates_five_independent_replicas():
    network = make_network()

    assert len(network.replicas) == 5
    assert network.all_replicas_converged()

def test_replicas_converge_after_broadcast():
    network = make_network()

    network.replicas["r1"].counter.increment(2)
    network.replicas["r2"].counter.increment(3)
    network.replicas["r3"].task_set.add("Buy milk")
    network.replicas["r4"].settings.put("theme", "blue")

    queued = network.broadcast_round()
    delivered = network.deliver_all(random_order=True, seed=42)

    assert queued == 20
    assert delivered == 20
    assert network.all_replicas_converged()

    for replica in network.replicas.values():
        assert replica.counter.value == 5
        assert replica.task_set.values == {"Buy milk"}
        assert replica.settings.items == {"theme": "blue"}

def test_duplicate_and_reordered_messages():
    network = make_network()

    network.replicas["r1"].counter.increment(2)
    network.replicas["r2"].counter.increment(3)

    network.broadcast_round()
    network.duplicate_message(0)
    network.deliver_all(random_order=True, seed=123)

    assert network.all_replicas_converged()

    for replica in network.replicas.values():
        assert replica.counter.value == 5

def test_partition_prevents_full_convergence_until_reconnected():
    network = make_network()

    network.replicas["r1"].counter.increment(5)
    network.replicas["r4"].task_set.add("Emergency supplies")

    # Queue snapshots while all replicas can communicate.
    network.broadcast_round()

    # Separate replicas into two disconnected groups.
    network.set_partitions([
        ["r1", "r2"],
        ["r3", "r4", "r5"],
    ])

    network.deliver_all(random_order=True, seed=7)

    assert not network.all_replicas_converged()
    assert len(network.queue) > 0

    # Reconnect the network and deliver pending messages.
    network.heal_partitions()
    network.deliver_all(random_order=True, seed=99)

    assert network.all_replicas_converged()

    for replica in network.replicas.values():
        assert replica.counter.value == 5
        assert replica.task_set.values == {"Emergency supplies"}

def test_network_metrics_are_recorded():
    network = make_network()

    network.replicas["r1"].counter.increment()
    network.broadcast_round()
    network.deliver_all()

    metrics = network.metrics

    assert metrics["messages_delivered"] == 20
    assert metrics["bytes_transmitted"] > 0
    assert metrics["average_merge_latency_ms"] >= 0

def test_delta_mode_converges_with_duplicates_and_reordering():
    network = make_network(sync_mode="delta")

    network.replicas["r1"].counter.increment(2)
    network.replicas["r2"].counter.increment(3)
    network.replicas["r3"].task_set.add("Milk")
    network.replicas["r4"].settings.put("theme", "blue")

    assert network.broadcast_round() > 0

    network.duplicate_message(0)
    network.deliver_all(random_order=True, seed=123)

    assert network.all_replicas_converged()

    for replica in network.replicas.values():
        assert replica.counter.value == 5
        assert replica.task_set.values == {"Milk"}
        assert replica.settings.items == {"theme": "blue"}

def test_delta_mode_converges_after_partition_recovery():
    network = make_network(sync_mode="delta")

    network.set_partitions([
        ["r1", "r2"],
        ["r3", "r4", "r5"],
    ])

    network.replicas["r1"].counter.increment(5)
    network.replicas["r4"].task_set.add("Emergency supplies")
    network.replicas["r2"].settings.put("theme", "blue")
    network.replicas["r3"].settings.put("theme", "red")

    # Exchange updates within each disconnected group.
    for round_number in range(2):
        network.broadcast_round()
        network.deliver_all(
            random_order=True,
            seed=100 + round_number,
        )

    assert not network.all_replicas_converged()

    # Reconnect and allow deltas to propagate over several rounds.
    network.heal_partitions()

    for round_number in range(8):
        network.broadcast_round()
        network.deliver_all(
            random_order=True,
            seed=200 + round_number,
        )

        if network.all_replicas_converged() and not network.queue:
            break

    assert network.all_replicas_converged()

    for replica in network.replicas.values():
        assert replica.counter.value == 5
        assert replica.task_set.values == {"Emergency supplies"}
        assert replica.settings.get("theme") == "red"

def test_delta_mode_skips_unchanged_snapshots():
    network = make_network(sync_mode="delta")

    # Empty replicas have no changes to send.
    assert network.broadcast_round() == 0

    network.replicas["r1"].counter.increment()

    # First round: r1 sends its update to the other replicas.
    network.broadcast_round()
    network.deliver_all()

    # Second round: the other replicas propagate what they learned.
    network.broadcast_round()
    network.deliver_all()

    # All sender-receiver records have now caught up.
    assert network.broadcast_round() == 0
    assert network.all_replicas_converged()

def test_delta_recovers_after_messages_are_lost():
    network = make_network(sync_mode="delta")

    network.replicas["r1"].counter.increment(2)
    network.replicas["r2"].counter.increment(3)
    network.replicas["r3"].task_set.add("Emergency supplies")
    network.replicas["r4"].settings.put("theme", "blue")

    # Queue updates, then simulate all queued packets being lost.
    network.broadcast_round()
    assert len(network.queue) > 0
    network.queue.clear()

    # No message was delivered, so the updates must be retried.
    assert network.messages_delivered == 0

    # Repeated synchronisation rounds retry the missing information.
    for round_number in range(10):
        network.broadcast_round()
        network.deliver_all(
            random_order=True,
            seed=700 + round_number,
        )

        if network.all_replicas_converged() and not network.queue:
            break

    assert network.all_replicas_converged()

    for replica in network.replicas.values():
        assert replica.counter.value == 5
        assert replica.task_set.values == {"Emergency supplies"}
        assert replica.settings.items == {"theme": "blue"}

def test_randomized_partition_recovery_across_both_modes():
    replica_ids = ["r1", "r2", "r3", "r4", "r5"]

    # Run 10 reproducible scenarios for each synchronisation mode.
    for mode in ("full", "delta"):
        for seed in range(10):
            rng = random.Random(seed)

            network = NetworkSimulator(
                replica_ids,
                sync_mode=mode,
            )

            # Create two disconnected network groups.
            shuffled = replica_ids.copy()
            rng.shuffle(shuffled)
            split = rng.randint(1, len(shuffled) - 1)

            network.set_partitions([
                shuffled[:split],
                shuffled[split:],
            ])

            # Each replica performs independent offline updates.
            for rid, replica in network.replicas.items():
                replica.counter.increment(rng.randint(1, 5))

                replica.task_set.add("shared-task")
                if rng.random() < 0.4:
                    replica.task_set.remove("shared-task")

                replica.task_set.add(f"task-{rid}")

                replica.settings.put(
                    "theme",
                    rng.choice(["light", "dark"]),
                )

            # Synchronise within the disconnected groups.
            for round_number in range(2):
                network.broadcast_round()

                original_count = len(network.queue)
                for index in range(original_count):
                    if rng.random() < 0.2:
                        network.duplicate_message(index)

                network.deliver_all(
                    random_order=True,
                    seed=rng.randrange(1_000_000_000),
                )

            # Reconnect every replica.
            network.heal_partitions()

            # Exchange updates until convergence or the round limit.
            for round_number in range(10):
                network.broadcast_round()

                original_count = len(network.queue)
                for index in range(original_count):
                    if rng.random() < 0.2:
                        network.duplicate_message(index)

                network.deliver_all(
                    random_order=True,
                    seed=rng.randrange(1_000_000_000),
                )

                if (
                    network.all_replicas_converged()
                    and not network.queue
                ):
                    break

            assert network.all_replicas_converged(), (
                f"Failed to converge: mode={mode}, seed={seed}"
            )

            for replica in network.replicas.values():
                assert replica.counter.value > 0



def test_transmission_bytes_count_packets_even_if_later_blocked():
    network = make_network()
    assert network.send_snapshot("r1", "r2") is True
    sent_bytes = network.bytes_transmitted
    assert sent_bytes > 0
    assert network.messages_sent == 1
    assert network.bytes_delivered == 0

    # The packet was transmitted while connected, but becomes undeliverable
    # after a partition is introduced.
    network.set_partitions([["r1"], ["r2", "r3", "r4", "r5"]])
    assert network.deliver(0) is False
    assert network.bytes_transmitted == sent_bytes
    assert network.bytes_delivered == 0


def test_concurrent_lww_writes_converge_after_heal_in_both_modes():
    """Concurrent same-key writes resolve identically regardless of delivery order."""
    for mode in ("full", "delta"):
        for seed in (11, 29):
            network = make_network(sync_mode=mode)
            network.set_partitions([["r1", "r2"], ["r3", "r4", "r5"]])

            # Same logical timestamp, distinct writers: lexicographically later
            # replica ID is the documented deterministic tie-break winner.
            network.replicas["r1"].settings.put("theme", "blue")
            network.replicas["r4"].settings.put("theme", "red")

            for round_number in range(2):
                network.broadcast_round()
                network.deliver_all(
                    random_order=True,
                    seed=seed + round_number,
                )

            assert not network.all_replicas_converged()
            network.heal_partitions()

            for round_number in range(10):
                network.broadcast_round()
                # Duplicate a queued message when available, exercising
                # idempotence alongside shuffled delivery.
                if network.queue:
                    network.duplicate_message(0)
                network.deliver_all(
                    random_order=True,
                    seed=seed * 100 + round_number,
                )
                if network.all_replicas_converged() and not network.queue:
                    break

            assert network.all_replicas_converged(), (mode, seed)
            assert all(
                replica.settings.get("theme") == "red"
                for replica in network.replicas.values()
            )


def test_reversed_delivery_order_does_not_change_lww_winner():
    """Two replicas converge to the same LWW value under opposite delivery orders."""
    final_states = []
    for reverse in (False, True):
        network = make_network(sync_mode="full")
        network.replicas["r1"].settings.put("status", "from-r1")
        network.replicas["r5"].settings.put("status", "from-r5")
        network.broadcast_round()

        # Reverse the queued order in one run to perturb delivery ordering.
        if reverse:
            network.queue.reverse()
        network.deliver_all(random_order=False)

        for _ in range(3):
            network.broadcast_round()
            network.deliver_all(random_order=reverse, seed=101 if reverse else 202)

        assert network.all_replicas_converged()
        final_states.append(network.replicas["r1"].settings.get("status"))

    assert final_states == ["from-r5", "from-r5"]



def test_task_addition_order_converges_in_full_and_delta_modes():
    expected = ["dipanshu", "prachi", "ayush", "khushal"]

    for mode in ("full", "delta"):
        network = make_network(sync_mode=mode)
        for name in expected:
            network.replicas["r1"].task_set.add(name)

        for round_number in range(5):
            network.broadcast_round()
            network.deliver_all(random_order=True, seed=9100 + round_number)
            if network.all_replicas_converged() and not network.queue:
                break

        assert network.all_replicas_converged()
        for replica in network.replicas.values():
            assert replica.task_set.ordered_values == expected



def test_addition_time_order_survives_partition_and_heal():
    """Independent task additions retain their timestamp order after healing."""
    expected = ["dipanshu", "prachi", "ayush", "khushal"]

    for mode in ("full", "delta"):
        network = make_network(sync_mode=mode)
        network.set_partitions([["r1", "r3"], ["r2", "r4", "r5"]])

        # Each task is created on a different replica while the network is split.
        network.replicas["r1"].task_set.add("dipanshu")
        network.replicas["r4"].task_set.add("prachi")
        network.replicas["r2"].task_set.add("ayush")
        network.replicas["r5"].task_set.add("khushal")

        # Synchronize within the current partitions, then reconnect everyone.
        for round_number in range(2):
            network.broadcast_round()
            network.deliver_all(random_order=True, seed=8200 + round_number)

        network.heal_partitions()
        for round_number in range(12):
            network.broadcast_round()
            network.deliver_all(random_order=True, seed=8300 + round_number)
            if network.all_replicas_converged() and not network.queue:
                break

        assert network.all_replicas_converged()
        for replica in network.replicas.values():
            assert replica.task_set.ordered_values == expected
