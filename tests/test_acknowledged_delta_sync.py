"""Comprehensive acceptance tests for Acknowledged Delta Synchronization (PS-39).

Covers:
- Test A: Happy-path acknowledged delta sync with intermediate state inspection
- Test B: Lost ACK and retry with message ID and payload preservation
- Test C: Duplicate ACK idempotence and metric accounting
- Test D: Stale, unknown, mismatched, and reordered ACKs
- Test E: Partition recovery and delivery blocking
- Test F: Seeded randomized loss across at least 50 reproducible trials
- Full-state mode compatibility
"""

import json
import random
import pytest

from crdts.g_counter import GCounter
from crdts.lww_map import LWWMap
from crdts.or_set import ORSet
from network.simulator import Message, NetworkSimulator


REPLICA_IDS = ["r1", "r2", "r3", "r4", "r5"]


def make_delta_network():
    return NetworkSimulator(REPLICA_IDS, sync_mode="delta")


def make_full_network():
    return NetworkSimulator(REPLICA_IDS, sync_mode="full")


# =====================================================================
# Test A: Happy-path acknowledged delta sync
# =====================================================================

def test_a_happy_path_acknowledged_delta_sync():
    """Verify happy-path acknowledged delta sync step-by-step across 5 replicas."""
    network = make_delta_network()

    # 1. Create independent updates while partitioned
    network.set_partitions([["r1", "r2"], ["r3", "r4", "r5"]])
    network.replicas["r1"].counter.increment(5)
    network.replicas["r1"].task_set.add("r1-emergency-task")
    network.replicas["r2"].settings.put("theme", "dark")
    network.replicas["r3"].counter.increment(2)
    network.replicas["r3"].task_set.add("r3-water-supply")

    # Broadcast reachable messages
    queued = network.broadcast_round()
    assert queued > 0

    # Verify data messages were queued
    r1_to_r2_data = [
        msg for msg in network.queue
        if msg.sender == "r1" and msg.receiver == "r2" and msg.msg_type == "data"
    ]
    assert len(r1_to_r2_data) == 1
    data_msg = r1_to_r2_data[0]
    assert data_msg.msg_id != ""

    # 4. Confirm sender's peer-known state has NOT advanced before delivery
    assert network.peer_known_states[("r1", "r2")].counter.value == 0
    assert "r1-emergency-task" not in network.peer_known_states[("r1", "r2")].task_set.values
    assert network.has_pending_delta("r1", "r2")

    # Deliver ONLY the data message from r1 to r2
    msg_index = network.queue.index(data_msg)
    assert network.deliver(msg_index) is True

    # 3. Confirm receiving replica merges incoming data
    assert network.replicas["r2"].counter.value >= 5
    assert "r1-emergency-task" in network.replicas["r2"].task_set.values

    # 4. Confirm sender's peer-known state STILL has not advanced (ACK not yet processed)
    assert network.peer_known_states[("r1", "r2")].counter.value == 0
    assert "r1-emergency-task" not in network.peer_known_states[("r1", "r2")].task_set.values
    assert network.has_pending_delta("r1", "r2")

    # Verify that an ACK was queued from r2 back to r1
    r2_to_r1_acks = [
        msg for msg in network.queue
        if msg.sender == "r2" and msg.receiver == "r1" and msg.msg_type == "ack"
    ]
    assert len(r2_to_r1_acks) == 1
    ack_msg = r2_to_r1_acks[0]
    assert ack_msg.ack_for == data_msg.msg_id

    # 5. Process the ACK and confirm only the acknowledged delta advances peer-known state
    ack_index = network.queue.index(ack_msg)
    assert network.deliver(ack_index) is True

    # Now peer-known state has advanced to reflect the acknowledged delta
    assert network.peer_known_states[("r1", "r2")].counter.value == 5
    assert "r1-emergency-task" in network.peer_known_states[("r1", "r2")].task_set.values
    assert not network.has_pending_delta("r1", "r2")

    # Deliver remaining reachable messages in the partitions
    network.deliver_all()

    # 6. Heal the network
    network.heal_partitions()

    # 7. Repeat synchronization until all replicas converge and queue is drained
    for _ in range(10):
        network.broadcast_round()
        network.deliver_all(random_order=True, seed=123)
        if network.all_replicas_converged() and not network.queue:
            break

    assert network.all_replicas_converged()
    assert len(network.queue) == 0
    assert network.all_deltas_acknowledged()

    for replica in network.replicas.values():
        assert replica.counter.value == 7
        assert "r1-emergency-task" in replica.task_set.values
        assert "r3-water-supply" in replica.task_set.values
        assert replica.settings.get("theme") == "dark"


# =====================================================================
# Test B: Lost ACK and retry
# =====================================================================

def test_b_lost_ack_and_retry():
    """Verify that a dropped ACK causes a safe retry preserving ID and payload."""
    network = make_delta_network()

    # 1. Queue a delta on r1
    network.replicas["r1"].task_set.add("critical-update")
    network.replicas["r1"].counter.increment(3)

    assert network.send_snapshot("r1", "r2") is True
    assert len(network.queue) == 1

    original_message = network.queue[0]
    original_id = original_message.msg_id
    original_payload = original_message.payload
    assert original_message.msg_type == "data"
    assert network.has_pending_delta("r1", "r2")

    # 2. Deliver it to the receiver r2
    assert network.deliver(0) is True
    assert "critical-update" in network.replicas["r2"].task_set.values
    assert network.replicas["r2"].counter.value == 3

    # An ACK is now queued
    assert len(network.queue) == 1
    assert network.queue[0].msg_type == "ack"
    assert network.queue[0].ack_for == original_id

    # 3. Drop its ACK before it reaches the sender
    dropped_ack = network.drop_first_ack()
    assert dropped_ack is not None
    assert len(network.queue) == 0

    # 4. Verify sender's peer-known state has not advanced
    assert network.peer_known_states[("r1", "r2")].counter.value == 0
    assert "critical-update" not in network.peer_known_states[("r1", "r2")].task_set.values
    assert network.has_pending_delta("r1", "r2")

    # 5. Run another broadcast round
    queued_count = network.broadcast_round()
    assert queued_count > 0

    # 6. Verify that the same logical delta is retried with the same message ID and payload
    retried_message = [
        msg for msg in network.queue
        if msg.sender == "r1" and msg.receiver == "r2" and msg.msg_type == "data"
    ][0]
    assert retried_message.msg_id == original_id
    assert retried_message.payload == original_payload

    # Avoid duplicate retries: subsequent broadcast when already in queue does not duplicate
    queued_again = network.broadcast_round()
    r1_r2_in_queue = [
        msg for msg in network.queue
        if msg.sender == "r1" and msg.receiver == "r2" and msg.msg_type == "data"
    ]
    assert len(r1_r2_in_queue) == 1

    # 7. Deliver the retry and its ACK
    network.deliver_all()
    assert len(network.queue) == 0

    # Confirm peer-known state has now advanced
    assert network.peer_known_states[("r1", "r2")].counter.value == 3
    assert "critical-update" in network.peer_known_states[("r1", "r2")].task_set.values
    assert not network.has_pending_delta("r1", "r2")

    # 8. Subsequent broadcast must not resend the acknowledged update
    network.broadcast_round()
    r1_to_r2_new = [
        msg for msg in network.queue
        if msg.sender == "r1" and msg.receiver == "r2"
    ]
    assert len(r1_to_r2_new) == 0


# =====================================================================
# Test C: Duplicate ACK
# =====================================================================

def test_c_duplicate_ack_handling():
    """Verify that duplicate ACKs are handled idempotently and metrics are accurate."""
    network = make_delta_network()

    network.replicas["r1"].counter.increment(4)
    network.send_snapshot("r1", "r2")

    # Deliver data message
    network.deliver(0)
    assert len(network.queue) == 1
    assert network.queue[0].msg_type == "ack"

    # Inject duplicate of the ACK
    network.duplicate_message(0)
    assert len(network.queue) == 2
    assert network.queue[0].msg_type == "ack"
    assert network.queue[1].msg_type == "ack"

    initial_ack_delivered = network.ack_messages_delivered
    initial_ack_bytes = network.ack_bytes_delivered

    # Deliver first ACK
    assert network.deliver(0) is True
    assert network.peer_known_states[("r1", "r2")].counter.value == 4
    assert not network.has_pending_delta("r1", "r2")
    known_state_after_first = network.peer_known_states[("r1", "r2")].snapshot()

    # Deliver duplicate ACK
    assert network.deliver(0) is True

    # Verify duplicate is safely ignored
    assert network.peer_known_states[("r1", "r2")].snapshot() == known_state_after_first
    assert not network.has_pending_delta("r1", "r2")

    # Verify ACK metrics
    assert network.ack_messages_delivered == initial_ack_delivered + 2
    assert network.ack_bytes_delivered > initial_ack_bytes


# =====================================================================
# Test D: Stale, unknown, mismatched, and reordered ACKs
# =====================================================================

def test_d_stale_unknown_and_mismatched_acks():
    """Verify that unknown, mismatched, stale, and reordered ACKs cannot corrupt state."""
    network = make_delta_network()

    # Prepare pending delta from r1 to r2
    network.replicas["r1"].counter.increment(2)
    network.send_snapshot("r1", "r2")
    msg1_id = network.queue[0].msg_id

    # 1. Unknown message ID
    bogus_ack = Message(
        sender="r2",
        receiver="r1",
        payload=json.dumps({"type": "ack", "ack_for": "unknown-id-12345"}),
        msg_id="ack-bogus",
        msg_type="ack",
        ack_for="unknown-id-12345",
    )
    network.queue.append(bogus_ack)
    # Deliver the bogus ACK
    bogus_idx = network.queue.index(bogus_ack)
    assert network.deliver(bogus_idx) is True
    # Verify pending delta and known state are untouched
    assert network.has_pending_delta("r1", "r2")
    assert network.pending_deltas[("r1", "r2")].msg_id == msg1_id
    assert network.peer_known_states[("r1", "r2")].counter.value == 0

    # 2. ACK from wrong peer (claiming to be from r3 instead of r2)
    wrong_peer_ack = Message(
        sender="r3",
        receiver="r1",
        payload=json.dumps({"type": "ack", "ack_for": msg1_id}),
        msg_id="ack-wrong-peer",
        msg_type="ack",
        ack_for=msg1_id,
    )
    network.queue.append(wrong_peer_ack)
    wrong_idx = network.queue.index(wrong_peer_ack)
    assert network.deliver(wrong_idx) is True
    # Must NOT clear r1->r2 pending delta
    assert network.has_pending_delta("r1", "r2")
    assert network.peer_known_states[("r1", "r2")].counter.value == 0

    # 3. Deliver valid data message and process its valid ACK
    # Deliver data message
    data_idx = [i for i, m in enumerate(network.queue) if m.msg_id == msg1_id][0]
    network.deliver(data_idx)
    # Deliver real ACK
    ack_idx = [i for i, m in enumerate(network.queue) if m.msg_type == "ack" and m.ack_for == msg1_id][0]
    network.deliver(ack_idx)
    assert network.peer_known_states[("r1", "r2")].counter.value == 2
    assert not network.has_pending_delta("r1", "r2")

    # 4. Stale ACK arriving after delta is already acknowledged
    stale_ack = Message(
        sender="r2",
        receiver="r1",
        payload=json.dumps({"type": "ack", "ack_for": msg1_id}),
        msg_id="ack-stale",
        msg_type="ack",
        ack_for=msg1_id,
    )
    network.queue.append(stale_ack)
    stale_idx = network.queue.index(stale_ack)
    assert network.deliver(stale_idx) is True
    # State remains 2, not regressed or duplicated
    assert network.peer_known_states[("r1", "r2")].counter.value == 2

    # 5. Older ACK arriving while a newer delta is pending
    # Now r1 makes a second update
    network.replicas["r1"].counter.increment(5)
    network.send_snapshot("r1", "r2")
    msg2_id = [m for m in network.queue if m.msg_type == "data"][0].msg_id
    assert msg2_id != msg1_id
    assert network.has_pending_delta("r1", "r2")

    # Old ACK for msg1 arrives while msg2 is pending
    network.queue.append(stale_ack)
    stale_idx2 = network.queue.index(stale_ack)
    assert network.deliver(stale_idx2) is True

    # Crucial check: acknowledging older delta must NOT clear newer pending delta
    assert network.has_pending_delta("r1", "r2")
    assert network.pending_deltas[("r1", "r2")].msg_id == msg2_id
    # Peer-known state must STILL be 2, not 7!
    assert network.peer_known_states[("r1", "r2")].counter.value == 2

    # 6. Verify application CRDT state is never mutated by ACKs
    app_counter_before = network.replicas["r1"].counter.value
    dummy_ack = Message(
        sender="r2",
        receiver="r1",
        payload=json.dumps({"type": "ack", "ack_for": "any-ack"}),
        msg_id="ack-dummy",
        msg_type="ack",
        ack_for="any-ack",
    )
    network.queue.append(dummy_ack)
    network.deliver(network.queue.index(dummy_ack))
    assert network.replicas["r1"].counter.value == app_counter_before


# =====================================================================
# Test E: Partition recovery
# =====================================================================

def test_e_partition_blocks_both_data_and_acks_then_recovers():
    """Verify partitions block data and ACKs equally, and healing restores full convergence."""
    network = make_delta_network()

    network.replicas["r1"].counter.increment(10)
    network.replicas["r4"].counter.increment(20)

    # Queue updates while connected
    network.broadcast_round()
    assert len(network.queue) > 0

    # Deliver data message from r1 to r2 to create an in-flight ACK
    r1_to_r2_idx = [i for i, m in enumerate(network.queue) if m.sender == "r1" and m.receiver == "r2"][0]
    network.deliver(r1_to_r2_idx)
    # Now there is an ACK from r2 to r1 in the queue

    # Now partition the network: group 1 [r1, r3], group 2 [r2, r4, r5]
    # This separates r1 from r2 (blocking r2->r1 ACK) and separates r1 from r4 (blocking r4->r1 data)
    network.set_partitions([["r1", "r3"], ["r2", "r4", "r5"]])

    # Verify cross-partition delivery of both data and ACK is blocked
    cross_partition_msgs = [
        (idx, m) for idx, m in enumerate(network.queue)
        if network._groups[m.sender] != network._groups[m.receiver]
    ]
    assert len(cross_partition_msgs) > 0

    # Delivery of any cross-partition message fails and leaves message in queue
    blocked_idx = cross_partition_msgs[0][0]
    blocked_msg = cross_partition_msgs[0][1]
    assert network.deliver(blocked_idx) is False
    assert network.queue[blocked_idx] == blocked_msg

    # Deliver all reachable within current partitions
    network.deliver_all()
    assert not network.all_replicas_converged()
    # Cross-partition messages remain in queue
    assert len(network.queue) > 0

    # Now heal the network
    network.heal_partitions()

    # Deliver all pending messages that were unblocked
    network.deliver_all()

    # Run anti-entropy rounds until complete convergence
    for _ in range(15):
        network.broadcast_round()
        network.deliver_all(random_order=True, seed=999)
        if network.all_replicas_converged() and not network.queue:
            break

    assert network.all_replicas_converged()
    assert len(network.queue) == 0
    assert network.all_deltas_acknowledged()

    for replica in network.replicas.values():
        assert replica.counter.value == 30


# =====================================================================
# Test F: Seeded randomized loss (50+ trials)
# =====================================================================

def test_f_seeded_randomized_loss_validation():
    """Execute at least 50 reproducible trials under random packet loss of data and ACKs."""
    total_trials = 60
    base_seed = 30000
    recovery_round_limit = 100

    converged_trials = 0
    failed_trials = []
    worst_recovery_rounds = 0

    for trial_idx in range(total_trials):
        seed = base_seed + trial_idx
        rng = random.Random(seed)

        network = NetworkSimulator(REPLICA_IDS, sync_mode="delta")

        # Partition network into two groups
        shuffled = REPLICA_IDS.copy()
        rng.shuffle(shuffled)
        split = rng.randint(1, len(shuffled) - 1)
        network.set_partitions([shuffled[:split], shuffled[split:]])

        # Generate updates on each replica
        for rid, replica in network.replicas.items():
            replica.counter.increment(rng.randint(1, 3))
            replica.task_set.add(f"task-{rid}-{seed % 100}")
            replica.settings.put("status", rng.choice(["online", "busy"]))

        # Phase 1: Disconnected operation with 15% packet loss
        for phase in range(2):
            network.broadcast_round()
            network.drop_random_messages(p_data=0.15, p_ack=0.15, rng=rng)
            network.deliver_all(random_order=True, seed=seed * 10 + phase)

        # Phase 2: Heal partitions and recover under 15% random loss
        network.heal_partitions()
        trial_rounds = 0

        for r_num in range(recovery_round_limit):
            trial_rounds += 1
            network.broadcast_round()
            # Drop data and ACK packets probabilistically
            network.drop_random_messages(p_data=0.15, p_ack=0.15, rng=rng)
            network.deliver_all(random_order=True, seed=seed * 100 + r_num)

            if (
                network.all_replicas_converged()
                and not network.queue
                and network.all_deltas_acknowledged()
            ):
                break

        if (
            network.all_replicas_converged()
            and not network.queue
            and network.all_deltas_acknowledged()
        ):
            converged_trials += 1
            if trial_rounds > worst_recovery_rounds:
                worst_recovery_rounds = trial_rounds
        else:
            failed_trials.append({
                "seed": seed,
                "rounds": trial_rounds,
                "queue_len": len(network.queue),
                "pending": network.pending_delta_count(),
            })

    assert len(failed_trials) == 0, f"Failed trials: {failed_trials}"
    assert converged_trials == total_trials
    assert worst_recovery_rounds < recovery_round_limit

    # Also verify full-state mode retains its expected baseline behavior
    full_net = make_full_network()
    full_net.replicas["r1"].counter.increment(5)
    full_net.broadcast_round()
    full_net.deliver_all()
    assert full_net.all_replicas_converged()
    assert full_net.metrics["ack_messages_sent"] == 0
    assert full_net.metrics["ack_messages_delivered"] == 0


# =====================================================================
# Additional invariants and edge cases
# =====================================================================

def test_full_state_mode_does_not_use_acks():
    """Verify that full-state mode does not send ACKs or use pending deltas."""
    network = make_full_network()

    network.replicas["r1"].counter.increment(5)
    queued = network.broadcast_round()
    assert queued == 20
    assert all(m.msg_type == "data" for m in network.queue)

    delivered = network.deliver_all()
    assert delivered == 20
    assert network.all_replicas_converged()

    metrics = network.metrics
    assert metrics["messages_sent"] == 20
    assert metrics["messages_delivered"] == 20
    assert metrics["ack_messages_sent"] == 0
    assert metrics["ack_messages_delivered"] == 0
    assert metrics["ack_bytes_transmitted"] == 0
    assert metrics["ack_bytes_delivered"] == 0
    assert len(network.pending_deltas) == 0


def test_metrics_api_completeness_and_separation():
    """Verify that ACK metrics are strictly separated from data message metrics."""
    network = make_delta_network()

    network.replicas["r1"].counter.increment(1)
    network.broadcast_round()

    # 4 data messages queued
    assert network.messages_sent == 4
    assert network.bytes_transmitted > 0
    assert network.ack_messages_sent == 0
    assert network.ack_bytes_transmitted == 0

    # Deliver first data message
    network.deliver(0)
    assert network.messages_delivered == 1
    assert network.bytes_delivered > 0
    assert network.ack_messages_sent == 1
    assert network.ack_bytes_transmitted > 0
    assert network.ack_messages_delivered == 0

    # Deliver the remaining messages (including newly generated ACKs)
    network.deliver_all()
    assert network.messages_delivered == 4
    assert network.ack_messages_delivered == 4
    assert network.ack_bytes_delivered == network.ack_bytes_transmitted
