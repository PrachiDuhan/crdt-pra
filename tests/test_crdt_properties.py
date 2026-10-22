"""Generated property tests for CRDT merge laws and partition recovery.

Hypothesis explores many generated states/sequences. This is strong empirical
property testing, not a mathematical proof over all possible executions.
"""

import random

from hypothesis import given, settings, strategies as st

from crdts.g_counter import GCounter
from crdts.lww_map import LWWMap
from crdts.or_set import ORSet
from network.simulator import NetworkSimulator

REPLICA_IDS = ["r1", "r2", "r3", "r4", "r5"]
KEYS = st.sampled_from(["alpha", "beta", "gamma"])
VALUES = st.sampled_from(["red", "blue", "green", "done"])
COUNTER_STATES = st.dictionaries(
    keys=st.sampled_from(["r1", "r2", "r3", "r4", "r5"]),
    values=st.integers(min_value=1, max_value=100),
    max_size=5,
)
ORSET_OPERATIONS = st.lists(
    st.tuples(st.sampled_from(["add", "remove"]), KEYS),
    max_size=30,
)
MAP_OPERATIONS = st.lists(
    st.tuples(st.sampled_from(["put", "remove"]), KEYS, VALUES),
    max_size=30,
)
NETWORK_OPERATIONS = st.lists(
    st.tuples(
        st.sampled_from(REPLICA_IDS),
        st.sampled_from(["increment", "add", "remove", "put", "delete"]),
        KEYS,
        VALUES,
        st.integers(min_value=1, max_value=5),
    ),
    max_size=60,
)


def merge_counter_states(*states):
    result = GCounter("merge-result")
    for state in states:
        result.merge_state(state)
    return result.state


def build_orset(replica_id, operations):
    result = ORSet(replica_id)
    for action, value in operations:
        if action == "add":
            result.add(value)
        else:
            result.remove(value)
    return result


def merge_orset_states(*states):
    result = ORSet("merge-result")
    for state in states:
        result.merge_state(state)
    return result.state


def build_lwwmap(replica_id, operations):
    result = LWWMap(replica_id)
    for action, key, value in operations:
        if action == "put":
            result.put(key, value)
        else:
            result.remove(key)
    return result


def merge_lwwmap_states(*states):
    result = LWWMap("merge-result")
    for state in states:
        result.merge_state(state)
    return result.state


def apply_operations(network, operations):
    for replica_id, action, key, value, amount in operations:
        replica = network.replicas[replica_id]
        if action == "increment":
            replica.counter.increment(amount)
        elif action == "add":
            replica.task_set.add(key)
        elif action == "remove":
            replica.task_set.remove(key)
        elif action == "put":
            replica.settings.put(key, value)
        else:
            replica.settings.remove(key)


def run_network_phase(network, seed, allow_drops=False):
    """Broadcast and deliver with reproducible reorder/duplicate/drop faults."""
    network.broadcast_round()
    original_count = len(network.queue)

    duplicate_rng = random.Random(seed + 17)
    for index in range(original_count):
        if duplicate_rng.random() < 0.15:
            network.duplicate_message(index)

    if allow_drops:
        drop_rng = random.Random(seed + 31)
        network.queue = [
            message
            for message in network.queue
            if drop_rng.random() >= 0.10
        ]

    network.deliver_all(random_order=True, seed=seed + 53)


@given(a=COUNTER_STATES, b=COUNTER_STATES, c=COUNTER_STATES)
@settings(max_examples=100, deadline=None)
def test_gcounter_merge_is_idempotent_commutative_and_associative(a, b, c):
    assert merge_counter_states(a, a) == merge_counter_states(a)
    assert merge_counter_states(a, b) == merge_counter_states(b, a)
    assert merge_counter_states(merge_counter_states(a, b), c) == (
        merge_counter_states(a, merge_counter_states(b, c))
    )


@given(a_ops=ORSET_OPERATIONS, b_ops=ORSET_OPERATIONS, c_ops=ORSET_OPERATIONS)
@settings(max_examples=100, deadline=None)
def test_orset_merge_is_idempotent_commutative_and_associative(
    a_ops, b_ops, c_ops
):
    a = build_orset("r1", a_ops).state
    b = build_orset("r2", b_ops).state
    c = build_orset("r3", c_ops).state

    assert merge_orset_states(a, a) == merge_orset_states(a)
    assert merge_orset_states(a, b) == merge_orset_states(b, a)
    assert merge_orset_states(merge_orset_states(a, b), c) == (
        merge_orset_states(a, merge_orset_states(b, c))
    )


@given(a_ops=MAP_OPERATIONS, b_ops=MAP_OPERATIONS, c_ops=MAP_OPERATIONS)
@settings(max_examples=100, deadline=None)
def test_lwwmap_merge_is_idempotent_commutative_and_associative(
    a_ops, b_ops, c_ops
):
    a = build_lwwmap("r1", a_ops).state
    b = build_lwwmap("r2", b_ops).state
    c = build_lwwmap("r3", c_ops).state

    assert merge_lwwmap_states(a, a) == merge_lwwmap_states(a)
    assert merge_lwwmap_states(a, b) == merge_lwwmap_states(b, a)
    assert merge_lwwmap_states(merge_lwwmap_states(a, b), c) == (
        merge_lwwmap_states(a, merge_lwwmap_states(b, c))
    )


@given(
    seed=st.integers(min_value=0, max_value=1_000_000),
    operations=NETWORK_OPERATIONS,
    sync_mode=st.sampled_from(["full", "delta"]),
)
@settings(max_examples=60, deadline=None)
def test_generated_partition_schedules_converge(seed, operations, sync_mode):
    rng = random.Random(seed)
    network = NetworkSimulator(REPLICA_IDS, sync_mode=sync_mode)

    shuffled = REPLICA_IDS.copy()
    rng.shuffle(shuffled)
    split = rng.randint(1, len(shuffled) - 1)
    network.set_partitions([shuffled[:split], shuffled[split:]])

    apply_operations(network, operations)

    # Exercise independent updates while replicas are partitioned.
    for phase in range(3):
        run_network_phase(network, seed * 100 + phase, allow_drops=True)

    network.heal_partitions()
    for phase in range(20):
        run_network_phase(network, seed * 1000 + phase, allow_drops=True)
        if network.all_replicas_converged() and not network.queue:
            break

    assert network.all_replicas_converged(), (
        f"mode={sync_mode}, seed={seed}, operations={operations!r}"
    )
