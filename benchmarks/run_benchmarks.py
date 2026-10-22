from __future__ import annotations

import json
import math
import random
import statistics
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from network.simulator import NetworkSimulator

REPLICA_IDS = ["r1", "r2", "r3", "r4", "r5"]


def create_partition(
    replica_ids: list[str],
    rng: random.Random,
) -> list[list[str]]:
    """Split replicas into two non-empty disconnected groups."""
    shuffled = replica_ids.copy()
    rng.shuffle(shuffled)
    split = rng.randint(1, len(shuffled) - 1)
    return [shuffled[:split], shuffled[split:]]


def generate_updates(
    network: NetworkSimulator,
    rng: random.Random,
) -> None:
    """Apply the same seeded workload independently to each replica."""
    for replica_id, replica in network.replicas.items():
        replica.counter.increment(rng.randint(1, 5))

        replica.task_set.add("shared-task")
        if rng.random() < 0.4:
            replica.task_set.remove("shared-task")

        for index in range(rng.randint(1, 3)):
            task = f"task-{replica_id}-{index}"
            replica.task_set.add(task)
            if rng.random() < 0.2:
                replica.task_set.remove(task)

        replica.settings.put("theme", rng.choice(["light", "dark"]))
        replica.settings.put(
            "status",
            rng.choice(["idle", "busy", "offline"]),
        )


def duplicate_random_messages(
    network: NetworkSimulator,
    rng: random.Random,
    probability: float = 0.15,
) -> None:
    """Inject duplicate copies of selected queued messages."""
    original_count = len(network.queue)
    for index in range(original_count):
        if rng.random() < probability:
            network.duplicate_message(index)


def phase_seed(trial_seed: int, phase: int) -> int:
    """Derive independent reproducible seeds for each delivery phase."""
    return trial_seed * 1_000_003 + phase * 97 + 13


def run_trial(seed: int, sync_mode: str) -> dict[str, Any]:
    """Run one randomised partition-and-recovery scenario."""
    scenario_rng = random.Random(seed)
    replica_ids = REPLICA_IDS.copy()
    network = NetworkSimulator(replica_ids, sync_mode=sync_mode)

    # Both modes see the same partition and local update workload for a seed.
    network.set_partitions(create_partition(replica_ids, scenario_rng))
    generate_updates(network, scenario_rng)

    # Synchronise only within the disconnected groups.
    network.broadcast_round()
    duplicate_random_messages(
        network,
        random.Random(phase_seed(seed, 0)),
    )
    network.deliver_all(random_order=True, seed=phase_seed(seed, 1))

    # Reconnect and repeat anti-entropy rounds until convergence or timeout.
    network.heal_partitions()
    rounds_after_heal = 0
    for round_number in range(20):
        rounds_after_heal = round_number + 1
        network.broadcast_round()
        duplicate_random_messages(
            network,
            random.Random(phase_seed(seed, 10 + round_number)),
        )
        network.deliver_all(
            random_order=True,
            seed=phase_seed(seed, 100 + round_number),
        )

        if network.all_replicas_converged() and not network.queue:
            break

    metrics = network.metrics
    logical_state_converged = network.all_replicas_converged()
    fully_quiescent = logical_state_converged and not network.queue
    return {
        "seed": seed,
        "converged": fully_quiescent,
        "logical_state_converged": logical_state_converged,
        "fully_quiescent": fully_quiescent,
        "rounds_after_heal": rounds_after_heal,
        "messages_sent": metrics["messages_sent"],
        "messages_delivered": metrics["messages_delivered"],
        "payload_bytes_sent": metrics["bytes_transmitted"],
        "payload_bytes_delivered": metrics["bytes_delivered"],
        "merge_latencies_ms": list(network.merge_latencies_ms),
        "queued_at_end": len(network.queue),
    }


def nearest_rank_percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def run_benchmarks(
    trials: int = 100,
    base_seed: int = 42,
    sync_mode: str = "full",
) -> dict[str, Any]:
    if trials < 1:
        raise ValueError("trials must be at least 1")
    if sync_mode not in {"full", "delta"}:
        raise ValueError("sync_mode must be 'full' or 'delta'")

    results = [
        run_trial(base_seed + index, sync_mode)
        for index in range(trials)
    ]
    latencies = [
        latency
        for result in results
        for latency in result["merge_latencies_ms"]
    ]
    converged_trials = sum(result["converged"] for result in results)

    return {
        "mode": sync_mode,
        "trials": trials,
        "converged_trials": converged_trials,
        "convergence_rate_percent": 100 * converged_trials / trials,
        "average_messages_sent_per_trial": statistics.mean(
            result["messages_sent"] for result in results
        ),
        "average_messages_per_trial": statistics.mean(
            result["messages_delivered"] for result in results
        ),
        # Kept for compatibility with the existing dashboard: this is bytes
        # transmitted when queued, not just bytes successfully delivered.
        "average_payload_bytes_per_trial": statistics.mean(
            result["payload_bytes_sent"] for result in results
        ),
        "average_payload_bytes_delivered_per_trial": statistics.mean(
            result["payload_bytes_delivered"] for result in results
        ),
        "average_merge_latency_ms": (
            statistics.mean(latencies) if latencies else 0.0
        ),
        "p95_merge_latency_ms": nearest_rank_percentile(latencies, 0.95),
        "results": results,
    }


def print_report(report: dict[str, Any]) -> None:
    print(f"\nMode: {report['mode']}")
    print("-" * 38)
    print(f"Trials: {report['trials']}")
    print(
        f"Converged: {report['converged_trials']}/"
        f"{report['trials']}"
    )
    print(f"Convergence rate: {report['convergence_rate_percent']:.2f}%")
    print(
        "Average messages sent/trial: "
        f"{report['average_messages_sent_per_trial']:.2f}"
    )
    print(
        "Average messages delivered/trial: "
        f"{report['average_messages_per_trial']:.2f}"
    )
    print(
        "Average payload bytes transmitted/trial: "
        f"{report['average_payload_bytes_per_trial']:.2f}"
    )
    print(
        "Average payload bytes delivered/trial: "
        f"{report['average_payload_bytes_delivered_per_trial']:.2f}"
    )
    print(
        "Average merge latency: "
        f"{report['average_merge_latency_ms']:.6f} ms"
    )
    print(f"P95 merge latency: {report['p95_merge_latency_ms']:.6f} ms")


def main() -> None:
    trials = 100
    base_seed = 42

    full = run_benchmarks(
        trials=trials,
        base_seed=base_seed,
        sync_mode="full",
    )
    delta = run_benchmarks(
        trials=trials,
        base_seed=base_seed,
        sync_mode="delta",
    )

    print("\nPS-39: Full-State vs Delta Synchronisation")
    print_report(full)
    print_report(delta)

    full_bytes = full["average_payload_bytes_per_trial"]
    delta_bytes = delta["average_payload_bytes_per_trial"]
    if full_bytes > 0:
        reduction = (full_bytes - delta_bytes) / full_bytes * 100
        print(f"\nPayload bytes transmitted reduction: {reduction:.2f}%")
    else:
        print("\nPayload reduction: not measurable")

    report = {
        "experiment": {
            "trials": trials,
            "base_seed": base_seed,
            "replicas": len(REPLICA_IDS),
            "workload": "seeded partition, concurrent updates, duplicates, recovery",
        },
        "full_state": full,
        "delta": delta,
    }
    output_path = ROOT / "benchmarks" / "comparison_results.json"
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)
    print(f"\nReport saved to: {output_path}")


if __name__ == "__main__":
    main()
