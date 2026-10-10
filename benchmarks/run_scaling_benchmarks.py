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


def derive_seed(seed: int, phase: int) -> int:
    return seed * 1_000_003 + phase * 97 + 13


def create_partition(replica_ids: list[str], rng: random.Random) -> list[list[str]]:
    shuffled = replica_ids.copy()
    rng.shuffle(shuffled)
    split = rng.randint(1, len(shuffled) - 1)
    return [shuffled[:split], shuffled[split:]]


def generate_updates(
    network: NetworkSimulator,
    rng: random.Random,
    items_per_replica: int,
) -> None:
    """Create equivalent seeded workloads at different dataset sizes."""
    for replica_id, replica in network.replicas.items():
        replica.counter.increment(rng.randint(1, 5))
        replica.task_set.add("shared-task")
        if rng.random() < 0.4:
            replica.task_set.remove("shared-task")

        for index in range(items_per_replica):
            value = f"task-{replica_id}-{index}"
            replica.task_set.add(value)
            if rng.random() < 0.2:
                replica.task_set.remove(value)

        replica.settings.put("theme", rng.choice(["light", "dark"]))
        replica.settings.put("status", rng.choice(["idle", "busy", "offline"]))


def duplicate_random_messages(
    network: NetworkSimulator,
    rng: random.Random,
    probability: float = 0.15,
) -> None:
    original_count = len(network.queue)
    for index in range(original_count):
        if rng.random() < probability:
            network.duplicate_message(index)


def run_trial(seed: int, sync_mode: str, items_per_replica: int) -> dict[str, Any]:
    scenario_rng = random.Random(seed)
    network = NetworkSimulator(REPLICA_IDS, sync_mode=sync_mode)

    network.set_partitions(create_partition(REPLICA_IDS, scenario_rng))
    generate_updates(network, scenario_rng, items_per_replica)

    network.broadcast_round()
    duplicate_random_messages(
        network, random.Random(derive_seed(seed, 0))
    )
    network.deliver_all(random_order=True, seed=derive_seed(seed, 1))

    network.heal_partitions()
    rounds_after_heal = 0
    for round_number in range(20):
        rounds_after_heal = round_number + 1
        network.broadcast_round()
        duplicate_random_messages(
            network,
            random.Random(derive_seed(seed, 10 + round_number)),
        )
        network.deliver_all(
            random_order=True,
            seed=derive_seed(seed, 100 + round_number),
        )
        if network.all_replicas_converged() and not network.queue:
            break

    metrics = network.metrics
    logical_converged = network.all_replicas_converged()
    quiescent = logical_converged and not network.queue
    return {
        "seed": seed,
        "items_per_replica": items_per_replica,
        "converged": quiescent,
        "logical_state_converged": logical_converged,
        "fully_quiescent": quiescent,
        "rounds_after_heal": rounds_after_heal,
        "messages_sent": metrics["messages_sent"],
        "messages_delivered": metrics["messages_delivered"],
        "payload_bytes_sent": metrics["bytes_transmitted"],
        "payload_bytes_delivered": metrics["bytes_delivered"],
        "merge_latencies_ms": list(network.merge_latencies_ms),
        "queued_at_end": len(network.queue),
    }


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def summarise(results: list[dict[str, Any]], mode: str, size: int) -> dict[str, Any]:
    latencies = [lat for result in results for lat in result["merge_latencies_ms"]]
    converged = sum(result["converged"] for result in results)
    return {
        "mode": mode,
        "items_per_replica": size,
        "trials": len(results),
        "converged_trials": converged,
        "convergence_rate_percent": 100 * converged / len(results),
        "average_messages_sent_per_trial": statistics.mean(r["messages_sent"] for r in results),
        "average_messages_delivered_per_trial": statistics.mean(r["messages_delivered"] for r in results),
        "average_payload_bytes_sent_per_trial": statistics.mean(r["payload_bytes_sent"] for r in results),
        "average_payload_bytes_delivered_per_trial": statistics.mean(r["payload_bytes_delivered"] for r in results),
        "average_merge_latency_ms": statistics.mean(latencies) if latencies else 0.0,
        "p95_merge_latency_ms": percentile(latencies, 0.95),
        "results": results,
    }


def main() -> None:
    # 20 paired trials per size keeps the scaling experiment tractable.
    # Increase TRIALS after confirming runtime and behaviour on your machine.
    trials = 20
    base_seed = 4200
    sizes = [10, 100, 500]
    summaries: list[dict[str, Any]] = []

    print("PS-39 scaling benchmark: full-state vs delta")
    print(f"Replicas: {len(REPLICA_IDS)} | trials per size/mode: {trials}")
    print(f"Items per replica: {sizes}\n")

    for size in sizes:
        mode_reports = {}
        for mode in ("full", "delta"):
            results = [
                run_trial(base_seed + size * 10_000 + i, mode, size)
                for i in range(trials)
            ]
            report = summarise(results, mode, size)
            mode_reports[mode] = report
            summaries.append(report)
            print(
                f"size={size:>3} mode={mode:<5} "
                f"convergence={report['convergence_rate_percent']:6.2f}% "
                f"sent_bytes/trial={report['average_payload_bytes_sent_per_trial']:12.2f} "
                f"messages_sent/trial={report['average_messages_sent_per_trial']:7.2f} "
                f"avg_merge_ms={report['average_merge_latency_ms']:.6f} "
                f"p95_merge_ms={report['p95_merge_latency_ms']:.6f}"
            )

        full_bytes = mode_reports["full"]["average_payload_bytes_sent_per_trial"]
        delta_bytes = mode_reports["delta"]["average_payload_bytes_sent_per_trial"]
        reduction = (full_bytes - delta_bytes) / full_bytes * 100 if full_bytes else 0.0
        print(f"  payload reduction at size {size}: {reduction:.2f}%\n")

    report = {
        "experiment": {
            "trials_per_size_and_mode": trials,
            "base_seed": base_seed,
            "replica_count": len(REPLICA_IDS),
            "items_per_replica": sizes,
            "notes": [
                "Both synchronisation modes use the same workload seed for each trial.",
                "Network delivery and duplicate schedules are reproducible but can differ as modes queue different messages.",
                "Payload bytes are serialised message payload bytes; protocol headers are not included.",
                "Merge latency measures local state-merge processing, not end-to-end convergence time.",
            ],
        },
        "results": summaries,
    }
    output_path = ROOT / "benchmarks" / "scaling_results.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Detailed results saved to: {output_path}")


if __name__ == "__main__":
    main()
