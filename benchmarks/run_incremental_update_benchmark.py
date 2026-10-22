from __future__ import annotations

import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from network.simulator import NetworkSimulator  # noqa: E402

REPLICA_IDS = ["r1", "r2", "r3", "r4", "r5"]
ITEM_SIZES = [10, 100, 500]
DEFAULT_TRIALS = 20
BASE_SEED = 5000


def nearest_rank_percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def converge_baseline(network: NetworkSimulator, seed: int, max_rounds: int = 100) -> int:
    """Synchronise the preloaded baseline, including delta peer-knowledge."""
    rounds = 0
    for round_index in range(max_rounds):
        queued = network.broadcast_round()
        network.deliver_all(random_order=True, seed=seed + round_index)
        rounds += 1

        if network.sync_mode == "delta":
            # A zero-queue broadcast proves all sender/receiver delta records
            # currently have nothing new to send.
            if queued == 0 and not network.queue and network.all_replicas_converged():
                return rounds
        elif network.all_replicas_converged() and not network.queue:
            return rounds

    raise RuntimeError(
        f"Baseline did not reach quiescence in mode={network.sync_mode} "
        f"after {max_rounds} rounds"
    )


def reset_measurement_counters(network: NetworkSimulator) -> None:
    """Reset counters after the baseline, retaining CRDT and peer-known state."""
    if network.queue:
        raise RuntimeError("Cannot reset measurements while messages are queued")
    network.messages_sent = 0
    network.messages_delivered = 0
    network.bytes_transmitted = 0
    network.bytes_delivered = 0
    network.merge_latencies_ms.clear()


def run_trial(item_count: int, mode: str, trial_seed: int) -> dict[str, Any]:
    """Measure the cost of propagating one small update after initial sync."""
    network = NetworkSimulator(REPLICA_IDS, sync_mode=mode)

    # Seed a sizable shared dataset on r1, then synchronise it to every replica.
    source = network.replicas["r1"]
    for index in range(item_count):
        source.task_set.add(f"baseline-item-{index:05d}")

    baseline_rounds = converge_baseline(network, seed=trial_seed * 100)
    if not network.all_replicas_converged() or network.queue:
        raise RuntimeError("Baseline is not fully converged and quiescent")

    reset_measurement_counters(network)

    # Only one new task is introduced after the replicas are already synced.
    update_value = f"incremental-update-{trial_seed}"
    network.replicas["r1"].task_set.add(update_value)

    queued = network.broadcast_round()
    delivered = network.deliver_all(
        random_order=True,
        seed=trial_seed * 100 + 77,
    )

    converged = network.all_replicas_converged() and not network.queue
    if converged:
        for replica in network.replicas.values():
            if update_value not in replica.task_set.values:
                converged = False
                break

    latencies = list(network.merge_latencies_ms)
    return {
        "seed": trial_seed,
        "item_count": item_count,
        "mode": mode,
        "baseline_rounds": baseline_rounds,
        "messages_queued_for_update": queued,
        "messages_delivered_for_update": delivered,
        "messages_sent_for_update": network.messages_sent,
        "payload_bytes_transmitted": network.bytes_transmitted,
        "payload_bytes_delivered": network.bytes_delivered,
        "average_merge_latency_ms": (
            statistics.mean(latencies) if latencies else 0.0
        ),
        "p95_merge_latency_ms": nearest_rank_percentile(latencies, 0.95),
        "converged_after_one_broadcast_round": converged,
        "queue_empty_after_round": not network.queue,
    }


def summarise(rows: list[dict[str, Any]], item_count: int, mode: str) -> dict[str, Any]:
    group = [
        row for row in rows
        if row["item_count"] == item_count and row["mode"] == mode
    ]
    n = len(group)
    converged_count = sum(row["converged_after_one_broadcast_round"] for row in group)
    all_latencies = [
        row["average_merge_latency_ms"]
        for row in group
    ]
    return {
        "item_count": item_count,
        "mode": mode,
        "trials": n,
        "converged_trials": converged_count,
        "convergence_rate_percent": (100.0 * converged_count / n) if n else 0.0,
        "average_messages_sent": statistics.mean(row["messages_sent_for_update"] for row in group) if n else 0.0,
        "average_messages_delivered": statistics.mean(row["messages_delivered_for_update"] for row in group) if n else 0.0,
        "average_payload_bytes_transmitted": statistics.mean(row["payload_bytes_transmitted"] for row in group) if n else 0.0,
        "average_payload_bytes_delivered": statistics.mean(row["payload_bytes_delivered"] for row in group) if n else 0.0,
        "average_merge_latency_ms": statistics.mean(all_latencies) if n else 0.0,
        "p95_trial_average_merge_latency_ms": nearest_rank_percentile(all_latencies, 0.95),
    }


def main() -> None:
    trials = DEFAULT_TRIALS
    rows: list[dict[str, Any]] = []

    print("PS-39 steady-state incremental-update benchmark")
    print(f"Replicas: {len(REPLICA_IDS)} | trials per size/mode: {trials}")
    print(f"Baseline OR-Set items: {ITEM_SIZES}")
    print("Measured phase: one new task added on r1 after initial convergence")

    for item_count in ITEM_SIZES:
        mode_summaries = {}
        for mode in ("full", "delta"):
            for trial_index in range(trials):
                seed = BASE_SEED + item_count * 1000 + trial_index
                rows.append(run_trial(item_count, mode, seed))
            summary = summarise(rows, item_count, mode)
            mode_summaries[mode] = summary
            print(
                f"size={item_count:3d} mode={mode:5s} "
                f"converged={summary['convergence_rate_percent']:6.2f}% "
                f"sent_bytes/update={summary['average_payload_bytes_transmitted']:12.2f} "
                f"messages_sent/update={summary['average_messages_sent']:6.2f} "
                f"avg_merge_ms={summary['average_merge_latency_ms']:.6f} "
                f"p95_trial_avg_ms={summary['p95_trial_average_merge_latency_ms']:.6f}"
            )

        full_bytes = mode_summaries["full"]["average_payload_bytes_transmitted"]
        delta_bytes = mode_summaries["delta"]["average_payload_bytes_transmitted"]
        reduction = (full_bytes - delta_bytes) / full_bytes * 100 if full_bytes else 0.0
        print(f"  payload reduction after one small update: {reduction:.2f}%\n")

    report = {
        "experiment": {
            "name": "steady-state incremental update propagation",
            "replica_count": len(REPLICA_IDS),
            "trials_per_size_mode": trials,
            "base_seed": BASE_SEED,
            "item_sizes": ITEM_SIZES,
            "setup": "r1 seeds baseline OR-Set items; replicas synchronise to quiescence; then r1 adds one new task",
            "measurement_window": "only the single update broadcast/delivery round after baseline metrics are reset",
            "payload_definition": "UTF-8 bytes of serialised message payloads; excludes network framing and transport overhead",
            "latency_definition": "local receiver merge latency measured by the simulator; not end-to-end network latency",
        },
        "summary": [
            summarise(rows, size, mode)
            for size in ITEM_SIZES
            for mode in ("full", "delta")
        ],
        "payload_reduction_percent": {
            str(size): (
                (summarise(rows, size, "full")["average_payload_bytes_transmitted"]
                 - summarise(rows, size, "delta")["average_payload_bytes_transmitted"])
                / summarise(rows, size, "full")["average_payload_bytes_transmitted"] * 100
                if summarise(rows, size, "full")["average_payload_bytes_transmitted"] else 0.0
            )
            for size in ITEM_SIZES
        },
        "trials": rows,
    }

    out_path = ROOT / "benchmarks" / "incremental_update_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Detailed results saved to: {out_path}")


if __name__ == "__main__":
    main()
