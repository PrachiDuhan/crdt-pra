
from __future__ import annotations

import json
from pathlib import Path

from run_benchmarks import run_benchmarks

ROOT = Path(__file__).resolve().parents[1]
TRIALS = 500
BASE_SEED = 10000


def summarize(report: dict) -> dict:
    results = report["results"]
    failed = [
        {
            "seed": result["seed"],
            "rounds_after_heal": result["rounds_after_heal"],
            "queued_at_end": result["queued_at_end"],
        }
        for result in results
        if not result["converged"]
    ]

    recovery_rounds = [
        result["rounds_after_heal"] for result in results
    ]

    return {
        "mode": report["mode"],
        "trials": report["trials"],
        "converged_trials": report["converged_trials"],
        "convergence_rate_percent":
            report["convergence_rate_percent"],
        "average_messages_sent_per_trial":
            report["average_messages_sent_per_trial"],
        "average_messages_delivered_per_trial":
            report["average_messages_per_trial"],
        "average_payload_bytes_transmitted_per_trial":
            report["average_payload_bytes_per_trial"],
        "average_payload_bytes_delivered_per_trial":
            report["average_payload_bytes_delivered_per_trial"],
        "average_merge_latency_ms":
            report["average_merge_latency_ms"],
        "p95_merge_latency_ms":
            report["p95_merge_latency_ms"],
        "maximum_recovery_rounds": max(recovery_rounds, default=0),
        "failed_trials": failed,
    }


def main() -> None:
    print(f"Running {TRIALS} trials per mode...")
    print("Five replicas; same seed range for both modes.")

    full = run_benchmarks(
        trials=TRIALS,
        base_seed=BASE_SEED,
        sync_mode="full",
    )
    delta = run_benchmarks(
        trials=TRIALS,
        base_seed=BASE_SEED,
        sync_mode="delta",
    )

    full_summary = summarize(full)
    delta_summary = summarize(delta)

    full_bytes = full_summary[
        "average_payload_bytes_transmitted_per_trial"
    ]
    delta_bytes = delta_summary[
        "average_payload_bytes_transmitted_per_trial"
    ]

    reduction = (
        (full_bytes - delta_bytes) / full_bytes * 100
        if full_bytes > 0
        else None
    )

    output = {
        "experiment": {
            "trials_per_mode": TRIALS,
            "base_seed": BASE_SEED,
            "replicas": 5,
            "purpose": "Extended randomized partition and recovery validation",
        },
        "full_state": full_summary,
        "delta": delta_summary,
        "delta_payload_reduction_percent": reduction,
    }

    output_path = ROOT / "benchmarks" / "stress_results.json"
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(output, file, indent=2)

    for name, summary in (
        ("FULL-STATE", full_summary),
        ("DELTA", delta_summary),
    ):
        print(f"\n{name}")
        print("-" * 36)
        print(
            f"Convergence: {summary['converged_trials']}/"
            f"{summary['trials']} "
            f"({summary['convergence_rate_percent']:.2f}%)"
        )
        print(
            "Average transmitted payload bytes/trial: "
            f"{summary['average_payload_bytes_transmitted_per_trial']:.2f}"
        )
        print(
            "Average merge latency: "
            f"{summary['average_merge_latency_ms']:.6f} ms"
        )
        print(f"Failed trials: {len(summary['failed_trials'])}")

    if reduction is not None:
        print(f"\nDelta payload reduction: {reduction:.2f}%")

    print(f"\nDetailed summary saved to: {output_path}")


if __name__ == "__main__":
    main()
