# PS-39: Offline-First Replicated Store with CRDTs

An experimental offline-first replicated data-store prototype for the PS-39 challenge. Five simulated replicas can accept independent changes while partitioned, then merge their state after communication is restored. The project compares full-state synchronisation with delta synchronisation and includes a Streamlit dashboard, automated tests, and reproducible benchmark scripts.

> **Scope:** this is an in-process Python simulation, not a production distributed database or a deployment across physical devices. Benchmark results below describe the tested simulator workloads only.

## What is implemented

- **G-Counter** — a grow-only replicated counter.
- **Observed-Remove Set (OR-Set)** — a replicated set supporting additions and removals.
- **Last-Writer-Wins Map (LWW-Map)** — a replicated map that resolves competing writes deterministically using logical timestamps and replica-ID tie-breaking.
- **Replica abstraction and local persistence** — per-replica CRDT state can be saved and restored.
- **Five-replica network simulator** — simulates partitions, shuffled/reordered delivery, duplicated messages, and recovery after partitions heal.
- **Two synchronisation modes** — full-state and delta.
- **Streamlit dashboard** — visual inspection of replica state, network operations, conflicts, persistence, and benchmark results.
- **Automated tests** — unit tests, Hypothesis-based property tests, network recovery tests, and persistence tests.
- **Benchmark reports** — JSON outputs for the main comparison, scaling, incremental updates, and stress validation.

## Quick start (Windows PowerShell)

Use Python 3.10 or newer. From the project root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If PowerShell blocks activation, allow it only for the current terminal session:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

### Run the dashboard

```powershell
python -m streamlit run src/dashboard.py
```

Open the local URL printed by Streamlit (normally `http://localhost:8501`). The dashboard lets you inspect replica data and experiment with the simulated network. For a partition-recovery demonstration, split the network, make independent changes on replicas in different groups, then heal the network and deliver pending messages. Verify that the replicas eventually hold the same logical CRDT state.

### Run the tests

```powershell
python -m pytest -q
```

The recorded local verification run passed **41 tests**. Hypothesis generates additional input sequences, but property-based testing is empirical testing—not a formal proof for every possible operation sequence or message schedule.

## Reproduce the benchmarks

Run these commands from the project root after installing the requirements:

| Experiment | Command | Results file |
|---|---|---|
| Main full-state vs delta comparison | `python benchmarks/run_benchmarks.py` | `benchmarks/comparison_results.json` |
| Dataset-size scaling | `python benchmarks/run_scaling_benchmarks.py` | `benchmarks/scaling_results.json` |
| One small update after initial convergence | `python benchmarks/run_incremental_update_benchmark.py` | `benchmarks/incremental_update_results.json` |
| Extended 500-trial stress validation per mode | `python benchmarks/run_stress_validation.py` | `benchmarks/stress_results.json` |

Keep the generated JSON files with the code revision used for the experiment. Timings depend on machine load and runtime conditions; rerun the experiments before making claims about a changed implementation.

## Selected evaluation results

### Main comparison — 100 trials per mode

Five simulated replicas were tested in each mode. The configured scenarios include seeded partitions, independent local updates, duplicate delivery, shuffled message delivery, and recovery after partitions heal.

| Metric | Full-state | Delta |
|---|---:|---:|
| Converged trials | 100 / 100 | 100 / 100 |
| Convergence rate | 100.00% | 100.00% |
| Average messages sent per trial | 29.96 | 29.96 |
| Average transmitted payload bytes per trial | 20,331.64 | 18,098.35 |
| Average merge latency | 0.076038 ms | 0.070094 ms |

Delta transmitted **10.98% fewer serialised payload bytes** in this run. Message counts were the same. These merge timings are simulator measurements and should not be interpreted as end-to-end network latency.

### Extended stress validation — 500 trials per mode

| Metric | Full-state | Delta |
|---|---:|---:|
| Converged trials | 500 / 500 | 500 / 500 |
| Convergence rate | 100.00% | 100.00% |
| Average transmitted payload bytes per trial | 20,330.44 | 18,097.51 |
| Average merge latency | 0.062522 ms | 0.142592 ms |

No convergence failures were observed in these 1,000 sampled trials. Delta transmitted 10.98% fewer payload bytes, but its measured average merge latency was higher in this stress run.

### Steady-state small update after initial convergence

This OR-Set-focused experiment added one task on replica `r1` after the replicas were already synchronised.

| Baseline items | Full-state bytes/update | Delta bytes/update | Payload reduction with delta | Messages: full vs delta |
|---:|---:|---:|---:|---:|
| 10 | 14,628 | 660 | 95.49% | 20 vs 4 |
| 100 | 126,232 | 664 | 99.47% | 20 vs 4 |
| 500 | 622,232 | 664 | 99.89% | 20 vs 4 |

This experiment shows the bandwidth advantage of sending a small change after initial synchronisation in the tested workload. Latency results were mixed across dataset sizes, so the project does **not** claim that delta mode is always faster.

See [`docs/evaluation_report.md`](docs/evaluation_report.md) for the full methodology, scaling results, metric definitions, environment record, and limitations.

## Repository layout

```text
src/
  crdts/              G-Counter, OR-Set, and LWW-Map implementations
  replicas/           Replica abstraction
  network/             In-memory network simulator
  storage/             Local replica-state persistence
  dashboard.py         Streamlit dashboard
  demo_g_counter.py    Small G-Counter demo
benchmarks/             Benchmark scripts and JSON result files
tests/                  Unit, property-based, network, and persistence tests
docs/
  architecture.md       Architecture notes
  evaluation_report.md  Detailed evaluation report
requirements.txt        Python dependencies
```

## Design notes and limitations

- All replicas and simulated message delivery run in one Python process; the project does not demonstrate communication between separate machines.
- A successful convergence result means the tested replicas ended with the same logical state and no queued messages. It does not prove correctness for every possible schedule.
- The simulator tracks delivered per-sender/per-receiver state for delta synchronisation. A real deployment would need explicit acknowledgements or an equivalent reliable anti-entropy protocol.
- LWW-Map resolves concurrent writes deterministically and does not retain every concurrent value.
- OR-Set and LWW-Map tombstones are retained; tombstone compaction and long-running storage-growth management are not implemented.
- Local persistence stores individual replica state, not the complete simulator state or in-flight network queue.
- Payload-byte figures measure serialised message payloads and exclude transport headers, encryption, and other network overhead.

## Environment used for the recorded verification

- OS: Windows 11 Home Single Language, version 25H2 (OS Build 26200.9550)
- Python: 3.11.9
- Automated tests: 41 passed in the recorded verification run
- Dependencies: [`requirements.txt`](requirements.txt)

Machine CPU/RAM details should be recorded separately before final submission.


## 🚀 Live Demo

**[Launch the PS-39 Dashboard](https://replicated-store.streamlit.app)**

Explore the five-replica simulator, test independent updates during network partitions, heal the network, and observe replica convergence. The dashboard also displays benchmark results comparing full-state and delta synchronisation.

> **Note:** The replica network runs in a single Python process. This is a simulated distributed system, not a deployment across separate physical machines.
