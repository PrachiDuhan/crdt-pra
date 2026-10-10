# PS-39: Offline-First Replicated Store with CRDTs

An experimental, in-memory simulation of an offline-first replicated data store. Replicas maintain independent state and merge updates after reconnecting.

## Implemented components

- G-Counter for replicated counting
- Observed-Remove Set (OR-Set) for additions/removals
- Last-Writer-Wins Map (LWW-Map) with logical timestamps, replica-ID tie-breaking, and deletion tombstones
- Five-replica network simulator with partitions, reordered delivery, duplicate delivery, and recovery
- Full-state and delta synchronisation modes
- Streamlit dashboard
- Unit, generated/property-based, and randomised recovery tests
- Benchmark comparison with JSON output

## Requirements

- Python 3.10 or newer
- pip

## Setup on Windows PowerShell

From the project root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If PowerShell blocks activation, use a process-scoped execution-policy change for that terminal only:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

## Run tests

```powershell
python -m pytest -q
```

`Hypothesis` generates additional state and operation sequences for CRDT merge-property tests. These tests are empirical and do not constitute a formal proof for every possible execution.

## Run the dashboard

```powershell
python -m streamlit run src/dashboard.py
```

## Run the benchmark

```powershell
python benchmarks/run_benchmarks.py
```

The benchmark uses paired scenario seeds for partition layouts and local updates. Results are saved to `benchmarks/comparison_results.json`.

## Measurements and definitions

- **Convergence rate:** proportion of trials where all replicas have equal logical state and no messages remain queued at the end.
- **Payload bytes transmitted:** UTF-8 size of serialized message payloads queued by senders. It excludes transport headers and lower-level network overhead. Injected network duplicates do not count as new sender transmissions.
- **Payload bytes delivered:** serialized payload bytes accepted by receivers, including duplicate deliveries.
- **Merge latency:** local time spent merging an incoming snapshot/delta into the destination replica. It is not end-to-end network latency.

## Limitations

- The network is simulated in one Python process, not deployed across separate devices.
- Results apply to tested seeds, workloads, and schedules; they are not proof over all possible schedules.
- Delta synchronization currently tracks successfully delivered per-sender/per-receiver state in the simulator. A real network protocol would need explicit acknowledgements or an equivalent reliable anti-entropy protocol.
- LWW-Map resolves concurrent writes with logical timestamp and replica-ID ordering; it does not preserve both concurrent values.
- OR-Set and LWW-Map tombstones are retained; compaction and long-running storage growth are not implemented.

## Reproducibility

Record the Python version, operating system, seed settings, number of trials, and benchmark JSON from the same code revision used in the demonstration. Re-run benchmarks after changing the synchronization algorithm; do not present results from an older build as if they came from the current build.
