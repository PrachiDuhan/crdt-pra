# PS-39 Source Audit Summary

## Source reviewed

The report is based on the uploaded `ps39-audit.zip` and the official PS-39 statement, which requires three CRDT types, delta/anti-entropy work, property-based convergence tests, partition simulation, and metrics across at least five replicas.

## Findings and corrections in this patch

- Removed duplicate same-named functions from `tests/test_network_simulator.py`; Python had been silently replacing earlier function definitions when loading the module.
- Made `GCounter.merge_state` validate all incoming entries before mutating state.
- Made `Replica.merge_state` merge into deep copies first and only commit after all component merges succeed, preventing malformed snapshots from partially updating a replica.
- Added Hypothesis-based generated merge-law tests for G-Counter, OR-Set and LWW-Map, plus generated partition/reordering/duplication/drop-recovery scenarios.
- Corrected simulator metric definitions: `bytes_transmitted` is measured when a sender queues a serialized payload; `bytes_delivered` counts payloads delivered to a receiver including duplicate deliveries. Merge latency now measures only the destination replica's state merge, excluding per-peer tracking bookkeeping.
- Made the benchmark use separate deterministic random streams for scenario generation and delivery/fault scheduling. It also distinguishes sent messages from delivered messages and considers a trial converged only when states agree and the queue is empty.
- Updated dashboard labels to distinguish transmitted from delivered payload bytes.
- Removed duplicate test definitions and clarified documentation/measurement definitions.

## Validation performed in the audit environment

- Original uploaded project: 28 tests passed before changes.
- Patched deterministic tests: 31 passed.
- 500 generated merge-law checks each for G-Counter, OR-Set and LWW-Map passed with standard-library randomized checks.
- 500 randomized partition/recovery runs in each synchronisation mode passed under dropped, reordered and duplicated messages using additional standard-library stress checks.
- Updated benchmark runner completed 100 trials per mode in the audit environment. Its timings are environment-specific and must not be submitted as the team's Windows results.
- The audit environment could not install Hypothesis because package network access was unavailable. The Hypothesis tests are included and `requirements.txt` declares the dependency; install requirements on the project machine and run the complete suite before accepting the patch.
- Streamlit was not installed in the audit environment, so the UI could not be launched here. The dashboard file passed syntax parsing; check its runtime locally after applying the patch.

## Important remaining limitation

The application is still a single-process network simulation, not a deployed multi-device store. The simulator centrally models delivery and per-peer delta tracking. A real deployment would need a transport protocol, durable per-device state, and explicit ACK/retry or equivalent anti-entropy semantics.
