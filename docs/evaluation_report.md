# PS-39 Evaluation Report — Offline-First Replicated Store with CRDTs

## 1. Implementation Under Evaluation

The prototype contains three CRDT data types—G-Counter, Observed-Remove Set (OR-Set), and Last-Writer-Wins Map (LWW-Map)—along with a replica abstraction, an in-memory five-replica network simulator, full-state and delta synchronisation modes, a Streamlit dashboard, and local replica-state persistence.

The network runs in one Python process. The results below are simulator results, not measurements from separate physical devices or a live network.

## 2. Main Benchmark: 100 Trials per Mode

**Configuration:** Five replicas and 100 trials per synchronisation mode. The benchmark uses seeded partitions, independently generated local updates, duplicate delivery, shuffled message delivery, and recovery after partitions heal.

| Metric | Full-state | Delta |
|---|---:|---:|
| Converged trials / total | 100 / 100 | 100 / 100 |
| Convergence rate | 100.00% | 100.00% |
| Average messages sent per trial | 29.96 | 29.96 |
| Average messages delivered per trial | 34.41 | 34.41 |
| Average payload bytes transmitted per trial | 20,331.64 | 18,098.35 |
| Average payload bytes delivered per trial | 23,351.12 | 20,808.16 |
| Average merge latency (ms) | 0.076038 | 0.070094 |
| P95 merge latency (ms) | 0.128600 | 0.118100 |

For this workload, delta mode transmitted **10.98% fewer serialized payload bytes** than full-state mode. Message counts were the same. The merge-latency difference is small in absolute terms and should not be generalised without repeated runs under controlled machine load.

Results were written to `benchmarks/comparison_results.json`.

## 3. Extended Stress Validation: 500 Trials per Mode

This additional experiment ran 500 trials for each synchronisation mode using five simulated replicas and a reproducible seed range starting at `10000`. It extends the main benchmark's randomized partition and recovery testing.

| Metric | Full-state | Delta |
|---|---:|---:|
| Trials | 500 | 500 |
| Converged trials | 500 / 500 | 500 / 500 |
| Convergence rate | 100.00% | 100.00% |
| Average transmitted payload bytes per trial | 20,330.44 | 18,097.51 |
| Average merge latency (ms) | 0.062522 | 0.142592 |
| Failed trials | 0 | 0 |

Delta mode transmitted **10.98% fewer payload bytes** than full-state mode in this run. Both modes converged in all 500 tested trials. However, delta mode's measured average merge latency was higher in this run (0.142592 ms versus 0.062522 ms). This reinforces that the observed payload reduction does not imply a consistent latency improvement.

These results are from the in-process simulator. A 100% convergence rate across 500 sampled trials is evidence for the tested schedules, not a proof of convergence for every possible operation sequence, partition schedule, or message delivery order.

Detailed results were written to `benchmarks/stress_results.json`.

## 4. Scaling Benchmark: 20 Trials per Size and Mode

This workload-size experiment tested 10, 100, and 500 generated task items per replica. It ran 20 trials for each size and synchronisation mode.

| Items per replica | Mode | Convergence | Messages sent per trial | Payload bytes sent per trial | Average merge latency (ms) | P95 merge latency (ms) |
|---:|---|---:|---:|---:|---:|---:|
| 10 | Full-state | 100.00% (20/20) | 30.00 | 53,599.45 | 0.169029 | 0.243100 |
| 10 | Delta | 100.00% (20/20) | 30.00 | 46,615.75 | 0.249526 | 0.492900 |
| 100 | Full-state | 100.00% (20/20) | 28.80 | 398,160.55 | 1.648738 | 2.894900 |
| 100 | Delta | 100.00% (20/20) | 28.80 | 343,968.35 | 3.730936 | 8.697200 |
| 500 | Full-state | 100.00% (20/20) | 30.00 | 2,160,922.05 | 25.282466 | 48.325300 |
| 500 | Delta | 100.00% (20/20) | 30.00 | 1,852,866.90 | 7.146119 | 12.360700 |

| Items per replica | Payload reduction with delta |
|---:|---:|
| 10 | 13.03% |
| 100 | 13.61% |
| 500 | 14.26% |

All tested trials converged. Delta mode transmitted fewer payload bytes at each tested size. The latency results vary by workload: delta was slower at sizes 10 and 100 but faster at size 500 in this run. Because timings are environment-sensitive and only 20 trials were used per size and mode, these results are preliminary evidence rather than a universal performance claim.

Results were written to `benchmarks/scaling_results.json`.

## 5. Steady-State Incremental-Update Benchmark

### Experimental setup

- Replicas: 5
- Trials: 20 per workload size and synchronisation mode
- Baseline OR-Set sizes: 10, 100, and 500 items
- Operation under test: add one new task on `r1` after initial convergence
- Modes: full-state and delta synchronisation

### Results

| Baseline OR-Set items | Mode | Convergence | Messages sent per update | Payload bytes per update | Average merge latency (ms) | P95 trial-average latency (ms) |
|---:|---|---:|---:|---:|---:|---:|
| 10 | Full-state | 100% | 20 | 14,628.00 | 0.087523 | 0.130165 |
| 10 | Delta | 100% | 4 | 660.00 | 0.088396 | 0.170600 |
| 100 | Full-state | 100% | 20 | 126,232.00 | 0.546312 | 0.711030 |
| 100 | Delta | 100% | 4 | 664.00 | 0.359778 | 0.463625 |
| 500 | Full-state | 100% | 20 | 622,232.00 | 4.843755 | 9.093530 |
| 500 | Delta | 100% | 4 | 664.00 | 6.600841 | 9.082650 |

### Interpretation

Delta synchronisation reduced measured payload bytes by **95.49%** at 10 items, **99.47%** at 100 items, and **99.89%** at 500 items. It used four messages per update instead of twenty in these experiments.

These results support delta synchronisation for small changes to an already-synchronised dataset. Measured latency results were mixed: delta mode was slightly slower at 10 items and had higher average merge latency at 500 items, but was faster at 100 items. Further repetitions would be needed to establish stable latency differences.

All 20 trials converged at each workload size and mode. This is evidence for the tested scenarios, not proof of correctness for every possible message schedule.

Results were written to `benchmarks/incremental_update_results.json`.

### Measurement limitations

Payload bytes refer to the benchmark's measured serialized message data, not complete network traffic. Average merge latency and P95 trial-average latency are different statistics and should be interpreted separately. The experiment uses a simulated network and an OR-Set-focused workload.

## 6. Measurement Definitions

- **Convergence:** All replicas have the same logical CRDT state and the message queue is empty at the end of a trial.
- **Messages sent:** Messages enqueued by send operations. An injected duplicate is a duplicate network packet, not a new sender-side send operation.
- **Messages delivered:** Successful message deliveries to a receiver, including duplicate deliveries.
- **Payload bytes transmitted:** UTF-8 byte size of serialized payloads at send/queue time. This excludes transport headers, encryption, and other real-network overhead.
- **Payload bytes delivered:** UTF-8 byte size of serialized payloads delivered to receivers, including duplicates.
- **Merge latency:** Local time spent merging a message into a receiver's CRDT state. It excludes network delay and should not be described as end-to-end synchronisation latency.
- **P95 merge latency:** Nearest-rank 95th percentile of recorded individual merge timings in the main and scaling benchmarks. The incremental-update benchmark reports **P95 trial-average latency**, which is a different statistic.

## 7. Correctness Evidence and Limitations

The local test suite reported **41 passing tests** after property-based tests and persistence/resynchronisation tests were added. Property-based testing explores many generated cases, but it is not a mathematical proof for every possible operation sequence or message schedule.

The simulator models replicas and message delivery in one process. It does not demonstrate communication between independent machines or guarantee production-grade crash recovery. Local persistence saves and restores individual replica state; it does not persist the network queue or complete simulator state.

The LWW-Map deliberately resolves competing writes by `(logical timestamp, writer replica ID)`. This gives deterministic convergence but does not preserve every concurrent value. OR-Set tombstones are retained and can increase memory usage over long periods; garbage collection is not implemented.

## 8. Failure Analysis and Next Experiments

The reported trials had no convergence failures. This means no failures were observed in these sampled scenarios, not that failure is impossible. Useful next experiments are:

1. Increase the number of trials and retain reproducible seeds.
2. Test more varied partition schedules, losses, reordering, and duplicate-delivery schedules.
3. Compare initial full synchronisation against a small steady-state update after replicas already share a large dataset.
4. Record any failed seed and reduce it to a minimal reproducible case.
5. Measure behaviour after process restart and independent replica restoration.

## 9. Environment Record

- **Operating system:** Windows 11 Home Single Language, version 25H2 (OS Build 26200.9550).
- **Python version:** Python 3.11.9.
- **Test suite:** `python -m pytest -q` — 41 passed in 4.22 seconds during the recorded verification run.
- **Machine / CPU / RAM:** Not recorded yet; fill in the actual device specifications before submission.
- **Benchmark commit/version:** Not available yet. `git rev-parse HEAD` returned `fatal: not a git repository`, so this project folder has not been initialised as a Git repository at the time of this check. Initialise Git and record the resulting commit hash before submission, or archive the exact submitted source.
- **Dependencies:** See `requirements.txt`.

## 10. Conclusion

The implementation demonstrates three CRDT types and five-replica simulated convergence under the tested duplicate, reordering, partition, and recovery scenarios. Delta mode reduced serialized payload bytes in the main, scaling, and steady-state experiments, while latency trade-offs depended on workload size. In the steady-state single-update experiment, delta mode also used fewer messages and transmitted substantially less payload data as the baseline dataset grew. The strongest supported conclusion is that delta mode reduced payload size in these experiments; further testing is needed before claiming a consistent latency improvement or production readiness.
