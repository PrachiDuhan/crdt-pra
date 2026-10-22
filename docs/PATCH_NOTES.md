# Audit patch notes

This package is an audited revision of the uploaded PS-39 project.

Changes made:

1. Cleaned up duplicated test function definitions in `tests/test_network_simulator.py`.
2. Made `GCounter.merge_state` validate all components before mutating its own state.
3. Made `Replica.merge_state` merge into copies and commit only if all three component merges succeed.
4. Clarified simulator measurements: payload bytes are counted at sender transmission time; delivered bytes are reported separately; merge latency measures only the destination replica's merge.
5. Adjusted benchmark random seeds so workload/partition generation and fault/delivery schedules do not depend on the number of random draws consumed by the other mode.
6. Added Hypothesis-based generated tests for G-Counter, OR-Set and LWW-Map merge laws and generated partition/recovery scenarios.
7. Updated requirements, README and architecture/evaluation-report structure.
8. Updated dashboard result labels to distinguish sent payload bytes from delivered payload bytes.

Before using this project locally:

```powershell
python -m pip install -r requirements.txt
python -m pytest -q
python benchmarks/run_benchmarks.py
python -m streamlit run src/dashboard.py
```

Regenerate `benchmarks/comparison_results.json` on the machine used for the final submission. Timing figures vary by machine and must not be copied from another environment.

Note: the review environment did not have network access to install Hypothesis. The property-test file was syntax-checked, and equivalent seeded generated checks were run locally; install the declared requirements and run the complete suite on the development machine to execute Hypothesis itself.
