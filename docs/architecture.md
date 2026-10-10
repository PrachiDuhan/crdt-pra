# PS-39 Architecture

## Components

1. **CRDT layer**
   - `GCounter`: independent non-decreasing component per replica; merge uses component-wise maximum.
   - `ORSet`: unique add tags and observed-remove tombstones; merge unions add tags and removed tags.
   - `LWWMap`: per-key logical timestamp and replica-ID ordering, including tombstones for deletes.
2. **Replica layer**
   - A `Replica` owns one instance of each CRDT.
   - `snapshot()` exposes serializable state.
   - `merge_state()` merges incoming full or partial state atomically by applying it to copies first.
3. **Network simulator**
   - Five independent replicas in the demo configuration.
   - Queued JSON payloads, partition groups, duplicate injection, random-order delivery, and recovery rounds.
   - Full-state mode sends a complete snapshot. Delta mode compares a sender's local CRDT state with state previously delivered from that sender to that receiver.
4. **Evaluation**
   - Unit tests and generated/property-based merge tests.
   - Randomised partition-recovery scenarios.
   - Full-state/delta benchmark with transmitted bytes, delivered bytes, convergence rate, and merge latency.
5. **Dashboard**
   - Streamlit UI for local edits, partition/reconnect actions, queued messages, replica state, and benchmark comparison.

## Data flow

A local user action mutates one replica's CRDT. A broadcast queues a serialized full snapshot or delta for each reachable peer. Delivery applies the state to the destination replica using the CRDT merge functions. Partitioned peers cannot exchange messages until connectivity is restored. Repeated broadcast/delivery rounds perform anti-entropy until the logical states converge.

## Important scope boundary

This is a single-process simulation. The simulator is not itself an authoritative database, but delivery and the per-peer delta bookkeeping are centrally modelled. A deployed multi-device system would need actual transport, durable local state, acknowledgements/retries, identity management, and a defined tombstone-retention strategy.
