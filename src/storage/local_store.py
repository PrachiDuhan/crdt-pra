import json
from pathlib import Path
from typing import Any

from replicas.replica import Replica


SCHEMA_VERSION = 1


def _replica_path(replica_id: str, directory: Path) -> Path:
    """Build a safe filename for a replica ID."""
    if (
        not isinstance(replica_id, str)
        or not replica_id.strip()
        or Path(replica_id).name != replica_id
        or replica_id in {".", ".."}
    ):
        raise ValueError("Invalid replica ID for storage")

    return directory / f"{replica_id}.json"


def save_replica(replica: Replica, directory: Path) -> Path:
    """Save a replica atomically to local JSON storage."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)

    target = _replica_path(replica.replica_id, directory)
    temporary = target.with_name(target.name + ".tmp")

    payload = {
        "schema_version": SCHEMA_VERSION,
        "replica_id": replica.replica_id,
        "snapshot": replica.snapshot(),
    }

    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(target)
    finally:
        # Clean up a leftover temporary file after a failed save.
        if temporary.exists():
            temporary.unlink()

    return target


def load_replica(replica_id: str, directory: Path) -> Replica:
    """Load a replica and validate its saved identity and state."""
    directory = Path(directory)
    target = _replica_path(replica_id, directory)

    if not target.is_file():
        raise FileNotFoundError(f"No saved state found for {replica_id}")

    try:
        payload: dict[str, Any] = json.loads(
            target.read_text(encoding="utf-8")
        )
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError("Saved replica file is not valid JSON") from exc

    if not isinstance(payload, dict):
        raise ValueError("Saved file must contain a JSON object")

    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Unsupported saved-state schema version")

    if payload.get("replica_id") != replica_id:
        raise ValueError("Saved file belongs to another replica")

    replica = Replica(replica_id)
    replica.restore_snapshot(payload.get("snapshot"))

    return replica