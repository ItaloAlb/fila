"""Section 3.4: the node registry, ``nodes.toml`` in the coordinator's home.

The file holds what the user registered and may edit by hand. What changes
while the server runs (probes, drain) lives in the database instead.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

try:
    import tomllib as _toml  # Python 3.11+
except ImportError:  # pragma: no cover - Python 3.9 and 3.10
    try:
        import tomli as _toml
    except ImportError:
        _toml = None

GENERAL_DEFAULTS = {
    "probe_interval_s": 30,
    "starvation_after": 3600.0,
    "run_timeout": 600.0,
    "json_tail_kb": 200,
    # Never prompt (keys only), give up on dead links; ControlMaster belongs in ~/.ssh/config.
    "ssh_command": ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                    "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3"],
    "ssh_timeout_s": 60.0,
}


@dataclass
class NodeEntry:
    name: str
    type: str  # "computer" | "slurm"
    shell: List[str]
    jobs_dir: str
    access: Optional[str] = None  # computers: "local" | "ssh" | "pull"
    os: Optional[str] = None
    host: Optional[str] = None
    via: Optional[str] = None
    python: Optional[str] = None
    limits: Dict[str, int] = field(default_factory=dict)  # cores, mem (bytes)
    reserve_always: Dict[str, int] = field(default_factory=dict)
    reserve_in_use: Dict[str, int] = field(default_factory=dict)
    on_use: str = "keep"
    idle_minutes: float = 5.0
    default_hint: str = "multithread"
    speed: float = 1.0
    speed_source: str = "default"  # "builtin", "command", "manual" or "default"
    min_free_disk: Optional[int] = None
    max_time: Optional[float] = None
    # Slurm clusters only (section 8)
    partition: Optional[str] = None
    account: Optional[str] = None
    qos: Optional[str] = None
    exclude: List[str] = field(default_factory=list)
    max_pending: int = 2
    max_cores_per_job: Optional[int] = None  # overrides what the partition and QoS say


class RegistryError(Exception):
    pass


def load_toml(path: str) -> dict:
    if _toml is None:
        raise RegistryError(f"reading {os.path.basename(path)} needs Python 3.11+ or the tomli package")
    with open(path, "rb") as handle:
        try:
            return _toml.load(handle)
        except ValueError as error:
            raise RegistryError(f"{path}: {error}") from None


class Registry:
    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> tuple:
        """(general settings, nodes by name). A missing file is an empty registry."""
        if not os.path.exists(self.path):
            return dict(GENERAL_DEFAULTS), {}
        data = load_toml(self.path)
        general = dict(GENERAL_DEFAULTS, **data.get("general", {}))
        nodes = {}
        for name, table in data.get("nodes", {}).items():
            reserve = table.pop("reserve", {})
            try:
                nodes[name] = NodeEntry(
                    name=name, reserve_always=reserve.get("always", {}), reserve_in_use=reserve.get("in_use", {}), **table
                )
            except TypeError as error:
                raise RegistryError(f"{self.path}: node {name}: {error}") from None
        return general, nodes

    def save(self, general: dict, nodes: Dict[str, NodeEntry]) -> None:
        lines = ["[general]"]
        lines += [f"{key} = {_value(value)}" for key, value in general.items()]
        for name in sorted(nodes):
            entry = asdict(nodes[name])
            reserve = {"always": entry.pop("reserve_always"), "in_use": entry.pop("reserve_in_use")}
            entry.pop("name")
            lines += ["", f"[nodes.{_key(name)}]"]
            lines += [f"{key} = {_value(value)}" for key, value in entry.items() if value not in (None, {}, [])]
            reserve = {key: value for key, value in reserve.items() if value}
            if reserve:
                lines.append(f"reserve = {_value(reserve)}")
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        temporary = self.path + ".tmp"
        with open(temporary, "w", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
        os.replace(temporary, self.path)


def _key(name: str) -> str:
    return name if name.replace("-", "").replace("_", "").isalnum() else json.dumps(name)


def _value(value) -> str:
    """TOML for the values the registry uses: strings, numbers, booleans, lists and inline tables."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)  # a JSON string is a valid TOML basic string
    if isinstance(value, list):
        return "[" + ", ".join(_value(item) for item in value) + "]"
    if isinstance(value, dict):
        return "{ " + ", ".join(f"{_key(k)} = {_value(v)}" for k, v in value.items()) + " }"
    raise TypeError(f"cannot write {value!r} to TOML")
