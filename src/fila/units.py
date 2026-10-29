"""Parsing and formatting of memory sizes and durations given on the command line."""

from __future__ import annotations

import re

_SIZES = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
_DURATIONS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_mem(text: str) -> int:
    """``"512M"``, ``"4G"``, ``"1.5G"`` or plain bytes, to bytes."""
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT]?)(?:i?B)?\s*", text, re.IGNORECASE)
    if not match:
        raise ValueError(f"invalid memory size: {text!r} (use e.g. 512M, 4G)")
    return int(float(match.group(1)) * _SIZES[match.group(2).upper()])


def parse_duration(text: str) -> float:
    """``"90s"``, ``"10m"``, ``"2h"``, ``"3d"``, Slurm's ``"7-00:00:00"`` or ``"HH:MM:SS"``, to seconds."""
    text = text.strip()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", text)
    if match:
        return float(match.group(1)) * _DURATIONS[match.group(2)]
    match = re.fullmatch(r"(?:(\d+)-)?(\d+):(\d{2})(?::(\d{2}))?", text)
    if match:
        days, hours, minutes, seconds = (int(group or 0) for group in match.groups())
        return float(((days * 24 + hours) * 60 + minutes) * 60 + seconds)
    raise ValueError(f"invalid duration: {text!r} (use e.g. 90s, 10m, 2h, 3d or 7-00:00:00)")


def format_mem(amount: int) -> str:
    """Exact when it divides (``4G``), otherwise rounded to one decimal (``15.9G``)."""
    for unit in ("T", "G", "M", "K"):
        size = _SIZES[unit]
        if amount >= size:
            if amount % size == 0:
                return f"{amount // size}{unit}"
            return f"{amount / size:.1f}{unit}"
    return str(amount)


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, seconds = divmod(rest, 60)
    if days:
        return f"{days}d{hours:02d}h"
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"
