import re

_DURATION = re.compile(r"^(\d{1,5})([smhd])$")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
DURATION_PATTERN = _DURATION.pattern


def parse_duration(text: str) -> int:
    """'15m' -> 900. Raises ValueError for anything that is not <number><s|m|h|d>."""
    match = _DURATION.fullmatch(text)
    if match is None:
        raise ValueError(f"invalid duration {text!r}; expected e.g. 30s, 15m, 6h, 7d")
    return int(match.group(1)) * _UNIT_SECONDS[match.group(2)]
