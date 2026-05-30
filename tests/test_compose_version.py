"""The example compose must pin the published image to the current release
version (never :latest, never drifting from src/__init__.py)."""

import re
from pathlib import Path

from src import __version__

COMPOSE = Path(__file__).resolve().parent.parent / "docker-compose.example.yml"


def test_example_compose_pins_current_version():
    text = COMPOSE.read_text(encoding="utf-8")
    m = re.search(r"image:\s*ghcr\.io/\S+:(\S+)", text)
    assert m, "no ghcr.io image tag found in docker-compose.example.yml"
    tag = m.group(1)
    assert tag != "latest", "compose must pin a version, not :latest"
    assert tag == __version__, (
        f"docker-compose.example.yml pins :{tag} but src/__init__.py is "
        f"{__version__} — bump both together"
    )
