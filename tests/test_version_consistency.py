"""Every place that carries the version must match src/__init__.py, so a
release can never ship mismatched versions. Bump them all with
scripts/bump-version.py."""

import json
import re
from pathlib import Path

from src import __version__

ROOT = Path(__file__).resolve().parent.parent


def _compose_image_tag(name):
    text = (ROOT / name).read_text(encoding="utf-8")
    m = re.search(r"image:\s*ghcr\.io/\S+:(\S+)", text)
    assert m, f"no ghcr.io image tag found in {name}"
    return m.group(1)


def test_compose_files_pin_current_version():
    for name in ("docker-compose.yml", "docker-compose.example.yml"):
        tag = _compose_image_tag(name)
        assert tag != "latest", f"{name} must pin a version, not :latest"
        assert tag == __version__, f"{name} pins :{tag}, expected {__version__}"


def test_addon_config_matches_version():
    cfg = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
    assert cfg["version"] == __version__, (
        f"config.json is {cfg['version']}, expected {__version__} "
        "— run scripts/bump-version.py"
    )
