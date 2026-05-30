#!/usr/bin/env python3
"""Bump the project version everywhere it appears, in one shot.

The version lives in several files that must stay in lockstep (a test enforces
it). Rather than edit each by hand, run:

    python scripts/bump-version.py 0.2.2

Then review the diff, commit and push — the release pipeline does the rest.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (relative path, regex with one capture-free version token, replacement template)
TARGETS = [
    ("src/__init__.py", r'__version__ = "[\d.]+"', '__version__ = "{v}"'),
    ("config.json", r'"version": "[\d.]+"', '"version": "{v}"'),
    ("docker-compose.yml", r'(image: ghcr\.io/[^\s:]+:)[\d.]+', r'\g<1>{v}'),
    ("docker-compose.example.yml", r'(image: ghcr\.io/[^\s:]+:)[\d.]+', r'\g<1>{v}'),
]


def main() -> None:
    if len(sys.argv) != 2 or not re.fullmatch(r"\d+\.\d+\.\d+", sys.argv[1]):
        sys.exit("usage: bump-version.py X.Y.Z")
    version = sys.argv[1]
    print(f"Bumping to {version}:")
    for rel, pattern, repl in TARGETS:
        path = ROOT / rel
        text = path.read_text(encoding="utf-8")
        new, n = re.subn(pattern, repl.format(v=version), text, count=1)
        if n != 1:
            sys.exit(f"error: version pattern not found in {rel}")
        path.write_text(new, encoding="utf-8")
        print(f"  updated {rel}")
    print("Done. Review the diff, then commit and push to cut the release.")


if __name__ == "__main__":
    main()
