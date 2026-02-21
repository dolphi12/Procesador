"""Self-check for the attendance processor.

Runs:
- compileall
- pytest
- smoke run of the CLI on demo_input.xlsx (no-interactive)

Exit code:
- 0 on success
- non-zero on failure

Usage:
    python scripts/selfcheck.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    print("\n$", " ".join(cmd))
    subprocess.run(cmd, cwd=str(ROOT), check=True)


def main() -> int:
    try:
        run([sys.executable, "-m", "compileall", "-q", "."])
        run([sys.executable, "-m", "pytest", "-q"])

        demo = ROOT / "demo_input.xlsx"
        if demo.exists():
            run(
                [
                    sys.executable,
                    "-m",
                    "procesador.cli",
                    "process",
                    "--input",
                    str(demo),
                    "--no-interactive",
                    "--verify",
                    "--audit-user",
                    "SELFTEST",
                ]
            )
        else:
            print("[INFO] demo_input.xlsx not found; skipping CLI smoke run.")
        print("\n[OK] selfcheck passed")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"\n[FAIL] selfcheck failed: {e}")
        return int(e.returncode or 1)


if __name__ == "__main__":
    raise SystemExit(main())
