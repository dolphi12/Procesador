from __future__ import annotations

import os
import json
from pathlib import Path

import pytest

from procesador.audit import log_change, make_sample_entries
from procesador.utils import get_or_create_audit_key, default_app_data_dir


def test_log_change_creates_jsonl(tmp_path: Path):
    audit_dir = tmp_path / "auditoria"
    recs = make_sample_entries()
    p = log_change(audit_dir=audit_dir, record=recs[0], rotate_max_bytes=0)
    assert p.exists()
    lines = p.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    obj = json.loads(lines[0])
    assert obj["run_id"] == "sample_run_001"


@pytest.mark.skipif(os.name == "nt", reason="chmod semantics differ on Windows")
def test_log_change_permissions_posix(tmp_path: Path):
    audit_dir = tmp_path / "auditoria"
    p = log_change(audit_dir=audit_dir, record=make_sample_entries()[0], rotate_max_bytes=0)
    mode = p.stat().st_mode & 0o777
    assert mode == 0o600


def test_audit_key_not_in_code_dir(tmp_path: Path):
    # by default it should go to app data dir, not under provided script_dir
    script_dir = tmp_path / "code"
    script_dir.mkdir()
    key_hex, kid = get_or_create_audit_key(script_dir, filename="audit_key.txt")
    assert len(key_hex) == 64
    assert len(kid) == 8
    assert not (script_dir / "audit_key.txt").exists()
    # file should exist in app data dir
    data_dir = default_app_data_dir("procesador")
    assert (data_dir / "audit_key.txt").exists()
