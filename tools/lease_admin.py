#!/usr/bin/env python3
"""管理命令包装：查看占用来源、超时扫描、安全回收孤儿记录。

示例：
    python3 tools/lease_admin.py list
    python3 tools/lease_admin.py sweep
    python3 tools/lease_admin.py reclaim --id lease-xxx
    python3 tools/lease_admin.py show lease-xxx
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from scheduling_lease.admin import main

if __name__ == "__main__":
    raise SystemExit(main())
