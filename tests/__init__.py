"""Shared fixture: every world is run once, through the same path as the replay command, in parallel."""
import atexit
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from recovery_replay import replay
from recovery_replay.ledger import Reconciler

RUNS = {**{w: (w, None) for w in ("A", "B1", "B2", "C1", "C2", "C3", "C4")},
        **{f"A without {e}": ("A", e) for e in sorted(Reconciler.ENDS)}}
_cache = {}


def runs():
    if not _cache:
        out = Path(tempfile.mkdtemp(prefix="replay-tests-"))
        atexit.register(shutil.rmtree, out, True)
        with ThreadPoolExecutor(max_workers=6) as pool:
            done = pool.map(lambda kv: (kv[0], replay.run(kv[1][0], out, kv[1][1])), RUNS.items())
            _cache.update(done)
    return _cache
