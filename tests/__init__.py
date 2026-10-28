"""Shared fixture: the three worlds are run once, through the same path as the replay command."""
import atexit
import shutil
import tempfile
from pathlib import Path

from recovery_replay import replay

_cache = {}


def runs():
    if not _cache:
        out = Path(tempfile.mkdtemp(prefix="replay-tests-"))
        atexit.register(shutil.rmtree, out, True)
        _cache.update({w: replay.run(w, out) for w in replay.WORLDS})
    return _cache
