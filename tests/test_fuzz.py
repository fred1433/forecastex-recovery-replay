"""Deterministic fuzzing of callback order at restart, invariants checked after every callback."""
import random
import sqlite3
from contextlib import closing
import unittest

from recovery_replay.contracts import LAX, MIA, SHEETS, net, vt
from recovery_replay.ledger import Journal, Reconciler

from . import runs

NOW = vt("00:10", 29)


def interleavings(callbacks, seed):
    streams = {}
    for c in callbacks:
        streams.setdefault(c.get("_req", "connect"), []).append(c)
    rng, out = random.Random(seed), []
    late = streams.pop("unsolicited", []) if seed % 4 == 0 else []  # worst case: unsolicited after every reply
    queues = [list(s) for s in streams.values()]
    while any(queues):  # uniform over interleavings that keep each stream's own order
        q = rng.choices(queues, weights=[len(q) for q in queues])[0]
        out.append(q.pop(0))
    return out + late


_recorded = {}


def recorded(world):
    if world not in _recorded:
        j = Journal(runs()[world]["db"])
        _recorded[world] = j.get("restart_callbacks")
        j.db.close()
    return _recorded[world]


def replay_once(world, seed):
    run = runs()[world]
    mem = sqlite3.connect(":memory:", isolation_level=None)
    with closing(sqlite3.connect(run["precrash"])) as src:
        src.backup(mem)
    j = Journal(None, conn=mem)
    rec = Reconciler(j, 11, NOW)
    ckpt, seen, history = j.get("checkpoint"), {}, []
    seen.update({r["root"]: (r["right"], r["shares"], r["contract"], r["client_id"])
                 for r in j.db.execute("SELECT * FROM exec")})
    for cb in interleavings(recorded(world), seed):
        rec.feed(cb)
        if cb["cb"] == "execDetails":
            e = cb["execution"]
            seen[e["execId"].rsplit(".", 1)[0]] = (cb["contract"]["right"], e["shares"], cb["contract"]["localKey"],
                                                   e["clientId"])
        for key in SHEETS:  # invariant: each execution counted once, corrections replace
            expect = sum(net(r, s) for r, s, k, c in seen.values() if k == key and c == 11)
            assert j.exposure(key, client_id=11) == expect, (seed, key)
        decided = {k: rec.decide(s, ckpt) for k, s in SHEETS.items()}
        history.append({k: d["decision"] + ("?" if d.get("inconsistent") else "") for k, d in decided.items()})
    mem.close()
    return history


class Fuzz(unittest.TestCase):
    def test_world_a_resumes_under_any_interleaving(self):
        waited = 0
        for seed in range(200):
            h = replay_once("A", seed)
            self.assertEqual(h[-1], {MIA.key: "CLOSED", LAX.key: "RESUME"}, seed)
            self.assertFalse([d for d in h if d[LAX.key].startswith("SUSPEND")], seed)
            waited += any(d[LAX.key] == "WAIT?" for d in h)
        self.assertGreater(waited, 0)  # some orders leave the evidence inconsistent for a while: it waits

    def test_worlds_b_never_resume(self):
        for world in ("B1", "B2"):
            for seed in range(200):
                h = replay_once(world, seed)
                self.assertEqual(h[-1][LAX.key], "SUSPEND", (world, seed))
                self.assertFalse([d for d in h if d[LAX.key].startswith("RESUME")], (world, seed))
