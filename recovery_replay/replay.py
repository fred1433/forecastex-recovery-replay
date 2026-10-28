"""python3 -m recovery_replay.replay : runs the three worlds, checks the properties, draws the figure."""
import hashlib
from datetime import datetime
from contextlib import closing
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
from pathlib import Path

from . import figure
from .contracts import LAX, MIA, SHEETS, TERMS
from .ledger import Journal

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "nws"  # every collected response: the figure's top panel
INCIDENT = ROOT / "data" / "incident"  # the incident's frozen, hashed inputs
WORLDS = ("A", "B1", "B2")
KILL_AT = f"{MIA.key}:P:2"  # the second new Miami No order: journaled, then the process dies


def control(port, cmd):
    with socket.create_connection(("127.0.0.1", port), timeout=30) as s:
        s.sendall((json.dumps({"control": cmd}) + "\n").encode())
        return json.loads(s.makefile("r").readline())


def run(world, out):
    """Broker process, controller process killed with SIGKILL, outage, new controller process."""
    out.mkdir(parents=True, exist_ok=True)
    db = out / f"journal_{world}.sqlite"
    for p in (db, Path(f"{db}-wal"), Path(f"{db}-shm")):
        p.unlink(missing_ok=True)
    py, env = sys.executable, {**os.environ, "PYTHONPATH": str(ROOT)}
    broker = subprocess.Popen([py, "-m", "recovery_replay.broker", "--world", world], stdout=subprocess.PIPE,
                              text=True, env=env)
    port = int(broker.stdout.readline())
    args = [py, "-m", "recovery_replay.controller", "--db", str(db), "--port", str(port), "--incident",
            str(INCIDENT)]
    live = subprocess.Popen(args + ["--phase", "live", "--kill-at", KILL_AT], stdout=subprocess.PIPE, text=True,
                            env=env)
    killpoint = live.stdout.readline().strip()
    os.kill(live.pid, signal.SIGKILL)
    live.wait()
    precrash = out / f"journal_{world}_precrash.sqlite"
    precrash.unlink(missing_ok=True)
    with closing(sqlite3.connect(db)) as src, closing(sqlite3.connect(precrash)) as dst:
        src.backup(dst)
    control(port, "outage")
    restart = subprocess.run(args + ["--phase", "restart"], capture_output=True, text=True, env=env, timeout=60)
    truth = control(port, "truth")
    control(port, "stop")
    broker.wait(timeout=10)
    broker.stdout.close()
    live.stdout.close()
    j = Journal(db)
    cbs = j.get("restart_callbacks")
    j.db.close()
    seen = json.dumps([{k: v for k, v in c.items() if k != "_req"} for c in cbs], sort_keys=True)
    return {"world": world, "killpoint": killpoint, "live_returncode": live.returncode,
            "decisions": json.loads(restart.stdout.strip().splitlines()[-1]), "stderr": restart.stderr,
            "truth": truth, "db": str(db), "precrash": str(precrash),
            "observation_digest": hashlib.sha256(seen.encode()).hexdigest()[:16]}


def audit(db):
    """intent -> input revision -> orderId/permId -> broker state -> own exposure after it."""
    j = Journal(db)
    rows = ["intent   kind   side qty lmt  input rev.   calc_id    orderId permId  broker state   filled"]
    for it in j.intents():
        filled = j.filled(11, it["order_id"]) if it["kind"] == "new" else ""
        rows.append(f"{it['id']:<8} {it['kind']:<6} {'Yes' if it['right'] == 'C' else 'No':<4} {it['qty']:>3} "
                    f"{it['lmt']:>3}  {it['input_rev']} {it['calc_id']} {it['order_id']:>7} "
                    f"{it['perm_id'] or '-':<7} {it['state']:<14} {filled:>6}")
    for s in (MIA, LAX):
        rows.append(f"own net exposure {s.key}: {j.exposure(s.key, client_id=11):+d} (Yes minus No)")
    return "\n".join(rows)


def properties(results):
    a, b1, b2 = results["A"], results["B1"], results["B2"]
    ja, jb = Journal(a["db"]), Journal(b1["db"])  # read-only use, closed with the process
    calls = [c for r in results.values() for c in r["truth"]["calls"]]
    late_mia = [c for c in calls if c["op"] == "placeOrder" and c["localKey"] == MIA.key and
                datetime.fromisoformat(c["vt"]) >= MIA.last_trade_utc()]
    lost = ja.intents("contract=? AND right='P' AND kind='new'", LAX.key)[1]  # the LAX No refill
    killed = ja.intents("contract=? AND right='P' AND kind='new'", MIA.key)[1]  # the Miami No refill
    return [
        (f"controller process killed (SIGKILL) at the chosen point ({killed['id']} journaled, not sent)",
         all(r["live_returncode"] == -signal.SIGKILL and r["killpoint"] == f"KILLPOINT {killed['id']}"
             for r in results.values())),
        ("A: LAX resumes, MIA is past its last trading time",
         a["decisions"] == {MIA.key: "CLOSED", LAX.key: "RESUME"}),
        (f"A: {lost['id']} found by orderId with matching orderRef, permId recorded; no second order for it",
         ja.intents("id=?", lost["id"])[0]["perm_id"] is not None and
         sum(1 for c in a["truth"]["calls"] if c["op"] == "placeOrder" and c["orderId"] == lost["order_id"]) == 1),
        (f"A: {killed['id']} never reached the broker, closed, id never reused",
         ja.intents("id=?", killed["id"])[0]["state"] == "NEVER_RECEIVED" and
         not any(c["op"] == "placeOrder" and c["orderId"] == killed["order_id"] for c in calls)),
        ("A: own exposure equals the broker's hidden truth",
         all(ja.exposure(k, client_id=11) == v for k, v in a["truth"]["own_net"].items())),
        ("B1 and B2: identical observations, identical decision, different truth",
         b1["observation_digest"] == b2["observation_digest"] and b1["decisions"] == b2["decisions"] ==
         {MIA.key: "CLOSED", LAX.key: "SUSPEND"} and
         b1["truth"]["own_net"].get(LAX.key) != b2["truth"]["own_net"].get(LAX.key)),
        ("B: the suspension names the missing evidence",
         any(f"orderRef {lost['id']}" in m for m in jb.get("decision:" + LAX.key)["missing"])),
        ("no MIA order after its last trading time; no reqGlobalCancel; no foreign order touched",
         not late_mia and any(c["localKey"] == MIA.key for c in calls) and
         not any(c["op"] == "reqGlobalCancel" for c in calls) and
         not any(c["op"] in ("cancelOrder", "placeOrder") and c["orderId"] in (0, 501, 502) for c in calls)),
    ]


def main():
    out = ROOT / "out"
    for s in SHEETS.values():
        print(f"{s.key}: {s.venue}, {s.product} {s.op} {s.strike} {s.unit}, {s.market_date}, {s.city}; settles on "
              f"{s.station} ({s.station_name}, {s.lat}, {s.lon}), {s.tz}, last trade 23:59 local; {s.settlement};"
              f" forecast cell {s.grid.rsplit('/', 2)[-2]}/{s.grid.rsplit('/', 1)[-1]}")
    print(f"terms: {TERMS['url']} (modified {TERMS['last_modified'][:10]}, consulted {TERMS['consulted']})\n")
    results = {w: run(w, out) for w in WORLDS}
    for r in results.values():
        print(f"world {r['world']:<2} restart decisions: {r['decisions']}  observation digest {r['observation_digest']}")
        if r["stderr"].strip():
            print(r["stderr"])
    print()
    ok = True
    for name, passed in properties(results):
        ok &= bool(passed)
        print(f"  {'pass' if passed else 'FAIL'}  {name}")
    text = audit(results["A"]["db"])
    (out / "audit_A.txt").write_text(text + "\n")
    print("\nAudit trail, world A\n" + text)
    svg = figure.render(results, DATA)
    (ROOT / "docs" / "figure.svg").write_text(svg)
    print("\nwrote docs/figure.svg, out/audit_A.txt, out/journal_*.sqlite")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
