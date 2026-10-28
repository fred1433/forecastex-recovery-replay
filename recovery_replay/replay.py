"""python3 -m recovery_replay.replay : runs the three worlds, checks the properties, draws the figure."""
import hashlib
from concurrent.futures import ThreadPoolExecutor
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
from .contracts import ET, LAX, MIA, SHEETS, TERMS, vt
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


def run(world, out, drop_end=None):
    """Broker process, controller process killed with SIGKILL, outage, new controller process."""
    out.mkdir(parents=True, exist_ok=True)
    name = world + (f"_no_{drop_end}" if drop_end else "")
    db = out / f"journal_{name}.sqlite"
    for p in (db, Path(f"{db}-wal"), Path(f"{db}-shm")):
        p.unlink(missing_ok=True)
    py, env = sys.executable, {**os.environ, "PYTHONPATH": str(ROOT)}
    broker = subprocess.Popen([py, "-m", "recovery_replay.broker", "--world", world] +
                              (["--drop-end", drop_end] if drop_end else []), stdout=subprocess.PIPE, text=True,
                              env=env)
    port = int(broker.stdout.readline())
    args = [py, "-m", "recovery_replay.controller", "--db", str(db), "--port", str(port), "--incident",
            str(INCIDENT)]
    live = subprocess.Popen(args + ["--phase", "live", "--kill-at", KILL_AT], stdout=subprocess.PIPE, text=True,
                            env=env)
    killpoint = live.stdout.readline().strip()
    os.kill(live.pid, signal.SIGKILL)
    live.wait()
    precrash = out / f"journal_{name}_precrash.sqlite"
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
    cbs = j.get("restart_callbacks") or []
    j.db.close()
    seen = json.dumps([{k: v for k, v in c.items() if k != "_req"} for c in cbs], sort_keys=True)
    lines = restart.stdout.strip().splitlines()
    return {"world": world, "drop_end": drop_end, "killpoint": killpoint, "live_returncode": live.returncode,
            "restart_returncode": restart.returncode, "decisions": json.loads(lines[-1]) if lines else {},
            "stderr": restart.stderr, "truth": truth, "db": str(db), "precrash": str(precrash),
            "observation_digest": hashlib.sha256(seen.encode()).hexdigest()[:16]}


def audit(db):
    """time -> contract -> intent -> input revision -> calc id -> orderId/permId -> state -> filled."""
    j = Journal(db)
    own = j.get("identity")["clientId"]
    rows = ["ET        contract  intent   kind   side qty lmt  input rev.   calc_id    orderId permId  state"
            "           filled"]
    for it in j.intents():
        filled = j.filled(own, it["order_id"]) if it["kind"] == "new" else ""
        when = datetime.fromisoformat(it["vt"]).astimezone(ET)
        rows.append(f"{when:%H:%M:%S}  {it['contract'][:3]:<8}  {it['id']:<8} {it['kind']:<6} "
                    f"{'Yes' if it['right'] == 'C' else 'No':<4} {it['qty']:>3} {it['lmt']:>3}  {it['input_rev']} "
                    f"{it['calc_id']} {it['order_id']:>7} {it['perm_id'] or '-':<7} {it['state']:<15} {filled:>6}")
    for s in (MIA, LAX):
        rows.append(f"own net exposure {s.key}: {j.exposure(s.key, client_id=own):+d} (Yes minus No), "
                    f"cost {j.cost(s.key, own)} cents")
    j.db.close()
    return "\n".join(rows)


def properties(results):
    a, b1, b2 = results["A"], results["B1"], results["B2"]
    ja, jb = Journal(a["db"]), Journal(b1["db"])  # read-only use, closed with the process
    calls = [c for r in results.values() for c in r["truth"]["calls"]]
    late_mia = [c for c in calls if c["op"] == "placeOrder" and c["localKey"] == MIA.key and
                datetime.fromisoformat(c["vt"]) >= MIA.last_trade_utc()]
    lost = ja.intents("contract=? AND right='P' AND kind='new'", LAX.key)[1]  # the Los Angeles No refill
    killed = ja.intents("contract=? AND right='P' AND kind='new'", MIA.key)[1]  # the Miami No refill
    seen_max = max(c["orderId"] for c in ja.get("restart_callbacks") if c["cb"] == "openOrder")
    new_after = [i["order_id"] for i in ja.intents("kind='new' AND vt>=?", vt("00:10", 29).isoformat())]
    return [
        (f"controller process killed (SIGKILL) at the chosen point ({killed['id']} journaled, not sent)",
         all(r["live_returncode"] == -signal.SIGKILL and r["killpoint"] == f"KILLPOINT {killed['id']}"
             for r in results.values())),
        ("A: Los Angeles resumes, Miami is past its last trading time",
         a["decisions"] == {MIA.key: "CLOSED", LAX.key: "RESUME"}),
        (f"A: {lost['id']} found by orderId with matching orderRef and permId; no second order for it",
         ja.intents("id=?", lost["id"])[0]["perm_id"] is not None and
         sum(1 for c in a["truth"]["calls"] if c["op"] == "placeOrder" and c["orderId"] == lost["order_id"]) == 1),
        (f"A: {killed['id']} closed with no fill, its outcome otherwise undetermined; its id never reused",
         ja.intents("id=?", killed["id"])[0]["state"] == "CLOSED_NO_FILL" and
         not any(c["op"] == "placeOrder" and c["orderId"] == killed["order_id"] for c in calls)),
        (f"A: new ids after the restart exceed every id seen, including the other strategy's {seen_max}",
         bool(new_after) and min(new_after) > seen_max),
        ("A: own exposure and cost equal the broker's hidden truth, correction included",
         all(ja.exposure(k, client_id=11) == v for k, v in a["truth"]["own_net"].items()) and
         all(ja.cost(k, 11) == v for k, v in a["truth"]["own_cost"].items())),
        ("B1 and B2: identical observations, identical decision, different truth",
         b1["observation_digest"] == b2["observation_digest"] and b1["decisions"] == b2["decisions"] ==
         {MIA.key: "CLOSED", LAX.key: "SUSPEND"} and
         b1["truth"]["own_net"].get(LAX.key) != b2["truth"]["own_net"].get(LAX.key)),
        ("B: the suspension names the missing evidence",
         any(f"orderRef {lost['id']}" in m for m in jb.get("decision:" + LAX.key)["missing"])),
        ("our working Yes and No bids never summed to $1.01 or more on any contract, at any order",
         all(s < 101 for r in results.values() for _, _, s in r["truth"]["pair_sums"])),
        ("no Miami order after its last trading time; no reqGlobalCancel; no foreign order touched",
         not late_mia and any(c["localKey"] == MIA.key for c in calls) and
         not any(c["op"] == "reqGlobalCancel" for c in calls) and
         not any(c["op"] in ("cancelOrder", "placeOrder") and c["orderId"] in r["truth"]["foreign_ids"]
                 for r in results.values() for c in r["truth"]["calls"])),
    ]


def main():
    out = ROOT / "out"
    for s in SHEETS.values():
        print(f"{s.key}: {s.venue}, {s.product} {s.op} {s.strike} {s.unit}, {s.market_date}, {s.city}; settles on "
              f"{s.station} ({s.station_name}, {s.lat}, {s.lon}), {s.tz}, last trade 23:59 local; {s.settlement};"
              f" forecast cell {s.grid.rsplit('/', 2)[-2]}/{s.grid.rsplit('/', 1)[-1]}")
    print(f"terms: {TERMS['url']} (modified {TERMS['last_modified'][:10]}, consulted {TERMS['consulted']})\n")
    with ThreadPoolExecutor() as pool:
        results = dict(zip(WORLDS, pool.map(lambda w: run(w, out), WORLDS)))
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
    (ROOT / "docs" / "figure.svg").write_text(figure.render(results, DATA))
    (ROOT / "docs" / "timeline.svg").write_text(figure.timeline(results))
    print("\nwrote docs/figure.svg, docs/timeline.svg, out/audit_A.txt, out/journal_*.sqlite")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
