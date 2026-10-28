"""SQLite journal and the reconciler that reads broker evidence into it.

The journal is written by the controller only. The intent row is committed before the order
leaves the process, so a crash leaves a trace of what might be at the broker.
"""
import json
import sqlite3
from datetime import datetime

from .contracts import ET, net

SCHEMA = """
CREATE TABLE IF NOT EXISTS intent(id TEXT PRIMARY KEY, contract TEXT, right TEXT, kind TEXT, order_id INT,
  qty INT, lmt INT, calc_id TEXT, input_rev TEXT, state TEXT, perm_id INT, filled INT DEFAULT 0, vt TEXT);
CREATE TABLE IF NOT EXISTS exec(root TEXT PRIMARY KEY, exec_id TEXT, version INT, order_id INT, perm_id INT,
  client_id INT, order_ref TEXT, contract TEXT, right TEXT, shares INT, price INT, vt TEXT);
CREATE TABLE IF NOT EXISTS event(seq INTEGER PRIMARY KEY, vt TEXT, actor TEXT, kind TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
"""


class Journal:
    def __init__(self, path, conn=None):
        self.db = conn or sqlite3.connect(path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)

    def log(self, when, actor, what, /, **detail):
        self.db.execute("INSERT INTO event(vt, actor, kind, detail) VALUES (?,?,?,?)",
                        (when.isoformat(), actor, what, json.dumps(detail, default=str)))

    def get(self, k, default=None):
        row = self.db.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return json.loads(row[0]) if row else default

    def put(self, k, v):
        self.db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, json.dumps(v, default=str)))

    def add_intent(self, iid, contract, right, kind, order_id, qty, lmt, calc_id, rev, when):
        with self.db:  # committed before anything is written to the socket
            self.db.execute("INSERT INTO intent(id, contract, right, kind, order_id, qty, lmt, calc_id, input_rev,"
                            " state, vt) VALUES (?,?,?,?,?,?,?,?,?,'JOURNALED',?)",
                            (iid, contract, right, kind, order_id, qty, lmt, calc_id, rev, when.isoformat()))

    def set(self, iid, **f):
        self.db.execute(f"UPDATE intent SET {', '.join(k + '=?' for k in f)} WHERE id=?", (*f.values(), iid))

    def intents(self, where="1", *args):
        return [dict(r) for r in self.db.execute(f"SELECT * FROM intent WHERE {where} ORDER BY vt, id", args)]

    def order(self, order_id):
        rows = self.intents("order_id=? AND kind='new'", order_id)
        return rows[0] if rows else None

    def terms(self, order):
        """Current (lmt, totalQuantity) of an order: its last acknowledged modification, else as placed."""
        mods = self.intents("kind='modify' AND order_id=? AND state='ACKED'", order["order_id"])
        last = mods[-1] if mods else order
        return last["lmt"], last["qty"]

    def max_order_id(self):
        return self.db.execute("SELECT COALESCE(MAX(order_id), 0) FROM intent").fetchone()[0]

    def apply_exec(self, e, contract, right):
        """Exact duplicate: no effect. Correction (same id up to the last period): replaces."""
        root, version = e["execId"].rsplit(".", 1)
        old = self.db.execute("SELECT exec_id, version FROM exec WHERE root=?", (root,)).fetchone()
        if old and old["exec_id"] == e["execId"]:
            return "duplicate"
        if old and int(version) < old["version"]:
            return "older version ignored"
        self.db.execute("INSERT OR REPLACE INTO exec VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (root, e["execId"], int(version), e["orderId"], e["permId"], e["clientId"], e["orderRef"],
                         contract, right, e["shares"], e["price"], e["time"]))
        return "correction" if old else "new"

    def filled(self, client_id, order_id):
        return self.db.execute("SELECT COALESCE(SUM(shares),0) FROM exec WHERE client_id=? AND order_id=?",
                               (client_id, order_id)).fetchone()[0]

    def exposure(self, contract, client_id=None, since=None):
        q, args = "SELECT right, shares FROM exec WHERE contract=?", [contract]
        if client_id is not None:
            q, args = q + " AND client_id=?", args + [client_id]
        if since is not None:
            q, args = q + " AND vt>=?", args + [since]
        return sum(net(r["right"], r["shares"]) for r in self.db.execute(q, args))


class Reconciler:
    """Integrates callbacks in whatever order they arrive; decides only once every reply has ended."""
    ENDS = {"openOrderEnd", "completedOrdersEnd", "execDetailsEnd", "positionEnd"}

    def __init__(self, journal, own, now, visible_from):
        self.j, self.own, self.now, self.visible_from = journal, own, now, visible_from
        self.ends, self.status, self.positions = set(), {}, {}
        self.next_valid = None

    def feed(self, cb):
        kind = cb["cb"]
        if kind in self.ENDS:
            self.ends.add(kind)
        elif kind == "nextValidId":
            self.next_valid = cb["orderId"]
        elif kind in ("openOrder", "completedOrder", "orderStatus"):
            cid, oid = cb["clientId"], cb["orderId"]
            if cid != self.own:
                return  # seen, never bound, never modified
            self.status[oid] = max(self.status.get(oid, 0), cb.get("filled", 0))
            row = self.j.order(oid) or next(iter(self.j.intents("id=?", cb.get("orderRef", ""))), None)
            if row:
                state = {"Filled": "FILLED", "Cancelled": "CANCELLED"}.get(cb.get("status"), "ACKED")
                if row["state"] in ("FILLED", "CANCELLED") and state == "ACKED":
                    state = row["state"]
                self.j.set(row["id"], perm_id=cb["permId"], state=state, filled=self.status[oid])
        elif kind == "execDetails":
            c = cb["contract"]
            return self.j.apply_exec(cb["execution"], c["localKey"], c["right"])
        elif kind == "position":
            self.positions[(cb["contract"]["localKey"], cb["contract"]["right"])] = cb["pos"]

    def decide(self, sheet, checkpoint):
        """Pure reading of the journal and the evidence; writes nothing."""
        out = {"contract": sheet.key, "reasons": [], "missing": [], "operator": [], "retire": []}
        if self.ends != self.ENDS:
            return {**out, "decision": "WAIT", "reasons": ["replies not complete"]}
        inconsistent = []
        covered = datetime.fromisoformat(checkpoint["vt"]) >= self.visible_from
        for it in self.j.intents("contract=? AND state IN ('JOURNALED','SENT','UNCERTAIN')", sheet.key):
            if datetime.fromisoformat(it["vt"]) >= self.visible_from:
                out["retire"].append(it["id"])
            else:
                out["reasons"].append(f"{it['id']} (orderId {it['order_id']}, buy {it['qty']} "
                                      f"{'Yes' if it['right'] == 'C' else 'No'}) not found in open orders, "
                                      "completed orders or executions")
                out["missing"].append(f"execution or completed-order record for orderRef {it['id']} between "
                                      f"{_et(it['vt'])} and {self.visible_from.astimezone(ET):%H:%M} ET")
        for it in self.j.intents("contract=? AND kind='new' AND state NOT IN ('JOURNALED','SENT','UNCERTAIN')",
                                 sheet.key):
            seen, booked = self.status.get(it["order_id"]), self.j.filled(self.own, it["order_id"])
            if seen is not None and seen != booked:
                inconsistent.append(f"orderId {it['order_id']}: status says {seen} filled, executions {booked}")
        broker = self.positions.get((sheet.key, "C"), 0) - self.positions.get((sheet.key, "P"), 0)
        delta, explained = broker - checkpoint["net"][sheet.key], self.j.exposure(sheet.key, since=checkpoint["vt"])
        if delta != explained:
            if covered:
                inconsistent.append(f"position moved {delta:+d} since checkpoint, executions explain {explained:+d}")
            else:
                out["reasons"].append(f"position moved {delta:+d} since the {_et(checkpoint['vt'])} checkpoint;"
                                      f" journal and visible executions explain {explained:+d};"
                                      f" {delta - explained:+d} unattributed")
                out["operator"].append("export the TWS Trade Log or the account statement for this contract over the "
                                       "missing window, or confirm in writing that no other strategy in this account "
                                       "trades it; quoting stays suspended until one of the two is supplied")
        out["own_exposure"] = self.j.exposure(sheet.key, client_id=self.own)
        out["broker_net"] = broker
        out["inconsistent"] = inconsistent
        if self.now >= sheet.last_trade_utc():
            out["decision"] = "CLOSED"
        elif out["reasons"]:
            out["decision"] = "SUSPEND"
        else:
            out["decision"] = "WAIT" if inconsistent else "RESUME"
        return out


def _et(iso):
    return f"{datetime.fromisoformat(iso).astimezone(ET):%H:%M:%S} ET"
