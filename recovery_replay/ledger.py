"""SQLite journal and the reconciler that reads broker evidence into it.

The journal is written by the controller only. The intent row is committed before the order
leaves the process, so a crash leaves a trace of what might be at the broker.

States of an order intent: JOURNALED (committed), SENT (written to the socket), UNCERTAIN (no
acknowledgement in time), ACKED (identity established, may be working), PENDING_CANCEL,
FILLED, CANCELLED; after a restart also CLOSED_NO_FILL (not working, no fill in a reply that
appears to cover it; whether the broker ever received it stays undetermined), EXPIRED (absent
after its contract's last trading time), UNRESOLVED. A modification can end NOT_APPLIED.
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


UNSETTLED = ("JOURNALED", "SENT", "UNCERTAIN")
WORKING = UNSETTLED + ("ACKED", "PENDING_CANCEL")


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

    def pending_terms(self, order):
        """Highest lmt and quantity the broker may be applying: acknowledged or still uncertain changes."""
        mods = self.intents("kind='modify' AND order_id=? AND state IN ('ACKED','SENT','UNCERTAIN','JOURNALED')",
                            order["order_id"])
        lmt, qty = self.terms(order)
        return max([lmt] + [m["lmt"] for m in mods]), max([qty] + [m["qty"] for m in mods])

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

    def roots(self):
        return [r[0] for r in self.db.execute("SELECT root FROM exec")]

    def exposure(self, contract, client_id=None, exclude=()):
        q, args = "SELECT root, right, shares FROM exec WHERE contract=?", [contract]
        if client_id is not None:
            q, args = q + " AND client_id=?", args + [client_id]
        return sum(net(r["right"], r["shares"]) for r in self.db.execute(q, args) if r["root"] not in exclude)

    def cost(self, contract, client_id):
        return self.db.execute("SELECT COALESCE(SUM(shares * price), 0) FROM exec WHERE contract=? AND client_id=?",
                               (contract, client_id)).fetchone()[0]


def empty(sheet):
    return {"contract": sheet.key, "decision": "WAIT", "reasons": [], "missing": [], "operator": [],
            "close_no_fill": [], "expired": [], "not_applied": [], "inconsistent": [], "awaiting": [],
            "exec_history_from": None, "own_exposure": None, "broker_net": None}


def finalize(d):
    """After the waiting budget, an undecided contract is suspended, with what is still missing."""
    if d["decision"] == "WAIT":
        d = {**d, "decision": "SUSPEND",
             "reasons": d["reasons"] + d["inconsistent"] + [f"no {e} received" for e in d["awaiting"]]}
    return d


class Reconciler:
    """Integrates callbacks in whatever order they arrive; decides only once every reply has ended.

    Identity is checked on every piece of evidence: openOrder and orderStatus by orderId and
    clientId, then orderRef and permId; completedOrder (which carries neither orderId nor
    clientId) by permId, then orderRef; execDetails by orderId, orderRef and permId. A
    contradiction is a conflict and suspends the contract.

    How far back the executions reply reaches is not configured but inferred, assuming the reply
    is one contiguous window: executions the journal holds and the reply omits place its start
    later; executions the reply contains place it earlier. It is an inference, not a guarantee.
    """
    ENDS = {"openOrderEnd", "completedOrdersEnd", "execDetailsEnd", "positionEnd"}

    def __init__(self, journal, own, now, exec_req=1):
        self.j, self.own, self.now, self.exec_req = journal, own, now, exec_req
        self.ends, self.positions, self.conflicts, self.seen_ids = set(), {}, [], set()
        self.status, self.open_seen, self.terminal_seen = {}, {}, set()
        self.known = {r["root"]: r["vt"] for r in journal.db.execute("SELECT root, vt FROM exec")}
        self.replied, self.next_valid = {}, None

    def exec_window_start(self):
        missing = [vt for root, vt in self.known.items() if root not in self.replied]
        anchors = [vt for vt in self.replied.values() if not missing or vt > max(missing)]
        return datetime.fromisoformat(min(anchors)) if anchors else self.now

    def conflict(self, msg):
        if msg not in self.conflicts:
            self.conflicts.append(msg)

    def bind(self, row, perm, ref):
        """Accept a piece of evidence for an intent only if its identity agrees with the journal."""
        if ref not in (None, "") and ref != row["id"]:
            self.conflict(f"orderId {row['order_id']}: evidence carries orderRef {ref}, journal says {row['id']}")
            return False
        if row["perm_id"] and perm and perm != row["perm_id"]:
            self.conflict(f"orderId {row['order_id']}: evidence carries permId {perm}, journal says {row['perm_id']}")
            return False
        if perm and not row["perm_id"]:
            self.j.set(row["id"], perm_id=perm)
        self.refresh(row["id"])
        return True

    def refresh(self, iid):
        """State of a new-order intent from all the evidence seen so far."""
        row = self.j.intents("id=?", iid)[0]
        if not row["perm_id"]:
            return  # identity not established: a bare status proves nothing about which order it is
        oid = row["order_id"]
        st = self.status.get(oid, {"filled": 0, "terminal": None})
        filled = max(st["filled"], self.j.filled(self.own, oid))
        if filled >= self.j.terms(row)[1] or st["terminal"] == "Filled":
            state = "FILLED"
        elif st["terminal"] == "Cancelled":
            state = "CANCELLED"
        else:
            state = "PENDING_CANCEL" if row["state"] == "PENDING_CANCEL" else "ACKED"
        self.j.set(iid, state=state, filled=filled)

    def note_status(self, oid, filled, status):
        st = self.status.setdefault(oid, {"filled": 0, "terminal": None})
        st["filled"] = max(st["filled"], filled or 0)
        if status in ("Filled", "Cancelled"):
            st["terminal"] = status

    def check_modifications(self, row, lmt, qty):
        for m in self.j.intents("kind='modify' AND order_id=? AND state IN ('JOURNALED','SENT','UNCERTAIN')",
                                row["order_id"]):
            if (m["lmt"], m["qty"]) == (lmt, qty):
                self.j.set(m["id"], state="ACKED", perm_id=row["perm_id"] or self.j.order(row["order_id"])["perm_id"])

    def feed(self, cb):
        kind = cb["cb"]
        if kind in self.ENDS:
            self.ends.add(kind)
        elif kind == "nextValidId":
            self.next_valid = cb["orderId"]
        elif kind in ("openOrder", "orderStatus"):
            oid = cb["orderId"]
            self.seen_ids.add(oid)
            if cb["clientId"] != self.own:
                return  # seen, never bound, never modified
            row = self.j.order(oid)
            if row is None:
                return self.conflict(f"orderId {oid} under our clientId is unknown to the journal")
            self.note_status(oid, cb.get("filled"), cb.get("status"))
            if kind == "openOrder":
                if self.bind(row, cb["permId"], cb.get("orderRef")):
                    self.open_seen[oid] = (cb["lmtPrice"], cb["totalQuantity"])
                    self.check_modifications(row, cb["lmtPrice"], cb["totalQuantity"])
                    self.refresh(row["id"])
            elif row["perm_id"]:
                if cb["permId"] != row["perm_id"]:
                    return self.conflict(f"orderId {oid}: status carries permId {cb['permId']}, "
                                         f"journal says {row['perm_id']}")
                self.refresh(row["id"])
        elif kind == "completedOrder":
            rows = self.j.intents("kind='new' AND perm_id=?", cb["permId"]) or \
                self.j.intents("kind='new' AND id=?", cb.get("orderRef") or "")
            if rows and self.bind(rows[0], cb["permId"], cb.get("orderRef")):
                self.note_status(rows[0]["order_id"], cb.get("filled"), cb.get("status"))
                self.terminal_seen.add(rows[0]["order_id"])
                self.check_modifications(rows[0], cb["lmtPrice"], cb["totalQuantity"])
                self.refresh(rows[0]["id"])
        elif kind == "execDetails":
            c, e = cb["contract"], cb["execution"]
            self.seen_ids.add(e["orderId"])
            if cb["reqId"] == self.exec_req:  # part of our executions reply, not a live report
                self.replied[e["execId"].rsplit(".", 1)[0]] = e["time"]
            result = self.j.apply_exec(e, c["localKey"], c["right"])
            if e["clientId"] == self.own:
                row = self.j.order(e["orderId"])
                if row is None:
                    self.conflict(f"execution {e['execId']} names orderId {e['orderId']}, unknown to the journal")
                else:
                    self.bind(row, e["permId"], e.get("orderRef"))
            return result
        elif kind == "position":
            self.positions[(cb["contract"]["localKey"], cb["contract"]["right"])] = cb["pos"]

    def decide(self, sheet, checkpoint):
        """Pure reading of the journal and the evidence; writes nothing. Same keys in every case."""
        out = empty(sheet)
        out["reasons"] = list(self.conflicts)
        out["awaiting"] = sorted(self.ENDS - self.ends)
        closed = self.now >= sheet.last_trade_utc()
        if out["awaiting"]:  # a contract past its last trading time stays closed, whatever is missing
            return {**out, "decision": "CLOSED" if closed else "WAIT"}
        start = self.exec_window_start()
        out["exec_history_from"] = start.isoformat()
        for it in self.j.intents("contract=? AND kind='new' AND state IN ('JOURNALED','SENT','UNCERTAIN')",
                                 sheet.key):  # no evidence of any kind for this order
            if datetime.fromisoformat(it["vt"]) >= start:
                out["close_no_fill"].append(it["id"])
            else:
                out["reasons"].append(f"{it['id']} (orderId {it['order_id']}, buy {it['qty']} "
                                      f"{'Yes' if it['right'] == 'C' else 'No'}) not found in open orders, "
                                      "completed orders or executions")
                out["missing"].append(f"execution or completed-order record for orderRef {it['id']} between "
                                      f"{_et(it['vt'])} and {_et(start.isoformat())}")
        for it in self.j.intents("contract=? AND kind='new' AND state IN ('ACKED','PENDING_CANCEL')", sheet.key):
            if it["order_id"] in self.open_seen or it["order_id"] in self.terminal_seen:
                continue
            if closed:
                out["expired"].append(it["id"])  # no pairing after the last trading time
            else:
                out["reasons"].append(f"{it['id']} (orderId {it['order_id']}) was working before the restart and "
                                      "appears in no reply")
                out["missing"].append(f"current or terminal state of orderId {it['order_id']} "
                                      f"(permId {it['perm_id']})")
        for m in self.j.intents("contract=? AND kind='modify' AND state IN ('JOURNALED','SENT','UNCERTAIN')",
                                sheet.key):
            parent = self.j.order(m["order_id"])
            if m["order_id"] in self.open_seen or parent["state"] in ("FILLED", "CANCELLED") or closed:
                out["not_applied"].append(m["id"])  # the order shows other terms, or is over
            else:
                out["reasons"].append(f"{m['id']}: change to orderId {m['order_id']} neither seen nor ruled out")
        for it in self.j.intents("contract=? AND kind='new'", sheet.key):
            seen = self.status.get(it["order_id"], {}).get("filled")
            booked = self.j.filled(self.own, it["order_id"])
            if seen is not None and it["perm_id"] and seen != booked:
                out["inconsistent"].append(f"orderId {it['order_id']}: status says {seen} filled, "
                                           f"executions {booked}")
        broker = self.positions.get((sheet.key, "C"), 0) - self.positions.get((sheet.key, "P"), 0)
        delta = broker - checkpoint["net"][sheet.key]
        explained = self.j.exposure(sheet.key, exclude=set(checkpoint["roots"]))
        if delta != explained:  # a late report can explain it: wait, unless the case is already lost
            (out["reasons"] if out["reasons"] else out["inconsistent"]).append(
                f"position moved {delta:+d} since the {_et(checkpoint['vt'])} checkpoint; journal and visible "
                f"executions explain {explained:+d}; {delta - explained:+d} unattributed")
            out["operator"].append("export the TWS Trade Log or the account statement for this contract over the "
                                   "missing window, or confirm in writing that no other strategy in this account "
                                   "trades it; quoting stays suspended until one of the two is supplied")
        out["own_exposure"] = self.j.exposure(sheet.key, client_id=self.own)
        out["broker_net"] = broker
        if closed:
            out["decision"] = "CLOSED"
        elif out["reasons"]:
            out["decision"] = "SUSPEND"
        else:
            out["decision"] = "WAIT" if out["inconsistent"] else "RESUME"
        return out


def _et(iso):
    return f"{datetime.fromisoformat(iso).astimezone(ET):%H:%M:%S} ET"
