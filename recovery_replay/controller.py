"""Controller process: quotes through the provider, journals, sends; after a restart it observes first.

Run by replay.py, twice per world: `--phase live` (killed with SIGKILL at a chosen point) and
`--phase restart` (new process, same SQLite file).
"""
import argparse
import json
import socket
import sys
import time
import dataclasses
from datetime import datetime

from . import weather
from .contracts import LAX, LIMIT, MIA, SHEETS, vt
from .ledger import UNSETTLED, WORKING, Journal, Reconciler, finalize
from .quotes import IllustrativeQuoteProvider, admit

ACK_WAIT = 0.3  # wall-clock seconds before an acknowledgement is declared missing


class Conn:
    def __init__(self, port):
        self.s, self.buf = socket.create_connection(("127.0.0.1", port)), b""

    def send(self, **m):
        self.s.sendall((json.dumps(m) + "\n").encode())

    def pump(self, handle, until=lambda: False, wait=ACK_WAIT):
        end = time.monotonic() + wait
        while not until():
            while b"\n" not in self.buf:
                left = end - time.monotonic()
                if left <= 0:
                    return False
                self.s.settimeout(left)
                try:
                    chunk = self.s.recv(65536)
                except (socket.timeout, TimeoutError):
                    return False
                if not chunk:
                    return False
                self.buf += chunk
            line, self.buf = self.buf.split(b"\n", 1)
            handle(json.loads(line))
        return True


class Controller:
    def __init__(self, a):
        if a.client_id == 0:
            sys.exit("refusing to start: clientId 0 can bind manual TWS orders (reqOpenOrders) and modify them")
        self.snaps = {k: weather.frozen(a.incident, s.station) for k, s in SHEETS.items()}  # before any socket
        self.own, self.j = a.client_id, Journal(a.db)
        identity = self.j.get("identity")
        if a.phase == "live" and (identity or self.j.intents()):
            sys.exit("refusing --phase live: this journal already holds an incident; use --phase restart")
        if a.phase == "restart" and identity != {"clientId": self.own}:
            sys.exit(f"refusing to restart as clientId {self.own}: the journal was written by {identity}; only the "
                     "same TWS user and clientId can modify its orders")
        self.j.put("identity", {"clientId": self.own})
        self.conn, self.provider = Conn(a.port), IllustrativeQuoteProvider()
        self.frozen, self.next_valid, self.record, self.now = {}, None, None, vt("23:40")
        self.recon = Reconciler(self.j, self.own, self.now)

    def tell(self, what, /, **d):
        self.j.log(self.now, "controller", what, **d)

    def clock(self, hms, day=28):
        self.now = vt(hms, day)
        self.recon.now = self.now
        self.conn.send(op="_clock", t=self.now.isoformat())
        self.conn.pump(self.handle, wait=0.15)

    # ---- callbacks ---------------------------------------------------------------------------------
    def handle(self, cb):
        if self.record is not None:
            self.record.append(cb)
        result, kind = self.recon.feed(cb), cb["cb"]
        if kind == "nextValidId":
            self.next_valid = cb["orderId"]
        elif kind == "execDetails":
            e = cb["execution"]
            self.tell("execution", execId=e["execId"], orderId=e["orderId"], clientId=e["clientId"],
                      shares=e["shares"], result=result)
        elif kind == "error":
            self.tell("error", code=cb["code"], orderId=cb.get("orderId"))
            if cb["code"] == 1100:  # connectivity between TWS and IB lost: nothing new leaves
                self.frozen.update({k: "1100 connectivity lost" for k in SHEETS if k not in self.frozen})
            elif cb["code"] in (1101, 1102):
                self.frozen = {k: r for k, r in self.frozen.items() if not r.startswith("1100")}
        for k, reason in list(self.frozen.items()):
            ref = reason.split()[0]
            if ref.startswith(("i-", "m-")) and self.j.intents(
                    "id=? AND state NOT IN ('JOURNALED','SENT','UNCERTAIN','PENDING_CANCEL')", ref):
                del self.frozen[k]
                self.tell("freeze lifted", contract=k, evidence=f"{ref} now {self.j.intents('id=?', ref)[0]['state']}")

    # ---- quoting -----------------------------------------------------------------------------------
    def requote(self, sheet, c, label):
        refusal = (f"forecast point {c.grid} is not the contract's {sheet.grid}" if c.grid != sheet.grid else
                   f"{len(c.missing)} of {len(c.hours)} contract-day hours missing" if c.missing else
                   "; ".join(c.problems))
        if refusal:  # admitted before the pricing call, never after
            return self.tell("input refused", contract=sheet.key, input=label, reason=refusal)
        same_input = self.j.get("input:" + sheet.key) == c.revision
        self.j.put("input:" + sheet.key, c.revision)
        inv = self.j.exposure(sheet.key, client_id=self.own)
        last = self.j.get("quote:" + sheet.key)
        if last and (last["input_revision"], last["inventory"]) == (c.revision, inv) and \
                self.now <= datetime.fromisoformat(last["valid_until"]):
            return self.tell("input unchanged, no recalculation", contract=sheet.key, input=label)
        q = self.provider.quote(sheet, c.high, c.revision, inv, self.now)
        bad = admit(q, sheet, c.revision, self.now)
        if bad:
            return self.tell("quote refused", contract=sheet.key, reasons=bad)
        terms = ("yes_cents", "no_cents", "yes_qty", "no_qty")
        same = last is not None and all(last[k] == getattr(q, k) for k in terms)
        self.tell("recalculated", contract=sheet.key, input=label, same_input=same_input, revision=c.revision,
                  calc_id=q.calc_id, high=c.high, yes=[q.yes_cents, q.yes_qty], no=[q.no_cents, q.no_qty],
                  order_terms="unchanged" if same else "changed")
        results = [self.align(sheet, right, q) for right in sorted("CP", key=lambda r: not self.lowers(sheet, r, q))]
        if not {"blocked", "cancel", "refused"} & set(results):  # anything held back is recalculated next time
            self.j.put("quote:" + sheet.key, {**q.__dict__, "inventory": inv})

    def working(self, key, right):
        return self.j.intents(f"contract=? AND right=? AND kind='new' AND state IN {WORKING}", key, right)

    def lowers(self, sheet, right, q):
        """True when this leg moves away from pairing (lower price, smaller size or cancel): it goes first."""
        work = [w for w in self.working(sheet.key, right) if w["state"] == "ACKED"]
        price, target = q.side(right)
        if not work:
            return False
        lmt, total = self.j.terms(work[-1])
        return price < lmt or target < total - work[-1]["filled"]

    def align(self, sheet, right, q):
        price, target = q.side(right)
        work = self.working(sheet.key, right)
        if sheet.key in self.frozen or any(w["state"] != "ACKED" for w in work):
            self.tell("blocked", contract=sheet.key, right=right, reason=self.frozen.get(sheet.key, "uncertain"))
            return "blocked"
        w = work[-1] if work else None
        if w and target == 0:
            return self.cancel(w)
        other = [self.j.pending_terms(o)[0] for o in self.working(sheet.key, "P" if right == "C" else "C")]
        if (target > 0 or w) and other and price + max(other) >= 101:
            self.tell("refused: would pair with our own other side", contract=sheet.key, right=right, lmt=price,
                      other=max(other))
            return "refused"
        if not w and target > 0:
            return self.submit(sheet, right, price, target, q, "new")
        if w and (self.j.terms(w)[0], self.j.terms(w)[1] - w["filled"]) != (price, target):
            return self.submit(sheet, right, price, w["filled"] + target, q, "modify", w)

    def worst_case(self, key, right, extra):
        """Executed plus everything still working, uncertain or pending, on the side that could fill."""
        inv = self.j.exposure(key, client_id=self.own)
        live = sum(self.j.pending_terms(i)[1] - i["filled"] for i in self.working(key, right))
        return inv + (live + extra) * (1 if right == "C" else -1)

    def submit(self, sheet, right, price, qty, q, kind, w=None):
        extra = qty - (self.j.pending_terms(w)[1] if w else 0)
        if abs(self.worst_case(sheet.key, right, max(extra, 0))) > LIMIT:
            self.tell("refused by exposure limit", contract=sheet.key, right=right, qty=qty)
            return "refused"
        if kind == "new":  # above nextValidId, every id in the journal and every id seen from any client
            oid = max(self.next_valid, self.j.max_order_id() + 1, max(self.recon.seen_ids | {0}) + 1)
            self.next_valid, iid, ref = oid + 1, f"i-{oid}", f"i-{oid}"
        else:
            oid, ref = w["order_id"], w["id"]
            iid = f"m-{oid}-{len(self.j.intents('order_id=? AND kind=?', oid, 'modify')) + 1}"
        self.j.add_intent(iid, sheet.key, right, kind, oid, qty, price, q.calc_id, q.input_revision, self.now)
        self.tell("intent", id=iid, kind=kind, orderId=oid, qty=qty, lmt=price, calc_id=q.calc_id)
        role = f"{sheet.key}:{right}:{len(self.j.intents('contract=? AND right=? AND kind=?', sheet.key, right, 'new'))}"
        if kind == "new" and role == self.kill_at:
            print("KILLPOINT", iid, flush=True)
            time.sleep(3600)
        self.conn.send(op="placeOrder", orderId=oid, contract=sheet.tws(right), order={
            "action": "BUY", "orderType": "LMT", "totalQuantity": qty, "lmtPrice": price, "tif": "DAY",
            "orderRef": ref})
        self.j.set(iid, state="SENT")
        self.tell("sent", id=iid)
        if not self.conn.pump(self.handle, lambda: self.j.intents("id=?", iid)[0]["state"] != "SENT"):
            if self.j.intents("id=?", iid)[0]["state"] == "SENT":
                self.j.set(iid, state="UNCERTAIN")
            self.frozen[sheet.key] = f"{iid} unacknowledged"
            self.tell("acknowledgement missing", id=iid, action="no resend; contract frozen until evidence")
            return "blocked"

    def cancel(self, w):
        """A cancel request is not a cancellation: the order counts as working until a terminal status."""
        self.j.set(w["id"], state="PENDING_CANCEL")
        self.tell("cancel requested", orderId=w["order_id"], remaining=self.j.terms(w)[1] - w["filled"])
        self.conn.send(op="cancelOrder", orderId=w["order_id"])
        done = lambda: self.j.intents("id=?", w["id"])[0]["state"] in ("CANCELLED", "FILLED")  # noqa: E731
        if self.conn.pump(self.handle, done, 1):
            row = self.j.intents("id=?", w["id"])[0]
            self.tell("cancel confirmed", orderId=w["order_id"], state=row["state"], filled=row["filled"])
        else:
            self.frozen[w["contract"]] = f"{w['id']} cancel unconfirmed"
            self.tell("cancel unconfirmed", orderId=w["order_id"],
                      action="order still counted as working; contract frozen until a terminal status")
        return "cancel"

    # ---- inputs: the frozen real responses, and labelled synthetic changes of them -------------------
    def admitted(self, snap, sheet, mutate=None):
        """Content plus the provenance problems found at collection time (the replay's clock)."""
        c = weather.content(mutate(snap) if mutate else snap, sheet)
        return dataclasses.replace(c, problems=tuple(weather.freshness(snap, snap.fetched_at)))

    def real(self, sheet, which):
        return self.admitted(self.snaps[sheet.key][which], sheet)

    def warm(self, sheet, delta, coolest=False):
        snap = self.snaps[sheet.key]["latest"]
        hours = weather.content(snap, sheet).hours
        pick = min if coolest else max
        hour = pick((h for h in range(len(hours)) if hours[h][1] is not None), key=lambda h: hours[h][1])
        return self.admitted(snap, sheet, lambda s: weather.bump_hour(s, sheet, hour, delta))

    # ---- phases ------------------------------------------------------------------------------------
    def live(self):
        self.conn.send(op="connect", clientId=self.own)
        self.conn.pump(self.handle, lambda: self.next_valid is not None, 2)
        self.conn.send(op="reqPositions")
        self.conn.pump(self.handle, lambda: "positionEnd" in self.recon.ends, 2)
        pos = self.recon.positions
        self.j.put("checkpoint", {"vt": self.now.isoformat(), "roots": self.j.roots(), "net": {
            k: pos.get((k, "C"), 0) - pos.get((k, "P"), 0) for k in SHEETS}})
        for s in (MIA, LAX):
            self.requote(s, self.real(s, "first"), "real NWS response")
        self.clock("23:44")
        self.conn.send(op="_blip", code=1100)
        self.conn.pump(self.handle, wait=0.15)
        self.requote(LAX, self.warm(LAX, 2), "synthetic: warmest hour +2 degF, during 1100")
        self.conn.send(op="_blip", code=1102)
        self.conn.pump(self.handle, wait=0.15)
        self.clock("23:45")
        self.clock("23:46")
        for s in (MIA, LAX):
            self.requote(s, self.real(s, "first"), "real NWS response")
        self.clock("23:46:30")
        for s in (MIA, LAX):  # a later real response, downloaded again: new timestamps, same content
            self.requote(s, self.real(s, "latest"), "later real NWS response")
        self.clock("23:47")
        self.requote(LAX, self.warm(LAX, 1, coolest=True), "synthetic: coolest hour +1 degF")
        self.clock("23:48")
        self.requote(LAX, self.warm(LAX, 2), "synthetic: warmest hour +2 degF")
        self.clock("23:49")
        self.requote(LAX, self.warm(LAX, 2), "same input, inventory changed")
        self.clock("23:49:20")
        self.requote(MIA, self.real(MIA, "latest"), "same input, inventory changed")

    def restart(self):
        self.now = vt("00:10", 29)
        self.recon, self.record = Reconciler(self.j, self.own, self.now), []
        self.tell("restart", unresolved=[i["id"] for i in self.j.intents(f"state IN {UNSETTLED}")])
        self.frozen = {k: "reconciling" for k in SHEETS}
        self.conn.send(op="connect", clientId=self.own)
        self.conn.pump(self.handle, lambda: self.next_valid is not None, 2)
        for op, extra in (("reqAllOpenOrders", {}), ("reqCompletedOrders", {"apiOnly": True}),
                          ("reqExecutions", {"reqId": self.recon.exec_req}), ("reqPositions", {})):
            self.conn.send(op=op, **extra)
        self.requote(LAX, self.warm(LAX, 2), "refresh during reconciliation")
        self.conn.pump(self.handle, lambda: self.recon.ends == Reconciler.ENDS, 3)
        ckpt = self.j.get("checkpoint")
        for _ in range(3):
            decisions = {k: self.recon.decide(s, ckpt) for k, s in SHEETS.items()}
            if all(d["decision"] != "WAIT" for d in decisions.values()):
                break
            self.conn.pump(self.handle, wait=0.5)
        self.j.put("restart_callbacks", self.record)
        self.record = None
        for k, d in list(decisions.items()):
            d = decisions[k] = finalize(d)
            for key, state in (("close_no_fill", "CLOSED_NO_FILL"), ("expired", "EXPIRED"),
                               ("not_applied", "NOT_APPLIED")):
                for iid in d[key]:
                    self.j.set(iid, state=state)
                    self.tell(state.lower().replace("_", " "), id=iid)
            for it in self.j.intents(f"contract=? AND state IN {UNSETTLED}", k):
                self.j.set(it["id"], state="UNRESOLVED")
            self.j.put("decision:" + k, d)
            self.tell("decision", **d)
        self.frozen = {k: d["decision"] for k, d in decisions.items() if d["decision"] != "RESUME"}
        for k, d in decisions.items():
            if d["decision"] == "SUSPEND":
                for w in self.j.intents("contract=? AND kind='new' AND state='ACKED'", k):
                    self.cancel(w)
            elif d["decision"] == "RESUME":
                sheet = SHEETS[k]
                self.requote(sheet, self.warm(sheet, 2), "after reconciliation")
                self.clock("00:12", 29)
                down = self.real(sheet, "latest")
                down = dataclasses.replace(down, hours=tuple((h, None if v is None else v - 12) for h, v in down.hours))
                self.requote(sheet, down, "synthetic: every hour -12 degF")
                self.clock("00:12:30", 29)
                self.requote(sheet, down, "same input, after the cancel's late answer")
        print(json.dumps({k: d["decision"] for k, d in decisions.items()}), flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--phase", choices=["live", "restart"], required=True)
    ap.add_argument("--incident", required=True, help="directory holding manifest.json and the frozen inputs")
    ap.add_argument("--client-id", type=int, default=11)
    ap.add_argument("--kill-at", default="")
    a = ap.parse_args()
    c = Controller(a)
    c.kill_at = a.kill_at
    c.live() if a.phase == "live" else c.restart()


if __name__ == "__main__":
    main()
