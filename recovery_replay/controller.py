"""Controller process: quotes through the provider, journals, sends; after a restart it observes first.

Run by replay.py, twice per world: `--phase live` (killed with SIGKILL at a chosen point) and
`--phase restart` (new process, same SQLite file).
"""
import argparse
import json
import socket
import sys
import time
from datetime import datetime, timedelta

from . import weather
from .contracts import ET, LAX, LIMIT, MIA, SHEETS, vt
from .ledger import Journal, Reconciler
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
        self.own, self.j, self.conn = a.client_id, Journal(a.db), Conn(a.port)
        self.provider, self.data, self.history = IllustrativeQuoteProvider(), a.data, a.history
        self.frozen, self.next_valid, self.record, self.now = {}, None, None, vt("23:40")
        self.recon = Reconciler(self.j, self.own, self.now, self.now)

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
        elif kind == "openOrder" and cb["clientId"] == self.own:
            for m in self.j.intents("kind='modify' AND order_id=? AND state='SENT'", cb["orderId"]):
                if (m["lmt"], m["qty"]) == (cb["lmtPrice"], cb["totalQuantity"]):
                    self.j.set(m["id"], state="ACKED", perm_id=cb["permId"])
        for k, reason in list(self.frozen.items()):
            ref = reason.split()[0]
            if ref.startswith(("i-", "m-")) and self.j.intents("id=? AND state NOT IN ('SENT','UNCERTAIN')", ref):
                del self.frozen[k]
                self.tell("freeze lifted", contract=k, evidence=f"{ref} acknowledged")

    # ---- quoting -----------------------------------------------------------------------------------
    def requote(self, sheet, c, label):
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
        self.tell("recalculated", contract=sheet.key, input=label, revision=c.revision, calc_id=q.calc_id,
                  high=c.high, yes=[q.yes_cents, q.yes_qty], no=[q.no_cents, q.no_qty],
                  order_terms="unchanged" if same else "changed")
        results = []
        for right in "CP":
            results.append(self.align(sheet, right, q))
            if results[-1] == "cancelled":
                return self.requote(sheet, c, label + ", after cancel")
        if "blocked" not in results:  # a blocked change stays pending: the next input recalculates
            self.j.put("quote:" + sheet.key, {**q.__dict__, "inventory": inv})

    def align(self, sheet, right, q):
        price, target = q.side(right)
        work = self.j.intents("contract=? AND right=? AND state IN ('JOURNALED','SENT','UNCERTAIN','ACKED') "
                              "AND kind='new'", sheet.key, right)
        if sheet.key in self.frozen or any(w["state"] != "ACKED" for w in work):
            self.tell("blocked", contract=sheet.key, right=right, reason=self.frozen.get(sheet.key, "uncertain"))
            return "blocked"
        w = work[-1] if work else None
        if w and target == 0:
            return self.cancel(w)
        if not w and target > 0:
            self.submit(sheet, right, price, target, q, "new")
        elif w and (self.j.terms(w)[0], self.j.terms(w)[1] - w["filled"]) != (price, target):
            self.submit(sheet, right, price, w["filled"] + target, q, "modify", w)

    def worst_case(self, key, right, extra):
        """Executed plus everything still working or uncertain, on the side that could fill."""
        inv = self.j.exposure(key, client_id=self.own)
        live = sum(self.j.terms(i)[1] - i["filled"] for i in self.j.intents(
            "contract=? AND right=? AND kind='new' AND state IN ('JOURNALED','SENT','UNCERTAIN','ACKED')", key, right))
        return inv + (live + extra) * (1 if right == "C" else -1)

    def submit(self, sheet, right, price, qty, q, kind, w=None):
        extra = qty - (self.j.terms(w)[1] if w else 0)
        if abs(self.worst_case(sheet.key, right, extra)) > LIMIT:
            return self.tell("refused by exposure limit", contract=sheet.key, right=right, qty=qty)
        if kind == "new":
            oid = max(self.next_valid, self.j.max_order_id() + 1)  # never reuse an id the journal knows
            self.next_valid, iid, ref = oid + 1, f"i-{oid}", f"i-{oid}"
        else:
            oid, ref = w["order_id"], w["id"]
            iid = f"m-{oid}-{len(self.j.intents('order_id=? AND kind=?', oid, 'modify')) + 1}"
        self.j.add_intent(iid, sheet.key, right, kind, oid, qty, price, q.calc_id, q.input_revision, self.now)
        self.tell("intent", id=iid, kind=kind, orderId=oid, qty=qty, lmt=price, calc_id=q.calc_id)
        if iid == self.kill_at:
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

    def cancel(self, w):
        self.tell("cancel requested", orderId=w["order_id"], remaining=w["qty"] - w["filled"])
        self.conn.send(op="cancelOrder", orderId=w["order_id"])
        self.conn.pump(self.handle, lambda: self.j.intents("id=?", w["id"])[0]["state"] in ("CANCELLED", "FILLED"), 1)
        row = self.j.intents("id=?", w["id"])[0]
        self.tell("cancel confirmed", orderId=w["order_id"], state=row["state"], filled=row["filled"])
        return "cancelled"

    # ---- inputs ------------------------------------------------------------------------------------
    def inputs(self, sheet):
        snaps = [s for s in weather.load(self.data, sheet.station) if not weather.freshness(s, s.fetched_at)]
        return [weather.content(s, sheet) for s in snaps[-2:]]

    # ---- phases ------------------------------------------------------------------------------------
    def live(self):
        self.conn.send(op="connect", clientId=self.own)
        self.conn.pump(self.handle, lambda: self.next_valid is not None, 2)
        self.conn.send(op="reqPositions")
        self.conn.pump(self.handle, lambda: "positionEnd" in self.recon.ends, 2)
        pos = self.recon.positions
        self.j.put("checkpoint", {"vt": self.now.isoformat(), "net": {
            k: pos.get((k, "C"), 0) - pos.get((k, "P"), 0) for k in SHEETS}})
        real = {k: self.inputs(s) for k, s in SHEETS.items()}
        for s in (MIA, LAX):
            self.requote(s, real[s.key][0], "real NWS response")
        self.clock("23:44")
        self.conn.send(op="_blip", code=1100)
        self.conn.pump(self.handle, wait=0.15)
        self.requote(LAX, warm(self.data, 2), "synthetic: warmest hour +2 degF, during 1100")
        self.conn.send(op="_blip", code=1102)
        self.conn.pump(self.handle, wait=0.15)
        self.clock("23:45")
        self.clock("23:46")
        for s in (MIA, LAX):
            self.requote(s, real[s.key][-1], "latest real NWS response")
        self.clock("23:47")
        self.requote(LAX, warm(self.data, 1, coolest=True), "synthetic: coolest hour +1 degF")
        self.clock("23:48")
        self.requote(LAX, warm(self.data, 2), "synthetic: warmest hour +2 degF")
        self.clock("23:49")
        self.requote(LAX, warm(self.data, 2), "same input, inventory changed")
        self.clock("23:49:20")
        self.requote(MIA, real[MIA.key][-1], "same input, inventory changed")

    def restart(self):
        self.now = vt("00:10", 29)
        seen_from = self.now - timedelta(days=7) if self.history == "7d" else \
            self.now.astimezone(ET).replace(hour=0, minute=0, second=0, microsecond=0)
        self.recon, self.record = Reconciler(self.j, self.own, self.now, seen_from), []
        self.tell("restart", unresolved=[i["id"] for i in self.j.intents("state IN ('JOURNALED','SENT','UNCERTAIN')")],
                  history_visible_from=seen_from.isoformat())
        self.frozen = {k: "reconciling" for k in SHEETS}
        self.conn.send(op="connect", clientId=self.own)
        self.conn.pump(self.handle, lambda: self.next_valid is not None, 2)
        for op, extra in (("reqAllOpenOrders", {}), ("reqCompletedOrders", {"apiOnly": True}),
                          ("reqExecutions", {"reqId": 1}), ("reqPositions", {})):
            self.conn.send(op=op, **extra)
        self.requote(LAX, warm(self.data, 2), "refresh during reconciliation")
        self.conn.pump(self.handle, lambda: self.recon.ends == Reconciler.ENDS, 3)
        ckpt = self.j.get("checkpoint")
        for _ in range(3):
            decisions = {k: self.recon.decide(s, ckpt) for k, s in SHEETS.items()}
            if all(d["decision"] != "WAIT" for d in decisions.values()):
                break
            self.conn.pump(self.handle, wait=0.5)
        self.j.put("restart_callbacks", self.record)
        self.record = None
        for k, d in decisions.items():
            if d["decision"] == "WAIT":
                d.update(decision="SUSPEND", reasons=d["inconsistent"])
            for iid in d["retire"]:
                self.j.set(iid, state="NEVER_RECEIVED")
                self.tell("closed as never received", id=iid, why="absent from every reply covering its lifetime")
            for it in self.j.intents("contract=? AND state IN ('JOURNALED','SENT','UNCERTAIN')", k):
                self.j.set(it["id"], state="UNRESOLVED")
            self.j.put("decision:" + k, d)
            self.tell("decision", **d)
        self.frozen = {k: d["decision"] for k, d in decisions.items() if d["decision"] != "RESUME"}
        for k, d in decisions.items():
            if d["decision"] == "SUSPEND":
                for w in self.j.intents("contract=? AND kind='new' AND state='ACKED'", k):
                    self.cancel(w)
            elif d["decision"] == "RESUME":
                self.requote(SHEETS[k], warm(self.data, 2), "after reconciliation")
                self.clock("00:12", 29)
                c = weather.content(weather.load(self.data, LAX.station)[-1], LAX)
                down = weather.Content(c.grid, tuple((h, None if v is None else v - 12) for h, v in c.hours))
                self.requote(SHEETS[k], down, "synthetic: every hour -12 degF")
        print(json.dumps({k: d["decision"] for k, d in decisions.items()}), flush=True)


def warm(data, delta, coolest=False):
    """Latest real LAX response with one hour changed: a labelled synthetic revision."""
    snap = weather.load(data, LAX.station)[-1]
    hours = weather.content(snap, LAX).hours
    pick = min if coolest else max
    hour = pick((h for h in range(24) if hours[h][1] is not None), key=lambda h: hours[h][1])
    return weather.content(weather.bump_hour(snap, LAX, hour, delta), LAX)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--phase", choices=["live", "restart"], required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--history", choices=["7d", "since-midnight"], required=True)
    ap.add_argument("--client-id", type=int, default=11)
    ap.add_argument("--kill-at", default="")
    a = ap.parse_args()
    c = Controller(a)
    c.kill_at = a.kill_at
    c.live() if a.phase == "live" else c.restart()


if __name__ == "__main__":
    main()
