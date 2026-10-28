"""Simulated broker: a separate process whose state survives controller restarts.

It speaks JSON lines shaped after TWS API requests and callbacks (payloads flattened). It is
not TWS and was never compared with a live session. Every world shares the pre-crash script
and differs in what happens while the controller is dead, or in one adversarial detail:
  A   executions visible for 7 days (TWS Trade Log setting); completed orders for the day only.
  B1  executions since midnight only (IB Gateway); our refill order filled before midnight.
  B2  same history; the refill was rejected and another strategy bought the same contracts.
  C1  as A, but the refill fills completely while the controller is dead.
  C2  as B1's history; a working Yes order fills and another strategy buys as many No.
  C3  as A, but the acknowledgement of a price change never arrives.
  C4  as A, but the refill's execution carries another permId and orderRef.
The script acts on roles (the n-th order of the API client on a contract side), not on ids.
"""
import argparse
import itertools
import json
import socket
import threading
from datetime import datetime

from .contracts import ET, LAX, MIA, vt

OTHER = 7  # another strategy on the same account, its own API client
ROLES = {"miami yes": (MIA.key, "C", 1), "miami no": (MIA.key, "P", 1), "la yes": (LAX.key, "C", 1),
         "la no": (LAX.key, "P", 1), "la no refill": (LAX.key, "P", 2)}
LONG_HISTORY = {"A", "C1", "C3", "C4"}


class Broker:
    def __init__(self, world, drop_end=None):
        self.world, self.drop_end, self.lock = world, drop_end, threading.RLock()
        self.next_id, self.perm, self.nexec, self.api, self.reported_max = 101, 7_000_000, 0, None, 0
        self.orders, self.execs, self.calls, self.notes, self.pair_sums = {}, [], [], [], []
        self.pos = {(LAX.key, "C"): 5, ("SEA-20261028-51", "C"): 2}  # held by other strategies
        self.conn, self.subscribed, self.held, self.late = None, False, [], []
        self.now = vt("23:40")
        self._order(OTHER, 501, LAX.key, "C", 3, 41, "other-strategy")
        self._order(0, 0, "SEA-20261028-51", "P", 5, 30, "manual TWS order")

    # ---- state helpers -------------------------------------------------------------------------
    def _order(self, cid, oid, key, right, qty, lmt, ref):
        self.perm += 1
        n = 1 + sum(1 for x in self.orders.values() if (x["clientId"], x["key"], x["right"]) == (cid, key, right))
        o = {"clientId": cid, "orderId": oid, "permId": self.perm, "key": key, "right": right, "qty": qty,
             "lmt": lmt, "orderRef": ref, "filled": 0, "status": "Submitted", "done_at": None,
             "role": (key, right, n) if cid == self.api else None}
        self.orders[(cid, oid)] = o
        return o

    def role(self, name):
        return next((o for o in self.orders.values() if o["role"] == ROLES[name]), None)

    def _status(self, o, stream="unsolicited"):
        return {"cb": "orderStatus", "orderId": o["orderId"], "clientId": o["clientId"], "permId": o["permId"],
                "status": o["status"], "filled": o["filled"], "remaining": o["qty"] - o["filled"], "_req": stream}

    def _open(self, o, stream="unsolicited"):
        return {"cb": "openOrder", "orderId": o["orderId"], "clientId": o["clientId"], "permId": o["permId"],
                "orderRef": o["orderRef"], "contract": {"right": o["right"], "localKey": o["key"]},
                "status": o["status"], "filled": o["filled"], "totalQuantity": o["qty"], "lmtPrice": o["lmt"],
                "_req": stream}

    def _completed(self, o):
        """TWS's completedOrder carries neither orderId nor clientId (ibapi decoder): both left at 0."""
        return {**self._open(o, "completed"), "cb": "completedOrder", "orderId": 0, "clientId": 0}

    def emit(self, msg):
        if self.conn and msg["cb"] != self.drop_end:  # callbacks sent while nobody listens are lost
            try:
                self.conn.sendall((json.dumps(msg) + "\n").encode())
            except OSError:
                self.conn = None

    def fill(self, o, n, price=None, correction_of=None, identity=None):
        self.nexec += 1
        who = "other strategy" if o["clientId"] == OTHER else str(o["orderId"])
        if correction_of:
            e = {**correction_of, "execId": correction_of["execId"][:-2] + "02", "price": price}
            self.notes.append((self.now, f"{who} correction .02, price {correction_of['price']}c to {price}c"))
        else:
            e = {"execId": f"{o['permId']:08x}.{self.nexec:06x}.01.01", "orderId": o["orderId"],
                 "clientId": o["clientId"], "permId": o["permId"], "orderRef": o["orderRef"], "shares": n,
                 "price": o["lmt"] if price is None else price, "time": self.now.isoformat(), "key": o["key"],
                 "right": o["right"], **(identity or {})}
            o["filled"] += n
            self.pos[(o["key"], o["right"])] = self.pos.get((o["key"], o["right"]), 0) + n
            if o["filled"] == o["qty"]:
                o["status"], o["done_at"] = "Filled", self.now
            self.notes.append((self.now, f"{who} fills {n}"))
        self.execs.append(e)
        self.emit(self._exec(e, -1))
        if not correction_of:
            self.emit(self._status(o))
            if self.subscribed:
                self.emit({"cb": "position", "contract": {"localKey": o["key"], "right": o["right"]},
                           "pos": self.pos[(o["key"], o["right"])], "_req": "unsolicited"})
        return e

    def _exec(self, e, req, stream="unsolicited"):
        return {"cb": "execDetails", "reqId": req, "contract": {"right": e["right"], "localKey": e["key"]},
                "execution": {k: e[k] for k in ("execId", "orderId", "clientId", "permId", "orderRef", "shares",
                                                 "price", "time")}, "_req": stream}

    def net_out(self, key):
        """IB nets opposing Yes and No positions; here it happens late, on purpose."""
        self.notes.append((self.now, "Yes and No netted"))
        n = min(self.pos.get((key, "C"), 0), self.pos.get((key, "P"), 0))
        self.pos[(key, "C")] -= n
        self.pos[(key, "P")] -= n

    def since_midnight(self, when):
        midnight = self.now.astimezone(ET).replace(hour=0, minute=0, second=0, microsecond=0)
        return when is not None and when >= midnight

    def pair_check(self, key):
        """Highest working Yes bid plus highest working No bid of the API client, after each order."""
        best = {r: max((o["lmt"] for o in self.orders.values() if (o["clientId"], o["key"], o["right"]) ==
                        (self.api, key, r) and o["status"] in ("Submitted", "PendingCancel")), default=None)
                for r in "CP"}
        if None not in best.values():
            self.pair_sums.append((self.now.isoformat(), key, best["C"] + best["P"]))

    # ---- scripted market activity ----------------------------------------------------------------
    def clock(self, t):
        self.now = t
        for e in self.late:  # a reply that comes late, with the next exchange
            e()
        self.late = []
        live = {"23:45:00": ("miami yes", lambda o: self._out_of_order_fill(o, 4)),
                "23:49:00": ("la no", lambda o: self.fill(o, o["qty"] - o["filled"])),
                "23:49:20": ("miami no", lambda o: self.fill(o, o["qty"] - o["filled"]))}
        name, act = live.get(t.astimezone(ET).strftime("%H:%M:%S"), (None, None))
        if name and self.role(name):
            act(self.role(name))

    def _out_of_order_fill(self, o, n):
        """execDetails before any status, then the same orderStatus twice."""
        held, self.conn = self.conn, None
        self.fill(o, n)
        self.notes[-1] = (self.now, f"{o['orderId']} fills {n}: execution sent first, then the same status twice")
        self.conn = held
        self.emit(self._exec(self.execs[-1], -1))
        self.emit(self._status(o))
        self.emit(self._status(o))

    def outage(self):
        """Everything the dead controller cannot see, from 23:50 to 00:05 ET."""
        refill, w = self.role("la no refill"), self.world
        self.now = vt("23:50"); self.notes.append((self.now, "1100 (simulated nightly reset)"))
        if w in ("A", "C3", "C4") and refill:
            bad = {"permId": refill["permId"] + 999, "orderRef": "someone-else"} if w == "C4" else None
            self.now = vt("23:52"); first = self.fill(refill, refill["qty"] - 1, identity=bad)
            self.now = vt("23:53"); self.fill(refill, 0, price=first["price"] - 1, correction_of=first)
        if w in ("A", "C3", "C4"):
            self.now = vt("23:55"); self.fill(self.role("la yes"), 4)
            self.now = vt("23:56"); self.fill(self.orders[(OTHER, 501)], 2)
        elif w in ("B1", "C1"):
            self.now = vt("23:52"); self.fill(refill, refill["qty"])
        elif w == "B2":
            self.now = vt("23:50:30"); refill["status"], refill["done_at"] = "Cancelled", self.now
            self.notes.append((self.now, f"{refill['orderId']} rejected"))
            self.now = vt("23:52")
            self.fill(self._order(OTHER, 502, LAX.key, "P", refill["qty"], refill["lmt"], "other-strategy"),
                      refill["qty"])
        elif w == "C2":
            yes = self.role("la yes")
            self.now = vt("23:55"); self.fill(yes, yes["qty"] - yes["filled"])
            self.now = vt("23:56")
            self.fill(self._order(OTHER, 503, LAX.key, "P", yes["qty"], 10, "other-strategy"), yes["qty"])
        self.now = vt("23:57"); self.net_out(LAX.key)
        self.now = vt("23:59")
        for o in self.orders.values():
            if o["key"] == MIA.key and o["status"] == "Submitted":
                o["status"], o["done_at"] = "Cancelled", self.now  # last trading time reached
        self.notes.append((self.now, "Miami stops trading"))
        self.now = vt("00:05", 29); self.notes.append((self.now, "1102 (simulated)"))
        self.now = vt("00:10", 29)

    # ---- API requests ----------------------------------------------------------------------------
    def handle(self, m):
        op = m["op"]
        self.calls.append({**{k: m[k] for k in m if k != "contract"}, "vt": self.now.isoformat(),
                           "localKey": m.get("contract", {}).get("localKey")})
        if op == "connect":
            self.cid = m["clientId"]
            self.api = self.api or self.cid
            self.emit({"cb": "nextValidId", "orderId": self.next_id})
        elif op == "_blip":  # a short loss of IB connectivity, announced by system message codes
            self.notes.append((self.now, f"{m['code']} (short simulated loss of connectivity)"))
            self.emit({"cb": "error", "code": m["code"]})
        elif op == "_clock":
            self.clock(datetime.fromisoformat(m["t"]))
        elif op == "placeOrder":
            self.place(m)
        elif op == "cancelOrder":
            self.cancel(self.orders[(self.cid, m["orderId"])])  # only our own ids are ever sent here
        elif op == "reqAllOpenOrders":
            for o in self.orders.values():
                if o["status"] in ("Submitted", "PendingCancel"):
                    self.reported_max = max(self.reported_max, o["orderId"])
                    self.emit(self._open(o, "open"))
                    self.emit(self._status(o, "open"))
            self.emit({"cb": "openOrderEnd", "_req": "open"})
        elif op == "reqCompletedOrders":  # orders of the current day only, in every world
            for o in self.orders.values():
                if o["status"] in ("Filled", "Cancelled") and self.since_midnight(o["done_at"]) and \
                        (o["clientId"] != 0 or not m.get("apiOnly")):
                    self.emit(self._completed(o))
            self.emit({"cb": "completedOrdersEnd", "_req": "completed"})
        elif op == "reqExecutions":
            for e in self.execs:
                if self.world in LONG_HISTORY or self.since_midnight(datetime.fromisoformat(e["time"])):
                    self.emit(self._exec(e, m["reqId"], "exec"))
            self.emit({"cb": "execDetailsEnd", "reqId": m["reqId"], "_req": "exec"})
            o = self.role("la no refill")
            if self.world in ("A", "C4") and o and o["status"] == "Submitted":
                n = o["qty"] - o["filled"]
                self.fill(o, n)  # after the executions reply, before the positions one
                self.notes[-1] = (self.now, f"{o['orderId']} fills {n} while the replies are being collected")
        elif op == "reqPositions":
            self.subscribed = True
            for (key, right), q in sorted(self.pos.items()):
                self.emit({"cb": "position", "contract": {"localKey": key, "right": right}, "pos": q, "_req": "pos"})
            self.emit({"cb": "positionEnd", "_req": "pos"})

    def cancel(self, o):
        o["status"] = "PendingCancel"
        self.emit(self._status(o))

        def finish():
            if self.world in LONG_HISTORY and o["role"] == ROLES["la yes"]:
                self.fill(o, 2)  # executes while the cancel is pending
                self.notes[-1] = (self.now, f"{o['orderId']} fills 2 while its cancel is pending")
            if o["status"] != "Filled":
                o["status"], o["done_at"] = "Cancelled", self.now
            self.notes.append((self.now, f"{o['orderId']} cancelled"))
            self.emit(self._status(o))
        if self.world in LONG_HISTORY:
            self.late.append(finish)  # no answer now; it comes with the next exchange
        else:
            finish()

    def place(self, m):
        oid, o = m["orderId"], self.orders.get((self.cid, m["orderId"]))
        created = False
        if o and o["status"] == "Submitted":  # same id, still working: a modification
            o["lmt"], o["qty"] = m["order"]["lmtPrice"], m["order"]["totalQuantity"]
        elif o or oid < self.next_id or oid <= self.reported_max:
            return self.emit({"cb": "error", "code": 103, "orderId": oid, "msg": "Duplicate order id"})
        else:
            self.next_id, c, created = oid + 1, m["contract"], True
            o = self._order(self.cid, oid, c["localKey"], c["right"], m["order"]["totalQuantity"],
                            m["order"]["lmtPrice"], m["order"]["orderRef"])
        self.pair_check(o["key"])
        if created and o["role"] == ROLES["la no refill"]:
            self.notes.append((self.now, f"{oid} received, acknowledgement never sent"))
            return  # accepted (or, in B2, about to be rejected) but the acknowledgement never arrives
        ack = [self._open(o), self._status(o)]
        if created and o["role"] == ROLES["miami no"]:
            self.notes.append((self.now, f"acknowledgement of {oid} held back"))
            self.held = ack  # late acknowledgement, delivered with the next request
            return
        if self.world == "C3" and not created and o["role"] == ROLES["la yes"] and \
                not any(n.startswith("acknowledgement of a change") for _, n in self.notes):
            self.notes.append((self.now, f"acknowledgement of a change to {oid} never sent"))
            return
        for a in ack:
            self.emit(a)

    def truth(self):
        """Hidden state, for the test suite and the figure only. The controller never asks for it.
        Executions are taken in their latest version, so a correction is seen as such."""
        latest = {}
        for e in self.execs:
            root, version = e["execId"].rsplit(".", 1)
            if root not in latest or int(version) > int(latest[root]["execId"].rsplit(".", 1)[1]):
                latest[root] = e
        net, cost = {}, {}
        for e in latest.values():
            if e["clientId"] == self.api:
                net[e["key"]] = net.get(e["key"], 0) + (e["shares"] if e["right"] == "C" else -e["shares"])
                cost[e["key"]] = cost.get(e["key"], 0) + e["shares"] * e["price"]
        return {"world": self.world, "own_net": net, "own_cost": cost, "api_client": self.api,
                "notes": [(t.isoformat(), s) for t, s in self.notes], "calls": self.calls,
                "foreign_ids": sorted({o["orderId"] for o in self.orders.values() if o["clientId"] != self.api}),
                "pair_sums": self.pair_sums}

    def serve(self, sock):
        f = sock.makefile("r")
        first = json.loads(f.readline())
        if "control" in first:
            with self.lock:
                cmd = first["control"]
                if cmd == "outage":
                    self.outage()
                reply = self.truth() if cmd == "truth" else {"ok": cmd}
            sock.sendall((json.dumps(reply, default=str) + "\n").encode())
            sock.close()
            return cmd
        with self.lock:
            self.conn = sock
        try:
            for line in itertools.chain([json.dumps(first)], iter(f.readline, "")):
                with self.lock:
                    if self.held and self.conn is sock:
                        held, self.held = self.held, []
                        for a in held:
                            self.emit(a)
                    self.handle(json.loads(line))
        except OSError:
            pass  # the controller was killed mid-conversation
        with self.lock:
            if self.conn is sock:
                self.conn = None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--world", choices=["A", "B1", "B2", "C1", "C2", "C3", "C4"], required=True)
    ap.add_argument("--drop-end", default=None, help="an end marker this broker never sends")
    a = ap.parse_args()
    b, srv = Broker(a.world, a.drop_end), socket.create_server(("127.0.0.1", 0))
    print(srv.getsockname()[1], flush=True)
    stop = threading.Event()

    def run(s):
        if b.serve(s) == "stop":
            stop.set()
            socket.create_connection(srv.getsockname()).close()  # wake accept()
    while not stop.is_set():
        s, _ = srv.accept()
        if not stop.is_set():
            threading.Thread(target=run, args=(s,), daemon=True).start()


if __name__ == "__main__":
    main()
