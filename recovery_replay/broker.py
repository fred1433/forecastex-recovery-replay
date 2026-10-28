"""Simulated broker: a separate process whose state survives controller restarts.

It speaks JSON lines shaped after TWS API requests and callbacks (payloads flattened). It is
not TWS and was never compared with a live session. Three worlds share the same pre-crash
script and differ in what happens while the controller is dead:
  A   TWS with the Trade Log set to 7 days: the evidence covers the outage.
  B1  IB Gateway, executions only since midnight; our order 105 filled before midnight.
  B2  same history; 105 was rejected and another strategy bought the same contracts.
B1 and B2 must look identical from the controller's side at 00:10.
"""
import argparse
import itertools
import json
from datetime import datetime
import socket
import threading

from .contracts import ET, LAX, MIA, vt

OWN, OTHER = 11, 7


class Broker:
    def __init__(self, world):
        self.world, self.lock = world, threading.RLock()
        self.next_id, self.perm, self.nexec = 101, 7_000_000, 0
        self.orders, self.execs, self.calls, self.notes = {}, [], [], []
        self.pos = {(LAX.key, "C"): 5, ("SEA-20261028-51", "C"): 2}  # held by other strategies
        self.conn, self.subscribed, self.held = None, False, []
        self.now = vt("23:40")
        self._order(OTHER, 501, LAX.key, "C", 3, 41, "other-strategy")
        self._order(0, 0, "SEA-20261028-51", "P", 5, 30, "manual TWS order")

    # ---- state helpers -------------------------------------------------------------------------
    def _order(self, cid, oid, key, right, qty, lmt, ref):
        self.perm += 1
        o = {"clientId": cid, "orderId": oid, "permId": self.perm, "key": key, "right": right, "qty": qty,
             "lmt": lmt, "orderRef": ref, "filled": 0, "status": "Submitted", "done_at": None}
        self.orders[(cid, oid)] = o
        return o

    def _contract(self, o):
        return {"secType": "OPT", "exchange": "FORECASTX", "right": o["right"], "localKey": o["key"]}

    def _status(self, o, stream="unsolicited"):
        return {"cb": "orderStatus", "orderId": o["orderId"], "clientId": o["clientId"], "permId": o["permId"],
                "status": o["status"], "filled": o["filled"], "remaining": o["qty"] - o["filled"], "_req": stream}

    def _open(self, o, stream="unsolicited", cb="openOrder"):
        return {"cb": cb, "orderId": o["orderId"], "clientId": o["clientId"], "permId": o["permId"],
                "orderRef": o["orderRef"], "contract": self._contract(o), "status": o["status"],
                "filled": o["filled"], "totalQuantity": o["qty"], "lmtPrice": o["lmt"], "_req": stream}

    def emit(self, msg):
        if self.conn:  # callbacks sent while nobody is connected are simply lost
            try:
                self.conn.sendall((json.dumps(msg) + "\n").encode())
            except OSError:
                self.conn = None

    def fill(self, o, n, price=None, correction_of=None):
        self.nexec += 1
        e = {"execId": f"{o['permId']:08x}.{self.nexec:06x}.01.01", "orderId": o["orderId"],
             "clientId": o["clientId"], "permId": o["permId"], "orderRef": o["orderRef"], "shares": n,
             "price": o["lmt"] if price is None else price, "time": self.now.isoformat(), "key": o["key"],
             "right": o["right"]}
        who = "other strategy" if o["clientId"] == OTHER else str(o["orderId"])
        self.notes.append((self.now, f"{who} correction .02" if correction_of else f"{who} fills {n}"))
        if correction_of:
            e = {**correction_of, "execId": correction_of["execId"][:-2] + "02", "price": price}
        else:
            o["filled"] += n
            self.pos[(o["key"], o["right"])] = self.pos.get((o["key"], o["right"]), 0) + n
            if o["filled"] == o["qty"]:
                o["status"], o["done_at"] = "Filled", self.now
        self.execs.append(e)
        self.emit(self._exec(e, -1))
        if not correction_of:
            self.emit(self._status(o))
            self._position(o["key"], o["right"])
        return e

    def _exec(self, e, req, stream="unsolicited"):
        return {"cb": "execDetails", "reqId": req, "contract": {"secType": "OPT", "exchange": "FORECASTX",
                "right": e["right"], "localKey": e["key"]}, "execution": {k: e[k] for k in (
                    "execId", "orderId", "clientId", "permId", "orderRef", "shares", "price", "time")}, "_req": stream}

    def _position(self, key, right):
        if self.subscribed:
            self.emit({"cb": "position", "contract": {"localKey": key, "right": right},
                       "pos": self.pos.get((key, right), 0), "_req": "unsolicited"})

    def net_out(self, key):
        """IB nets opposing Yes and No positions; here it happens late, on purpose."""
        self.notes.append((self.now, "Yes and No netted"))
        n = min(self.pos.get((key, "C"), 0), self.pos.get((key, "P"), 0))
        self.pos[(key, "C")] -= n
        self.pos[(key, "P")] -= n

    def visible(self, when):
        if self.world == "A":
            return True
        midnight = self.now.astimezone(ET).replace(hour=0, minute=0, second=0, microsecond=0)
        return when is not None and when >= midnight

    # ---- scripted market activity ----------------------------------------------------------------
    def clock(self, t):
        own = lambda i: self.orders.get((OWN, i))  # noqa: E731
        live = {"23:45:00": lambda: self._out_of_order_fill(own(101), 4),
                "23:49:00": lambda: self.fill(own(104), own(104)["qty"]),
                "23:49:20": lambda: self.fill(own(102), own(102)["qty"])}
        self.now = t
        hhmm = t.astimezone(ET).strftime("%H:%M:%S")
        if hhmm in live:
            live[hhmm]()

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
        own105 = self.orders.get((OWN, 105))
        self.now = vt("23:50"); self.notes.append((self.now, "1100 (simulated nightly reset)"))
        if self.world == "A":
            self.now = vt("23:52"); first = self.fill(own105, 3)
            self.now = vt("23:53"); self.fill(own105, 0, price=first["price"], correction_of=first)
            self.now = vt("23:55"); self.fill(self.orders[(OWN, 103)], 4)
            self.now = vt("23:56"); self.fill(self.orders[(OTHER, 501)], 2)
        elif self.world == "B1":
            self.now = vt("23:52"); self.fill(own105, own105["qty"])
        else:
            self.now = vt("23:50:30"); own105["status"], own105["done_at"] = "Cancelled", self.now
            self.notes.append((self.now, "105 rejected"))
            self.now = vt("23:52")
            self.fill(self._order(OTHER, 502, LAX.key, "P", own105["qty"], own105["lmt"], "other-strategy"),
                      own105["qty"])
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
        self.calls.append({k: m[k] for k in m if k != "contract"})
        if op == "connect":
            self.cid = m["clientId"]
            self.emit({"cb": "nextValidId", "orderId": self.next_id})
        elif op == "_blip":  # a short loss of IB connectivity, announced by system message codes
            self.notes.append((self.now, f"{m['code']} (short simulated loss of connectivity)"))
            self.emit({"cb": "error", "code": m["code"]})
        elif op == "_clock":
            self.clock(datetime.fromisoformat(m["t"]))
        elif op == "placeOrder":
            self.place(m)
        elif op == "cancelOrder":
            o = self.orders[(self.cid, m["orderId"])]  # only our own ids are ever sent here
            o["status"] = "PendingCancel"
            self.emit(self._status(o))
            if self.world == "A" and o["right"] == "C" and o["key"] == LAX.key:
                self.fill(o, 2)  # executes while the cancel is pending
                self.notes[-1] = (self.now, f"{o['orderId']} fills 2 while its cancel is pending")
            if o["status"] != "Filled":
                o["status"], o["done_at"] = "Cancelled", self.now
            self.emit(self._status(o))
        elif op == "reqAllOpenOrders":
            for o in self.orders.values():
                if o["status"] in ("Submitted", "PendingCancel"):
                    self.emit(self._open(o, "open"))
                    self.emit(self._status(o, "open"))
            self.emit({"cb": "openOrderEnd", "_req": "open"})
        elif op == "reqCompletedOrders":
            for o in self.orders.values():
                if o["status"] in ("Filled", "Cancelled") and self.visible(o["done_at"]):
                    self.emit(self._open(o, "completed", "completedOrder"))
            self.emit({"cb": "completedOrdersEnd", "_req": "completed"})
        elif op == "reqExecutions":
            for e in self.execs:
                if self.visible(datetime.fromisoformat(e["time"])):
                    self.emit(self._exec(e, m["reqId"], "exec"))
            self.emit({"cb": "execDetailsEnd", "reqId": m["reqId"], "_req": "exec"})
            o = self.orders.get((OWN, 105))
            if self.world == "A" and o and o["filled"] < o["qty"]:
                self.fill(o, o["qty"] - o["filled"])  # after the executions reply, before the positions one
                self.notes[-1] = (self.now, f"105 fills {o['filled'] - 3} while the replies are being collected")
                self.emit(self._status(o))
        elif op == "reqPositions":
            self.subscribed = True
            for (key, right), q in sorted(self.pos.items()):
                self.emit({"cb": "position", "contract": {"localKey": key, "right": right}, "pos": q, "_req": "pos"})
            self.emit({"cb": "positionEnd", "_req": "pos"})

    def place(self, m):
        oid, o = m["orderId"], self.orders.get((self.cid, m["orderId"]))
        if o and o["status"] == "Submitted":  # same id, still working: a modification
            o["lmt"], o["qty"] = m["order"]["lmtPrice"], m["order"]["totalQuantity"]
        elif o or oid < self.next_id:
            return self.emit({"cb": "error", "code": 103, "orderId": oid, "msg": "Duplicate order id"})
        else:
            self.next_id = oid + 1
            c = m["contract"]
            o = self._order(self.cid, oid, c["localKey"], c["right"], m["order"]["totalQuantity"],
                            m["order"]["lmtPrice"], m["order"]["orderRef"])
        if oid == 105:
            self.notes.append((self.now, "105 received, acknowledgement never sent"))
            return  # accepted (or, in B2, about to be rejected) but the acknowledgement never arrives
        ack = [self._open(o), self._status(o)]
        if oid == 102:
            self.notes.append((self.now, "acknowledgement of 102 held back"))
            self.held = ack  # late acknowledgement, delivered with the next request
            return
        for a in ack:
            self.emit(a)

    def truth(self):
        """Hidden state, for the test suite and the figure only. The controller never asks for it."""
        own = {}
        for e in self.execs:
            if e["clientId"] == OWN and not e["execId"].endswith(".02"):
                own[e["key"]] = own.get(e["key"], 0) + (e["shares"] if e["right"] == "C" else -e["shares"])
        return {"world": self.world, "own_net": own, "notes": [(t.isoformat(), s) for t, s in self.notes],
                "calls": self.calls}

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
    ap.add_argument("--world", choices=["A", "B1", "B2"], required=True)
    b, srv = Broker(ap.parse_args().world), socket.create_server(("127.0.0.1", 0))
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
