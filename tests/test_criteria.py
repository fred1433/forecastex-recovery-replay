"""The criteria table, the judges' counter-examples and the boundary checks, on the processes the replay runs."""
import dataclasses
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path

from recovery_replay import replay, weather
from recovery_replay.contracts import LAX, LIMIT, MIA, vt
from recovery_replay.controller import Controller
from recovery_replay.ledger import Journal, Reconciler
from recovery_replay.quotes import IllustrativeQuoteProvider, admit

from . import runs

RESTART = vt("00:10", 29).isoformat()


def calls(world, op):
    return [c for c in runs()[world]["truth"]["calls"] if c["op"] == op]


def events(j, kind):
    return [(w, json.loads(d)) for w, d in j.db.execute("SELECT vt, detail FROM event WHERE kind=? ORDER BY seq",
                                                         (kind,))]


def journal(world):
    return Journal(runs()[world]["db"])


def controller_without_broker(snaps=None):
    """A controller object on an in-memory journal and a connection that never answers."""
    class Silent:
        def __init__(self):
            self.sent = []

        def send(self, **m):
            self.sent.append(m)

        def pump(self, handle, until=lambda: False, wait=0):
            return until()
    c = Controller.__new__(Controller)
    c.j, c.own, c.now, c.frozen, c.next_valid, c.kill_at, c.record = Journal(":memory:"), 11, vt("23:48"), {}, 10, "", None
    c.provider, c.conn, c.snaps = IllustrativeQuoteProvider(), Silent(), snaps or {}
    c.recon = Reconciler(c.j, 11, c.now)
    return c


class Criteria(unittest.TestCase):
    def setUp(self):
        self.a, self.b1, self.b2 = journal("A"), journal("B1"), journal("B2")

    def tearDown(self):
        for j in (self.a, self.b1, self.b2):
            j.db.close()

    def test_01_intent_committed_then_crash_before_sending(self):
        self.assertEqual(self.a.intents("id='i-106'")[0]["state"], "CLOSED_NO_FILL")
        self.assertEqual(self.b1.intents("id='i-106'")[0]["state"], "UNRESOLVED")
        for w in ("A", "B1", "B2"):
            self.assertFalse([c for c in calls(w, "placeOrder") if c["orderId"] == 106])
        self.assertEqual(self.a.filled(11, 106), 0)

    def test_02_order_accepted_ack_lost_client_killed(self):
        row = self.a.intents("id='i-105'")[0]
        self.assertIsNotNone(row["perm_id"])
        cbs = self.a.get("restart_callbacks")
        self.assertIn({"orderRef": "i-105", "permId": row["perm_id"]},
                      [{"orderRef": c["orderRef"], "permId": c["permId"]} for c in cbs if c["cb"] == "openOrder"])
        self.assertEqual([c["orderId"] for c in calls("A", "placeOrder")].count(105), 1)
        self.assertEqual(runs()["A"]["live_returncode"], -9)

    def test_03_partial_fill_while_down(self):
        self.assertEqual((self.a.filled(11, 105), self.a.intents("id='i-105'")[0]["state"]), (4, "FILLED"))
        truth = runs()["A"]["truth"]
        for key, true_net in truth["own_net"].items():
            self.assertEqual(self.a.exposure(key, client_id=11), true_net)
            self.assertEqual(self.a.cost(key, 11), truth["own_cost"][key])

    def test_04_fill_during_a_cancel_answered_late(self):
        kinds = [k for k, in self.a.db.execute("SELECT kind FROM event WHERE kind LIKE 'cancel%' ORDER BY seq")]
        self.assertEqual(kinds, ["cancel requested", "cancel unconfirmed"])  # no answer within the wait
        lifted = [d for _, d in events(self.a, "freeze lifted") if d["evidence"].startswith("i-103")]
        self.assertEqual(lifted[-1]["evidence"], "i-103 now CANCELLED")  # the late answer, with its fill
        self.assertEqual((self.a.intents("id='i-103'")[0]["state"], self.a.filled(11, 103)), ("CANCELLED", 6))
        refill = self.a.intents("contract=? AND right='P' AND kind='new' AND vt>=?", LAX.key, RESTART)[0]
        lmt, total = self.a.terms(refill)
        inv = self.a.exposure(LAX.key, client_id=11)
        self.assertEqual((inv, total), (-8, 6))
        self.assertGreaterEqual(inv - total, -LIMIT)  # worst case if the whole No order fills

    def test_05_duplicate_then_correction(self):
        results = [d["result"] for _, d in events(self.a, "execution")]
        self.assertGreaterEqual(results.count("duplicate"), 3)
        self.assertEqual(results.count("correction"), 1)
        j = Journal(":memory:")
        self.addCleanup(j.db.close)
        e = {"execId": "00ab.0001.01.01", "orderId": 5, "clientId": 11, "permId": 9, "orderRef": "i-5",
             "shares": 3, "price": 15, "time": "t"}
        self.assertEqual(j.apply_exec(e, LAX.key, "P"), "new")
        self.assertEqual(j.apply_exec(e, LAX.key, "P"), "duplicate")
        self.assertEqual(j.apply_exec({**e, "execId": "00ab.0001.01.02", "price": 14}, LAX.key, "P"), "correction")
        self.assertEqual(j.apply_exec(e, LAX.key, "P"), "older version ignored")
        self.assertEqual((j.exposure(LAX.key), j.cost(LAX.key, 11)), (-3, 42))

    def test_06_other_strategy_untouched(self):
        for w in runs():
            truth = runs()[w]["truth"]
            self.assertFalse(calls(w, "reqGlobalCancel") or calls(w, "reqOpenOrders"))
            touched = {c["orderId"] for c in calls(w, "placeOrder") + calls(w, "cancelOrder")}
            self.assertFalse(touched & set(truth["foreign_ids"]), w)
        self.assertEqual(self.a.db.execute("SELECT SUM(shares) FROM exec WHERE client_id=7").fetchone()[0], 2)

    def test_07_insufficient_history_suspends_with_reason(self):
        b1, b2 = runs()["B1"], runs()["B2"]
        self.assertEqual(b1["observation_digest"], b2["observation_digest"])
        self.assertEqual(b1["decisions"], b2["decisions"])
        self.assertEqual(b1["decisions"][LAX.key], "SUSPEND")
        self.assertNotEqual(b1["truth"]["own_net"][LAX.key], b2["truth"]["own_net"][LAX.key])
        d = self.b1.get("decision:" + LAX.key)
        self.assertTrue(any("orderRef i-105" in m for m in d["missing"]) and d["operator"])
        self.assertEqual([c["orderId"] for c in calls("B1", "cancelOrder")], [103])
        self.assertFalse([c for c in calls("B1", "placeOrder") if c["vt"] >= RESTART])

    def test_08_equivalent_weather_response_changes_no_order(self):
        at = vt("23:46:30").isoformat()  # a later real response: new timestamps and headers, same content
        skipped = [d["contract"] for w, d in events(self.a, "input unchanged, no recalculation") if w == at]
        self.assertEqual(skipped, [MIA.key, LAX.key])
        self.assertFalse([d for w, d in events(self.a, "recalculated") if w == at])
        self.assertFalse(self.a.intents("vt=?", at))  # no order touched

    def test_09_new_input_same_quote_no_order_change(self):
        at = vt("23:47").isoformat()
        rec = [d for w, d in events(self.a, "recalculated") if w == at]
        self.assertEqual([d["order_terms"] for d in rec], ["unchanged"])
        self.assertFalse([d for w, d in events(self.a, "intent") if w == at])

    def test_10_wrong_date_stale_or_negative_output_refused(self):
        p, now = IllustrativeQuoteProvider(), vt("23:40")
        q = p.quote(LAX, 74, "rev", 0, now)
        self.assertEqual(admit(q, LAX, "rev", now), [])
        self.assertTrue(admit(q, MIA, "rev", now))
        self.assertTrue(admit(dataclasses.replace(q, market_date="2026-10-27"), LAX, "rev", now))
        self.assertTrue(admit(q, LAX, "rev", now + timedelta(minutes=6)))
        self.assertTrue(admit(q, LAX, "other-rev", now))
        self.assertTrue(admit(dataclasses.replace(q, yes_qty=-1), LAX, "rev", now))
        self.assertTrue(admit(p.quote(MIA, 86, "rev", 0, vt("23:59")), MIA, "rev", vt("23:59")))

    # ---- the counter-examples of the review, on the full controller and broker path -------------------
    def test_uncertain_order_filled_while_down_is_not_closed(self):  # world C1
        j = journal("C1")
        self.addCleanup(j.db.close)
        self.assertEqual((j.intents("id='i-105'")[0]["state"], j.filled(11, 105)), ("FILLED", 4))
        self.assertEqual(runs()["C1"]["decisions"][LAX.key], "RESUME")
        self.assertNotIn("i-105", j.get("decision:" + LAX.key)["close_no_fill"])

    def test_order_working_before_crash_absent_from_every_reply_suspends(self):  # world C2
        j = journal("C2")
        self.addCleanup(j.db.close)
        d = j.get("decision:" + LAX.key)
        self.assertEqual(d["decision"], "SUSPEND")
        self.assertIn("i-103 (orderId 103) was working before the restart and appears in no reply", d["reasons"])

    def test_uncertain_modification_recognised_from_the_order_seen(self):  # world C3
        pre, j = sqlite3.connect(runs()["C3"]["precrash"]), journal("C3")
        self.addCleanup(j.db.close)
        with closing(pre):
            self.assertEqual(pre.execute("SELECT state FROM intent WHERE id='m-103-1'").fetchone()[0], "UNCERTAIN")
        self.assertEqual(j.intents("id='m-103-1'")[0]["state"], "ACKED")
        self.assertEqual(j.terms(j.order(103))[0], 81)

    def test_execution_with_another_identity_is_a_conflict(self):  # world C4
        j = journal("C4")
        self.addCleanup(j.db.close)
        d = j.get("decision:" + LAX.key)
        self.assertEqual(d["decision"], "SUSPEND")
        self.assertTrue(any("orderRef someone-else" in r for r in d["reasons"]))

    def test_missing_end_marker_suspends_without_crashing(self):
        for end in sorted(Reconciler.ENDS):
            r = runs()[f"A without {end}"]
            self.assertEqual(r["restart_returncode"], 0, r["stderr"])
            d = journal(f"A without {end}").get("decision:" + LAX.key)
            self.assertEqual(d["decision"], "SUSPEND", end)
            self.assertIn(f"no {end} received", d["reasons"])

    # ---- identity, ids, history ---------------------------------------------------------------------
    def test_new_ids_exceed_every_id_seen(self):
        new = [i["order_id"] for i in self.a.intents("kind='new' AND vt>=?", RESTART)]
        self.assertTrue(new and min(new) >= 502)  # the other strategy's 501 was seen at restart

    def test_history_depth_is_inferred_not_declared(self):
        self.assertEqual(self.a.get("decision:" + LAX.key)["exec_history_from"], vt("23:45").isoformat())
        for j in (self.b1, self.b2):
            self.assertEqual(j.get("decision:" + LAX.key)["exec_history_from"], RESTART)
            self.assertEqual({i["state"] for i in j.intents("id IN ('i-105','i-106')")}, {"UNRESOLVED"})

    def test_orderref_mismatch_on_an_open_order_suspends(self):
        mem = sqlite3.connect(":memory:", isolation_level=None)
        with closing(sqlite3.connect(runs()["A"]["precrash"])) as src:
            src.backup(mem)
        j = Journal(None, conn=mem)
        rec = Reconciler(j, 11, vt("00:10", 29))
        for cb in self.a.get("restart_callbacks"):
            if cb["cb"] == "openOrder" and cb["orderId"] == 105:
                cb = {**cb, "orderRef": "someone-else"}
            rec.feed(cb)
        d = rec.decide(LAX, j.get("checkpoint"))
        self.assertEqual(d["decision"], "SUSPEND")
        self.assertIn("orderRef someone-else", " ".join(d["reasons"]))
        mem.close()

    def test_restart_under_another_identity_or_live_on_a_used_journal_is_refused(self):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        db = tmp / "j.sqlite"
        with closing(sqlite3.connect(runs()["A"]["db"])) as src, closing(sqlite3.connect(db)) as dst:
            src.backup(dst)
        for extra, needle in ((["--phase", "restart", "--client-id", "12"], "journal was written by"),
                              (["--phase", "live"], "already holds an incident")):
            r = subprocess.run([sys.executable, "-m", "recovery_replay.controller", "--db", str(db), "--port", "1",
                                "--incident", str(replay.INCIDENT)] + extra, capture_output=True, text=True)
            self.assertNotEqual(r.returncode, 0)
            self.assertIn(needle, r.stderr)

    def test_client_id_zero_refused(self):
        r = subprocess.run([sys.executable, "-m", "recovery_replay.controller", "--db", ":memory:", "--port", "1",
                            "--phase", "live", "--incident", ".", "--client-id", "0"],
                           capture_output=True, text=True)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("clientId 0", r.stderr)

    # ---- the controller's own boundaries --------------------------------------------------------------
    def test_lowering_leg_first_and_raise_held_without_proof(self):
        snap = weather.load(replay.DATA, LAX.station)[0]
        c = controller_without_broker({LAX.key: {"latest": snap}})
        self.addCleanup(c.j.db.close)
        for iid, right, lmt in (("i-1", "C", 56), ("i-2", "P", 40)):
            c.j.add_intent(iid, LAX.key, right, "new", int(iid[2:]), 10, lmt, "calc", "rev", vt("23:40"))
            c.j.set(iid, state="ACKED", perm_id=int(iid[2:]))
        c.requote(LAX, c.warm(LAX, 2), "synthetic: warmest hour +2 degF")  # Yes to 81, No to 15
        sent = [(m["orderId"], m["order"]["lmtPrice"]) for m in c.conn.sent if m["op"] == "placeOrder"]
        self.assertEqual(sent, [(2, 15)])  # the No leg goes down first; with no ack, Yes is not raised
        self.assertEqual([d["right"] for _, d in events(c.j, "blocked")], ["C"])

    def test_inputs_refused_before_pricing(self):
        snap = weather.load(replay.DATA, LAX.station)[0]
        c = controller_without_broker({LAX.key: {"latest": snap}})
        self.addCleanup(c.j.db.close)
        holed = c.admitted(snap, LAX, lambda s: weather.drop_hour(s, LAX, 14))
        stale = dataclasses.replace(snap, props={**snap.props, "updateTime": "2026-10-27T20:00:00+00:00"})
        miami = weather.content(weather.load(replay.DATA, MIA.station)[0], MIA)
        c.requote(LAX, holed, "a response with a hole")
        c.requote(LAX, c.admitted(stale, LAX), "an old forecast")
        c.requote(LAX, miami, "Miami's forecast")
        reasons = [d["reason"] for _, d in events(c.j, "input refused")]
        self.assertEqual(len(reasons), 3)
        self.assertIn("contract-day hours missing", reasons[0])
        self.assertIn("older than", reasons[1])
        self.assertIn("is not the contract's", reasons[2])
        self.assertFalse(events(c.j, "recalculated") or c.conn.sent or c.j.get("quote:" + LAX.key))

    def test_frozen_inputs_are_checked(self):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        shutil.copytree(replay.INCIDENT, tmp / "incident")
        f = tmp / "incident" / "KLAX_20261028T131951Z.json"
        f.write_text(f.read_text().replace('"value": 15', '"value": 16', 1))
        r = subprocess.run([sys.executable, "-m", "recovery_replay.controller", "--db", ":memory:", "--port", "1",
                            "--phase", "live", "--incident", str(tmp / "incident")], capture_output=True, text=True)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("hash differs", r.stderr)


class Weather(unittest.TestCase):
    def test_canonical_fahrenheit(self):
        self.assertEqual(weather.to_f(16.666666666666668, "wmoUnit:degC"), 62)
        with self.assertRaises(ValueError):
            weather.to_f(1, "wmoUnit:K")

    def test_local_day_has_its_real_length(self):
        snap = weather.load(replay.DATA, LAX.station)[0]
        fall = dataclasses.replace(LAX, market_date=date(2026, 11, 1))  # clocks go back in Los Angeles
        spring = dataclasses.replace(LAX, market_date=date(2027, 3, 14))  # clocks go forward
        self.assertEqual(len(weather.content(snap, fall).hours), 25)
        self.assertEqual(len(weather.content(snap, spring).hours), 23)
        self.assertEqual(len(weather.content(snap, LAX).hours), 24)
        self.assertEqual(weather.content(snap, fall).hours[-1][0], "2026-11-02T07:00:00+00:00")  # 23:00 PST

    def test_max_temperature_does_not_cover_the_contract_day(self):
        snap = weather.load(replay.DATA, LAX.station)[0]
        durations = {weather.parse_valid(v["validTime"])[1] for v in snap.props["maxTemperature"]["values"]}
        self.assertTrue(all(d < timedelta(hours=24) for d in durations))

    def test_synthetic_mutations_are_content_changes(self):
        snap = weather.load(replay.DATA, MIA.station)[0]
        base = weather.content(snap, MIA)
        shifted = weather.content(weather.shift_interval(snap, 1), MIA)
        holed = weather.content(weather.drop_hour(snap, MIA, 15), MIA)
        regrouped = weather.content(weather.bump_hour(snap, MIA, 12, 0), MIA)  # same values, runs split
        self.assertNotEqual(shifted.revision, base.revision)
        self.assertTrue(holed.missing and holed.revision != base.revision)
        self.assertEqual(regrouped.revision, base.revision)
        with self.assertRaises(ValueError):
            weather.content(snap, LAX)  # a Miami grid point cannot feed a Los Angeles contract
