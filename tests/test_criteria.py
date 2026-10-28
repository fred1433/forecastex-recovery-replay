"""One test per row of the criteria table in README.md, on the processes the replay runs."""
import json
import subprocess
import sys
import unittest
from datetime import timedelta

from recovery_replay import replay, weather
from recovery_replay.contracts import LAX, LIMIT, MIA, vt
from recovery_replay.ledger import Journal
from recovery_replay.quotes import IllustrativeQuoteProvider, admit

from . import runs


def calls(world, op):
    return [c for c in runs()[world]["truth"]["calls"] if c["op"] == op]


def events(j, kind):
    return [(w, json.loads(d)) for w, d in j.db.execute("SELECT vt, detail FROM event WHERE kind=?", (kind,))]


class Criteria(unittest.TestCase):
    def setUp(self):
        self.r = runs()
        self.a, self.b1, self.b2 = (Journal(self.r[w]["db"]) for w in ("A", "B1", "B2"))

    def tearDown(self):
        for j in (self.a, self.b1, self.b2):
            j.db.close()

    def test_01_intent_committed_then_crash_before_sending(self):
        self.assertEqual(self.a.intents("id='i-106'")[0]["state"], "NEVER_RECEIVED")
        self.assertEqual(self.b1.intents("id='i-106'")[0]["state"], "UNRESOLVED")
        for w in self.r:
            self.assertFalse([c for c in calls(w, "placeOrder") if c["orderId"] == 106])
        self.assertEqual(self.a.filled(11, 106), 0)

    def test_02_order_accepted_ack_lost_client_killed(self):
        row = self.a.intents("id='i-105'")[0]
        self.assertIsNotNone(row["perm_id"])
        self.assertEqual([c["orderId"] for c in calls("A", "placeOrder")].count(105), 1)
        self.assertEqual(self.r["A"]["live_returncode"], -9)

    def test_03_partial_fill_while_down(self):
        self.assertEqual(self.a.filled(11, 105), 4)
        self.assertEqual(self.a.intents("id='i-105'")[0]["state"], "FILLED")
        for key, true_net in self.r["A"]["truth"]["own_net"].items():
            self.assertEqual(self.a.exposure(key, client_id=11), true_net)

    def test_04_fill_during_cancel_counted_and_replacement_within_limit(self):
        self.assertEqual(self.a.intents("id='i-103'")[0]["state"], "CANCELLED")
        self.assertEqual(self.a.filled(11, 103), 6)
        lmt, total = self.a.terms(self.a.order(107))
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
        self.assertEqual((j.exposure(LAX.key), j.db.execute("SELECT price FROM exec").fetchone()[0]), (-3, 14))

    def test_06_other_strategy_untouched(self):
        for w in self.r:
            self.assertFalse(calls(w, "reqGlobalCancel"))
            self.assertFalse(calls(w, "reqOpenOrders"))
            touched = [c["orderId"] for c in calls(w, "placeOrder") + calls(w, "cancelOrder")]
            self.assertFalse(set(touched) & {0, 501, 502})
        foreign = self.a.db.execute("SELECT SUM(shares) FROM exec WHERE client_id=7").fetchone()[0]
        self.assertEqual(foreign, 2)

    def test_07_insufficient_history_suspends_with_reason(self):
        b1, b2 = self.r["B1"], self.r["B2"]
        self.assertEqual(b1["observation_digest"], b2["observation_digest"])
        self.assertEqual(b1["decisions"], b2["decisions"])
        self.assertEqual(b1["decisions"][LAX.key], "SUSPEND")
        self.assertNotEqual(b1["truth"]["own_net"][LAX.key], b2["truth"]["own_net"][LAX.key])
        d = self.b1.get("decision:" + LAX.key)
        self.assertTrue(any("orderRef i-105" in m for m in d["missing"]) and d["operator"])
        self.assertEqual([c["orderId"] for c in calls("B1", "cancelOrder")], [103])
        self.assertFalse([c for c in calls("B1", "placeOrder") if c["orderId"] > 106])

    def test_08_equivalent_weather_response_changes_no_order(self):
        snap = weather.load(replay.DATA, LAX.station)[-1]
        regrouped = weather.bump_hour(snap, LAX, 12, 0)  # same values, runs split differently
        self.assertNotEqual(regrouped.props["temperature"]["values"], snap.props["temperature"]["values"])
        self.assertEqual(weather.content(regrouped, LAX).revision, weather.content(snap, LAX).revision)
        for step in weather.walk(weather.load(replay.DATA, LAX.station), LAX)[1:]:
            self.assertIn(step["kind"], ("refresh", "revision"))

    def test_09_new_input_same_quote_no_order_change(self):
        at = vt("23:47").isoformat()
        rec = [d for w, d in events(self.a, "recalculated") if w == at]
        self.assertEqual([d["order_terms"] for d in rec], ["unchanged"])
        self.assertFalse([d for w, d in events(self.a, "intent") if w == at])

    def test_10_wrong_date_or_stale_output_refused(self):
        p, now = IllustrativeQuoteProvider(), vt("23:40")
        q = p.quote(LAX, 74, "rev", 0, now)
        self.assertEqual(admit(q, LAX, "rev", now), [])
        self.assertTrue(admit(q, MIA, "rev", now))
        self.assertTrue(admit(q.__class__(**{**q.__dict__, "market_date": "2026-10-27"}), LAX, "rev", now))
        self.assertTrue(admit(q, LAX, "rev", now + timedelta(minutes=6)))
        self.assertTrue(admit(q, LAX, "other-rev", now))
        self.assertTrue(admit(p.quote(MIA, 86, "rev", 0, vt("23:59")), MIA, "rev", vt("23:59")))
        snap = weather.load(replay.DATA, LAX.station)[0]
        self.assertTrue(weather.freshness(snap, snap.fetched_at + timedelta(minutes=45)))  # late response
        self.assertTrue(weather.freshness(snap, snap.update_time + timedelta(hours=13)))  # old source

    def test_client_id_zero_refused(self):
        r = subprocess.run([sys.executable, "-m", "recovery_replay.controller", "--db", ":memory:", "--port", "1",
                            "--phase", "live", "--data", ".", "--history", "7d", "--client-id", "0"],
                           capture_output=True, text=True)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("clientId 0", r.stderr)


class Weather(unittest.TestCase):
    def test_canonical_fahrenheit(self):
        self.assertEqual(weather.to_f(16.666666666666668, "wmoUnit:degC"), 62)
        with self.assertRaises(ValueError):
            weather.to_f(1, "wmoUnit:K")

    def test_max_temperature_does_not_cover_the_contract_day(self):
        snap = weather.load(replay.DATA, LAX.station)[0]
        durations = {weather.parse_valid(v["validTime"])[1] for v in snap.props["maxTemperature"]["values"]}
        self.assertTrue(all(d < timedelta(hours=24) for d in durations))

    def test_synthetic_mutations_are_content_changes(self):
        snap = weather.load(replay.DATA, MIA.station)[0]
        base = weather.content(snap, MIA)
        shifted = weather.content(weather.shift_interval(snap, 1), MIA)
        holed = weather.content(weather.drop_hour(snap, MIA, 15), MIA)
        self.assertNotEqual(shifted.revision, base.revision)
        self.assertTrue(holed.missing and holed.revision != base.revision)
        self.assertEqual(len(base.hours), 24)
        with self.assertRaises(ValueError):
            weather.content(snap, LAX)  # a Miami grid point cannot feed a Los Angeles contract
