"""Real NWS gridpoint snapshots: relevant content versus transport noise.

The input is the hourly `temperature` series of /gridpoints/{wfo}/{x},{y}, which the NWS
run-length encodes (`validTime` = start/ISO 8601 duration). It is expanded to every local
hour of the contract day: 24, or 23 and 25 on the days clocks change. `maxTemperature` is not used: its periods (PT13H, sometimes
shorter) do not cover the contract day from 00:00 to 23:59 local time.
"""
import dataclasses
import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

DUR = re.compile(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?)?$")
MAX_SOURCE_AGE = timedelta(hours=12)   # policy parameter, illustrative
MAX_FETCH_AGE = timedelta(minutes=30)  # policy parameter, illustrative


def parse_valid(s):
    start, dur = s.split("/")
    d, h, m = (int(x or 0) for x in DUR.match(dur).groups())
    return datetime.fromisoformat(start), timedelta(days=d, hours=h, minutes=m)


def to_f(value, uom):
    if value is None:
        return None
    if uom.endswith("degC"):
        value = value * 9 / 5 + 32
    elif not uom.endswith("degF"):
        raise ValueError(f"unexpected unit {uom}")
    return math.floor(value + 0.5)  # NDFD works in whole degF; 16.666... degC is 62 degF


@dataclass(frozen=True)
class Snapshot:
    station: str
    fetched_at: datetime
    url: str
    headers: dict
    props: dict
    synthetic: str = ""  # empty for a real response; otherwise the name of the mutation

    @property
    def update_time(self):
        return datetime.fromisoformat(self.props["updateTime"])


def read(path, station):
    d = json.loads(Path(path).read_text())
    fetched = datetime.strptime(d["fetched_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return Snapshot(station, fetched, d["url"], d["headers"], d["body"]["properties"])


def load(directory, station):
    return [read(f, station) for f in sorted(Path(directory).glob(f"{station}_*.json"))]


def frozen(directory, station):
    """The incident's inputs: named in manifest.json and checked against their SHA-256."""
    d = Path(directory)
    m = json.loads((d / "manifest.json").read_text())
    out = {}
    for role, name in m["stations"][station].items():
        if hashlib.sha256((d / name).read_bytes()).hexdigest() != m["sha256"][name]:
            raise SystemExit(f"{name}: hash differs from manifest.json, refusing to run the incident")
        out[role] = read(d / name, station)
    return out


@dataclass(frozen=True)
class Content:
    grid: str
    hours: tuple  # ((utc iso hour, degF or None), ...) for every local hour of the contract day

    @property
    def revision(self):
        return hashlib.sha256(json.dumps([self.grid, self.hours]).encode()).hexdigest()[:12]

    @property
    def high(self):
        vals = [v for _, v in self.hours if v is not None]
        return max(vals) if vals else None

    @property
    def missing(self):
        return [h for h, v in self.hours if v is None]


def content(snap, sheet):
    """Relevant content only: variable, canonical unit, place, validity interval kept per hour."""
    if snap.url != sheet.grid:
        raise ValueError(f"snapshot for {snap.url}, contract forecast point is {sheet.grid}")
    series = snap.props["temperature"]
    runs = [(*parse_valid(v["validTime"]), v["value"]) for v in series["values"]]
    t, end = sheet.day_utc()
    hours = []
    while t < end:  # 23, 24 or 25 hours, whatever the local day really lasts
        val = next((to_f(v, series["uom"]) for s, d, v in runs if s <= t < s + d), None)
        hours.append((t.isoformat(), val))
        t += timedelta(hours=1)
    return Content(snap.url, tuple(hours))


def freshness(snap, now):
    """Freshness and provenance, checked apart from content: equal values prove nothing here."""
    problems = []
    if now - snap.update_time > MAX_SOURCE_AGE:
        problems.append(f"source updateTime {snap.props['updateTime']} older than {MAX_SOURCE_AGE}")
    if now - snap.fetched_at > MAX_FETCH_AGE:
        problems.append(f"response collected at {snap.fetched_at:%H:%MZ}, older than {MAX_FETCH_AGE}")
    if snap.fetched_at < snap.update_time:
        problems.append("collected before its own updateTime")
    return problems


def walk(snaps, sheet):
    """Each response in collection order: refresh (same content) or revision (content changed)."""
    steps, prev = [], None
    for s in snaps:
        c = content(s, sheet)
        step = {"fetched": s.fetched_at, "update_time": s.props["updateTime"], "revision": c.revision,
                "high": c.high, "missing": len(c.missing), "synthetic": s.synthetic,
                "freshness": freshness(s, s.fetched_at)}
        if prev is None:
            step["kind"] = "first"
        elif c.revision == prev[1].revision:
            step["kind"] = "refresh"
            step["metadata_changed"] = [k for k in ("Last-Modified", "ETag", "Expires", "Date")
                                        if s.headers.get(k) != prev[0].headers.get(k)]
        else:
            old = dict(prev[1].hours)
            changed = [h for h, v in c.hours if old.get(h) != v]
            step["kind"] = "revision"
            step["changed_hours"] = len(changed)
            step["value_changed"] = any(old.get(h) is not None and v is not None for h, v in c.hours
                                        if h in changed)
        steps.append(step)
        prev = (s, c)
    return steps


# Synthetic mutations, always labelled as such: they test cases the real data may not show.
def _mutate(snap, label, fn, **changes):
    props = json.loads(json.dumps(snap.props))
    props["temperature"]["values"] = [fn(v) for v in props["temperature"]["values"]]
    return dataclasses.replace(snap, props=props, synthetic=label, **changes)


def shift_interval(snap, hours=1):
    def f(v):
        s, d = v["validTime"].split("/")
        return {**v, "validTime": (datetime.fromisoformat(s) + timedelta(hours=hours)).isoformat() + "/" + d}
    return _mutate(snap, f"interval shifted {hours:+d} h", f)


def bump_hour(snap, sheet, local_hour, delta_f):
    """Change one local hour of the contract day by delta_f degF, splitting its run if needed."""
    start, _ = sheet.day_utc()
    t = start + timedelta(hours=local_hour)
    vals = []
    for v in snap.props["temperature"]["values"]:
        s, d = parse_valid(v["validTime"])
        if s <= t < s + d:
            for k in range(int(d.total_seconds() // 3600)):
                h = s + timedelta(hours=k)
                val = v["value"] + (delta_f * 5 / 9 if h == t else 0)
                vals.append({"validTime": h.isoformat() + "/PT1H", "value": val})
        else:
            vals.append(v)
    props = json.loads(json.dumps(snap.props))
    props["temperature"]["values"] = vals
    return dataclasses.replace(snap, props=props, synthetic=f"{local_hour:02d}:00 local {delta_f:+d} degF")


def drop_hour(snap, sheet, local_hour):
    """The run covering that local hour loses its value (a response with a hole in it)."""
    t = sheet.day_utc()[0] + timedelta(hours=local_hour)

    def f(v):
        s, d = parse_valid(v["validTime"])
        return {**v, "value": None} if s <= t < s + d else v
    return _mutate(snap, f"value missing around {local_hour:02d}:00 local", f)
