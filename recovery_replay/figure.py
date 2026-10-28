"""The README figure, drawn from the replay's own outputs. Plain SVG, no dependency."""
import json
import textwrap
from datetime import datetime, timedelta
from html import escape

from . import weather
from .contracts import ET, LAX, MIA, vt
from .ledger import Journal

W = 1040
INK, MUTED, RULE, GREY, SEA, OCHRE = "#1d2633", "#5f6b7a", "#d9dde3", "#aab2bd", "#0b6584", "#9a6200"
FONT = "Charter, 'Bitstream Charter', 'Iowan Old Style', Georgia, serif"


def text(x, y, s, size=11.5, fill=INK, anchor="start", weight="normal"):
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" fill="{fill}" text-anchor="{anchor}" '
            f'font-weight="{weight}">{escape(s)}</text>')


def line(x1, y1, x2, y2, stroke=RULE, width=1, dash=""):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{stroke}" stroke-width="{width}"{d}/>'


def station(data, x0, y0, w, sheet):
    """Hourly forecast for the contract day, one line per distinct content; responses on a strip below."""
    snaps = weather.load(data, sheet.station)
    steps = weather.walk(snaps, sheet)
    contents = [weather.content(s, sheet) for s in snaps]
    h = 120
    out = [text(x0, y0, f"{sheet.city}, settles on {sheet.station}, high {sheet.op} {sheet.strike} °F", 13,
                weight="bold")]
    if not steps:
        return out
    vals = [v for c in contents for _, v in c.hours if v is not None] + [sheet.strike]
    lo, hi = min(vals) - 2, max(vals) + 2
    top = y0 + 16
    fx = lambda i: x0 + 26 + i / 23 * (w - 26)  # noqa: E731
    fy = lambda v: top + h - (v - lo) / (hi - lo) * h  # noqa: E731
    for v in range(lo + (lo % 2), hi + 1, 4):
        out.append(line(x0 + 26, fy(v), x0 + w, fy(v), "#f0f2f5"))
        out.append(text(x0 + 20, fy(v) + 4, f"{v}", 10, MUTED, "end"))
    out.append(line(x0 + 26, fy(sheet.strike), x0 + w, fy(sheet.strike), MUTED, 1, "3 4"))
    distinct = []
    for c in contents:
        if not distinct or distinct[-1].revision != c.revision:
            distinct.append(c)
    for k, c in enumerate(distinct):
        last = k == len(distinct) - 1
        segs, cur = [], []
        for i, (_, v) in enumerate(c.hours):
            if v is None:
                segs, cur = segs + [cur] if cur else segs, []
            else:
                cur.append(f"{fx(i):.1f},{fy(v):.1f}")
        segs += [cur] if cur else []
        for sg in segs:
            out.append(f'<polyline points="{" ".join(sg)}" fill="none" stroke="{INK if last else GREY}" '
                       f'stroke-width="{1.6 if last else 1}"/>')
    for i in range(0, 24, 6):
        out.append(text(fx(i), top + h + 14, f"{i:02d}:00", 10, MUTED, "middle"))
    out.append(text(x0 + w, top + h + 14, "local hour", 10, MUTED, "end"))
    sy = top + h + 40
    t0, t1 = steps[0]["fetched"], max(steps[-1]["fetched"], steps[0]["fetched"] + timedelta(hours=2))
    gx = lambda t: x0 + 26 + (t - t0) / (t1 - t0) * (w - 26)  # noqa: E731
    out.append(line(x0 + 26, sy, x0 + w, sy, RULE))
    for s in steps:
        x = gx(s["fetched"])
        if s["kind"] == "revision":
            out.append(f'<circle cx="{x:.1f}" cy="{sy:.1f}" r="5" fill="#fff" stroke="{SEA}" stroke-width="2"/>')
        else:
            out.append(f'<circle cx="{x:.1f}" cy="{sy:.1f}" r="3.2" fill="{GREY if s["kind"] == "refresh" else INK}"/>')
    out.append(text(x0 + 26, sy + 17, f"{t0:%H:%M}Z", 10, MUTED))
    out.append(text(x0 + w, sy + 17, f"{steps[-1]['fetched']:%H:%M}Z, 28 Oct", 10, MUTED, "end"))
    n_ref = sum(s["kind"] == "refresh" for s in steps)
    n_rev = sum(s["kind"] == "revision" for s in steps)
    out.append(text(x0 + 26, sy + 33, f"{len(steps)} responses collected. Refreshes rejected: {n_ref}."
                    f" Content revisions: {n_rev}.", 10.5, MUTED))
    return out


def ledger_cells(results):
    """{(time, column): [(text, colour)]} from the world A journal, the broker's notes and the replies."""
    a = results["A"]
    j = Journal(a["db"])
    cells, restart = {}, vt("00:10", 29)

    def put(t, col, s, color=INK):
        if s and (s, color) not in cells.setdefault((t, col), []):
            cells[(t, col)].append((s, color))
    events = [(datetime.fromisoformat(w), k, json.loads(d)) for w, k, d in
              j.db.execute("SELECT vt, kind, detail FROM event ORDER BY seq")]
    sent = {d["id"] for _, k, d in events if k == "sent"}
    touched = {(t, d["id"]) for t, k, d in events if k == "intent"}
    city = {MIA.key: "Miami", LAX.key: "Los Angeles"}
    for t, what, d in events:
        tone = SEA if "105" in json.dumps(d) and what != "recalculated" else INK
        if what == "intent" and d["kind"] == "new":
            put(t, 0, f"{d['id']} new order, {d['qty']} at {d['lmt']}c: " + (
                "sent" if d["id"] in sent else "journaled, not sent"), tone)
        elif what == "intent":
            put(t, 0, f"{d['id']}: order {d['orderId']} changed to {d['qty']} total at {d['lmt']}c", tone)
        elif what == "acknowledgement missing":
            put(t, 0, f"no ack for {d['id']}: no resend, contract frozen", tone)
        elif what == "freeze lifted":
            put(t, 0, f"{d['evidence']} late; contract released")
        elif what == "error":
            put(t, 0, {1100: "1100: nothing new leaves", 1102: "1102: released"}.get(d["code"]),
                OCHRE if d["code"] == 1100 else INK)
        elif what == "execution" and t != restart:
            put(t, 0, f"fill seen: {d['orderId']} x{d['shares']}")
        elif what == "recalculated" and (d["input"].startswith("synthetic") or d["order_terms"] == "unchanged"):
            src = d["input"].replace("degF", "°F").replace(", after cancel", "")
            quiet = d["order_terms"] == "unchanged" and not any(x == t for x, _ in touched)
            put(t, 0, f"{src}: recalculated" + (", quote unchanged, no order touched" if quiet else ""))
        elif what == "blocked":
            put(t, 0, "changed quote held back" if t < restart else "refresh held while reconciling")
        elif what == "decision":
            put(t, 0, f"{city[d['contract']]}: {d['decision']}", SEA if d["decision"] == "RESUME" else INK)
        elif what == "closed as never received":
            put(t, 0, f"{d['id']} closed as never received; its id is retired")
        elif what == "cancel requested":
            put(t, 0, f"cancel of {d['orderId']} requested")
        elif what == "cancel confirmed":
            put(t, 0, f"cancel confirmed: {d['filled']} filled in total, the late fill counted")
        elif what == "restart":
            put(t, 0, "new process, same SQLite file: " + ", ".join(d["unresolved"]) + " unresolved")
    put(vt("23:49:20"), 0, "process killed (SIGKILL) before i-106 reached the socket", OCHRE)
    for t, s in a["truth"]["notes"]:
        put(datetime.fromisoformat(t), 1, s, SEA if "105" in s else (OCHRE if s.startswith("1100") else INK))
    cbs = j.get("restart_callbacks")
    own = sorted({c["orderId"] for c in cbs if c["cb"] == "openOrder" and c["clientId"] == 11})
    other = sorted({c["orderId"] for c in cbs if c["cb"] == "openOrder" and c["clientId"] != 11})
    done = sorted({c["orderId"] for c in cbs if c["cb"] == "completedOrder"})
    dup = j.db.execute("SELECT COUNT(*) FROM event WHERE kind='execution' AND vt>=? AND detail LIKE "
                       "'%duplicate%'", (restart.isoformat(),)).fetchone()[0]
    for s in (f"open orders: {', '.join(map(str, own))} ours, matched by orderId, orderRef and permId; "
              f"{', '.join(map(str, other))} not ours, left alone",
              f"completed orders: {', '.join(map(str, done))}",
              f"{sum(c['cb'] == 'execDetails' for c in cbs)} executions: {dup} exact duplicates ignored,"
              " 1 correction replacing its original",
              "positions: Yes and No already netted; net per strike checked against the journal"):
        put(restart, 2, s)
    return cells


def ledger(results, x0, y0):
    cols, widths = [x0 + 66, x0 + 400, x0 + 690], [318, 276, 270]
    out = [text(x0, y0, "ET", 11, MUTED)]
    for c, name in zip(cols, ("What the controller knew", "What the simulated broker did", "Evidence at restart")):
        out.append(text(c, y0, name, 12, weight="bold"))
    cells = ledger_cells(results)
    y, kill_y, rest_y = y0 + 12, None, None
    for t in sorted({t for t, _ in cells}):
        y += 20
        out.append(line(x0, y - 14, x0 + 960, y - 14, "#eef0f3"))
        out.append(text(x0, y, f"{t.astimezone(ET):%H:%M:%S}", 10.5, MUTED))
        if t == vt("00:10", 29):
            rest_y = y - 14
        tallest = 1
        for col in range(3):
            k = 0
            for s, color in cells.get((t, col), []):
                for ln in textwrap.wrap(s, int(widths[col] / 5.8)):
                    out.append(text(cols[col], y + k * 14, ln, 11, color))
                    k += 1
            tallest = max(tallest, k)
        if t == vt("23:49:20"):
            kill_y = y + 14 * tallest - 6
        y += 14 * (tallest - 1)
    if kill_y and rest_y:
        out.insert(0, f'<rect x="{cols[0] - 6}" y="{kill_y:.1f}" width="{widths[0]}" height="{rest_y - kill_y:.1f}" '
                      f'fill="url(#hatch)"/>')
        out.append(text(cols[0] + widths[0] / 2, (kill_y + rest_y) / 2 + 4, "no controller process", 11, MUTED,
                        "middle"))
    return out, y + 10


def verdicts(results, x0, y0):
    b1 = Journal(results["B1"]["db"]).get("decision:" + LAX.key)
    missing = next((r.split("; ")[-1] for r in b1["reasons"] if "unattributed" in r), "")
    a_lines = ["Los Angeles resumes quoting. i-105 is found by orderRef",
               "and permId, its fills and the correction are booked once,",
               "i-106 is closed as never received and its id is retired.",
               "Miami passed 23:59 local and is not resupplied."]
    b_lines = ["Same observations in both worlds, different truths:",
               "our order filled, or another strategy bought the same",
               f"contracts. Los Angeles is suspended ({missing}), our",
               "working order is cancelled, and the operator is asked",
               "for the Trade Log or statement covering 23:49 to 00:00."]
    out = [line(x0, y0, x0, y0 + 96, SEA, 3), line(x0 + 490, y0, x0 + 490, y0 + 96, OCHRE, 3),
           text(x0 + 14, y0 + 14, "World A: history covers the outage", 13.5, SEA, weight="bold"),
           text(x0 + 504, y0 + 14, "Worlds B1 and B2: history starts at midnight", 13.5, OCHRE, weight="bold")]
    out += [text(x0 + 14, y0 + 34 + 15 * k, s, 11.5) for k, s in enumerate(a_lines)]
    out += [text(x0 + 504, y0 + 34 + 15 * k, s, 11.5) for k, s in enumerate(b_lines)]
    return out


def render(results, data):
    body = [text(40, 40, "Real NWS responses: a refresh changes timestamps, a revision changes the day's hours",
                 16, weight="bold"),
            text(40, 60, "Hourly temperature forecast over the contract day, 00:00 to 23:59 local. Black: latest"
                 " content. Grey: earlier content. Dots: each response; grey ones were rejected as refreshes.",
                 11, MUTED)]
    body += station(data, 40, 96, 450, MIA)
    body += station(data, 550, 96, 450, LAX)
    body += [line(40, 330, W - 40, 330, RULE),
             text(40, 362, "Simulated: the controller dies at 23:49:20 ET and a new process restarts at 00:10",
                  16, weight="bold"),
             text(40, 382, "The broker is a separate simulated process that keeps its state. Nothing here touched"
                  " a real account. World A shown; B differs after 23:50.", 11, MUTED)]
    led, y = ledger(results, 40, 418)
    body += led
    body += verdicts(results, 40, y + 26)
    h = y + 140
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {h}" width="{W}" height="{h}" '
            f'font-family="{FONT}"><rect width="{W}" height="{h}" fill="#ffffff"/>'
            '<defs><pattern id="hatch" width="7" height="7" patternUnits="userSpaceOnUse" '
            'patternTransform="rotate(45)"><line x1="0" y1="0" x2="0" y2="7" stroke="#eceff3" stroke-width="3"/>'
            '</pattern></defs>' + "".join(body) + "</svg>\n")
