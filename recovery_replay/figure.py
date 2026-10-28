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
    fx = lambda i: x0 + 26 + i / (len(contents[0].hours) - 1) * (w - 26)  # noqa: E731
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
    t0, t1 = steps[0]["fetched"], steps[-1]["fetched"]
    t1 = t1 if t1 > t0 else t0 + timedelta(hours=1)  # a single response still gets an axis
    gx = lambda t: x0 + 26 + (t - t0) / (t1 - t0) * (w - 26)  # noqa: E731
    out.append(line(x0 + 26, sy, x0 + w, sy, RULE))
    for s in steps:
        x = gx(s["fetched"])
        if s["kind"] == "revision":
            out.append(f'<circle cx="{x:.1f}" cy="{sy:.1f}" r="5" fill="#fff" stroke="{SEA}" stroke-width="2"/>')
        else:
            out.append(f'<circle cx="{x:.1f}" cy="{sy:.1f}" r="3.2" fill="{GREY if s["kind"] == "refresh" else INK}"/>')
    out.append(text(x0 + 26, sy + 17, f"{t0:%H:%M}Z", 10, MUTED))
    out.append(text(x0 + w, sy + 17, f"{t1:%H:%M}Z, 28 Oct", 10, MUTED, "end"))
    n_ref = sum(s["kind"] == "refresh" for s in steps)
    n_rev = sum(s["kind"] == "revision" for s in steps)
    out.append(text(x0 + 26, sy + 33, f"{len(steps)} responses: {n_ref} unchanged refreshes,"
                    f" {n_rev} contract-day temperature revisions.", 10.5, MUTED))
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
    refills = j.intents("contract=? AND right='P' AND kind='new'", LAX.key)
    lost = refills[1]["id"] if len(refills) > 1 else "-"  # the Los Angeles No refill, the order in doubt
    killed_at, killed = next((t, d["id"]) for t, k, d in events if k == "intent" and d["id"] not in sent)
    touched = {(t, d["id"]) for t, k, d in events if k == "intent"}
    city = {MIA.key: "Miami", LAX.key: "Los Angeles"}
    for t, what, d in events:
        tone = SEA if lost in json.dumps(d) and what != "recalculated" else INK
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
        elif what == "input unchanged, no recalculation":
            put(t, 0, "later real response, same content: nothing recalculated, no order touched")
        elif what == "blocked":
            put(t, 0, "changed quote held back" if t < restart else f"change held: {d['reason']}")
        elif what == "decision":
            put(t, 0, f"{city[d['contract']]}: {d['decision']}", SEA if d["decision"] == "RESUME" else INK)
        elif what == "closed no fill":
            put(t, 0, f"{d['id']} closed: not working, no fill; whether it was received stays open; id retired")
        elif what == "expired":
            put(t, 0, f"{d['id']} past its contract's last trading time: expired")
        elif what == "cancel requested":
            put(t, 0, f"cancel of {d['orderId']} requested")
        elif what == "cancel unconfirmed":
            put(t, 0, f"no answer to the cancel of {d['orderId']}: still counted as working, contract frozen", OCHRE)
        elif what == "cancel confirmed":
            put(t, 0, f"cancel of {d['orderId']} confirmed: {d['filled']} filled in total, the late fill counted")
        elif what == "restart":
            put(t, 0, "new process, same SQLite file: " + ", ".join(d["unresolved"]) + " unresolved")
    put(killed_at, 0, f"process killed (SIGKILL) before {killed} reached the socket", OCHRE)
    for t, s in a["truth"]["notes"]:
        put(datetime.fromisoformat(t), 1, s, SEA if lost[2:] in s else (OCHRE if s.startswith("1100") else INK))
    cbs = j.get("restart_callbacks")
    own = sorted({c["orderId"] for c in cbs if c["cb"] == "openOrder" and c["clientId"] == j.get("identity")["clientId"]})
    other = sorted({c["orderId"] for c in cbs if c["cb"] == "openOrder"} - set(own))
    done = sorted({c["permId"] for c in cbs if c["cb"] == "completedOrder"})
    results = [d["result"] for t, k, d in events if k == "execution" and t >= restart]
    for s in (f"open orders: {', '.join(map(str, own))} ours, matched by orderId, orderRef and permId; "
              f"{', '.join(map(str, other))} not ours, left alone",
              f"completed orders of the day: {len(done)}" + (f" (permIds {', '.join(map(str, done))})" if done else ""),
              f"{sum(c['cb'] == 'execDetails' for c in cbs)} executions: {results.count('duplicate')} exact "
              f"duplicates ignored, {results.count('correction')} correction replacing its original",
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
        if any("process killed" in s for s, _ in cells.get((t, 0), [])):
            kill_y = y + 14 * tallest - 6
        y += 14 * (tallest - 1)
    if kill_y and rest_y:
        out.insert(0, f'<rect x="{cols[0] - 6}" y="{kill_y:.1f}" width="{widths[0]}" height="{rest_y - kill_y:.1f}" '
                      f'fill="url(#hatch)"/>')
        out.append(text(cols[0] + widths[0] / 2, (kill_y + rest_y) / 2 + 4, "no controller process", 11, MUTED,
                        "middle"))
    return out, y + 10


def decisions(results):
    """One row per world, read from its journal: what the reply showed, and both decisions."""
    rows = []
    for w in ("A", "B1", "B2"):
        j = Journal(results[w]["db"])
        d = {s.key: j.get("decision:" + s.key) for s in (MIA, LAX)}
        start = datetime.fromisoformat(d[LAX.key]["exec_history_from"])
        restart = vt("00:10", 29)
        seen = f"executions back to {start.astimezone(ET):%H:%M} ET" if start < restart else \
            "none of the fills the journal saw"
        la = d[LAX.key]
        why = ("all orders accounted for; the lost one matched on orderId, orderRef, permId"
               if la["decision"] == "RESUME" else
               "; ".join(r.split(" (orderId")[0] + " not found" if "not found" in r else
                         "position " + r.split("; ")[-1] for r in la["reasons"][:2]))
        rows.append((w, seen, d[MIA.key]["decision"], la["decision"], why))
        j.db.close()
    return rows


def frieze(results, x0, y0, w):
    """Trading windows around the restart: the decision that differs between the two cities."""
    t0, t1 = vt("23:30"), vt("04:20", 29)
    fx = lambda t: x0 + 120 + (t - t0) / (t1 - t0) * (w - 120)  # noqa: E731
    out = []
    for k, s in enumerate((MIA, LAX)):
        y = y0 + 20 + k * 30
        end = s.last_trade_utc()
        out.append(text(x0, y + 4, f"{s.city}", 12, weight="bold"))
        out.append(f'<rect x="{fx(t0):.1f}" y="{y - 7}" width="{fx(end) - fx(t0):.1f}" height="14" fill="#dfe8ee"/>')
        lx = fx(end) + 6
        if lx < fx(vt("00:10", 29)) + 6 < lx + 190:  # keep the label clear of the restart line
            lx = fx(vt("00:10", 29)) + 8
        out.append(text(lx, y + 4, f"trading ends {end.astimezone(ET):%H:%M} ET, 23:59 local", 10.5, MUTED))
    j = Journal(results["A"]["db"])
    sent = {json.loads(d)["id"] for (d,) in j.db.execute("SELECT detail FROM event WHERE kind='sent'")}
    killed = min(datetime.fromisoformat(i["vt"]) for i in j.intents() if i["id"] not in sent)
    j.db.close()
    marks = [(killed, f"killed {killed.astimezone(ET):%H:%M:%S}", OCHRE), (vt("00:10", 29), "restart 00:10", INK)]
    for t, label, color in marks:
        out.append(line(fx(t), y0 + 4, fx(t), y0 + 66, color, 1.2, "3 3"))
        out.append(text(fx(t) + (-4 if color == OCHRE else 4), y0 - 2, label, 10.5, color,
                        "end" if color == OCHRE else "start"))
    for k in range(0, 4):
        t = vt("00:00", 29) + timedelta(hours=k)
        out.append(text(fx(t), y0 + 84, f"{t.astimezone(ET):%H:%M}", 10, MUTED, "middle"))
    out.append(text(fx(vt("23:30")), y0 + 84, "23:30 ET", 10, MUTED, "middle"))
    return out


def table(results, x0, y0):
    cols = [x0, x0 + 64, x0 + 300, x0 + 400, x0 + 520]
    out = [text(c, y0, h, 11.5, MUTED) for c, h in zip(cols, ("World", "What the executions reply showed",
                                                           "Miami", "Los Angeles", "Why"))]
    for k, (w, seen, mia, la, why) in enumerate(decisions(results)):
        y = y0 + 26 + k * 24
        out.append(line(x0, y - 16, x0 + 960, y - 16, RULE))
        color = SEA if la == "RESUME" else OCHRE
        out += [text(cols[0], y, w, 12.5, weight="bold"), text(cols[1], y, seen, 11.5),
                text(cols[2], y, mia.lower(), 12), text(cols[3], y, la.lower(), 12.5, color, weight="bold"),
                text(cols[4], y, why[:78], 11.5)]
    return out


def render(results, data):
    steps = [s for sh in (MIA, LAX) for s in weather.walk(weather.load(data, sh.station), sh)]
    refresh, revs = sum(s["kind"] == "refresh" for s in steps), sum(s["kind"] == "revision" for s in steps)
    first, last = min(s["fetched"] for s in steps), max(s["fetched"] for s in steps)
    body = [text(40, 40, "Can quoting resume after a crash at midnight Eastern? It depends on what the broker can"
                 " still show.", 17, weight="bold"),
            text(40, 61, "A simulated broker, three simulated histories, real weather inputs. Decisions read from each"
                 " world's journal at 00:10 ET; A's holds if the executions reply is one contiguous window.", 11.5,
                 MUTED)]
    body += table(results, 40, 98)
    body += [text(40, 214, "Trading windows around the restart", 13, weight="bold")]
    body += frieze(results, 40, 236, 960)
    body += [line(40, 344, W - 40, 344, RULE),
             text(40, 374, f"{len(steps)} NWS responses, {first:%H:%M} to {last:%H:%M} UTC: {refresh} unchanged "
                  f"refreshes, {revs} contract-day temperature revisions", 14, weight="bold"),
             text(40, 393, "Hourly forecast for the grid cell next to each settlement station, 00:00 to 23:59 local."
                  " Settlement uses the station's observations, not this forecast.", 11, MUTED)]
    body += station(data, 40, 428, 450, MIA)
    body += station(data, 550, 428, 450, LAX)
    h = 680
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {h}" width="{W}" height="{h}" '
            f'font-family="{FONT}"><rect width="{W}" height="{h}" fill="#ffffff"/>' + "".join(body) + "</svg>\n")


def timeline(results):
    body = [text(40, 40, "World A, every event: what the controller knew, what the simulated broker did, what the"
                 " restart received", 15, weight="bold"),
            text(40, 60, "Generated from the world A journal, the simulated broker's log and the recorded replies."
                 " Nothing here touched a real account.", 11, MUTED)]
    led, y = ledger(results, 40, 96)
    body += led
    h = y + 30
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {h}" width="{W}" height="{h}" '
            f'font-family="{FONT}"><rect width="{W}" height="{h}" fill="#ffffff"/>'
            '<defs><pattern id="hatch" width="7" height="7" patternUnits="userSpaceOnUse" '
            'patternTransform="rotate(45)"><line x1="0" y1="0" x2="0" y2="7" stroke="#eceff3" stroke-width="3"/>'
            '</pattern></defs>' + "".join(body) + "</svg>\n")
