"""The pricing boundary. The controller never prices; it admits or refuses what a provider returns.

`IllustrativeQuoteProvider` stands in for an existing pricing implementation. Its numbers
are illustrative and carry no view on fair value. What matters is the contract between the
two sides: identity, input revision, calculation id, integer cents, validity.
"""
import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from .contracts import LIMIT, Sheet


@dataclass(frozen=True)
class QuoteOutput:
    contract: str
    market_date: str
    input_revision: str
    calc_id: str
    yes_cents: int
    no_cents: int
    yes_qty: int
    no_qty: int
    computed_at: datetime
    valid_until: datetime

    def side(self, right):
        return (self.yes_cents, self.yes_qty) if right == "C" else (self.no_cents, self.no_qty)


class QuoteProvider(Protocol):
    def quote(self, sheet: Sheet, high_f: int, revision: str, inventory: int, now: datetime) -> QuoteOutput: ...


class IllustrativeQuoteProvider:
    version = "illustrative-1"

    def quote(self, sheet, high_f, revision, inventory, now):
        p = 1 / (1 + math.exp(-(high_f - sheet.strike - 0.5) / 1.6))
        fair = min(97, max(3, round(100 * p)))
        yes, no = max(2, fair - 2), max(2, 100 - fair - 2)
        yes_qty = 0 if fair <= 5 else max(0, min(10, LIMIT - inventory))
        no_qty = 0 if fair >= 95 else max(0, min(10, LIMIT + inventory))
        calc = hashlib.sha256(f"{self.version}|{sheet.key}|{revision}|{inventory}|{now.isoformat()}".encode()
                              ).hexdigest()[:10]
        return QuoteOutput(sheet.key, sheet.market_date.isoformat(), revision, calc, yes, no, yes_qty, no_qty,
                           now, now + timedelta(minutes=5))


def admit(q, sheet, current_revision, now):
    """Reasons to refuse a provider output; an empty list means it may drive orders."""
    bad = []
    if q.contract != sheet.key:
        bad.append(f"output is for {q.contract}, not {sheet.key}")
    if q.market_date != sheet.market_date.isoformat():
        bad.append(f"market date {q.market_date} differs from {sheet.market_date}")
    if q.input_revision != current_revision:
        bad.append(f"computed on input {q.input_revision}, current input is {current_revision}")
    if not q.computed_at <= now <= q.valid_until:
        bad.append(f"outside validity {q.computed_at:%H:%M:%S}..{q.valid_until:%H:%M:%S}Z")
    if now >= sheet.last_trade_utc():
        bad.append("contract past its last trading time")
    for name in ("yes_cents", "no_cents", "yes_qty", "no_qty"):
        if type(getattr(q, name)) is not int:
            bad.append(f"{name} is not an integer")
        elif getattr(q, name) < 0:
            bad.append(f"{name} is negative")  # a zero quantity means: do not quote that side
    if not bad and not (2 <= q.yes_cents <= 99 and 2 <= q.no_cents <= 99):
        bad.append("price outside the $0.02..$0.99 bid range")
    if not bad and q.yes_cents + q.no_cents >= 101:
        bad.append("own Yes and No bids would pair with each other at $1.01")
    return bad
