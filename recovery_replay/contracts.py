"""Contract sheets: a dated contract is chosen first, the city follows from it.

Every field below is copied from a source listed in README.md (section "Sourced facts"), mainly
IBKR's public ForecastTrader contract list saved in data/sources/. The contracts are identified by
the conIds IBKR publishes; the TWS `symbol` has not been verified and is not used.
"""
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc
ET = ZoneInfo("America/New_York")
TERMS = {
    "url": "https://data.forecastex.com/regulatory/DailyTemperatureTermsandConditions.pdf",
    "last_modified": "2026-08-06T21:44:24Z",
    "sha256": "226826cb5d64cddc52c86b99a2e908f25ba48aa431f6c9ffe2c30e55731db511",
    "consulted": "2026-10-28",
}
LIMIT = 14  # illustrative risk limit: |net exposure| per strike, in contracts


@dataclass(frozen=True)
class Sheet:
    key: str
    city: str
    station: str
    station_name: str
    lat: float
    lon: float
    tz: str
    market_date: date
    strike: int
    conid_yes: int
    conid_no: int
    question: str
    grid: str
    venue: str = "ForecastEx"
    product: str = "Daily Temperature Forecast Contract (IBKR product: Daily Temperature High)"
    op: str = ">"  # terms: "exceed"; listing title: "Greater than"
    unit: str = "degF"
    settlement: str = "Weather Underground, Daily Observations table, History tab, Daily view"

    def day_utc(self):
        start = datetime.combine(self.market_date, time(0), ZoneInfo(self.tz))
        return start.astimezone(UTC), (start + timedelta(days=1)).astimezone(UTC)

    def last_trade_utc(self):
        """Terms: "Last Trading Time: 11:59 PM local time on the date listed in the Contract"."""
        return datetime.combine(self.market_date, time(23, 59), ZoneInfo(self.tz)).astimezone(UTC)

    def tws(self, right):
        """TWS API contract by conId; `localKey` exists only in the simulator."""
        return {"conId": self.conid_yes if right == "C" else self.conid_no, "secType": "OPT",
                "exchange": "FORECASTX", "currency": "USD", "right": right, "strike": self.strike,
                "localKey": self.key}


MIA = Sheet("MIA-20261028-84", "Miami", "KMIA", "Miami International Airport", 25.79056, -80.31639,
            "America/New_York", date(2026, 10, 28), 84, 939505631, 939505635,
            "Will the highest temperature in Miami (MIA)(KMIA) exceed 84 F on October 28, 2026?",
            "https://api.weather.gov/gridpoints/MFL/106,51")
LAX = Sheet("LAX-20261028-71", "Los Angeles", "KLAX", "Los Angeles International Airport", 33.93806, -118.38889,
            "America/Los_Angeles", date(2026, 10, 28), 71, 939505324, 939505330,
            "Will the highest temperature in Los Angeles (LA)(LAX)(KLAX) exceed 71 F on October 28, 2026?",
            "https://api.weather.gov/gridpoints/LOX/148,41")
SHEETS = {s.key: s for s in (MIA, LAX)}


def vt(hms, day=28):
    """Scenario clock: an Eastern wall time on 28 or 29 October 2026, returned as UTC."""
    h, m, s = (int(x) for x in (hms + ":00")[:8].split(":"))
    return datetime(2026, 10, day, h, m, s, tzinfo=ET).astimezone(UTC)


def net(right, shares):
    """Signed exposure per strike: Yes (C) counts +, No (P) counts -. Netting by IB keeps it."""
    return shares if right == "C" else -shares
