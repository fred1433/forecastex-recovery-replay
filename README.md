# ForecastEx recovery replay

A controller can restart after a crash without enough evidence to quote again. This replay shows one restart where the evidence is enough, under stated assumptions, and two histories the controller cannot tell apart, where it must stop.

| World, simulated | What the executions reply showed at 00:10 ET | Miami | Los Angeles |
|---|---|---|---|
| A | every fill the journal already held, back to 23:45 ET | closed | resumes |
| B1, B2 | none of the fills the journal saw | closed | suspended: one order unaccounted for, 4 contracts unattributed |

Two ForecastEx Daily Temperature Forecast Contracts for 28 October 2026 are quoted by one controller process: Miami, which settles on KMIA in Eastern time, and Los Angeles, which settles on KLAX in Pacific time. At 23:49:20 Eastern the controller is killed with SIGKILL, with one Los Angeles order sent but never acknowledged and one Miami order committed to its SQLite journal but not sent. A new process starts at 00:10 on the same file. Miami stopped trading at 23:59 Eastern; Los Angeles trades three more hours.

The new process resends nothing. It asks for open orders, completed orders, executions and positions, waits for each reply's end marker, checks every piece of evidence against the journal by orderId, orderRef and permId, and only then decides, contract by contract. The broker is a separate simulated process that kept its state.

Nobody tells the controller which world it is in. In A, the executions reply contains every fill the journal holds; if that reply is one contiguous window, the lost order is found and the unsent one can be closed with no fill. In B1 our order filled before midnight; in B2 it was rejected and another strategy bought the same four contracts. The evidence is identical in both, so Los Angeles is suspended and the decision names the record that would settle it.

Weather inputs are real National Weather Service responses; pricing is an illustrative stand-in; the broker is simulated, and no real account was touched.

![Decisions per simulated world, the trading windows around the restart, and the real NWS responses for both contract days.](docs/figure.svg)

The figure is drawn by the replay from its own outputs: the table from each world's journal, the trading windows from the contract sheets, the weather panel from every response in `data/nws/`. Every event of world A is in [docs/timeline.svg](docs/timeline.svg). The incident reads only `data/incident/`: four of those responses, frozen and named in `manifest.json` with their SHA-256, which the controller checks before it starts. Adding responses to `data/nws/` changes the weather panel and nothing else.

## Run it

Python 3.10 or later, standard library only. No account, no key, no network access.

```
python3 -m recovery_replay.replay
python3 -m unittest
```

The first command runs worlds A, B1 and B2: for each, a simulated broker process and a controller process, killed at the chosen point, the outage, a new controller on the same journal. It checks ten properties, writes the audit trail and redraws both figures; it exits non-zero if a property fails. Its full output on this data is in [docs/replay_output.txt](docs/replay_output.txt):

```
world A  restart decisions: {'MIA-20261028-84': 'CLOSED', 'LAX-20261028-71': 'RESUME'}  observation digest 7cf2a1c054fd535c
world B1 restart decisions: {'MIA-20261028-84': 'CLOSED', 'LAX-20261028-71': 'SUSPEND'}  observation digest 506d30c180e5af8f
world B2 restart decisions: {'MIA-20261028-84': 'CLOSED', 'LAX-20261028-71': 'SUSPEND'}  observation digest 506d30c180e5af8f
  pass  controller process killed (SIGKILL) at the chosen point (i-106 journaled, not sent)
  pass  A: Los Angeles resumes, Miami is past its last trading time
  pass  A: i-105 found by orderId with matching orderRef and permId; no second order for it
  pass  A: i-106 closed with no fill, its outcome otherwise undetermined; its id never reused
  pass  A: new ids after the restart exceed every id seen, including the other strategy's 501
  pass  A: own exposure and cost equal the broker's hidden truth, correction included
  pass  B1 and B2: identical observations, identical decision, different truth
  pass  B: the suspension names the missing evidence
  pass  our working Yes and No bids never summed to $1.01 or more on any contract, at any order
  pass  no Miami order after its last trading time; no reqGlobalCancel; no foreign order touched
```

The second command runs eleven worlds through the same path (A, B1, B2, the four counter-examples C1 to C4, and A four more times, each without one end marker), then 600 deterministic interleavings of the callbacks recorded at restart in A, B1 and B2. The interleavings exercise the reconciler on recorded evidence, not the controller's actions. Output: [docs/test_output.txt](docs/test_output.txt), `Ran 29 tests in 14.920s`, `OK`.

## Criteria

`tests/test_criteria.py`; every test except the boundary checks at the end reads the journals and broker logs of real processes.

| Situation | Required result | Test |
|---|---|---|
| Intent committed, process killed before sending | Found; no execution invented; closed with no fill, its receipt left undetermined | `test_01` |
| Order accepted, acknowledgement lost, client killed | The same order found by orderId, orderRef and permId; no replacement | `test_02` |
| Partial fill while down | Fills and cost equal the broker's hidden truth | `test_03` |
| Fill during a cancel that is answered late | No "confirmed" before a terminal status; the late fill counted; the replacement within the limit | `test_04` |
| Duplicate, then a correction that changes the price | The duplicate changes nothing; the correction replaces | `test_05` |
| Another strategy's activity | None of its orders cancelled, modified or bound | `test_06` |
| History too short | Suspended, with the missing record named | `test_07` |
| Same weather content downloaded again | Nothing recalculated, no order touched | `test_08` |
| New input, same quote | Recalculation traced, no order touched | `test_09` |
| Wrong date, stale or negative output | Refused | `test_10` |
| C1: unacknowledged order filled completely while down | FILLED from the executions alone, never "closed" | `test_uncertain_order_filled_while_down_is_not_closed` |
| C2: working order filled, another strategy bought the opposite, net unchanged | Suspended: the order appears in no reply | `test_order_working_before_crash_absent_from_every_reply_suspends` |
| C3: price change whose acknowledgement never came | Recognised from the order's observed terms | `test_uncertain_modification_recognised_from_the_order_seen` |
| C4: execution with another permId and orderRef | Identity conflict, suspended | `test_execution_with_another_identity_is_a_conflict` |
| Any one end marker missing | Suspended with the marker named, no crash | `test_missing_end_marker_suspends_without_crashing` |

Further tests: new ids above every id seen; history depth inferred, not declared; an orderRef mismatch; restart under another client id, or a live start on a used journal, refused; clientId 0 refused; the lower leg sent first and the raise held without its acknowledgement; incomplete, stale or misplaced weather refused before pricing; a tampered frozen input refused; local days of 23 and 25 hours.

## Audit trail

`out/audit_A.txt`, world A. Each order links back to the provider output that produced it.

```
ET        contract  intent   kind   side qty lmt  input rev.   calc_id    orderId permId  state           filled
23:40:00  MIA       i-101    new    Yes   10  56  8e919427a5fd 95a27c552a     101 7000003 EXPIRED              4
23:40:00  MIA       i-102    new    No    10  40  8e919427a5fd 95a27c552a     102 7000004 FILLED              10
23:40:00  LAX       i-103    new    Yes   10  56  26c82b97c49b 605d027239     103 7000005 CANCELLED            6
23:40:00  LAX       i-104    new    No    10  40  26c82b97c49b 605d027239     104 7000006 FILLED              10
23:46:00  MIA       m-101-1  modify Yes   14  56  8e919427a5fd a6e3994499     101 7000003 ACKED
23:48:00  LAX       m-103-1  modify Yes   10  81  94548bc50fed 15c67940fd     103 7000005 ACKED
23:48:00  LAX       m-104-1  modify No    10  15  94548bc50fed 15c67940fd     104 7000006 ACKED
23:49:00  LAX       i-105    new    No     4  15  94548bc50fed ab86cacf9f     105 7000007 FILLED               4
23:49:20  MIA       i-106    new    No     8  40  8e919427a5fd 45806e7993     106 -       CLOSED_NO_FILL       0
00:10:00  LAX       i-502    new    No     4  15  94548bc50fed 970a9cce29     502 7000008 ACKED                0
00:10:00  LAX       m-103-2  modify Yes   14  81  94548bc50fed 970a9cce29     103 7000005 ACKED
00:12:30  LAX       m-502-1  modify No     6  95  d7ae88e02e6d 07ed81b8c7     502 7000008 ACKED
own net exposure MIA-20261028-84: -6 (Yes minus No), cost 624 cents
own net exposure LAX-20261028-71: -8 (Yes minus No), cost 693 cents
```

`i-105` is the order whose acknowledgement was lost, found at restart; `i-106` was journaled and never sent, closed with no fill. `i-101` expired with Miami's last trading time. `i-502` takes its id above the other strategy's 501. At 00:12 the cancel of `i-103` gets no answer: the order stays counted as working and Los Angeles stays frozen until 00:12:30, when the late answer brings 2 more Yes fills and the terminal status; `m-502-1` then resizes No to 6, so the worst case is -8 - 6 = -14, the limit.

## Running it as an operator

- Restart: start the controller on the same journal with the same client id; it refuses another one, and refuses `--phase live` on a journal that already holds an incident. It freezes every contract, asks for the four replies, and holds any quote change until it has decided.
- It is not told how deep the broker's history goes. Executions the journal holds but the reply omits place the start of that history later; executions the reply contains place it earlier. That inference assumes one contiguous window.
- State: `sqlite3 out/journal_A.sqlite "select id, kind, state, order_id, perm_id, filled from intent"`; the decision per contract is in `meta` under `decision:<contract>`.
- Incident export: `select vt, kind, detail from event order by seq` gives the timeline; `select v from meta where k='restart_callbacks'` gives every callback received before the decision.
- Resume a contract only when: all four replies have ended; no orderId, orderRef or permId contradicts the journal; every order that may have been working is seen open, terminal, or fully filled; every unsent or unacknowledged intent is either found, or falls inside the window the executions reply appears to cover; the net position change since the checkpoint is explained by executions. Otherwise the contract is suspended, its own working orders are cancelled, and the decision lists what is missing.

## Sourced facts

Consulted on 28 October 2026. Where a fact drives code, the code cites it.

ForecastEx, [Daily Temperature Contract Terms and Conditions](https://data.forecastex.com/regulatory/DailyTemperatureTermsandConditions.pdf), file last modified 6 August 2026, SHA-256 `226826cb5d64cddc52c86b99a2e908f25ba48aa431f6c9ffe2c30e55731db511`; Appendix A of the [CFTC 40.2 filing of 6 August 2026](https://www.cftc.gov/filings/ptc/ptc08062614351.pdf) carries the same wording:

- Settlement: "This Contract resolves based solely on the final daily temperature values displayed in the Daily Observations data table" on Weather Underground for the station and date; "No other data, tables, graphs, or displays ... shall be used". Source Agency: Weather Underground.
- "Last Trading Time: 11:59 PM local time on the date listed in the Contract." This is why Miami is closed at 00:10 Eastern while Los Angeles is not.
- Settlement value $1.00 or $0.00; minimum tick $0.01; resolution when the first observation of a later calendar day is published.
- The question template reads "exceed [##]". The sheets use a strict `>`.

IBKR's public ForecastTrader contract list, fetched on 28 October 2026 from `https://www.interactivebrokers.com/tws.proxy/public/forecasttrader/prediction/contract/list?conid=853400801&exchange=FORECASTX` for Miami and `conid=853400777` for Los Angeles (the endpoint its [Prediction Markets](https://www.interactivebrokers.com/predictionmarkets/en/home.php) pages call; response saved in `data/sources/ibkr_forecasttrader_daily_temperature_20261028.json`):

- "Will the highest temperature in Miami (MIA)(KMIA) exceed 84 F on October 28, 2026?", conIds 939505631 (Yes) and 939505635 (No), last trade 03:59 UTC on 29 October, that is 23:59 Eastern.
- "Will the highest temperature in Los Angeles (LA)(LAX)(KLAX) exceed 71 F on October 28, 2026?", conIds 939505324 and 939505330, last trade 06:59 UTC, 23:59 Pacific.
- The same list names KLGA for New York and KBKF for Denver on that date, where the [launch article of 25 November 2025](https://www.interactivebrokers.com/campus/traders-insight/ibkr-climate-energy/daily-high-temperature-markets-at-forecastex/) listed Central Park (KNYC) and Denver International (KDEN). A city is not a contract; the sheet records the station of the dated contract.
- As a complement only, Robinhood's pages for the same contracts name the same stations (excerpts in `data/sources/listings_20261028.txt`).

ForecastEx [Forecast Contract Risk Disclosure](https://www.cftc.gov/sites/default/files/filings/orgrules/25/04/rules04142519330.pdf) (rule filing published on cftc.gov in April 2025; a later version may exist): bids "at prices between $0.02 and $0.99"; "When the combined value of the best Bids for the 'Yes' Position and 'No' Position equal $1.01, the Exchange executes". `admit()` therefore refuses an output whose own Yes and No bids would sum to $1.01 or more.

Stations and grid points: [api.weather.gov/stations/KMIA](https://api.weather.gov/stations/KMIA) and [KLAX](https://api.weather.gov/stations/KLAX) give the station coordinates (saved in `data/sources/`).

Interactive Brokers TWS API documentation:

- [Event contract definition](https://www.interactivebrokers.com/docs/tws-api/doc/ec/contracts): ForecastEx contracts are `OPT` on exchange `FORECASTX`; "Call = Yes and Put = No"; "ForecastEx instruments do not permit Sell orders. Instead, ForecastEx positions are flattened or reduced by buying the opposing contract."
- [Event contract orders](https://www.interactivebrokers.com/docs/tws-api/doc/ec/orders): limit orders only; time in force Day, Good till Canceled or Immediate-Or-Cancel; whole quantities; "IB will net the opposing positions together automatically". Hence exposure is tracked as Yes minus No per strike, a quantity netting does not change.
- [placeOrder](https://www.interactivebrokers.com/docs/tws-api/doc/orders/place-order): "Places or modifies an order"; a new order with an id less than or equal to a previous one is an error. [Modifying orders](https://www.interactivebrokers.com/docs/tws-api/doc/orders/modify-order): same TWS user and same client id; `reqAllOpenOrders` does not bind; binding cancels and resubmits a working order.
- [Next valid id](https://www.interactivebrokers.com/docs/tws-api/doc/next-valid-id): "The order ID sequence persists between sessions."
- [Open orders](https://www.interactivebrokers.com/docs/tws-api/doc/order-management/open-orders): `reqAllOpenOrders` returns open orders once and "does not initiate a subscription", ending with `openOrderEnd`; under client id 0, `reqOpenOrders` binds manual TWS orders.
- [Completed orders](https://www.interactivebrokers.com/docs/tws-api/doc/order-management/completed-orders): orders "for the given day that are no longer modifiable"; every simulated world therefore returns only the day's completed orders. In the official Python client (`ibapi` 9.81.1 on PyPI, `decoder.py`, `processCompletedOrderMsg`), a completed order is decoded without its orderId or clientId and handed to `completedOrder(contract, order, orderState)`; it is matched here by permId, then orderRef, never by orderId. The end marker `completedOrdersEnd` is an `EWrapper` callback of that client; the documentation page does not show it.
- [Execution details](https://www.interactivebrokers.com/docs/tws-api/doc/order-management/execution-details): by default only executions since midnight; the TWS Trade Log setting can extend this to seven days; "The IB Gateway is limited to only executions from the current trading day since midnight"; `execDetailsEnd`; a correction has an `execId` that "differs from a previous ExecId in only the digits after the final period". The page says a correction repeats every other parameter; the simulated correction changes the price by one cent so that replacement, not just deduplication, is tested.
- [Master Client ID](https://www.interactivebrokers.com/docs/tws-api/doc/order-management/master-client-id): only a client set as Master API client ID receives other API connections' orders and trades. The controller (client 11) is assumed to be that client; it is how it sees the other strategy's executions.
- [Order submission, older official guide](https://interactivebrokers.github.io/tws-api/order_submission.html): "if the function reqAllOpenOrders is used by a client, subsequent orders placed by that client must have order IDs greater than the order IDs of all orders returned because of that function call." New ids are therefore above nextValidId, the journal and every id seen; after seeing the other strategy's 501, the next new order is 502.
- [Positions](https://www.interactivebrokers.com/docs/tws-api/doc/accounts/positions): per account and contract; `positionEnd` after the initial batch, then updates.
- [Cancelling](https://www.interactivebrokers.com/docs/tws-api/doc/orders/cancel-order): `cancelOrder` only for the same client id; `reqGlobalCancel` "will cancel ALL open orders including those placed directly from TWS". It is never called here.
- [Order status](https://www.interactivebrokers.com/docs/tws-api/doc/order-management/order-status): "Often there are duplicate orderStatus messages"; under PendingCancel "You may still receive an execution while your cancellation request is pending."
- [System message codes](https://www.interactivebrokers.com/docs/tws-api/doc/error/codescodes): 1100 connectivity lost (internet, "a nightly reset of the IB servers, or a competing session"); 1101 restored, market data lost; 1102 restored, market data maintained; during a reset "execution reports and simulated orders will be delayed".

The contracts are addressed by the conIds IBKR publishes. The TWS `symbol` for each city is not written anywhere here: it would come from `reqContractDetails` against a real session, and I have not verified it.

## Limits

- Real: the NWS gridpoint responses (raw JSON with URL, UTC collection time and HTTP headers, every 20 minutes from 12:39 UTC on 28 October), the station metadata, IBKR's contract list, the contract facts above.
- Simulated: the broker, the clock, fills, rejections, the netting delay, the connectivity losses at 23:44 and 23:50, the other strategy. The reset times are assumptions: IBKR documents a daily reset without stating its time on the pages above.
- Taken from documentation, not observed: every TWS behaviour listed above. Callbacks are flattened JSON imitations, prices travel as integer cents; a real adapter converts to `lmtPrice` in dollars at the edge.
- To validate against a real configuration: TWS or Gateway version and Trade Log setting; whether the executions reply is one contiguous window; the time zone of "since midnight"; whether orderRef and the execution history survive a restart of TWS or Gateway itself; when DAY orders expire on FORECASTX for contracts that trade past midnight Eastern, and whether GTC is needed; whether a rejected order advances `nextValidId`; how long netting takes; the Master API client ID setting; the TWS symbols.
- Weather: the forecast cells used are `MFL/106,51` and `LOX/148,41`; the cells containing the KMIA and KLAX coordinates are `MFL/105,51` and `LOX/149,41`, one cell away (`data/sources/points_*_station.json`). Settlement uses the station's Weather Underground observations, not a forecast. Hours of the contract day already past still carry forecast values, not observations. In the incident, inputs collected on 28 October are replayed at a simulated clock near midnight: their freshness is checked at collection time, which an operational controller would replace with its own clock.
- Pricing: the provider and the limit of 14 contracts are illustrative. Ownership convention: other strategies may trade the same contracts in the same account, so positions check attribution and never establish it.

## Author

Frederic de Lavenne de Choulot, independent engineer. I build and run production automation systems for clients. I have not operated an IBKR or ForecastEx system in production, and this repository has never touched a real account: every order in it goes to the simulated broker.

A 15-minute call: https://cal.theaipipe.com
