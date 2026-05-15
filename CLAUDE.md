# Masttro API — Reference & Exploration Guide

This document is the source of truth for working with the Masttro API in this project. Read it before making any API calls or writing any integration code.

---

## Project context

We are exploring the Masttro API to evaluate replacing slow in-platform reporting with internal apps that read Masttro data directly. The primary downstream consumer will be the **Stonebridge Cashflow Planner**, with potential extensions to alternatives tracking and client-facing dashboards.

**Current phase: exploration.** Goal is to understand actual response shapes, data quality, freshness, and quirks before committing to a Supabase schema or production sync architecture. Do not build production pipelines yet.

---

## Operating rules

### Credentials
- API key and secret live in `.env.local` (gitignored). Never commit, never log, never echo to stdout.
- Pass via env vars: `MASTTRO_API_KEY`, `MASTTRO_API_SECRET`.
- Auth header format: `Authorization: Basic base64(key:secret)`.

### Rate limits
- ~2,000 requests per day. 120 seconds max per request.
- During exploration, assume a much tighter self-imposed budget: **no more than 50 calls per session** unless explicitly approved.
- Every script that makes API calls must log the request count to stdout at the end.

### Response handling
- **Save every raw response to `responses/<endpoint>_<timestamp>_<descriptor>.json`** before doing anything else with it. We want a local cache of real payloads for reference, schema design, and offline reasoning.
- Never hit the same endpoint with the same parameters twice in one session if a saved response exists — read from disk instead.
- Pretty-print JSON on save (`JSON.stringify(data, null, 2)` or `json.dumps(data, indent=2)`).

### Exploration discipline
- **Start with one client (`id=<specific>`) before using `id=0` for tenant-wide pulls.** The `id=0` trick returns all clients subject to visibility — useful in production, overwhelming for inspection.
- Never loop across all entities on the first call to a new endpoint. Pick one entity, inspect the response, then expand.
- When testing, prefer `period=0` (MTD) over longer periods to keep payloads small.
- Add a CLI flag or env var for "dry run" mode that prints the URL it would call without actually calling it.

### Code conventions
- Build a thin client wrapper (`client.ts` or `client.py`) with `auth`, `get(path, params)`, `saveResponse(name, data)`. Use it for every call. No direct fetch/curl/requests in scripts.
- Log call count, response time (ms), and response size (bytes) per request.
- Use TypeScript or Python — pick one and stick to it for this exploration phase.

### What NOT to do yet
- Do not write Supabase migrations.
- Do not build sync workers or GitHub Actions.
- Do not normalise responses into typed domain models.
- Do not integrate with the Cashflow Planner.

All of that comes after we understand the API's behaviour in practice.

---

## API basics

- **Base URL:** `https://dfo.masttro.com/api/`
- **Auth:** HTTP Basic. Construct via `base64(apiKey + ":" + apiSecret)` and pass as `Authorization: Basic <encoded>`.
- **Protocol:** HTTPS only. Synchronous. JSON output. GET requests only.
- **Date formats:** `YYYYMM` for month parameters (e.g. `202605`), `YYYYMMDD` for full dates.
- **Currency:** ISO 3-letter code passed as `ccy` parameter (e.g. `AUD`, `USD`). Responses include both local-currency and reporting-currency values where applicable.
- **Period codes** (the `period` / `metric` parameter):
  - `0` = MTD
  - `1` = YTD
  - `2` = 3M
  - `3` = 6M
  - `4` = 12M

### Sample curl
```bash
curl -X GET 'https://dfo.masttro.com/api/Clients' \
  -H 'accept: text/plain' \
  -H "Authorization: Basic $(echo -n "$MASTTRO_API_KEY:$MASTTRO_API_SECRET" | base64)"
```

---

## Endpoint dependency model

`GET /Clients` is the root of everything. The `id` returned from Clients is the input for every other endpoint.

```
Clients (id)
  ├── GWM (nodes, including investment vehicle nodeIDs)
  ├── Holdings (current and historical positions)
  ├── Performance
  ├── Transactions
  ├── TaxLot/UnrealizedGL
  ├── TaxLot/RealizedGL
  ├── cef (closed-end funds / alternatives)
  ├── DataFeedUpdates
  ├── PublishedReports → DocumentID → Documents
  ├── DAI/Valuation → DocumentID → Documents  (Doc AI users only)
  └── DAI/CashFlow → DocumentID → Documents  (Doc AI users only)
```

**Key trick:** Passing `id=0` to most endpoints returns data for all clients visible to the API key. Use this for tenant-wide pulls. Use a specific `id` for testing and inspection.

GWM is only needed if you want to filter holdings/performance/transactions/datafeed by a specific investment vehicle subtree. For "all data" pulls, GWM is optional.

---

## URN connectivity (node IDs across wealth structures)

This matters for the Stonebridge multi-family setup.

- `nodeId` is **scoped per wealth structure**. The same node has a different `nodeId` in different wealth structures.
- `groupNodeId` is the **cross-structure identifier** assigned when a node is shared between or within wealth structures.
- `securityId` is a globally unique security identifier.

**Rule of thumb:**
- Joining nodes within one wealth structure → use `nodeId`.
- Joining nodes across wealth structures → use `groupNodeId`.
- Joining by security regardless of holder → use `securityId`.

Some endpoints (e.g. Unrealized G/L) report against the original `nodeId` rather than `groupNodeId`, so cross-structure aggregation may need a translation step via GWM output.

---

## Endpoint reference

All endpoints return JSON. Optional parameters in `[brackets]`.

### GET /Clients
**URL:** `https://dfo.masttro.com/api/Clients`
**Input:** none.
**Output fields (verified 2026-05-14 against this tenant):**
- `id` (int) — internal client code; **input for all other endpoints**
- `clientID` (string, **capital ID**) — alphanumeric ID from Clients module. Null for every Stonebridge entity in this tenant — do not rely on it for joins.
- `name` (string)
- `clientAlias` (string) — note: the original Masttro docs call this `alias`, but the actual JSON key is `clientAlias`.

### GET /GWM/{id}
**URL:** `https://dfo.masttro.com/api/GWM/{id}?ccy={ccy}[&status={status}]`
**Input:**
- `id` — from Clients
- `ccy` — reporting currency
- `status` (optional) — `1` for removed only, `2` for active + removed; default is active only.

**Output fields:** `nodeId`, `name`, `alias`, `bankBroker`, `accountNumber`, `ownershipPct`, `valuation`, `parentNodeId`, `date`, `groupNodeId`, `status`.

**Use:** entity tree, ownership structure, account-level valuations.

### GET /Holdings/{id}
**URL:** `https://dfo.masttro.com/api/Holdings/{id}?ccy={ccy}&yearMonth={yyyymm}[&investmentVehicle={invID}&historicalMonths={hMonths}]`
**Input:**
- `id`, `ccy`, `yearMonth`
- `investmentVehicle` (optional) — GWM nodeId to filter
- `historicalMonths` (optional, max 12) — number of months back from `yearMonth` to include in one call

**Position date behaviour:**
- Past month → returns last day of that month
- Current month → returns **current day** (this is what you want for "current positions")

**Output fields (selected):** `clientId`, `clientAlias`, `parentNodeId`, `bankBroker`, `accountNumber`, `assetName`, `nodeId`, `securityId`, `isin`, `sedol`, `cusip`, `ticker`, `assetClass`, `securityType`, `sector`, `geographicalExposure`, `quantity`, `unitCost`, `totalCost`, `price`, `marketValue`, `localCCY`, `localMarketValue`, `accruedInterest`, `localAccruedInterest`, `date`.

**Note:** for more than 12 months, paginate manually with multiple calls.

### GET /Performance/{id}
**URL:** `https://dfo.masttro.com/api/Performance/{id}?ccy={ccy}&yearMonth={yyyymm}&period={p}[&investmentVehicle={invID}]`
**Output fields (selected):** asset metadata + `deposits`, `withdrawals`, `transferInOut`, `periodRealizedGl`, `periodUnrealizedGl`, `income`, `totalPl`, `avgCapBase`, `marketValueInitial`, `marketValue`, `irr`, `twr`, `initialDate`, `date`.

### GET /Transactions/{id}
**URL:** `https://dfo.masttro.com/api/Transactions/{id}?ccy={ccy}&yearMonth={yyyymm}&period={p}[&investmentVehicle={invID}]`
**Output fields (selected):** asset metadata + `transactionDate`, `transactionType`, `concept`, `gwmInExType`, `comments`, `quantity`, `netPriceLocal`, `netAmountLocal`, `localCCY`, `netAmountRep`, `initialDate`, `date`.

### GET /TaxLot/UnrealizedGL/{id}
**URL:** `https://dfo.masttro.com/api/TaxLot/UnrealizedGL/{id}?ccy={ccy}&yearMonth={yyyymm}&period={p}`
**Output fields (selected):** tax lot level: `taxLot`, `acquisitionDate`, `unitCostLocalCCY`, `units`, `totalCostLocalCCY`, `exchangeRateCost`, `totalCostRepCCY`, `lastPriceLocalCCY`, `marketValueLocalCCY`, `exchangeRateLastPrice`, `marketValueRepCCY`, `unrealizedGainLossRepCCY`.

### GET /TaxLot/RealizedGL/{id}
**URL:** `https://dfo.masttro.com/api/TaxLot/RealizedGL/{id}?ccy={ccy}&yearMonth={yyyymm}&period={p}`
**Output fields (selected):** as above plus `saleDate`, `salePriceLocalCCY`, `tradeAmountLocalCCY`, `exchangeRateSellDate`, `tradeAmountRepCCY`, `realizedGainLossRepCCY`.

### GET /cef/{id} — Closed-End Funds (Alternatives)
**URL:** `https://dfo.masttro.com/api/cef/{id}?ccy={ccy}&yearMonth={yyyymm}&period={p}`

**Important:** the `period` parameter only affects reporting-currency gains/losses for CEFs. All other fields are point-in-time.

**Output fields (selected):**
- Identity: `clientID`, `clientAlias`, `invVehicle`, `invVehicleCode`, `parentNodeId`, `bankBroker`, `accountNumber`, `assetName`, `nodeId`, `assetManager`, `assetClass`, `securityType`
- Commitments: `initialInvestmentDate`, `commitment`, `capitalCalled`, `unfundedCommitment`, `pctCapitalCalled`
- Distributions: `capitalDistributed`, `recallableDistributions`, `dividends`, `feesPaid`, `income`
- Performance: `capitalGainsLocalCCY`, `localMarketValue`, `gainLossRepCCY`, `marketValueRepCCY`, `dpi`, `tvpi`, `pic`, `rvpi`, `xirrCumulative`
- Context: `lastValuationDate`, `sector`, `geoFocus`, `strategy`, `vintageYear`, `issuer`, `industryFocus`, `pctAssets`

**This is the gold endpoint for alternatives.** Replaces most of what you'd need from Doc AI extractions.

### GET /PublishedReports/{id}
**URL (two variants — use one, not both):**
- `https://dfo.masttro.com/api/PublishedReports/{id}?days={dd}` — reports published in last N days
- `https://dfo.masttro.com/api/PublishedReports/{id}?startdate={yyyymmdd}&enddate={yyyymmdd}` — date range

**Output fields:** `name`, `Location`, `publicationDate`, `reportDate`, `expirationDate`, `comments`, `documentId` (feed this to GET /Documents).

### GET /Documents/{documentId}
**URL:** `https://dfo.masttro.com/api/Documents/{documentId}`

`documentId` comes from PublishedReports or DAI endpoints (use `DocumentID`, **not** `aiDocumentID`).

**Output:** `id`, `fileExt`, `document` (Base64-encoded file content). Decode to retrieve the PDF.

**One document per call.** Burns rate limit if you pull many — consider whether you actually need the binary.

### GET /DAI/Valuation/{id} (Doc AI users only)
**URL:** `https://dfo.masttro.com/api/DAI/Valuation/{id}?initUploadDate={yyyymm}&endUploadDate={yyyymm}`

Lists Doc AI valuation statements in the upload date window. Includes extracted fields: `investor_name`, `fund_name`, `date`, `valuation`, plus `documentId` for retrieval.

Only documents marked "Successfully Processed" are returned.

### GET /DAI/CashFlow/{id} (Doc AI users only)
**URL:** `https://dfo.masttro.com/api/DAI/CashFlow/{id}?initUploadDate={yyyymm}&endUploadDate={yyyymm}`

Lists Doc AI cash flow statements. Rich extracted fields:
- `net_cashflow`, `investment_capital`, `management_fees`, `placements_fees`, `capital_gain`, `capital_loss`, `dividends`, `taxes`, `tax_refund`, `interest_earned`, `other_expenses`, `other_income`, `return_of_expenses`, `other_fees`, `other_contributions`, `syndication_costs`, `negative_capital_call`, `performance_fees`, `fee_refund`, `negative_distribution`, `negative_income`, `outflow_adjustment`, `corporate_tax_refund`, `inflow_adjustment`, `management_fee_refund`, `performance_fee_refund`, `placement_fee_refund`, `corporate_Tax`, `interest_expense`, `return_of_capital`

**Check whether our API key has Doc AI access before hitting these.** If 403 or empty, the user that generated the key lacks the module.

### GET /DataFeedUpdates/{id}
**URL:** `https://dfo.masttro.com/api/DataFeedUpdates/{id}?yearMonth={yyyymm}[&investmentVehicle={invID}]`

Returns `lastUpdate` timestamp per account/node. **Use this to monitor feed freshness across custodians** — flags stale data sources before users do.

---

## Critical caveats

1. **No `updated_since` / delta queries.** Every pull is a full snapshot. Plan syncs accordingly — full snapshots, append-only history in our own DB.
2. **Holdings is capped at 12 months per call.** For historical backfill, paginate manually.
3. **No `as_of` freshness timestamp on response payloads** (other than DataFeedUpdates). Record `sync_timestamp` ourselves.
4. **Equity prices lag ~24 hours, no weekend updates.** This is upstream of the API — Masttro does not have live market data feeds. Not solvable client-side.
5. **`nodeId` is per-wealth-structure.** Use `groupNodeId` for cross-structure joins.
6. **Documents are Base64 PDFs returned inline.** One per call. Don't pull in bulk casually.
7. **Doc AI endpoints require module access on the user that generated the API key.** Test early; if blocked, ask Masttro to provision.
8. **CEF `period` parameter only affects reporting-currency G/L.** Other CEF fields are point-in-time regardless.
9. **Holdings position date semantics:** past month → month-end; current month → current day.

---

## Exploration playbook

Suggested order of operations. **Do not skip steps.** Each step's output informs the next.

### Step 1 — Auth & connection sanity check
- Set up `.env.local` with credentials.
- Build minimal `client.ts` (or `.py`) with Basic auth and a `get(path, params)` method.
- Hit `GET /Clients`. Save response. Confirm we see expected wealth structures.
- Note: how many clients, what the `id` values look like, naming convention.

**Stop and review before continuing.** Confirm the client list matches expectations.

### Step 2 — One client, deep inspection
Pick one client `id` from Step 1 — ideally a smaller/simpler one for first inspection. For each of the following, make ONE call and save the response:

- `GET /GWM/{id}?ccy=AUD` — see the entity tree.
- `GET /Holdings/{id}?ccy=AUD&yearMonth=<currentYYYYMM>` — current positions.
- `GET /Transactions/{id}?ccy=AUD&yearMonth=<currentYYYYMM>&period=1` — YTD transactions.
- `GET /cef/{id}?ccy=AUD&yearMonth=<currentYYYYMM>&period=1` — alternatives.

Inspect each JSON:
- What fields are populated vs. null?
- Are values sensible vs. what the Masttro UI shows for the same client?
- How big is the payload?
- How long did the call take?

### Step 3 — Compare API output to UI
Open Masttro UI for the same client. Spot-check 3-5 holdings:
- Do market values match?
- Does currency conversion look correct?
- Are alternatives values (commitment, called, unfunded, NAV) consistent?
- Any holdings in UI but not in API output, or vice versa?

Document any discrepancies. This is the most important step for trusting the data.

### Step 4 — Tenant-wide pull
Now try `id=0`:
- `GET /Holdings/0?ccy=AUD&yearMonth=<currentYYYYMM>`
- Compare row count and total market value to the per-client pulls.
- Confirm all clients appear.
- Check response time and payload size — this is what production syncs will pull.

### Step 5 — Freshness check
- `GET /DataFeedUpdates/0?yearMonth=<currentYYYYMM>` for all entities.
- Identify any custodians/accounts with stale `lastUpdate`.
- This is a candidate for an internal monitoring dashboard later.

### Step 6 — Doc AI gate test
- `GET /DAI/Valuation/{id}?initUploadDate=<prevYYYYMM>&endUploadDate=<currentYYYYMM>`
- If it works → we have Doc AI module access on the API key.
- If 403 / empty → ask Masttro to provision Doc AI on the API-generating user.

### Step 7 — Document the findings
After exploration, write a short summary covering:
- Endpoint reliability and response times
- Data quality observations (gaps, mismatches, weird values)
- Whether each endpoint we tested is suitable for production sync
- Schema decisions (what to store, what to ignore)
- Open questions for the Masttro RM or solutions architect

Only after this summary do we move to building.

---

## Useful local file layout

```
/
├── .env.local                  (gitignored)
├── .gitignore
├── client.ts                   (or client.py)
├── responses/                  (raw JSON, gitignored or committed for reference)
│   ├── clients_20260514_initial.json
│   ├── gwm_<clientid>_20260514.json
│   └── ...
├── scripts/
│   ├── 01_list_clients.ts
│   ├── 02_inspect_client.ts
│   └── ...
├── MASTTRO_API.md              (this file)
└── README.md                   (project status, findings as we go)
```

---

## Open questions to answer through exploration

- Does `id=0` work on every endpoint that accepts an `id`, or only on some?
- For Holdings with `id=0`, what's the actual response size and call duration?
- Does CEF data reconcile with what's shown in the Masttro alternatives module UI?
- For accounts where the upstream custodian feed has lagged, does the API return stale data or omit it?
- What HTTP status codes does Masttro return on auth failure, rate limit, and bad parameters? (Test by malforming a request.)
- Are response payloads chunked/streamed for large pulls, or sent as a single JSON blob? (Affects how we handle the 120-second timeout.)
- Does the same `securityId` appear consistently across endpoints? (Holdings vs. Transactions vs. CEF.)

Add answers here as we go.