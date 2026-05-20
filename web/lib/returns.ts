// Modified-Dietz period returns. Mirrors tracker/compute.py::period_performance
// (Python). Formula: r = (end_nav - start_nav - flows) / (start_nav + 0.5 * flows).
// 0.5 weighting is the simple-Dietz midpoint approximation — fine for short
// periods, matches what the Python pipeline produces, and skips the per-flow
// time-weighting that true modified Dietz uses.

export type PeriodKey = "1d" | "mtd" | "ytd" | "6m" | "1y";

export const PERIODS: { key: PeriodKey; label: string }[] = [
  { key: "1d", label: "1D" },
  { key: "mtd", label: "MTD" },
  { key: "ytd", label: "YTD" },
  { key: "6m", label: "6M" },
  { key: "1y", label: "1Y" },
];

export interface NavPoint {
  date: string; // ISO yyyy-mm-dd
  nav: number;
}

export interface Flow {
  date: string; // ISO
  amount: number;
}

export interface PeriodReturn {
  period: PeriodKey;
  start_date: string | null;
  end_date: string | null;
  start_nav: number | null;
  end_nav: number | null;
  flows: number;
  gain: number | null;
  return_pct: number | null;
}

function shiftDate(end: Date, period: PeriodKey): Date {
  const d = new Date(end);
  switch (period) {
    case "1d":
      d.setUTCDate(d.getUTCDate() - 1);
      return d;
    case "mtd":
      // Last day of the previous month. nearestOnOrBefore then picks
      // whatever snapshot we actually have at or before that date —
      // typically the prior month-end.
      return new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), 0));
    case "ytd":
      // Dec 31 of the previous year.
      return new Date(Date.UTC(end.getUTCFullYear() - 1, 11, 31));
    case "6m":
      d.setUTCMonth(d.getUTCMonth() - 6);
      return d;
    case "1y":
      d.setUTCFullYear(d.getUTCFullYear() - 1);
      return d;
  }
}

function toISO(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function nearestOnOrBefore(
  navs: NavPoint[],
  target: string,
): NavPoint | null {
  // navs assumed sorted ascending by date.
  let candidate: NavPoint | null = null;
  for (const p of navs) {
    if (p.date <= target) candidate = p;
    else break;
  }
  return candidate;
}

export interface ReturnOverrides {
  // Refreshed end NAV (yfinance-priced sum of today's positions). Replaces
  // the last point in the Masttro NAV series so end-of-period values are
  // current to today's market rather than yesterday's settled close.
  endNav?: number;
  // Today's positions valued at yfinance previous-close. Used as the start
  // for the 1D return so it reflects pure intraday market movement (no
  // flow adjustment — 1-day-overnight flows are usually nil and including
  // them double-counts since both sides hold the same position quantity).
  endNavYesterday?: number;
  // Per-period precise start NAV (and the actual date used). Overrides the
  // nearestOnOrBefore lookup against the snapshot grid. Populated from the
  // reconstructed_nav_at RPC for 6M / 1Y so those returns reflect the
  // exact target date instead of snapping to month-end.
  startNavByPeriod?: Partial<Record<PeriodKey, { nav: number; date: string }>>;
}

export function computePeriodReturn(
  navs: NavPoint[],
  flows: Flow[],
  period: PeriodKey,
  overrides: ReturnOverrides = {},
): PeriodReturn {
  if (navs.length === 0 && overrides.endNav == null) {
    return {
      period,
      start_date: null,
      end_date: null,
      start_nav: null,
      end_nav: null,
      flows: 0,
      gain: null,
      return_pct: null,
    };
  }

  const lastNav = navs[navs.length - 1];
  // End date is the latest Masttro snapshot date — that's the date our
  // historical NAV / position quantities are anchored to, even though the
  // price layer on top of it is refreshed.
  const endDate = lastNav?.date ?? toISO(new Date());
  const endNav = overrides.endNav ?? lastNav.nav;

  // 1D special-case: use overrides.endNavYesterday as the start NAV. This
  // sidesteps the Masttro snapshot grid entirely — no flow adjustment
  // since both sides are today's quantities at different prices.
  if (period === "1d" && overrides.endNavYesterday != null) {
    const startDateGuess = (() => {
      const d = new Date(endDate + "T00:00:00Z");
      d.setUTCDate(d.getUTCDate() - 1);
      return toISO(d);
    })();
    const startNav = overrides.endNavYesterday;
    const gain = endNav - startNav;
    return {
      period,
      start_date: startDateGuess,
      end_date: endDate,
      start_nav: startNav,
      end_nav: endNav,
      flows: 0,
      gain,
      return_pct: startNav !== 0 ? gain / startNav : null,
    };
  }

  // Precise start NAV via reconstruction (if provided). Bypasses the
  // snapshot-grid snap entirely so 6M / 1Y use exact target dates.
  const startOverride = overrides.startNavByPeriod?.[period];
  if (startOverride && startOverride.date < endDate) {
    let periodFlows = 0;
    for (const f of flows) {
      if (f.date > startOverride.date && f.date <= endDate) {
        periodFlows += f.amount;
      }
    }
    const gain = endNav - startOverride.nav - periodFlows;
    const denom = startOverride.nav + 0.5 * periodFlows;
    return {
      period,
      start_date: startOverride.date,
      end_date: endDate,
      start_nav: startOverride.nav,
      end_nav: endNav,
      flows: periodFlows,
      gain,
      return_pct: denom !== 0 ? gain / denom : null,
    };
  }

  const target = toISO(shiftDate(new Date(endDate + "T00:00:00Z"), period));

  // Clamp target to the earliest snapshot if the period reaches before our
  // data window (e.g. 1Y when only 9 months of history exist).
  const earliest = navs[0]?.date ?? endDate;
  const clampedTarget = target < earliest ? earliest : target;

  const start = nearestOnOrBefore(navs, clampedTarget);
  if (!start || start.date >= endDate) {
    return {
      period,
      start_date: start?.date ?? null,
      end_date: endDate,
      start_nav: start?.nav ?? null,
      end_nav: endNav,
      flows: 0,
      gain: null,
      return_pct: null,
    };
  }

  // Sum flows strictly after start_date and on/before end_date.
  let periodFlows = 0;
  for (const f of flows) {
    if (f.date > start.date && f.date <= endDate) periodFlows += f.amount;
  }

  const gain = endNav - start.nav - periodFlows;
  const denom = start.nav + 0.5 * periodFlows;
  const return_pct = denom !== 0 ? gain / denom : null;

  return {
    period,
    start_date: start.date,
    end_date: endDate,
    start_nav: start.nav,
    end_nav: endNav,
    flows: periodFlows,
    gain,
    return_pct,
  };
}

// Compute the precise target date for a given period offset from a reference
// date. Symmetric with shiftDate() but exposed so callers (page layer) can
// pre-compute targets to send to reconstructed_nav_at.
export function computePeriodStart(period: PeriodKey, end: Date): string {
  return toISO(shiftDate(new Date(end), period));
}

export function computeAllPeriodReturns(
  navs: NavPoint[],
  flows: Flow[],
  overrides: ReturnOverrides = {},
): Record<PeriodKey, PeriodReturn> {
  return Object.fromEntries(
    PERIODS.map(p => [p.key, computePeriodReturn(navs, flows, p.key, overrides)]),
  ) as Record<PeriodKey, PeriodReturn>;
}

// ---------------------------------------------------------------------------
// Index benchmark returns
// ---------------------------------------------------------------------------

export interface IndexPricePoint {
  date: string;
  price: number;
}

export function computeIndexReturn(
  prices: IndexPricePoint[],
  startDate: string | null,
  endDate: string | null,
  period?: PeriodKey,
): number | null {
  if (prices.length === 0) return null;

  // 1D is a "previous-close vs latest-close" comparison, not a snapshot-grid
  // calc. Use the last two available prices regardless of the portfolio's
  // reported period dates (which can land on weekends or holidays).
  if (period === "1d") {
    if (prices.length < 2) return null;
    const last = prices[prices.length - 1];
    const prev = prices[prices.length - 2];
    return prev.price !== 0 ? last.price / prev.price - 1 : null;
  }

  if (!startDate || !endDate) return null;

  // Walk forwards taking the latest price on/before each target date.
  let startPrice: number | null = null;
  let endPrice: number | null = null;
  for (const p of prices) {
    if (p.date <= startDate) startPrice = p.price;
    if (p.date <= endDate) endPrice = p.price;
    if (p.date > endDate) break;
  }
  if (startPrice == null || endPrice == null || startPrice === 0) return null;
  return endPrice / startPrice - 1;
}

export function computeIndexReturnsForAllPeriods(
  prices: IndexPricePoint[],
  returns: Record<PeriodKey, PeriodReturn>,
): Record<PeriodKey, number | null> {
  return Object.fromEntries(
    PERIODS.map(p => [
      p.key,
      computeIndexReturn(prices, returns[p.key].start_date, returns[p.key].end_date, p.key),
    ]),
  ) as Record<PeriodKey, number | null>;
}
