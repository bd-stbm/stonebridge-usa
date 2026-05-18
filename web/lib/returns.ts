// Modified-Dietz period returns. Mirrors tracker/compute.py::period_performance
// (Python). Formula: r = (end_nav - start_nav - flows) / (start_nav + 0.5 * flows).
// 0.5 weighting is the simple-Dietz midpoint approximation — fine for short
// periods, matches what the Python pipeline produces, and skips the per-flow
// time-weighting that true modified Dietz uses.

export type PeriodKey = "1d" | "1w" | "ytd" | "6m" | "1y";

export const PERIODS: { key: PeriodKey; label: string }[] = [
  { key: "1d", label: "1D" },
  { key: "1w", label: "1W" },
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
      d.setDate(d.getDate() - 1);
      return d;
    case "1w":
      d.setDate(d.getDate() - 7);
      return d;
    case "ytd":
      // Dec 31 of the previous year.
      return new Date(Date.UTC(end.getUTCFullYear() - 1, 11, 31));
    case "6m":
      d.setMonth(d.getMonth() - 6);
      return d;
    case "1y":
      d.setFullYear(d.getFullYear() - 1);
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

export function computePeriodReturn(
  navs: NavPoint[],
  flows: Flow[],
  period: PeriodKey,
): PeriodReturn {
  if (navs.length === 0) {
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

  const end = navs[navs.length - 1];
  const endDate = end.date;
  const target = toISO(shiftDate(new Date(end.date + "T00:00:00Z"), period));

  // Clamp target to the earliest snapshot if the period reaches before our
  // data window (e.g. 1Y when only 9 months of history exist).
  const earliest = navs[0].date;
  const clampedTarget = target < earliest ? earliest : target;

  const start = nearestOnOrBefore(navs, clampedTarget);
  if (!start || start.date >= endDate) {
    return {
      period,
      start_date: start?.date ?? null,
      end_date: endDate,
      start_nav: start?.nav ?? null,
      end_nav: end.nav,
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

  const gain = end.nav - start.nav - periodFlows;
  const denom = start.nav + 0.5 * periodFlows;
  const return_pct = denom !== 0 ? gain / denom : null;

  return {
    period,
    start_date: start.date,
    end_date: endDate,
    start_nav: start.nav,
    end_nav: end.nav,
    flows: periodFlows,
    gain,
    return_pct,
  };
}

export function computeAllPeriodReturns(
  navs: NavPoint[],
  flows: Flow[],
): Record<PeriodKey, PeriodReturn> {
  return Object.fromEntries(
    PERIODS.map(p => [p.key, computePeriodReturn(navs, flows, p.key)]),
  ) as Record<PeriodKey, PeriodReturn>;
}
