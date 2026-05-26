// Shared types + key helper for the holdings_period_attribution RPC
// output. Split out from lib/queries.ts so client components can import
// it without dragging in next/headers and the Supabase server client.

import type { PeriodKey } from "./returns";

// 1D is computed client-side from v_positions_refreshed.mv_reporting /
// mv_reporting_yesterday, so the RPC doesn't cover it.
export type HoldingsPeriodKey = Exclude<PeriodKey, "1d">;

export interface HoldingsGainPieces {
  start_mv: number;
  flows: number;
  income: number;
}

export type HoldingsPeriodGainMap = Map<string, HoldingsGainPieces>;

export function holdingsGainKey(
  period: HoldingsPeriodKey,
  accountNodeId: string,
  securityId: number,
): string {
  return `${period}|${accountNodeId}|${securityId}`;
}
