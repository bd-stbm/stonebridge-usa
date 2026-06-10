// Shown on the returns-bearing pages (Overview, Performance) when a Vehicle/SPV
// filter is active. Those surfaces compute returns through the carry-forward
// grid + reconstruction RPCs, which are vehicle-scoped only in Option C Phase 2.
// Until then, be explicit that the filter doesn't apply here.
export default function VehicleScopeNote({ vehicles }: { vehicles: string[] }) {
  if (vehicles.length === 0) return null;
  const label =
    vehicles.length === 1 ? vehicles[0] : `${vehicles.length} vehicles`;
  return (
    <div className="rounded-md border border-amber-200 bg-amber-50 px-4 py-2 text-xs text-amber-800">
      Vehicle filter (<span className="font-medium">{label}</span>) applies to
      Holdings, Income, Transactions and Net Worth. Returns and performance on
      this page are not yet vehicle-scoped.
    </div>
  );
}
