import clsx from "clsx";

interface Props {
  label: string;
  value: string;
  hint?: string;
  tone?: "default" | "positive" | "negative";
}

export default function KpiTile({ label, value, hint, tone = "default" }: Props) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-5">
      <div className="text-xs uppercase tracking-wide text-slate-500">{label}</div>
      <div
        className={clsx(
          "mt-2 text-2xl font-semibold",
          tone === "positive" && "text-emerald-600",
          tone === "negative" && "text-rose-600",
          tone === "default" && "text-slate-900",
        )}
      >
        {value}
      </div>
      {hint && <div className="mt-1 text-xs text-slate-400">{hint}</div>}
    </div>
  );
}
