// Rendered instantly by Next.js while the route's server component is
// fetching data. Header lives in the parent layout, so it stays visible
// during the transition — only the main content area shows the skeleton.

function SkeletonBlock({ className }: { className: string }) {
  return (
    <div
      className={`animate-pulse rounded bg-slate-200/70 ${className}`}
      aria-hidden
    />
  );
}

export default function Loading() {
  return (
    <main className="mx-auto max-w-7xl px-4 py-8 sm:px-6">
      <div className="mb-6 flex items-baseline justify-between">
        <SkeletonBlock className="h-7 w-40" />
        <SkeletonBlock className="h-4 w-56" />
      </div>
      <div className="mb-6 grid grid-cols-2 gap-4 md:grid-cols-4">
        <SkeletonBlock className="h-24 w-full" />
        <SkeletonBlock className="h-24 w-full" />
        <SkeletonBlock className="h-24 w-full" />
        <SkeletonBlock className="h-24 w-full" />
      </div>
      <SkeletonBlock className="mb-6 h-64 w-full" />
      <div className="space-y-2">
        <SkeletonBlock className="h-8 w-full" />
        <SkeletonBlock className="h-8 w-full" />
        <SkeletonBlock className="h-8 w-full" />
        <SkeletonBlock className="h-8 w-full" />
        <SkeletonBlock className="h-8 w-full" />
        <SkeletonBlock className="h-8 w-full" />
      </div>
    </main>
  );
}
