import "./globals.css";
import type { Metadata, Viewport } from "next";

export const metadata: Metadata = {
  title: "Stonebridge Dashboard",
  description: "Wealth tracking dashboard for Stonebridge clients",
};

// Next injects a sensible default, but pin it explicitly so phones render
// at device width and don't allow awkward zoom-out on the data tables.
export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen">{children}</body>
    </html>
  );
}
