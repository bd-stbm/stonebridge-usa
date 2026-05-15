import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Stonebridge Dashboard",
  description: "Wealth tracking dashboard for Stonebridge clients",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen">{children}</body>
    </html>
  );
}
