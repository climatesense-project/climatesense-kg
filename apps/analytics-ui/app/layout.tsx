import type { Metadata } from "next";
import Link from "next/link";

import "./globals.css";

import { cn } from "@/lib/utils";

export const metadata: Metadata = {
  title: "ClimateSense Analytics",
  description: "Dashboard for pipeline and knowledge graph health",
};

const navLinks = [
  { href: "/dashboard", label: "Pipeline" },
  { href: "/knowledge-graph", label: "Knowledge Graph" },
];

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-background font-sans text-foreground">
        <div className="flex min-h-screen flex-col">
          <header className="border-b bg-card">
            <div className="mx-auto flex w-full max-w-6xl items-center justify-between px-6 py-4">
              <Link href="/dashboard" className="text-lg font-semibold">
                ClimateSense Analytics
              </Link>
              <nav className="flex items-center gap-4 text-sm font-medium text-muted-foreground">
                {navLinks.map((link) => (
                  <Link
                    key={link.href}
                    href={{
                      pathname: link.href
                    }}
                    className={cn(
                      "rounded-md px-3 py-1.5 transition-colors hover:text-foreground"
                    )}
                  >
                    {link.label}
                  </Link>
                ))}
              </nav>
            </div>
          </header>
          <main className="mx-auto w-full max-w-6xl flex-1 px-6 py-8">{children}</main>
          <footer className="border-t bg-card py-4 text-center text-xs text-muted-foreground">
            ClimateSense Project · {new Date().getFullYear()}
          </footer>
        </div>
      </body>
    </html>
  );
}
