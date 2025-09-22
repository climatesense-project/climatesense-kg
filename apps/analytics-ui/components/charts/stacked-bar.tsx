"use client";

import { cn } from "@/lib/utils";

interface StackedBarProps {
  success: number;
  failure: number;
  className?: string;
}

export function StackedBar({ success, failure, className }: StackedBarProps) {
  const total = success + failure;
  const successPct = total === 0 ? 0 : (success / total) * 100;
  const failurePct = total === 0 ? 0 : (failure / total) * 100;

  return (
    <div className={cn("flex h-3 w-full overflow-hidden rounded-full bg-muted", className)}>
      <div
        className="h-full bg-blue-600 transition-all"
        style={{ width: `${successPct}%` }}
      />
      <div
        className="h-full bg-destructive/80 transition-all"
        style={{ width: `${failurePct}%` }}
      />
    </div>
  );
}
