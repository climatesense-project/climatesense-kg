"use client";

import { cn } from "@/lib/utils";

interface MetricProgressProps {
  value: number;
  max?: number;
  label?: string;
  showRatio?: boolean;
  className?: string;
}

export function MetricProgress({
  value,
  max = 100,
  label,
  showRatio = false,
  className
}: MetricProgressProps) {
  const ratio = max === 0 ? 0 : Math.min(100, (value / max) * 100);
  return (
    <div className={cn("flex flex-col space-y-1", className)}>
      {label ? <span className="text-sm text-muted-foreground">{label}</span> : null}
      <div className="flex h-2 w-full overflow-hidden rounded-full bg-muted">
        <div className="bg-blue-600" style={{ width: `${ratio}%` }} />
      </div>
      {showRatio && <span className="text-xs text-muted-foreground">{ratio.toFixed(2)}%</span>}
    </div>
  );
}
