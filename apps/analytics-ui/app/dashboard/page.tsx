"use client";

import { AlertTriangle, BarChart3, CheckCircle2, Globe2 } from "lucide-react";

import { StackedBar } from "@/components/charts/stacked-bar";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  useEnricherActivity,
  useEnricherDomainFailures,
  useEnricherErrors,
  useEnricherSuccess,
} from "@/lib/hooks";

function MetricsSkeleton() {
  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {Array.from({ length: 4 }).map((_, index) => (
        <Card key={index}>
          <CardHeader>
            <Skeleton className="h-4 w-24" />
          </CardHeader>
          <CardContent>
            <Skeleton className="h-8 w-16" />
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

export default function DashboardPage() {
  const { data: successData, loading: successLoading } = useEnricherSuccess();
  const { data: errorData, loading: errorLoading } = useEnricherErrors({ limit: 25 });
  const { data: domainFailures, loading: domainLoading } = useEnricherDomainFailures({ limit: 10 });
  const { data: recentActivity, loading: activityLoading } = useEnricherActivity({ limit: 10 });

  const totals = successData?.reduce(
    (acc, current) => {
      acc.totalEntries += current.total_entries;
      acc.successful += current.successful;
      acc.failed += current.failed;
      return acc;
    },
    { totalEntries: 0, successful: 0, failed: 0 }
  );

  const overallSuccessRate = totals
    ? totals.totalEntries === 0
      ? 0
      : (totals.successful / totals.totalEntries) * 100
    : 0;

  return (
    <div className="space-y-6">
      <section className="space-y-2">
        <h1 className="text-3xl font-semibold tracking-tight">Pipeline Overview</h1>
        <p className="text-muted-foreground">
          Monitor the health and performance of the data enrichment pipeline.
        </p>
      </section>

      {successLoading ? (
        <MetricsSkeleton />
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Success rate</CardTitle>
              <CheckCircle2 className="h-4 w-4 text-primary" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{overallSuccessRate.toFixed(2)}%</div>
              <CardDescription>Aggregate across all enrichers</CardDescription>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total processed</CardTitle>
              <BarChart3 className="h-4 w-4 text-primary" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{totals?.totalEntries ?? 0}</div>
              <CardDescription>Cache entries observed</CardDescription>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Successes</CardTitle>
              <CheckCircle2 className="h-4 w-4 text-primary" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-emerald-500">
                {totals?.successful ?? 0}
              </div>
              <CardDescription>Successful enrichment runs</CardDescription>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Failures</CardTitle>
              <AlertTriangle className="h-4 w-4 text-destructive" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-destructive">
                {totals?.failed ?? 0}
              </div>
              <CardDescription>Across all enrichers</CardDescription>
            </CardContent>
          </Card>
        </div>
      )}

      <section className="grid gap-4 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Enricher success breakdown</CardTitle>
            <CardDescription>Relative success vs failure per enricher step</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {successLoading ? (
              <Skeleton className="h-40 w-full" />
            ) : (
              successData?.map((item) => {
                const total = item.successful + item.failed;
                const label = item.step.replace("enricher.", "");
                return (
                  <div key={item.step} className="space-y-2">
                    <div className="flex items-center justify-between text-sm">
                      <span className="font-medium">{label}</span>
                      <span className="text-muted-foreground">
                        {total} runs · {item.success_rate_percent.toFixed(2)}% success
                      </span>
                    </div>
                    <StackedBar success={item.successful} failure={item.failed} />
                  </div>
                );
              })
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Top failing domains</CardTitle>
            <CardDescription>URL text extraction hotspots</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {domainLoading ? (
              <Skeleton className="h-40 w-full" />
            ) : (
              domainFailures?.map((item) => (
                <div key={`${item.step}-${item.domain}`} className="flex items-center justify-between">
                  <div className="flex items-center gap-2 text-sm">
                    <Globe2 className="h-4 w-4 text-muted-foreground" />
                    <span className="truncate" title={item.domain}>
                      {item.domain}
                    </span>
                  </div>
                  <span className="text-sm font-medium">{item.failure_count}</span>
                </div>
              )) ?? <p className="text-sm text-muted-foreground">No failures recorded.</p>
            )}
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Error breakdown</CardTitle>
            <CardDescription>Most frequent error types per enricher</CardDescription>
          </CardHeader>
          <CardContent>
            {errorLoading ? (
              <Skeleton className="h-40 w-full" />
            ) : errorData && errorData.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Enricher</TableHead>
                    <TableHead>Error type</TableHead>
                    <TableHead className="text-right">Count</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {errorData.map((row) => (
                    <TableRow key={`${row.step}-${row.error_type ?? "unknown"}`}>
                      <TableCell>{row.step.replace("enricher.", "")}</TableCell>
                      <TableCell>
                        {row.error_type?.replaceAll("_", " ") ?? "unknown"}
                      </TableCell>
                      <TableCell className="text-right">{row.error_count}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
                <TableCaption>Limited to the top 25 error signatures.</TableCaption>
              </Table>
            ) : (
              <p className="text-sm text-muted-foreground">No errors recorded for the selected window.</p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Recent activity</CardTitle>
            <CardDescription>Most active enrichers in the last 24h</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {activityLoading ? (
              <Skeleton className="h-40 w-full" />
            ) : recentActivity && recentActivity.length > 0 ? (
              recentActivity.map((entry) => (
                <div key={entry.step} className="space-y-1">
                  <div className="flex items-center justify-between text-sm">
                    <div className="flex items-center gap-2">
                      <AlertTriangle className="h-4 w-4 text-muted-foreground" />
                      <span>{entry.step.replace("enricher.", "")}</span>
                    </div>
                    <span className="text-xs text-muted-foreground">
                      {entry.recent_entries} events
                    </span>
                  </div>
                  <StackedBar success={entry.successful} failure={entry.failed} />
                  <p className="text-xs text-muted-foreground">
                    Latest update: {entry.latest ? new Date(entry.latest).toLocaleString() : "n/a"}
                  </p>
                </div>
              ))
            ) : (
              <p className="text-sm text-muted-foreground">No activity detected in the selected window.</p>
            )}
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
