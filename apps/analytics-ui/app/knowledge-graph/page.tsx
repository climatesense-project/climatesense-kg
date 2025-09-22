"use client";

import { Database, Layers, Link2, Sparkles } from "lucide-react";

import { MetricProgress } from "@/components/charts/metric-progress";
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
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  useKgClassDistribution,
  useKgEnrichmentCoverage,
  useKgEntityTypes,
  useKgProvenance,
  useKgTripleStats,
} from "@/lib/hooks";

export default function KnowledgeGraphPage() {
  const { data: tripleStats, loading: tripleLoading } = useKgTripleStats();
  const { data: classDistribution, loading: classLoading } =
    useKgClassDistribution();
  const { data: provenance, loading: provenanceLoading } = useKgProvenance();
  const { data: enrichment, loading: enrichmentLoading } =
    useKgEnrichmentCoverage();
  const { data: entityTypes, loading: entityLoading } = useKgEntityTypes();

  const totalTriples =
    tripleStats?.reduce((acc, row) => acc + row.triple_count, 0) ?? 0;

  return (
    <div className="space-y-6">
      <section className="space-y-2">
        <h1 className="text-3xl font-semibold tracking-tight">
          Knowledge Graph Analytics
        </h1>
        <p className="text-muted-foreground">
          Overview of Knowledge Graph statistics and enrichment coverage.
        </p>
      </section>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total triples</CardTitle>
            <Database className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            {tripleLoading ? (
              <Skeleton className="h-8 w-20" />
            ) : (
              <div className="text-2xl font-bold">
                {totalTriples.toLocaleString()}
              </div>
            )}
            <CardDescription>Summed across named graphs</CardDescription>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Named graphs</CardTitle>
            <Layers className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            {tripleLoading ? (
              <Skeleton className="h-8 w-16" />
            ) : (
              <div className="text-2xl font-bold">
                {tripleStats?.length ?? 0}
              </div>
            )}
            <CardDescription>Graphs with recorded triples</CardDescription>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              Reviews with authors
            </CardTitle>
            <Link2 className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            {provenanceLoading ? (
              <Skeleton className="h-8 w-20" />
            ) : (
              <div className="text-2xl font-bold">
                {provenance?.reviews_with_author.toLocaleString() ?? "0"}
              </div>
            )}
            <CardDescription>
              Out of {provenance?.total_reviews.toLocaleString() ?? "0"} reviews
            </CardDescription>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              Claims with enrichment
            </CardTitle>
            <Sparkles className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            {enrichmentLoading ? (
              <Skeleton className="h-8 w-20" />
            ) : (
              <div className="text-2xl font-bold">
                {enrichment?.claims_with_sentiment.toLocaleString() ?? "0"}
              </div>
            )}
            <CardDescription>Claims enriched with sentiment</CardDescription>
          </CardContent>
        </Card>
      </div>

      <section className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Triple volume by graph</CardTitle>
            <CardDescription>
              Top named graphs ranked by triple count
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {tripleLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : tripleStats && tripleStats.length > 0 ? (
              tripleStats.map((row) => (
                <div key={row.graph ?? "default"} className="space-y-1">
                  <div className="flex items-center justify-between text-sm">
                    <span className="truncate" title={row.graph ?? "default"}>
                      {row.graph ?? "default"}
                    </span>
                    <span className="font-medium">
                      {row.triple_count.toLocaleString()}
                    </span>
                  </div>
                  <MetricProgress
                    value={row.triple_count}
                    max={totalTriples}
                    showRatio
                  />
                </div>
              ))
            ) : (
              <p className="text-sm text-muted-foreground">
                No triple statistics available.
              </p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Class distribution</CardTitle>
            <CardDescription>
              Most frequent RDF classes in the KG
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {classLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : classDistribution && classDistribution.length > 0 ? (
              classDistribution.slice(0, 10).map((row) => (
                <div key={row.class_uri ?? "unknown"} className="space-y-1">
                  <div className="flex items-center justify-between text-sm">
                    <span
                      className="truncate"
                      title={row.class_uri ?? "unknown"}
                    >
                      {row.class_uri ?? "unknown"}
                    </span>
                    <span className="font-medium">
                      {row.count.toLocaleString()}
                    </span>
                  </div>
                  <MetricProgress
                    value={row.count}
                    max={classDistribution[0]?.count ?? 1}
                  />
                </div>
              ))
            ) : (
              <p className="text-sm text-muted-foreground">
                No class data available.
              </p>
            )}
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Provenance completeness</CardTitle>
            <CardDescription>
              Coverage of critical review linkages
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {provenanceLoading ? (
              <Skeleton className="h-32 w-full" />
            ) : provenance ? (
              <>
                <MetricProgress
                  label="Reviews with authors"
                  value={provenance.reviews_with_author}
                  max={provenance.total_reviews}
                  showRatio
                />
                <MetricProgress
                  label="Reviews with ratings"
                  value={provenance.reviews_with_rating}
                  max={provenance.total_reviews}
                  showRatio
                />
                <MetricProgress
                  label="Reviews with normalized ratings"
                  value={provenance.reviews_with_normalized_rating}
                  max={provenance.total_reviews}
                  showRatio
                />
              </>
            ) : (
              <p className="text-sm text-muted-foreground">
                No provenance data available.
              </p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Top entities</CardTitle>
            <CardDescription>
              Most frequently mentioned entities in claims
            </CardDescription>
          </CardHeader>
          <CardContent>
            {entityLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : entityTypes && entityTypes.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Type</TableHead>
                    <TableHead className="text-right">Mentions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {entityTypes.slice(0, 10).map((row) => (
                    <TableRow key={row.type_uri ?? "unknown"}>
                      <TableCell
                        className="truncate"
                        title={row.type_uri ?? "unknown"}
                      >
                        {row.type_uri?.startsWith("http") ? (
                          <a
                            href={row.type_uri}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="hover:underline"
                          >
                            {row.type_uri}
                          </a>
                        ) : (
                          row.type_uri ?? "unknown"
                        )}
                      </TableCell>
                      <TableCell className="text-right">
                        {row.count.toLocaleString()}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <p className="text-sm text-muted-foreground">
                No entity statistics captured.
              </p>
            )}
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
