"use client";

import { Database, FileCheckIcon, FileIcon, StarIcon } from "lucide-react";

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
  useKgClaimFactors,
  useKgCoreCounts,
  useKgEntityTypes,
  useKgTripleStats,
} from "@/lib/hooks";

export default function KnowledgeGraphPage() {
  const { data: tripleStats, loading: tripleLoading } = useKgTripleStats();
  const { data: classDistribution, loading: classLoading } =
    useKgClassDistribution();
  const { data: coreCounts, loading: coreCountsLoading } = useKgCoreCounts();
  const { data: entityTypes, loading: entityLoading } = useKgEntityTypes();
  const { data: claimFactors, loading: factorsLoading } = useKgClaimFactors();

  const totalTriples =
    tripleStats?.reduce((acc, row) => acc + row.triple_count, 0) ?? 0;
  const namedGraphs = tripleStats?.length ?? 0;
  const sentimentTotal = claimFactors
    ? claimFactors.sentiment.reduce((acc, item) => acc + item.count, 0)
    : 0;
  const leaningTotal = claimFactors
    ? claimFactors.political_leaning.reduce((acc, item) => acc + item.count, 0)
    : 0;
  const emotionMax =
    claimFactors && claimFactors.emotion.length > 0
      ? claimFactors.emotion[0].count
      : 0;
  const tropeMax =
    claimFactors && claimFactors.tropes.length > 0
      ? claimFactors.tropes[0].count
      : 0;
  const persuasionMax =
    claimFactors && claimFactors.persuasion_techniques.length > 0
      ? claimFactors.persuasion_techniques[0].count
      : 0;
  const mentionedMax =
    claimFactors && claimFactors.conspiracies_mentioned.length > 0
      ? claimFactors.conspiracies_mentioned[0].count
      : 0;
  const promotedMax =
    claimFactors && claimFactors.conspiracies_promoted.length > 0
      ? claimFactors.conspiracies_promoted[0].count
      : 0;
  const conspiracyFactors =
    claimFactors &&
    (claimFactors.conspiracies_mentioned.length > 0 ||
      claimFactors.conspiracies_promoted.length > 0)
      ? claimFactors
      : null;

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
            <CardDescription>
              Summed across {namedGraphs.toLocaleString()} named graphs
            </CardDescription>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total claims</CardTitle>
            <FileIcon className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            {coreCountsLoading ? (
              <Skeleton className="h-8 w-20" />
            ) : (
              <div className="text-2xl font-bold">
                {coreCounts?.total_claims.toLocaleString() ?? "0"}
              </div>
            )}
            <CardDescription>Unique claims in the KG</CardDescription>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Claim reviews</CardTitle>
            <FileCheckIcon className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            {coreCountsLoading ? (
              <Skeleton className="h-8 w-20" />
            ) : (
              <div className="text-2xl font-bold">
                {coreCounts?.total_claim_reviews.toLocaleString() ?? "0"}
              </div>
            )}
            <CardDescription>Total reviews linked to claims</CardDescription>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total ratings</CardTitle>
            <StarIcon className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            {coreCountsLoading ? (
              <Skeleton className="h-8 w-20" />
            ) : (
              <div className="text-2xl font-bold">
                {coreCounts?.total_ratings.toLocaleString() ?? "0"}
              </div>
            )}
            <CardDescription>Ratings associated with reviews</CardDescription>
          </CardContent>
        </Card>
      </div>

      <section className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Claim sentiment split</CardTitle>
            <CardDescription>
              Distribution of sentiment-enriched claims
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {factorsLoading ? (
              <Skeleton className="h-24 w-full" />
            ) : claimFactors && claimFactors.sentiment.length > 0 ? (
              claimFactors.sentiment.map((item) => (
                <MetricProgress
                  key={item.value}
                  label={`${item.label} (${item.count.toLocaleString()})`}
                  value={item.count}
                  max={sentimentTotal || 1}
                  showRatio
                />
              ))
            ) : (
              <p className="text-sm text-muted-foreground">
                No sentiment enrichment data available.
              </p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Political leaning split</CardTitle>
            <CardDescription>
              Claims grouped by inferred political leaning
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {factorsLoading ? (
              <Skeleton className="h-24 w-full" />
            ) : claimFactors && claimFactors.political_leaning.length > 0 ? (
              claimFactors.political_leaning.map((item) => (
                <MetricProgress
                  key={item.value}
                  label={`${item.label} (${item.count.toLocaleString()})`}
                  value={item.count}
                  max={leaningTotal || 1}
                  showRatio
                />
              ))
            ) : (
              <p className="text-sm text-muted-foreground">
                No political leaning data available.
              </p>
            )}
          </CardContent>
        </Card>
      </section>

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
            <CardTitle>Top emotions</CardTitle>
            <CardDescription>
              Most common emotion assignments from enrichment
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {factorsLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : claimFactors && claimFactors.emotion.length > 0 ? (
              claimFactors.emotion.slice(0, 10).map((item) => (
                <div key={item.value} className="space-y-1">
                  <div className="flex items-center justify-between text-sm">
                    <span>{item.label}</span>
                    <span className="font-medium">
                      {item.count.toLocaleString()}
                    </span>
                  </div>
                  <MetricProgress
                    value={item.count}
                    max={emotionMax || 1}
                  />
                </div>
              ))
            ) : (
              <p className="text-sm text-muted-foreground">
                No emotion data available.
              </p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Conspiracy references</CardTitle>
            <CardDescription>
              Claims mentioning or promoting conspiracy topics
            </CardDescription>
          </CardHeader>
          <CardContent>
            {factorsLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : conspiracyFactors ? (
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <h4 className="text-sm font-medium">Mentioned</h4>
                  {conspiracyFactors.conspiracies_mentioned.length > 0 ? (
                    conspiracyFactors.conspiracies_mentioned
                      .slice(0, 10)
                      .map((item) => (
                        <div key={`mentioned-${item.value}`} className="space-y-1">
                          <div className="flex items-center justify-between text-sm">
                            <span>{item.label}</span>
                            <span className="font-medium">
                              {item.count.toLocaleString()}
                            </span>
                          </div>
                          <MetricProgress
                            value={item.count}
                            max={mentionedMax || 1}
                          />
                        </div>
                      ))
                  ) : (
                    <p className="text-sm text-muted-foreground">
                      No conspiracies mentioned.
                    </p>
                  )}
                </div>
                <div className="space-y-2">
                  <h4 className="text-sm font-medium">Promoted</h4>
                  {conspiracyFactors.conspiracies_promoted.length > 0 ? (
                    conspiracyFactors.conspiracies_promoted
                      .slice(0, 10)
                      .map((item) => (
                        <div key={`promoted-${item.value}`} className="space-y-1">
                          <div className="flex items-center justify-between text-sm">
                            <span>{item.label}</span>
                            <span className="font-medium">
                              {item.count.toLocaleString()}
                            </span>
                          </div>
                          <MetricProgress
                            value={item.count}
                            max={promotedMax || 1}
                          />
                        </div>
                      ))
                  ) : (
                    <p className="text-sm text-muted-foreground">
                      No conspiracies promoted.
                    </p>
                  )}
                </div>
              </div>
            ) : claimFactors ? (
              <p className="text-sm text-muted-foreground">
                No conspiracy enrichment data available.
              </p>
            ) : (
              <p className="text-sm text-muted-foreground">
                Conspiracy enrichment results unavailable.
              </p>
            )}
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Top tropes</CardTitle>
            <CardDescription>
              Most common narrative tropes detected in claims
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {factorsLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : claimFactors && claimFactors.tropes.length > 0 ? (
              claimFactors.tropes.slice(0, 10).map((item) => (
                <div key={item.value} className="space-y-1">
                  <div className="flex items-center justify-between text-sm">
                    <span>{item.label}</span>
                    <span className="font-medium">
                      {item.count.toLocaleString()}
                    </span>
                  </div>
                  <MetricProgress value={item.count} max={tropeMax || 1} />
                </div>
              ))
            ) : (
              <p className="text-sm text-muted-foreground">
                No trope enrichment data available.
              </p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Persuasion techniques</CardTitle>
            <CardDescription>
              Detected persuasive techniques within claim language
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {factorsLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : claimFactors && claimFactors.persuasion_techniques.length > 0 ? (
              claimFactors.persuasion_techniques.slice(0, 10).map((item) => (
                <div key={item.value} className="space-y-1">
                  <div className="flex items-center justify-between text-sm">
                    <span>{item.label}</span>
                    <span className="font-medium">
                      {item.count.toLocaleString()}
                    </span>
                  </div>
                  <MetricProgress value={item.count} max={persuasionMax || 1} />
                </div>
              ))
            ) : (
              <p className="text-sm text-muted-foreground">
                No persuasion technique data available.
              </p>
            )}
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
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
