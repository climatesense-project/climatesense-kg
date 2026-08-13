"use client";

import { useEffect, useMemo, useState } from "react";

import { fetchJson } from "./api-client";

export type StageSuccessRate = {
  stage_name: string;
  stage_version: string;
  total_results: number;
  successful: number;
  failed: number;
  success_rate_percent: number;
};

export type StageErrorBreakdown = {
  stage_name: string;
  stage_version: string;
  status: string;
  error_type: string | null;
  failure_category: string | null;
  http_status: number | null;
  error_count: number;
};

export type StageDomainFailure = {
  stage_name: string;
  stage_version: string;
  status: string;
  domain: string;
  failure_count: number;
};

export type StageRecentActivity = {
  stage_name: string;
  stage_version: string;
  recent_results: number;
  earliest: string | null;
  latest: string | null;
  successful: number;
  failed: number;
};

export type GraphTripleCount = {
  graph: string | null;
  triple_count: number;
};

export type ClassDistribution = {
  class_uri: string | null;
  count: number;
};

export type CoreCounts = {
  total_claim_reviews: number;
  total_claims: number;
  total_ratings: number;
};

export type EnrichmentCoverage = {
  total_claims: number;
  claims_with_emotion: number;
  claims_with_sentiment: number;
  claims_with_political_leaning: number;
  claims_with_conspiracy: number;
  claims_with_tropes: number;
  claims_with_persuasion_techniques: number;
  claims_with_climate_relatedness: number;
};

export type EntityTypeCount = {
  type_uri: string | null;
  count: number;
};

export type FactorDistributionItem = {
  value: string;
  label: string;
  count: number;
};

export type ClaimFactorDistributions = {
  sentiment: FactorDistributionItem[];
  political_leaning: FactorDistributionItem[];
  climate_related: FactorDistributionItem[];
  emotion: FactorDistributionItem[];
  tropes: FactorDistributionItem[];
  persuasion_techniques: FactorDistributionItem[];
  conspiracies_mentioned: FactorDistributionItem[];
  conspiracies_promoted: FactorDistributionItem[];
};

export type UseAnalyticsResult<T> = {
  data: T | null;
  error: Error | null;
  loading: boolean;
};

function buildEndpoint(
  endpoint: string,
  params?: Record<string, string | number | undefined>
): string {
  if (!params) {
    return endpoint;
  }
  const searchParams = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }
    searchParams.append(key, String(value));
  });
  const query = searchParams.toString();
  return query ? `${endpoint}?${query}` : endpoint;
}

function useAnalyticsData<T>(
  endpoint: string,
  params?: Record<string, string | number | undefined>
): UseAnalyticsResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState<boolean>(true);

  const resolvedEndpoint = useMemo(
    () => buildEndpoint(endpoint, params),
    [endpoint, params]
  );

  useEffect(() => {
    let isMounted = true;
    setLoading(true);
    fetchJson<T>(resolvedEndpoint)
      .then((payload) => {
        if (!isMounted) return;
        setData(payload);
        setError(null);
      })
      .catch((err: Error) => {
        if (!isMounted) return;
        setError(err);
        setData(null);
      })
      .finally(() => {
        if (!isMounted) return;
        setLoading(false);
      });

    return () => {
      isMounted = false;
    };
  }, [resolvedEndpoint]);

  return { data, error, loading };
}

export function useStageSuccess(
  params?: Record<string, string | number | undefined>
): UseAnalyticsResult<StageSuccessRate[]> {
  return useAnalyticsData<StageSuccessRate[]>("/metrics/stages/success-rate", params);
}

export function useStageErrors(
  params?: Record<string, string | number | undefined>
): UseAnalyticsResult<StageErrorBreakdown[]> {
  return useAnalyticsData<StageErrorBreakdown[]>("/metrics/stages/error-types", params);
}

export function useStageDomainFailures(
  params?: Record<string, string | number | undefined>
): UseAnalyticsResult<StageDomainFailure[]> {
  return useAnalyticsData<StageDomainFailure[]>("/metrics/stages/domain-failures", params);
}

export function useStageActivity(
  params?: Record<string, string | number | undefined>
): UseAnalyticsResult<StageRecentActivity[]> {
  return useAnalyticsData<StageRecentActivity[]>("/metrics/stages/recent-activity", params);
}

export function useKgTripleStats(): UseAnalyticsResult<GraphTripleCount[]> {
  return useAnalyticsData<GraphTripleCount[]>("/metrics/kg/triple-volume");
}

export function useKgClassDistribution(): UseAnalyticsResult<ClassDistribution[]> {
  return useAnalyticsData<ClassDistribution[]>("/metrics/kg/class-distribution");
}

export function useKgCoreCounts(): UseAnalyticsResult<CoreCounts> {
  return useAnalyticsData<CoreCounts>("/metrics/kg/core-counts");
}

export function useKgEnrichmentCoverage(): UseAnalyticsResult<EnrichmentCoverage> {
  return useAnalyticsData<EnrichmentCoverage>("/metrics/kg/enrichment-coverage");
}

export function useKgEntityTypes(): UseAnalyticsResult<EntityTypeCount[]> {
  return useAnalyticsData<EntityTypeCount[]>("/metrics/kg/entity-types");
}

export function useKgClaimFactors(): UseAnalyticsResult<ClaimFactorDistributions> {
  return useAnalyticsData<ClaimFactorDistributions>("/metrics/kg/claim-factors");
}
