"use client";

import { useEffect, useMemo, useState } from "react";

import { fetchJson } from "./api-client";

export type EnricherSuccessRate = {
  step: string;
  total_entries: number;
  successful: number;
  failed: number;
  success_rate_percent: number;
};

export type EnricherErrorBreakdown = {
  step: string;
  error_type: string | null;
  error_count: number;
};

export type EnricherDomainFailure = {
  step: string;
  domain: string;
  failure_count: number;
};

export type EnricherRecentActivity = {
  step: string;
  recent_entries: number;
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

export function useEnricherSuccess(
  params?: Record<string, string | number | undefined>
): UseAnalyticsResult<EnricherSuccessRate[]> {
  return useAnalyticsData<EnricherSuccessRate[]>("/metrics/enrichers/success-rate", params);
}

export function useEnricherErrors(
  params?: Record<string, string | number | undefined>
): UseAnalyticsResult<EnricherErrorBreakdown[]> {
  return useAnalyticsData<EnricherErrorBreakdown[]>("/metrics/enrichers/error-types", params);
}

export function useEnricherDomainFailures(
  params?: Record<string, string | number | undefined>
): UseAnalyticsResult<EnricherDomainFailure[]> {
  return useAnalyticsData<EnricherDomainFailure[]>("/metrics/enrichers/domain-failures", params);
}

export function useEnricherActivity(
  params?: Record<string, string | number | undefined>
): UseAnalyticsResult<EnricherRecentActivity[]> {
  return useAnalyticsData<EnricherRecentActivity[]>("/metrics/enrichers/recent-activity", params);
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
