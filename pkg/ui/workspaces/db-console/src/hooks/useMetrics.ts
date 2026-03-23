// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/**
 * useMetrics is a React hook that fetches time series data from the
 * CockroachDB metrics API.
 *
 * Key features:
 * - Request batching: multiple useMetrics calls in the same render
 *   cycle are coalesced into a single API call via metricsBatchFetcher.
 * - Caching: SWR caches responses by request identity, avoiding
 *   redundant fetches when the request hasn't changed.
 * - Stale-while-revalidate: previous data is shown while new data
 *   loads, so graphs don't flicker during time window advances.
 *
 * Polling is NOT handled by this hook. The existing MetricsTimeManager
 * advances the time window in Redux, which changes the request passed
 * to useMetrics, which triggers SWR to fetch fresh data. This preserves
 * the current behavior: live/moving windows poll via MetricsTimeManager,
 * fixed historical windows don't poll.
 */

import { useRef } from "react";
import useSWRImmutable from "swr/immutable";

import * as protos from "src/js/protos";

import { requestBatched } from "./metricsBatchFetcher";

type TSRequest = protos.cockroach.ts.tspb.TimeSeriesQueryRequest;
type TSResponse = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;

export interface UseMetricsResult {
  /** The most recently fetched response, or undefined if not yet loaded. */
  data: TSResponse | undefined;
  /** The error from the most recent fetch attempt, or undefined. */
  error: Error | undefined;
  /** True while the initial fetch is in progress (no cached data yet). */
  isLoading: boolean;
  /** True while any fetch is in progress (including revalidation). */
  isValidating: boolean;
}

/**
 * serializeRequest produces a stable array key for a TSRequest so
 * SWR can determine whether the request has changed. Two requests
 * with the same queries and timespan produce the same key.
 *
 * The key encodes: start_nanos, end_nanos, sample_nanos, and each
 * query's name + sources + tenant_id + aggregator settings.
 */
function serializeRequest(req: TSRequest) {
  return [
    req.start_nanos?.toString() ?? "",
    req.end_nanos?.toString() ?? "",
    req.sample_nanos?.toString() ?? "",
    req.queries.map(q => [
      q.name ?? "",
      q.sources ?? [],
      q.tenant_id?.id?.toString() ?? "",
      q.downsampler ?? 0,
      q.source_aggregator ?? 0,
      q.derivative ?? 0,
    ]),
  ];
}

/**
 * useMetrics fetches time series data for the given request. Pass
 * undefined to skip fetching (e.g. when time info isn't available).
 *
 * Multiple components calling useMetrics in the same render cycle
 * have their requests batched into a single server call.
 */
export function useMetrics(request: TSRequest | undefined): UseMetricsResult {
  // SWR key: null means "don't fetch". Otherwise a stable array
  // derived from the request so SWR can detect changes.
  const key = request ? serializeRequest(request) : null;

  // Keep a ref to the latest request so the fetcher always uses the
  // current value. SWR may cache the fetcher closure from a previous
  // render (e.g. during deduped revalidation), which would otherwise
  // close over a stale `request`.
  const requestRef = useRef(request);
  requestRef.current = request;

  const { data, error, isLoading, isValidating } = useSWRImmutable<
    TSResponse,
    Error
  >(
    key,
    // requestRef.current is guaranteed non-null here because `key` is
    // null when request is undefined, and SWR skips the fetcher for
    // null keys.
    () => requestBatched(requestRef.current as TSRequest),
    {
      // Disable SWR's built-in deduplication. The batch fetcher
      // already coalesces concurrent requests within a microtask, and
      // a non-zero dedupingInterval would cause SWR to serve stale
      // cached data during rapid time window changes (e.g. dragging
      // the time range picker) instead of fetching for the new window.
      dedupingInterval: 0,
      // When the SWR key changes (i.e. the time window moves), keep
      // showing the previous response until the new one arrives. This
      // prevents graphs from flickering to empty during every time
      // window advance — matching the old Redux behavior where stale
      // data stayed visible until a fresh response landed.
      keepPreviousData: true,
      // Cap retries on error. The old saga never retried; SWR defaults
      // to infinite exponential backoff. Two retries balances self-healing
      // from transient blips against hammering a genuinely down server.
      errorRetryCount: 2,
    },
  );

  return { data, error, isLoading, isValidating };
}
