// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/**
 * metricsBatchFetcher coalesces concurrent time series requests into
 * batched API calls.
 *
 * When multiple graphs render in the same event loop tick, each calls
 * requestBatched() which queues the request. After the current microtask
 * completes, the queue is flushed: requests are grouped by timespan
 * (start_nanos + end_nanos), each group is merged into a single API
 * call, and the results are split back to the original callers.
 *
 * This avoids N separate server round-trips for N graphs on a page.
 */

import { util } from "@cockroachlabs/cluster-ui";
import clone from "lodash/clone";
import flatMap from "lodash/flatMap";
import groupBy from "lodash/groupBy";

import * as protos from "src/js/protos";
import { queryTimeSeries } from "src/util/api";

type TSRequest = protos.cockroach.ts.tspb.TimeSeriesQueryRequest;
type TSResponse = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;

interface PendingRequest {
  request: TSRequest;
  resolve: (response: TSResponse) => void;
  reject: (error: Error) => void;
}

// Module-level queue. All calls to requestBatched() within a single
// microtask accumulate here before being flushed together.
let pendingRequests: PendingRequest[] = [];
let flushScheduled = false;

/**
 * timespanKey produces a grouping key for requests that share the same
 * time window. Requests with identical timespans can be merged into a
 * single server call (the server handles multiple queries in one
 * request as long as the timespan matches).
 */
function timespanKey(req: TSRequest): string {
  return [
    req.start_nanos?.toString(),
    req.end_nanos?.toString(),
    req.sample_nanos?.toString(),
    req.return_sources_separately ? "ps" : "",
  ].join(":");
}

/**
 * flush drains the pending queue, groups requests by timespan, and
 * sends each group as a single API call. Results are split back to
 * the original callers based on how many queries each submitted.
 */
async function flush(): Promise<void> {
  const batch = pendingRequests;
  pendingRequests = [];
  flushScheduled = false;

  const groups = groupBy(batch, entry => timespanKey(entry.request));

  // Process all timespan groups concurrently (like the saga's yield all).
  const groupPromises = Object.values(groups).map(async group => {
    // Merge all queries in the group into a single request.
    const unifiedRequest = clone(group[0].request);
    unifiedRequest.queries = flatMap(group, entry => entry.request.queries);

    try {
      const response = await queryTimeSeries(unifiedRequest);
      if (response.results.length !== unifiedRequest.queries.length) {
        throw new Error(
          `mismatched count of results (${response.results.length}) and queries (${unifiedRequest.queries.length})`,
        );
      }

      // Split the unified response back to individual callers. The
      // response.results array is ordered to match the unified query
      // array, so we can slice by each caller's query count.
      let offset = 0;
      for (const entry of group) {
        const count = entry.request.queries.length;
        entry.resolve(
          new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
            results: response.results.slice(offset, offset + count),
          }),
        );
        offset += count;
      }
    } catch (e) {
      const err = util.maybeError(e);
      for (const entry of group) {
        entry.reject(err);
      }
    }
  });

  await Promise.all(groupPromises);
}

/**
 * requestBatched queues a time series request to be sent in the next
 * batch flush. Returns a promise that resolves with the response for
 * this specific request (i.e. only the results matching the queries
 * in the provided request, not the full batch).
 *
 * Multiple calls within the same microtask are batched together.
 */
export function requestBatched(request: TSRequest): Promise<TSResponse> {
  return new Promise<TSResponse>((resolve, reject) => {
    pendingRequests.push({ request, resolve, reject });

    if (!flushScheduled) {
      flushScheduled = true;
      // setTimeout(0) defers to the next macrotask, matching the old
      // saga's delay(0). This is critical because SWR v2 calls
      // fetchers from useLayoutEffect callbacks that fire in separate
      // microtask windows (each SWR hook's setCache triggers React
      // state updates between hooks). queueMicrotask fires too early,
      // flushing after only a subset of hooks have queued their
      // requests. setTimeout(0) waits for the full render + effects
      // cycle to complete, so all MetricsDataProviders accumulate
      // their requests before a single batch is sent.
      setTimeout(() => {
        flush();
      }, 0);
    }
  });
}

/**
 * resetBatchState is exposed for testing only. It clears the internal
 * queue so tests start from a clean state.
 */
export function resetBatchState(): void {
  pendingRequests = [];
  flushScheduled = false;
}
