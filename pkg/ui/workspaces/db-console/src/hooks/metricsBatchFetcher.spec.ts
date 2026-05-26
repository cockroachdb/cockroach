// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import map from "lodash/map";
import Long from "long";

import * as protos from "src/js/protos";
import { queryTimeSeries } from "src/util/api";

import { requestBatched, resetBatchState } from "./metricsBatchFetcher";

type TSRequest = protos.cockroach.ts.tspb.TimeSeriesQueryRequest;

jest.mock("src/util/api", () => ({
  queryTimeSeries: jest.fn(),
}));
const mockQueryTimeSeries = queryTimeSeries as jest.MockedFunction<
  typeof queryTimeSeries
>;

// Helper to create a TSRequest with the given timespan and metric names.
function createRequest(
  start: number,
  end: number,
  ...names: string[]
): TSRequest {
  return createRequestWithSample(start, end, undefined, ...names);
}

function createRequestWithSample(
  start: number,
  end: number,
  sampleNanos: number | undefined,
  ...names: string[]
): TSRequest {
  return new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
    start_nanos: Long.fromNumber(start),
    end_nanos: Long.fromNumber(end),
    sample_nanos: sampleNanos ? Long.fromNumber(sampleNanos) : undefined,
    queries: map(names, name => ({ name })),
  });
}

// Helper to create a response with one result per query.
function createResponse(
  queries: protos.cockroach.ts.tspb.IQuery[],
): protos.cockroach.ts.tspb.TimeSeriesQueryResponse {
  return new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
    results: queries.map(q => ({
      query: q,
      datapoints: [],
    })),
  });
}

describe("metricsBatchFetcher", () => {
  beforeEach(() => {
    resetBatchState();
    mockQueryTimeSeries.mockReset();
  });

  it("sends a single request when only one is queued", async () => {
    const req = createRequest(0, 100, "metric.a");
    const expectedResponse = createResponse(req.queries);
    mockQueryTimeSeries.mockResolvedValue(expectedResponse);

    const result = await requestBatched(req);

    expect(mockQueryTimeSeries).toHaveBeenCalledTimes(1);
    expect(result.results).toHaveLength(1);
    expect(result.results[0].query.name).toBe("metric.a");
  });

  it("batches requests with the same timespan into one API call", async () => {
    const req1 = createRequest(0, 100, "metric.a");
    const req2 = createRequest(0, 100, "metric.b", "metric.c");
    const req3 = createRequest(0, 100, "metric.d");

    // The batch layer will merge these into a single request with 4
    // queries. Mock the unified response.
    mockQueryTimeSeries.mockImplementation(async unified => {
      return createResponse(unified.queries);
    });

    // Fire all three in the same microtask.
    const [r1, r2, r3] = await Promise.all([
      requestBatched(req1),
      requestBatched(req2),
      requestBatched(req3),
    ]);

    // Only one API call should have been made.
    expect(mockQueryTimeSeries).toHaveBeenCalledTimes(1);
    const sentRequest = mockQueryTimeSeries.mock.calls[0][0];
    expect(sentRequest.queries).toHaveLength(4);

    // Each caller gets back only its own results.
    expect(r1.results).toHaveLength(1);
    expect(r1.results[0].query.name).toBe("metric.a");
    expect(r2.results).toHaveLength(2);
    expect(r2.results[0].query.name).toBe("metric.b");
    expect(r2.results[1].query.name).toBe("metric.c");
    expect(r3.results).toHaveLength(1);
    expect(r3.results[0].query.name).toBe("metric.d");
  });

  it("groups requests by timespan into separate API calls", async () => {
    const shortReq = createRequest(400, 500, "short.1");
    const longReq = createRequest(0, 500, "long.1");

    mockQueryTimeSeries.mockImplementation(async unified => {
      return createResponse(unified.queries);
    });

    const [shortResult, longResult] = await Promise.all([
      requestBatched(shortReq),
      requestBatched(longReq),
    ]);

    // Two different timespans = two API calls.
    expect(mockQueryTimeSeries).toHaveBeenCalledTimes(2);

    expect(shortResult.results).toHaveLength(1);
    expect(shortResult.results[0].query.name).toBe("short.1");
    expect(longResult.results).toHaveLength(1);
    expect(longResult.results[0].query.name).toBe("long.1");
  });

  it("groups requests by sample_nanos into separate API calls", async () => {
    const req10s = createRequestWithSample(0, 100, 10, "metric.a");
    const req30m = createRequestWithSample(0, 100, 1800, "metric.b");

    mockQueryTimeSeries.mockImplementation(async unified => {
      return createResponse(unified.queries);
    });

    const [r10s, r30m] = await Promise.all([
      requestBatched(req10s),
      requestBatched(req30m),
    ]);

    // Same timespan but different sample_nanos = two API calls.
    expect(mockQueryTimeSeries).toHaveBeenCalledTimes(2);

    expect(r10s.results).toHaveLength(1);
    expect(r10s.results[0].query.name).toBe("metric.a");
    expect(r30m.results).toHaveLength(1);
    expect(r30m.results[0].query.name).toBe("metric.b");
  });

  it("propagates errors to all callers in the failed batch", async () => {
    const req1 = createRequest(0, 100, "metric.a");
    const req2 = createRequest(0, 100, "metric.b");
    const networkError = new Error("network error");

    mockQueryTimeSeries.mockRejectedValue(networkError);

    const results = await Promise.allSettled([
      requestBatched(req1),
      requestBatched(req2),
    ]);

    expect(results[0].status).toBe("rejected");
    expect(results[1].status).toBe("rejected");
    expect((results[0] as PromiseRejectedResult).reason.message).toBe(
      "network error",
    );
    expect((results[1] as PromiseRejectedResult).reason.message).toBe(
      "network error",
    );
  });

  it("errors in one timespan group don't affect other groups", async () => {
    const goodReq = createRequest(0, 100, "good.metric");
    const badReq = createRequest(200, 300, "bad.metric");

    mockQueryTimeSeries.mockImplementation(async unified => {
      // Fail the second timespan group.
      const startNanos = unified.start_nanos as Long;
      if (startNanos.toNumber() === 200) {
        throw new Error("server error");
      }
      return createResponse(unified.queries);
    });

    const results = await Promise.allSettled([
      requestBatched(goodReq),
      requestBatched(badReq),
    ]);

    expect(results[0].status).toBe("fulfilled");
    expect(results[1].status).toBe("rejected");
    const fulfilled =
      results[0] as PromiseFulfilledResult<protos.cockroach.ts.tspb.TimeSeriesQueryResponse>;
    expect(fulfilled.value.results[0].query.name).toBe("good.metric");
  });

  it("rejects if response result count mismatches query count", async () => {
    const req = createRequest(0, 100, "metric.a", "metric.b");

    // Return only one result for two queries.
    mockQueryTimeSeries.mockResolvedValue(
      new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
        results: [{ query: { name: "metric.a" }, datapoints: [] }],
      }),
    );

    await expect(requestBatched(req)).rejects.toThrow(
      "mismatched count of results",
    );
  });

  it("separate microtasks result in separate batches", async () => {
    mockQueryTimeSeries.mockImplementation(async unified => {
      return createResponse(unified.queries);
    });

    // First batch: queue and flush.
    const r1 = await requestBatched(createRequest(0, 100, "batch1"));

    // Second batch: separate microtask.
    const r2 = await requestBatched(createRequest(0, 100, "batch2"));

    // Each batch triggers its own API call.
    expect(mockQueryTimeSeries).toHaveBeenCalledTimes(2);
    expect(r1.results[0].query.name).toBe("batch1");
    expect(r2.results[0].query.name).toBe("batch2");
  });
});
