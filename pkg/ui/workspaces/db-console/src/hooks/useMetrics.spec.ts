// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, waitFor, act } from "@testing-library/react";
import Long from "long";
import React from "react";
import { SWRConfig } from "swr";

import * as protos from "src/js/protos";

import { resetBatchState, requestBatched } from "./metricsBatchFetcher";
import { useMetrics, UseMetricsResult } from "./useMetrics";

type TSRequest = protos.cockroach.ts.tspb.TimeSeriesQueryRequest;

// Mock the batch fetcher so we can control responses without hitting
// the real API. The batch layer is tested separately in
// metricsBatchFetcher.spec.ts.
jest.mock("./metricsBatchFetcher", () => {
  const actual = jest.requireActual("./metricsBatchFetcher");
  return {
    ...actual,
    requestBatched: jest.fn(),
  };
});

const mockRequestBatched = requestBatched as jest.MockedFunction<
  typeof requestBatched
>;

function createRequest(
  start: number,
  end: number,
  ...names: string[]
): TSRequest {
  return new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
    start_nanos: Long.fromNumber(start),
    end_nanos: Long.fromNumber(end),
    queries: names.map(name => ({ name })),
  });
}

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

// TestConsumer is a component that uses the useMetrics hook and exposes
// its return value via a capture callback, allowing tests to assert on
// the hook's output without needing renderHook.
let capturedResult: UseMetricsResult | null = null;

// Helper to unwrap capturedResult with a Jest assertion instead of
// a non-null assertion operator (which is banned by the linter).
function getResult(): UseMetricsResult {
  expect(capturedResult).not.toBeNull();
  return capturedResult as UseMetricsResult;
}

function TestConsumer({ request }: { request: TSRequest | undefined }) {
  const result = useMetrics(request);
  capturedResult = result;
  return React.createElement("div", {
    "data-testid": "consumer",
    "data-loading": String(result.isLoading),
    "data-has-data": String(!!result.data),
    "data-has-error": String(!!result.error),
  });
}

// Wrap each test in a fresh SWR cache to prevent cross-test pollution.
function renderWithSWR(request: TSRequest | undefined) {
  return render(
    React.createElement(
      SWRConfig,
      { value: { provider: () => new Map() } },
      React.createElement(TestConsumer, { request }),
    ),
  );
}

describe("useMetrics", () => {
  beforeEach(() => {
    resetBatchState();
    mockRequestBatched.mockReset();
    capturedResult = null;
  });

  it("returns loading state initially then data after fetch", async () => {
    const req = createRequest(0, 100, "metric.a");
    const response = createResponse(req.queries);
    mockRequestBatched.mockResolvedValue(response);

    renderWithSWR(req);

    // Initially loading.
    expect(getResult().isLoading).toBe(true);
    expect(getResult().data).toBeUndefined();

    // Wait for fetch to complete.
    await waitFor(() => {
      expect(getResult().isLoading).toBe(false);
    });

    expect(getResult().data).toBeDefined();
    expect(getResult().data?.results).toHaveLength(1);
    expect(getResult().error).toBeUndefined();
  });

  it("returns error when fetch fails", async () => {
    const req = createRequest(0, 100, "metric.a");
    mockRequestBatched.mockRejectedValue(new Error("server error"));

    renderWithSWR(req);

    await waitFor(() => {
      expect(getResult().error).toBeDefined();
    });

    expect(getResult().error?.message).toBe("server error");
  });

  it("does not fetch when request is undefined", async () => {
    renderWithSWR(undefined);

    // Give it a tick to ensure nothing fires.
    await act(async () => {
      await new Promise(r => setTimeout(r, 10));
    });

    expect(mockRequestBatched).not.toHaveBeenCalled();
    expect(getResult().data).toBeUndefined();
    expect(getResult().isLoading).toBe(false);
  });

  it("refetches when request changes", async () => {
    const req1 = createRequest(0, 100, "metric.a");
    const req2 = createRequest(100, 200, "metric.a");
    const response1 = createResponse(req1.queries);
    const response2 = createResponse(req2.queries);

    mockRequestBatched
      .mockResolvedValueOnce(response1)
      .mockResolvedValueOnce(response2);

    const { rerender } = renderWithSWR(req1);

    await waitFor(() => {
      expect(getResult().data).toBeDefined();
    });
    expect(mockRequestBatched).toHaveBeenCalledTimes(1);

    // Change the request by rerendering with a new one.
    rerender(
      React.createElement(
        SWRConfig,
        { value: { provider: () => new Map() } },
        React.createElement(TestConsumer, { request: req2 }),
      ),
    );

    await waitFor(() => {
      expect(mockRequestBatched).toHaveBeenCalledTimes(2);
    });
  });

  it("keeps previous data while fetching new request", async () => {
    const req1 = createRequest(0, 100, "metric.a");
    const req2 = createRequest(100, 200, "metric.a");
    const response1 = createResponse(req1.queries);

    // First request resolves immediately; second hangs until we
    // resolve it manually.
    let resolveSecond: (
      v: protos.cockroach.ts.tspb.TimeSeriesQueryResponse,
    ) => void = () => {};
    const secondPromise =
      new Promise<protos.cockroach.ts.tspb.TimeSeriesQueryResponse>(r => {
        resolveSecond = r;
      });
    mockRequestBatched
      .mockResolvedValueOnce(response1)
      .mockReturnValueOnce(secondPromise);

    const { rerender } = renderWithSWR(req1);

    await waitFor(() => {
      expect(getResult().data).toBeDefined();
    });
    expect(getResult().data).toEqual(response1);

    // Switch to req2. The fetch is still in-flight, so
    // keepPreviousData should keep response1 visible.
    rerender(
      React.createElement(
        SWRConfig,
        { value: { provider: () => new Map() } },
        React.createElement(TestConsumer, { request: req2 }),
      ),
    );

    // Data should still be response1 (not undefined) while req2 loads.
    expect(getResult().data).toEqual(response1);

    // Resolve the second fetch and verify data updates.
    const response2 = createResponse(req2.queries);
    await act(async () => {
      resolveSecond(response2);
    });

    await waitFor(() => {
      expect(getResult().data).toEqual(response2);
    });
  });

  it("does not refetch when request is unchanged", async () => {
    const req = createRequest(0, 100, "metric.a");
    const response = createResponse(req.queries);
    mockRequestBatched.mockResolvedValue(response);

    const { rerender } = renderWithSWR(req);

    await waitFor(() => {
      expect(getResult().data).toBeDefined();
    });
    expect(mockRequestBatched).toHaveBeenCalledTimes(1);

    // Rerender with the same request object.
    rerender(
      React.createElement(
        SWRConfig,
        { value: { provider: () => new Map() } },
        React.createElement(TestConsumer, { request: req }),
      ),
    );

    // Still only one call — SWR sees the same key.
    expect(mockRequestBatched).toHaveBeenCalledTimes(1);
  });
});
