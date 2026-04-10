// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { renderHook, waitFor } from "@testing-library/react";
import moment from "moment-timezone";
import React from "react";
import { SWRConfig } from "swr";

import { ClusterDetailsContext, ClusterDetailsContextType } from "../contexts";

import {
  useCombinedStatementStats,
  useStatementDetails,
  useCombinedTransactionStats,
  SqlStatsSortOptions,
} from "./statementsApi";

const mockGetCombinedStatements = jest.fn();

jest.mock("src/api/fetchData", () => ({
  fetchData: (...args: unknown[]) => mockGetCombinedStatements(...args),
}));

const clusterContext: ClusterDetailsContextType = {
  isTenant: false,
  clusterId: "test-cluster",
};

const wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <SWRConfig
    value={{
      provider: () => new Map(),
      dedupingInterval: 0,
      shouldRetryOnError: false,
    }}
  >
    <ClusterDetailsContext.Provider value={clusterContext}>
      {children}
    </ClusterDetailsContext.Provider>
  </SWRConfig>
);

const mockStatementsResponse = {
  statements: [
    {
      key: { key_data: { query: "SELECT 1" } },
      stats: { count: 5 },
    },
  ],
  stmts_total_runtime_secs: 10.5,
  oldest_aggregated_ts_returned: { seconds: 1000 },
};

describe("useCombinedStatementStats", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it("fetches statements and returns data", async () => {
    mockGetCombinedStatements.mockResolvedValueOnce(mockStatementsResponse);

    const timeScale = {
      windowSize: moment.duration(1, "hour"),
      sampleSize: moment.duration(30, "seconds"),
      fixedWindowEnd: moment.utc("2024-01-01 14:00"),
      key: "Past 1 Hour",
    };

    const { result } = renderHook(
      () =>
        useCombinedStatementStats(
          timeScale,
          100,
          SqlStatsSortOptions.PCT_RUNTIME,
        ),
      { wrapper },
    );

    expect(result.current.isLoading).toBe(true);

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
      expect(result.current.data).toBeDefined();
      expect(result.current.error).toBeUndefined();
      expect(mockGetCombinedStatements).toHaveBeenCalledTimes(1);
    });
  });

  it("skips fetch when timeScale is null", async () => {
    const { result } = renderHook(
      () =>
        useCombinedStatementStats(null, 100, SqlStatsSortOptions.PCT_RUNTIME),
      { wrapper },
    );

    // With a null key, SWR should not fetch.
    expect(result.current.data).toBeUndefined();
    expect(result.current.isLoading).toBe(false);
    expect(mockGetCombinedStatements).not.toHaveBeenCalled();
  });

  it("returns error when fetch fails", async () => {
    mockGetCombinedStatements.mockRejectedValue(new Error("fetch failed"));

    const timeScale = {
      windowSize: moment.duration(1, "hour"),
      sampleSize: moment.duration(30, "seconds"),
      fixedWindowEnd: moment.utc("2024-01-01 14:00"),
      key: "Past 1 Hour",
    };

    const { result } = renderHook(
      () =>
        useCombinedStatementStats(
          timeScale,
          100,
          SqlStatsSortOptions.PCT_RUNTIME,
        ),
      { wrapper },
    );

    await waitFor(() => {
      expect(result.current.error).toBeDefined();
    });
  });
});

const mockStatementDetailsResponse = {
  statement: {
    metadata: { query: "SELECT 1" },
    stats: { count: 1 },
  },
};

describe("useStatementDetails", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it("fetches statement details and returns data", async () => {
    mockGetCombinedStatements.mockResolvedValueOnce(
      mockStatementDetailsResponse,
    );

    const timeScale = {
      windowSize: moment.duration(1, "hour"),
      sampleSize: moment.duration(30, "seconds"),
      fixedWindowEnd: moment.utc("2024-01-01 14:00"),
      key: "Past 1 Hour",
    };

    const { result } = renderHook(
      () => useStatementDetails("abc123", "app1,app2", timeScale),
      { wrapper },
    );

    expect(result.current.isLoading).toBe(true);

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
      expect(result.current.data).toBeDefined();
      expect(result.current.error).toBeUndefined();
      expect(mockGetCombinedStatements).toHaveBeenCalledTimes(1);
    });
  });

  it("skips fetch when timeScale is null", () => {
    const { result } = renderHook(
      () => useStatementDetails("abc123", undefined, null),
      { wrapper },
    );

    expect(result.current.data).toBeUndefined();
    expect(result.current.isLoading).toBe(false);
    expect(mockGetCombinedStatements).not.toHaveBeenCalled();
  });

  it("skips fetch when fingerprintId is empty", () => {
    const timeScale = {
      windowSize: moment.duration(1, "hour"),
      sampleSize: moment.duration(30, "seconds"),
      fixedWindowEnd: moment.utc("2024-01-01 14:00"),
      key: "Past 1 Hour",
    };

    const { result } = renderHook(
      () => useStatementDetails("", "app1", timeScale),
      { wrapper },
    );

    expect(result.current.data).toBeUndefined();
    expect(result.current.isLoading).toBe(false);
    expect(mockGetCombinedStatements).not.toHaveBeenCalled();
  });

  it("returns error when fetch fails", async () => {
    mockGetCombinedStatements.mockRejectedValue(new Error("fetch failed"));

    const timeScale = {
      windowSize: moment.duration(1, "hour"),
      sampleSize: moment.duration(30, "seconds"),
      fixedWindowEnd: moment.utc("2024-01-01 14:00"),
      key: "Past 1 Hour",
    };

    const { result } = renderHook(
      () => useStatementDetails("abc123", undefined, timeScale),
      { wrapper },
    );

    await waitFor(() => {
      expect(result.current.error).toBeDefined();
    });
  });
});

describe("useCombinedTransactionStats", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it("fetches transactions and returns data", async () => {
    const mockTxnResponse = {
      transactions: [{ stats_data: { transaction_fingerprint_id: "1" } }],
      txns_source_table: "crdb_internal.transaction_statistics",
    };
    mockGetCombinedStatements.mockResolvedValueOnce(mockTxnResponse);

    const timeScale = {
      windowSize: moment.duration(1, "hour"),
      sampleSize: moment.duration(30, "seconds"),
      fixedWindowEnd: moment.utc("2024-01-01 14:00"),
      key: "Past 1 Hour",
    };

    const { result } = renderHook(
      () =>
        useCombinedTransactionStats(
          timeScale,
          100,
          SqlStatsSortOptions.SERVICE_LAT,
        ),
      { wrapper },
    );

    expect(result.current.isLoading).toBe(true);

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
      expect(result.current.data).toBeDefined();
      expect(result.current.error).toBeUndefined();
      expect(mockGetCombinedStatements).toHaveBeenCalledTimes(1);
    });
  });

  it("skips fetch when timeScale is null", async () => {
    const { result } = renderHook(
      () =>
        useCombinedTransactionStats(null, 100, SqlStatsSortOptions.SERVICE_LAT),
      { wrapper },
    );

    expect(result.current.data).toBeUndefined();
    expect(result.current.isLoading).toBe(false);
    expect(mockGetCombinedStatements).not.toHaveBeenCalled();
  });

  it("returns error when fetch fails", async () => {
    mockGetCombinedStatements.mockRejectedValue(new Error("fetch failed"));

    const timeScale = {
      windowSize: moment.duration(1, "hour"),
      sampleSize: moment.duration(30, "seconds"),
      fixedWindowEnd: moment.utc("2024-01-01 14:00"),
      key: "Past 1 Hour",
    };

    const { result } = renderHook(
      () =>
        useCombinedTransactionStats(
          timeScale,
          100,
          SqlStatsSortOptions.SERVICE_LAT,
        ),
      { wrapper },
    );

    await waitFor(() => {
      expect(result.current.error).toBeDefined();
    });
  });
});
