// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { renderHook, act } from "@testing-library/react-hooks";
import Long from "long";
import React from "react";
import { SWRConfig } from "swr";

import { ClusterDetailsContext, ClusterDetailsContextType } from "../contexts";

import {
  useCancelDiagnosticsReport,
  useCreateDiagnosticsReport,
  useStatementDiagnostics,
} from "./statementDiagnosticsApi";

const mockFetchData = jest.fn();

jest.mock("src/api", () => ({
  fetchData: (...args: unknown[]) => mockFetchData(...args),
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

// Build a protobuf-like report object that getStatementDiagnosticsReports
// can transform into StatementDiagnosticsReport.
function makeProtoReport(id: number, completed: boolean) {
  return {
    id: Long.fromNumber(id),
    statement_fingerprint: "SELECT 1",
    completed,
    statement_diagnostics_id: Long.fromNumber(completed ? 100 + id : 0),
    requested_at: { seconds: Long.fromNumber(1704067200) },
    min_execution_latency: null as null,
    expires_at: { seconds: Long.fromNumber(1704070800) },
  };
}

function makeGetResponse(reports: ReturnType<typeof makeProtoReport>[]) {
  return { reports };
}

describe("useStatementDiagnostics", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it("fetches diagnostics reports and transforms them", async () => {
    mockFetchData.mockResolvedValueOnce(
      makeGetResponse([makeProtoReport(1, true), makeProtoReport(2, false)]),
    );

    const { result, waitForNextUpdate } = renderHook(
      () => useStatementDiagnostics(),
      { wrapper },
    );

    expect(result.current.isLoading).toBe(true);

    await waitForNextUpdate();

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toHaveLength(2);
    expect(result.current.data[0].id).toBe("1");
    expect(result.current.data[0].completed).toBe(true);
    expect(result.current.data[1].id).toBe("2");
    expect(result.current.data[1].completed).toBe(false);
    expect(result.current.error).toBeUndefined();
  });

  it("returns empty array before data loads", () => {
    mockFetchData.mockReturnValue(new Promise(() => {})); // never resolves
    const { result } = renderHook(() => useStatementDiagnostics(), {
      wrapper,
    });

    expect(result.current.data).toEqual([]);
  });

  it("createReport calls API and revalidates", async () => {
    // Initial fetch.
    mockFetchData.mockResolvedValueOnce(
      makeGetResponse([makeProtoReport(1, true)]),
    );

    const { result, waitForNextUpdate } = renderHook(
      () => ({
        diagnostics: useStatementDiagnostics(),
        create: useCreateDiagnosticsReport(),
      }),
      { wrapper },
    );

    await waitForNextUpdate();
    expect(mockFetchData).toHaveBeenCalledTimes(1);

    // Mock the create response then the revalidation fetch.
    mockFetchData.mockResolvedValueOnce({ report: {} });
    mockFetchData.mockResolvedValueOnce(
      makeGetResponse([makeProtoReport(1, true), makeProtoReport(2, false)]),
    );

    await act(async () => {
      await result.current.create.createReport({
        stmtFingerprint: "SELECT 1",
        planGist: "gist",
        redacted: false,
      });
    });

    // 1 initial + 1 create + 1 revalidation = 3 calls.
    expect(mockFetchData).toHaveBeenCalledTimes(3);
  });

  it("cancelReport calls API and revalidates", async () => {
    mockFetchData.mockResolvedValueOnce(
      makeGetResponse([makeProtoReport(1, false)]),
    );

    const { result, waitForNextUpdate } = renderHook(
      () => ({
        diagnostics: useStatementDiagnostics(),
        cancel: useCancelDiagnosticsReport(),
      }),
      { wrapper },
    );

    await waitForNextUpdate();

    // Mock cancel response (no error) then revalidation.
    mockFetchData.mockResolvedValueOnce({ error: "" });
    mockFetchData.mockResolvedValueOnce(makeGetResponse([]));

    await act(async () => {
      await result.current.cancel.cancelReport({ requestId: "1" });
    });

    // 1 initial + 1 cancel + 1 revalidation = 3 calls.
    expect(mockFetchData).toHaveBeenCalledTimes(3);
  });
});
