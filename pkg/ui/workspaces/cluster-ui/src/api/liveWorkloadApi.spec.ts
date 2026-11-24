// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { renderHook } from "@testing-library/react-hooks";
import React from "react";

import { ClusterDetailsContext } from "../contexts";
import * as hooks from "../util/hooks";

import { useLiveWorkload } from "./liveWorkloadApi";

describe("useLiveWorkload", () => {
  let spyUseSwrWithClusterId: jest.SpyInstance;

  beforeEach(() => {
    spyUseSwrWithClusterId = jest.spyOn(hooks, "useSwrWithClusterId");
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const mockSessionsResponse = {
    sessions: [
      {
        id: new Uint8Array([1]),
        node_id: 1,
        username: "root",
        client_address: "127.0.0.1",
        application_name: "my-app",
        active_queries: [
          {
            id: "stmt-1",
            txn_id: new Uint8Array([2]),
            sql: "SELECT 1",
            phase: 1, // Executing
            start: { seconds: { low: 1639267200, high: 0 } },
            sql_no_constants: "SELECT _",
            is_full_scan: false,
          },
        ],
        active_txn: {
          id: new Uint8Array([2]),
          start: { seconds: { low: 1639267200, high: 0 } },
          num_statements_executed: 1,
          num_retries: 0,
          num_auto_retries: 0,
          priority: "normal",
          isolation_level: "SERIALIZABLE",
        },
        status: 1, // Active
      },
    ],
    errors: [] as unknown[],
    internal_app_name_prefix: "$ internal",
  };

  const mockClusterLocksResponse = {
    results: [] as unknown[],
    maxSizeReached: false,
  };

  it("returns combined active executions from sessions and locks", () => {
    spyUseSwrWithClusterId
      .mockReturnValueOnce({
        data: mockSessionsResponse,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      })
      .mockReturnValueOnce({
        data: mockClusterLocksResponse,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      });

    const { result } = renderHook(() => useLiveWorkload());

    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
    expect(result.current.data.internalAppNamePrefix).toBe("$ internal");
    expect(result.current.data.maxSizeApiReached).toBe(false);
    expect(result.current.data.clusterLocks).toEqual([]);
  });

  it("returns loading state when sessions are loading", () => {
    spyUseSwrWithClusterId
      .mockReturnValueOnce({
        data: undefined,
        error: undefined,
        isLoading: true,
        mutate: jest.fn(),
      })
      .mockReturnValueOnce({
        data: undefined,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      });

    const { result } = renderHook(() => useLiveWorkload());

    expect(result.current.isLoading).toBe(true);
    expect(result.current.data.statements).toEqual([]);
    expect(result.current.data.transactions).toEqual([]);
  });

  it("does not report loading when only cluster locks are loading", () => {
    spyUseSwrWithClusterId
      .mockReturnValueOnce({
        data: mockSessionsResponse,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      })
      .mockReturnValueOnce({
        data: undefined,
        error: undefined,
        isLoading: true,
        mutate: jest.fn(),
      });

    const { result } = renderHook(() => useLiveWorkload());

    expect(result.current.isLoading).toBe(false);
  });

  it("returns sessions error when present", () => {
    const sessionsError = new Error("sessions failed");
    spyUseSwrWithClusterId
      .mockReturnValueOnce({
        data: undefined,
        error: sessionsError,
        isLoading: false,
        mutate: jest.fn(),
      })
      .mockReturnValueOnce({
        data: undefined,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      });

    const { result } = renderHook(() => useLiveWorkload());

    expect(result.current.error).toBe(sessionsError);
  });

  it("returns cluster locks error when sessions succeeds", () => {
    const locksError = new Error("locks failed");
    spyUseSwrWithClusterId
      .mockReturnValueOnce({
        data: mockSessionsResponse,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      })
      .mockReturnValueOnce({
        data: undefined,
        error: locksError,
        isLoading: false,
        mutate: jest.fn(),
      });

    const { result } = renderHook(() => useLiveWorkload());

    expect(result.current.error).toBe(locksError);
  });

  it("prefers sessions error over locks error", () => {
    const sessionsError = new Error("sessions failed");
    const locksError = new Error("locks failed");
    spyUseSwrWithClusterId
      .mockReturnValueOnce({
        data: undefined,
        error: sessionsError,
        isLoading: false,
        mutate: jest.fn(),
      })
      .mockReturnValueOnce({
        data: undefined,
        error: locksError,
        isLoading: false,
        mutate: jest.fn(),
      });

    const { result } = renderHook(() => useLiveWorkload());

    expect(result.current.error).toBe(sessionsError);
  });

  it("reports maxSizeApiReached when cluster locks hit max", () => {
    spyUseSwrWithClusterId
      .mockReturnValueOnce({
        data: mockSessionsResponse,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      })
      .mockReturnValueOnce({
        data: { results: [], maxSizeReached: true },
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      });

    const { result } = renderHook(() => useLiveWorkload());

    expect(result.current.data.maxSizeApiReached).toBe(true);
  });

  it("calls both mutate functions on refresh", () => {
    const mutateSessions = jest.fn();
    const mutateLocks = jest.fn();
    spyUseSwrWithClusterId
      .mockReturnValueOnce({
        data: undefined,
        error: undefined,
        isLoading: false,
        mutate: mutateSessions,
      })
      .mockReturnValueOnce({
        data: undefined,
        error: undefined,
        isLoading: false,
        mutate: mutateLocks,
      });

    const { result } = renderHook(() => useLiveWorkload());

    result.current.refresh();

    expect(mutateSessions).toHaveBeenCalled();
    expect(mutateLocks).toHaveBeenCalled();
  });

  it("passes refreshInterval to SWR config", () => {
    spyUseSwrWithClusterId
      .mockReturnValueOnce({
        data: undefined,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      })
      .mockReturnValueOnce({
        data: undefined,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      });

    renderHook(() => useLiveWorkload({ refreshInterval: 10_000 }));

    // Verify both SWR calls received the refreshInterval.
    expect(spyUseSwrWithClusterId).toHaveBeenCalledTimes(2);
    expect(spyUseSwrWithClusterId.mock.calls[0][2]).toMatchObject({
      refreshInterval: 10_000,
    });
    expect(spyUseSwrWithClusterId.mock.calls[1][2]).toMatchObject({
      refreshInterval: 10_000,
    });
  });

  it("disables revalidation when immutable is true", () => {
    spyUseSwrWithClusterId
      .mockReturnValueOnce({
        data: undefined,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      })
      .mockReturnValueOnce({
        data: undefined,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      });

    renderHook(() => useLiveWorkload({ immutable: true }));

    expect(spyUseSwrWithClusterId).toHaveBeenCalledTimes(2);
    const expectedImmutableConfig = {
      revalidateIfStale: false,
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
    };
    expect(spyUseSwrWithClusterId.mock.calls[0][2]).toMatchObject(
      expectedImmutableConfig,
    );
    expect(spyUseSwrWithClusterId.mock.calls[1][2]).toMatchObject(
      expectedImmutableConfig,
    );
  });

  it("skips cluster locks fetch when isTenant is true", () => {
    spyUseSwrWithClusterId
      .mockReturnValueOnce({
        data: mockSessionsResponse,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      })
      .mockReturnValueOnce({
        data: undefined,
        error: undefined,
        isLoading: false,
        mutate: jest.fn(),
      });

    const wrapper = ({ children }: { children: React.ReactNode }) =>
      React.createElement(
        ClusterDetailsContext.Provider,
        { value: { isTenant: true } },
        children,
      );

    const { result } = renderHook(() => useLiveWorkload(), { wrapper });

    // The second SWR call (cluster locks) should receive null as the
    // fetcher, telling SWR to skip the request.
    expect(spyUseSwrWithClusterId).toHaveBeenCalledTimes(2);
    expect(spyUseSwrWithClusterId.mock.calls[1][1]).toBeNull();

    // Data should still be valid with null cluster locks.
    expect(result.current.data.clusterLocks).toBeNull();
    expect(result.current.data.statements).toBeDefined();
  });
});
