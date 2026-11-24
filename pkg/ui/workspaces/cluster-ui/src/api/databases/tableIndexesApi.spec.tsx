// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { renderHook } from "@testing-library/react-hooks";
import Long from "long";
import React from "react";
import { SWRConfig } from "swr";

import {
  ClusterDetailsContext,
  ClusterDetailsContextType,
} from "../../contexts";

import { useTableIndexStats, resetIndexStatsApi } from "./tableIndexesApi";

const RecommendationType = cockroach.sql.IndexRecommendation.RecommendationType;

const mockGetIndexStats = jest.fn();
const mockResetIndexStats = jest.fn();

jest.mock("../indexDetailsApi", () => ({
  getIndexStats: (...args: any[]) => mockGetIndexStats(...args),
  resetIndexStats: (...args: any[]) => mockResetIndexStats(...args),
}));

const clusterContext: ClusterDetailsContextType = {
  isTenant: false,
  clusterId: "test-cluster",
};

const wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <SWRConfig value={{ provider: () => new Map(), dedupingInterval: 0 }}>
    <ClusterDetailsContext.Provider value={clusterContext}>
      {children}
    </ClusterDetailsContext.Provider>
  </SWRConfig>
);

describe("useTableIndexStats", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it("should return empty data when API returns no statistics", async () => {
    mockGetIndexStats.mockResolvedValueOnce({
      statistics: [],
      last_reset: null,
      database_id: 5,
      index_recommendations: [],
    });

    const { result, waitForNextUpdate } = renderHook(
      () =>
        useTableIndexStats({
          dbName: "testdb",
          schemaName: "public",
          tableName: "testtable",
        }),
      { wrapper },
    );

    await waitForNextUpdate();

    expect(result.current.indexStats.tableIndexes).toEqual([]);
    expect(result.current.indexStats.lastReset).toBeNull();
    expect(result.current.indexStats.databaseID).toBe(5);
  });

  it("should transform protobuf response to TableIndex[]", async () => {
    const lastReadSeconds = Math.floor(Date.now() / 1000);
    const lastResetSeconds = lastReadSeconds - 3600;

    mockGetIndexStats.mockResolvedValueOnce({
      statistics: [
        {
          statistics: {
            key: {
              table_id: 15,
              index_id: 2,
            },
            stats: {
              total_read_count: Long.fromInt(10, true),
              last_read: {
                seconds: Long.fromInt(lastReadSeconds, false),
                nanos: 0,
              },
              total_rows_read: Long.fromInt(100, true),
              total_write_count: Long.fromInt(5, true),
              last_write: null,
              total_rows_written: Long.fromInt(50, true),
            },
          },
          index_name: "test_index",
          index_type: "SECONDARY",
          create_statement:
            "CREATE INDEX test_index ON testdb.public.testtable (col1 ASC)",
        },
      ],
      last_reset: {
        seconds: Long.fromInt(lastResetSeconds, false),
        nanos: 0,
      },
      database_id: 10,
      index_recommendations: [
        {
          table_id: 15,
          index_id: 2,
          type: RecommendationType.DROP_UNUSED,
          reason: "This index has not been used",
        },
      ],
    });

    const { result, waitForNextUpdate } = renderHook(
      () =>
        useTableIndexStats({
          dbName: "testdb",
          schemaName: "public",
          tableName: "testtable",
        }),
      { wrapper },
    );

    await waitForNextUpdate();

    const { tableIndexes, lastReset, databaseID } = result.current.indexStats;

    expect(tableIndexes).toHaveLength(1);
    const idx = tableIndexes[0];
    expect(idx.indexName).toBe("test_index");
    expect(idx.dbName).toBe("testdb");
    expect(idx.tableName).toBe("testtable");
    expect(idx.indexType).toBe("secondary");
    expect(idx.createStatement).toBe(
      "CREATE INDEX test_index ON testdb.public.testtable (col1 ASC)",
    );
    expect(idx.tableID).toBe("15");
    expect(idx.indexID).toBe("2");
    expect(idx.totalReads).toBe(10);
    expect(idx.totalRowsRead).toBe(100);
    expect(idx.lastRead).not.toBeNull();
    expect(idx.indexRecs).toHaveLength(1);
    expect(idx.indexRecs[0].type).toBe(RecommendationType.DROP_UNUSED);
    expect(idx.indexRecs[0].reason).toBe("This index has not been used");

    expect(lastReset).not.toBeNull();
    expect(databaseID).toBe(10);
  });

  it("should set lastRead to null when timestamp is epoch zero", async () => {
    mockGetIndexStats.mockResolvedValueOnce({
      statistics: [
        {
          statistics: {
            key: { table_id: 1, index_id: 1 },
            stats: {
              total_read_count: Long.fromInt(0, true),
              last_read: {
                seconds: Long.fromInt(0, false),
                nanos: 0,
              },
              total_rows_read: Long.fromInt(0, true),
            },
          },
          index_name: "primary",
          index_type: "PRIMARY",
          create_statement: "",
        },
      ],
      last_reset: { seconds: Long.fromInt(0, false), nanos: 0 },
      database_id: 1,
      index_recommendations: [],
    });

    const { result, waitForNextUpdate } = renderHook(
      () =>
        useTableIndexStats({
          dbName: "testdb",
          schemaName: "public",
          tableName: "testtable",
        }),
      { wrapper },
    );

    await waitForNextUpdate();

    expect(result.current.indexStats.tableIndexes[0].lastRead).toBeNull();
    expect(result.current.indexStats.lastReset).toBeNull();
  });

  it("should filter index recommendations by index_id", async () => {
    mockGetIndexStats.mockResolvedValueOnce({
      statistics: [
        {
          statistics: {
            key: { table_id: 1, index_id: 10 },
            stats: {
              total_read_count: Long.fromInt(0, true),
              last_read: null,
              total_rows_read: Long.fromInt(0, true),
            },
          },
          index_name: "idx_a",
          index_type: "secondary",
          create_statement: "",
        },
        {
          statistics: {
            key: { table_id: 1, index_id: 20 },
            stats: {
              total_read_count: Long.fromInt(0, true),
              last_read: null,
              total_rows_read: Long.fromInt(0, true),
            },
          },
          index_name: "idx_b",
          index_type: "secondary",
          create_statement: "",
        },
      ],
      last_reset: null,
      database_id: 1,
      index_recommendations: [
        {
          table_id: 1,
          index_id: 10,
          type: RecommendationType.DROP_UNUSED,
          reason: "rec for idx_a",
        },
        {
          table_id: 1,
          index_id: 20,
          type: RecommendationType.DROP_UNUSED,
          reason: "rec for idx_b",
        },
      ],
    });

    const { result, waitForNextUpdate } = renderHook(
      () =>
        useTableIndexStats({
          dbName: "testdb",
          schemaName: "public",
          tableName: "testtable",
        }),
      { wrapper },
    );

    await waitForNextUpdate();

    const { tableIndexes } = result.current.indexStats;
    const idxA = tableIndexes.find(i => i.indexName === "idx_a");
    const idxB = tableIndexes.find(i => i.indexName === "idx_b");

    expect(idxA.indexRecs).toHaveLength(1);
    expect(idxA.indexRecs[0].reason).toBe("rec for idx_a");

    expect(idxB.indexRecs).toHaveLength(1);
    expect(idxB.indexRecs[0].reason).toBe("rec for idx_b");
  });

  it("should not fetch when dbName is empty", () => {
    const { result } = renderHook(
      () =>
        useTableIndexStats({
          dbName: "",
          schemaName: "public",
          tableName: "testtable",
        }),
      { wrapper },
    );

    // Should not call the API.
    expect(mockGetIndexStats).not.toHaveBeenCalled();
    expect(result.current.indexStats.tableIndexes).toEqual([]);
  });
});

describe("resetIndexStatsApi", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it("should call resetIndexStats with an empty request", async () => {
    mockResetIndexStats.mockResolvedValueOnce({});
    await resetIndexStatsApi();
    expect(mockResetIndexStats).toHaveBeenCalledWith(
      expect.objectContaining({}),
    );
  });

  it("should propagate errors from resetIndexStats", async () => {
    const err = new Error("permission denied");
    mockResetIndexStats.mockRejectedValueOnce(err);
    await expect(resetIndexStatsApi()).rejects.toThrow("permission denied");
  });
});
