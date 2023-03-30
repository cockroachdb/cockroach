// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import Long from "long";
import {
  createCombinedStmtsRequest,
  getCombinedStatements,
  getFlushedTxnStatsApi,
  SqlStatsSortOptions,
  SqlStatsSortType,
} from "./statementsApi";
import { mockStmtStats, mockTxnStats } from "./testUtils";
import * as fetchData from "./fetchData";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { shuffle } from "lodash";

type Stmt =
  cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type Txn =
  cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;

const mockReturnVal = (stmts: Stmt[], txns: Txn[]) => {
  jest.spyOn(fetchData, "fetchData").mockReturnValue(
    Promise.resolve(
      new cockroach.server.serverpb.StatementsResponse({
        statements: stmts,
        transactions: txns,
      }),
    ),
  );
};

type GeneratorFn<T> = (length: number, i: number) => Partial<T>;

function createStmtsOverLoop(
  length: number,
  stmtGenerator: GeneratorFn<Stmt>,
): Stmt[] {
  return Array.from(new Array(length)).map((_, i) =>
    mockStmtStats(stmtGenerator(length, i)),
  );
}

function createTxnsOverLoop(
  length: number,
  generator: GeneratorFn<Txn>,
): Txn[] {
  return Array.from(new Array(length)).map((_, i) =>
    mockTxnStats(generator(length, i)),
  );
}

describe("getCombinedStatements", () => {
  afterAll(() => {
    jest.resetModules();
  });

  it("truncate response when the payload does not adhere to the limit from the request", async () => {
    const tests = [
      { limit: 50, respSize: 100 },
      { limit: 10, respSize: 10 }, // No truncation occurs.
      { limit: 10, respSize: 11 },
      { limit: 100, respSize: 11 }, // No truncation occurs.
    ];

    for (const tc of tests) {
      const req = createCombinedStmtsRequest({
        limit: tc.limit,
        sort: SqlStatsSortOptions.EXECUTION_COUNT,
        start: null,
        end: null,
      });

      const stmts: Stmt[] = [];
      const txns: Txn[] = [];

      for (let i = 1; i <= tc.respSize; ++i) {
        stmts.push(mockStmtStats({ id: Long.fromInt(i) }));
        txns.push(
          mockTxnStats({
            stats_data: { transaction_fingerprint_id: Long.fromInt(i) },
          }),
        );
      }

      mockReturnVal(stmts, txns);

      const res = await getCombinedStatements(req);

      const expectedLen = tc.limit > tc.respSize ? tc.respSize : tc.limit;
      expect(res?.statements?.length).toBe(expectedLen);

      // // Transactions half should have been discarded, regardless of whether we have truncated.
      expect(res?.transactions?.length).toBe(0);
    }
  });

  // Each test case will take a list of statements ordered by the provided
  // sort value. The test will shuffle the given array to use as the mocked
  // return value, and verify that the truncated list is ordered by the sort.
  it.each([
    [
      "EXECUTION_COUNT",
      createStmtsOverLoop(100, (length, i) => ({
        id: Long.fromInt(i),
        stats: { count: Long.fromInt(length - i) },
      })),
      SqlStatsSortOptions.EXECUTION_COUNT,
    ],
    [
      "CONTENTION_TIME",
      createStmtsOverLoop(100, (length, i) => ({
        id: Long.fromInt(i),
        stats: {
          count: Long.fromInt(i),
          exec_stats: {
            contention_time: {
              mean: length - i,
              squared_diffs: 0,
            },
          },
        },
      })),
      SqlStatsSortOptions.CONTENTION_TIME,
    ],
    [
      "SVC_LAT",
      createStmtsOverLoop(100, (length, i) => ({
        id: Long.fromInt(i),
        stats: {
          count: Long.fromInt(i),
          service_lat: {
            mean: length - i,
            squared_diffs: 0,
          },
        },
      })),
      SqlStatsSortOptions.SERVICE_LAT,
    ],
    [
      "PCT_RUNTIME",
      createStmtsOverLoop(100, (length, i) => ({
        id: Long.fromInt(i),
        stats: {
          count: Long.fromInt(5),
          service_lat: { mean: length - i, squared_diffs: 0 },
        },
      })),
      SqlStatsSortOptions.PCT_RUNTIME,
    ],
  ])(
    "sorts data by requested option before truncating > %s",
    async (_name: string, stmtsOrdered: Stmt[], sortBy: SqlStatsSortType) => {
      const shuffledStmts = shuffle(stmtsOrdered);

      mockReturnVal(shuffledStmts, null);

      const limit = Math.floor(stmtsOrdered.length / 2);
      const req = createCombinedStmtsRequest({
        limit,
        sort: sortBy,
        start: null,
        end: null,
      });

      const res = await getCombinedStatements(req);

      expect(res.statements.length).toEqual(limit);

      res.statements.forEach((stmt, i) =>
        expect(stmt.id.toInt()).toEqual(stmtsOrdered[i].id.toInt()),
      );
    },
  );
});

describe("getFlushedTxnStatsApi", () => {
  afterAll(() => {
    jest.resetModules();
  });

  it("truncates response when the payload does not adhere to the limit from the request", async () => {
    const tests = [
      { limit: 50, respSize: 100 },
      { limit: 20, respSize: 56 },
      { limit: 10, respSize: 10 }, // No truncation occurs.
      { limit: 10, respSize: 11 },
      { limit: 100, respSize: 11 }, // No truncation occurs.
    ];

    for (const tc of tests) {
      const req = createCombinedStmtsRequest({
        limit: tc.limit,
        sort: SqlStatsSortOptions.EXECUTION_COUNT,
        start: null,
        end: null,
      });

      const stmts: Stmt[] = [];
      const txns: Txn[] = [];

      // The txn ID we'll assign to stmts.
      // No txn in the resp will have this ID and so we'll expect nothing in the
      // transformed stmts response.
      const txnIDForStmts = tc.respSize + 2;

      for (let i = 1; i <= tc.respSize; ++i) {
        stmts.push(
          mockStmtStats({
            id: Long.fromInt(i),
            key: {
              key_data: {
                transaction_fingerprint_id: Long.fromInt(txnIDForStmts),
              },
            },
          }),
        );
        txns.push(
          mockTxnStats({
            stats_data: { transaction_fingerprint_id: Long.fromInt(i) },
          }),
        );
      }

      mockReturnVal(stmts, txns);

      const res = await getFlushedTxnStatsApi(req);

      const expectedLen = tc.limit > tc.respSize ? tc.respSize : tc.limit;
      expect(res?.transactions?.length).toBe(expectedLen);
      // No txn in the resp will have this ID and so we'll expect nothing in the
      // transformed stmts response.
      if (tc.respSize > tc.limit) {
        // We only filter out stmts if we had to truncate the data.
        expect(res?.statements?.length).toBe(0);
      }
    }
  });

  // Each test case will take a list of transactions ordered by the provided
  // sort value. The test will shuffle the given array to use as the mocked
  // return value, and verify that the truncated list is ordered by the sort.
  it.each([
    [
      "EXECUTION_COUNT",
      createTxnsOverLoop(100, (length, i) => ({
        stats_data: {
          transaction_fingerprint_id: Long.fromInt(i),
          stats: { count: Long.fromInt(length - i) },
        },
      })),
      SqlStatsSortOptions.EXECUTION_COUNT,
    ],
    [
      "CONTENTION_TIME",
      createTxnsOverLoop(100, (length, i) => ({
        stats_data: {
          transaction_fingerprint_id: Long.fromInt(i),
          stats: {
            count: Long.fromInt(i),
            exec_stats: {
              contention_time: {
                mean: length - i,
                squared_diffs: 0,
              },
            },
          },
        },
      })),
      SqlStatsSortOptions.CONTENTION_TIME,
    ],
    [
      "SVC_LAT",
      createTxnsOverLoop(100, (length, i) => ({
        stats_data: {
          transaction_fingerprint_id: Long.fromInt(i),
          stats: {
            count: Long.fromInt(i),
            service_lat: {
              mean: length - i,
              squared_diffs: 0,
            },
          },
        },
      })),
      SqlStatsSortOptions.SERVICE_LAT,
    ],
    [
      "PCT_RUNTIME",
      createTxnsOverLoop(100, (length, i) => ({
        stats_data: {
          transaction_fingerprint_id: Long.fromInt(i),
          stats: {
            count: Long.fromInt(2),
            service_lat: {
              mean: length - i,
              squared_diffs: 0,
            },
          },
        },
      })),
      SqlStatsSortOptions.PCT_RUNTIME,
    ],
  ])(
    "sorts data by requested option before truncating > %s",
    async (_name: string, txnsOrdered: Txn[], sortBy: SqlStatsSortType) => {
      const shuffledTxns = shuffle(txnsOrdered);

      mockReturnVal(null, shuffledTxns);

      const limit = Math.floor(txnsOrdered.length / 2);
      const req = createCombinedStmtsRequest({
        limit,
        sort: sortBy,
        start: null,
        end: null,
      });

      const res = await getFlushedTxnStatsApi(req);

      expect(res.transactions.length).toEqual(limit);

      res.transactions.forEach((txn, i) =>
        expect(txn.stats_data.transaction_fingerprint_id.toInt()).toEqual(
          txnsOrdered[i].stats_data.transaction_fingerprint_id.toInt(),
        ),
      );
    },
  );
});
