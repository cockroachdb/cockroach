// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import shuffle from "lodash/shuffle";
import Long from "long";

import { mockTxnStats, Txn, Stmt, mockStmtStats } from "../api/testUtils";
import { unset } from "../util";

import {
  getStatementsForTransaction,
  getTxnFromSqlStatsTxns,
  getTxnQueryString,
} from "./transactionDetailsUtils";

describe("getTxnFromSqlStatsTxns", () => {
  // Each transaction will be mocked with an exec count of 1.
  // We will verify that we aggregated the expected number of transactions.
  it.each([
    [
      [
        { id: 1, app: "hello_world" },
        { id: 2, app: "cockroach" },
        { id: 3, app: "" },
        { id: 3, app: "cockroach" },
        { id: 3, app: "cockroach" },
        { id: 3, app: "my_app" },
        { id: 4, app: "my_app" },
      ],
      "3", // fingerprint id
      ["cockroach", "my_app"], // app name
      3, // Expected count
    ],
    [
      [
        { id: 1, app: "hello_world" },
        { id: 2, app: "cockroach_app" },
        { id: 3, app: "" },
        { id: 3, app: "cockroach" },
        { id: 3, app: "my_app" },
        { id: 4, app: "my_app" },
      ],
      "3", // fingerprint id
      ["cockroach", "my_app"], // app name
      2, // Expected count.
    ],
    [
      [
        { id: 1, app: "hello_world" },
        { id: 2, app: "cockroach" },
        { id: 2, app: "cockrooch" },
        { id: 3, app: "cockroach" },
        { id: 4, app: "my_app" },
      ],
      "2", // fingerprint id
      null, // app names
      2, // Expected idx.
    ],
    [
      [
        { id: 1, app: "aaaaaa" },
        { id: 2, app: "bbbbbb" },
        { id: 2, app: "cccccc" },
        { id: 3, app: "dddddd" },
        { id: 4, app: "dddddd" },
      ],
      "3", // fingerprint id
      ["dddddd"], // app names
      1, // Expected idx.
    ],
    // Test unset app name. The '(unset)' app name should be explicitly
    // provided to the function instead of the empty string.
    [
      [
        { id: 1, app: "aaaaaa" },
        { id: 3, app: "cccccc" },
        { id: 4, app: "" },
        { id: 4, app: "" },
        { id: 5, app: "" },
      ],
      "4", // fingerprint id
      [unset], // app names
      2, // Expected idx.
    ],
  ])(
    "should return the txn aggregated by fingerprint ID and app names",
    (
      txnsToMock,
      fingerprintID: string,
      apps: string[] | null,
      expectedCount: number,
    ) => {
      const txns = txnsToMock.map((txn: { id: number; app: string }) =>
        mockTxnStats({
          stats_data: {
            transaction_fingerprint_id: Long.fromInt(txn.id),
            app: txn.app,
            stats: {
              count: Long.fromNumber(1),
            },
          },
        }),
      );

      const txn = getTxnFromSqlStatsTxns(txns, fingerprintID, apps);
      expect(txn.stats_data.transaction_fingerprint_id.toString()).toEqual(
        fingerprintID,
      );
      expect(txn.stats_data.stats.count.toNumber()).toEqual(expectedCount);
    },
  );

  it("should return null if no txn can be found", () => {
    const txns = [1, 2, 3, 4, 5, 6].map(txn =>
      mockTxnStats({
        stats_data: {
          transaction_fingerprint_id: Long.fromInt(txn),
          app: "uncool_app",
        },
      }),
    );

    const txn = getTxnFromSqlStatsTxns(txns, "1", ["cool_app"]);
    expect(txn == null).toBe(true);
  });

  it.each([
    [null, null, null],
    [null, "123", null],
    [null, null, ["app"]],
    [[mockTxnStats()], null, null],
    [[mockTxnStats()], "123", []],
    [[mockTxnStats()], null, ["app"]],
    [[mockTxnStats()], "", ["app"]],
    [null, "123", ["app"]],
  ])(
    "should return null when given invalid parameters: (%p, %p, %p)",
    (
      txns: Txn[] | null,
      fingerprintID: string | null,
      apps: string[] | null,
    ) => {
      const txn = getTxnFromSqlStatsTxns(txns, fingerprintID, apps);
      expect(txn == null).toBe(true);
    },
  );
});

describe("getTxnQueryString", () => {
  const extraStmts = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(i =>
    mockStmtStats({
      id: Long.fromInt(i + 100),
      txn_fingerprint_ids: [Long.fromInt(9999999999)],
    }),
  );

  const queryStrTestCases = [
    {
      txnID: 1,
      stmtIDs: [4, 5, 6],
      queries: ["SELECT 1", "SELECT 2", "SELECT 3"],
    },
    {
      txnID: 2,
      stmtIDs: [2, 11],
      queries: ["INSERT INTO foo VALUES (1, 2), (3, 4)", "SELECT * FROM foo"],
    },
    {
      txnID: 3,
      stmtIDs: [8],
      queries: ["SELECT * FROM foo"],
    },
    {
      txnID: 4,
      stmtIDs: [3, 5, 7, 9],
      queries: ["a", "b", "c", "d"],
    },
  ].map((tc): [Txn, Stmt[], string] => {
    const txnID = Long.fromInt(tc.txnID);

    const txn = mockTxnStats({
      stats_data: {
        transaction_fingerprint_id: txnID,
        statement_fingerprint_ids: tc.stmtIDs.map(id => Long.fromInt(id)),
      },
    });

    // Stub statements to have the test case txn id and appropriate query strings.
    const stmts = tc.queries.map((query, i) =>
      mockStmtStats({
        id: Long.fromInt(tc.stmtIDs[i]),
        key: { key_data: { query, transaction_fingerprint_id: txnID } },
      }),
    );

    return [txn, stmts, tc.queries.join("\n")];
  });

  it.each(queryStrTestCases)(
    "should build the full txn query string from the provided txn and stmt list",
    (txn: Txn, stmts: Stmt[], expected: string) => {
      const txnStr = getTxnQueryString(txn, shuffle([...extraStmts, ...stmts]));
      expect(txnStr).toEqual(expected);
    },
  );

  it("builds partial query when there is a stmt missing from the provided list", () => {
    const txn = mockTxnStats({
      stats_data: {
        transaction_fingerprint_id: Long.fromInt(1),
        statement_fingerprint_ids: [Long.fromInt(1), Long.fromInt(2)],
      },
    });
    const stmts = [
      mockStmtStats({
        id: Long.fromInt(2),
        key: { key_data: { query: "HELLO" } },
      }),
    ];

    const txnStr = getTxnQueryString(txn, stmts);
    expect(txnStr).toEqual("\nHELLO");
  });

  it.each([
    [null, null],
    [null, extraStmts],
    [mockTxnStats(), null],
    [mockTxnStats(), []],
  ])(
    "should return the empty string when given invalid params",
    (txn: Txn, stmts: Stmt[] | null) => {
      const txnStr = getTxnQueryString(txn, stmts);
      expect(txnStr).toEqual("");
    },
  );
});

describe("getStatementsForTransaction", () => {
  // These are the statements we'll throw in with the expected statements
  // that should be filtered out.
  // We'll use txn ids in the range [1,10] when testing this function.
  // Although some of the mocked statements below will have ids in that range,
  // the app name will never match the given transactions.
  const extraStmts = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(i =>
    mockStmtStats({
      id: Long.fromInt(i),
      key: {
        key_data: {
          transaction_fingerprint_id: Long.fromInt(i),
          app: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
      },
      txn_fingerprint_ids: [Long.fromInt(i)],
    }),
  );

  // The statements specified in each test case will be assigned the txn id in the test
  //  and also be randomly be assigned one of the app names in the provided apps list.
  const testCases = [
    {
      txnID: 1,
      apps: ["cockroach", "myApp"],
      stmtIDs: [2, 4, 6, 8, 10, 12],
    },
    {
      txnID: 2,
      apps: ["myApp"],
      stmtIDs: [1],
    },
    {
      txnID: 3,
      apps: ["hello-world"],
      stmtIDs: [],
    },
    {
      txnID: 3,
      apps: ["$ internal-my-app"],
      stmtIDs: [4, 5, 6],
      useArrayProp: true,
    },
  ].map((tc): [Txn, string[], Stmt[]] => {
    const txnID = Long.fromInt(tc.txnID);

    const txn = mockTxnStats({
      stats_data: { transaction_fingerprint_id: txnID, app: tc.apps[0] },
    });

    const randomApp = tc.apps?.[Math.floor(Math.random() * tc.apps.length)];
    const stmts = tc.stmtIDs.map(id =>
      mockStmtStats({
        id: Long.fromInt(id),
        txn_fingerprint_ids: tc.useArrayProp ? [txnID] : null,
        key: {
          key_data: {
            transaction_fingerprint_id: !tc.useArrayProp ? txnID : null,
            app: randomApp,
          },
        },
      }),
    );
    return [txn, tc.apps, stmts];
  });

  it.each(testCases)(
    "should return the list of stmts that have txn ids matching the provided txn and one of the app names",
    (txn: Txn, apps: string[], expectedStmts: Stmt[]) => {
      const stmtsRes = getStatementsForTransaction(
        txn?.stats_data?.transaction_fingerprint_id?.toString(),
        apps,
        shuffle([...extraStmts, ...expectedStmts]),
      );

      expect(stmtsRes.length).toEqual(expectedStmts.length);
      const expectedStmtIDs = new Set(expectedStmts.map(s => s.id.toString()));
      stmtsRes.forEach(s => expect(expectedStmtIDs.has(s.id.toString())));
    },
  );

  it.each([
    [null, null],
    [mockTxnStats(), null],
    [null, []],
  ])(
    "should return empty array when given invalid params",
    (txn: Txn | null, stmts: Stmt[] | null) => {
      expect(
        getStatementsForTransaction(
          txn?.stats_data.transaction_fingerprint_id.toString(),
          [txn?.stats_data.app],
          stmts,
        ),
      ).toEqual([]);
    },
  );
});
