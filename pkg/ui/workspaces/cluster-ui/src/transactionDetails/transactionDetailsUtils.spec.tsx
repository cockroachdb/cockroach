// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  getStatementsForTransaction,
  getTxnFromSqlStatsTxns,
  getTxnQueryString,
} from "./transactionDetailsUtils";
import { mockTxnStats, Txn, Stmt, mockStmtStats } from "../api/testUtils";
import { shuffle } from "lodash";
import Long from "long";

describe("getTxnFromSqlStatsTxns", () => {
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
      3, // Expected idx.
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
      3, // Expected idx.
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
      1, // Expected idx.
    ],
  ])(
    "should return the first txn with the fingerprint ID and app name specified",
    (
      txnsToMock,
      fingerprintID: string,
      apps: string[] | null,
      expectedIdx: number,
    ) => {
      const txns = txnsToMock.map((txn: { id: number; app: string }) =>
        mockTxnStats({
          stats_data: {
            transaction_fingerprint_id: Long.fromInt(txn.id),
            app: txn.app,
          },
        }),
      );

      const expectedTxn = txns[expectedIdx];
      const txn = getTxnFromSqlStatsTxns(txns, fingerprintID, apps);
      expect(txn).toEqual(expectedTxn);
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
  ].map(tc => {
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
  // ALthough some of the mocked statements below will have ids in that range,
  // the app name will never match the given transactions.
  const extraStmts = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(i =>
    mockStmtStats({
      id: Long.fromInt(i),
      key: {
        key_data: {
          transaction_fingerprint_id: Long.fromInt(i),
          app: "aaaaaaaaaaaaaaaaaa",
        },
      },
      txn_fingerprint_ids: [Long.fromInt(i)],
    }),
  );

  const testCases = [
    {
      txnID: 1,
      stmtIDs: [2, 4, 6, 8],
      app: "cockroach",
    },
    {
      txnID: 2,
      stmtIDs: [1],
      app: "myApp",
    },
    {
      txnID: 3,
      stmtIDs: [],
      app: "hello-world",
    },
    {
      txnID: 3,
      stmtIDs: [4, 5, 6],
      useArrayProp: true,
      app: "$ internal-my-app",
    },
  ].map(tc => {
    const txnID = Long.fromInt(tc.txnID);

    const txn = mockTxnStats({
      stats_data: { transaction_fingerprint_id: txnID, app: tc.app },
    });

    const stmts = tc.stmtIDs.map(id =>
      mockStmtStats({
        id: Long.fromInt(id),
        txn_fingerprint_ids: tc.useArrayProp ? [txnID] : null,
        key: {
          key_data: {
            transaction_fingerprint_id: !tc.useArrayProp ? txnID : null,
            app: tc.app,
          },
        },
      }),
    );
    return [txn, stmts];
  });

  it.each(testCases)(
    "should return the list of stmts that have txn ids matching the provided txn AND same app name",
    (txn: Txn, expectedStmts: Stmt[]) => {
      const stmtsRes = getStatementsForTransaction(
        txn,
        shuffle([...extraStmts, ...expectedStmts]),
      );

      expect(stmtsRes.length).toEqual(expectedStmts.length);
    },
  );

  it.each([
    [null, null],
    [mockTxnStats(), null],
    [null, []],
  ])(
    "should return empty array when given invalid params",
    (txn: Txn | null, stmts: Stmt[] | null) => {
      expect(getStatementsForTransaction(txn, stmts)).toEqual([]);
    },
  );
});
