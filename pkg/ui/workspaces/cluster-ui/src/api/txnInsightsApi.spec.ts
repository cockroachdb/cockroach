// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import {
  InsightExecEnum,
  InsightNameEnum,
  TxnContentionInsightDetails,
} from "../insights";
import { MockSqlResponse } from "../util/testing";

import {
  ContentionResponseColumns,
  getTxnInsightsContentionDetailsApi,
} from "./contentionApi";
import * as sqlApi from "./sqlApi";
import { SqlExecutionResponse } from "./sqlApi";
import {
  TxnStmtFingerprintsResponseColumns,
  FingerprintStmtsResponseColumns,
} from "./txnInsightsApi";

type TxnContentionDetailsTests = {
  name: string;
  contentionResp: SqlExecutionResponse<ContentionResponseColumns>;
  txnFingerprintsResp: SqlExecutionResponse<TxnStmtFingerprintsResponseColumns>;
  stmtsFingerprintsResp: SqlExecutionResponse<FingerprintStmtsResponseColumns>;
  expected: TxnContentionInsightDetails;
};

describe("test txn insights api functions", () => {
  const waitingTxnID = "1a2a4828-5fc6-42d1-ab93-fadd4a514b69";
  const contentionDetailsMock: ContentionResponseColumns = {
    contention_duration: "00:00:00.00866",
    waiting_stmt_id: "17761e953a52c0300000000000000001",
    waiting_stmt_fingerprint_id: "b75264458f6e2ef3",
    schema_name: "public",
    database_name: "system",
    table_name: "namespace",
    index_name: "primary",
    key: `/NamespaceTable/30/1/0/0/"movr"/4/1`,
    collection_ts: "2023-07-28 19:25:36.434081+00",
    blocking_txn_id: "a13773b3-9bca-4019-9cfb-a376d6a4f412",
    blocking_txn_fingerprint_id: "4329ab5f4493f82d",
    waiting_txn_id: waitingTxnID,
    waiting_txn_fingerprint_id: "1831d909096f992c",
    contention_type: "LOCK_WAIT",
  };

  afterEach(() => {
    jest.resetAllMocks();
    jest.clearAllMocks();
  });

  test.each([
    {
      name: "all api responses empty",
      contentionResp: MockSqlResponse([]),
      txnFingerprintsResp: MockSqlResponse([]),
      stmtsFingerprintsResp: MockSqlResponse([]),
      expected: null,
    },
    {
      name: "no fingerprints available",
      contentionResp: MockSqlResponse([contentionDetailsMock]),
      txnFingerprintsResp: MockSqlResponse([]),
      stmtsFingerprintsResp: MockSqlResponse([]),
      expected: {
        transactionExecutionID: contentionDetailsMock.waiting_txn_id,
        application: undefined,
        transactionFingerprintID:
          contentionDetailsMock.waiting_txn_fingerprint_id,
        blockingContentionDetails: [
          {
            blockingExecutionID: contentionDetailsMock.blocking_txn_id,
            blockingTxnFingerprintID:
              contentionDetailsMock.blocking_txn_fingerprint_id,
            blockingTxnQuery: null,
            collectionTimeStamp: moment("2023-07-28 19:25:36.434081+00").utc(),
            contendedKey: '/NamespaceTable/30/1/0/0/"movr"/4/1',
            contentionTimeMs: 9,
            databaseName: contentionDetailsMock.database_name,
            indexName: contentionDetailsMock.index_name,
            schemaName: contentionDetailsMock.schema_name,
            tableName: contentionDetailsMock.table_name,
            waitingStmtFingerprintID:
              contentionDetailsMock.waiting_stmt_fingerprint_id,
            waitingStmtID: contentionDetailsMock.waiting_stmt_id,
            waitingTxnFingerprintID:
              contentionDetailsMock.waiting_txn_fingerprint_id,
            waitingTxnID: contentionDetailsMock.waiting_txn_id,
            contentionType: "LOCK_WAIT",
          },
        ],
        execType: InsightExecEnum.TRANSACTION,
        insightName: InsightNameEnum.HIGH_CONTENTION,
      },
    },
    {
      name: "no stmt fingerprints available",
      contentionResp: MockSqlResponse([contentionDetailsMock]),
      txnFingerprintsResp: MockSqlResponse<TxnStmtFingerprintsResponseColumns>([
        {
          transaction_fingerprint_id:
            contentionDetailsMock.blocking_txn_fingerprint_id,
          query_ids: ["a", "b", "c"],
          app_name: undefined,
        },
      ]),
      stmtsFingerprintsResp: MockSqlResponse([]),
      expected: {
        transactionExecutionID: contentionDetailsMock.waiting_txn_id,
        application: undefined,
        transactionFingerprintID:
          contentionDetailsMock.waiting_txn_fingerprint_id,
        blockingContentionDetails: [
          {
            blockingExecutionID: contentionDetailsMock.blocking_txn_id,
            blockingTxnFingerprintID:
              contentionDetailsMock.blocking_txn_fingerprint_id,
            blockingTxnQuery: [
              "Query unavailable for statement fingerprint 000000000000000a",
              "Query unavailable for statement fingerprint 000000000000000b",
              "Query unavailable for statement fingerprint 000000000000000c",
            ],
            collectionTimeStamp: moment("2023-07-28 19:25:36.434081+00").utc(),
            contendedKey: '/NamespaceTable/30/1/0/0/"movr"/4/1',
            contentionTimeMs: 9,
            databaseName: contentionDetailsMock.database_name,
            indexName: contentionDetailsMock.index_name,
            schemaName: contentionDetailsMock.schema_name,
            tableName: contentionDetailsMock.table_name,
            waitingStmtFingerprintID:
              contentionDetailsMock.waiting_stmt_fingerprint_id,
            waitingStmtID: contentionDetailsMock.waiting_stmt_id,
            waitingTxnFingerprintID:
              contentionDetailsMock.waiting_txn_fingerprint_id,
            waitingTxnID: contentionDetailsMock.waiting_txn_id,
            contentionType: "LOCK_WAIT",
          },
        ],
        execType: InsightExecEnum.TRANSACTION,
        insightName: InsightNameEnum.HIGH_CONTENTION,
      },
    },
    {
      name: "all info available",
      contentionResp: MockSqlResponse([contentionDetailsMock]),
      txnFingerprintsResp: MockSqlResponse<TxnStmtFingerprintsResponseColumns>([
        {
          transaction_fingerprint_id:
            contentionDetailsMock.blocking_txn_fingerprint_id,
          query_ids: ["a", "b", "c"],
          app_name: undefined,
        },
      ]),
      stmtsFingerprintsResp: MockSqlResponse<FingerprintStmtsResponseColumns>([
        {
          statement_fingerprint_id: "a",
          query: "select 1",
        },
        {
          statement_fingerprint_id: "b",
          query: "select 2",
        },
        {
          statement_fingerprint_id: "c",
          query: "select 3",
        },
      ]),
      expected: {
        transactionExecutionID: contentionDetailsMock.waiting_txn_id,
        application: undefined,
        transactionFingerprintID:
          contentionDetailsMock.waiting_txn_fingerprint_id,
        blockingContentionDetails: [
          {
            blockingExecutionID: contentionDetailsMock.blocking_txn_id,
            blockingTxnFingerprintID:
              contentionDetailsMock.blocking_txn_fingerprint_id,
            blockingTxnQuery: ["select 1", "select 2", "select 3"],
            collectionTimeStamp: moment("2023-07-28 19:25:36.434081+00").utc(),
            contendedKey: '/NamespaceTable/30/1/0/0/"movr"/4/1',
            contentionTimeMs: 9,
            databaseName: contentionDetailsMock.database_name,
            indexName: contentionDetailsMock.index_name,
            schemaName: contentionDetailsMock.schema_name,
            tableName: contentionDetailsMock.table_name,
            waitingStmtFingerprintID:
              contentionDetailsMock.waiting_stmt_fingerprint_id,
            waitingStmtID: contentionDetailsMock.waiting_stmt_id,
            waitingTxnFingerprintID:
              contentionDetailsMock.waiting_txn_fingerprint_id,
            waitingTxnID: contentionDetailsMock.waiting_txn_id,
            contentionType: "LOCK_WAIT",
          },
        ],
        execType: InsightExecEnum.TRANSACTION,
        insightName: InsightNameEnum.HIGH_CONTENTION,
      },
    },
  ] as TxnContentionDetailsTests[])(
    "$name",
    async (tc: TxnContentionDetailsTests) => {
      await jest
        .spyOn(sqlApi, "executeInternalSql")
        .mockReturnValueOnce(Promise.resolve(tc.contentionResp))
        .mockReturnValueOnce(Promise.resolve(tc.txnFingerprintsResp))
        .mockReturnValueOnce(Promise.resolve(tc.stmtsFingerprintsResp));

      const res = await getTxnInsightsContentionDetailsApi({
        txnExecutionID: waitingTxnID,
      });
      expect(res).toEqual(tc.expected);
    },
  );
});
