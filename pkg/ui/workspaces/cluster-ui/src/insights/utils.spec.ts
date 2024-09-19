// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import {
  ContentionDetails,
  failedExecutionInsight,
  highContentionInsight,
  highRetryCountInsight,
  InsightExecEnum,
  InsightNameEnum,
  planRegressionInsight,
  slowExecutionInsight,
  StatementStatus,
  StmtInsightEvent,
  suboptimalPlanInsight,
  TransactionStatus,
  TxnInsightDetails,
  TxnInsightEvent,
} from "./types";
import {
  filterStatementInsights,
  filterTransactionInsights,
  getAppsFromStatementInsights,
  getAppsFromTransactionInsights,
  getInsightsFromProblemsAndCauses,
  mergeTxnInsightDetails,
} from "./utils";

const INTERNAL_APP_PREFIX = "$ internal";

const blockedContentionMock: ContentionDetails = {
  collectionTimeStamp: moment(),
  blockingExecutionID: "execution",
  blockingTxnFingerprintID: "block",
  blockingTxnQuery: ["select 1"],
  waitingStmtFingerprintID: "waitingStmtFingerprintID",
  waitingStmtID: "waitingStmtID",
  waitingTxnFingerprintID: "waitingTxnFingerprintID",
  waitingTxnID: "waitingTxnID",
  contendedKey: "key",
  schemaName: "schema",
  databaseName: "db",
  tableName: "table",
  indexName: "index",
  contentionTimeMs: 500,
  contentionType: "LOCK_WAIT",
};

const statementInsightMock: StmtInsightEvent = {
  databaseName: "defaultDb",
  username: "craig",
  priority: "high",
  retries: 0,
  implicitTxn: false,
  sessionID: "123",
  transactionExecutionID: "execution",
  transactionFingerprintID: "fingerprint",
  application: "sql_obs_fun_times",
  lastRetryReason: null,
  statementExecutionID: "execution",
  statementFingerprintID: "fingerprint",
  startTime: moment(),
  isFullScan: false,
  elapsedTimeMillis: 100,
  contentionTime: moment.duration(100, "millisecond"),
  endTime: moment(),
  rowsRead: 4,
  rowsWritten: 1,
  query: "select 1",
  insights: [failedExecutionInsight(InsightExecEnum.STATEMENT)],
  indexRecommendations: [],
  planGist: "gist",
  cpuSQLNanos: 50,
  errorCode: "",
  errorMsg: "",
  status: StatementStatus.COMPLETED,
};

function mockStmtInsightEvent(
  fields: Partial<StmtInsightEvent> = {},
): StmtInsightEvent {
  return {
    ...statementInsightMock,
    transactionExecutionID: "transactionExecution",
    transactionFingerprintID: "fingerprintExecution",
    implicitTxn: false,
    sessionID: "sessionID",
    databaseName: "defaultdb",
    username: "sql-obs",
    priority: "high",
    retries: 0,
    application: "coolApp",
    ...fields,
  };
}

const txnInsightEventMock: TxnInsightEvent = {
  username: "craig",
  priority: "high",
  retries: 0,
  implicitTxn: false,
  sessionID: "123",
  transactionExecutionID: "execution",
  transactionFingerprintID: "fingerprint",
  application: "sql_obs_fun_times",
  lastRetryReason: null,
  contentionTime: null,
  insights: [failedExecutionInsight(InsightExecEnum.TRANSACTION)],
  query: "select 1",
  rowsRead: 4,
  rowsWritten: 1,
  startTime: moment(),
  endTime: moment(),
  elapsedTimeMillis: 1,
  stmtExecutionIDs: [statementInsightMock.statementExecutionID],
  cpuSQLNanos: 50,
  errorCode: "",
  errorMsg: "",
  status: TransactionStatus.COMPLETED,
};

function mockTxnInsightEvent(
  fields: Partial<TxnInsightEvent> = {},
): TxnInsightEvent {
  return { ...txnInsightEventMock, ...fields };
}

const txnInsightDetailsMock: TxnInsightDetails = {
  txnDetails: txnInsightEventMock,
  blockingContentionDetails: [blockedContentionMock],
  statements: [statementInsightMock],
};

describe("test workload insights utils", () => {
  describe("filterTransactionInsights", () => {
    const txns = [
      mockTxnInsightEvent({ application: "hello" }),
      mockTxnInsightEvent({ application: "world" }),
      mockTxnInsightEvent({ application: "cat" }),
      mockTxnInsightEvent({ application: "frog" }),
      mockTxnInsightEvent({ application: "cockroach" }),
      mockTxnInsightEvent({ application: "cockroach" }),
      mockTxnInsightEvent({ application: "db" }),
      mockTxnInsightEvent({ application: "db" }),
    ];

    it("should filter out txns not matching provided filters", () => {
      const filters = { app: "cockroach,db" };
      const filtered = filterTransactionInsights(
        txns,
        filters,
        INTERNAL_APP_PREFIX,
      );
      expect(filtered.length).toEqual(4);
    });

    it("should filter out or include internal txns depending on filters", () => {
      const txnsWithInternal = [
        ...txns,
        mockTxnInsightEvent({ application: "$ internal-my-app" }),
        mockTxnInsightEvent({ application: "$ internal-my-app" }),
      ];
      // If internal app name not included in filter, internal apps should be
      // filtered out.
      const filters = { app: "" };
      let filtered = filterTransactionInsights(
        txnsWithInternal,
        filters,
        INTERNAL_APP_PREFIX,
      );
      expect(filtered.length).toEqual(txns.length);

      // Now they should be included.
      filters.app = INTERNAL_APP_PREFIX;
      filtered = filterTransactionInsights(
        txnsWithInternal,
        filters,
        INTERNAL_APP_PREFIX,
      );
      expect(filtered.length).toEqual(2);
    });

    it("should filter out txns not matching search", () => {
      const txnsWithQueries = [
        mockTxnInsightEvent({ query: "select foo ; update bar" }),
        mockTxnInsightEvent({ query: "hello ; world ; foo" }),
      ];

      let filtered = filterTransactionInsights(
        txnsWithQueries,
        { app: "" },
        INTERNAL_APP_PREFIX,
        "foo",
      );
      expect(filtered.length).toEqual(2);

      filtered = filterTransactionInsights(
        txnsWithQueries,
        { app: "" },
        INTERNAL_APP_PREFIX,
        "update",
      );
      expect(filtered.length).toEqual(1);

      filtered = filterTransactionInsights(
        txnsWithQueries,
        { app: "" },
        INTERNAL_APP_PREFIX,
        "no results",
      );
      expect(filtered.length).toEqual(0);

      filtered = filterTransactionInsights(
        [
          ...txnsWithQueries,
          mockTxnInsightEvent({ sessionID: "my-uniq-session-id-11223344" }),
          mockTxnInsightEvent({
            stmtExecutionIDs: ["statement-exec-id-11223344"],
          }),
          mockTxnInsightEvent({
            transactionExecutionID: "txn-exec-id-11223344",
          }),
          mockTxnInsightEvent({
            transactionFingerprintID: "txn-fingerprint-id-11223344",
          }),
        ],
        { app: "" },
        INTERNAL_APP_PREFIX,
        "11223344",
      );
      expect(filtered.length).toEqual(4);
    });

    it("should filter txns given a mix of requirements", () => {
      const txnsMixed = [
        // This should be the only txn remaining.
        mockTxnInsightEvent({
          application: "myApp",
          query: "select foo",
        }),
        // No required search term.
        mockTxnInsightEvent({ application: "myApp", query: "update bar" }),
        // No required app.
        mockTxnInsightEvent({ query: "hello ; world ; select foo" }),
        // Internal app should be filtered out.
        mockTxnInsightEvent({
          application: INTERNAL_APP_PREFIX,
          query: "select foo",
        }),
      ];

      const filtered = filterTransactionInsights(
        txnsMixed,
        { app: "myApp" },
        INTERNAL_APP_PREFIX,
        "select foo",
      );
      expect(filtered.length).toEqual(1);
    });
  });

  describe("getAppsFromTransactionInsights", () => {
    const appNames = ["one", "two", "three"];
    const txns = appNames.map(app => mockTxnInsightEvent({ application: app }));

    // Multiple internal app names should all become the internal
    // app name prefix.
    txns.push(
      mockTxnInsightEvent({
        application: "$ internal-app",
      }),
      mockTxnInsightEvent({
        application: "$ internal-another-app",
      }),
    );

    const appsFromTxns = getAppsFromTransactionInsights(
      txns,
      INTERNAL_APP_PREFIX,
    );
    expect(appsFromTxns.length).toEqual(appNames.length + 1);
    appNames.forEach(app => expect(appsFromTxns.includes(app)).toBeTruthy());
  });

  describe("filterStatementInsights", () => {
    const stmts = [
      mockStmtInsightEvent({ application: "hello" }),
      mockStmtInsightEvent({ application: "world" }),
      mockStmtInsightEvent({ application: "cat" }),
      mockStmtInsightEvent({ application: "frog" }),
      mockStmtInsightEvent({ application: "cockroach" }),
      mockStmtInsightEvent({ application: "cockroach" }),
      mockStmtInsightEvent({ application: "db" }),
      mockStmtInsightEvent({ application: "db" }),
    ];

    it("should filter out stmts not matching provided filters", () => {
      const filters = { app: "cockroach,db" };
      const filtered = filterStatementInsights(
        stmts,
        filters,
        INTERNAL_APP_PREFIX,
      );
      expect(filtered.length).toEqual(4);
    });

    it("should filter out or include internal stmts depending on filters", () => {
      const stmtsWithInternal = [
        ...stmts,
        mockStmtInsightEvent({ application: "$ internal-my-app" }),
        mockStmtInsightEvent({ application: "$ internal-my-app" }),
      ];
      // If internal app name not included in filter, internal apps should be
      // filtered out.
      const filters = { app: "" };
      let filtered = filterStatementInsights(
        stmtsWithInternal,
        filters,
        INTERNAL_APP_PREFIX,
      );
      expect(filtered.length).toEqual(stmts.length);

      // Now they should be included.
      filters.app = INTERNAL_APP_PREFIX;
      filtered = filterStatementInsights(
        stmtsWithInternal,
        filters,
        INTERNAL_APP_PREFIX,
      );
      expect(filtered.length).toEqual(2);
    });

    it("should filter out txns not matching search", () => {
      const stmtsWithQueries = [
        mockStmtInsightEvent({ query: "select foo" }),
        mockStmtInsightEvent({ query: "hello" }),
      ];

      let filtered = filterStatementInsights(
        stmtsWithQueries,
        { app: "" },
        INTERNAL_APP_PREFIX,
        "foo",
      );
      expect(filtered.length).toEqual(1);

      filtered = filterStatementInsights(
        stmtsWithQueries,
        { app: "" },
        INTERNAL_APP_PREFIX,
        "hello",
      );
      expect(filtered.length).toEqual(1);

      filtered = filterStatementInsights(
        stmtsWithQueries,
        { app: "" },
        INTERNAL_APP_PREFIX,
        "no results",
      );
      expect(filtered.length).toEqual(0);

      filtered = filterStatementInsights(
        [
          ...stmtsWithQueries,
          mockStmtInsightEvent({
            transactionFingerprintID: "txn-fingerprint-id-11223344",
          }),
          mockStmtInsightEvent({
            transactionExecutionID: "txn-exec-id-11223344",
          }),
          mockStmtInsightEvent({ sessionID: "session-id-11223344" }),
          mockStmtInsightEvent({
            statementFingerprintID: "stmt-fingerprint-id-11223344",
          }),
          mockStmtInsightEvent({
            statementExecutionID: "stmt-exec-id-11223344",
          }),
        ],
        { app: "" },
        INTERNAL_APP_PREFIX,
        "11223344",
      );
      expect(filtered.length).toEqual(5);
    });

    it("should filter txns given a mix of requirements", () => {
      const stmtsMixed = [
        // This should be the only txn remaining.
        mockStmtInsightEvent({
          application: "myApp",
          query: "select foo",
        }),
        // No required search term.
        mockStmtInsightEvent({
          application: "myApp",
          query: "update bar",
        }),
        // No required app.
        mockStmtInsightEvent({
          query: "hello world",
        }),
        // Internal app should be filtered out.
        mockStmtInsightEvent({
          application: INTERNAL_APP_PREFIX,
          query: "select foo",
        }),
      ];

      const filtered = filterStatementInsights(
        stmtsMixed,
        { app: "myApp" },
        INTERNAL_APP_PREFIX,
        "select foo",
      );
      expect(filtered.length).toEqual(1);
    });
    it;
  });

  describe("getAppsFromStatementInsights", () => {
    const appNames = ["one", "two", "three"];
    const stmts = appNames.map(app =>
      mockStmtInsightEvent({ application: app }),
    );

    // Internal app name should all be consolidated to the prefix..
    stmts.push(
      mockStmtInsightEvent({
        application: "$ internal-name",
      }),
      mockStmtInsightEvent({
        application: "$ internal-another-app",
      }),
    );

    const appsFromStmts = getAppsFromStatementInsights(
      stmts,
      INTERNAL_APP_PREFIX,
    );
    expect(appsFromStmts.length).toEqual(appNames.length + 1);
    appNames.forEach(app => expect(appsFromStmts.includes(app)).toBeTruthy());
  });

  describe("getInsightsFromProblemsAndCauses", () => {
    const createTestCases = (execType: InsightExecEnum) => [
      {
        problem: "FailedExecution",
        causes: [InsightNameEnum.FAILED_EXECUTION],
        expectedInsights: [failedExecutionInsight(execType)],
      },
      {
        problem: "SlowExecution",
        causes: [InsightNameEnum.FAILED_EXECUTION],
        expectedInsights: [failedExecutionInsight(execType)],
      },
      {
        problem: "SlowExecution",
        causes: [],
        expectedInsights: [slowExecutionInsight(execType)],
      },
      {
        problem: "SlowExecution",
        causes: [
          InsightNameEnum.PLAN_REGRESSION,
          InsightNameEnum.SUBOPTIMAL_PLAN,
          InsightNameEnum.HIGH_RETRY_COUNT,
          InsightNameEnum.HIGH_CONTENTION,
        ],
        expectedInsights: [
          planRegressionInsight(execType),
          suboptimalPlanInsight(execType),
          highRetryCountInsight(execType),
          highContentionInsight(execType),
        ],
      },
      {
        problem: "random",
        causes: [InsightNameEnum.FAILED_EXECUTION],
        expectedInsights: [],
      },
    ];

    [InsightExecEnum.STATEMENT, InsightExecEnum.TRANSACTION].forEach(type => {
      createTestCases(type).forEach(tc => {
        const insights = getInsightsFromProblemsAndCauses(
          [tc.problem],
          tc.causes,
          type,
        );
        expect(insights.length).toEqual(tc.expectedInsights.length);
        insights.forEach((insight, i) => {
          expect(insight.name).toEqual(tc.expectedInsights[i].name);
          expect(insight.description).toEqual(
            tc.expectedInsights[i].description,
          );
        });
      });
    });
  });

  describe("mergeTxnInsightDetails", () => {
    const txnInsightFromOverview = mockTxnInsightEvent({
      insights: [slowExecutionInsight(InsightExecEnum.TRANSACTION)],
    });

    it("should choose txn details obj when it is present", () => {
      let merged = mergeTxnInsightDetails(
        txnInsightFromOverview,
        [],
        txnInsightDetailsMock,
      );
      expect(merged).toEqual(txnInsightDetailsMock);

      merged = mergeTxnInsightDetails(
        txnInsightFromOverview,
        null,
        txnInsightDetailsMock,
      );
      expect(merged).toEqual(txnInsightDetailsMock);
    });

    it("should merge details when cached txnInsightDetails obj not present", () => {
      const merged = mergeTxnInsightDetails(
        txnInsightFromOverview,
        [statementInsightMock],
        null,
      );
      expect(merged.txnDetails).toEqual(txnInsightFromOverview);
      expect(merged.statements).toContain(statementInsightMock);
      expect(merged.statements.length).toEqual(1);
    });
  });
});
