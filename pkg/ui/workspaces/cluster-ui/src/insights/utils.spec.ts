// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";
import {
  mergeTxnContentionAndStmtInsights,
  filterTransactionInsights,
  getAppsFromTransactionInsights,
  filterStatementInsights,
  getAppsFromStatementInsights,
  getInsightsFromProblemsAndCauses,
  flattenTxnInsightsToStmts,
  mergeTxnInsightDetails,
  dedupInsights,
} from "./utils";
import {
  TxnInsightEvent,
  StmtInsightEvent,
  InsightNameEnum,
  failedExecutionInsight,
  FlattenedStmtInsightEvent,
  InsightExecEnum,
  TxnContentionInsightEvent,
  highContentionInsight,
  slowExecutionInsight,
  planRegressionInsight,
  suboptimalPlanInsight,
  highRetryCountInsight,
  BlockedContentionDetails,
  TxnInsightDetails,
} from "./types";

const INTERNAL_APP_PREFIX = "$ internal";

const txnContentionEventMock: TxnContentionInsightEvent = {
  transactionID: "execution",
  transactionFingerprintID: "fingerprint",
  queries: ["select 1"],
  insights: [highContentionInsight(InsightExecEnum.TRANSACTION)],
  startTime: moment(),
  contentionDuration: moment.duration(100, "millisecond"),
  contentionThreshold: 100,
  application: "sql_obs_fun_times",
  execType: InsightExecEnum.TRANSACTION,
};

const blockedContentionMock: BlockedContentionDetails = {
  collectionTimeStamp: moment(),
  blockingExecutionID: "execution",
  blockingTxnFingerprintID: "block",
  blockingQueries: ["select 1"],
  contendedKey: "key",
  schemaName: "schema",
  databaseName: "db",
  tableName: "table",
  indexName: "index",
  contentionTimeMs: 500,
};

function mockTxnContentionInsightEvent(
  fields: Partial<TxnContentionInsightEvent> = {},
): TxnContentionInsightEvent {
  return { ...txnContentionEventMock, ...fields };
}

const statementInsightMock: StmtInsightEvent = {
  statementExecutionID: "execution",
  statementFingerprintID: "fingerprint",
  startTime: moment(),
  isFullScan: false,
  elapsedTimeMillis: 100,
  totalContentionTime: moment.duration(100, "millisecond"),
  endTime: moment(),
  rowsRead: 4,
  rowsWritten: 1,
  causes: ["FailedExecution"],
  problem: "SlowExecution",
  query: "select 1",
  insights: [failedExecutionInsight(InsightExecEnum.STATEMENT)],
  indexRecommendations: [],
  planGist: "gist",
};

function mockStmtInsight(fields: Partial<StmtInsightEvent>) {
  return { ...statementInsightMock, ...fields };
}

function mockFlattenedStmtInsightEvent(
  fields: Partial<FlattenedStmtInsightEvent> = {},
): FlattenedStmtInsightEvent {
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
  contention: null,
  statementInsights: [statementInsightMock],
  insights: [failedExecutionInsight(InsightExecEnum.TRANSACTION)],
  queries: ["select 1"],
};

function mockTxnInsightEvent(
  fields: Partial<TxnInsightEvent> = {},
): TxnInsightEvent {
  return { ...txnInsightEventMock, ...fields };
}

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
        mockTxnInsightEvent({ queries: ["select foo", "update bar"] }),
        mockTxnInsightEvent({ queries: ["hello", "world", "foo"] }),
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
    });

    it("should filter txns given a mix of requirements", () => {
      const txnsMixed = [
        // This should be the only txn remaining.
        mockTxnInsightEvent({
          application: "myApp",
          queries: ["select foo"],
        }),
        // No required search term.
        mockTxnInsightEvent({ application: "myApp", queries: ["update bar"] }),
        // No required app.
        mockTxnInsightEvent({ queries: ["hello", "world", "select foo"] }),
        // Internal app should be filtered out.
        mockTxnInsightEvent({
          application: INTERNAL_APP_PREFIX,
          queries: ["select foo"],
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
      mockFlattenedStmtInsightEvent({ application: "hello" }),
      mockFlattenedStmtInsightEvent({ application: "world" }),
      mockFlattenedStmtInsightEvent({ application: "cat" }),
      mockFlattenedStmtInsightEvent({ application: "frog" }),
      mockFlattenedStmtInsightEvent({ application: "cockroach" }),
      mockFlattenedStmtInsightEvent({ application: "cockroach" }),
      mockFlattenedStmtInsightEvent({ application: "db" }),
      mockFlattenedStmtInsightEvent({ application: "db" }),
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
        mockFlattenedStmtInsightEvent({ application: "$ internal-my-app" }),
        mockFlattenedStmtInsightEvent({ application: "$ internal-my-app" }),
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
        mockFlattenedStmtInsightEvent({ query: "select foo" }),
        mockFlattenedStmtInsightEvent({ query: "hello" }),
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
    });

    it("should filter txns given a mix of requirements", () => {
      const stmtsMixed = [
        // This should be the only txn remaining.
        mockFlattenedStmtInsightEvent({
          application: "myApp",
          query: "select foo",
        }),
        // No required search term.
        mockFlattenedStmtInsightEvent({
          application: "myApp",
          query: "update bar",
        }),
        // No required app.
        mockFlattenedStmtInsightEvent({
          query: "hello world",
        }),
        // Internal app should be filtered out.
        mockFlattenedStmtInsightEvent({
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
      mockFlattenedStmtInsightEvent({ application: app }),
    );

    // Internal app name should all be consolidated to the prefix..
    stmts.push(
      mockFlattenedStmtInsightEvent({
        application: "$ internal-name",
      }),
      mockFlattenedStmtInsightEvent({
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
        causes: [InsightNameEnum.failedExecution],
        expectedInsights: [failedExecutionInsight(execType)],
      },
      {
        problem: "SlowExecution",
        causes: [InsightNameEnum.failedExecution],
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
          InsightNameEnum.planRegression,
          InsightNameEnum.suboptimalPlan,
          InsightNameEnum.highRetryCount,
          InsightNameEnum.highContention,
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
        causes: [InsightNameEnum.failedExecution],
        expectedInsights: [],
      },
    ];

    [InsightExecEnum.STATEMENT, InsightExecEnum.TRANSACTION].forEach(type => {
      createTestCases(type).forEach(tc => {
        const insights = getInsightsFromProblemsAndCauses(
          tc.problem,
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

  describe("flattenTxnInsightsToStmts", () => {
    // Mock transactions, where each txn will have 2 stmt insights
    // with problems and 2 with no problems.
    // The 2 with no problems should NOT be included in the
    // flattened array.
    const txns = new Array(4).map((_, i) =>
      mockTxnInsightEvent({
        transactionExecutionID: `exec${i}`,
        statementInsights: [
          mockStmtInsight({ statementExecutionID: `exec${i * 2}` }),
          mockStmtInsight({ statementExecutionID: `exec${i * 2 + 1}` }),
          mockStmtInsight({ insights: [] }), // should be excluded
          mockStmtInsight({ insights: [] }), // should be excluded
        ],
      }),
    );

    const numStmts = txns.reduce(
      (sum, txn) => (sum += txn.statementInsights.length),
      0,
    );

    const flattened = flattenTxnInsightsToStmts(txns);
    expect(flattened.length).toEqual(numStmts);

    txns.forEach((txn, ti) => {
      txn.statementInsights.forEach((stmt, si) => {
        const flattenedStmt = flattened[ti * 2 + si];
        expect(stmt.statementExecutionID).toEqual(
          flattenedStmt.statementExecutionID,
        );
        expect(stmt.elapsedTimeMillis).toEqual(
          flattenedStmt.statementExecutionID,
        );
        expect(stmt.startTime.unix()).toEqual(flattenedStmt.startTime.unix());
        expect(stmt.endTime.unix()).toEqual(flattenedStmt.endTime.unix());
        expect(txn.transactionFingerprintID).toEqual(
          flattenedStmt.transactionExecutionID,
        );
        expect(txn.transactionExecutionID).toEqual(
          flattenedStmt.transactionExecutionID,
        );
        expect(txn.sessionID).toEqual(flattenedStmt.sessionID);
        expect(txn.application).toEqual(flattenedStmt.application);
        expect(txn.databaseName).toEqual(flattenedStmt.databaseName);
        expect(txn.implicitTxn).toEqual(flattenedStmt.implicitTxn);
        expect(txn.username).toEqual(flattenedStmt.username);
        expect(txn.priority).toEqual(flattenedStmt.priority);
        expect(txn.retries).toEqual(flattenedStmt.retries);
      });
    });
  });

  describe("mergeTxnContentionAndStmtInsights", () => {
    const txnInsights = [
      mockTxnInsightEvent({
        transactionExecutionID: "hello",
        transactionFingerprintID: "world",
        insights: [...txnContentionEventMock.insights],
      }),
      mockTxnInsightEvent({
        transactionExecutionID: "cockroach",
        transactionFingerprintID: "labs",
      }),
    ];

    const txnContentionInsights = [
      mockTxnContentionInsightEvent({
        // This entry should be merged with above.
        transactionID: "hello",
        transactionFingerprintID: "world",
        insights: txnContentionEventMock.insights,
      }),
      mockTxnContentionInsightEvent({
        transactionID: "just",
        transactionFingerprintID: "by myself",
      }),
    ];

    const merged = mergeTxnContentionAndStmtInsights(
      txnInsights,
      txnContentionInsights,
    );

    expect(merged.length).toEqual(3);

    const mergedTxn = merged.find(
      txn => txn.transactionExecutionID === "hello",
    );

    expect(mergedTxn.contention.asMilliseconds()).toEqual(
      txnContentionEventMock.contentionDuration.asMilliseconds(),
    );

    // These fields are not available on the contention event but are on
    // the txn insight event from stmts.
    expect(mergedTxn.sessionID).toEqual(txnInsightEventMock.sessionID);
    expect(mergedTxn.databaseName).toEqual(txnInsightEventMock.databaseName);
    expect(mergedTxn.username).toEqual(txnInsightEventMock.username);
    expect(mergedTxn.priority).toEqual(txnInsightEventMock.priority);
    expect(mergedTxn.retries).toEqual(txnInsightEventMock.retries);
    expect(mergedTxn.implicitTxn).toEqual(txnInsightEventMock.implicitTxn);
    expect(mergedTxn.sessionID).toEqual(txnInsightEventMock.sessionID);

    // Check insights are de-duplicated.
    expect(mergedTxn.insights.length).toEqual(1);
  });

  describe("mergeTxnInsightDetails", () => {
    const txnInsightFromStmts = mockTxnInsightEvent({
      insights: [slowExecutionInsight(InsightExecEnum.TRANSACTION)],
    });
    const txnContentionDetails = {
      transactionExecutionID: txnInsightEventMock.transactionExecutionID,
      queries: txnInsightEventMock.queries,
      insights: [
        highContentionInsight(InsightExecEnum.TRANSACTION),
        slowExecutionInsight(InsightExecEnum.TRANSACTION),
      ],
      startTime: moment(),
      totalContentionTimeMs: 500,
      contentionThreshold: 100,
      application: txnInsightEventMock.application,
      transactionFingerprintID: txnInsightEventMock.transactionFingerprintID,
      blockingContentionDetails: [blockedContentionMock],
      execType: InsightExecEnum.TRANSACTION,
      insightName: "HighContention",
    };

    const testMergedAgainstContentionFields = (merged: TxnInsightDetails) => {
      expect(merged.startTime.unix()).toEqual(
        txnContentionDetails.startTime.unix(),
      );
      expect(merged.contentionThreshold).toEqual(
        txnContentionDetails.contentionThreshold,
      );
      expect(merged.blockingContentionDetails).toEqual(
        txnContentionDetails.blockingContentionDetails,
      );
      expect(merged.totalContentionTimeMs).toEqual(
        txnContentionDetails.totalContentionTimeMs,
      );
    };

    const testMergedAgainstTxnFromInsights = (merged: TxnInsightDetails) => {
      expect(merged.databaseName).toEqual(txnInsightFromStmts.databaseName);
      expect(merged.retries).toEqual(txnInsightFromStmts.retries);
      expect(merged.implicitTxn).toEqual(txnInsightFromStmts.implicitTxn);
      expect(merged.priority).toEqual(txnInsightFromStmts.priority);
      expect(merged.username).toEqual(txnInsightFromStmts.username);
      expect(merged.sessionID).toEqual(txnInsightFromStmts.sessionID);
    };

    it("should merge objects when both are present", () => {
      const merged = mergeTxnInsightDetails(
        txnInsightFromStmts,
        txnContentionDetails,
      );
      testMergedAgainstContentionFields(merged);
      testMergedAgainstTxnFromInsights(merged);
      // Insights should be de-duped
      const insightNamesUniqe = new Set(
        txnContentionDetails.insights
          .map(i => i.name)
          .concat(txnInsightFromStmts.insights.map(i => i.name)),
      );
      expect(merged.insights.length).toEqual(insightNamesUniqe.size);
    });

    it("should return details when contention details aren't present", () => {
      const merged = mergeTxnInsightDetails(txnInsightFromStmts, null);
      testMergedAgainstTxnFromInsights(merged);
      expect(merged.insights.length).toBe(txnInsightFromStmts.insights.length);
    });

    it("should return details when txn insights from stmts aren't present", () => {
      const merged = mergeTxnInsightDetails(null, txnContentionDetails);
      testMergedAgainstContentionFields(merged);
      expect(merged.insights.length).toBe(txnContentionDetails.insights.length);
    });
  });

  describe("dedupInsights", () => {
    const e = InsightExecEnum.STATEMENT;
    const insights = [
      highContentionInsight(e),
      highRetryCountInsight(e),
      highRetryCountInsight(e),
      slowExecutionInsight(e),
      slowExecutionInsight(e),
      highRetryCountInsight(e),
    ];
    const expected = [
      highContentionInsight(e),
      highRetryCountInsight(e),
      slowExecutionInsight(e),
    ];

    const deduped = dedupInsights(insights);
    expect(deduped.length).toEqual(expected.length);
    deduped.forEach((insight, i) => {
      expect(insight.name).toEqual(expected[i].name);
    });
  });
});
