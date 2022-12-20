// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { limitStringArray, unset } from "src/util";
import { FlattenedStmtInsights } from "src/api/insightsApi";
import {
  ExecutionDetails,
  FlattenedStmtInsightEvent,
  getInsightFromCause,
  Insight,
  InsightExecEnum,
  InsightNameEnum,
  InsightRecommendation,
  InsightType,
  MergedTxnInsightEvent,
  SchemaInsightEventFilters,
  StatementInsightEvent,
  TxnContentionInsightDetails,
  TxnContentionInsightEvent,
  TxnInsightDetails,
  TxnInsightEvent,
  WorkloadInsightEventFilters,
} from "./types";

export const filterTransactionInsights = (
  transactions: MergedTxnInsightEvent[] | null,
  filters: WorkloadInsightEventFilters,
  internalAppNamePrefix: string,
  search?: string,
): MergedTxnInsightEvent[] => {
  if (transactions == null) return [];

  let filteredTransactions = transactions;

  const isInternal = (txn: { application?: string }) =>
    txn?.application?.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredTransactions = filteredTransactions.filter(txn => {
      const apps = filters.app.toString().split(",");
      let showInternal = false;
      if (apps.includes(internalAppNamePrefix)) {
        showInternal = true;
      }
      if (apps.includes(unset)) {
        apps.push("");
      }

      return (
        (showInternal && isInternal(txn)) || apps.includes(txn.application)
      );
    });
  } else {
    filteredTransactions = filteredTransactions.filter(txn => !isInternal(txn));
  }
  if (filters.workloadInsightType) {
    const workloadInsightTypes = filters.workloadInsightType
      .toString()
      .split(",");
    filteredTransactions = filteredTransactions.filter(
      transactionInsight =>
        workloadInsightTypes.length === 0 ||
        workloadInsightTypes.includes(
          transactionInsight.insights.map(insight => insight.label).toString(),
        ),
    );
  }
  if (search) {
    search = search.toLowerCase();

    filteredTransactions = filteredTransactions.filter(
      txn =>
        txn.transactionExecutionID.toLowerCase()?.includes(search) ||
        limitStringArray(txn.queries, 300).toLowerCase().includes(search),
    );
  }
  return filteredTransactions;
};

export function getAppsFromTransactionInsights(
  transactions: MergedTxnInsightEvent[] | null,
  internalAppNamePrefix: string,
): string[] {
  if (transactions == null) return [];

  const uniqueAppNames = new Set(
    transactions.map(t => {
      if (t?.application.startsWith(internalAppNamePrefix)) {
        return internalAppNamePrefix;
      }
      return t?.application ? t.application : unset;
    }),
  );

  return Array.from(uniqueAppNames).sort();
}

export const filterSchemaInsights = (
  schemaInsights: InsightRecommendation[],
  filters: SchemaInsightEventFilters,
  search?: string,
): InsightRecommendation[] => {
  if (schemaInsights == null) return [];

  let filteredSchemaInsights = schemaInsights;

  if (filters.database) {
    const databases =
      filters.database.toString().length > 0
        ? filters.database.toString().split(",")
        : [];
    if (databases.includes(unset)) {
      databases.push("");
    }
    filteredSchemaInsights = filteredSchemaInsights.filter(
      schemaInsight =>
        databases.length === 0 || databases.includes(schemaInsight.database),
    );
  }

  if (filters.schemaInsightType) {
    const schemaInsightTypes =
      filters.schemaInsightType.toString().length > 0
        ? filters.schemaInsightType.toString().split(",")
        : [];
    if (schemaInsightTypes.includes(unset)) {
      schemaInsightTypes.push("");
    }
    filteredSchemaInsights = filteredSchemaInsights.filter(
      schemaInsight =>
        schemaInsightTypes.length === 0 ||
        schemaInsightTypes.includes(insightType(schemaInsight.type)),
    );
  }

  if (search) {
    search = search.toLowerCase();
    filteredSchemaInsights = filteredSchemaInsights.filter(
      schemaInsight =>
        schemaInsight.query?.toLowerCase().includes(search) ||
        schemaInsight.indexDetails?.indexName?.toLowerCase().includes(search) ||
        schemaInsight.execution?.statement.toLowerCase().includes(search) ||
        schemaInsight.execution?.summary.toLowerCase().includes(search) ||
        schemaInsight.execution?.fingerprintID.toLowerCase().includes(search),
    );
  }
  return filteredSchemaInsights;
};

export function insightType(type: InsightType): string {
  switch (type) {
    case "CreateIndex":
      return "Create Index";
    case "DropIndex":
      return "Drop Unused Index";
    case "ReplaceIndex":
      return "Replace Index";
    case "AlterIndex":
      return "Alter Index";
    case "HighContention":
      return "High Contention";
    case "HighRetryCount":
      return "High Retry Counts";
    case "SuboptimalPlan":
      return "Suboptimal Plan";
    case "PlanRegression":
      return "Plan Regression";
    case "FailedExecution":
      return "Failed Execution";
    default:
      return "Slow Execution";
  }
}

export const filterStatementInsights = (
  statements: FlattenedStmtInsights | null,
  filters: WorkloadInsightEventFilters,
  internalAppNamePrefix: string,
  search?: string,
): FlattenedStmtInsights => {
  if (statements == null) return [];

  let filteredStatements = statements;

  const isInternal = (appName: string) =>
    appName.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredStatements = filteredStatements.filter(
      (stmt: FlattenedStmtInsightEvent) => {
        const apps = filters.app.toString().split(",");
        let showInternal = false;
        if (apps.includes(internalAppNamePrefix)) {
          showInternal = true;
        }
        if (apps.includes(unset)) {
          apps.push("");
        }

        return (
          (showInternal && isInternal(stmt.application)) ||
          apps.includes(stmt.application)
        );
      },
    );
  } else {
    filteredStatements = filteredStatements.filter(
      stmt => !isInternal(stmt.application),
    );
  }
  if (filters.workloadInsightType && filters.workloadInsightType.length > 0) {
    const workloadInsightTypes = filters.workloadInsightType
      .toString()
      .split(",");

    filteredStatements = filteredStatements.filter(statementInsight =>
      statementInsight.insights.some(stmtInsight =>
        workloadInsightTypes.some(
          workloadType => workloadType === stmtInsight.label,
        ),
      ),
    );
  }
  if (search) {
    search = search.toLowerCase();
    filteredStatements = filteredStatements.filter(
      stmt =>
        !search ||
        stmt.statementExecutionID.toLowerCase()?.includes(search) ||
        stmt.query?.toLowerCase().includes(search),
    );
  }
  return filteredStatements;
};

export function getAppsFromStatementInsights(
  statements: FlattenedStmtInsights | null,
  internalAppNamePrefix: string,
): string[] {
  if (statements == null || statements?.length === 0) return [];

  const uniqueAppNames = new Set(
    statements.map(t => {
      if (t.application.startsWith(internalAppNamePrefix)) {
        return internalAppNamePrefix;
      }
      return t.application ? t.application : unset;
    }),
  );

  return Array.from(uniqueAppNames).sort();
}

/**
 * getInsightsFromProblemsAndCauses returns a list of insight objects with
 * labels and descriptions based on the problem, causes for the problem, and
 * the execution type.
 * @param problem the problem with the query e.g. SlowExecution, should be a InsightNameEnum
 * @param causes an array of strings detailing the causes for the problem, if known
 * @param execType execution type
 * @returns list of insight objects
 */
export function getInsightsFromProblemsAndCauses(
  problem: string,
  causes: string[] | null,
  execType: InsightExecEnum,
): Insight[] {
  // TODO(ericharmeling,todd): Replace these strings when using the insights protos.
  const insights: Insight[] = [];

  switch (problem) {
    case "SlowExecution":
      causes?.forEach(cause =>
        insights.push(getInsightFromCause(cause, execType)),
      );

      if (insights.length === 0) {
        insights.push(
          getInsightFromCause(InsightNameEnum.slowExecution, execType),
        );
      }
      break;

    case "FailedExecution":
      insights.push(
        getInsightFromCause(InsightNameEnum.failedExecution, execType),
      );
      break;

    default:
  }

  return insights;
}

/**
 * flattenTxnInsightsToStmts flattens the txn insights array
 * into its stmt insights, including the txn level ifnormation.
 * Only stmts with non-empty insights array will be included.
 * @param txnInsights array of transaction insights
 * @returns An array of FlattenedStmtInsightEvent where each elem
 * includes stmt and txn info. All elements have a non-empty
 * insights array.
 */
export function flattenTxnInsightsToStmts(
  txnInsights: TxnInsightEvent[],
): FlattenedStmtInsightEvent[] {
  if (!txnInsights?.length) return [];
  const stmtInsights: FlattenedStmtInsightEvent[] = [];
  txnInsights.forEach(txnInsight => {
    const { statementInsights, ...txnInfo } = txnInsight;
    statementInsights?.forEach(stmt => {
      if (!stmt.insights?.length) return;
      stmtInsights.push({ ...txnInfo, ...stmt, query: stmt.query });
    });
  });
  return stmtInsights;
}

/**
 * mergeTxnContentionAndStmtInsights merges a list of txn insights
 * aggregated from stmt insights, and a list of txn contention insights.
 * If a txn exists in both lists, its information will be merged.
 * @param txnInsightsFromStmts txn insights aggregated from stmts
 * @param txnContentionInsights txn contention insights
 * @returns list of merged txn insights
 */
export function mergeTxnContentionAndStmtInsights(
  txnInsightsFromStmts: TxnInsightEvent[],
  txnContentionInsights: TxnContentionInsightEvent[],
): MergedTxnInsightEvent[] {
  const eventByTxnFingerprint: Record<string, MergedTxnInsightEvent> = {};
  txnContentionInsights?.forEach(txn => {
    const formattedTxn = {
      transactionExecutionID: txn.transactionID,
      transactionFingerprintID: txn.transactionFingerprintID,
      contention: txn.contentionDuration,
      statementInsights: [] as StatementInsightEvent[],
      insights: txn.insights,
      queries: txn.queries,
      startTime: txn.startTime,
      application: txn.application,
    };
    eventByTxnFingerprint[txn.transactionFingerprintID] = formattedTxn;
  });

  txnInsightsFromStmts?.forEach(txn => {
    const existingContentionEvent =
      eventByTxnFingerprint[txn.transactionFingerprintID];
    if (existingContentionEvent) {
      if (
        existingContentionEvent.transactionExecutionID !==
        txn.transactionExecutionID
      ) {
        // Not the same execution - for now we opt to return the contention event.
        // TODO (xinhaoz) return the txn that executed more recently once
        // we have txn start and end in the insights table. For now let's
        // take the entry from the contention registry.
        return; // Continue
      }
      // Merge the two results.
      eventByTxnFingerprint[txn.transactionFingerprintID] = {
        ...txn,
        contention: existingContentionEvent.contention,
        startTime: existingContentionEvent.startTime,
        insights: dedupInsights(
          txn.insights.concat(existingContentionEvent.insights),
        ),
      };
      return; // Continue
    }

    // This is a new key.
    eventByTxnFingerprint[txn.transactionFingerprintID] = txn;
  });

  return Object.values(eventByTxnFingerprint);
}

export function mergeTxnInsightDetails(
  txnDetailsFromStmts: TxnInsightEvent | null,
  txnContentionDetails: TxnContentionInsightDetails | null,
): TxnInsightDetails {
  if (!txnContentionDetails)
    return txnDetailsFromStmts
      ? { ...txnDetailsFromStmts, execType: InsightExecEnum.TRANSACTION }
      : null;

  // Merge info from txnDetailsFromStmts, if it exists.
  return {
    transactionExecutionID: txnContentionDetails.transactionExecutionID,
    transactionFingerprintID: txnContentionDetails.transactionFingerprintID,
    application:
      txnContentionDetails.application ?? txnDetailsFromStmts?.application,
    lastRetryReason: txnDetailsFromStmts?.lastRetryReason,
    sessionID: txnDetailsFromStmts?.sessionID,
    retries: txnDetailsFromStmts?.retries,
    databaseName: txnDetailsFromStmts?.databaseName,
    implicitTxn: txnDetailsFromStmts?.implicitTxn,
    username: txnDetailsFromStmts?.username,
    priority: txnDetailsFromStmts?.priority,
    statementInsights: txnDetailsFromStmts?.statementInsights,
    insights: dedupInsights(
      txnContentionDetails.insights.concat(txnDetailsFromStmts?.insights ?? []),
    ),
    queries: txnContentionDetails.queries,
    startTime: txnContentionDetails.startTime,
    blockingContentionDetails: txnContentionDetails.blockingContentionDetails,
    contentionThreshold: txnContentionDetails.contentionThreshold,
    totalContentionTimeMs: txnContentionDetails.totalContentionTimeMs,
    execType: InsightExecEnum.TRANSACTION,
  };
}

export function getRecommendationForExecInsight(
  insight: Insight,
  execDetails: ExecutionDetails | null,
): InsightRecommendation {
  switch (insight.name) {
    case InsightNameEnum.highContention:
      return {
        type: InsightNameEnum.highContention,
        execution: execDetails,
        details: {
          duration: execDetails.contentionTime,
          description: insight.description,
        },
      };
    case InsightNameEnum.failedExecution:
      return {
        type: InsightNameEnum.failedExecution,
        execution: execDetails,
      };
    case InsightNameEnum.highRetryCount:
      return {
        type: InsightNameEnum.highRetryCount,
        execution: execDetails,
        details: {
          description: insight.description,
        },
      };
    case InsightNameEnum.planRegression:
      return {
        type: InsightNameEnum.planRegression,
        execution: execDetails,
        details: {
          description: insight.description,
        },
      };
    case InsightNameEnum.suboptimalPlan:
      return {
        type: InsightNameEnum.suboptimalPlan,
        database: execDetails.databaseName,
        execution: execDetails,
        details: {
          description: insight.description,
        },
      };
    default:
      return {
        type: "Unknown",
        execution: execDetails,
        details: {
          duration: execDetails.elapsedTimeMillis,
          description: insight.description,
        },
      };
  }
}

export function getStmtInsightRecommendations(
  insightDetails: Partial<FlattenedStmtInsightEvent> | null,
): InsightRecommendation[] {
  if (!insightDetails) return [];

  const execDetails: ExecutionDetails = {
    statement: insightDetails.query,
    fingerprintID: insightDetails.statementFingerprintID,
    retries: insightDetails.retries,
    indexRecommendations: insightDetails.indexRecommendations,
    databaseName: insightDetails.databaseName,
    elapsedTimeMillis: insightDetails.elapsedTimeMillis,
    contentionTime: insightDetails.totalContentionTime?.asMilliseconds(),
  };

  const recs: InsightRecommendation[] = insightDetails.insights?.map(insight =>
    getRecommendationForExecInsight(insight, execDetails),
  );

  return recs;
}

export function getTxnInsightRecommendations(
  insightDetails: TxnInsightDetails | null,
): InsightRecommendation[] {
  if (!insightDetails) return [];

  const execDetails: ExecutionDetails = {
    retries: insightDetails.retries,
    databaseName: insightDetails.databaseName,
    contentionTime: insightDetails.totalContentionTimeMs,
  };
  const recs: InsightRecommendation[] = [];

  insightDetails.statementInsights?.forEach(stmt =>
    getStmtInsightRecommendations({
      ...stmt,
      ...execDetails,
    })?.forEach(rec => recs.push(rec)),
  );

  // This is necessary since txn contention insight currently is not
  // surfaced from the  stmt level for txns.
  if (recs.length === 0) {
    insightDetails.insights?.forEach(insight =>
      recs.push(getRecommendationForExecInsight(insight, execDetails)),
    );
  }

  return recs;
}

export function dedupInsights(insights: Insight[]): Insight[] {
  // De-duplicate top-level txn insights.
  const insightsSeen = new Set<string>();
  return insights.reduce((deduped, i) => {
    if (insightsSeen.has(i.name)) return deduped;
    insightsSeen.add(i.name);
    deduped.push(i);
    return deduped;
  }, []);
}
