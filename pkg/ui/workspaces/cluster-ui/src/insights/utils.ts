// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { unset } from "src/util";

import {
  ExecutionDetails,
  getInsightFromCause,
  Insight,
  InsightExecEnum,
  InsightNameEnum,
  InsightRecommendation,
  InsightType,
  SchemaInsightEventFilters,
  StmtInsightEvent,
  TxnInsightDetails,
  TxnInsightEvent,
  WorkloadInsightEventFilters,
} from "./types";

export const filterTransactionInsights = (
  transactions: TxnInsightEvent[] | null,
  filters: WorkloadInsightEventFilters,
  internalAppNamePrefix: string,
  search?: string,
): TxnInsightEvent[] => {
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
  if (filters.workloadInsightType && filters.workloadInsightType.length > 0) {
    const workloadInsightTypes = filters.workloadInsightType
      .toString()
      .split(",");

    filteredTransactions = filteredTransactions.filter(transaction =>
      workloadInsightTypes.some(workloadType =>
        transaction.insights.some(
          txnInsight => workloadType === txnInsight.label,
        ),
      ),
    );
  }
  if (search) {
    search = search.toLowerCase();

    filteredTransactions = filteredTransactions.filter(
      txn =>
        [
          txn.transactionExecutionID,
          txn.transactionFingerprintID,
          txn.sessionID,
        ]
          .concat(txn.stmtExecutionIDs)
          .some(id => id.toLowerCase()?.includes(search)) ||
        txn.query.toLowerCase().includes(search),
    );
  }
  return filteredTransactions;
};

export function getAppsFromTransactionInsights(
  transactions: TxnInsightEvent[] | null,
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
  statements: StmtInsightEvent[] | null,
  filters: WorkloadInsightEventFilters,
  internalAppNamePrefix: string,
  search?: string,
): StmtInsightEvent[] => {
  if (statements == null) return [];

  let filteredStatements = statements;

  const isInternal = (appName: string) =>
    appName.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredStatements = filteredStatements.filter((stmt: StmtInsightEvent) => {
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
    });
  } else {
    filteredStatements = filteredStatements.filter(
      stmt => !isInternal(stmt.application),
    );
  }
  if (filters.workloadInsightType && filters.workloadInsightType.length > 0) {
    const workloadInsightTypes = filters.workloadInsightType
      .toString()
      .split(",");

    filteredStatements = filteredStatements.filter(statement =>
      workloadInsightTypes.some(workloadType =>
        statement.insights.some(
          stmtInsight => workloadType === stmtInsight.label,
        ),
      ),
    );
  }

  if (search) {
    search = search.toLowerCase();
    filteredStatements = filteredStatements.filter(
      stmt =>
        !search ||
        // Search results in all ID columns of StmtInsightEvent.
        [
          stmt.sessionID,
          stmt.transactionExecutionID,
          stmt.transactionFingerprintID,
          stmt.statementExecutionID,
          stmt.statementFingerprintID,
        ].some(s => s.toLowerCase()?.includes(search)) ||
        stmt.query?.toLowerCase().includes(search),
    );
  }
  return filteredStatements;
};

export function getAppsFromStatementInsights(
  statements: StmtInsightEvent[] | null,
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
 * @param problems the array of problems with the query, should be a InsightNameEnum[]
 * @param causes an array of strings detailing the causes for the problem, if known
 * @param execType execution type
 * @returns list of insight objects
 */
export function getInsightsFromProblemsAndCauses(
  problems: string[],
  causes: string[] | null,
  execType: InsightExecEnum,
): Insight[] {
  // TODO(ericharmeling,todd): Replace these strings when using the insights protos.
  const insights: Insight[] = [];

  problems.forEach(problem => {
    switch (problem) {
      case "SlowExecution":
        causes?.forEach(cause =>
          insights.push(getInsightFromCause(cause, execType)),
        );

        if (insights.length === 0) {
          insights.push(
            getInsightFromCause(InsightNameEnum.SLOW_EXECUTION, execType),
          );
        }
        break;

      case "FailedExecution":
        insights.push(
          getInsightFromCause(InsightNameEnum.FAILED_EXECUTION, execType),
        );
        break;

      default:
    }
  });

  return insights;
}

export function mergeTxnInsightDetails(
  overviewDetails: TxnInsightEvent | null,
  stmtInsights: StmtInsightEvent[],
  txnInsightDetails: TxnInsightDetails | null,
): TxnInsightDetails {
  let statements: StmtInsightEvent[] = null;
  // If the insight details exists in the cache, return that since it
  // contains the full transaction information.
  // Otherwise, we'll attempt to build it from other cached data, to
  // avoid fetching all of the details since that can be expensive.
  if (txnInsightDetails) return txnInsightDetails;

  if (overviewDetails) {
    statements = stmtInsights?.filter(
      stmt =>
        stmt.transactionExecutionID === overviewDetails.transactionExecutionID,
    );
    if (!statements?.length) statements = null;
  }

  return {
    txnDetails: overviewDetails,
    statements,
  };
}

export function getRecommendationForExecInsight(
  insight: Insight,
  execDetails: ExecutionDetails | null,
): InsightRecommendation {
  switch (insight.name) {
    case InsightNameEnum.HIGH_CONTENTION:
      return {
        type: InsightNameEnum.HIGH_CONTENTION,
        execution: execDetails,
        details: {
          duration: execDetails.contentionTimeMs,
          description: insight.description,
        },
      };
    case InsightNameEnum.FAILED_EXECUTION:
      return {
        type: InsightNameEnum.FAILED_EXECUTION,
        execution: execDetails,
      };
    case InsightNameEnum.HIGH_RETRY_COUNT:
      return {
        type: InsightNameEnum.HIGH_RETRY_COUNT,
        execution: execDetails,
        details: {
          description: insight.description,
        },
      };
    case InsightNameEnum.PLAN_REGRESSION:
      return {
        type: InsightNameEnum.PLAN_REGRESSION,
        execution: execDetails,
        details: {
          description: insight.description,
        },
      };
    case InsightNameEnum.SUBOPTIMAL_PLAN:
      return {
        type: InsightNameEnum.SUBOPTIMAL_PLAN,
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
  insightDetails: Partial<StmtInsightEvent> | null,
): InsightRecommendation[] {
  if (!insightDetails) return [];

  const execDetails: ExecutionDetails = {
    application: insightDetails?.application,
    statement: insightDetails?.query,
    fingerprintID: insightDetails?.statementFingerprintID,
    retries: insightDetails?.retries,
    indexRecommendations: insightDetails?.indexRecommendations,
    databaseName: insightDetails?.databaseName,
    elapsedTimeMillis: insightDetails?.elapsedTimeMillis,
    contentionTimeMs: insightDetails?.contentionTime?.asMilliseconds(),
    statementExecutionID: insightDetails?.statementExecutionID,
    transactionExecutionID: insightDetails?.transactionExecutionID,
    execType: InsightExecEnum.STATEMENT,
    errorCode: insightDetails?.errorCode,
    errorMsg: insightDetails?.errorMsg,
    status: insightDetails?.status,
  };

  const recs: InsightRecommendation[] = insightDetails?.insights?.map(insight =>
    getRecommendationForExecInsight(insight, execDetails),
  );

  return recs;
}

export function getTxnInsightRecommendations(
  insightDetails: TxnInsightEvent | null,
): InsightRecommendation[] {
  if (!insightDetails) return [];

  const execDetails: ExecutionDetails = {
    application: insightDetails?.application,
    transactionExecutionID: insightDetails?.transactionExecutionID,
    retries: insightDetails?.retries,
    contentionTimeMs: insightDetails?.contentionTime.asMilliseconds(),
    elapsedTimeMillis: insightDetails?.elapsedTimeMillis,
    execType: InsightExecEnum.TRANSACTION,
    errorCode: insightDetails?.errorCode,
    errorMsg: insightDetails?.errorMsg,
  };
  const recs: InsightRecommendation[] = [];

  insightDetails?.insights?.forEach(insight =>
    recs.push(getRecommendationForExecInsight(insight, execDetails)),
  );
  return recs;
}
