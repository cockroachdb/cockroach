// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { unset } from "src/util";
import {
  StatementInsights,
  TransactionInsightEventDetailsResponse,
  TransactionInsightEventDetailsState,
  TransactionInsightEventsResponse,
  TransactionInsightEventState,
} from "src/api/insightsApi";
import {
  getInsightFromProblem,
  Insight,
  InsightExecEnum,
  InsightNameEnum,
  InsightRecommendation,
  InsightType,
  InsightTypes,
  SchemaInsightEventFilters,
  StatementInsightEvent,
  TransactionInsightEvent,
  TransactionInsightEventDetails,
  WorkloadInsightEventFilters,
} from "./types";

export const getTransactionInsights = (
  eventState:
    | TransactionInsightEventState
    | TransactionInsightEventDetailsState,
): Insight[] => {
  const insights: Insight[] = [];
  InsightTypes.forEach(insight => {
    if (
      insight(eventState.execType, eventState.contentionThreshold).name ==
      eventState.insightName
    ) {
      insights.push(
        insight(eventState.execType, eventState.contentionThreshold),
      );
    }
  });
  return insights;
};

export function getInsightsFromState(
  insightEventsResponse: TransactionInsightEventsResponse,
): TransactionInsightEvent[] {
  const insightEvents: TransactionInsightEvent[] = [];
  if (!insightEventsResponse || insightEventsResponse?.length < 0) {
    return insightEvents;
  }

  insightEventsResponse.forEach(e => {
    const insightsForEvent = getTransactionInsights(e);
    if (insightsForEvent.length < 1) {
      return;
    } else {
      insightEvents.push({
        transactionID: e.transactionID,
        fingerprintID: e.fingerprintID,
        queries: e.queries,
        insights: insightsForEvent,
        startTime: e.startTime,
        contentionDuration: e.contentionDuration,
        application: e.application,
        execType: InsightExecEnum.TRANSACTION,
        contentionThreshold: e.contentionThreshold,
      });
    }
  });

  return insightEvents;
}

export function getTransactionInsightEventDetailsFromState(
  insightEventDetailsResponse: TransactionInsightEventDetailsResponse,
): TransactionInsightEventDetails {
  let insightEventDetails: TransactionInsightEventDetails = null;
  const insightsForEventDetails = getTransactionInsights(
    insightEventDetailsResponse[0],
  );
  if (insightsForEventDetails.length > 0) {
    delete insightEventDetailsResponse[0].insightName;
    insightEventDetails = {
      ...insightEventDetailsResponse[0],
      insights: insightsForEventDetails,
    };
  }
  return insightEventDetails;
}

export const filterTransactionInsights = (
  transactions: TransactionInsightEvent[] | null,
  filters: WorkloadInsightEventFilters,
  internalAppNamePrefix: string,
  search?: string,
): TransactionInsightEvent[] => {
  if (transactions == null) return [];

  let filteredTransactions = transactions;

  const isInternal = (txn: TransactionInsightEvent) =>
    txn.application.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredTransactions = filteredTransactions.filter(
      (txn: TransactionInsightEvent) => {
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
      },
    );
  } else {
    filteredTransactions = filteredTransactions.filter(txn => !isInternal(txn));
  }
  if (search) {
    search = search.toLowerCase();
    filteredTransactions = filteredTransactions.filter(
      txn =>
        !search ||
        txn.transactionID.toLowerCase()?.includes(search) ||
        txn.queries?.find(query => query.toLowerCase().includes(search)),
    );
  }
  return filteredTransactions;
};

export function getAppsFromTransactionInsights(
  transactions: TransactionInsightEvent[] | null,
  internalAppNamePrefix: string,
): string[] {
  if (transactions == null) return [];

  const uniqueAppNames = new Set(
    transactions.map(t => {
      if (t.application.startsWith(internalAppNamePrefix)) {
        return internalAppNamePrefix;
      }
      return t.application ? t.application : unset;
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
      return "Create New Index";
    case "DropIndex":
      return "Drop Unused Index";
    case "ReplaceIndex":
      return "Replace Index";
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
      return "Insight";
  }
}

export const filterStatementInsights = (
  statements: StatementInsights | null,
  filters: WorkloadInsightEventFilters,
  internalAppNamePrefix: string,
  search?: string,
): StatementInsights => {
  if (statements == null) return [];

  let filteredStatements = statements;

  const isInternal = (appName: string) =>
    appName.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredStatements = filteredStatements.filter(
      (stmt: StatementInsightEvent) => {
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
  if (search) {
    search = search.toLowerCase();
    filteredStatements = filteredStatements.filter(
      stmt =>
        !search ||
        stmt.statementID.toLowerCase()?.includes(search) ||
        stmt.query?.toLowerCase().includes(search),
    );
  }
  return filteredStatements;
};

export function getAppsFromStatementInsights(
  statements: StatementInsights | null,
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

export function populateStatementInsightsFromProblemAndCauses(
  statements: StatementInsightEvent[],
): void {
  if (!statements || statements?.length === 0) {
    return;
  }
  statements.map(x => {
    // TODO(ericharmeling,todd): Replace these strings when using the insights protos.
    if (x.problem === "SlowExecution") {
      if (x.causes?.length === 0) {
        x.insights = [
          getInsightFromProblem(
            InsightNameEnum.slowExecution,
            InsightExecEnum.STATEMENT,
          ),
        ];
      } else {
        x.insights = x.causes?.map(x =>
          getInsightFromProblem(x, InsightExecEnum.STATEMENT),
        );
      }
    } else if (x.problem === "FailedExecution") {
      x.insights = [
        getInsightFromProblem(
          InsightNameEnum.failedExecution,
          InsightExecEnum.STATEMENT,
        ),
      ];
    }
  });
}
