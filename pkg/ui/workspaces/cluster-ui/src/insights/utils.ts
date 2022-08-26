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
  InsightEventDetailsResponse,
  InsightEventDetailsState,
  InsightEventsResponse,
  InsightEventState,
  StatementInsights,
} from "src/api/insightsApi";
import {
  getInsightFromProblem,
  Insight,
  InsightEvent,
  InsightEventDetails,
  InsightEventFilters,
  InsightExecEnum,
  InsightTypes,
  SchemaInsightEventFilters,
  InsightType,
  InsightRecommendation,
  StatementInsightEvent,
} from "./types";

export const getInsights = (
  eventState: InsightEventState | InsightEventDetailsState,
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
  insightEventsResponse: InsightEventsResponse,
): InsightEvent[] {
  const insightEvents: InsightEvent[] = [];
  if (!insightEventsResponse || insightEventsResponse?.length < 0) {
    return insightEvents;
  }

  insightEventsResponse.forEach(e => {
    const insightsForEvent = getInsights(e);
    if (insightsForEvent.length < 1) {
      return;
    } else {
      insightEvents.push({
        transactionID: e.transactionID,
        fingerprintID: e.fingerprintID,
        queries: e.queries,
        insights: insightsForEvent,
        startTime: e.startTime,
        elapsedTimeMillis: e.elapsedTimeMillis,
        application: e.application,
        execType: InsightExecEnum.TRANSACTION,
        contentionThreshold: e.contentionThreshold,
      });
    }
  });

  return insightEvents;
}

export function getInsightEventDetailsFromState(
  insightEventDetailsResponse: InsightEventDetailsResponse,
): InsightEventDetails {
  let insightEventDetails: InsightEventDetails = null;
  const insightsForEventDetails = getInsights(insightEventDetailsResponse[0]);
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
  transactions: InsightEvent[] | null,
  filters: InsightEventFilters,
  internalAppNamePrefix: string,
  search?: string,
): InsightEvent[] => {
  if (transactions == null) return [];

  let filteredTransactions = transactions;

  const isInternal = (txn: InsightEvent) =>
    txn.application.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredTransactions = filteredTransactions.filter((txn: InsightEvent) => {
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
  transactions: InsightEvent[] | null,
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
    case "CREATE_INDEX":
      return "Create New Index";
    case "DROP_INDEX":
      return "Drop Unused Index";
    case "REPLACE_INDEX":
      return "Replace Index";
    case "HighContentionTime":
      return "High Wait Time";
    case "HighRetryCount":
      return "High Retry Counts";
    case "SuboptimalPlan":
      return "Sub-Optimal Plan";
    case "FAILED":
      return "Failed Execution";
    default:
      return "Insight";
  }
}

export const filterStatementInsights = (
  statements: StatementInsights | null,
  filters: InsightEventFilters,
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

export function populateStatementInsightsFromProblems(
  statements: StatementInsightEvent[],
): void {
  if (!statements || statements?.length === 0) {
    return;
  }

  statements.map(x => {
    x.insights = x.problems?.map(x =>
      getInsightFromProblem(x, InsightExecEnum.STATEMENT),
    );
  });
}
