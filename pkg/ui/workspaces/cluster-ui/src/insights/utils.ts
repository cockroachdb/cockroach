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
  InsightEventsResponse,
  InsightEventState,
  InsightEventDetailsResponse,
  InsightEventDetailsState,
} from "src/api/insightsApi";
import {
  Insight,
  InsightExecEnum,
  InsightTypes,
  InsightEvent,
  InsightEventFilters,
  InsightEventDetails,
} from "./types";

export const getInsights = (
  eventState: InsightEventState | InsightEventDetailsState,
): Insight[] => {
  const insights: Insight[] = [];
  InsightTypes.forEach(insight => {
    if (insight(eventState.execType).name == eventState.insightName) {
      insights.push(insight(eventState.execType));
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
        executionID: e.executionID,
        queries: e.queries,
        insights: insightsForEvent,
        startTime: e.startTime,
        elapsedTime: e.elapsedTime,
        application: e.application,
        execType: InsightExecEnum.TRANSACTION,
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
        txn.executionID.toLowerCase()?.includes(search) ||
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
