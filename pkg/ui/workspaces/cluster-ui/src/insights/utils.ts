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
import { ActiveStatementFilters } from "src/activeExecutions";
import {
  TransactionInsightsResponse,
  TransactionInsightState,
} from "src/api/insightsApi";
import {
  Insight,
  InsightExecEnum,
  InsightTypes,
  TransactionInsight,
} from "./types";

export type InsightEventFilters = Omit<
  ActiveStatementFilters,
  "username" | "sessionStatus"
>;

export const getInsights = (
  insightState: TransactionInsightState,
): Insight[] => {
  const insights: Insight[] = [];
  InsightTypes.forEach(insight => {
    if (insight.check(insightState)) {
      insights.push(insight.insight());
    }
  });
  console.log(insights);
  return insights;
};

export function getInsightsFromState(
  transactionInsightsResponse: TransactionInsightsResponse,
): TransactionInsight[] {
  const transactionInsights: TransactionInsight[] = [];
  if (!transactionInsightsResponse || transactionInsightsResponse?.length < 0) {
    return transactionInsights;
  }

  transactionInsightsResponse.forEach(txn => {
    const insightsForTxn = getInsights(txn);
    if (insightsForTxn.length < 1) {
      console.log("no insights for txn");
      return;
    } else {
      transactionInsights.push({
        executionID: txn.executionID,
        query: txn.query,
        insights: insightsForTxn,
        startTime: txn.startTime,
        elapsedTime: txn.elapsedTime,
        application: txn.application,
        execType: InsightExecEnum.TRANSACTION,
      });
    }
  });

  return transactionInsights;
}

export const filterTransactionInsights = (
  transactions: TransactionInsight[] | null,
  filters: InsightEventFilters,
  internalAppNamePrefix: string,
  search?: string,
): TransactionInsight[] => {
  if (transactions == null) return [];

  let filteredTransactions = transactions;

  const isInternal = (txn: TransactionInsight) =>
    txn.application.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredTransactions = filteredTransactions.filter(
      (txn: TransactionInsight) => {
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
    filteredTransactions = filteredTransactions.filter(
      txn =>
        !search ||
        txn.executionID?.includes(search) ||
        txn.query?.includes(search),
    );
  }
  return filteredTransactions;
};

export function getAppsFromTransactionInsights(
  transactions: TransactionInsight[] | null,
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
