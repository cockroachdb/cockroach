// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  Insight,
  InsightTransaction,
  InsightTypes,
  IExtendedContentionEvent,
} from "./types";
import { TransactionContentionEventsResponse } from "src/api/contentionApi";
import { DurationToNumber, TimestampToMoment, unset } from "../util";
import Long from "long";
import { ActiveTransactionFilters } from "../activeExecutions";
import { byteArrayToUuid } from "../sessions";

export type InsightsTransactionFilters = ActiveTransactionFilters;

export const getInsights = (event: IExtendedContentionEvent): Insight[] => {
  const insights = [];
  if (DurationToNumber(event.blocking_event.duration) > 0.2) {
    insights.push(InsightTypes()[0]);
  }
  return insights;
};

export function getInsightTransactionsFromContentionEvent(
  transactionContentionEventResponse: TransactionContentionEventsResponse,
): InsightTransaction[] {
  const insightTransactions: InsightTransaction[] = [];
  if (transactionContentionEventResponse?.events == null) {
    return insightTransactions;
  }

  transactionContentionEventResponse.events.forEach(event => {
    const insights = getInsights(event);
    if (insights.length < 1) {
      return;
    } else {
      insightTransactions.push({
        executionID: byteArrayToUuid(event.blocking_event.txn_meta.id),
        query: getQueryFromFingerprint(event.blocking_txn_fingerprint_id),
        insights: getInsights(event),
        startTime: TimestampToMoment(event.collection_ts),
        elapsedTime: DurationToNumber(event.blocking_event.duration) * 1000,
        application: getAppFromFingerprint(event.blocking_txn_fingerprint_id),
      });
    }
  });

  return insightTransactions;
}

export const filterInsightsTransactions = (
  transactions: InsightTransaction[] | null,
  filters: InsightsTransactionFilters,
  internalAppNamePrefix: string,
  search?: string,
): InsightTransaction[] => {
  if (transactions == null) return [];

  let filteredTransactions = transactions;

  const isInternal = (txn: InsightTransaction) =>
    txn.application.startsWith(internalAppNamePrefix);
  if (filters.app) {
    filteredTransactions = filteredTransactions.filter(
      (txn: InsightTransaction) => {
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
      txn => !search || txn.executionID?.includes(search),
    );
  }
  return filteredTransactions;
};

export function getAppsFromInsightsTransactions(
  transactions: InsightTransaction[] | null,
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

const getQueryFromFingerprint = (_fingerprintID: Long.Long): string => {
  return "Not implemented";
};

const getAppFromFingerprint = (_fingerprintID: Long.Long): string => {
  return "Not implemented";
};
