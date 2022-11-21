// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { FlattenedStmtInsights } from "src/api/insightsApi";
import {
  mergeTxnInsightDetails,
  flattenTxnInsightsToStmts,
  FlattenedStmtInsightEvent,
  TxnInsightEvent,
  TxnContentionInsightEvent,
  MergedTxnInsightEvent,
  mergeTxnContentionAndStmtInsights,
  TxnContentionInsightDetails,
  TxnInsightDetails,
} from "src/insights";

// The functions in this file are agnostic to the different shape of each
// state in db-console and cluster-ui. This file contains selector functions
// and combiners that can be reused across both packages.

export const selectFlattenedStmtInsightsCombiner = (
  executionInsights: TxnInsightEvent[],
): FlattenedStmtInsights => {
  return flattenTxnInsightsToStmts(executionInsights);
};

export const selectStatementInsightDetailsCombiner = (
  statementInsights: FlattenedStmtInsights,
  executionID: string,
): FlattenedStmtInsightEvent | null => {
  if (!statementInsights || statementInsights?.length < 1 || !executionID) {
    return null;
  }

  return statementInsights.find(
    insight => insight.statementExecutionID === executionID,
  );
};

export const selectTxnInsightsCombiner = (
  txnInsightsFromStmts: TxnInsightEvent[],
  txnContentionInsights: TxnContentionInsightEvent[],
): MergedTxnInsightEvent[] => {
  if (!txnInsightsFromStmts && !txnContentionInsights) return [];

  // Merge the two insights lists.
  return mergeTxnContentionAndStmtInsights(
    txnInsightsFromStmts,
    txnContentionInsights,
  );
};

export const selectTxnInsightDetailsCombiner = (
  txnInsightsFromStmts: TxnInsightEvent,
  txnContentionInsights: TxnContentionInsightDetails,
): TxnInsightDetails => {
  return mergeTxnInsightDetails(txnInsightsFromStmts, txnContentionInsights);
};
