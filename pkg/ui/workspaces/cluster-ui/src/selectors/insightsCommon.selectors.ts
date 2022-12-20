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
  mergeTxnInsightDetails,
  StmtInsightEvent,
  TxnInsightEvent,
  MergedTxnInsightEvent,
  TxnContentionInsightDetails,
  TxnInsightDetails,
} from "src/insights";

export const selectStatementInsightDetailsCombiner = (
  statementInsights: StmtInsightEvent[],
  executionID: string,
): StmtInsightEvent | null => {
  if (!statementInsights || statementInsights?.length < 1 || !executionID) {
    return null;
  }

  return statementInsights.find(
    insight => insight.statementExecutionID === executionID,
  );
};

export const selectTxnInsightsCombiner = (
  txnInsightsFromStmts: TxnInsightEvent[],
): MergedTxnInsightEvent[] => {
  // Merge the two insights lists.
  return txnInsightsFromStmts;
};

export const selectTxnInsightDetailsCombiner = (
  txnInsightsFromStmts: TxnInsightEvent,
  txnContentionInsights: TxnContentionInsightDetails,
): TxnInsightDetails => {
  return mergeTxnInsightDetails(txnInsightsFromStmts, txnContentionInsights);
};
