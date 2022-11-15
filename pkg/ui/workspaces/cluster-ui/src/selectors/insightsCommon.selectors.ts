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
  TxnInsightDetails,
} from "src/insights";

// The functions in this file are agnostic to the different shape of each
// state in db-console and cluster-ui. This file contains selector functions
// and combiners that can be reused across both packages.

export const selectStatementInsightDetailsCombiner = (
  statementInsights: StmtInsightEvent[],
  executionID: string,
): StmtInsightEvent | null => {
  if (!statementInsights || !executionID) {
    return null;
  }
  return statementInsights.find(
    insight => insight.statementExecutionID === executionID,
  );
};

export const selectStatementInsightDetailsCombinerByFingerprint = (
  statementInsights: StmtInsightEvent[],
  fingerprintID: string,
): StmtInsightEvent[] | null => {
  if (!statementInsights?.length || !fingerprintID) {
    return null;
  }
  const insightEvents = statementInsights.filter(
    insight => insight.statementFingerprintID === fingerprintID,
  );
  return insightEvents;
};

export const selectTxnInsightDetailsCombiner = (
  txnInsights: TxnInsightEvent,
  txnInsightsDetails: TxnInsightDetails,
  stmtInsights: StmtInsightEvent[] | null,
): TxnInsightDetails => {
  return mergeTxnInsightDetails(txnInsights, stmtInsights, txnInsightsDetails);
};
