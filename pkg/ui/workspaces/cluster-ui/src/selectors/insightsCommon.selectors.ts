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
  StatementInsights,
  TransactionInsightEventsResponse,
} from "src/api/insightsApi";
import {
  StatementInsightEvent,
  populateStatementInsightsFromProblemAndCauses,
} from "src/insights";

// The functions in this file are agnostic to the different shape of each
// state in db-console and cluster-ui. This file contains selector functions
// and combiners that can be reused across both packages.

export const selectStatementInsightsCombiner = (
  statementInsights: StatementInsights,
): StatementInsights => {
  if (!statementInsights) return [];
  return populateStatementInsightsFromProblemAndCauses(statementInsights);
};

export const selectStatementInsightDetailsCombiner = (
  statementInsights: StatementInsights,
  executionID: string,
): StatementInsightEvent | null => {
  if (!statementInsights) {
    return null;
  }

  return statementInsights.find(insight => insight.statementID === executionID);
};

export const selectTxnInsightsCombiner = (
  txnInsights: TransactionInsightEventsResponse,
): TransactionInsightEventsResponse => {
  if (!txnInsights) return [];
  return txnInsights;
};
