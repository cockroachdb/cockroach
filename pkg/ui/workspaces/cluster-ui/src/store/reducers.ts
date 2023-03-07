// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { combineReducers, createStore } from "redux";
import { createAction, createReducer } from "@reduxjs/toolkit";
import { LocalStorageState, reducer as localStorage } from "./localStorage";
import { NodesState, reducer as nodes } from "./nodes";
import { LivenessState, reducer as liveness } from "./liveness";
import { SessionsState, reducer as sessions } from "./sessions";
import { DOMAIN_NAME } from "./utils";
import { SQLStatsState, reducer as sqlStats } from "./sqlStats";
import {
  IndexStatsReducerState,
  reducer as indexStats,
} from "./indexStats/indexStats.reducer";
import { JobsState, reducer as jobs } from "./jobs";
import { JobDetailsReducerState, reducer as job } from "./jobDetails";
import {
  ClusterLocksReqState,
  reducer as clusterLocks,
} from "./clusterLocks/clusterLocks.reducer";
import {
  TransactionInsightsState,
  reducer as transactionInsights,
} from "./insights/transactionInsights";
import {
  StatementInsightsState,
  reducer as statementInsights,
} from "./insights/statementInsights";
import {
  SchemaInsightsState,
  reducer as schemaInsights,
} from "./schemaInsights";
import {
  reducer as transactionInsightDetails,
  TransactionInsightDetailsCachedState,
} from "./insightDetails/transactionInsightDetails";
import {
  reducer as sqlDetailsStats,
  SQLDetailsStatsReducerState,
} from "./statementDetails";
import {
  reducer as statementDiagnostics,
  StatementDiagnosticsState,
} from "./statementDiagnostics";
import {
  reducer as terminateQuery,
  TerminateQueryState,
} from "./terminateQuery";
import { reducer as uiConfig, UIConfigState } from "./uiConfig";
import { reducer as txnStats, TxnStatsState } from "./transactionStats";

export type AdminUiState = {
  statementDiagnostics: StatementDiagnosticsState;
  localStorage: LocalStorageState;
  nodes: NodesState;
  liveness: LivenessState;
  sessions: SessionsState;
  terminateQuery: TerminateQueryState;
  uiConfig: UIConfigState;
  statements: SQLStatsState;
  transactions: TxnStatsState;
  sqlDetailsStats: SQLDetailsStatsReducerState;
  indexStats: IndexStatsReducerState;
  jobs: JobsState;
  job: JobDetailsReducerState;
  clusterLocks: ClusterLocksReqState;
  transactionInsights: TransactionInsightsState;
  transactionInsightDetails: TransactionInsightDetailsCachedState;
  statementInsights: StatementInsightsState;
  schemaInsights: SchemaInsightsState;
};

export type AppState = {
  adminUI: AdminUiState;
};

export const reducers = combineReducers<AdminUiState>({
  localStorage,
  statementDiagnostics,
  nodes,
  liveness,
  sessions,
  transactionInsights,
  transactionInsightDetails,
  statementInsights,
  terminateQuery,
  uiConfig,
  statements: sqlStats,
  transactions: txnStats,
  sqlDetailsStats,
  indexStats,
  jobs,
  job,
  clusterLocks,
  schemaInsights,
});

export const rootActions = {
  resetState: createAction(`${DOMAIN_NAME}/RESET_STATE`),
};

/**
 * rootReducer consolidates reducers slices and cases for handling global actions related to entire state.
 **/
export const rootReducer = createReducer(undefined, builder => {
  builder
    .addCase(rootActions.resetState, () => createStore(reducers).getState())
    .addDefaultCase(reducers);
});
