// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createAction, createReducer } from "@reduxjs/toolkit";
import { combineReducers, createStore } from "redux";
import {
  ClusterLocksReqState,
  reducer as clusterLocks,
} from "./clusterLocks/clusterLocks.reducer";
import {
  DatabasesListState,
  reducer as databasesList,
} from "./databasesList/databasesList.reducers";
import {
  IndexStatsReducerState,
  reducer as indexStats,
} from "./indexStats/indexStats.reducer";
import {
  reducer as txnInsightDetails,
  TxnInsightDetailsCachedState,
} from "./insightDetails/transactionInsightDetails";
import {
  StmtInsightsState,
  reducer as stmtInsights,
} from "./insights/statementInsights";
import {
  TxnInsightsState,
  reducer as txnInsights,
} from "./insights/transactionInsights";
import { JobDetailsReducerState, reducer as job } from "./jobDetails";
import { JobsState, reducer as jobs } from "./jobs";
import { LivenessState, reducer as liveness } from "./liveness";
import { LocalStorageState, reducer as localStorage } from "./localStorage";
import { NodesState, reducer as nodes } from "./nodes";
import {
  reducer as schemaInsights,
  SchemaInsightsState,
} from "./schemaInsights";
import { reducer as sessions, SessionsState } from "./sessions";
import { reducer as sqlStats, SQLStatsState } from "./sqlStats";
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
import { DOMAIN_NAME } from "./utils";
import {
  reducer as statementFingerprintInsights,
  StatementFingerprintInsightsCachedState,
} from "./insights/statementFingerprintInsights";
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
  databasesList: DatabasesListState;
  stmtInsights: StmtInsightsState;
  txnInsightDetails: TxnInsightDetailsCachedState;
  txnInsights: TxnInsightsState;
  schemaInsights: SchemaInsightsState;
  statementFingerprintInsights: StatementFingerprintInsightsCachedState;
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
  txnInsightDetails,
  stmtInsights,
  txnInsights,
  terminateQuery,
  uiConfig,
  statements: sqlStats,
  transactions: txnStats,
  sqlDetailsStats,
  indexStats,
  jobs,
  job,
  clusterLocks,
  databasesList,
  schemaInsights,
  statementFingerprintInsights,
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
