// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createReducer } from "@reduxjs/toolkit";
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
  reducer as stmtInsights,
  StmtInsightsState,
} from "./insights/statementInsights";
import {
  reducer as txnInsights,
  TxnInsightsState,
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
import {
  reducer as statementFingerprintInsights,
  StatementFingerprintInsightsCachedState,
} from "./insights/statementFingerprintInsights";
import { reducer as txnStats, TxnStatsState } from "./transactionStats";
import {
  ClusterSettingsState,
  reducer as clusterSettings,
} from "./clusterSettings/clusterSettings.reducer";
import {
  KeyedDatabaseDetailsState,
  KeyedDatabaseDetailsSpanStatsState,
  databaseDetailsReducer,
  databaseDetailsSpanStatsReducer,
} from "./databaseDetails";
import {
  KeyedTableDetailsState,
  reducer as tableDetails,
} from "./databaseTableDetails/tableDetails.reducer";
import {
  JobProfilerExecutionDetailFilesState,
  reducer as executionDetailFiles,
} from "./jobs/jobProfiler.reducer";
import { rootActions } from "./rootActions";

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
  executionDetailFiles: JobProfilerExecutionDetailFilesState;
  clusterLocks: ClusterLocksReqState;
  databasesList: DatabasesListState;
  databaseDetails: KeyedDatabaseDetailsState;
  databaseDetailsSpanStats: KeyedDatabaseDetailsSpanStatsState;
  tableDetails: KeyedTableDetailsState;
  stmtInsights: StmtInsightsState;
  txnInsightDetails: TxnInsightDetailsCachedState;
  txnInsights: TxnInsightsState;
  schemaInsights: SchemaInsightsState;
  statementFingerprintInsights: StatementFingerprintInsightsCachedState;
  clusterSettings: ClusterSettingsState;
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
  executionDetailFiles,
  clusterLocks,
  databasesList,
  databaseDetails: databaseDetailsReducer.reducer,
  databaseDetailsSpanStats: databaseDetailsSpanStatsReducer.reducer,
  tableDetails,
  schemaInsights,
  statementFingerprintInsights,
  clusterSettings,
});

/**
 * rootReducer consolidates reducers slices and cases for handling global actions related to entire state.
 **/
export const rootReducer = createReducer(undefined, builder => {
  builder
    .addCase(rootActions.resetState, () => createStore(reducers).getState())
    .addDefaultCase(reducers);
});
