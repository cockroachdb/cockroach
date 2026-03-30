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
  ClusterSettingsState,
  reducer as clusterSettings,
} from "./clusterSettings/clusterSettings.reducer";
import {
  DatabasesListState,
  reducer as databasesList,
} from "./databasesList/databasesList.reducers";
import {
  reducer as txnInsights,
  TxnInsightsState,
} from "./insights/transactionInsights";
import {
  LivenessState,
  reducer as liveness,
} from "./liveness/liveness.reducer";
import { LocalStorageState, reducer as localStorage } from "./localStorage";
import { NodesState, reducer as nodes } from "./nodes";
import { rootActions } from "./rootActions";
import { reducer as sqlStats, SQLStatsState } from "./sqlStats";
import {
  reducer as statementDiagnostics,
  StatementDiagnosticsState,
} from "./statementDiagnostics";
import { reducer as txnStats, TxnStatsState } from "./transactionStats";
import { reducer as uiConfig, UIConfigState } from "./uiConfig";

export type AdminUiState = {
  statementDiagnostics: StatementDiagnosticsState;
  localStorage: LocalStorageState;
  nodes: NodesState;
  liveness: LivenessState;
  uiConfig: UIConfigState;
  statements: SQLStatsState;
  transactions: TxnStatsState;
  clusterLocks: ClusterLocksReqState;
  databasesList: DatabasesListState;
  txnInsights: TxnInsightsState;
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
  txnInsights,
  uiConfig,
  statements: sqlStats,
  transactions: txnStats,
  clusterLocks,
  databasesList,
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
