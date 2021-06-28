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
import { StatementsState, reducer as statements } from "./statements";
import { LocalStorageState, reducer as localStorage } from "./localStorage";
import {
  StatementDiagnosticsState,
  reducer as statementDiagnostics,
} from "./statementDiagnostics";
import { NodesState, reducer as nodes } from "./nodes";
import { LivenessState, reducer as liveness } from "./liveness";
import { SessionsState, reducer as sessions } from "./sessions";
import { TransactionsState, reducer as transactions } from "./transactions";
import {
  TerminateQueryState,
  reducer as terminateQuery,
} from "./terminateQuery";
import { UIConfigState, reducer as uiConfig } from "./uiConfig";
import { DOMAIN_NAME } from "./utils";

export type AdminUiState = {
  statements: StatementsState;
  statementDiagnostics: StatementDiagnosticsState;
  localStorage: LocalStorageState;
  nodes: NodesState;
  liveness: LivenessState;
  sessions: SessionsState;
  transactions: TransactionsState;
  terminateQuery: TerminateQueryState;
  uiConfig: UIConfigState;
};

export type AppState = {
  adminUI: AdminUiState;
};

export const reducers = combineReducers<AdminUiState>({
  localStorage,
  statementDiagnostics,
  statements,
  nodes,
  liveness,
  sessions,
  transactions,
  terminateQuery,
  uiConfig,
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
