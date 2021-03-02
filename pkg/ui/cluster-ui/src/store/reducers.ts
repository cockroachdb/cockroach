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
