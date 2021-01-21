import { combineReducers } from "redux";
import { StatementsState, reducer as statements } from "./statements";
import { LocalStorageState, reducer as localStorage } from "./localStorage";
import {
  StatementDiagnosticsState,
  reducer as statementDiagnostics,
} from "./statementDiagnostics";
import { NodesState, reducer as nodes } from "./nodes";
import { LivenessState, reducer as liveness } from "./liveness";

export type AdminUiState = {
  statements: StatementsState;
  statementDiagnostics: StatementDiagnosticsState;
  localStorage: LocalStorageState;
  nodes: NodesState;
  liveness: LivenessState;
};

export type AppState = {
  adminUI: AdminUiState;
};

export const rootReducer = combineReducers<AdminUiState>({
  localStorage,
  statementDiagnostics,
  statements,
  nodes,
  liveness,
});
