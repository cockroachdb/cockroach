import { combineReducers } from "redux";
import { StatementsState, reducer as statements } from "./statements";
import { LocalStorageState, reducer as localStorage } from "./localStorage";
import {
  StatementDiagnosticsState,
  reducer as statementDiagnostics,
} from "./statementDiagnostics";

export type AdminUiState = {
  statements: StatementsState;
  statementDiagnostics: StatementDiagnosticsState;
  localStorage: LocalStorageState;
};

export type AppState = {
  adminUI: AdminUiState;
};

export const rootReducer = combineReducers<AdminUiState>({
  localStorage,
  statementDiagnostics,
  statements,
});
