// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createReducer } from "@reduxjs/toolkit";
import { combineReducers, createStore } from "redux";

import { LocalStorageState, reducer as localStorage } from "./localStorage";
import { rootActions } from "./rootActions";
import { reducer as sqlStats, SQLStatsState } from "./sqlStats";
import { reducer as uiConfig, UIConfigState } from "./uiConfig";

export type AdminUiState = {
  localStorage: LocalStorageState;
  uiConfig: UIConfigState;
  statements: SQLStatsState;
};

export type AppState = {
  adminUI: AdminUiState;
};

export const reducers = combineReducers<AdminUiState>({
  localStorage,
  uiConfig,
  statements: sqlStats,
});

/**
 * rootReducer consolidates reducers slices and cases for handling global actions related to entire state.
 **/
export const rootReducer = createReducer(undefined, builder => {
  builder
    .addCase(rootActions.resetState, () => createStore(reducers).getState())
    .addDefaultCase(reducers);
});
