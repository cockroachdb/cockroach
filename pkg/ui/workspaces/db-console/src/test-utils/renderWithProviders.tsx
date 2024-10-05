// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { configureStore } from "@reduxjs/toolkit";
import type { PreloadedState } from "@reduxjs/toolkit";
import { Provider } from "react-redux";

import { AdminUIState, flagsReducer } from "src/redux/state";
import { createMemoryHistory } from "history";
import { apiReducersReducer } from "src/redux/apiReducers";
import { hoverReducer } from "src/redux/hover";
import { localSettingsReducer } from "src/redux/localsettings";
import { metricsReducer } from "src/redux/metrics";
import { queryManagerReducer } from "src/redux/queryManager/reducer";
import { timeScaleReducer } from "src/redux/timeScale";
import { uiDataReducer } from "src/redux/uiData";
import { loginReducer } from "src/redux/login";
import { connectRouter } from "connected-react-router";

export function renderWithProviders(
  element: React.ReactElement,
  preloadedState?: PreloadedState<AdminUIState>,
): React.ReactElement {
  const history = createMemoryHistory({
    initialEntries: ["/"],
  });
  const routerReducer = connectRouter(history);
  const store = configureStore<AdminUIState>({
    reducer: {
      cachedData: apiReducersReducer,
      hover: hoverReducer,
      localSettings: localSettingsReducer,
      metrics: metricsReducer,
      queryManager: queryManagerReducer,
      router: routerReducer,
      timeScale: timeScaleReducer,
      uiData: uiDataReducer,
      login: loginReducer,
      flags: flagsReducer,
    },
    preloadedState,
  });
  return <Provider store={store}>{element}</Provider>;
}
