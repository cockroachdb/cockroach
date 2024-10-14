// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { configureStore } from "@reduxjs/toolkit";
import { connectRouter } from "connected-react-router";
import { createMemoryHistory } from "history";
import React from "react";
import { Provider } from "react-redux";

import { apiReducersReducer } from "src/redux/apiReducers";
import { hoverReducer } from "src/redux/hover";
import { localSettingsReducer } from "src/redux/localsettings";
import { loginReducer } from "src/redux/login";
import { metricsReducer } from "src/redux/metrics";
import { queryManagerReducer } from "src/redux/queryManager/reducer";
import { AdminUIState, flagsReducer } from "src/redux/state";
import { timeScaleReducer } from "src/redux/timeScale";
import { uiDataReducer } from "src/redux/uiData";

import type { PreloadedState } from "@reduxjs/toolkit";

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
      // TODO (koorosh): cannot properly cast Query Manager Action types to AnyAction.
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
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
