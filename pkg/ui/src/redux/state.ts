// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import { hashHistory } from "react-router";
import { syncHistoryWithStore, routerMiddleware, routerReducer, RouterState } from "react-router-redux";
import { createStore, combineReducers, applyMiddleware, compose, GenericStoreEnhancer, Store } from "redux";
import createSagaMiddleware from "redux-saga";
import thunk from "redux-thunk";

import { apiReducersReducer, APIReducersState } from "./apiReducers";
import { hoverReducer, HoverState } from "./hover";
import { localSettingsReducer, LocalSettingsState } from "./localsettings";
import { metricsReducer, MetricsState, queryMetricsSaga } from "./metrics";
import { queryManagerReducer, QueryManagerState } from "./queryManager/reducer";
import { timeWindowReducer, TimeWindowState } from "./timewindow";
import { uiDataReducer, UIDataState } from "./uiData";
import { loginReducer, LoginAPIState } from "./login";

export interface AdminUIState {
    cachedData: APIReducersState;
    hover: HoverState;
    localSettings: LocalSettingsState;
    metrics: MetricsState;
    queryManager: QueryManagerState;
    routing: RouterState;
    timewindow: TimeWindowState;
    uiData: UIDataState;
    login: LoginAPIState;
}

// createAdminUIStore is a function that returns a new store for the admin UI.
// It's in a function so it can be recreated as necessary for testing.
export function createAdminUIStore() {
  const sagaMiddleware = createSagaMiddleware();

  const s: Store<AdminUIState> = createStore(
    combineReducers<AdminUIState>({
      cachedData: apiReducersReducer,
      hover: hoverReducer,
      localSettings: localSettingsReducer,
      metrics: metricsReducer,
      queryManager: queryManagerReducer,
      routing: routerReducer,
      timewindow: timeWindowReducer,
      uiData: uiDataReducer,
      login: loginReducer,
    }),
    compose(
      applyMiddleware(thunk, sagaMiddleware, routerMiddleware(hashHistory)),
      // Support for redux dev tools
      // https://chrome.google.com/webstore/detail/redux-devtools/lmhkpmbekcpmknklioeibfkpmmfibljd
      (window as any).devToolsExtension ? (window as any).devToolsExtension({
        // TODO(maxlang): implement {,de}serializeAction.
        // TODO(maxlang): implement deserializeState.
        serializeState: (_key: string, value: any): Object => {
          if (value && value.toRaw) {
            return value.toRaw();
          }
          return value;
        },
      }) : _.identity,
    ) as GenericStoreEnhancer,
  );

  sagaMiddleware.run(queryMetricsSaga);
  return s;
}

export const store = createAdminUIStore();

// Connect react-router history with redux.
export const history = syncHistoryWithStore(hashHistory, store);
