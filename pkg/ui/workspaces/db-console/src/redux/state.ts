// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import {
  createStore,
  combineReducers,
  applyMiddleware,
  compose,
  Store,
  Action,
} from "redux";
import createSagaMiddleware from "redux-saga";
import thunk, { ThunkDispatch } from "redux-thunk";
import {
  connectRouter,
  routerMiddleware,
  RouterState,
} from "connected-react-router";
import { createHashHistory, History } from "history";

import { apiReducersReducer, APIReducersState } from "./apiReducers";
import { hoverReducer, HoverState } from "./hover";
import { localSettingsReducer, LocalSettingsState } from "./localsettings";
import { metricsReducer, MetricsState } from "./metrics";
import { queryManagerReducer, QueryManagerState } from "./queryManager/reducer";
import { timeScaleReducer, TimeScaleState } from "./timeScale";
import { uiDataReducer, UIDataState } from "./uiData";
import { loginReducer, LoginAPIState } from "./login";
import rootSaga from "./sagas";

export interface AdminUIState {
  cachedData: APIReducersState;
  hover: HoverState;
  localSettings: LocalSettingsState;
  metrics: MetricsState;
  queryManager: QueryManagerState;
  router: RouterState;
  timeScale: TimeScaleState;
  uiData: UIDataState;
  login: LoginAPIState;
}

const history = createHashHistory();

const routerReducer = connectRouter(history);

// createAdminUIStore is a function that returns a new store for the admin UI.
// It's in a function so it can be recreated as necessary for testing.
export function createAdminUIStore(historyInst: History<any>) {
  const sagaMiddleware = createSagaMiddleware();

  const s: Store<AdminUIState> = createStore(
    combineReducers<AdminUIState>({
      cachedData: apiReducersReducer,
      hover: hoverReducer,
      localSettings: localSettingsReducer,
      metrics: metricsReducer,
      queryManager: queryManagerReducer,
      router: routerReducer,
      timeScale: timeScaleReducer,
      uiData: uiDataReducer,
      login: loginReducer,
    }),
    compose(
      applyMiddleware(thunk, sagaMiddleware, routerMiddleware(historyInst)),
      // Support for redux dev tools
      // https://chrome.google.com/webstore/detail/redux-devtools/lmhkpmbekcpmknklioeibfkpmmfibljd
      (window as any).__REDUX_DEVTOOLS_EXTENSION__
        ? (window as any).__REDUX_DEVTOOLS_EXTENSION__({
            serialize: {
              options: {
                function: (_key: string, value: any): Object => {
                  if (value && value.toRaw) {
                    return value.toRaw();
                  }
                  return value;
                },
              },
            },
          })
        : _.identity,
    ),
  );

  sagaMiddleware.run(rootSaga);
  return s;
}

const store = createAdminUIStore(history);

export type AppDispatch = ThunkDispatch<AdminUIState, unknown, Action>;

export { history, store };
