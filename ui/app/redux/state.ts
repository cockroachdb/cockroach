import _ = require("lodash");
import { createStore, combineReducers, applyMiddleware, compose, StoreEnhancer } from "redux";
import { hashHistory } from "react-router";
import { syncHistoryWithStore, routerReducer } from "react-router-redux";
import thunk from "redux-thunk";

import uiReducer from "./ui";
import uiDataReducer from "./uiData";
import metricsReducer from "./metrics";
import timeWindowReducer from "./timewindow";
import databaseInfoReducer from "./databaseInfo";
import { apiReducers } from "./apiReducers";

const routerReducerState = routerReducer(null, null);
const uiReducerState = uiReducer(null, null);
const uiDataReducerState = uiDataReducer(null, null);
const metricsReducerState = metricsReducer(null, null);
const timeWindowReducerState = timeWindowReducer(null, null);
const databaseInfoReducerState = databaseInfoReducer(null, null);
const apiReducersState = apiReducers(null, null);

export interface AdminUIState {
    routing: typeof routerReducerState;
    ui: typeof uiReducerState;
    uiData: typeof uiDataReducerState;
    metrics: typeof metricsReducerState;
    timewindow: typeof timeWindowReducerState;
    databaseInfo: typeof databaseInfoReducerState;
    cachedData: typeof apiReducersState;
}

export const store = createStore<AdminUIState>(
  combineReducers<AdminUIState>({
    routing: routerReducer,
    ui: uiReducer,
    uiData: uiDataReducer,
    metrics: metricsReducer,
    timewindow: timeWindowReducer,
    databaseInfo: databaseInfoReducer,
    cachedData: apiReducers,
  }),
  compose(
    applyMiddleware(thunk),
    // Support for redux dev tools
    // https://chrome.google.com/webstore/detail/redux-devtools/lmhkpmbekcpmknklioeibfkpmmfibljd
    (window as any).devToolsExtension ? (window as any).devToolsExtension() : _.identity
  ) as StoreEnhancer<AdminUIState>
);

// Connect react-router history with redux.
export const history = syncHistoryWithStore(hashHistory, store);
