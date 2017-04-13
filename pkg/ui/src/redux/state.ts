import _ from "lodash";
import { createStore, combineReducers, applyMiddleware, compose, GenericStoreEnhancer } from "redux";
import { hashHistory } from "react-router";
import { syncHistoryWithStore, routerReducer, RouterState } from "react-router-redux";
import thunk from "redux-thunk";

import localSettingsReducer from "./localsettings";
import * as localSettings from "./localsettings";
import uiDataReducer from "./uiData";
import * as uiData from "./uiData";
import metricsReducer from "./metrics";
import * as metrics from "./metrics";
import timeWindowReducer from "./timewindow";
import * as timewindow from "./timewindow";
import apiReducersReducer from "./apiReducers";
import * as apiReducers from "./apiReducers";

export interface AdminUIState {
    routing: RouterState;
    localSettings: localSettings.LocalSettingsDict;
    uiData: uiData.UIDataSet;
    metrics: metrics.MetricQueryState;
    timewindow: timewindow.TimeWindowState;
    cachedData: apiReducers.APIReducersState;
}

// createAdminUIStore is a function that returns a new store for the admin UI.
// It's in a function so it can be recreated as necessary for testing.
export const createAdminUIStore = () => createStore(
  combineReducers<AdminUIState>({
    routing: routerReducer,
    localSettings: localSettingsReducer,
    uiData: uiDataReducer,
    metrics: metricsReducer,
    timewindow: timeWindowReducer,
    cachedData: apiReducersReducer,
  }),
  compose(
    applyMiddleware(thunk),
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

export const store = createAdminUIStore();

// Connect react-router history with redux.
export const history = syncHistoryWithStore(hashHistory, store);
