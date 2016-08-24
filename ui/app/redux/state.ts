import _ from "lodash";
import { createStore, combineReducers, applyMiddleware, compose, StoreEnhancer } from "redux";
import { hashHistory } from "react-router";
import { syncHistoryWithStore, routerReducer, IRouterState } from "react-router-redux";
import thunk from "redux-thunk";

import uiReducer from "./ui";
import * as ui from "./ui";
import uiDataReducer from "./uiData";
import * as uiData from "./uiData";
import metricsReducer from "./metrics";
import * as metrics from "./metrics";
import timeWindowReducer from "./timewindow";
import * as timewindow from "./timewindow";
import apiReducersReducer from "./apiReducers";
import * as apiReducers from "./apiReducers";

export interface AdminUIState {
    routing: IRouterState;
    ui: ui.UISettingsDict;
    uiData: uiData.UIDataSet;
    metrics: metrics.MetricQueryState;
    timewindow: timewindow.TimeWindowState;
    cachedData: apiReducers.APIReducersState;
}

export const store = createStore<AdminUIState>(
  combineReducers<AdminUIState>({
    routing: routerReducer,
    ui: uiReducer,
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
      serializeState: (key: string, value: any): Object => {
        if (value && value.toRaw) {
          return value.toRaw();
        }
        return value;
      },
    }) : _.identity
  ) as StoreEnhancer<AdminUIState>
);

// Connect react-router history with redux.
export const history = syncHistoryWithStore(hashHistory, store);
