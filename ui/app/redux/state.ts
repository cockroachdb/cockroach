import _ = require("lodash");
import { createStore, combineReducers, applyMiddleware, compose, StoreEnhancer } from "redux";
import { hashHistory } from "react-router";
import { syncHistoryWithStore, routerReducer, IRouterState } from "react-router-redux";
import thunk from "redux-thunk";

import uiReducer, * as ui from "./ui";
import uiDataReducer, * as uiData from "./uiData";
import metricsReducer, * as metrics from "./metrics";
import timeWindowReducer, * as timewindow from "./timewindow";
import apiReducersReducer, * as apiReducers from "./apiReducers";

import { PayloadAction } from "../interfaces/action";

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
      // The nodes state had a circular reference in it which was breaking redux devtools
      // TODO (maxlang): file issues upstream
      actionsFilter: (action: PayloadAction<any>): PayloadAction<any> => (/nodes/).test(action.type) ? {type: action.type, payload: "<<NODE_DATA>>"} : action,
      statesFilter: (state: AdminUIState): AdminUIState => {
        if (state.cachedData.nodes.data) {
          let clone = _.clone(state);
          clone.cachedData.nodes.data = <any>"<<NODE_DATA>>";
          return clone;
        }
        return state;
      },
    }) : _.identity
  ) as StoreEnhancer<AdminUIState>
);

// Connect react-router history with redux.
export const history = syncHistoryWithStore(hashHistory, store);
