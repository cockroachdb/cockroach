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
      /**
       * HACK HACK HACK
       * The state object insn't currently serializeable, which redux dev tools
       * expects, because there's a circular reference which is causing
       * JSON.stringify to fail. The specific path with the circular reference
       * appears to be state.cachedData.nodes.data.metrics.field
       *
       * Fix inspired by this suggestion for avoiding large blobs in redux dev
       * tools: https://github.com/zalmoxisus/redux-devtools-extension/issues/159#issuecomment-231034408
       * TODO (maxlang): file issues upstream
       *
       * NOTE: this only affects environments with react dev tools installed
       */
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
