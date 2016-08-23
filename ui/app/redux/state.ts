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
       * expects, because there's a an issue with the path
       * state.cachedData.nodes.data[...].metrics.field. Redux-dev-tools should
       * be using jsan, which should be able to handle circular references, but
       * something about the generated ProtoBufMap type is breaking it. It will
       * take some time to construct an example that appropriately demonstrates
       * why ProtoBufMap breaks jsan which will be necessary to file issues for
       * those libraries.
       *
       * TODO (maxlang): Create an example demonstrating ProtoBufMap breaking
       * jsan and file issues upstream.
       *
       * Current fix inspired by this suggestion for avoiding large blobs in
       * redux dev tools:
       * https://github.com/zalmoxisus/redux-devtools-extension/issues/159#issuecomment-231034408
       *
       * NOTE: This only affects environments with redux dev tools installed and
       * opened.
       */
      actionsFilter: (action: PayloadAction<any>): PayloadAction<any> => (/nodes/).test(action.type) ? {type: action.type, payload: []} : action,
      statesFilter: (state: AdminUIState): AdminUIState => {
        let clone = _.clone(state);
        // Filter out circular reference in nodes.data[...].metrics.field.
        if (state.cachedData.nodes.data) {
          clone.cachedData = _.clone(clone.cachedData);
          clone.cachedData.nodes = _.clone(clone.cachedData.nodes);
          clone.cachedData.nodes.data = [];
        }
        return clone;
      },
    }) : _.identity
  ) as StoreEnhancer<AdminUIState>
);

// Connect react-router history with redux.
export const history = syncHistoryWithStore(hashHistory, store);
