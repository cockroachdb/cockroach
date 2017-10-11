import _ from "lodash";
import { hashHistory } from "react-router";
import { syncHistoryWithStore, routerReducer, RouterState } from "react-router-redux";
import { createStore, combineReducers, applyMiddleware, compose, GenericStoreEnhancer } from "redux";
import thunk from "redux-thunk";

import { apiReducersReducer, APIReducersState } from "./apiReducers";
import { localSettingsReducer, LocalSettingsState } from "./localsettings";
import { metricsReducer, MetricsState } from "./metrics";
import { timeWindowReducer, TimeWindowState } from "./timewindow";
import { uiDataReducer, UIDataState } from "./uiData";

export interface AdminUIState {
    cachedData: APIReducersState;
    localSettings: LocalSettingsState;
    metrics: MetricsState;
    routing: RouterState;
    timewindow: TimeWindowState;
    uiData: UIDataState;
}

// createAdminUIStore is a function that returns a new store for the admin UI.
// It's in a function so it can be recreated as necessary for testing.
export const createAdminUIStore = () => createStore(
  combineReducers<AdminUIState>({
    cachedData: apiReducersReducer,
    localSettings: localSettingsReducer,
    metrics: metricsReducer,
    routing: routerReducer,
    timewindow: timeWindowReducer,
    uiData: uiDataReducer,
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
