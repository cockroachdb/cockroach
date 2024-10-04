// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Provider } from "react-redux";
import { ConnectedRouter, connectRouter } from "connected-react-router";
import { createMemoryHistory } from "history";
import { createStore, combineReducers } from "redux";

const history = createMemoryHistory();
const routerReducer = connectRouter(history);

const store = createStore(
  combineReducers({
    router: routerReducer,
  }),
);

export const withRouterProvider = (storyFn: any) => (
  <Provider store={store}>
    <ConnectedRouter history={history}>{storyFn()}</ConnectedRouter>
  </Provider>
);
