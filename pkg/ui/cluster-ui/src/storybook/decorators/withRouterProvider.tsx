// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
