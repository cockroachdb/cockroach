// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import {
  Action,
  Store,
  createStore,
  combineReducers,
  applyMiddleware,
} from "redux";
import { Provider } from "react-redux";
import {
  ConnectedRouter,
  connectRouter,
  routerMiddleware,
} from "connected-react-router";
import { createMemoryHistory } from "history";
import { AppState, rootReducer } from "src/store";

export const TestStoreProvider: React.FC = ({ children }) => {
  const history = createMemoryHistory({
    initialEntries: ["/"],
  });
  const routerReducer = connectRouter(history);
  const store: Store<AppState, Action> = createStore(
    combineReducers({
      adminUI: rootReducer,
      router: routerReducer,
    }),
    applyMiddleware(routerMiddleware(history)),
  );
  return (
    <Provider store={store}>
      <ConnectedRouter history={history}>{children}</ConnectedRouter>
    </Provider>
  );
};
