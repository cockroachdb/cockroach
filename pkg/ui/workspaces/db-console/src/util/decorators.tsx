// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { RenderFunction } from "storybook__react";
import { Provider } from "react-redux";
import { combineReducers, createStore } from "redux";
import { ConnectedRouter, connectRouter } from "connected-react-router";
import { createMemoryHistory } from "history";

const history = createMemoryHistory();
const routerReducer = connectRouter(history);

const store = createStore(
  combineReducers({
    router: routerReducer,
  }),
);

export const styledWrapper =
  (styles: React.CSSProperties) => (storyFn: RenderFunction) =>
    <div style={styles}>{storyFn()}</div>;

export const withRouterDecorator = (storyFn: RenderFunction) => (
  <Provider store={store}>
    <ConnectedRouter history={history}>{storyFn()}</ConnectedRouter>
  </Provider>
);
