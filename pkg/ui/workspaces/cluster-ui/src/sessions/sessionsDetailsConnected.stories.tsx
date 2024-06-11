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
import { storiesOf } from "@storybook/react";
import { createMemoryHistory } from "history";
import createSagaMiddleware from "redux-saga";
import { Provider } from "react-redux";
import {
  ConnectedRouter,
  connectRouter,
  routerMiddleware,
} from "connected-react-router";
import {
  applyMiddleware,
  combineReducers,
  compose,
  createStore,
  Store,
  StoreEnhancer,
} from "redux";
import { Route } from "react-router-dom";

import { AppState, sagas, rootReducer } from "../store";

import { SessionDetailsPageConnected } from "./sessionDetailsConnected";

const TEST_ID = "1673deaf-76af-ea86-0000-000000000001";

const history = createMemoryHistory({
  initialEntries: [`/session/${TEST_ID}`],
});

const routerReducer = connectRouter(history);
const sagaMiddleware = createSagaMiddleware();

const store: Store<AppState> = createStore(
  combineReducers({
    router: routerReducer,
    adminUI: rootReducer,
  }),
  compose(
    applyMiddleware(sagaMiddleware, routerMiddleware(history)),
    window.__REDUX_DEVTOOLS_EXTENSION__ &&
      (window.__REDUX_DEVTOOLS_EXTENSION__() as StoreEnhancer),
  ),
);

sagaMiddleware.run(sagas);

storiesOf("Sessions Details Page Connected", module)
  .addDecorator(storyFn => (
    <Provider store={store}>
      <ConnectedRouter history={history}>
        <Route path={"/session/:session"}>{storyFn()}</Route>
      </ConnectedRouter>
    </Provider>
  ))
  .addDecorator(storyFn => (
    <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn()}</div>
  ))
  .add("with data", () => <SessionDetailsPageConnected />);
