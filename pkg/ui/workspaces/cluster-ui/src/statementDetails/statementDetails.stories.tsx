// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import {
  ConnectedRouter,
  connectRouter,
  routerMiddleware,
} from "connected-react-router";
import { createMemoryHistory } from "history";
import React from "react";
import { Provider } from "react-redux";
import {
  applyMiddleware,
  combineReducers,
  compose,
  createStore,
  Store,
} from "redux";

import { AppState, rootReducer } from "../store";

import { StatementDetails } from "./statementDetails";
import { getStatementDetailsPropsFixture } from "./statementDetails.fixture";

const history = createMemoryHistory();
const routerReducer = connectRouter(history);

const store: Store<AppState> = createStore(
  combineReducers({
    router: routerReducer,
    adminUI: rootReducer,
  }),
  compose(
    applyMiddleware(routerMiddleware(history)),
    window.__REDUX_DEVTOOLS_EXTENSION__ &&
      window.__REDUX_DEVTOOLS_EXTENSION__(),
  ),
);

storiesOf("StatementDetails", module)
  .addDecorator(storyFn => (
    <Provider store={store}>
      <ConnectedRouter history={history}>{storyFn()}</ConnectedRouter>
    </Provider>
  ))
  .add("Overview tab", () => (
    <StatementDetails {...getStatementDetailsPropsFixture()} />
  ))
  .add("with VIEWACTIVITYREDACTED", () => {
    const props = getStatementDetailsPropsFixture();
    props.hasViewActivityRedactedRole = true;
    return <StatementDetails {...props} />;
  })
  .add("Diagnostics tab", () => {
    const props = getStatementDetailsPropsFixture();
    props.history.location.search = new URLSearchParams([
      ["tab", "diagnostics"],
    ]).toString();
    return <StatementDetails {...props} />;
  })
  .add("Diagnostics tab with hidden Statement Diagnostics link", () => {
    const props = getStatementDetailsPropsFixture();
    props.history.location.search = new URLSearchParams([
      ["tab", "diagnostics"],
    ]).toString();
    props.uiConfig.showStatementDiagnosticsLink = false;
    return <StatementDetails {...props} />;
  })
  .add("Explain Plan tab", () => {
    const props = getStatementDetailsPropsFixture();
    props.history.location.search = new URLSearchParams([
      ["tab", "explain-plan"],
    ]).toString();
    return <StatementDetails {...props} />;
  })
  .add("Execution Stats tab", () => {
    const props = getStatementDetailsPropsFixture();
    props.history.location.search = new URLSearchParams([
      ["tab", "execution-stats"],
    ]).toString();
    return <StatementDetails {...props} />;
  })
  .add("Loading", () => {
    const props = getStatementDetailsPropsFixture();
    props.statementDetails = null;
    props.isLoading = true;
    return <StatementDetails {...props} />;
  })
  .add(
    "No data for this time frame; has statement cached from previous time frame",
    () => {
      const props = getStatementDetailsPropsFixture(false);
      return <StatementDetails {...props} />;
    },
  )
  .add("No data for this time frame; no cached statement", () => {
    const props = getStatementDetailsPropsFixture(false);
    return <StatementDetails {...props} />;
  });
