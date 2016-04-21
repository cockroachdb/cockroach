/// <reference path="../typings/main.d.ts" />

import * as React from "react";
import * as ReactDOM from "react-dom";
import { createStore, combineReducers, applyMiddleware } from "redux";
import { Provider } from "react-redux";
import { Router, Route, IndexRedirect, hashHistory } from "react-router";
import { syncHistoryWithStore, routerReducer } from "react-router-redux";
import thunk from "redux-thunk";

import nodesReducer from "./redux/nodes";

import Layout from "./containers/layout";
import { ClusterMain, ClusterTitle } from "./containers/cluster";
import { DatabasesMain, DatabasesTitle } from "./containers/databases";
import { HelpUsMain, HelpUsTitle } from "./containers/helpus";
import { NodesMain, NodesTitle } from "./containers/nodes";

const store = createStore(
  combineReducers({
    routing: routerReducer,
    nodes: nodesReducer,
  }),
  applyMiddleware(thunk)
);

// Connect react-router history with redux.
const history = syncHistoryWithStore(hashHistory, store);

ReactDOM.render(
  <Provider store={store}>
    <Router history={history}>
      <Route path="/" component={Layout}>
        <IndexRedirect to="cluster" />
        <Route path="cluster"
               components={{main: ClusterMain, title:ClusterTitle}}/>
        <Route path="nodes"
               components={{main: NodesMain, title:NodesTitle}}/>
        <Route path="databases"
               components={{main: DatabasesMain, title:DatabasesTitle}}/>
        <Route path="help-us/reporting"
               components={{main: HelpUsMain, title:HelpUsTitle}}/>
      </Route>
    </Router>
  </Provider>,
  document.getElementById("react-layout")
);
