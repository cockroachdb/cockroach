/// <reference path="../typings/main.d.ts" />

import * as React from "react";
import * as ReactDOM from "react-dom";
import { createStore, combineReducers, applyMiddleware, compose } from "redux";
import { Provider } from "react-redux";
import { Router, Route, IndexRedirect, hashHistory } from "react-router";
import { syncHistoryWithStore, routerReducer } from "react-router-redux";
import thunk from "redux-thunk";

import nodesReducer from "./redux/nodes";
import uiReducer from "./redux/ui";

import Layout from "./containers/layout";
import { ClusterMain, ClusterTitle } from "./containers/cluster";
import { DatabasesMain, DatabasesTitle } from "./containers/databases";
import { HelpUsMain, HelpUsTitle } from "./containers/helpus";
import { NodesMain, NodesTitle } from "./containers/nodes";

// TODO(mrtracy): Redux now provides official typings, and their Store
// definition is generic. That would let us enforce that the store actually has
// the shape of the AdminUIStore interface (defined in /interfaces/store.d.ts).
// However, that typings file is currently incompatible with any available
// typings for react-redux.
const store = createStore(
  combineReducers({
    routing: routerReducer,
    nodes: nodesReducer,
    ui: uiReducer,
  }),
  compose(
    applyMiddleware(thunk),
    // Support for redux dev tools
    // https://chrome.google.com/webstore/detail/redux-devtools/lmhkpmbekcpmknklioeibfkpmmfibljd
    (window as any).devToolsExtension ? (window as any).devToolsExtension() : (f: any): any => f
  )
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
