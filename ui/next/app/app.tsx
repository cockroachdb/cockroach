/// <reference path="../typings/main.d.ts" />

/**
 * UI/NEXT TODO LIST
 *
 * ! = Potentially difficult to implement
 *
 * - Visualization Components
 *    - Graphs
 *      - Greyed-out display on error
 *    ! Events table
 *    - Global Timespan UI Component
 *    - Cluster health indicator
 * ! Notification Banners
 *    - Help Us
 *    - Cluster Unreachable
 *    - Cockroach out of date
 * - Cluster Page
 *    - Events page
 * - Node Page
 *    - Overview page with table
 *    - Graphs page
 *    ! Logs Page
 * ! Databases Page
 *    - Databases drilldown
 *    - Table drilldown
 * ! HelpUs Page
 *    - Forms
 * ! HelpUs Modal
 * - Layout Footer
 *
 *
 * NICE TO HAVE:
 *  - "generateCacheReducer()" method; most of our data reducers are extremely
 *  similar (storing read-only, cachable data queried from the server), we could
 *  cut down on a lot of boilerplate and testing by creating such a function.
 *
 *  - Create a "NodeStatusProvider" similar to "MetricsDataProvider", allowing
 *  different components to access nodes data.
 *
 *  - Commonize code between different graph types (LineGraph and
 *  StackedAreaGraph). This can likely be done by converting them into stateless
 *  functions, that return an underlying "Common" graph component. The props of
 *  the Common graph component would include the part of `initGraph` and
 *  `drawGraph` that are different for these two chart types.
 *
 */

import * as React from "react";
import * as ReactDOM from "react-dom";
import { createStore, combineReducers, applyMiddleware, compose } from "redux";
import { Provider } from "react-redux";
import { Router, Route, IndexRoute, IndexRedirect, hashHistory } from "react-router";
import { syncHistoryWithStore, routerReducer } from "react-router-redux";
import thunk from "redux-thunk";

import nodesReducer from "./redux/nodes";
import uiReducer from "./redux/ui";
import metricsReducer from "./redux/metrics";
import databaseListReducer from "./redux/databases";
import timeWindowReducer from "./redux/timewindow";

import Layout from "./containers/layout";
import Cluster from "./containers/cluster";
import ClusterOverview from "./containers/clusterOverview";
import ClusterEvents from "./containers/clusterEvents";
import Databases from "./containers/databases";
import HelpUs from "./containers/helpus";
import Nodes from "./containers/nodes";
import NodesOverview from "./containers/nodesOverview";
import NodesGraphs from "./containers/nodesGraphs";
import Node from "./containers/node";

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
    metrics: metricsReducer,
    databaseList: databaseListReducer,
    timewindow: timeWindowReducer,
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
        <Route path="cluster" component={ Cluster }>
          <IndexRoute component={ ClusterOverview } />
          <Route path="events" component={ ClusterEvents } />
        </Route>
        <Route path="nodes" component={ Nodes }>
          <IndexRedirect to="overview" />
          <Route path="overview" component={ NodesOverview } />
          <Route path="graphs" component={ NodesGraphs } />
        </Route>
        <Route path="nodes">
          // This path has to match the "nodes" route for the purpose of
          // highlighting links, but the page does not render as a child of the
          // Nodes component.
          <Route path=":node_id" component={ Node } />
        </Route>
        <Route path="databases" component={ Databases } />
        <Route path="help-us/reporting" component={ HelpUs } />
      </Route>
    </Router>
  </Provider>,
  document.getElementById("react-layout")
);
