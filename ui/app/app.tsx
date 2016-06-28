/**
 * UI/NEXT TODO LIST
 *
 * ! = Potentially difficult to implement
 *
 * - Visualization Components
 *    - Graphs
 *      - Greyed-out display on error
 *    - Cluster health indicator
 * ! Notification Banners
 *    - Cockroach out of date
 * - Node Page
 *    ! Logs Page
 * - Layout Footer
 * ! HelpUs communication with CRL server
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
 *  - It is possible to create race conditions using the time scale selector.
 *  The issue: a user selects a new time scale, which immediately initiates a
 *  server query. Before that query completes, change the time scale again,
 *  initiating another query to the server. The order in which these queries
 *  complete is indeterminate, and could result in the charts displaying data
 *  for the wrong time scale.
 *
 */

import "nvd3/build/nv.d3.min.css!";
import "build/app.css!";

import * as React from "react";
import * as ReactDOM from "react-dom";
import { createStore, combineReducers, applyMiddleware, compose } from "redux";
import { Provider } from "react-redux";
import { Router, Route, IndexRoute, IndexRedirect, hashHistory } from "react-router";
import { syncHistoryWithStore, routerReducer } from "react-router-redux";
import thunk from "redux-thunk";

import nodesReducer from "./redux/nodes";
import uiReducer from "./redux/ui";
import uiDataReducer from "./redux/uiData";
import metricsReducer from "./redux/metrics";
import timeWindowReducer from "./redux/timewindow";
import databaseInfoReducer from "./redux/databaseInfo";
import versionReducer from "./redux/version";
import { clusterReducer, healthReducer, eventsReducer, raftReducer } from "./redux/apiReducers";

import Layout from "./containers/layout";
import Cluster from "./containers/cluster";
import ClusterOverview from "./containers/clusterOverview";
import ClusterEvents from "./containers/clusterEvents";
import Databases from "./containers/databases/databases";
import DatabaseList from "./containers/databases/databaseList";
import DatabaseDetails from "./containers/databases/databaseDetails";
import TableDetails from "./containers/databases/tableDetails";
import HelpUs from "./containers/helpus";
import Nodes from "./containers/nodes";
import NodesOverview from "./containers/nodesOverview";
import NodesGraphs from "./containers/nodesGraphs";
import Node from "./containers/node";
import NodeOverview from "./containers/nodeOverview";
import NodeGraphs from "./containers/nodeGraphs";
import NodeLogs from "./containers/nodeLogs";
import Raft from "./containers/raft";
import RaftRanges from "./containers/raftRanges";

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
    uiData: uiDataReducer,
    metrics: metricsReducer,
    timewindow: timeWindowReducer,
    databaseInfo: databaseInfoReducer,
    events: eventsReducer,
    raft: raftReducer,
    health: healthReducer,
    version: versionReducer,
    cluster: clusterReducer,
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
          <Route path=":node_id" component={ Node }>
            <IndexRoute component={ NodeOverview } />
            <Route path="graphs" component={ NodeGraphs } />
            <Route path="logs" component={ NodeLogs } />
          </Route>
        </Route>
        <Route path="databases" component= { Databases }>
          <IndexRoute component={ DatabaseList } />
          <Route path=":database_name" >
            <IndexRoute component={ DatabaseDetails } />
            <Route path=":table_name" component={ TableDetails } />
          </Route>
        </Route>
        <Route path="help-us/reporting" component={ HelpUs } />
        <Route path="raft" component={ Raft }>
          <IndexRedirect to="ranges" />
          <Route path="ranges" component={ RaftRanges } />
        </Route>
      </Route>
    </Router>
  </Provider>,
  document.getElementById("react-layout")
);
