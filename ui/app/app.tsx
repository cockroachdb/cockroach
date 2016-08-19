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
import { Provider } from "react-redux";
import { Router, Route, IndexRoute, IndexRedirect } from "react-router";

import { databaseNameAttr, nodeIDAttr, tableNameAttr } from "./util/constants";

import { store, history } from "./redux/state";
import Layout from "./containers/layout";
import Cluster from "./containers/cluster";
import ClusterOverview from "./containers/clusterOverview";
import ClusterEvents from "./containers/clusterEvents";
import Databases from "./containers/databases/databases";
import DatabaseList from "./containers/databases/databaseList";
import DatabaseEvents from "./containers/databases/databaseEvents";
import DatabaseDetails from "./containers/databases/databaseDetails";
import DatabaseGrants from "./containers/databases/databaseGrants";
import TableDetails from "./containers/databases/tableDetails";
import HelpUs from "./containers/helpus";
import Nodes from "./containers/nodes";
import NodesOverview from "./containers/nodesOverview";
import Node from "./containers/node";
import NodeOverview from "./containers/nodeOverview";
import NodeGraphs from "./containers/nodeGraphs";
import NodeLogs from "./containers/nodeLogs";
import Raft from "./containers/raft";
import RaftRanges from "./containers/raftRanges";

// tslint:disable-next-line:variable-name
const DOMNode = document.getElementById("react-layout");

// Voodoo to force react-router to reload stuff when directed by livereload.
// See https://github.com/capaj/systemjs-hot-reloader.
export function __unload() {
  ReactDOM.unmountComponentAtNode(DOMNode);
}

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
          <Route path="graphs" component={ NodeGraphs } />
        </Route>
        <Route path="nodes">
          // This path has to match the "nodes" route for the purpose of
          // highlighting links, but the page does not render as a child of the
          // Nodes component.
          <Route path={ `:${nodeIDAttr}` } component={ Node }>
            <IndexRoute component={ NodeOverview } />
            <Route path="graphs" component={ NodeGraphs } />
            <Route path="logs" component={ NodeLogs } />
          </Route>
        </Route>
        <Route path="databases" component= { Databases }>
          <IndexRedirect to="overview" />
          <Route path="overview" component={ DatabaseList } />
          <Route path="events" component={ DatabaseEvents } />
          <Route path="database" >
            <Route path={ `:${databaseNameAttr}` } >
              <IndexRedirect to="overview" />
              <Route path="overview" component={ DatabaseDetails } />
              <Route path="events" component={ DatabaseEvents } />
              <Route path="grants" component={ DatabaseGrants } />
              <Route path="table">
                <Route path={ `:${tableNameAttr}` } >
                  <IndexRedirect to="overview" />
                  <Route path="overview" component={ TableDetails } />
                  <Route path="events" component={ DatabaseEvents } />
                  <Route path="grants" component={ DatabaseGrants } />
                </Route>
              </Route>
            </Route>
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
  DOMNode
);
