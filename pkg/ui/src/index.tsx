/// <reference path="../node_modules/protobufjs/stub-node.d.ts" />

/**
 * UI/NEXT TODO LIST
 *
 * ! = Potentially difficult to implement
 *
 * - All Pages / Shared Components
 *    - "Last Updated"
 *    - Dropdowns
 *      - Fix 1px offset bug
 *    - Tables
 *      - CSS Match to design
 *      - Management of column widths
 * - Cluster Page
 *    - Alert notifications
 *      - Mismatched/Out-of-date Version
 *      - Help us
 *    - Right-side Summary Section
 *      - Link to Nodes page
 *      - Events?
 *    - Graphs
 *      - Tooltip when hover over title
 *    - Code block syntax highlighting
 *      - Choose keywords correctly
 *      - Fix bug on direct page load
 * - Databases Page
 *    - Last Updated Column
 *      - Retrieve/Filter events
 *    - Single database page
 *       - Table component row limit
 *       - Route to single database
 *    - Schema Change
 *      - Retrieve information from backend
 *      - Display in table list column
 *      - Display alert on table details page
 *    - Table details page
 *      - Schema Change notification
 *      - Fill out summary stats
 *      - Back Button
 *      - Column widths for grants table
 * - Nodes page
 *  - Table Style
 *  - Add Summary Section
 *  - Remove Link from Navigation Bar
 * - Helpus Page
 *  - New Navigation Bar Icon
 *  - Header links
 *  - New form field Appearance
 *
 * NICE TO HAVE:
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

import "nvd3/build/nv.d3.min.css";
import "react-select/dist/react-select.css";
import "../styl/app.styl";
import "./js/sim/style.css";

import * as protobuf from "protobufjs/minimal";
import Long from "long";

protobuf.util.Long = Long as any;
protobuf.configure();

import * as React from "react";
import * as ReactDOM from "react-dom";
import { Provider } from "react-redux";
import { Router, Route, IndexRoute, IndexRedirect } from "react-router";

import {
  tableNameAttr, databaseNameAttr, nodeIDAttr, dashboardNameAttr,
} from "./util/constants";

import { store, history } from "./redux/state";
import Layout from "./containers/layout";
import { DatabaseTablesList, DatabaseGrantsList } from "./containers/databases/databases";
import TableDetails from "./containers/databases/tableDetails";
import Nodes from "./containers/nodes";
import NodesOverview from "./containers/nodesOverview";
import NodeOverview from "./containers/nodeOverview";
import NodeGraphs from "./containers/nodeGraphs";
import NodeLogs from "./containers/nodeLogs";
import { EventPage } from "./containers/events";
import Raft from "./containers/raft";
import RaftRanges from "./containers/raftRanges";
import ClusterViz from "./containers/clusterViz";
import { alertDataSync } from "./redux/alerts";

ReactDOM.render(
  <Provider store={store}>
    <Router history={history}>
      <Route path="/" component={Layout}>
        <IndexRedirect to="cluster" />
        <Route path="cluster" component={ Nodes }>
          <IndexRedirect to="all/overview" />
          <Route path={`all/:${dashboardNameAttr}`} component={NodeGraphs} />
          <Route path={ `node/:${nodeIDAttr}/:${dashboardNameAttr}` } component={NodeGraphs} />
        </Route>
        <Route path="cluster">
          <Route path="nodes">
            <IndexRoute component={NodesOverview} />
            <Route path={`:${nodeIDAttr}`}>
              <IndexRoute component={NodeOverview} />
              <Route path="logs" component={ NodeLogs } />
            </Route>
          </Route>
          <Route path="events" component={ EventPage } />
        </Route>
        <Route path="databases">
          <IndexRedirect to="tables" />
          <Route path="tables" component={ DatabaseTablesList } />
          <Route path="grants" component={ DatabaseGrantsList } />
          <Route path={ `database/:${databaseNameAttr}/table/:${tableNameAttr}` } component={ TableDetails } />
        </Route>
        <Route path="raft" component={ Raft }>
          <IndexRedirect to="ranges" />
          <Route path="ranges" component={ RaftRanges } />
        </Route>
        <Route path="clusterviz" component={ ClusterViz } />
      </Route>
    </Router>
  </Provider>,
  document.getElementById("react-layout"),
);

store.subscribe(alertDataSync(store));
