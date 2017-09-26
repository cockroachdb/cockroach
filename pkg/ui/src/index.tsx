/// <reference path="../node_modules/protobufjs/stub-node.d.ts" />

import "codemirror/lib/codemirror.css";
import "codemirror/theme/neat.css";
import "nvd3/build/nv.d3.min.css";
import "react-select/dist/react-select.css";
import "styl/app.styl";
import "src/js/sim/style.css";

import * as protobuf from "protobufjs/minimal";
import Long from "long";

protobuf.util.Long = Long as any;
protobuf.configure();

import React from "react";
import * as ReactDOM from "react-dom";
import { Provider } from "react-redux";
import { Router, Route, IndexRoute, IndexRedirect } from "react-router";

import {
  tableNameAttr, databaseNameAttr, nodeIDAttr, dashboardNameAttr, rangeIDAttr,
} from "src/util/constants";

import "src/redux/analytics";
import { store, history } from "src/redux/state";

import Layout from "src/views/app/containers/layout";

import { DatabaseTablesList, DatabaseGrantsList } from "src/views/databases/containers/databases";
import TableDetails from "src/views/databases/containers/tableDetails";

import JobsPage from "src/views/jobs";

import NodesOverview from "src/views/cluster/containers/nodesOverview";
import NodeOverview from "src/views/cluster/containers/nodeOverview";
import NodeGraphs from "src/views/cluster/containers/nodeGraphs";
import NodeLogs from "src/views/cluster/containers/nodeLogs";
import { EventPage } from "src/views/cluster/containers/events";

import Raft from "src/views/devtools/containers/raft";
import RaftRanges from "src/views/devtools/containers/raftRanges";
import RaftMessages from "src/views/devtools/containers/raftMessages";
import ClusterViz from "src/views/devtools/containers/clusterViz";
import ProblemRanges from "src/views/reports/containers/problemRanges";
import Network from "src/views/reports/containers/network";
import Nodes from "src/views/reports/containers/nodes";
import Certificates from "src/views/reports/containers/certificates";
import Range from "src/views/reports/containers/range";
import Debug from "src/views/reports/containers/debug";

import { alertDataSync } from "src/redux/alerts";

// NOTE: If you are adding a new path to the router, and that path contains any
// components that are personally identifying information, you MUST update the
// redactions list in src/redux/analytics.ts.
//
// Examples of PII: Database names, Table names, IP addresses; Any value that
// could identify a specific user.
//
// Serial numeric values, such as NodeIDs or Descriptor IDs, are not PII and do
// not need to be redacted.
ReactDOM.render(
  <Provider store={store}>
    <Router history={history}>
      <Route path="/" component={Layout}>
        <IndexRedirect to="cluster" />
        <Route path="cluster">
          <IndexRedirect to="all/overview" />
          <Route path={`all/:${dashboardNameAttr}`} component={NodeGraphs} />
          <Route path={ `node/:${nodeIDAttr}/:${dashboardNameAttr}` } component={NodeGraphs} />
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
        <Route path="jobs" component={ JobsPage } />
        <Route path="raft" component={ Raft }>
          <IndexRedirect to="ranges" />
          <Route path="ranges" component={ RaftRanges } />
          <Route path="messages/all" component={ RaftMessages } />
          <Route path={`messages/node/:${nodeIDAttr}`} component={ RaftMessages } />
        </Route>
        <Route path="clusterviz" component={ ClusterViz } />
        <Route path="debug" component={ Debug } />
        <Route path="reports">
          <Route path="problemranges" component={ ProblemRanges }>
            <Route path={`:${nodeIDAttr}`} component={ ProblemRanges }/>
          </Route>
          <Route path="network" component={ Network } />
          <Route path="nodes" component={ Nodes } />
          <Route path={`certificates/:${nodeIDAttr}`} component={ Certificates } />
          <Route path={`range/:${rangeIDAttr}`} component={ Range } />
        </Route>
      </Route>
    </Router>
  </Provider>,
  document.getElementById("react-layout"),
);

store.subscribe(alertDataSync(store));
