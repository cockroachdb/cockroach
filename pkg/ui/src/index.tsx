/// <reference path="../node_modules/protobufjs/stub-node.d.ts" />

import "codemirror/lib/codemirror.css";
import "codemirror/theme/neat.css";
import "nvd3/build/nv.d3.min.css";
import "react-select/dist/react-select.css";
import "styl/app.styl";

import "src/polyfills";

import * as protobuf from "protobufjs/minimal";
import Long from "long";

protobuf.util.Long = Long as any;
protobuf.configure();

import React from "react";
import * as ReactDOM from "react-dom";
import { Provider } from "react-redux";
import { Router, Route, IndexRoute, IndexRedirect, Redirect } from "react-router";

import {
  tableNameAttr, databaseNameAttr, nodeIDAttr, dashboardNameAttr, rangeIDAttr,
} from "src/util/constants";

import { alertDataSync } from "src/redux/alerts";
import "src/redux/analytics";
import { store, history } from "src/redux/state";

import visualizationRoutes from "src/routes/visualization";

import NotFound from "src/views/app/components/NotFound";
import Layout from "src/views/app/containers/layout";
import { DatabaseTablesList, DatabaseGrantsList } from "src/views/databases/containers/databases";
import TableDetails from "src/views/databases/containers/tableDetails";
import { EventPage } from "src/views/cluster/containers/events";
import Raft from "src/views/devtools/containers/raft";
import RaftRanges from "src/views/devtools/containers/raftRanges";
import RaftMessages from "src/views/devtools/containers/raftMessages";
import NodeGraphs from "src/views/cluster/containers/nodeGraphs";
import NodesOverview from "src/views/cluster/containers/nodesOverview";
import NodeOverview from "src/views/cluster/containers/nodeOverview";
import NodeLogs from "src/views/cluster/containers/nodeLogs";
import JobsPage from "src/views/jobs";
import Certificates from "src/views/reports/containers/certificates";
import CommandQueue from "src/views/reports/containers/commandQueue";
import CustomChart from "src/views/reports/containers/customChart";
import Debug from "src/views/reports/containers/debug";
import ProblemRanges from "src/views/reports/containers/problemRanges";
import Localities from "src/views/reports/containers/localities";
import Network from "src/views/reports/containers/network";
import Nodes from "src/views/reports/containers/nodes";
import ReduxDebug from "src/views/reports/containers/redux";
import Range from "src/views/reports/containers/range";
import Settings from "src/views/reports/containers/settings";

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
        <IndexRedirect to="overview" />

        { /* overview page */ }
        { visualizationRoutes() }

        { /* time series metrics */ }
        <Route path="metrics">
          <IndexRedirect to="overview/cluster" />
          <Route path={ `:${dashboardNameAttr}` }>
            <IndexRedirect to="cluster" />
            <Route path="cluster" component={ NodeGraphs } />
            <Route path="node">
              <IndexRedirect to={ `/metrics/:${dashboardNameAttr}/cluster` } />
              <Route path={ `:${nodeIDAttr}` } component={ NodeGraphs } />
            </Route>
          </Route>
        </Route>

        { /* node details */ }
        <Route path="nodes">
          <IndexRoute component={ NodesOverview } />
        </Route>
        <Route path="node">
          <IndexRedirect to="/nodes" />
          <Route path={ `:${nodeIDAttr}` }>
            <IndexRoute component={ NodeOverview } />
            <Route path="logs" component={ NodeLogs } />
          </Route>
        </Route>

        { /* events & jobs */ }
        <Route path="events" component={ EventPage } />
        <Route path="jobs" component={ JobsPage } />

        { /* databases */ }
        <Route path="databases">
          <IndexRedirect to="tables" />
          <Route path="tables" component={ DatabaseTablesList } />
          <Route path="grants" component={ DatabaseGrantsList } />
          <Redirect
            from={ `database/:${databaseNameAttr}/table/:${tableNameAttr}` }
            to={ `/database/:${databaseNameAttr}/table/:${tableNameAttr}` }
          />
        </Route>
        <Route path="database">
          <IndexRedirect to="/databases" />
          <Route path={ `:${databaseNameAttr}` }>
            <IndexRedirect to="/databases" />
            <Route path="table">
              <IndexRedirect to="/databases" />
              <Route path={ `:${tableNameAttr}` } component={ TableDetails } />
            </Route>
          </Route>
        </Route>

        { /* debug pages */ }
        <Route path="debug">
          <IndexRoute component={Debug} />
          <Route path="redux" component={ReduxDebug} />
          <Route path="chart" component={CustomChart} />
        </Route>
        <Route path="raft" component={ Raft }>
          <IndexRedirect to="ranges" />
          <Route path="ranges" component={ RaftRanges } />
          <Route path="messages/all" component={ RaftMessages } />
          <Route path={`messages/node/:${nodeIDAttr}`} component={ RaftMessages } />
        </Route>
        <Route path="reports">
          <Route path="problemranges" component={ ProblemRanges }>
            <Route path={`:${nodeIDAttr}`} component={ ProblemRanges }/>
          </Route>
          <Route path="localities" component={ Localities } />
          <Route path="network" component={ Network } />
          <Route path="nodes" component={ Nodes } />
          <Route path="settings" component={ Settings } />
          <Route path={`certificates/:${nodeIDAttr}`} component={ Certificates } />
          <Route path={`range/:${rangeIDAttr}`} component={ Range } />
          <Route path={`range/:${rangeIDAttr}/cmdqueue`} component={ CommandQueue } />
        </Route>

        { /* old route redirects */ }
        <Route path="cluster">
          <IndexRedirect to="/metrics/overview/cluster" />
          <Redirect
            from={`all/:${dashboardNameAttr}`}
            to={ `/metrics/:${dashboardNameAttr}/cluster` }
          />
          <Redirect
            from={ `node/:${nodeIDAttr}/:${dashboardNameAttr}` }
            to={ `/metrics/:${dashboardNameAttr}/node/:${nodeIDAttr}` }
          />
          <Route path="nodes">
            <IndexRedirect to="/nodes" />
            <Route path={`:${nodeIDAttr}`}>
              <IndexRedirect to={ `/node/:${nodeIDAttr}` } />
              <Redirect from="logs" to={ `/node/:${nodeIDAttr}/logs` } />
            </Route>
          </Route>
          <Redirect from="events" to="/events" />
        </Route>

        { /* 404 */ }
        <Route path="*" component={ NotFound } />
      </Route>
    </Router>
  </Provider>,
  document.getElementById("react-layout"),
);

store.subscribe(alertDataSync(store));
