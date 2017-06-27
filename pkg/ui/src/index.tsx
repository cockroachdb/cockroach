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
  tableNameAttr, databaseNameAttr, nodeIDAttr, dashboardNameAttr,
} from "src/util/constants";

import { store, history } from "src/redux/state";
import Layout from "src/views/app/containers/layout";

import { DatabaseTablesList, DatabaseGrantsList } from "src/views/databases/containers/databases";
import TableDetails from "src/views/databases/containers/tableDetails";

import NodesOverview from "src/views/cluster/containers/nodesOverview";
import NodeOverview from "src/views/cluster/containers/nodeOverview";
import NodeGraphs from "src/views/cluster/containers/nodeGraphs";
import NodeLogs from "src/views/cluster/containers/nodeLogs";
import { EventPage } from "src/views/cluster/containers/events";

import QueryPlan from "src/views/devtools/containers/queryPlan";
import Raft from "src/views/devtools/containers/raft";
import RaftRanges from "src/views/devtools/containers/raftRanges";
import ClusterViz from "src/views/devtools/containers/clusterViz";
import ProblemRanges from "src/views/reports/containers/problemRanges";
import Network from "src/views/reports/containers/network";

import { alertDataSync } from "src/redux/alerts";

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
        <Route path="raft" component={ Raft }>
          <IndexRedirect to="ranges" />
          <Route path="ranges" component={ RaftRanges } />
        </Route>
        <Route path="queryplan" component={ QueryPlan } />
        <Route path="clusterviz" component={ ClusterViz } />
        <Route path="reports">
          <Route path="problemranges" component={ ProblemRanges } />
          <Route path="network" component={ Network } />
        </Route>
      </Route>
    </Router>
  </Provider>,
  document.getElementById("react-layout"),
);

store.subscribe(alertDataSync(store));
