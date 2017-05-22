/// <reference path="../node_modules/protobufjs/stub-node.d.ts" />

import "codemirror/lib/codemirror.css";
import "codemirror/theme/neat.css";
import "nvd3/build/nv.d3.min.css";
import "react-select/dist/react-select.css";
import "../styl/app.styl";
import "js/sim/style.css";

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
} from "util/constants";

import { store, history } from "redux/state";
import Layout from "containers/layout";
import { DatabaseTablesList, DatabaseGrantsList } from "containers/databases/databases";
import TableDetails from "containers/databases/tableDetails";
import Nodes from "containers/nodes";
import NodesOverview from "containers/nodesOverview";
import NodeOverview from "containers/nodeOverview";
import NodeGraphs from "containers/nodeGraphs";
import NodeLogs from "containers/nodeLogs";
import { EventPage } from "containers/events";
import QueryPlan from "containers/queryPlan";
import Raft from "containers/raft";
import RaftRanges from "containers/raftRanges";
import ClusterViz from "containers/clusterViz";
import { alertDataSync } from "redux/alerts";

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
        <Route path="queryplan" component={ QueryPlan } />
        <Route path="clusterviz" component={ ClusterViz } />
      </Route>
    </Router>
  </Provider>,
  document.getElementById("react-layout"),
);

store.subscribe(alertDataSync(store));
