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
import Layout from "src/containers/layout";
import { DatabaseTablesList, DatabaseGrantsList } from "src/containers/databases/databases";
import TableDetails from "src/containers/databases/tableDetails";
import Nodes from "src/containers/nodes";
import NodesOverview from "src/containers/nodesOverview";
import NodeOverview from "src/containers/nodeOverview";
import NodeGraphs from "src/containers/nodeGraphs";
import NodeLogs from "src/containers/nodeLogs";
import { EventPage } from "src/containers/events";
import QueryPlan from "src/containers/queryPlan";
import Raft from "src/containers/raft";
import RaftRanges from "src/containers/raftRanges";
import ClusterViz from "src/containers/clusterViz";
import { alertDataSync } from "src/redux/alerts";

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
