// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  CockroachCloudContext,
  crlTheme,
  ConfigProvider as ClusterUIConfigProvider,
  DatabasesPageV2,
  DatabaseDetailsPageV2,
  TableDetailsPageV2,
} from "@cockroachlabs/cluster-ui";
import { ConfigProvider } from "antd";
import { ConnectedRouter } from "connected-react-router";
import { History } from "history";
import "nvd3/build/nv.d3.min.css";
import React from "react";
import { Provider, ReactReduxContext } from "react-redux";
import { Redirect, Route, Switch } from "react-router-dom";
import "react-select/dist/react-select.css";
import { Action, Store } from "redux";

import { TimezoneProvider } from "src/contexts/timezoneProvider";
import { AdminUIState } from "src/redux/state";
import { createLoginRoute, createLogoutRoute } from "src/routes/login";
import { RedirectToStatementDetails } from "src/routes/RedirectToStatementDetails";
import visualizationRoutes from "src/routes/visualization";
import {
  aggregatedTsAttr,
  appAttr,
  dashboardNameAttr,
  databaseAttr,
  databaseNameAttr,
  databaseIDAttr,
  executionIdAttr,
  implicitTxnAttr,
  indexNameAttr,
  nodeIDAttr,
  rangeIDAttr,
  sessionAttr,
  statementAttr,
  tabAttr,
  tableNameAttr,
  tenantNameAttr,
  txnFingerprintIdAttr,
  viewAttr,
  idAttr,
  tableIdAttr,
} from "src/util/constants";
import NotFound from "src/views/app/components/errorMessage/notFound";
import Layout from "src/views/app/containers/layout";
import DataDistributionPage from "src/views/cluster/containers/dataDistribution";
import { EventPage } from "src/views/cluster/containers/events";
import NodeGraphs from "src/views/cluster/containers/nodeGraphs";
import NodeLogs from "src/views/cluster/containers/nodeLogs";
import NodeOverview from "src/views/cluster/containers/nodeOverview";
import { IndexDetailsPage } from "src/views/databases/indexDetailsPage";
import Raft from "src/views/devtools/containers/raft";
import RaftMessages from "src/views/devtools/containers/raftMessages";
import RaftRanges from "src/views/devtools/containers/raftRanges";
import HotRangesPage from "src/views/hotRanges/index";
import JobDetails from "src/views/jobs/jobDetails";
import JobsPage from "src/views/jobs/jobsPage";
import KeyVisualizerPage from "src/views/keyVisualizer";
import { ConnectedDecommissionedNodeHistory } from "src/views/reports";
import Certificates from "src/views/reports/containers/certificates";
import CustomChart from "src/views/reports/containers/customChart";
import Debug from "src/views/reports/containers/debug";
import EnqueueRange from "src/views/reports/containers/enqueueRange";
import HotRanges from "src/views/reports/containers/hotranges";
import Localities from "src/views/reports/containers/localities";
import Network from "src/views/reports/containers/network";
import Nodes from "src/views/reports/containers/nodes";
import ProblemRanges from "src/views/reports/containers/problemRanges";
import Range from "src/views/reports/containers/range";
import ReduxDebug from "src/views/reports/containers/redux";
import Settings from "src/views/reports/containers/settings";
import StatementsDiagnosticsHistoryView from "src/views/reports/containers/statementDiagnosticsHistory";
import Stores from "src/views/reports/containers/stores";
import ScheduleDetails from "src/views/schedules/scheduleDetails";
import SchedulesPage from "src/views/schedules/schedulesPage";
import SessionDetails from "src/views/sessions/sessionDetails";
import SQLActivityPage from "src/views/sqlActivity/sqlActivityPage";
import StatementDetails from "src/views/statements/statementDetails";
import { SnapshotRouter } from "src/views/tracez_v2/snapshotRoutes";
import TransactionDetails from "src/views/transactions/transactionDetails";
import "styl/app.styl";

import InsightsOverviewPage from "./views/insights/insightsOverview";
import StatementInsightDetailsPage from "./views/insights/statementInsightDetailsPage";
import TransactionInsightDetailsPage from "./views/insights/transactionInsightDetailsPage";
import { JwtAuthTokenPage } from "./views/jwt/jwtAuthToken";
import ActiveStatementDetails from "./views/statements/activeStatementDetailsConnected";
import ActiveTransactionDetails from "./views/transactions/activeTransactionDetailsConnected";

// NOTE: If you are adding a new path to the router, and that path contains any
// components that are personally identifying information, you MUST update the
// redactions list in src/redux/analytics.ts.
//
// Examples of PII: Database names, Table names, IP addresses; Any value that
// could identify a specific user.
//
// Serial numeric values, such as NodeIDs or Descriptor IDs, are not PII and do
// not need to be redacted.

export interface AppProps {
  history: History;
  store: Store<AdminUIState, Action>;
}

export const App: React.FC<AppProps> = (props: AppProps) => {
  const { store, history } = props;

  return (
    <Provider store={store} context={ReactReduxContext}>
      <ConnectedRouter history={history} context={ReactReduxContext}>
        <CockroachCloudContext.Provider value={false}>
          <TimezoneProvider>
            {/* Apply CRL theme twice, with ConfigProvider instance from Db Console and
             imported instance from Cluster UI as it applies theme imported components only. */}
            <ClusterUIConfigProvider theme={crlTheme} prefixCls={"crdb-ant"}>
              <ConfigProvider theme={crlTheme} prefixCls={"crdb-ant"}>
                <Switch>
                  {/* login */}
                  {createLoginRoute()}
                  {createLogoutRoute(store)}
                  <Route path="/jwt/:oidc" component={JwtAuthTokenPage} />
                  <Route path="/">
                    <Layout>
                      <Switch>
                        <Redirect exact from="/" to="/overview" />
                        {/* overview page */}
                        {visualizationRoutes()}

                        {/* time series metrics */}
                        <Redirect
                          exact
                          from="/metrics"
                          to="/metrics/overview/cluster"
                        />
                        <Redirect
                          exact
                          from={`/metrics/:${dashboardNameAttr}`}
                          to={`/metrics/:${dashboardNameAttr}/cluster`}
                        />
                        <Route
                          exact
                          path={`/metrics/:${dashboardNameAttr}/cluster`}
                          component={NodeGraphs}
                        />
                        <Redirect
                          exact
                          path={`/metrics/:${dashboardNameAttr}/node`}
                          to={`/metrics/:${dashboardNameAttr}/cluster`}
                        />
                        <Route
                          exact
                          path={`/metrics/:${dashboardNameAttr}/node/:${nodeIDAttr}`}
                          component={NodeGraphs}
                        />
                        <Route
                          exact
                          path={`/metrics/:${dashboardNameAttr}/node/:${nodeIDAttr}/tenant/:${tenantNameAttr}`}
                          component={NodeGraphs}
                        />
                        <Route
                          exact
                          path={`/metrics/:${dashboardNameAttr}/cluster/tenant/:${tenantNameAttr}`}
                          component={NodeGraphs}
                        />

                        {/* node details */}
                        <Redirect exact from="/node" to="/overview/list" />
                        <Route
                          exact
                          path={`/node/:${nodeIDAttr}`}
                          component={NodeOverview}
                        />
                        <Route
                          exact
                          path={`/node/:${nodeIDAttr}/logs`}
                          component={NodeLogs}
                        />

                        {/* events & jobs */}
                        <Route path="/events" component={EventPage} />
                        <Route exact path="/jobs" component={JobsPage} />
                        <Route
                          path={`/jobs/:${idAttr}`}
                          component={JobDetails}
                        />

                        <Route
                          exact
                          path="/schedules"
                          component={SchedulesPage}
                        />
                        <Route
                          path={`/schedules/:${idAttr}`}
                          component={ScheduleDetails}
                        />

                        {/* databases */}
                        <Route
                          exact
                          path={"/databases"}
                          component={DatabasesPageV2}
                        />
                        <Route
                          exact
                          path={`/databases/:${databaseIDAttr}`}
                          component={DatabaseDetailsPageV2}
                        />
                        <Route
                          exact
                          path={`/table/:${tableIdAttr}`}
                          component={TableDetailsPageV2}
                        />
                        <Redirect
                          from={`/databases/database/:${databaseNameAttr}/table/:${tableNameAttr}`}
                          to={`/database/:${databaseNameAttr}/table/:${tableNameAttr}`}
                        />

                        <Redirect exact from="/database" to="/databases" />
                        <Redirect
                          exact
                          from={`/database/:${databaseNameAttr}/table`}
                          to={`/database/:${databaseNameAttr}`}
                        />
                        <Route
                          exact
                          path={`/database/:${databaseNameAttr}/table/:${tableNameAttr}/index/:${indexNameAttr}`}
                          component={IndexDetailsPage}
                        />
                        <Redirect
                          exact
                          from={`/database/:${databaseNameAttr}/table/:${tableNameAttr}/index`}
                          to={`/database/:${databaseNameAttr}/table/:${tableNameAttr}`}
                        />

                        {/* data distribution */}
                        <Route
                          exact
                          path="/data-distribution"
                          component={DataDistributionPage}
                        />

                        {/* SQL activity */}
                        <Route
                          exact
                          path="/sql-activity"
                          component={SQLActivityPage}
                        />

                        {/* Active executions */}
                        <Route
                          exact
                          path={`/execution/statement/:${executionIdAttr}`}
                          component={ActiveStatementDetails}
                        />

                        <Route
                          exact
                          path={`/execution/transaction/:${executionIdAttr}`}
                          component={ActiveTransactionDetails}
                        />

                        {/* statement statistics */}
                        <Redirect
                          exact
                          from={`/statements`}
                          to={`/sql-activity?${tabAttr}=Statements&${viewAttr}=fingerprints`}
                        />
                        <Redirect
                          exact
                          from={`/statements/:${appAttr}`}
                          to={`/statements?${appAttr}=:${appAttr}`}
                        />
                        <Route
                          exact
                          path={`/statement/:${implicitTxnAttr}/:${statementAttr}`}
                          component={StatementDetails}
                        />
                        <Route
                          exact
                          path={`/statements/:${appAttr}/:${statementAttr}`}
                          render={RedirectToStatementDetails}
                        />
                        <Route
                          exact
                          path={`/statements/:${appAttr}/:${implicitTxnAttr}/:${statementAttr}`}
                          render={RedirectToStatementDetails}
                        />
                        <Route
                          exact
                          path={`/statements/:${appAttr}/:${databaseAttr}/:${implicitTxnAttr}/:${statementAttr}`}
                          render={RedirectToStatementDetails}
                        />
                        <Route
                          exact
                          path={`/statement/:${implicitTxnAttr}/:${statementAttr}`}
                          render={RedirectToStatementDetails}
                        />
                        <Route
                          exact
                          path={`/statement/:${databaseAttr}/:${implicitTxnAttr}/:${statementAttr}`}
                          render={RedirectToStatementDetails}
                        />
                        <Redirect
                          exact
                          from={`/statement`}
                          to={`/sql-activity?${tabAttr}=Statements&view=fingerprints`}
                        />

                        {/* sessions */}
                        <Redirect
                          exact
                          from={`/sessions`}
                          to={`/sql-activity?${tabAttr}=Sessions`}
                        />
                        <Route
                          exact
                          path={`/session/:${sessionAttr}`}
                          component={SessionDetails}
                        />

                        {/* transactions */}
                        <Redirect
                          exact
                          from={`/transactions`}
                          to={`/sql-activity?${tabAttr}=Transactions`}
                        />
                        <Route
                          exact
                          path={`/transaction/:${txnFingerprintIdAttr}`}
                          component={TransactionDetails}
                        />
                        <Redirect
                          exact
                          from={`/transaction/:${aggregatedTsAttr}/:${txnFingerprintIdAttr}`}
                          to={`/transaction/:${txnFingerprintIdAttr}`}
                        />

                        {/* Insights */}
                        <Route
                          exact
                          path="/insights"
                          component={InsightsOverviewPage}
                        />
                        <Route
                          path={`/insights/transaction/:${idAttr}`}
                          component={TransactionInsightDetailsPage}
                        />
                        <Route
                          path={`/insights/statement/:${idAttr}`}
                          component={StatementInsightDetailsPage}
                        />

                        {/* debug pages */}
                        <Route exact path="/debug" component={Debug} />
                        <Route
                          path="/debug/tracez"
                          component={SnapshotRouter}
                        />
                        <Route
                          exact
                          path="/debug/redux"
                          component={ReduxDebug}
                        />
                        <Route
                          exact
                          path="/debug/chart"
                          component={CustomChart}
                        />
                        <Route
                          exact
                          path="/debug/enqueue_range"
                          component={EnqueueRange}
                        />
                        <Route
                          exact
                          path="/debug/hotranges"
                          component={HotRanges}
                        />
                        <Route
                          exact
                          path="/debug/hotranges/:node_id"
                          component={HotRanges}
                        />
                        <Route
                          exact
                          path={`/keyvisualizer`}
                          component={KeyVisualizerPage}
                        />
                        <Route path="/raft">
                          <Raft>
                            <Switch>
                              <Redirect exact from="/raft" to="/raft/ranges" />
                              <Route
                                exact
                                path="/raft/ranges"
                                component={RaftRanges}
                              />
                              <Route
                                exact
                                path="/raft/messages/all"
                                component={RaftMessages}
                              />
                              <Route
                                exact
                                path={`/raft/messages/node/:${nodeIDAttr}`}
                                component={RaftMessages}
                              />
                            </Switch>
                          </Raft>
                        </Route>

                        <Route
                          exact
                          path="/reports/problemranges"
                          component={ProblemRanges}
                        />
                        <Route
                          exact
                          path={`/reports/problemranges/:${nodeIDAttr}`}
                          component={ProblemRanges}
                        />
                        <Route
                          exact
                          path="/reports/localities"
                          component={Localities}
                        />
                        <Route
                          exact
                          path={`/reports/network/:${nodeIDAttr}`}
                          component={Network}
                        />
                        <Redirect
                          from={`/reports/network`}
                          to={`/reports/network/region`}
                        />
                        <Route exact path="/reports/nodes" component={Nodes} />
                        <Route
                          exact
                          path="/reports/nodes/history"
                          component={ConnectedDecommissionedNodeHistory}
                        />
                        <Route
                          exact
                          path="/reports/settings"
                          component={Settings}
                        />
                        <Route
                          exact
                          path={`/reports/certificates/:${nodeIDAttr}`}
                          component={Certificates}
                        />
                        <Route
                          exact
                          path={`/reports/range/:${rangeIDAttr}`}
                          component={Range}
                        />
                        <Route
                          exact
                          path={`/reports/stores/:${nodeIDAttr}`}
                          component={Stores}
                        />
                        <Route
                          exact
                          path={`/reports/statements/diagnosticshistory`}
                          component={StatementsDiagnosticsHistoryView}
                        />
                        {/* hot ranges */}
                        <Route
                          exact
                          path={`/hotranges`}
                          component={HotRangesPage}
                        />
                        {/* old route redirects */}
                        <Redirect
                          exact
                          from="/cluster"
                          to="/metrics/overview/cluster"
                        />
                        <Redirect
                          from={`/cluster/all/:${dashboardNameAttr}`}
                          to={`/metrics/:${dashboardNameAttr}/cluster`}
                        />
                        <Redirect
                          from={`/cluster/node/:${nodeIDAttr}/:${dashboardNameAttr}`}
                          to={`/metrics/:${dashboardNameAttr}/node/:${nodeIDAttr}`}
                        />
                        <Redirect
                          exact
                          from="/cluster/nodes"
                          to="/overview/list"
                        />
                        <Redirect
                          exact
                          from={`/cluster/nodes/:${nodeIDAttr}`}
                          to={`/node/:${nodeIDAttr}`}
                        />
                        <Redirect
                          from={`/cluster/nodes/:${nodeIDAttr}/logs`}
                          to={`/node/:${nodeIDAttr}/logs`}
                        />
                        <Redirect from="/cluster/events" to="/events" />

                        <Redirect exact from="/nodes" to="/overview/list" />

                        {/* 404 */}
                        <Route path="*" component={NotFound} />
                      </Switch>
                    </Layout>
                  </Route>
                </Switch>
              </ConfigProvider>
            </ClusterUIConfigProvider>
          </TimezoneProvider>
        </CockroachCloudContext.Provider>
      </ConnectedRouter>
    </Provider>
  );
};
