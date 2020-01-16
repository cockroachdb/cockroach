// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { assert } from "chai";
import { Action, Store } from "redux";
import { createMemoryHistory } from "react-router";
import { syncHistoryWithStore } from "react-router-redux";
import { mount, ReactWrapper } from "enzyme";

import "src/enzymeInit";
import { App } from "src/app";
import { AdminUIState, createAdminUIStore, History } from "src/redux/state";

import ClusterOverview from "src/views/cluster/containers/clusterOverview";
import NodeList from "src/views/clusterviz/containers/map/nodeList";
import { ClusterVisualization } from "src/views/clusterviz/containers/map";
import { NodeGraphs } from "src/views/cluster/containers/nodeGraphs";
import { NodeOverview } from "src/views/cluster/containers/nodeOverview";
import { Logs } from "src/views/cluster/containers/nodeLogs";
import { EventPageUnconnected } from "src/views/cluster/containers/events";
import { JobsTable } from "src/views/jobs";
import {
  DatabaseGrantsList,
  DatabaseTablesList,
} from "src/views/databases/containers/databases";
import { TableMain } from "src/views/databases/containers/tableDetails";
import { DataDistributionPage } from "src/views/cluster/containers/dataDistribution";
import { StatementsPage } from "src/views/statements/statementsPage";
import { StatementDetails } from "src/views/statements/statementDetails";
import Debug from "src/views/reports/containers/debug";
import { ReduxDebug } from "src/views/reports/containers/redux";
import { CustomChart } from "src/views/reports/containers/customChart";
import { EnqueueRange } from "src/views/reports/containers/enqueueRange";

describe("Routing to", () => {
  const store: Store<AdminUIState, Action> = createAdminUIStore();
  const memoryHistory = createMemoryHistory({
    entries: ["/"],
  });
  const history: History = syncHistoryWithStore(memoryHistory, store);
  const appWrapper: ReactWrapper = mount(<App history={history} store={store}/>);

  after(() => {
    appWrapper.unmount();
  });

  const navigateToPath = (path: string) => {
    history.push(path);
    appWrapper.update();
  };

  describe("'/' path", () => {
    it("routes to <ClusterOverview> component", () => {
      navigateToPath("/");
      assert.lengthOf(appWrapper.find(ClusterOverview), 1);
    });

    it("redirected to '/overview'", () => {
      navigateToPath("/");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/overview/list");
    });
  });

  describe("'/overview' path", () => {
    it("routes to <ClusterOverview> component", () => {
      navigateToPath("/overview");
      assert.lengthOf(appWrapper.find(ClusterOverview), 1);
    });

    it("redirected to '/overview'", () => {
      navigateToPath("/overview");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/overview/list");
    });
  });

  describe("'/overview/list' path", () => {
    it("routes to <NodeList> component", () => {
      navigateToPath("/overview");
      const clusterOverview = appWrapper.find(ClusterOverview);
      assert.lengthOf(clusterOverview, 1);
      const nodeList = clusterOverview.find(NodeList);
      assert.lengthOf(nodeList, 1);
    });
  });

  describe("'/overview/map' path", () => {
    it("routes to <ClusterViz> component", () => {
      navigateToPath("/overview/map");
      const clusterOverview = appWrapper.find(ClusterOverview);
      const clusterViz = appWrapper.find(ClusterVisualization);
      assert.lengthOf(clusterOverview, 1);
      assert.lengthOf(clusterViz, 1);
    });
  });

  { /* time series metrics */}
  describe("'/metrics' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics");
      assert.lengthOf(appWrapper.find(NodeGraphs), 1);
    });

    it("redirected to '/metrics/overview/cluster'", () => {
      navigateToPath("/metrics");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/metrics/overview/cluster");
    });
  });

  describe("'/metrics/overview/cluster' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/overview/cluster");
      assert.lengthOf(appWrapper.find(NodeGraphs), 1);
    });
  });

  describe("'/metrics/overview/node' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/overview/node");
      assert.lengthOf(appWrapper.find(NodeGraphs), 1);
    });
  });

  describe("'/metrics/:dashboardNameAttr' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard");
      assert.lengthOf(appWrapper.find(NodeGraphs), 1);
    });

    it("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      navigateToPath("/metrics/some-dashboard");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/metrics/some-dashboard/cluster");
    });
  });

  describe("'/metrics/:dashboardNameAttr/cluster' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/cluster");
      assert.lengthOf(appWrapper.find(NodeGraphs), 1);
    });
  });

  describe("'/metrics/:dashboardNameAttr/node' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/node");
      assert.lengthOf(appWrapper.find(NodeGraphs), 1);
    });

    it("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      navigateToPath("/metrics/some-dashboard/node");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/metrics/some-dashboard/cluster");
    });
  });

  describe("'/metrics/:dashboardNameAttr/node/:nodeIDAttr' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/node/123");
      assert.lengthOf(appWrapper.find(NodeGraphs), 1);
    });
  });

  { /* node details */}
  describe("'/node' path", () => {
    it("routes to <NodeList> component", () => {
      navigateToPath("/node");
      assert.lengthOf(appWrapper.find(NodeList), 1);
    });

    it("redirected to '/overview/list'", () => {
      navigateToPath("/node");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/overview/list");
    });
  });

  describe("'/node/:nodeIDAttr' path", () => {
    it("routes to <NodeOverview> component", () => {
      navigateToPath("/node/1");
      assert.lengthOf(appWrapper.find(NodeOverview), 1);
    });
  });

  describe("'/node/:nodeIDAttr/logs' path", () => {
    it("routes to <Logs> component", () => {
      navigateToPath("/node/1/logs");
      assert.lengthOf(appWrapper.find(Logs), 1);
    });
  });

  { /* events & jobs */}
  describe("'/events' path", () => {
    it("routes to <EventPageUnconnected> component", () => {
      navigateToPath("/events");
      assert.lengthOf(appWrapper.find(EventPageUnconnected), 1);
    });
  });

  describe("'/jobs' path", () => {
    it("routes to <JobsTable> component", () => {
      navigateToPath("/jobs");
      assert.lengthOf(appWrapper.find(JobsTable), 1);
    });
  });

  { /* databases */}
  describe("'/databases' path", () => {
    it("routes to <DatabaseTablesList> component", () => {
      navigateToPath("/databases");
      assert.lengthOf(appWrapper.find(DatabaseTablesList), 1);
    });

    it("redirected to '/databases/tables'", () => {
      navigateToPath("/databases");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/databases/tables");
    });
  });

  describe("'/databases/tables' path", () => {
    it("routes to <DatabaseTablesList> component", () => {
      navigateToPath("/databases/tables");
      assert.lengthOf(appWrapper.find(DatabaseTablesList), 1);
    });
  });

  describe("'/databases/grants' path", () => {
    it("routes to <DatabaseGrantsList> component", () => {
      navigateToPath("/databases/grants");
      assert.lengthOf(appWrapper.find(DatabaseGrantsList), 1);
    });
  });

  describe("'/databases/database/:${databaseNameAttr}/table/:${tableNameAttr}' path", () => {
    it("redirected to '/database/:${databaseNameAttr}/table/:${tableNameAttr}'", () => {
      navigateToPath("/databases/database/some-db-name/table/some-table-name");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/database/some-db-name/table/some-table-name");
    });
  });

  describe("'/database' path", () => {
    it("redirected to '/databases'", () => {
      navigateToPath("/databases/tables");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/databases/tables");
    });
  });

  describe("'/database/:${databaseNameAttr}' path", () => {
    it("redirected to '/databases'", () => {
      navigateToPath("/database/some-db-name");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/databases/tables");
    });
  });

  describe("'/database/:${databaseNameAttr}/table' path", () => {
    it("redirected to '/databases/tables'", () => {
      navigateToPath("/database/some-db-name/table");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/databases/tables");
    });
  });

  describe("'/database/:${databaseNameAttr}/table/:${tableNameAttr}' path", () => {
    it("routes to <TableMain> component", () => {
      navigateToPath("/database/some-db-name/table/some-table-name");
      assert.lengthOf(appWrapper.find(TableMain), 1);
    });
  });

  { /* data distribution */}
  describe("'/data-distribution' path", () => {
    it("routes to <DataDistributionPage> component", () => {
      navigateToPath("/data-distribution");
      assert.lengthOf(appWrapper.find(DataDistributionPage), 1);
    });
  });

  { /* statement statistics */}
  describe("'/statements' path", () => {
    it("routes to <StatementsPage> component", () => {
      navigateToPath("/statements");
      assert.lengthOf(appWrapper.find(StatementsPage), 1);
    });
  });

  describe("'/statements/:${appAttr}' path", () => {
    it("routes to <StatementsPage> component", () => {
      navigateToPath("/statements/(internal)");
      assert.lengthOf(appWrapper.find(StatementsPage), 1);
    });
  });

  describe("'/statements/:${appAttr}/:${statementAttr}' path", () => {
    it("routes to <StatementDetails> component", () => {
      navigateToPath("/statements/(internal)/true");
      assert.lengthOf(appWrapper.find(StatementDetails), 1);
    });
  });

  describe("'/statements/:${implicitTxnAttr}/:${statementAttr}' path", () => {
    it("routes to <StatementDetails> component", () => {
      navigateToPath("/statements/implicit-txn-attr/statement-attr");
      assert.lengthOf(appWrapper.find(StatementDetails), 1);
    });
  });

  describe("'/statement' path", () => {
    it("redirected to '/statements'", () => {
      navigateToPath("/statement");
      const location = history.getCurrentLocation();
      assert.equal(location.pathname, "/statements");
    });
  });

  describe("'/statement/:${statementAttr}' path", () => {
    it("routes to <StatementDetails> component", () => {
      navigateToPath("/statement/statement-attr");
      assert.lengthOf(appWrapper.find(StatementDetails), 1);
    });
  });

  describe("'/statement/:${implicitTxnAttr}/:${statementAttr}' path", () => {
    it("routes to <StatementDetails> component", () => {
      navigateToPath("/statement/implicit-attr/statement-attr/");
      assert.lengthOf(appWrapper.find(StatementDetails), 1);
    });
  });

  { /* debug pages */}
  describe("'/debug' path", () => {
    it("routes to <Debug> component", () => {
      navigateToPath("/debug");
      assert.lengthOf(appWrapper.find(Debug), 1);
    });
  });

  // TODO (koorosh): Disabled due to strange failure on internal
  // behaviour of ReduxDebug component under test env.
  xdescribe("'/debug/redux' path", () => {
    it("routes to <ReduxDebug> component", () => {
      navigateToPath("/debug/redux");
      assert.lengthOf(appWrapper.find(ReduxDebug), 1);
    });
  });

  describe("'/debug/chart' path", () => {
    it("routes to <CustomChart> component", () => {
      navigateToPath("/debug/chart");
      // assert.lengthOf(appWrapper.find(Debug), 1);
      assert.lengthOf(appWrapper.find(CustomChart), 1);
    });
  });

  describe("'/debug/enqueue_range' path", () => {
    it("routes to <EnqueueRange> component", () => {
      navigateToPath("/debug/enqueue_range");
      assert.lengthOf(appWrapper.find(EnqueueRange), 1);
    });
  });
});
