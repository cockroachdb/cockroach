// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { stubComponentInModule } from "./test-utils/mockComponent";
stubComponentInModule(
  "src/views/databases/databaseDetailsPage",
  "DatabaseDetailsPage",
);
stubComponentInModule(
  "src/views/databases/databaseTablePage",
  "DatabaseTablePage",
);
stubComponentInModule(
  "src/views/cluster/containers/dataDistribution",
  "default",
);
stubComponentInModule("src/views/statements/statementsPage", "default");
stubComponentInModule("src/views/transactions/transactionsPage", "default");

import React from "react";
import { Action, Store } from "redux";
import { createMemoryHistory, MemoryHistory } from "history";
import { mount } from "enzyme";

import { App } from "src/app";
import { AdminUIState, createAdminUIStore } from "src/redux/state";

import ClusterOverview from "src/views/cluster/containers/clusterOverview";
import NodeList from "src/views/clusterviz/containers/map/nodeList";
import { ClusterVisualization } from "src/views/clusterviz/containers/map";
import { NodeGraphs } from "src/views/cluster/containers/nodeGraphs";
import { NodeOverview } from "src/views/cluster/containers/nodeOverview";
import { Logs } from "src/views/cluster/containers/nodeLogs";
import { EventPageUnconnected } from "src/views/cluster/containers/events";
import { DatabasesPage } from "src/views/databases/databasesPage";
import { DatabaseDetailsPage } from "src/views/databases/databaseDetailsPage";
import { DatabaseTablePage } from "src/views/databases/databaseTablePage";
import DataDistributionPageConnected from "src/views/cluster/containers/dataDistribution";
import SQLActivityPage from "src/views/sqlActivity/sqlActivityPage";
import StatementsPage from "src/views/statements/statementsPage";
import TransactionsPage from "src/views/transactions/transactionsPage";
import {
  JobsPage,
  StatementDetails,
  TransactionDetails,
  ActiveStatementDetails,
  ActiveTransactionDetails,
} from "@cockroachlabs/cluster-ui";
import Debug from "src/views/reports/containers/debug";
import { ReduxDebug } from "src/views/reports/containers/redux";
import { CustomChart } from "src/views/reports/containers/customChart";
import { EnqueueRange } from "src/views/reports/containers/enqueueRange";
import { RangesMain } from "src/views/devtools/containers/raftRanges";
import { RaftMessages } from "src/views/devtools/containers/raftMessages";
import Raft from "src/views/devtools/containers/raft";
import NotFound from "src/views/app/components/errorMessage/notFound";
import { ProblemRanges } from "src/views/reports/containers/problemRanges";
import { Localities } from "src/views/reports/containers/localities";
import { Nodes } from "src/views/reports/containers/nodes";
import { DecommissionedNodeHistory } from "src/views/reports";
import { Network } from "src/views/reports/containers/network";
import { Settings } from "src/views/reports/containers/settings";
import { Certificates } from "src/views/reports/containers/certificates";
import { Range } from "src/views/reports/containers/range";
import { Stores } from "src/views/reports/containers/stores";

describe("Routing to", () => {
  let history: MemoryHistory<unknown>;
  let appWrapper: ReturnType<typeof mount>;

  beforeEach(() => {
    history = createMemoryHistory({
      initialEntries: ["/"],
    });
    const store: Store<AdminUIState, Action> = createAdminUIStore(history);
    appWrapper = mount(<App history={history} store={store} />);
  });

  afterEach(() => {
    appWrapper.unmount();
  });

  const navigateToPath = (path: string) => {
    history.push(path);
    appWrapper.update();
  };

  describe("'/' path", () => {
    it("routes to <ClusterOverview> component", () => {
      navigateToPath("/");
      expect(appWrapper.find(ClusterOverview).length).toBe(1);
    });

    it("redirected to '/overview'", () => {
      navigateToPath("/");
      const location = history.location;
      expect(location.pathname).toEqual("/overview/list");
    });
  });

  describe("'/overview' path", () => {
    it("routes to <ClusterOverview> component", () => {
      navigateToPath("/overview");
      expect(appWrapper.find(ClusterOverview).length).toBe(1);
    });

    it("redirected to '/overview'", () => {
      navigateToPath("/overview");
      const location = history.location;
      expect(location.pathname).toEqual("/overview/list");
    });
  });

  describe("'/overview/list' path", () => {
    it("routes to <NodeList> component", () => {
      navigateToPath("/overview");
      const clusterOverview = appWrapper.find(ClusterOverview);
      expect(clusterOverview.length).toBe(1);
      const nodeList = clusterOverview.find(NodeList);
      expect(nodeList.length).toBe(1);
    });
  });

  describe("'/overview/map' path", () => {
    it("routes to <ClusterViz> component", () => {
      navigateToPath("/overview/map");
      const clusterOverview = appWrapper.find(ClusterOverview);
      const clusterViz = appWrapper.find(ClusterVisualization);
      expect(clusterOverview.length).toBe(1);
      expect(clusterViz.length).toBe(1);
    });
  });

  {
    /* time series metrics */
  }
  describe("'/metrics' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics");
      expect(appWrapper.find(NodeGraphs).length).toBe(1);
    });

    it("redirected to '/metrics/overview/cluster'", () => {
      navigateToPath("/metrics");
      const location = history.location;
      expect(location.pathname).toEqual("/metrics/overview/cluster");
    });
  });

  describe("'/metrics/overview/cluster' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/overview/cluster");
      expect(appWrapper.find(NodeGraphs).length).toBe(1);
    });
  });

  describe("'/metrics/overview/node' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/overview/node");
      expect(appWrapper.find(NodeGraphs).length).toBe(1);
    });
  });

  describe("'/metrics/:dashboardNameAttr' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard");
      expect(appWrapper.find(NodeGraphs).length).toBe(1);
    });

    it("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      navigateToPath("/metrics/some-dashboard");
      const location = history.location;
      expect(location.pathname).toEqual("/metrics/some-dashboard/cluster");
    });
  });

  describe("'/metrics/:dashboardNameAttr/cluster' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/cluster");
      expect(appWrapper.find(NodeGraphs).length).toBe(1);
    });
  });

  describe("'/metrics/:dashboardNameAttr/node' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/node");
      expect(appWrapper.find(NodeGraphs).length).toBe(1);
    });

    it("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      navigateToPath("/metrics/some-dashboard/node");
      const location = history.location;
      expect(location.pathname).toEqual("/metrics/some-dashboard/cluster");
    });
  });

  describe("'/metrics/:dashboardNameAttr/node/:nodeIDAttr' path", () => {
    it("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/node/123");
      expect(appWrapper.find(NodeGraphs).length).toBe(1);
    });
  });

  {
    /* node details */
  }
  describe("'/node' path", () => {
    it("routes to <NodeList> component", () => {
      navigateToPath("/node");
      expect(appWrapper.find(NodeList).length).toBe(1);
    });

    it("redirected to '/overview/list'", () => {
      navigateToPath("/node");
      const location = history.location;
      expect(location.pathname).toEqual("/overview/list");
    });
  });

  describe("'/node/:nodeIDAttr' path", () => {
    it("routes to <NodeOverview> component", () => {
      navigateToPath("/node/1");
      expect(appWrapper.find(NodeOverview).length).toBe(1);
    });
  });

  describe("'/node/:nodeIDAttr/logs' path", () => {
    it("routes to <Logs> component", () => {
      navigateToPath("/node/1/logs");
      expect(appWrapper.find(Logs).length).toBe(1);
    });
  });

  {
    /* events & jobs */
  }
  describe("'/events' path", () => {
    it("routes to <EventPageUnconnected> component", () => {
      navigateToPath("/events");
      expect(appWrapper.find(EventPageUnconnected).length).toBe(1);
    });
  });

  describe("'/jobs' path", () => {
    it("routes to <JobsTable> component", () => {
      navigateToPath("/jobs");
      expect(appWrapper.find(JobsPage).length).toBe(1);
    });
  });

  {
    /* databases */
  }
  describe("'/databases' path", () => {
    it("routes to <DatabasesPage> component", () => {
      navigateToPath("/databases");
      expect(appWrapper.find(DatabasesPage).length).toBe(1);
    });
  });

  describe("'/databases/tables' path", () => {
    it("redirected to '/databases'", () => {
      navigateToPath("/databases/tables");
      const location = history.location;
      expect(location.pathname).toEqual("/databases");
    });
  });

  describe("'/databases/grants' path", () => {
    it("redirected to '/databases'", () => {
      navigateToPath("/databases/grants");
      const location = history.location;
      expect(location.pathname).toEqual("/databases");
    });
  });

  describe("'/databases/database/:${databaseNameAttr}/table/:${tableNameAttr}' path", () => {
    it("redirected to '/database/:${databaseNameAttr}/table/:${tableNameAttr}'", () => {
      navigateToPath("/databases/database/some-db-name/table/some-table-name");
      const location = history.location;
      expect(location.pathname).toEqual(
        "/database/some-db-name/table/some-table-name",
      );
    });
  });

  describe("'/database' path", () => {
    it("redirected to '/databases'", () => {
      navigateToPath("/database");
      const location = history.location;
      expect(location.pathname).toEqual("/databases");
    });
  });

  describe("'/database/:${databaseNameAttr}' path", () => {
    it("routes to <DatabaseDetailsPage> component", () => {
      navigateToPath("/database/some-db-name");
      expect(appWrapper.find(DatabaseDetailsPage).length).toBe(1);
    });
  });

  describe("'/database/:${databaseNameAttr}/table' path", () => {
    it("redirected to '/databases/:${databaseNameAttr}'", () => {
      navigateToPath("/database/some-db-name/table");
      const location = history.location;
      expect(location.pathname).toEqual("/database/some-db-name");
    });
  });

  describe("'/database/:${databaseNameAttr}/table/:${tableNameAttr}' path", () => {
    it("routes to <DatabaseTablePage> component", () => {
      navigateToPath("/database/some-db-name/table/some-table-name");
      expect(appWrapper.find(DatabaseTablePage).length).toBe(1);
    });
  });

  {
    /* data distribution */
  }
  describe("'/data-distribution' path", () => {
    it("routes to <DataDistributionPage> component", () => {
      navigateToPath("/data-distribution");
      expect(appWrapper.find(DataDistributionPageConnected).length).toBe(1);
    });
  });

  {
    /* statement statistics */
  }
  describe("'/statements' path", () => {
    it("redirects to '/sql-activity' statement tab", () => {
      navigateToPath("/statements");
      expect(appWrapper.find(SQLActivityPage).length).toBe(1);
    });
  });

  describe("'/statements/:${appAttr}' path", () => {
    it("routes to <StatementsPage> component", () => {
      navigateToPath("/statements/%24+internal");
      expect(appWrapper.find(StatementsPage).length).toBe(1);
    });
  });

  describe("'/statements/:${appAttr}/:${statementAttr}' path", () => {
    it("routes to <StatementDetails> component", () => {
      navigateToPath("/statements/%24+internal/true");
      expect(appWrapper.find(StatementDetails).length).toBe(1);
    });
  });

  describe("'/statements/:${implicitTxnAttr}/:${statementAttr}' path", () => {
    it("routes to <StatementDetails> component", () => {
      navigateToPath("/statements/implicit-txn-attr/statement-attr");
      expect(appWrapper.find(StatementDetails).length).toBe(1);
    });
  });

  describe("'/statement' path", () => {
    it("redirected to '/sql-activity?tab=Statements&view=fingerprints'", () => {
      navigateToPath("/statement");
      const location = history.location;
      expect(location.pathname + location.search).toEqual(
        "/sql-activity?tab=Statements&view=fingerprints",
      );
    });
  });

  describe("'/statement/:${implicitTxnAttr}/:${statementAttr}' path", () => {
    it("routes to <StatementDetails> component", () => {
      navigateToPath("/statement/implicit-attr/statement-attr/");
      expect(appWrapper.find(StatementDetails).length).toBe(1);
    });
  });

  describe("'/sql-activity?tab=Statements' path", () => {
    it("routes to <StatementsPage> component", () => {
      navigateToPath("/sql-activity?tab=Statements");
      expect(appWrapper.find(StatementsPage).length).toBe(1);
    });

    it("routes to <StatementsPage> component with view=fingerprints", () => {
      navigateToPath("/sql-activity?tab=Statements&view=fingerprints");
      expect(appWrapper.find(StatementsPage).length).toBe(1);
    });

    it("routes to <ActiveStatementsView> component with view=active", () => {
      navigateToPath("/sql-activity?tab=Statements&view=active");
      expect(appWrapper.find(StatementsPage).length).toBe(1);
    });
  });

  {
    /* transactions statistics */
  }
  describe("'/sql-activity?tab=Transactions' path", () => {
    it("routes to <TransactionsPage> component", () => {
      navigateToPath("/sql-activity?tab=Transactions");
      expect(appWrapper.find(TransactionsPage).length).toBe(1);
    });

    it("routes to <TransactionsPage> component with view=fingerprints", () => {
      navigateToPath("/sql-activity?tab=Transactions&view=fingerprints");
      expect(appWrapper.find(TransactionsPage).length).toBe(1);
    });

    it("routes to <ActiveTransactionsView> component with view=active", () => {
      navigateToPath("/sql-activity?tab=Transactions&view=active");
      expect(appWrapper.find(TransactionsPage).length).toBe(1);
    });
  });

  describe("'/transaction/:aggregated_ts/:txn_fingerprint_id' path", () => {
    it("routes to <TransactionDetails> component", () => {
      navigateToPath("/transaction/1637877600/4948941983164833719");
      expect(appWrapper.find(TransactionDetails).length).toBe(1);
    });
  });

  // Active execution details.

  describe("'/execution' path", () => {
    it("'/execution/statement/statementID' routes to <ActiveStatementDetails>", () => {
      navigateToPath("/execution/statement/stmtID");
      expect(appWrapper.find(ActiveStatementDetails).length).toBe(1);
    });

    it("'/execution/transaction/transactionID' routes to <ActiveTransactionDetails>", () => {
      navigateToPath("/execution/transaction/transactionID");
      expect(appWrapper.find(ActiveTransactionDetails).length).toBe(1);
    });
  });
  {
    /* debug pages */
  }
  describe("'/debug' path", () => {
    it("routes to <Debug> component", () => {
      navigateToPath("/debug");
      expect(appWrapper.find(Debug).length).toBe(1);
    });
  });

  // TODO (koorosh): Disabled due to strange failure on internal
  // behavior of ReduxDebug component under test env.
  xdescribe("'/debug/redux' path", () => {
    it("routes to <ReduxDebug> component", () => {
      navigateToPath("/debug/redux");
      expect(appWrapper.find(ReduxDebug).length).toBe(1);
    });
  });

  describe("'/debug/chart' path", () => {
    it("routes to <CustomChart> component", () => {
      navigateToPath("/debug/chart");
      expect(appWrapper.find(CustomChart).length).toBe(1);
    });
  });

  describe("'/debug/enqueue_range' path", () => {
    it("routes to <EnqueueRange> component", () => {
      navigateToPath("/debug/enqueue_range");
      expect(appWrapper.find(EnqueueRange).length).toBe(1);
    });
  });

  {
    /* raft pages */
  }
  describe("'/raft' path", () => {
    it("routes to <Raft> component", () => {
      navigateToPath("/raft");
      expect(appWrapper.find(Raft).length).toBe(1);
    });

    it("redirected to '/raft/ranges'", () => {
      navigateToPath("/raft");
      const location = history.location;
      expect(location.pathname).toEqual("/raft/ranges");
    });
  });

  describe("'/raft/ranges' path", () => {
    it("routes to <RangesMain> component", () => {
      navigateToPath("/raft/ranges");
      expect(appWrapper.find(RangesMain).length).toBe(1);
    });
  });

  describe("'/raft/messages/all' path", () => {
    it("routes to <RaftMessages> component", () => {
      navigateToPath("/raft/messages/all");
      expect(appWrapper.find(RaftMessages).length).toBe(1);
    });
  });

  describe("'/raft/messages/node/:${nodeIDAttr}' path", () => {
    it("routes to <RaftMessages> component", () => {
      navigateToPath("/raft/messages/node/node-id-attr");
      expect(appWrapper.find(RaftMessages).length).toBe(1);
    });
  });

  describe("'/reports/problemranges' path", () => {
    it("routes to <ProblemRanges> component", () => {
      navigateToPath("/reports/problemranges");
      expect(appWrapper.find(ProblemRanges).length).toBe(1);
    });
  });

  describe("'/reports/problemranges/:nodeIDAttr' path", () => {
    it("routes to <ProblemRanges> component", () => {
      navigateToPath("/reports/problemranges/1");
      expect(appWrapper.find(ProblemRanges).length).toBe(1);
    });
  });

  describe("'/reports/localities' path", () => {
    it("routes to <Localities> component", () => {
      navigateToPath("/reports/localities");
      expect(appWrapper.find(Localities).length).toBe(1);
    });
  });

  describe("'/reports/nodes' path", () => {
    it("routes to <Nodes> component", () => {
      navigateToPath("/reports/nodes");
      expect(appWrapper.find(Nodes).length).toBe(1);
    });
  });

  describe("'/reports/nodes/history' path", () => {
    it("routes to <DecommissionedNodeHistory> component", () => {
      navigateToPath("/reports/nodes/history");
      expect(appWrapper.find(DecommissionedNodeHistory).length).toBe(1);
    });
  });

  describe("'/reports/network' path", () => {
    it("routes to <Network> component", () => {
      navigateToPath("/reports/network");
      expect(appWrapper.find(Network).length).toBe(1);
    });
  });

  describe("'/reports/network/:nodeIDAttr' path", () => {
    it("routes to <Network> component", () => {
      navigateToPath("/reports/network/1");
      expect(appWrapper.find(Network).length).toBe(1);
    });
  });

  describe("'/reports/settings' path", () => {
    it("routes to <Settings> component", () => {
      navigateToPath("/reports/settings");
      expect(appWrapper.find(Settings).length).toBe(1);
    });
  });

  describe("'/reports/certificates/:nodeIDAttr' path", () => {
    it("routes to <Certificates> component", () => {
      navigateToPath("/reports/certificates/1");
      expect(appWrapper.find(Certificates).length).toBe(1);
    });
  });

  describe("'/reports/range/:nodeIDAttr' path", () => {
    it("routes to <Range> component", () => {
      navigateToPath("/reports/range/1");
      expect(appWrapper.find(Range).length).toBe(1);
    });
  });

  describe("'/reports/stores/:nodeIDAttr' path", () => {
    it("routes to <Stores> component", () => {
      navigateToPath("/reports/stores/1");
      expect(appWrapper.find(Stores).length).toBe(1);
    });
  });

  {
    /* old route redirects */
  }
  describe("'/cluster' path", () => {
    it("redirected to '/metrics/overview/cluster'", () => {
      navigateToPath("/cluster");
      const location = history.location;
      expect(location.pathname).toEqual("/metrics/overview/cluster");
    });
  });

  describe("'/cluster/all/:${dashboardNameAttr}' path", () => {
    it("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      const dashboardNameAttr = "some-dashboard-name";
      navigateToPath(`/cluster/all/${dashboardNameAttr}`);
      const location = history.location;
      expect(location.pathname).toEqual(
        `/metrics/${dashboardNameAttr}/cluster`,
      );
    });
  });

  describe("'/cluster/node/:${nodeIDAttr}/:${dashboardNameAttr}' path", () => {
    it("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      const dashboardNameAttr = "some-dashboard-name";
      const nodeIDAttr = 1;
      navigateToPath(`/cluster/node/${nodeIDAttr}/${dashboardNameAttr}`);
      const location = history.location;
      expect(location.pathname).toEqual(
        `/metrics/${dashboardNameAttr}/node/${nodeIDAttr}`,
      );
    });
  });

  describe("'/cluster/nodes' path", () => {
    it("redirected to '/overview/list'", () => {
      navigateToPath("/cluster/nodes");
      const location = history.location;
      expect(location.pathname).toEqual("/overview/list");
    });
  });

  describe("'/cluster/nodes/:${nodeIDAttr}' path", () => {
    it("redirected to '/node/:${nodeIDAttr}'", () => {
      const nodeIDAttr = 1;
      navigateToPath(`/cluster/nodes/${nodeIDAttr}`);
      const location = history.location;
      expect(location.pathname).toEqual(`/node/${nodeIDAttr}`);
    });
  });

  describe("'/cluster/nodes/:${nodeIDAttr}/logs' path", () => {
    it("redirected to '/node/:${nodeIDAttr}/logs'", () => {
      const nodeIDAttr = 1;
      navigateToPath(`/cluster/nodes/${nodeIDAttr}/logs`);
      const location = history.location;
      expect(location.pathname).toEqual(`/node/${nodeIDAttr}/logs`);
    });
  });

  describe("'/cluster/events' path", () => {
    it("redirected to '/events'", () => {
      navigateToPath("/cluster/events");
      const location = history.location;
      expect(location.pathname).toEqual("/events");
    });
  });

  describe("'/cluster/nodes' path", () => {
    it("redirected to '/overview/list'", () => {
      navigateToPath("/cluster/nodes");
      const location = history.location;
      expect(location.pathname).toEqual("/overview/list");
    });
  });

  describe("'/unknown-url' path", () => {
    it("routes to <errorMessage> component", () => {
      navigateToPath("/some-random-ulr");
      expect(appWrapper.find(NotFound).length).toBe(1);
    });
  });

  describe("'/statements' path", () => {
    it("redirected to '/sql-activity?tab=Statements&view=fingerprints'", () => {
      navigateToPath("/statements");
      const location = history.location;
      expect(location.pathname + location.search).toEqual(
        "/sql-activity?tab=Statements&view=fingerprints",
      );
    });
  });

  describe("'/sessions' path", () => {
    it("redirected to '/sql-activity?tab=Sessions'", () => {
      navigateToPath("/sessions");
      const location = history.location;
      expect(location.pathname + location.search).toEqual(
        "/sql-activity?tab=Sessions",
      );
    });
  });

  describe("'/transactions' path", () => {
    it("redirected to '/sql-activity?tab=Transactions'", () => {
      navigateToPath("/transactions");
      const location = history.location;
      expect(location.pathname + location.search).toEqual(
        "/sql-activity?tab=Transactions",
      );
    });
  });
});
