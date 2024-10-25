// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// stubComponentInModule functions must be called before any other import in this test file to make sure
// components are stubbed and then stubbed instances of components are imported as part of regular module
// resolution workflow.
// eslint-disable-next-line import/order
import { stubComponentInModule } from "./test-utils/mockComponent";

stubComponentInModule(
  "@cockroachlabs/cluster-ui",
  "DatabasesPageV2",
  "DatabaseDetailsPageV2",
);
stubComponentInModule("src/views/cluster/containers/nodeGraphs", "default");
stubComponentInModule("src/views/cluster/containers/events", "EventPage");
stubComponentInModule(
  "src/views/cluster/containers/dataDistribution",
  "default",
);
stubComponentInModule("src/views/statements/statementsPage", "default");
stubComponentInModule("src/views/statements/statementDetails", "default");
stubComponentInModule("src/views/transactions/transactionsPage", "default");
stubComponentInModule("src/views/transactions/transactionDetails", "default");
stubComponentInModule(
  "src/views/statements/activeStatementDetailsConnected",
  "default",
);
stubComponentInModule(
  "src/views/transactions/activeTransactionDetailsConnected",
  "default",
);
stubComponentInModule("src/views/insights/workloadInsightsPage", "default");
stubComponentInModule(
  "src/views/insights/transactionInsightDetailsPage",
  "default",
);
stubComponentInModule(
  "src/views/insights/statementInsightDetailsPage",
  "default",
);
stubComponentInModule("src/views/insights/schemaInsightsPage", "default");
stubComponentInModule("src/views/schedules/schedulesPage", "default");
stubComponentInModule("src/views/schedules/scheduleDetails", "default");
stubComponentInModule("src/views/tracez_v2/snapshotPage", "default");
stubComponentInModule(
  "src/views/app/components/tenantDropdown/tenantDropdown",
  "default",
);
stubComponentInModule(
  "src/views/shared/components/alertBar/alertBar",
  "ThrottleNotificationBar",
);

// NOTE: All imports should go after `stubComponentInModule` functions calls.
import { screen, render } from "@testing-library/react";
import { createMemoryHistory, MemoryHistory } from "history";
import React from "react";
import { Action, Store } from "redux";

import { App } from "src/app";
import { AdminUIState, createAdminUIStore } from "src/redux/state";

const CLUSTER_OVERVIEW_CAPACITY_LABEL = "Capacity Usage";
const CLUSTER_VIZ_NODE_MAP_LABEL = "Node Map";
const NODE_LIST_LABEL = /Nodes \([\d]\)/;
const LOADING_CLUSTER_STATUS = /Loading cluster status.*/;
const NODE_LOG_HEADER = /Logs Node.*/;
const JOBS_HEADER = "Jobs";
const SQL_ACTIVITY_HEADER = "SQL Activity";
const ADVANCED_DEBUG_HEADER = "Advanced Debug";
const REDUX_DEBUG_HEADER = "Redux State";
const CUSTOM_METRICS_CHART_HEADER = "Custom Chart";
const ENQUEUE_RANGE_HEADER = "Manually enqueue range in a replica queue";
const RAFT_HEADER = "Raft";
const RAFT_MESSAGES_HEADER = "Pending Heartbeats";
const PROBLEM_RANGES_HEADER = "Problem Ranges Report";
const LOCALITIES_REPORT_HEADER = "Localities";
const NODE_DIAGNOSTICS_REPORT_HEADER = "Node Diagnostics";
const DECOMMISSIONED_HISTORY_REPORT = "Decommissioned Node History";
const NETWORK_DIAGNOSTICS_REPORT_HEADER = "Network";
const CLUSTER_SETTINGS_REPORT = "Cluster Settings";
const CERTIFICATES_REPORT_HEADER = "Certificates";
const RANGE_REPORT_HEADER = /Range Report for.*/;
const STORES_REPORT_HEADER = "Stores";
const UNKNOWN_PAGE_LABEL = /We can.*t find the page.*/;

describe("Routing to", () => {
  let history: MemoryHistory<unknown>;

  beforeEach(() => {
    history = createMemoryHistory({
      initialEntries: ["/"],
    });
    const store: Store<AdminUIState, Action> = createAdminUIStore(history);
    render(<App history={history} store={store} />);
  });

  const navigateToPath = (path: string) => {
    history.push(path);
  };

  describe("'/' path", () => {
    test("routes to <ClusterOverview> component", () => {
      navigateToPath("/");
      screen.getByText(CLUSTER_OVERVIEW_CAPACITY_LABEL);
    });

    test("redirected to '/overview'", () => {
      navigateToPath("/");
      expect(history.location.pathname).toBe("/overview/list");
    });
  });

  describe("'/overview' path", () => {
    test("routes to <ClusterOverview> component", () => {
      navigateToPath("/overview");
      screen.getByText(CLUSTER_OVERVIEW_CAPACITY_LABEL);
    });

    test("redirected to '/overview'", () => {
      navigateToPath("/overview");
      expect(history.location.pathname).toBe("/overview/list");
    });
  });

  describe("'/overview/list' path", () => {
    test("routes to <NodeList> component", () => {
      navigateToPath("/overview");
      screen.getByText(CLUSTER_OVERVIEW_CAPACITY_LABEL);
      screen.getByText(NODE_LIST_LABEL);
    });
  });

  describe("'/overview/map' path", () => {
    test("routes to <ClusterViz> component", () => {
      navigateToPath("/overview/map");
      expect(screen.getAllByText(CLUSTER_VIZ_NODE_MAP_LABEL).length).toBe(2);
    });
  });

  {
    /* time series metrics */
  }
  describe("'/metrics' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics");
      screen.getByTestId("nodeGraphs");
    });

    test("redirected to '/metrics/overview/cluster'", () => {
      navigateToPath("/metrics");
      expect(history.location.pathname).toBe("/metrics/overview/cluster");
    });
  });

  describe("'/metrics/overview/cluster' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/overview/cluster");
      screen.getByTestId("nodeGraphs");
    });
  });

  describe("'/metrics/overview/node' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/overview/node");
      screen.getByTestId("nodeGraphs");
    });
  });

  describe("'/metrics/:dashboardNameAttr' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard");
      screen.getByTestId("nodeGraphs");
    });

    test("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      navigateToPath("/metrics/some-dashboard");
      expect(history.location.pathname).toBe("/metrics/some-dashboard/cluster");
    });
  });

  describe("'/metrics/:dashboardNameAttr/cluster' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/cluster");
      screen.getByTestId("nodeGraphs");
    });
  });

  describe("'/metrics/:dashboardNameAttr/node' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/node");
      screen.getByTestId("nodeGraphs");
    });

    test("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      navigateToPath("/metrics/some-dashboard/node");
      expect(history.location.pathname).toBe("/metrics/some-dashboard/cluster");
    });
  });

  describe("'/metrics/:dashboardNameAttr/node/:nodeIDAttr' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/node/123");
      screen.getByTestId("nodeGraphs");
    });
  });

  {
    /* node details */
  }
  describe("'/node' path", () => {
    test("routes to <NodeList> component", () => {
      navigateToPath("/node");
      screen.getByText(NODE_LIST_LABEL);
    });

    test("redirected to '/overview/list'", () => {
      navigateToPath("/node");
      expect(history.location.pathname).toBe("/overview/list");
    });
  });

  describe("'/node/:nodeIDAttr' path", () => {
    test("routes to <NodeOverview> component", () => {
      navigateToPath("/node/1");
      screen.getByText(LOADING_CLUSTER_STATUS, { selector: "h1" });
    });
  });

  describe("'/node/:nodeIDAttr/logs' path", () => {
    test("routes to <Logs> component", () => {
      navigateToPath("/node/1/logs");
      screen.getByText(NODE_LOG_HEADER, { selector: "h2" });
    });
  });

  {
    /* events & jobs */
  }
  describe("'/events' path", () => {
    test("routes to <EventPageUnconnected> component", () => {
      navigateToPath("/events");
      screen.getByTestId("EventPage");
    });
  });

  describe("'/jobs' path", () => {
    test("routes to <JobsTable> component", () => {
      navigateToPath("/jobs");
      screen.getByText(JOBS_HEADER, { selector: "h3" });
    });
  });

  describe("'/schedules' path", () => {
    test("routes to <SchedulesPage> component", () => {
      navigateToPath("/schedules");
      screen.getByTestId("schedulesPage");
    });
  });

  describe("'/schedules/:id' path", () => {
    test("routes to <ScheduleDetails> component", () => {
      navigateToPath("/schedules/12345");
      screen.getByTestId("scheduleDetails");
    });
  });

  {
    /* databases */
  }
  describe("'/databases' path", () => {
    test("routes to <DatabasesPageV2> component", () => {
      navigateToPath("/databases");
      screen.getByTestId("DatabasesPageV2");
    });
  });

  describe("'/databases/:${databaseId} path", () => {
    test("routes to <DatabaseDetailsPageV2> component", () => {
      navigateToPath("/databases/1");
      screen.getByTestId("DatabaseDetailsPageV2");
    });
  });

  describe("'/database' path", () => {
    test("redirected to '/databases'", () => {
      navigateToPath("/database");
      expect(history.location.pathname).toBe("/databases");
    });
  });

  {
    /* data distribution */
  }
  describe("'/data-distribution' path", () => {
    test("routes to <DataDistributionPage> component", () => {
      navigateToPath("/data-distribution");
      screen.getByTestId("dataDistribution");
    });
  });

  {
    /* statement statistics */
  }
  describe("'/statements' path", () => {
    test("redirects to '/sql-activity' statement tab", () => {
      navigateToPath("/statements");
      screen.getByText(SQL_ACTIVITY_HEADER, { selector: "h3" });
      screen.getByRole("tab", { name: "Statements", selected: true });
    });
  });

  describe("'/statements/:${appAttr}' path", () => {
    test("routes to <StatementsPage> component", () => {
      navigateToPath("/statements/%24+internal");
      screen.getByText(SQL_ACTIVITY_HEADER, { selector: "h3" });
      screen.getByRole("tab", { name: "Statements", selected: true });
    });
  });

  describe("'/statements/:${appAttr}/:${statementAttr}' path", () => {
    test("routes to <StatementDetails> component", () => {
      navigateToPath("/statements/%24+internal/true");
      screen.getByTestId("statementDetails");
    });
  });

  describe("'/statements/:${implicitTxnAttr}/:${statementAttr}' path", () => {
    test("routes to <StatementDetails> component", () => {
      navigateToPath("/statements/implicit-txn-attr/statement-attr");
      screen.getByTestId("statementDetails");
    });
  });

  describe("'/statement' path", () => {
    test("redirected to '/sql-activity?tab=Statements&view=fingerprints'", () => {
      navigateToPath("/statement");
      expect(history.location.pathname + history.location.search).toBe(
        "/sql-activity?tab=Statements&view=fingerprints",
      );
    });
  });

  describe("'/statement/:${implicitTxnAttr}/:${statementAttr}' path", () => {
    test("routes to <StatementDetails> component", () => {
      navigateToPath("/statement/implicit-attr/statement-attr/");
      screen.getByTestId("statementDetails");
    });
  });

  describe("'/sql-activity?tab=Statements' path", () => {
    test("routes to <StatementsPage> component", () => {
      navigateToPath("/sql-activity?tab=Statements");
      screen.getByRole("tab", { name: "Statements", selected: true });
    });

    test("routes to <StatementsPage> component with view=fingerprints", () => {
      navigateToPath("/sql-activity?tab=Statements&view=fingerprints");
      screen.getByRole("tab", { name: "Statements", selected: true });
    });

    test("routes to <ActivetStatementsView> component with view=active", () => {
      navigateToPath("/sql-activity?tab=Statements&view=active");
      screen.getByRole("tab", { name: "Statements", selected: true });
    });
  });

  {
    /* transactions statistics */
  }
  describe("'/sql-activity?tab=Transactions' path", () => {
    test("routes to <TransactionsPage> component", () => {
      navigateToPath("/sql-activity?tab=Transactions");
      screen.getByRole("tab", { name: "Transactions", selected: true });
    });

    test("routes to <TransactionsPage> component with view=fingerprints", () => {
      navigateToPath("/sql-activity?tab=Transactions&view=fingerprints");
      screen.getByRole("tab", { name: "Transactions", selected: true });
    });

    test("routes to <ActiveTransactionsView> component with view=active", () => {
      navigateToPath("/sql-activity?tab=Transactions&view=active");
      screen.getByRole("tab", { name: "Transactions", selected: true });
    });
  });

  describe("'/transaction/:aggregated_ts/:txn_fingerprint_id' path", () => {
    test("routes to <TransactionDetails> component", () => {
      navigateToPath("/transaction/1637877600/4948941983164833719");
      screen.getByTestId("transactionDetails");
    });
  });

  // Active execution details.

  describe("'/execution' path", () => {
    test("'/execution/statement/statementID' routes to <ActiveStatementDetails>", () => {
      navigateToPath("/execution/statement/stmtID");
      screen.getByTestId("activeStatementDetailsConnected");
    });

    test("'/execution/transaction/transactionID' routes to <ActiveTransactionDetails>", () => {
      navigateToPath("/execution/transaction/transactionID");
      screen.getByTestId("activeTransactionDetailsConnected");
    });
  });
  {
    /* insights */
  }
  describe("'/insights' path", () => {
    test("routes to <InsightsOverviewPage> component - workload insights page", () => {
      navigateToPath("/insights");
      screen.getByTestId("workloadInsightsPage");
    });
    test("routes to <InsightsOverviewPage> component - schema insights page", () => {
      navigateToPath("/insights?tab=Schema+Insights");
      screen.getByTestId("schemaInsightsPage");
    });
  });
  describe("'/insights/transaction/insightID' path", () => {
    test("routes to <TransactionInsightDetailsPage> component", () => {
      navigateToPath("/insights/transaction/insightID");
      screen.getByTestId("transactionInsightDetailsPage");
    });
  });
  describe("'/insights/statement/insightID' path", () => {
    test("routes to <StatementInsightDetailsPage> component", () => {
      navigateToPath("/insights/statement/insightID");
      screen.getByTestId("statementInsightDetailsPage");
    });
  });
  {
    /* debug pages */
  }
  describe("'/debug' path", () => {
    test("routes to <Debug> component", () => {
      navigateToPath("/debug");
      screen.getByText(ADVANCED_DEBUG_HEADER, {
        selector: "h3",
      });
    });
  });

  describe("'/debug/redux' path", () => {
    test("routes to <ReduxDebug> component", () => {
      navigateToPath("/debug/redux");
      screen.getByText(REDUX_DEBUG_HEADER);
    });
  });

  describe("'/debug/chart' path", () => {
    test("routes to <CustomChart> component", () => {
      navigateToPath("/debug/chart");
      screen.getByText(CUSTOM_METRICS_CHART_HEADER);
    });
  });

  describe("'/debug/enqueue_range' path", () => {
    test("routes to <EnqueueRange> component", () => {
      navigateToPath("/debug/enqueue_range");
      screen.getByText(ENQUEUE_RANGE_HEADER, {
        selector: "h1",
      });
    });
  });

  describe("'/debug/tracez/node/:nodeID/snapshot/:snapshotID' path", () => {
    test("routes to <SnapshotPage> component", () => {
      navigateToPath("/debug/tracez/node/1/snapshot/12345");
      screen.getByTestId("snapshotPage");
    });
  });

  {
    /* raft pages */
  }
  describe("'/raft' path", () => {
    test("routes to <Raft> component", () => {
      navigateToPath("/raft");
      screen.getByText(RAFT_HEADER);
    });

    test("redirected to '/raft/ranges'", () => {
      navigateToPath("/raft");
      expect(history.location.pathname).toBe("/raft/ranges");
    });
  });

  describe("'/raft/ranges' path", () => {
    test("routes to <RangesMain> component", () => {
      navigateToPath("/raft/ranges");
      screen.getByText(RAFT_HEADER);
    });
  });

  describe("'/raft/messages/all' path", () => {
    test("routes to <RaftMessages> component", () => {
      navigateToPath("/raft/messages/all");
      screen.getByText(RAFT_MESSAGES_HEADER);
    });
  });

  describe("'/raft/messages/node/:${nodeIDAttr}' path", () => {
    test("routes to <RaftMessages> component", () => {
      navigateToPath("/raft/messages/node/node-id-attr");
      screen.getByText(RAFT_MESSAGES_HEADER);
    });
  });

  describe("'/reports/problemranges' path", () => {
    test("routes to <ProblemRanges> component", () => {
      navigateToPath("/reports/problemranges");
      screen.getByText(PROBLEM_RANGES_HEADER);
    });
  });

  describe("'/reports/problemranges/:nodeIDAttr' path", () => {
    it("routes to <ProblemRanges> component", () => {
      navigateToPath("/reports/problemranges/1");
      navigateToPath("/reports/problemranges");
      screen.getByText(PROBLEM_RANGES_HEADER);
    });
  });

  describe("'/reports/localities' path", () => {
    test("routes to <Localities> component", () => {
      navigateToPath("/reports/localities");
      screen.getByText(LOCALITIES_REPORT_HEADER);
    });
  });

  describe("'/reports/nodes' path", () => {
    test("routes to <Nodes> component", () => {
      navigateToPath("/reports/nodes");
      screen.getByText(NODE_DIAGNOSTICS_REPORT_HEADER);
    });
  });

  describe("'/reports/nodes/history' path", () => {
    test("routes to <DecommissionedNodeHistory> component", () => {
      navigateToPath("/reports/nodes/history");
      screen.getByText(DECOMMISSIONED_HISTORY_REPORT);
    });
  });

  describe("'/reports/network' path", () => {
    test("routes to <Network> component", () => {
      navigateToPath("/reports/network");
      screen.getByText(NETWORK_DIAGNOSTICS_REPORT_HEADER, {
        selector: "h3",
      });
      expect(history.location.pathname).toBe("/reports/network/region");
    });
  });

  describe("'/reports/network/:nodeIDAttr' path", () => {
    test("routes to <Network> component", () => {
      navigateToPath("/reports/network/1");
      screen.getByText(NETWORK_DIAGNOSTICS_REPORT_HEADER, {
        selector: "h3",
      });
    });
  });

  describe("'/reports/settings' path", () => {
    test("routes to <Settings> component", () => {
      navigateToPath("/reports/settings");
      screen.getByText(CLUSTER_SETTINGS_REPORT);
    });
  });

  describe("'/reports/certificates/:nodeIDAttr' path", () => {
    test("routes to <Certificates> component", () => {
      navigateToPath("/reports/certificates/1");
      screen.getByText(CERTIFICATES_REPORT_HEADER);
    });
  });

  describe("'/reports/range/:nodeIDAttr' path", () => {
    test("routes to <Range> component", () => {
      navigateToPath("/reports/range/1");
      screen.getByText(RANGE_REPORT_HEADER);
    });
  });

  describe("'/reports/stores/:nodeIDAttr' path", () => {
    test("routes to <Stores> component", () => {
      navigateToPath("/reports/stores/1");
      screen.getByText(STORES_REPORT_HEADER);
    });
  });

  {
    /* old route redirects */
  }
  describe("'/cluster' path", () => {
    test("redirected to '/metrics/overview/cluster'", () => {
      navigateToPath("/cluster");
      expect(history.location.pathname).toBe("/metrics/overview/cluster");
    });
  });

  describe("'/cluster/all/:${dashboardNameAttr}' path", () => {
    test("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      const dashboardNameAttr = "some-dashboard-name";
      navigateToPath(`/cluster/all/${dashboardNameAttr}`);
      expect(history.location.pathname).toBe(
        `/metrics/${dashboardNameAttr}/cluster`,
      );
    });
  });

  describe("'/cluster/node/:${nodeIDAttr}/:${dashboardNameAttr}' path", () => {
    test("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      const dashboardNameAttr = "some-dashboard-name";
      const nodeIDAttr = 1;
      navigateToPath(`/cluster/node/${nodeIDAttr}/${dashboardNameAttr}`);
      expect(history.location.pathname).toBe(
        `/metrics/${dashboardNameAttr}/node/${nodeIDAttr}`,
      );
    });
  });

  describe("'/cluster/nodes' path", () => {
    test("redirected to '/overview/list'", () => {
      navigateToPath("/cluster/nodes");
      expect(history.location.pathname).toBe("/overview/list");
    });
  });

  describe("'/cluster/nodes/:${nodeIDAttr}' path", () => {
    test("redirected to '/node/:${nodeIDAttr}'", () => {
      const nodeIDAttr = 1;
      navigateToPath(`/cluster/nodes/${nodeIDAttr}`);
      expect(history.location.pathname).toBe(`/node/${nodeIDAttr}`);
    });
  });

  describe("'/cluster/nodes/:${nodeIDAttr}/logs' path", () => {
    test("redirected to '/node/:${nodeIDAttr}/logs'", () => {
      const nodeIDAttr = 1;
      navigateToPath(`/cluster/nodes/${nodeIDAttr}/logs`);
      expect(history.location.pathname).toBe(`/node/${nodeIDAttr}/logs`);
    });
  });

  describe("'/cluster/events' path", () => {
    test("redirected to '/events'", () => {
      navigateToPath("/cluster/events");
      expect(history.location.pathname).toBe("/events");
    });
  });

  describe("'/cluster/nodes' path", () => {
    test("redirected to '/overview/list'", () => {
      navigateToPath("/cluster/nodes");
      expect(history.location.pathname).toBe("/overview/list");
    });
  });

  describe("'/unknown-url' path", () => {
    test("routes to <errorMessage> component", () => {
      navigateToPath("/some-random-ulr");
      screen.getByText(UNKNOWN_PAGE_LABEL);
    });
  });

  describe("'/statements' path", () => {
    test("redirected to '/sql-activity?tab=Statements&view=fingerprints'", () => {
      navigateToPath("/statements");
      expect(history.location.pathname + history.location.search).toBe(
        "/sql-activity?tab=Statements&view=fingerprints",
      );
    });
  });

  describe("'/sessions' path", () => {
    test("redirected to '/sql-activity?tab=Sessions'", () => {
      navigateToPath("/sessions");
      expect(history.location.pathname + history.location.search).toBe(
        "/sql-activity?tab=Sessions",
      );
    });
  });

  describe("'/transactions' path", () => {
    test("redirected to '/sql-activity?tab=Transactions'", () => {
      navigateToPath("/transactions");
      expect(history.location.pathname + history.location.search).toBe(
        "/sql-activity?tab=Transactions",
      );
    });
  });
});
