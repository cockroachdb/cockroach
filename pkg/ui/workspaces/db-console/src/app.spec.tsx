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
import { screen, render } from "@testing-library/react";

import { App } from "src/app";
import { AdminUIState, createAdminUIStore } from "src/redux/state";

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
      expect(screen.getByText(/Capacity Usage/)).toBeInTheDocument();
    });

    test("redirected to '/overview'", () => {
      navigateToPath("/");
      const location = history.location;
      expect(location.pathname).toBe("/overview/list");
    });
  });

  describe("'/overview' path", () => {
    test("routes to <ClusterOverview> component", () => {
      navigateToPath("/overview");
      expect(screen.getByText(/Capacity Usage/)).toBeInTheDocument();
    });

    test("redirected to '/overview'", () => {
      navigateToPath("/overview");
      const location = history.location;
      expect(location.pathname).toBe("/overview/list");
    });
  });

  describe("'/overview/list' path", () => {
    test("routes to <NodeList> component", () => {
      navigateToPath("/overview");
      expect(screen.getByText(/Capacity Usage/)).toBeInTheDocument();
      expect(screen.getByText(/Nodes \([\d]\)/)).toBeInTheDocument();
    });
  });

  describe("'/overview/map' path", () => {
    test("routes to <ClusterViz> component", () => {
      navigateToPath("/overview/map");
      const elems = screen.getAllByText(/Node Map/);
      expect(elems.length).toBe(2);
    });
  });

  {
    /* time series metrics */
  }
  describe("'/metrics' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics");
      expect(
        screen.getByText(/Metrics/i, { selector: "h3" }),
      ).toBeInTheDocument();
    });

    test("redirected to '/metrics/overview/cluster'", () => {
      navigateToPath("/metrics");
      const location = history.location;
      expect(location.pathname).toBe("/metrics/overview/cluster");
    });
  });

  describe("'/metrics/overview/cluster' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/overview/cluster");
      expect(
        screen.getByText(/Metrics/i, { selector: "h3" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/metrics/overview/node' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/overview/node");
      expect(
        screen.getByText(/Metrics/i, { selector: "h3" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/metrics/:dashboardNameAttr' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard");
      expect(
        screen.getByText(/Metrics/i, { selector: "h3" }),
      ).toBeInTheDocument();
    });

    test("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      navigateToPath("/metrics/some-dashboard");
      const location = history.location;
      expect(location.pathname).toBe("/metrics/some-dashboard/cluster");
    });
  });

  describe("'/metrics/:dashboardNameAttr/cluster' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/cluster");
      expect(
        screen.getByText(/Metrics/i, { selector: "h3" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/metrics/:dashboardNameAttr/node' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/node");
      expect(
        screen.getByText(/Metrics/i, { selector: "h3" }),
      ).toBeInTheDocument();
    });

    test("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      navigateToPath("/metrics/some-dashboard/node");
      const location = history.location;
      expect(location.pathname).toBe("/metrics/some-dashboard/cluster");
    });
  });

  describe("'/metrics/:dashboardNameAttr/node/:nodeIDAttr' path", () => {
    test("routes to <NodeGraphs> component", () => {
      navigateToPath("/metrics/some-dashboard/node/123");
      expect(
        screen.getByText(/Metrics/i, { selector: "h3" }),
      ).toBeInTheDocument();
    });
  });

  {
    /* node details */
  }
  describe("'/node' path", () => {
    test("routes to <NodeList> component", () => {
      navigateToPath("/node");
      // assert.lengthOf(appWrapper.find(NodeList), 1);
      expect(screen.getByText(/Nodes \([\d]\)/)).toBeInTheDocument();
    });

    test("redirected to '/overview/list'", () => {
      navigateToPath("/node");
      const location = history.location;
      expect(location.pathname).toBe("/overview/list");
    });
  });

  describe("'/node/:nodeIDAttr' path", () => {
    test("routes to <NodeOverview> component", () => {
      navigateToPath("/node/1");
      // assert.lengthOf(appWrapper.find(NodeOverview), 1);
      expect(
        screen.getByText(/Loading cluster status.*/, { selector: "h1" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/node/:nodeIDAttr/logs' path", () => {
    test("routes to <Logs> component", () => {
      navigateToPath("/node/1/logs");
      expect(
        screen.getByText(/Logs Node.*/, { selector: "h2" }),
      ).toBeInTheDocument();
    });
  });

  {
    /* events & jobs */
  }
  describe("'/events' path", () => {
    test("routes to <EventPageUnconnected> component", () => {
      navigateToPath("/events");
      // assert.lengthOf(appWrapper.find(EventPageUnconnected), 1);
      expect(
        screen.getByText(/Events/, { selector: "h1" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/jobs' path", () => {
    test("routes to <JobsTable> component", () => {
      navigateToPath("/jobs");
      // assert.lengthOf(appWrapper.find(JobsTable), 1);
      expect(screen.getByText(/Jobs/, { selector: "h3" })).toBeInTheDocument();
    });
  });

  {
    /* databases */
  }
  describe("'/databases' path", () => {
    test("routes to <DatabasesPage> component", () => {
      navigateToPath("/databases");
      expect(
        screen.getByText(/Databases/, { selector: "h3" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/databases/tables' path", () => {
    test("redirected to '/databases'", () => {
      navigateToPath("/databases/tables");
      const location = history.location;
      expect(location.pathname).toBe("/databases");
    });
  });

  describe("'/databases/grants' path", () => {
    test("redirected to '/databases'", () => {
      navigateToPath("/databases/grants");
      const location = history.location;
      expect(location.pathname).toBe("/databases");
    });
  });

  describe("'/databases/database/:${databaseNameAttr}/table/:${tableNameAttr}' path", () => {
    test("redirected to '/database/:${databaseNameAttr}/table/:${tableNameAttr}'", () => {
      navigateToPath("/databases/database/some-db-name/table/some-table-name");
      const location = history.location;
      expect(location.pathname).toBe(
        "/database/some-db-name/table/some-table-name",
      );
    });
  });

  describe("'/database' path", () => {
    test("redirected to '/databases'", () => {
      navigateToPath("/database");
      const location = history.location;
      expect(location.pathname).toBe("/databases");
    });
  });

  describe("'/database/:${databaseNameAttr}' path", () => {
    test("routes to <DatabaseDetailsPage> component", () => {
      navigateToPath("/database/some-db-name");
      const result = document.querySelector(
        "[data-componentname=DatabaseDetailsPage]",
      );
      expect(result).not.toBeNull();
    });
  });

  describe("'/database/:${databaseNameAttr}/table' path", () => {
    test("redirected to '/databases/:${databaseNameAttr}'", () => {
      navigateToPath("/database/some-db-name/table");
      const location = history.location;
      expect(location.pathname).toBe("/database/some-db-name");
    });
  });

  describe("'/database/:${databaseNameAttr}/table/:${tableNameAttr}' path", () => {
    test("routes to <DatabaseTablePage> component", () => {
      navigateToPath("/database/some-db-name/table/some-table-name");
      const result = document.querySelector(
        "[data-componentname=DatabaseTablePage]",
      );
      expect(result).not.toBeNull();
    });
  });

  {
    /* data distribution */
  }
  describe("'/data-distribution' path", () => {
    test("routes to <DataDistributionPage> component", () => {
      navigateToPath("/data-distribution");
      const result = document.querySelector(
        "[data-componentname=dataDistribution]",
      );
      expect(result).not.toBeNull();
    });
  });

  {
    /* statement statistics */
  }
  describe("'/statements' path", () => {
    test("redirects to '/sql-activity' statement tab", () => {
      navigateToPath("/statements");
      expect(
        screen.getByText(/SQL Activity/, { selector: "h3" }),
      ).toBeInTheDocument();
      expect(
        screen.getByRole("tab", { name: "Statements", selected: true }),
      ).toBeInTheDocument();
    });
  });

  describe("'/statements/:${appAttr}' path", () => {
    test("routes to <StatementsPage> component", () => {
      navigateToPath("/statements/%24+internal");
      expect(
        screen.getByText(/SQL Activity/, { selector: "h3" }),
      ).toBeInTheDocument();
      expect(
        screen.getByRole("tab", { name: "Statements", selected: true }),
      ).toBeInTheDocument();
    });
  });

  describe("'/statements/:${appAttr}/:${statementAttr}' path", () => {
    test("routes to <StatementDetails> component", () => {
      navigateToPath("/statements/%24+internal/true");
      expect(
        screen.getByText(/Statement Fingerprint/, { selector: "h3" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/statements/:${implicitTxnAttr}/:${statementAttr}' path", () => {
    test("routes to <StatementDetails> component", () => {
      navigateToPath("/statements/implicit-txn-attr/statement-attr");
      // assert.lengthOf(appWrapper.find(StatementDetails), 1);
      expect(
        screen.getByText(/Statement Fingerprint/, { selector: "h3" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/statement' path", () => {
    test("redirected to '/sql-activity?tab=Statements&view=fingerprints'", () => {
      navigateToPath("/statement");
      const location = history.location;
      expect(location.pathname + location.search).toBe(
        "/sql-activity?tab=Statements&view=fingerprints",
      );
    });
  });

  describe("'/statement/:${implicitTxnAttr}/:${statementAttr}' path", () => {
    test("routes to <StatementDetails> component", () => {
      navigateToPath("/statement/implicit-attr/statement-attr/");
      expect(
        screen.getByText(/Statement Fingerprint/, { selector: "h3" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/sql-activity?tab=Statements' path", () => {
    test("routes to <StatementsPage> component", () => {
      navigateToPath("/sql-activity?tab=Statements");
      expect(
        screen.getByRole("tab", { name: "Statements", selected: true }),
      ).toBeInTheDocument();
    });

    test("routes to <StatementsPage> component with view=fingerprints", () => {
      navigateToPath("/sql-activity?tab=Statements&view=fingerprints");
      expect(
        screen.getByRole("tab", { name: "Statements", selected: true }),
      ).toBeInTheDocument();
    });

    test("routes to <ActiveStatementsView> component with view=active", () => {
      navigateToPath("/sql-activity?tab=Statements&view=active");
      expect(
        screen.getByRole("tab", { name: "Statements", selected: true }),
      ).toBeInTheDocument();
    });
  });

  {
    /* transactions statistics */
  }
  describe("'/sql-activity?tab=Transactions' path", () => {
    test("routes to <TransactionsPage> component", () => {
      navigateToPath("/sql-activity?tab=Transactions");
      expect(
        screen.getByRole("tab", { name: "Transactions", selected: true }),
      ).toBeInTheDocument();
    });

    test("routes to <TransactionsPage> component with view=fingerprints", () => {
      navigateToPath("/sql-activity?tab=Transactions&view=fingerprints");
      expect(
        screen.getByRole("tab", { name: "Transactions", selected: true }),
      ).toBeInTheDocument();
    });

    test("routes to <ActiveTransactionsView> component with view=active", () => {
      navigateToPath("/sql-activity?tab=Transactions&view=active");
      expect(
        screen.getByRole("tab", { name: "Transactions", selected: true }),
      ).toBeInTheDocument();
    });
  });

  describe("'/transaction/:aggregated_ts/:txn_fingerprint_id' path", () => {
    test("routes to <TransactionDetails> component", () => {
      navigateToPath("/transaction/1637877600/4948941983164833719");
      expect(
        screen.getByText(/Transaction Details/, { selector: "h3" }),
      ).toBeInTheDocument();
    });
  });

  // Active execution details.

  describe("'/execution' path", () => {
    test("'/execution/statement/statementID' routes to <ActiveStatementDetails>", () => {
      navigateToPath("/execution/statement/stmtID");
      expect(
        screen.getByText(/Statement Execution ID:.*/, { selector: "h3" }),
      ).toBeInTheDocument();
    });

    test("'/execution/transaction/transactionID' routes to <ActiveTransactionDetails>", () => {
      navigateToPath("/execution/transaction/transactionID");
      expect(
        screen.getByText(/Transaction Execution ID:.*/, { selector: "h3" }),
      ).toBeInTheDocument();
    });
  });
  {
    /* debug pages */
  }
  describe("'/debug' path", () => {
    test("routes to <Debug> component", () => {
      navigateToPath("/debug");
      expect(
        screen.getByText(/Advanced Debugging/, { selector: "h3" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/debug/redux' path", () => {
    test("routes to <ReduxDebug> component", () => {
      navigateToPath("/debug/redux");
      expect(screen.getByText(/Redux State/)).toBeInTheDocument();
    });
  });

  describe("'/debug/chart' path", () => {
    test("routes to <CustomChart> component", () => {
      navigateToPath("/debug/chart");
      expect(
        screen.getByText(/Custom Chart/, { selector: "h1" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/debug/enqueue_range' path", () => {
    test("routes to <EnqueueRange> component", () => {
      navigateToPath("/debug/enqueue_range");
      expect(
        screen.getByText(/Manually enqueue range in a replica queue/, {
          selector: "h1",
        }),
      ).toBeInTheDocument();
    });
  });

  {
    /* raft pages */
  }
  describe("'/raft' path", () => {
    test("routes to <Raft> component", () => {
      navigateToPath("/raft");
      expect(screen.getByText(/Raft/, { selector: "h1" })).toBeInTheDocument();
    });

    test("redirected to '/raft/ranges'", () => {
      navigateToPath("/raft");
      const location = history.location;
      expect(location.pathname).toBe("/raft/ranges");
    });
  });

  describe("'/raft/ranges' path", () => {
    test("routes to <RangesMain> component", () => {
      navigateToPath("/raft/ranges");
      expect(screen.getByText(/Raft/, { selector: "h1" })).toBeInTheDocument();
    });
  });

  describe("'/raft/messages/all' path", () => {
    test("routes to <RaftMessages> component", () => {
      navigateToPath("/raft/messages/all");
      expect(screen.getByText(/Pending Heartbeats/)).toBeInTheDocument();
    });
  });

  describe("'/raft/messages/node/:${nodeIDAttr}' path", () => {
    test("routes to <RaftMessages> component", () => {
      navigateToPath("/raft/messages/node/node-id-attr");
      expect(screen.getByText(/Pending Heartbeats/)).toBeInTheDocument();
    });
  });

  describe("'/reports/problemranges' path", () => {
    test("routes to <ProblemRanges> component", () => {
      navigateToPath("/reports/problemranges");
      expect(
        screen.getByText(/Problem Ranges Report/, { selector: "h1" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/reports/problemranges/:nodeIDAttr' path", () => {
    it("routes to <ProblemRanges> component", () => {
      navigateToPath("/reports/problemranges/1");
      navigateToPath("/reports/problemranges");
      expect(
        screen.getByText(/Problem Ranges Report/, { selector: "h1" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/reports/localities' path", () => {
    test("routes to <Localities> component", () => {
      navigateToPath("/reports/localities");
      expect(screen.getByText(/Localities/)).toBeInTheDocument();
    });
  });

  describe("'/reports/nodes' path", () => {
    test("routes to <Nodes> component", () => {
      navigateToPath("/reports/nodes");
      expect(screen.getByText(/Node Diagnostics/)).toBeInTheDocument();
    });
  });

  describe("'/reports/nodes/history' path", () => {
    test("routes to <DecommissionedNodeHistory> component", () => {
      navigateToPath("/reports/nodes/history");
      expect(
        screen.getByText(/Decommissioned Node History/, { selector: "h1" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/reports/network' path", () => {
    test("routes to <Network> component", () => {
      navigateToPath("/reports/network");
      expect(screen.getByText(/Network Diagnostics/)).toBeInTheDocument();
    });
  });

  describe("'/reports/network/:nodeIDAttr' path", () => {
    test("routes to <Network> component", () => {
      navigateToPath("/reports/network/1");
      expect(screen.getByText(/Network Diagnostics/)).toBeInTheDocument();
    });
  });

  describe("'/reports/settings' path", () => {
    test("routes to <Settings> component", () => {
      navigateToPath("/reports/settings");
      expect(screen.getByText(/Cluster Settings/)).toBeInTheDocument();
    });
  });

  describe("'/reports/certificates/:nodeIDAttr' path", () => {
    test("routes to <Certificates> component", () => {
      navigateToPath("/reports/certificates/1");
      expect(screen.getByText(/Certificates/)).toBeInTheDocument();
    });
  });

  describe("'/reports/range/:nodeIDAttr' path", () => {
    test("routes to <Range> component", () => {
      navigateToPath("/reports/range/1");
      expect(
        screen.getByText(/Range Report for.*/, { selector: "h1" }),
      ).toBeInTheDocument();
    });
  });

  describe("'/reports/stores/:nodeIDAttr' path", () => {
    test("routes to <Stores> component", () => {
      navigateToPath("/reports/stores/1");
      expect(screen.getByText(/Stores/)).toBeInTheDocument();
    });
  });

  {
    /* old route redirects */
  }
  describe("'/cluster' path", () => {
    test("redirected to '/metrics/overview/cluster'", () => {
      navigateToPath("/cluster");
      const location = history.location;
      expect(location.pathname).toBe("/metrics/overview/cluster");
    });
  });

  describe("'/cluster/all/:${dashboardNameAttr}' path", () => {
    test("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      const dashboardNameAttr = "some-dashboard-name";
      navigateToPath(`/cluster/all/${dashboardNameAttr}`);
      const location = history.location;
      expect(location.pathname).toBe(`/metrics/${dashboardNameAttr}/cluster`);
    });
  });

  describe("'/cluster/node/:${nodeIDAttr}/:${dashboardNameAttr}' path", () => {
    test("redirected to '/metrics/:${dashboardNameAttr}/cluster'", () => {
      const dashboardNameAttr = "some-dashboard-name";
      const nodeIDAttr = 1;
      navigateToPath(`/cluster/node/${nodeIDAttr}/${dashboardNameAttr}`);
      const location = history.location;
      expect(location.pathname).toBe(
        `/metrics/${dashboardNameAttr}/node/${nodeIDAttr}`,
      );
    });
  });

  describe("'/cluster/nodes' path", () => {
    test("redirected to '/overview/list'", () => {
      navigateToPath("/cluster/nodes");
      const location = history.location;
      expect(location.pathname).toBe("/overview/list");
    });
  });

  describe("'/cluster/nodes/:${nodeIDAttr}' path", () => {
    test("redirected to '/node/:${nodeIDAttr}'", () => {
      const nodeIDAttr = 1;
      navigateToPath(`/cluster/nodes/${nodeIDAttr}`);
      const location = history.location;
      expect(location.pathname).toBe(`/node/${nodeIDAttr}`);
    });
  });

  describe("'/cluster/nodes/:${nodeIDAttr}/logs' path", () => {
    test("redirected to '/node/:${nodeIDAttr}/logs'", () => {
      const nodeIDAttr = 1;
      navigateToPath(`/cluster/nodes/${nodeIDAttr}/logs`);
      const location = history.location;
      expect(location.pathname).toBe(`/node/${nodeIDAttr}/logs`);
    });
  });

  describe("'/cluster/events' path", () => {
    test("redirected to '/events'", () => {
      navigateToPath("/cluster/events");
      const location = history.location;
      expect(location.pathname).toBe("/events");
    });
  });

  describe("'/cluster/nodes' path", () => {
    test("redirected to '/overview/list'", () => {
      navigateToPath("/cluster/nodes");
      const location = history.location;
      expect(location.pathname).toBe("/overview/list");
    });
  });

  describe("'/unknown-url' path", () => {
    test("routes to <errorMessage> component", () => {
      navigateToPath("/some-random-ulr");
      expect(screen.getByText(/We can.*t find the page.*/)).toBeInTheDocument();
    });
  });

  describe("'/statements' path", () => {
    test("redirected to '/sql-activity?tab=Statements&view=fingerprints'", () => {
      navigateToPath("/statements");
      const location = history.location;
      expect(location.pathname + location.search).toBe(
        "/sql-activity?tab=Statements&view=fingerprints",
      );
    });
  });

  describe("'/sessions' path", () => {
    test("redirected to '/sql-activity?tab=Sessions'", () => {
      navigateToPath("/sessions");
      const location = history.location;
      expect(location.pathname + location.search).toBe(
        "/sql-activity?tab=Sessions",
      );
    });
  });

  describe("'/transactions' path", () => {
    test("redirected to '/sql-activity?tab=Transactions'", () => {
      navigateToPath("/transactions");
      const location = history.location;
      expect(location.pathname + location.search).toBe(
        "/sql-activity?tab=Transactions",
      );
    });
  });
});
