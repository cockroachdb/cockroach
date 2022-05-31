// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { TIME_UNTIL_NODE_DEAD } from "../../support/constants";

describe("Nodes status change", () => {
  beforeEach(() => {
    cy.teardown();
    cy.startCluster();
    cy.visit("#/overview/list");
  });

  describe("from Live to Decommissioned", () => {
    it("changes to Decommissioned status", () => {
      const nodeIdToDecommission = 2;
      cy
        .log("Validate all 4 nodes are Live")
        .get(
          ".nodes-overview__live-nodes-table tbody tr td span span:contains(Live)",
          {logMessage: "Node List table > rows with Live status"},
        )
        .should("have.length", 4);

      cy
        .log("Cluster summary section displays 4 live nodes")
        .get(
          ".node-liveness.cluster-summary__metric.live-nodes",
          {logMessage: "Cluster Summary > get Live Nodes value"},
        )
        .should("contain", 4);

      cy.decommissionNode(nodeIdToDecommission);
      cy.stopNode(nodeIdToDecommission);

      cy
        .log("Validate that only 3 live nodes remain")
        .get(
          ".nodes-overview__live-nodes-table tbody tr td span span:contains(Live)",
          { logMessage: "Node List table > rows with Live status" },
        )
        .should("have.length", 3);

      cy
        .log("...and 1 node is Decommissioning table")
        .get(
          ".nodes-overview__live-nodes-table",
          {logMessage: "Node List table > rows with Decommissioning status"},
        )
        .find("tbody tr:contains(Decommissioning)")
        .should("have.length", 1);

      cy
        .log("...and Cluster summary shows that 1 node is suspected")
        .get(
          ".node-liveness.cluster-summary__metric.suspect-nodes",
          {logMessage: "Cluster Summary > get Suspected Nodes value"},
        )
        .should("contain", 1);

      cy.wait(TIME_UNTIL_NODE_DEAD);

      cy
        .log("Validate that Decommissioned node table exists and contains one record")
        .get(
          ".nodes-overview__decommissioned-nodes-table",
          { logMessage: "Decommissioned Nodes table exists" },
        )
        .should("exist")
        .contains("tbody tr .status-column.status-column--color-decommissioned > span", "Decommissioned")
        .should("exist");
    });
  });
});
