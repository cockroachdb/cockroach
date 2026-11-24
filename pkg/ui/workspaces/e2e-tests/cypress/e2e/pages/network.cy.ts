// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";
import commonChecks from "../../support/commonChecks";

describe("network page", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    // Wait for login redirect to complete before navigating further
    cy.location("hash").should("equal", "#/overview/list");
    cy.visit("#/reports/network");
  });

  commonChecks();

  it("displays network controls", () => {
    // Check for Sort By dropdown
    cy.get('[class*="Sort-latency__dropdown"]').contains("Sort By:");

    // Check for Filter dropdown
    cy.get(".Filter-latency").find('[class*="dropdown"]').contains("Filter:");

    // Check for Collapse Nodes checkbox
    cy.get('[class*="crdb-ant-checkbox-wrapper"]').contains("Collapse Nodes");
  });

  it("displays network standard deviation", () => {
    cy.get('[class*="Legend"]').should("exist");
    cy.get('[class*="Legend"]').contains("Standard Deviation");
    cy.get('[class*="Legend"]')
      .get('[class*="Chip"]')
      .should("have.length.at.least", 5);
    cy.get('[class*="Legend"]').contains("-2");
    cy.get('[class*="Legend"]').contains("-1");
    cy.get('[class*="Legend"]').contains("Mean");
    cy.get('[class*="Legend"]').contains("+1");
    cy.get('[class*="Legend"]').contains("+2");
  });

  it("displays networks latency table", () => {
    cy.get("table.latency-table").should("exist");

    // Verify table has region headers
    cy.get("table.latency-table th.region-name").should(
      "have.length.at.least",
      1,
    );

    // Verify node links are present
    cy.get("table.latency-table a[href*='#/node/']").should(
      "have.length.at.least",
      1,
    );

    cy.get("table.latency-table .Chip").should("have.length.at.least", 1);
    cy.get("table.latency-table tbody tr").should("have.length.at.least", 1);
    cy.get("table.latency-table [class*='latency-table__cell']")
      .contains(/\d+\.\d+ms/)
      .should("exist");
  });

  it("displays node information when clicking node link", () => {
    cy.get("table.latency-table a[href*='#/node/1']").first().click();

    // Verify node information
    cy.get('[data-testid="node-overview-table"]').should("exist");
    cy.get('[data-testid="node-overview-table"] thead').within(() => {
      cy.contains("Node 1");
      cy.contains("Store 1");
    });

    // Verify key metrics are present in the table
    cy.get('[data-testid="node-overview-table"] tbody').within(() => {
      cy.contains("Live Bytes");
      cy.contains("Key Bytes");
      cy.contains("Value Bytes");
      cy.contains("MVCC Range Key Bytes");
      cy.contains("MVCC Range Value Bytes");
      cy.contains("Intent Bytes");
      cy.contains("System Bytes");
      cy.contains("GC Bytes Age");
      cy.contains("Total Replicas");
      cy.contains("Raft Leaders");
      cy.contains("Total Ranges");
      cy.contains("Unavailable %");
      cy.contains("Under Replicated %");
      cy.contains("Used Capacity");
      cy.contains("Available Capacity");
      cy.contains("Maximum Capacity");
    });

    // Verify Node Summary section
    cy.get(".summary-section").should("exist");
    cy.get(".summary-section").contains("Node Summary");
    cy.get(".summary-section").contains("Health");
    cy.get(".summary-section").contains("Last Update");
    cy.get(".summary-section").contains("Build");
    cy.get(".summary-section").contains("Logs");

    // view node logs
    cy.contains("View Logs").click();
    cy.location("hash").should("include", "/node/1/logs");
  });
});
