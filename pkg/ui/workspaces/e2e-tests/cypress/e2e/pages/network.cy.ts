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
    cy.visit("#/reports/network");
  });

  commonChecks();

  it("displays network controls", () => {
    // Check for Sort By dropdown
    cy.get('[class*="Sort-latency__dropdown"]')
      .should("exist")
      .should("contain", "Sort By:");

    // Check for Filter dropdown
    cy.get(".Filter-latency")
      .should("exist")
      .find('[class*="dropdown"]')
      .should("contain", "Filter:");

    // Check for Collapse Nodes checkbox
    cy.get('[class*="crdb-ant-checkbox-wrapper"]')
      .should("exist")
      .should("contain", "Collapse Nodes");
  });

  it("displays network standard deviation", () => {
    cy.get('[class*="Legend"]').should("exist");
    cy.get('[class*="Legend--container__head--title"]').should(
      "contain",
      "Standard Deviation",
    );

    cy.get('[class*="Legend--container__body"]').within(() => {
      cy.get('[class*="Chip"]').should("have.length.at.least", 5);
      cy.contains("span", "-2").should("exist");
      cy.contains("span", "-1").should("exist");
      cy.contains("span", "Mean").should("exist");
      cy.contains("span", "+1").should("exist");
      cy.contains("span", "+2").should("exist");
    });
  });

  it("displays networks latency table", () => {
    cy.get("table.latency-table").should("exist");
    cy.get("table.latency-table__multiple").should("exist");

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
      cy.contains("th", "Node 1").should("exist");
      cy.contains("th", "Store 1").should("exist");
    });

    // Verify key metrics are present in the table
    cy.get('[data-testid="node-overview-table"] tbody').within(() => {
      cy.contains("td", "Live Bytes").should("exist");
      cy.contains("td", "Key Bytes").should("exist");
      cy.contains("td", "Value Bytes").should("exist");
      cy.contains("td", "MVCC Range Key Bytes").should("exist");
      cy.contains("td", "MVCC Range Value Bytes").should("exist");
      cy.contains("td", "Intent Bytes").should("exist");
      cy.contains("td", "System Bytes").should("exist");
      cy.contains("td", "GC Bytes Age").should("exist");
      cy.contains("td", "Total Replicas").should("exist");
      cy.contains("td", "Raft Leaders").should("exist");
      cy.contains("td", "Total Ranges").should("exist");
      cy.contains("td", "Unavailable %").should("exist");
      cy.contains("td", "Under Replicated %").should("exist");
      cy.contains("td", "Used Capacity").should("exist");
      cy.contains("td", "Available Capacity").should("exist");
      cy.contains("td", "Maximum Capacity").should("exist");
    });

    // Verify Node Summary section
    cy.get(".summary-section").should("exist");
    cy.contains(".summary-label", "Node Summary").should("exist");
    cy.get(".summary-section").within(() => {
      cy.contains(".summary-stat__title", "Health").should("exist");
      cy.contains("span", "healthy").should("exist");

      cy.contains(".summary-stat__title", "Last Update").should("exist");

      cy.contains(".summary-stat__title", "Build").should("exist");

      cy.contains(".summary-stat__title", "Logs").should("exist");
      cy.contains("a", "View Logs").should("exist");
    });
  });
});
