// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";
import commonChecks from "../../support/commonChecks";

describe("insights page", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    cy.visit("#/insights");
  });

  commonChecks();

  it("displays insights controls", () => {
    cy.get('[class*="crl-dropdown__handler"]')
      .find("button")
      .should("exist")
      .should("contain", "Statement Executions");

    cy.get('input[placeholder="Search Statements"]').should("exist");

    cy.get('[class*="dropdown-btn"]')
      .should("exist")
      .should("contain", "Filters");

    cy.get('[class*="range__range-title"]').contains("1h");
    cy.get('[class*="Select-value-label"]').contains("Past Hour");

    cy.get('button[aria-label="previous time interval"]').should("exist");
    cy.get('button[aria-label="next time interval"]').should("exist");
    cy.get("button").contains("Now").should("exist");
  });

  it("displays insights statements table with column headers", () => {
    cy.get('[data-testid="statement-insights-table"]').should("exist");

    // Verify key column headers are present
    cy.get('[data-testid="statement-insights-table"] thead').within(() => {
      cy.contains("th", "Latest Statement Execution ID").should("exist");
      cy.contains("th", "Statement Fingerprint ID").should("exist");
      cy.contains("th", "Statement Execution").should("exist");
      cy.contains("th", "Status").should("exist");
      cy.contains("th", "Insights").should("exist");
      cy.contains("th", "Start Time (UTC)").should("exist");
      cy.contains("th", "Elapsed Time").should("exist");
      cy.contains("th", "User Name").should("exist");
      cy.contains("th", "Application Name").should("exist");
      cy.contains("th", "Rows Processed").should("exist");
    });
  });

  it("displays insights transactions table with column headers", () => {
    cy.get('[class*="crl-dropdown__handler"]')
      .find("button")
      .should("contain", "Statement Executions")
      .click();
    cy.get('[class*="crl-dropdown__item"]')
      .contains("Transaction Executions")
      .click();

    cy.get('[class*="crl-dropdown__handler"]')
      .find("button")
      .should("contain", "Transaction Executions");

    cy.get('[data-testid="transaction-insights-table"]').should("exist");

    // Verify key column headers are present
    cy.get('[data-testid="transaction-insights-table"] thead').within(() => {
      cy.contains("th", "Latest Transaction Execution ID").should("exist");
      cy.contains("th", "Transaction Fingerprint ID").should("exist");
      cy.contains("th", "Transaction Execution").should("exist");
      cy.contains("th", "Status").should("exist");
      cy.contains("th", "Insights").should("exist");
      cy.contains("th", "Start Time (UTC)").should("exist");
      cy.contains("th", "Contention Time").should("exist");
      cy.contains("th", "SQL CPU Time").should("exist");
      cy.contains("th", "Application Name").should("exist");
    });
  });

  it("switches to Schema Insights tab", () => {
    cy.get('[data-node-key="Schema Insights"] .crdb-ant-tabs-tab-btn').click();

    cy.get('[data-node-key="Schema Insights"]').should(
      "have.class",
      "crdb-ant-tabs-tab-active",
    );

    cy.get('[data-node-key="Workload Insights"]').should(
      "not.have.class",
      "crdb-ant-tabs-tab-active",
    );

    // Verify Schema Insights table headers
    cy.get('[data-testid="schema-insights-table"]').should("exist");
    cy.get('[data-testid="schema-insights-table"] thead').within(() => {
      cy.contains("th", "Insights").should("exist");
      cy.contains("th", "Details").should("exist");
    });
  });
});
