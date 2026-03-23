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
    // Wait for login redirect to complete before navigating further
    cy.location("hash").should("equal", "#/overview/list");
    cy.visit("#/insights");
  });

  commonChecks();

  it("displays insights controls", () => {
    cy.get('[class*="crl-dropdown__handler"]').contains("Statement Executions");

    cy.get('input[placeholder="Search Statements"]').should("exist");

    cy.get('[class*="dropdown-btn"]').contains("Filters");

    cy.get('[class*="range__range-title"]').contains("1h");
    cy.get('[class*="Select-value-label"]').contains("Past Hour");

    cy.get('button[aria-label="previous time interval"]').should("exist");
    cy.get('button[aria-label="next time interval"]').should("exist");
    cy.get("button").contains("Now").should("exist");
  });

  it("displays insights statements table with column headers", () => {
    cy.get('[data-testid="statement-insights-table"]').should("exist");
    // default header
    cy.get('[data-testid="statement-insights-table"]').contains(
      "Latest Statement Execution ID",
    );
    cy.get('[data-testid="statement-insights-table"]').contains(
      "Statement Fingerprint ID",
    );
    cy.get('[data-testid="statement-insights-table"]').contains(
      "Statement Execution",
    );
    cy.get('[data-testid="statement-insights-table"]').contains("Status");
    cy.get('[data-testid="statement-insights-table"]').contains("Insights");
    cy.get('[data-testid="statement-insights-table"]').contains(
      "Start Time (UTC)",
    );
    cy.get('[data-testid="statement-insights-table"]').contains("Elapsed Time");
    cy.get('[data-testid="statement-insights-table"]').contains("User Name");
    cy.get('[data-testid="statement-insights-table"]').contains(
      "Application Name",
    );
    cy.get('[data-testid="statement-insights-table"]').contains(
      "Rows Processed",
    );
  });

  it("displays insights transactions table with column headers", () => {
    cy.contains("Statement Executions").click();
    cy.get('[class*="crl-dropdown__item"]')
      .contains("Transaction Executions")
      .click();

    cy.get('[data-testid="transaction-insights-table"]').should("exist");

    cy.get('[data-testid="transaction-insights-table"]').contains(
      "Latest Transaction Execution ID",
    );
    cy.get('[data-testid="transaction-insights-table"]').contains(
      "Transaction Fingerprint ID",
    );
    cy.get('[data-testid="transaction-insights-table"]').contains(
      "Transaction Execution",
    );
    cy.get('[data-testid="transaction-insights-table"]').contains("Status");
    cy.get('[data-testid="transaction-insights-table"]').contains("Insights");
    cy.get('[data-testid="transaction-insights-table"]').contains(
      "Start Time (UTC)",
    );
    cy.get('[data-testid="transaction-insights-table"]').contains(
      "Contention Time",
    );
    cy.get('[data-testid="transaction-insights-table"]').contains(
      "SQL CPU Time",
    );
    cy.get('[data-testid="transaction-insights-table"]').contains(
      "Application Name",
    );
  });

  it("switches to Schema Insights tab", () => {
    cy.get('[data-node-key="Schema Insights"]').click();
    cy.location("hash").should("match", /insights\?tab=Schema\+Insights/);

    cy.get('[data-testid="schema-insights-table"]').should("exist");
    cy.get('[data-testid="schema-insights-table"]').contains("Insights");
    cy.get('[data-testid="schema-insights-table"]').contains("Details");
  });
});
