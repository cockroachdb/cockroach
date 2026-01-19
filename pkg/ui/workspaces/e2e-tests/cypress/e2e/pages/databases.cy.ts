// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";
import commonChecks from "../../support/commonChecks";

describe("databases page", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    // Wait for login redirect to complete before navigating further
    cy.location("hash").should("equal", "#/");
    cy.visit("#/databases");
  });

  commonChecks();

  it("displays table of databases", () => {
    function checkDatabases(): void {
      cy.get('[data-testid="databases-table"]').should("exist");
      cy.get('[data-testid="databases-table"]').contains("Name");
      cy.get('[data-testid="databases-table"]').contains("Size");
      cy.get('[data-testid="databases-table"]').contains("Tables");
      cy.get('[data-testid="databases-table"]').contains("Regions / Nodes");

      cy.get('[data-testid="databases-table"]').should(
        "contain.text",
        "defaultdb",
      );
      cy.get('[data-testid="databases-table"]').should(
        "contain.text",
        "postgres",
      );
      cy.get('[data-testid="databases-table"]').should(
        "contain.text",
        "system",
      );
    }

    // make 2 checks: once with a direct navigation and once after a refresh
    checkDatabases();
    cy.reload();
    checkDatabases();
  });

  it("filters databases via search input", () => {
    cy.get('input[placeholder="Search databases"]').as("dbSearch");
    cy.get("@dbSearch").clear().type("sys{enter}");

    // 'system' should be visible
    cy.get('[data-testid="databases-table"]').should("contain.text", "system");
    // 'defaultdb' and 'postgres' should not be present
    cy.get('[data-testid="databases-table"]').should(
      "not.contain.text",
      "defaultdb",
    );
    cy.get('[data-testid="databases-table"]').should(
      "not.contain.text",
      "postgres",
    );
  });

  it("displays database metadata", () => {
    cy.get('[data-testid="databases-table"]').contains("system").click();
    cy.location("hash").should("include", "/databases/1");

    cy.get('input[placeholder="Search tables"]').should("exist");
    cy.get(".crdb-ant-select-selector")
      .contains("Select Nodes")
      .should("exist");

    // Table of tables assertions
    cy.get('[data-testid="tables-table"]').should("exist");
    cy.get('[data-testid="tables-table"]').contains("Name");
    cy.get('[data-testid="tables-table"]').contains("Replication Size");
    cy.get('[data-testid="tables-table"]').contains("Ranges");
    cy.get('[data-testid="tables-table"]').contains("Columns");
    cy.get('[data-testid="tables-table"]').contains("Indexes");
    cy.get('[data-testid="tables-table"]').contains("Regions / Nodes");
    cy.get('[data-testid="tables-table"]').contains("% of Live data");
    cy.get('[data-testid="tables-table"]').contains("Table auto stats enabled");
    cy.get('[data-testid="tables-table"]').contains("Stats last updated");
    cy.get('[data-testid="tables-table"] tbody tr').should(
      "have.length.at.least",
      1,
    );

    // Click the 'Grants' tab
    cy.contains("Grants").click();

    cy.get('[data-testid="database-grants-table"]').contains("Grantee");
    cy.get('[data-testid="database-grants-table"]').contains("Privileges");
    cy.get('[data-testid="database-grants-table"] tbody tr').should(
      "have.length.at.least",
      1,
    );
  });

  it("displays table metadata", () => {
    cy.get('[data-testid="databases-table"]').contains("system").click();
    // just an arbitrary table in the systems database
    cy.get('[data-testid="tables-table"]').contains("public.comments").click();
    cy.location("hash").should("match", /\/table\/\d+/);

    // summaries of table
    cy.contains('[class*="summary--card__item--label"]', "Size");
    cy.contains('[class*="summary--card__item--label"]', "Ranges");
    cy.contains('[class*="summary--card__item--label"]', "Replicas");
    cy.contains('[class*="summary--card__item--label"]', "Regions / Nodes");
    cy.contains('[class*="summary--card__item--label"]', "% of Live data");
    cy.contains(
      '[class*="summary--card__item--label"]',
      "Auto stats collections",
    );
    cy.contains('[class*="summary--card__item--label"]', "Stats last updated");

    // grants of the table
    cy.contains("Grants").click();
    cy.get('[data-testid="table-grants-table"]').contains("Grantee");
    cy.get('[data-testid="table-grants-table"]').contains("Privileges");
    cy.get('[data-testid="table-grants-table"] tbody tr').should(
      "have.length.at.least",
      1,
    );

    // indexes of the table
    cy.contains("Indexes").click();

    cy.get('[data-testid="table-indexes-table"]').contains("Index Name");
    cy.get('[data-testid="table-indexes-table"]').contains("Last Read");
    cy.get('[data-testid="table-indexes-table"]').contains("Total Reads");
    cy.get('[data-testid="table-indexes-table"]').contains("Recommendations");
    cy.get('[data-testid="table-indexes-table"]').contains("Action");
    cy.get('[data-testid="table-indexes-table"] tbody tr').should(
      "have.length.at.least",
      1,
    );
  });
});
