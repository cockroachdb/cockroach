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
    cy.visit("#/databases");
  });

  commonChecks();

  it("displays table of databases", () => {
    function checkDatabases(): void {
      cy.get("table").should("exist");
      cy.get("table").contains("Name");
      cy.get("table").contains("Size");
      cy.get("table").contains("Tables");
      cy.get("table").contains("Regions / Nodes");

      cy.get("table tbody").should("contain.text", "defaultdb");
      cy.get("table tbody").should("contain.text", "postgres");
      cy.get("table tbody").should("contain.text", "system");
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
    cy.get("table tbody").should("contain.text", "system");
    // 'defaultdb' and 'postgres' should not be present
    cy.get("table tbody").should("not.contain.text", "defaultdb");
    cy.get("table tbody").should("not.contain.text", "postgres");
  });

  it("displays database metadata", () => {
    cy.get("table tbody").contains("system").click();
    cy.location("hash").should("include", "/databases/1");

    cy.get('input[placeholder="Search tables"]').should("exist");
    cy.get(".crdb-ant-select-selector")
      .contains("Select Nodes")
      .should("exist");

    // Table of tables assertions
    cy.get("table").should("exist");
    cy.get("table").contains("Name");
    cy.get("table").contains("Replication Size");
    cy.get("table").contains("Ranges");
    cy.get("table").contains("Columns");
    cy.get("table").contains("Indexes");
    cy.get("table").contains("Regions / Nodes");
    cy.get("table").contains("% of Live data");
    cy.get("table").contains("Table auto stats enabled");
    cy.get("table").contains("Stats last updated");
    cy.get("table tbody tr").should("have.length.at.least", 1);

    // Click the 'Grants' tab
    cy.contains("Grants").click();

    cy.get("table").contains("Grantee");
    cy.get("table").contains("Privileges");
    cy.get("table tbody tr").should("have.length.at.least", 1);
  });

  it("displays table metadata", () => {
    cy.get("table tbody").contains("system").click();
    // just an arbitrary table in the systems database
    cy.get("table tbody").contains("public.comments").click();
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
    cy.get("table").contains("Grantee");
    cy.get("table").contains("Privileges");
    cy.get("table tbody tr").should("have.length.at.least", 1);

    // indexes of the table
    cy.contains("Indexes").click();

    cy.get("table").contains("Index Name");
    cy.get("table").contains("Last Read");
    cy.get("table").contains("Total Reads");
    cy.get("table").contains("Recommendations");
    cy.get("table").contains("Action");
    cy.get("table tbody tr").should("have.length.at.least", 1);
  });
});
