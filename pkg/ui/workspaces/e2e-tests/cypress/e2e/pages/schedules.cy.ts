// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";
import commonChecks from "../../support/commonChecks";

describe("schedules page", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    cy.visit("#/schedules");
  });

  commonChecks();

  it("displays schedules controls", () => {
    // Status dropdown
    cy.get('[class*="crl-dropdown__handler"]')
      .first()
      .find("button")
      .should("exist")
      .should("contain", "Status");
    cy.get('[class*="crl-dropdown__handler"]')
      .first()
      .parent()
      .within(() => {
        cy.get('[class*="crl-dropdown__overlay"]').within(() => {
          cy.contains("div", "All").should("exist");
          cy.contains("div", "Active").should("exist");
          cy.contains("div", "Paused").should("exist");
        });
      });

    // Show dropdown
    cy.get('[class*="crl-dropdown__handler"]')
      .eq(1)
      .find("button")
      .should("exist")
      .should("contain", "Show");
    cy.get('[class*="crl-dropdown__handler"]')
      .eq(1)
      .parent()
      .within(() => {
        cy.get('[class*="crl-dropdown__overlay"]').within(() => {
          cy.contains("div", "Latest 50").should("exist");
          cy.contains("div", "All").should("exist");
        });
      });
  });

  it("displays schedules table with column headers and rows", () => {
    cy.get("table[class*='schedules-table']").should("exist");

    // Verify key column headers are present
    cy.get("table[class*='schedules-table'] thead").within(() => {
      cy.contains("th", "Schedule ID").should("exist");
      cy.contains("th", "Label").should("exist");
      cy.contains("th", "Status").should("exist");
      cy.contains("th", "Next Execution Time (UTC)").should("exist");
      cy.contains("th", "Recurrence").should("exist");
      cy.contains("th", "Jobs Running").should("exist");
      cy.contains("th", "Owner").should("exist");
      cy.contains("th", "Creation Time (UTC)").should("exist");
    });

    // Verify table body has at least one row (sql-stats-compaction)
    cy.get("table[class*='schedules-table'] tbody tr").should(
      "have.length.at.least",
      1,
    );
  });

  it("displays schedule detail summary card when clicking schedule id", () => {
    cy.get("table[class*='schedules-table'] tbody tr")
      .first()
      .find("a")
      .first()
      .click();

    // Verify summary card exists
    cy.get('[class*="summary--card"]').should("have.length.at.least", 2);
    cy.get('[class*="summary--card"]')
      .first()
      .within(() => {
        cy.contains("h4", "Label").should("exist");
        cy.contains("h4", "Status").should("exist");
        cy.contains("h4", "State").should("exist");
      });

    // Verify second summary card fields
    cy.get('[class*="summary--card"]')
      .last()
      .within(() => {
        cy.contains("h4", "Creation Time").should("exist");
        cy.contains("h4", "Next Execution Time").should("exist");
        cy.contains("h4", "Recurrence").should("exist");
        cy.contains("h4", "Jobs Running").should("exist");
        cy.contains("h4", "Owner").should("exist");
      });
  });
});
