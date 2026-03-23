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
    // Wait for login redirect to complete before navigating further
    cy.location("hash").should("equal", "#/overview/list");
    cy.visit("#/schedules");
  });

  commonChecks();

  it("displays schedules controls", () => {
    cy.get('[class*="crl-dropdown__handler"]').eq(0).contains("Status");
    cy.get('[class*="crl-dropdown__overlay"]').eq(0).contains("All");
    cy.get('[class*="crl-dropdown__overlay"]').eq(0).contains("Active");
    cy.get('[class*="crl-dropdown__overlay"]').eq(0).contains("Paused");

    cy.get('[class*="crl-dropdown__handler"]').eq(1).contains("Show");
    cy.get('[class*="crl-dropdown__overlay"]').eq(1).contains("Latest 50");
    cy.get('[class*="crl-dropdown__overlay"]').eq(1).contains("All");
  });

  it("displays schedules table with column headers and rows", () => {
    cy.get('[class*="schedules-table"]').should("exist");
    // Verify column headers
    cy.get('[class*="schedules-table"] thead').contains("Schedule ID");
    cy.get('[class*="schedules-table"] thead').contains("Label");
    cy.get('[class*="schedules-table"] thead').contains("Status");
    cy.get('[class*="schedules-table"] thead').contains(
      "Next Execution Time (UTC)",
    );
    cy.get('[class*="schedules-table"] thead').contains("Recurrence");
    cy.get('[class*="schedules-table"] thead').contains("Jobs Running");
    cy.get('[class*="schedules-table"] thead').contains("Owner");
    cy.get('[class*="schedules-table"] thead').contains("Creation Time (UTC)");

    // table body has at least one schedule ("sql-stats-compaction")
    cy.get('[class*="schedules-table"] tbody tr').should(
      "have.length.at.least",
      1,
    );
  });

  it("displays schedule details when clicking schedule id", () => {
    cy.get('[class*="schedules-table"] tbody tr').first().find("a").click();
    cy.location("hash").should("include", "/schedules/");

    // Verify summary cards exists and check details
    cy.get('[class*="summary--card"]').should("have.length.at.least", 2);
    cy.get('[class*="summary--card__item"]').contains("Label");
    cy.get('[class*="summary--card__item"]').contains("Status");
    cy.get('[class*="summary--card__item"]').contains("State");

    cy.get('[class*="summary--card__item"]').contains("Creation Time");
    cy.get('[class*="summary--card__item"]').contains("Next Execution Time");
    cy.get('[class*="summary--card__item"]').contains("Recurrence");
    cy.get('[class*="summary--card__item"]').contains("Jobs Running");
    cy.get('[class*="summary--card__item"]').contains("Owner");
  });
});
