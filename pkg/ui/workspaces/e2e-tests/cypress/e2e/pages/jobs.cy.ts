// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";
import commonChecks from "../../support/commonChecks";

describe("jobs page", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    // Wait for login redirect to complete before navigating further
    cy.location("hash").should("equal", "#/overview/list");
    cy.visit("#/jobs");
  });

  commonChecks();

  it("displays jobs controls", () => {
    cy.get('[class*="crl-dropdown__handler"]').eq(0).contains("Status");

    cy.get('[class*="crl-dropdown__handler"]').eq(1).contains("Type");

    cy.get('[class*="crl-dropdown__handler"]').eq(2).contains("Show");
  });

  it("displays jobs table with dynamic columns", () => {
    cy.get("table[class*='jobs-table']").should("exist");

    // default column headers
    cy.get('[class*="jobs-table"]').contains("Description");
    cy.get('[class*="jobs-table"]').contains("Status");
    cy.get('[class*="jobs-table"]').contains("Job ID");
    cy.get('[class*="jobs-table"]').contains("User Name");
    cy.get('[class*="jobs-table"]').contains("Creation Time (UTC)");
    cy.get('[class*="jobs-table"]').contains("Last Modified Time (UTC)");
    cy.get('[class*="jobs-table"]').contains("Completed Time (UTC)");

    // table body has at least one row ("intitialize the cluster")
    cy.get("[class*='jobs-table'] tbody tr").should("have.length.at.least", 1);

    // non-default header
    cy.get('[class*="jobs-table"]').should(
      "not.contain.text",
      "Coordinator Node",
    );
    cy.contains("Columns").click();
    cy.get('[class*="dropdown-area"]')
      .should("exist")
      .within(() => {
        cy.contains("Coordinator Node").click();
        cy.contains("Apply").click();
      });
    cy.get('[class*="jobs-table"]').contains("Coordinator Node");
  });

  it("displays job detail summary card when clicking job", () => {
    cy.get('[class*="jobs-table"] tbody tr')
      .first()
      .within(() => {
        cy.get("a").first().click();
      });

    cy.location("hash").should("include", "/jobs/");

    cy.get('[class*="summary--card"]').should("exist");
    cy.get('[class*="summary--card"]').contains("Status");
    cy.get('[class*="summary--card"]').contains("Creation Time");
    cy.get('[class*="summary--card"]').contains("Last Modified Time");
    cy.get('[class*="summary--card"]').contains("User Name");
    cy.get('[class*="summary--card"]').contains("Coordinator Node");

    cy.get('[class*="job-messages"]').should("exist");
    cy.get('[class*="job-messages"]').contains("When");
    cy.get('[class*="job-messages"]').contains("Kind");
    cy.get('[class*="job-messages"]').contains("Message");

    // Verify events table has at least one row
    cy.get('[class*="job-messages"] table tbody tr').should(
      "have.length.at.least",
      1,
    );

    // switch to advance debugging tab
    cy.contains("Advanced Debugging").click();
    cy.location("hash").should("match", /\/jobs\/\d+\?tab=profiler/);
  });
});
