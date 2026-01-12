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
    cy.visit("#/jobs");
  });

  commonChecks();

  it("displays jobs controls", () => {
    // Status dropdown
    cy.get('[class*="crl-dropdown__handler"]')
      .first()
      .find("button")
      .should("exist")
      .should("contain", "Status");

    // Type dropdown
    cy.get('[class*="crl-dropdown__handler"]')
      .eq(1)
      .find("button")
      .should("exist")
      .should("contain", "Type");

    // Show dropdown
    cy.get('[class*="crl-dropdown__handler"]')
      .eq(2)
      .find("button")
      .should("exist")
      .should("contain", "Show");
  });

  it("displays jobs table with column headers and rows", () => {
    cy.get("table[class*='jobs-table']").should("exist");

    // Verify key column headers are present
    cy.get("table[class*='jobs-table'] thead").within(() => {
      cy.contains("th", "Description").should("exist");
      cy.contains("th", "Status").should("exist");
      cy.contains("th", "Job ID").should("exist");
      cy.contains("th", "User Name").should("exist");
      cy.contains("th", "Creation Time (UTC)").should("exist");
      cy.contains("th", "Last Modified Time (UTC)").should("exist");
      cy.contains("th", "Completed Time (UTC)").should("exist");
    });

    // Verify table body has at least one row (intitialize the cluster)
    cy.get("table[class*='jobs-table'] tbody tr").should(
      "have.length.at.least",
      1,
    );
  });

  it("displays job detail summary card when clicking job", () => {
    cy.get("table[class*='jobs-table'] tbody tr")
      .contains("intitialize the cluster")
      .closest("tr")
      .within(() => {
        cy.get("a").first().click();
      });

    cy.get('[class*="summary--card"]').should("exist");
    cy.get('[class*="summary--card"]').within(() => {
      cy.contains("h4", "Status").should("exist");
      cy.contains("h4", "Creation Time").should("exist");
      cy.contains("h4", "Last Modified Time").should("exist");
      cy.contains("h4", "Completed Time").should("exist");
      cy.contains("h4", "User Name").should("exist");
      cy.contains("h4", "Coordinator Node").should("exist");
    });

    cy.get("div[class*='job-messages']").should("exist");
    cy.get("div[class*='job-messages'] table thead").within(() => {
      cy.contains("th", "When").should("exist");
      cy.contains("th", "Kind").should("exist");
      cy.contains("th", "Message").should("exist");
    });

    // Verify events table has at least one row
    cy.get("div[class*='job-messages'] table tbody tr").should(
      "have.length.at.least",
      1,
    );
  });
});
