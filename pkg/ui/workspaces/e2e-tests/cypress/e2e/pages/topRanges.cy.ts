// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";
import commonChecks from "../../support/commonChecks";

describe("top ranges page", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    cy.visit("#/topranges");
  });

  commonChecks();

  it("displays ranges controls", () => {
    // Check for node selector
    cy.get('[class*="crdb-ant-select-multiple"]')
      .should("exist")
      .within(() => {
        cy.get('[class*="crdb-ant-select-selection-placeholder"]').should(
          "contain",
          "Select Nodes",
        );
      });

    // Check for filter button
    cy.get('[class*="crl-dropdown__handler"]')
      .find("button")
      .should("exist")
      .should("contain", "Filter");

    // Verify filter dropdown contains expected filter options
    cy.get('[class*="crl-dropdown__handler"]')
      .parent()
      .within(() => {
        cy.contains("div", "Databases").should("exist");
        cy.contains("div", "Table").should("exist");
        cy.contains("div", "Index").should("exist");
        cy.contains("div", "Store ID").should("exist");
      });
  });

  it("displays top ranges table", () => {
    cy.get('[data-testid="top-ranges-table"]').should("exist");

    // Verify key column headers are present
    cy.get('[data-testid="top-ranges-table"] thead').within(() => {
      cy.contains("th", "Range ID").should("exist");
      cy.contains("th", "QPS").should("exist");
      cy.contains("th", "CPU").should("exist");
      cy.contains("th", "Write (keys)").should("exist");
      cy.contains("th", "Write (bytes)").should("exist");
      cy.contains("th", "Read (keys)").should("exist");
      cy.contains("th", "Read (bytes)").should("exist");
      cy.contains("th", "Nodes").should("exist");
      cy.contains("th", "Store ID").should("exist");
      cy.contains("th", "Leaseholder").should("exist");
      cy.contains("th", "Database").should("exist");
      cy.contains("th", "Table").should("exist");
      cy.contains("th", "Index").should("exist");
      cy.contains("th", "Locality").should("exist");
    });
  });
});
