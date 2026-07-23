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
    // Wait for login redirect to complete before navigating further
    cy.location("hash").should("equal", "#/overview/list");
    cy.visit("#/topranges");
  });

  commonChecks();

  it("displays ranges controls", () => {
    // node selector
    cy.get('[name="nodeRegions"]').within(() => {
      cy.contains("Select Nodes");
    });

    // filter button
    cy.contains("button", "Filter");

    // Verify filter options
    cy.get('[class*="crl-dropdown__container"]').within(() => {
      cy.contains("Databases");
      cy.contains("Table");
      cy.contains("Index");
      cy.contains("Store ID");
    });
  });

  it("displays top ranges table", () => {
    cy.get('[data-testid="top-ranges-table"]').should("exist");

    cy.get('[data-testid="top-ranges-table"]').contains("Range ID");
    cy.get('[data-testid="top-ranges-table"]').contains("QPS");
    cy.get('[data-testid="top-ranges-table"]').contains("CPU");
    cy.get('[data-testid="top-ranges-table"]').contains("Write (keys)");
    cy.get('[data-testid="top-ranges-table"]').contains("Write (bytes)");
    cy.get('[data-testid="top-ranges-table"]').contains("Read (keys)");
    cy.get('[data-testid="top-ranges-table"]').contains("Read (bytes)");
    cy.get('[data-testid="top-ranges-table"]').contains("Nodes");
    cy.get('[data-testid="top-ranges-table"]').contains("Store ID");
    cy.get('[data-testid="top-ranges-table"]').contains("Leaseholder");
    cy.get('[data-testid="top-ranges-table"]').contains("Database");
    cy.get('[data-testid="top-ranges-table"]').contains("Table");
    cy.get('[data-testid="top-ranges-table"]').contains("Index");
    cy.get('[data-testid="top-ranges-table"]').contains("Locality");
  });

  it("verify node selector", () => {
    cy.get('[name="nodeRegions"]').click();
    cy.get('[class*="crdb-ant-select-dropdown"]:not([class*="hidden"])').within(
      () => {
        cy.contains("label", "n1").click();
        cy.contains("button", "Apply").click();
      },
    );

    cy.get('[data-testid="top-ranges-table"] tbody tr').should(
      "have.length.at.least",
      1,
    );
  });
});
