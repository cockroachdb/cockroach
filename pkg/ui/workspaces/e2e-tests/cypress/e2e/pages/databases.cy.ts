// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";

describe("databases page", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    cy.visit("#/databases");
  });

  it("displays cluster ID", () => {
    cy.findByText(
      /^Cluster id: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
      { timeout: 30_000 },
    );
  });

  it("displays table of databases", () => {
    cy.get("table").should("exist");
    cy.get("table").contains("Name");
    cy.get("table").contains("Size");
    cy.get("table").contains("Tables");
    cy.get("table").contains("Regions / Nodes");

    cy.get("table tbody").within(() => {
      cy.contains("tr", "defaultdb").within(() => {
        cy.get("td").should("have.length.greaterThan", 0);
        // Size column should have a value
        cy.get("td").eq(1).invoke("text").should("not.be.empty");
        // Tables column should have a value
        cy.get("td").eq(2).invoke("text").should("not.be.empty");
      });

      cy.contains("tr", "postgres").within(() => {
        cy.get("td").should("have.length.greaterThan", 0);
        cy.get("td").eq(1).invoke("text").should("not.be.empty");
        cy.get("td").eq(2).invoke("text").should("not.be.empty");
      });

      cy.contains("tr", "system").within(() => {
        cy.get("td").should("have.length.greaterThan", 0);
        cy.get("td").eq(1).invoke("text").should("not.be.empty");
        cy.get("td").eq(2).invoke("text").should("not.be.empty");
        cy.get("td").eq(3).invoke("text").should("not.be.empty");
      });
    });
  });

  it("filters databases via search input", () => {
    cy.get('input[placeholder="Search databases"]').as("dbSearch");
    cy.get("@dbSearch").clear().type("sys{enter}");

    cy.get("table tbody")
      .should("exist")
      .within(() => {
        // 'system' should be visible
        cy.contains("tr", "system").should("exist");

        // 'defaultdb' and 'postgres' should not be present
        cy.contains("tr", "defaultdb").should("not.exist");
        cy.contains("tr", "postgres").should("not.exist");
      });
  });

  it("displays navigation sidebar", () => {
    cy.findByRole("navigation").within(() => {
      cy.findByRole("link", { name: "Overview" });
      cy.findByRole("link", { name: "Metrics" });
      cy.findByRole("link", { name: "Databases" });
      cy.findByRole("link", { name: "SQL Activity" });
      cy.findByRole("link", { name: "Insights" });
      cy.findByRole("link", { name: "Top Ranges" });
      cy.findByRole("link", { name: "Jobs" });
      cy.findByRole("link", { name: "Schedules" });
      cy.findByRole("link", { name: "Advanced Debug" });
    });
  });
});
