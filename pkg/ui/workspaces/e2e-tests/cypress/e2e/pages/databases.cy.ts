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
    }

    // make 2 checks: once with a direct navigation and once after a refresh
    checkDatabases();
    cy.reload();
    checkDatabases();
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

  it("displays database metadata", () => {
    cy.get("table tbody").within(() => {
      cy.contains("tr", "system").within(() => {
        cy.get("a").contains("system").click();
      });
    });
    cy.location("hash").should("include", "/databases/1");

    cy.get('input[placeholder="Search tables"]').should("exist");
    cy.get(".crdb-ant-select-selector")
      .contains("Select Nodes")
      .should("exist");

    // Table of tables assertions
    cy.get(".crdb-ant-table-container").within(() => {
      cy.get("table").should("exist");

      cy.get("table thead").within(() => {
        cy.contains("th", "Name");
        cy.contains("th", "Replication Size");
        cy.contains("th", "Ranges");
        cy.contains("th", "Columns");
        cy.contains("th", "Indexes");
        cy.contains("th", "Regions / Nodes");
        cy.contains("th", "% of Live data");
        cy.contains("th", "Table auto stats enabled");
        cy.contains("th", "Stats last updated");
      });

      cy.get("table tbody tr").should("have.length.at.least", 1);
    });

    // Click the 'Grants' tab
    cy.get(".crdb-ant-tabs-nav-list").within(() => {
      cy.contains(
        '[data-node-key="grants"] .crdb-ant-tabs-tab-btn, .crdb-ant-tabs-tab-btn',
        "Grants",
      ).click();
    });

    cy.get(".crdb-ant-table-container").within(() => {
      cy.get("table thead").within(() => {
        cy.contains("th", "Grantee");
        cy.contains("th", "Privileges");
      });

      cy.get("table tbody tr").should("have.length.at.least", 1);
    });
  });

  it("displays table metadata", () => {
    cy.get("table tbody").within(() => {
      cy.contains("tr", "system").within(() => {
        cy.get("a").contains("system").click();
      });
    });
    cy.get(".crdb-ant-table-container").within(() => {
      cy.get("table tbody tr")
        .first()
        .within(() => {
          cy.get("td")
            .eq(0)
            .find("a")
            .then(($a) => {
              cy.wrap($a).click();
            });
        });
    });
    cy.location("hash").should("match", /\/table\/\d+/);

    // summaries of table
    const summaryLabels = [
      "Size",
      "Ranges",
      "Replicas",
      "Regions / Nodes",
      "% of Live data",
      "Auto stats collections",
      "Stats last updated",
    ];

    cy.wrap(summaryLabels).each((label) => {
      cy.contains(".summary--card__item--label--21ANh", String(label));
    });

    // grants of the table
    cy.get(".crdb-ant-tabs-nav-list").within(() => {
      cy.contains(
        '[data-node-key="grants"] .crdb-ant-tabs-tab-btn, .crdb-ant-tabs-tab-btn',
        "Grants",
      ).click();
    });

    cy.get(".crdb-ant-table-container").within(() => {
      cy.get("table thead").within(() => {
        cy.contains("th", "Grantee");
        cy.contains("th", "Privileges");
      });

      cy.get("table tbody tr").should("have.length.at.least", 1);
    });

    // indexes of the table
    cy.get(".crdb-ant-tabs-nav-list").within(() => {
      cy.contains(
        '[data-node-key="grants"] .crdb-ant-tabs-tab-btn, .crdb-ant-tabs-tab-btn',
        "Indexes",
      ).click();
    });

    cy.get(".crdb-ant-table-container").within(() => {
      cy.get("table thead").within(() => {
        cy.contains("th", "Index Name");
        cy.contains("th", "Last Read");

        cy.contains("th", "Total Reads");
        cy.contains("th", "Recommendations");
        cy.contains("th", "Action");
      });

      cy.get("table tbody tr").should("have.length.at.least", 1);
    });
  });
});
