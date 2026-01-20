// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";
import commonChecks from "../../support/commonChecks";

describe("SQL Activity page - Statements Tab", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    // Wait for login redirect to complete before navigating further
    cy.location("hash").should("equal", "#/overview/list");
    cy.visit("#/sql-activity");
  });

  commonChecks();

  it("displays selection options for statement view", () => {
    cy.get('[class*="select-options"]').should("exist");
    cy.get('[class*="radio-group"]').within(() => {
      cy.contains("label", "Statement Fingerprints");
      cy.contains("label", "Active Executions");

      cy.get('input[value="fingerprints"]').should("be.checked");
    });
  });

  it("displays page config list in search area", () => {
    cy.get('[class*="search-area"]').within(() => {
      cy.get('[class*="page-config__list"]').within(() => {
        cy.contains("Top");
        cy.get('[class*="dropdown-value-small"]').contains("100");

        cy.contains("By");
        cy.get('[class*="dropdown-value-medium"]').contains("% of All Runtime");

        cy.contains("Time Range");
        cy.get('[class*="range__range-title"]').contains("1h");
        cy.get('[class*="Select-value-label"]').contains("Past Hour");

        cy.get('button[aria-label="previous time interval"]').should("exist");
        cy.get('button[aria-label="next time interval"]').should("exist");
        cy.get("button").contains("Now");

        cy.contains("Apply");
      });
    });
  });

  it("displays statement filter list in table area", () => {
    cy.get('[class*="table-area"]').within(() => {
      cy.get('[class*="page-config__list"]').within(() => {
        cy.get('input[placeholder="Search Statements"]').should("exist");

        cy.get('[class*="dropdown-btn"]').contains("Filters").should("exist");

        cy.get("button").contains("Columns").should("exist");
      });
    });
  });

  it("displays table of statements", () => {
    cy.get('[class*="statements-table"]').should("exist");
    cy.get('[class*="statements-table"]').within(() => {
      cy.get("thead").within(() => {
        // default column headers
        cy.contains("Statements");
        cy.contains("Execution Count");
        cy.contains("Application Name");
        cy.contains("Statement Time");
        cy.contains("% of All Runtime");
        cy.contains("Contention Time");
        cy.contains("SQL CPU Time");
        cy.contains("KV CPU Time");
        cy.contains("Admission Wait Time");
        cy.contains("Max Latency");
        cy.contains("Rows Processed");
        cy.contains("Bytes Read");
        cy.contains("Max Memory");
        cy.contains("Network");
        cy.contains("Retries");
        cy.contains("Diagnostics");
      });
    });

    // non-default header
    cy.get('[class*="statements-table"]').should(
      "not.contain.text",
      "Min Latency",
    );
    cy.contains("Columns").click();
    cy.get('[class*="dropdown-area"]')
      .should("exist")
      .within(() => {
        cy.contains("Min Latency").click();
        cy.contains("Apply").click();
      });
    cy.get('[class*="statements-table"]').contains("Min Latency");
  });

  it("displays statement filter list for active executions table", () => {
    cy.get('input[value="active"]').click();
    cy.get('input[value="active"]').should("be.checked");

    cy.get('[class*="page-config__list"]').should("exist");
    cy.get('input[placeholder="Search Statements"]').should("exist");
    cy.get('[class*="dropdown-btn"]').contains("Filters").should("exist");
    cy.get('[class*="refresh-text"]').contains("Refresh").should("exist");
    cy.get('[class*="crdb-ant-switch"]').should("exist");
  });

  it("displays table of active executions when selected", () => {
    cy.get('input[value="active"]').click();
    cy.get('input[value="active"]').should("be.checked");

    cy.get('[class*="statements-table"]').should("exist");
    cy.get('[class*="statements-table"]').within(() => {
      cy.get("thead").within(() => {
        cy.contains("Statement Execution ID");
        cy.contains("Statement Execution");
        cy.contains("Status");
        cy.contains("Start Time");
        cy.contains("Elapsed Time");
        cy.contains("Time Spent Waiting");
        cy.contains("Isolation Level");
        cy.contains("Application");
      });
    });

    // non-default header
    cy.contains("Columns").click();
    cy.get('[class*="dropdown-area"]')
      .should("exist")
      .within(() => {
        cy.contains("Application").click();
        cy.contains("Apply").click();
      });
    cy.get('[class*="statements-table"]').should(
      "not.contain.text",
      "Application",
    );
  });

  it("switches to transactions tab", () => {
    cy.get('[data-node-key="Transactions"] .crdb-ant-tabs-tab-btn').click();
    cy.get('[data-node-key="Transactions"]').should(
      "have.class",
      "crdb-ant-tabs-tab-active",
    );
  });

  it("switches to sessions tab", () => {
    cy.get('[data-node-key="Sessions"] .crdb-ant-tabs-tab-btn').click();
    cy.get('[data-node-key="Sessions"]').should(
      "have.class",
      "crdb-ant-tabs-tab-active",
    );
  });
});

describe("SQL Activity page - Transactions Tab", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    // Wait for login redirect to complete before navigating further
    cy.location("hash").should("equal", "#/overview/list");
    cy.visit("#/sql-activity?tab=Transactions");
  });

  commonChecks();

  it("displays selection options for transaction view", () => {
    cy.get('[class*="select-options"]').should("exist");
    cy.get('[class*="radio-group"]').within(() => {
      cy.contains("label", "Transaction Fingerprints").should("exist");
      cy.contains("label", "Active Executions").should("exist");

      cy.get('input[value="fingerprints"]').should("be.checked");
    });
  });

  it("displays page config list in search area", () => {
    cy.get('[class*="search-area"]').within(() => {
      cy.get('[class*="page-config__list"]').within(() => {
        cy.contains("Top");
        cy.get('[class*="dropdown-value-small"]').contains("100");

        cy.contains("By");
        cy.get('[class*="dropdown-value-medium"]').contains("Transaction Time");

        cy.contains("Time Range");
        cy.get('[class*="range__range-title"]').contains("1h");
        cy.get('[class*="Select-value-label"]').contains("Past Hour");

        cy.get('button[aria-label="previous time interval"]').should("exist");
        cy.get('button[aria-label="next time interval"]').should("exist");
        cy.get("button").contains("Now").should("exist");

        cy.get("button").contains("Apply").should("exist");
      });
    });
  });

  it("displays transaction filter list in table area", () => {
    cy.get('[class*="table-area"]').within(() => {
      cy.get('[class*="page-config__list"]').within(() => {
        cy.get('input[placeholder="Search Statements"]').should("exist");

        cy.get('[class*="dropdown-btn"]').contains("Filters").should("exist");

        cy.get("button").contains("Columns").should("exist");
      });
    });
  });

  it("displays table of transactions", () => {
    cy.get('[class*="statements-table"]').should("exist");
    cy.get('[class*="statements-table"]').within(() => {
      cy.get("thead").within(() => {
        // Check for default column headers
        cy.contains("Transactions");
        cy.contains("Execution Count");
        cy.contains("Application Name");
        cy.contains("Rows Processed");
        cy.contains("Bytes Read");
        cy.contains("Transaction Time");
        cy.contains("Commit Latency");
        cy.contains("Contention Time");
        cy.contains("SQL CPU Time");
        cy.contains("Admission Wait Time");
        cy.contains("KV CPU Time");
        cy.contains("Max Memory");
        cy.contains("Network");
        cy.contains("Retries");
      });
    });
  });

  it("displays transaction filter list for active executions table", () => {
    cy.get('input[value="active"]').click();
    cy.get('input[value="active"]').should("be.checked");

    cy.get('[class*="page-config__list"]').should("exist");
    cy.get('input[placeholder="Search Transactions"]').should("exist");
    cy.get('[class*="dropdown-btn"]').contains("Filters").should("exist");
    cy.get('[class*="refresh-text"]').contains("Refresh").should("exist");
    cy.get('[class*="crdb-ant-switch"]').should("exist");
  });

  it("displays active executions table when selected", () => {
    cy.get('input[value="active"]').click();
    cy.get('input[value="active"]').should("be.checked");

    cy.get('[class*="cl-table-wrapper"]').within(() => {
      cy.get("thead").within(() => {
        // Check for transaction-specific column headers
        cy.contains("Transaction Execution ID");
        cy.contains("Most Recent Statement");
        cy.contains("Status");
        cy.contains("Start Time");
        cy.contains("Elapsed Time");
        cy.contains("Time Spent Waiting");
        cy.contains("Statements");
        cy.contains("Retries");
        cy.contains("Isolation Level");
        cy.contains("Application");
      });
    });
  });
});

describe("SQL Activity page - Sessions Tab", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    // Wait for login redirect to complete before navigating further
    cy.location("hash").should("equal", "#/overview/list");
    cy.visit("#/sql-activity?tab=Sessions");
  });

  commonChecks();

  it("displays table of sessions", () => {
    cy.get('[class*="dropdown-btn"]').contains("Filters").should("exist");

    cy.get('[class*="sessions-table"]').should("exist");
    cy.get('[class*="sessions-table"]').within(() => {
      cy.get("thead").within(() => {
        // Check for default column headers
        cy.contains("Session Start Time (UTC)");
        cy.contains("Session Duration");
        cy.contains("Session Active Duration");
        cy.contains("Status");
        cy.contains("Most Recent Statement");
        cy.contains("Statement Start Time (UTC)");
        cy.contains("Transaction Count");
        cy.contains("Memory Usage");
        cy.contains("Client IP Address");
        cy.contains("User Name");
        cy.contains("Application Name");
        cy.contains("Default Isolation Level");
        cy.contains("Actions");
      });
    });
  });
});
