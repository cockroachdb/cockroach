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
    cy.visit("#/sql-activity");
  });

  commonChecks();

  it("displays selection options for statement view", () => {
    cy.get('[class*="select-options"]').should("exist");
    cy.get('[class*="radio-group"]').within(() => {
      cy.contains("label", "Statement Fingerprints").should("exist");
      cy.contains("label", "Active Executions").should("exist");

      cy.get('input[value="fingerprints"]').should("be.checked");
    });
  });

  it("displays page config list in search area", () => {
    cy.get('[class*="search-area"]').within(() => {
      cy.get('[class*="page-config__list"]').should("exist");

      cy.contains("label", "Top").should("exist");
      cy.get('[class*="dropdown-value-small"]').contains("100");

      cy.contains("label", "By").should("exist");
      cy.get('[class*="dropdown-value-medium"]').contains("% of All Runtime");

      cy.contains("label", "Time Range").should("exist");
      cy.get('[class*="range__range-title"]').contains("1h");
      cy.get('[class*="Select-value-label"]').contains("Past Hour");

      cy.get('button[aria-label="previous time interval"]').should("exist");
      cy.get('button[aria-label="next time interval"]').should("exist");
      cy.get("button").contains("Now").should("exist");

      cy.get("button").contains("Apply").should("exist");
    });
  });

  it("displays statement filter list in table area", () => {
    cy.get('[class*="table-area"]').within(() => {
      cy.get('[class*="page-config__list"]').should("exist");

      cy.get('input[placeholder="Search Statements"]').should("exist");

      cy.get('[class*="dropdown-btn"]').contains("Filters").should("exist");

      cy.get("button").contains("Columns").should("exist");
    });
  });

  it("displays table of statements", () => {
    cy.get('[class*="statements-table"]').should("exist");
    cy.get('[class*="statements-table"]').within(() => {
      cy.get("thead").within(() => {
        // Check for default column headers
        cy.contains("th", "Statements");
        cy.contains("th", "Execution Count");
        cy.contains("th", "Application Name");
        cy.contains("th", "Statement Time");
        cy.contains("th", "% of All Runtime");
        cy.contains("th", "Contention Time");
        cy.contains("th", "SQL CPU Time");
        cy.contains("th", "KV CPU Time");
        cy.contains("th", "Admission Wait Time");
        cy.contains("th", "Max Latency");
        cy.contains("th", "Rows Processed");
        cy.contains("th", "Bytes Read");
        cy.contains("th", "Max Memory");
        cy.contains("th", "Network");
        cy.contains("th", "Retries");
        cy.contains("th", "Diagnostics");
      });
    });
  });

  it("displays active executions table when selected", () => {
    cy.get('input[value="active"]').click();
    cy.get('input[value="active"]').should("be.checked");

    cy.get('[class*="page-config__list"]').should("exist");
    cy.get('input[placeholder="Search Statements"]').should("exist");
    cy.get('[class*="dropdown-btn"]').contains("Filters").should("exist");
    cy.get('[class*="refresh-text"]').contains("Refresh").should("exist");
    cy.get('[class*="crdb-ant-switch"]').should("exist");

    cy.get('[class*="statements-table"]').should("exist");
    cy.get('[class*="statements-table"]').within(() => {
      cy.get("thead").within(() => {
        cy.contains("th", "Statement Execution ID");
        cy.contains("th", "Statement Execution");
        cy.contains("th", "Status");
        cy.contains("th", "Start Time");
        cy.contains("th", "Elapsed Time");
        cy.contains("th", "Time Spent Waiting");
        cy.contains("th", "Isolation Level");
        cy.contains("th", "Application");
      });
    });
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
      cy.get('[class*="page-config__list"]').should("exist");

      cy.contains("label", "Top").should("exist");
      cy.get('[class*="dropdown-value-small"]').contains("100");

      cy.contains("label", "By").should("exist");
      cy.get('[class*="dropdown-value-medium"]').contains("Transaction Time");

      cy.contains("label", "Time Range").should("exist");
      cy.get('[class*="range__range-title"]').contains("1h");
      cy.get('[class*="Select-value-label"]').contains("Past Hour");

      cy.get('button[aria-label="previous time interval"]').should("exist");
      cy.get('button[aria-label="next time interval"]').should("exist");
      cy.get("button").contains("Now").should("exist");

      cy.get("button").contains("Apply").should("exist");
    });
  });

  it("displays transaction filter list in table area", () => {
    cy.get('[class*="table-area"]').within(() => {
      cy.get('[class*="page-config__list"]').should("exist");

      cy.get('input[placeholder="Search Statements"]').should("exist");

      cy.get('[class*="dropdown-btn"]').contains("Filters").should("exist");

      cy.get("button").contains("Columns").should("exist");
    });
  });

  it("displays table of transactions", () => {
    cy.get('[class*="statements-table"]').should("exist");
    cy.get('[class*="statements-table"]').within(() => {
      cy.get("thead").within(() => {
        // Check for default column headers
        cy.contains("th", "Transactions");
        cy.contains("th", "Execution Count");
        cy.contains("th", "Application Name");
        cy.contains("th", "Rows Processed");
        cy.contains("th", "Bytes Read");
        cy.contains("th", "Transaction Time");
        cy.contains("th", "Commit Latency");
        cy.contains("th", "Contention Time");
        cy.contains("th", "SQL CPU Time");
        cy.contains("th", "Admission Wait Time");
        cy.contains("th", "KV CPU Time");
        cy.contains("th", "Max Memory");
        cy.contains("th", "Network");
        cy.contains("th", "Retries");
      });
    });
  });

  it("displays active executions table when selected", () => {
    cy.get('input[value="active"]').click();
    cy.get('input[value="active"]').should("be.checked");

    cy.get('[class*="page-config__list"]').should("exist");
    cy.get('input[placeholder="Search Transactions"]').should("exist");
    cy.get('[class*="dropdown-btn"]').contains("Filters").should("exist");
    cy.get('[class*="refresh-text"]').contains("Refresh").should("exist");
    cy.get('[class*="crdb-ant-switch"]').should("exist");

    // Verify the transactions table is displayed with expected headers
    cy.get('[class*="cl-table-wrapper"]').within(() => {
      cy.get("table thead").within(() => {
        // Check for transaction-specific column headers
        cy.contains("th", "Transaction Execution ID");
        cy.contains("th", "Most Recent Statement");
        cy.contains("th", "Status");
        cy.contains("th", "Start Time");
        cy.contains("th", "Elapsed Time");
        cy.contains("th", "Time Spent Waiting");
        cy.contains("th", "Statements");
        cy.contains("th", "Retries");
        cy.contains("th", "Isolation Level");
        cy.contains("th", "Application");
      });
    });
  });
});

describe("SQL Activity page - Sessions Tab", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    cy.visit("#/sql-activity?tab=Sessions");
  });

  commonChecks();

  it("displays table of sessions", () => {
    cy.get('[class*="dropdown-btn"]').contains("Filters").should("exist");

    cy.get('[class*="sessions-table"]').should("exist");
    cy.get('[class*="sessions-table"]').within(() => {
      cy.get("thead").within(() => {
        // Check for default column headers
        cy.contains("th", "Session Start Time (UTC)");
        cy.contains("th", "Session Duration");
        cy.contains("th", "Session Active Duration");
        cy.contains("th", "Status");
        cy.contains("th", "Most Recent Statement");
        cy.contains("th", "Statement Start Time (UTC)");
        cy.contains("th", "Transaction Count");
        cy.contains("th", "Memory Usage");
        cy.contains("th", "Client IP Address");
        cy.contains("th", "User Name");
        cy.contains("th", "Application Name");
        cy.contains("th", "Default Isolation Level");
        cy.contains("th", "Actions");
      });
    });
  });
});
