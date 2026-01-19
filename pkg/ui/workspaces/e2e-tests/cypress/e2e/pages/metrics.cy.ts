// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";
import commonChecks from "../../support/commonChecks";

describe("metrics page", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    // Wait for login redirect to complete before navigating further
    cy.location("hash").should("equal", "#/");
    // make sure we deterministic start on a fixed set of configurations
    cy.visit("#/metrics/overview/cluster?preset=past-hour");
  });

  commonChecks();

  it("displays metrics controls", () => {
    // Check for node selector
    cy.get('[class*="dropdown__title"]').contains("Graph:");
    cy.get(".Select-value-label").contains("Cluster");

    // Check for dashboard selector
    cy.get('[class*="dropdown__title"]').contains("Dashboard:");
    cy.get(".Select-value-label").contains("Overview");

    // Check for time range selector
    cy.get('[class*="range__range-title"]').contains("1h");
    cy.get('[class*="Select-value-label"]').contains("Past Hour");

    // Check for time navigation controls
    cy.get('button[aria-label="previous time interval"]').should("exist");
    cy.get('button[aria-label="next time interval"]').should("exist");
    cy.get("button").contains("Now").should("exist");
  });

  it("displays overview graphs", () => {
    cy.get('[class*="visualization-header"]').contains(
      "SQL Queries Per Second",
    );
    cy.get('[class*="visualization-header"]').contains(
      "Service Latency: SQL Statements, 99th percentile",
    );
    cy.get('[class*="visualization-header"]').contains(
      "SQL Statement Contention",
    );
    cy.get('[class*="visualization-header"]').contains("Replicas per Node");
    cy.get('[class*="visualization-header"]').contains("Capacity");
  });

  it("can switch graph view to specific node", () => {
    cy.get('[class*="dropdown__title"]').contains("Graph:").click();
    cy.get(".Select-menu-outer .Select-option").contains("n1").click();
    cy.get(".Select-value-label").contains("n1").should("exist");
    cy.get(".Select-value-label").contains("Cluster").should("not.exist");
  });

  it("can switch dashboards", () => {
    const dashboards: { [key: string]: string } = {
      Hardware: "hardware",
      Runtime: "runtime",
      Networking: "networking",
      SQL: "sql",
      Storage: "storage",
      Replication: "replication",
      Distributed: "distributed",
      Queues: "queues",
      "Slow Requests": "requests",
      Changefeeds: "changefeeds",
      Overload: "overload",
      TTL: "ttl",
      "Physical Cluster Replication": "crossClusterReplication",
      "Logical Data Replication": "logicalDataReplication",
    };

    cy.wrap(Object.entries(dashboards)).each(([title, pathSegment]) => {
      cy.get('[class*="dropdown__title"]').contains("Dashboard:").click();
      cy.get(".Select-menu-outer .Select-option")
        .contains(title)
        .scrollIntoView()
        .should("be.visible")
        .click();

      cy.location("hash").should("include", pathSegment);
    });
  });

  it("can switch time range view", () => {
    cy.get('[class*="range__range-title"]').contains("1h").click();
    cy.get('[class*="range-selector"]').find("button").contains("30m").click();
    cy.get('[class*="Select-value-label"]').contains("Past 30 Minutes");
  });

  it("displays summary side bar", () => {
    cy.get(".summary-section").within(() => {
      cy.get(".summary-label").contains("Summary");

      cy.contains("Total Nodes").should("exist");
      cy.contains("View nodes list").should(
        "have.attr",
        "href",
        "#/overview/list",
      );
      cy.contains("Capacity Usage").should("exist");
      cy.contains("Unavailable ranges").should("exist");
      cy.contains("Queries per second").should("exist");
      cy.contains("P99 latency").should("exist");
    });
  });

  it("displays events section", () => {
    cy.get(".summary-section").within(() => {
      cy.contains(".summary-label", "Events");

      cy.contains("View all events").should("have.attr", "href", "#/events");
    });
  });
});
