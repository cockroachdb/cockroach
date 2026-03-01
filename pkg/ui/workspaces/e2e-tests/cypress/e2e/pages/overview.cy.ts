// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";
import { isTextGreaterThanZero } from "../../support/utils";
import commonChecks from "../../support/commonChecks";

describe("overview page", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    // Wait for login redirect to complete before navigating further
    cy.location("hash").should("equal", "#/overview/list");
  });

  commonChecks();

  it("displays capacity usage metrics", () => {
    cy.findByText("Capacity Usage", { selector: "h3>span" });
    cy.get(".cluster-summary__metric.storage-used").should(
      isTextGreaterThanZero,
    );
    cy.get(".cluster-summary__metric.storage-usable").should(
      isTextGreaterThanZero,
    );
  });

  it("displays node status metrics", () => {
    cy.findByText("Node Status");
    cy.get(".cluster-summary__label.live-nodes").should("exist");
    cy.get(".cluster-summary__metric.live-nodes").should(isTextGreaterThanZero);
    cy.get(".cluster-summary__metric.suspect-nodes").should("exist");
    cy.get(".cluster-summary__metric.draining-nodes").should("exist");
    cy.get(".cluster-summary__metric.dead-nodes").should("exist");
  });

  it("displays replication status metrics", () => {
    cy.findByText("Replication Status");
    cy.get(".cluster-summary__metric.total-ranges").should(
      isTextGreaterThanZero,
    );
    cy.get(".cluster-summary__metric.under-replicated-ranges").should("exist");
    cy.get(".cluster-summary__metric.unavailable-ranges").should("exist");
  });

  it("displays nodes list table with data", () => {
    cy.get(".table-section.embedded-table").should("exist");
    cy.get(".table-section.embedded-table table tbody tr").should(
      "have.length.at.least",
      1,
    );
    cy.contains("n1").should("have.attr", "href", "#/node/1");
    cy.contains("Logs").should("have.attr", "href", "#/node/1/logs");
  });
});
