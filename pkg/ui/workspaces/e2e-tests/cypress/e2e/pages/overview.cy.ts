// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";
import { isTextGreaterThanZero } from "../../support/e2e";

describe("overview page", () => {
  beforeEach(() => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]).then((user) => {
      cy.login(user.username, user.password);
    });
    cy.visit("#/");
  });

  it("displays cluster ID", () => {
    cy.findByText(
      /^Cluster id: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
      { timeout: 30_000 },
    );
  });

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

    cy.get(".cluster-visualization-layout").should("not.exist");
    cy.get(".cluster-visualization-layout__content").should("not.exist");
  });

  it("can switch to node map view", () => {
    cy.get(".crl-dropdown__handler button").click();
    cy.get(".crl-dropdown__overlay .crl-dropdown__item")
      .contains("Node Map")
      .click();

    cy.get(".cluster-visualization-layout").should("exist");
    cy.get(".cluster-visualization-layout__content").should("exist");

    cy.get(".table-section.embedded-table").should("not.exist");
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
