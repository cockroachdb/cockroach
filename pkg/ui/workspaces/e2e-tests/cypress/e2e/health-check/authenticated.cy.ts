// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";

describe("health check: authenticated user", () => {
  it("serves a DB Console overview page", () => {
    cy.getUserWithExactPrivileges([SQLPrivilege.ADMIN]);
    cy.fixture("users").then((users) => {
      cy.login(users[0].username, users[0].password);
    });

    // Ensure that something reasonable renders at / when authenticated, making
    // just enough assertions to ensure the right page loaded. If this test
    // fails, the server probably isn't running or authentication is broken.
    cy.visit({
      url: "/",
      failOnStatusCode: true,
    });

    // Ensure the Cluster ID appears
    cy.findByText(
      /^Cluster id: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
      { timeout: 30_000 },
    );

    // Check (roughly) for "Overview" tab contents
    cy.findByText("Capacity Usage", { selector: "h3>span" });
    cy.findByText("Node Status");
    cy.findByText("Replication Status");
    cy.findByText("Nodes (1)");

    // Check for sidebar contents
    cy.findByRole("navigation").within(() => {
      cy.findByRole("link", { name: "Overview" });
      cy.findByRole("link", { name: "Metrics" });
      cy.findByRole("link", { name: "Databases" });
      cy.findByRole("link", { name: "SQL Activity" });
      cy.findByRole("link", { name: "Hot Ranges" });
      cy.findByRole("link", { name: "Jobs" });
      cy.findByRole("link", { name: "Advanced Debug" });
    });
  });
});
