// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

describe("demo login: tenant", () => {
  it("serves a DB Console overview page", () => {
    cy.loginOld();

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
    cy.findByText("Nodes (3)");

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
  it("logs in with a legacy cookie sitting around", () => {
    cy.setCookie("session", "abc123");
    cy.loginOld();

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
  });
  it("logs in with a legacy and new cookie sitting around", () => {
    cy.setCookie("session", "abc123");
    cy.setCookie("multitenant-session", "abc123");
    cy.visit({
      url: "/",
      failOnStatusCode: true,
    });
    cy.findByText("Log in to the DB Console");

    cy.loginOld();

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
  });
  it("logs in with junk cookies and a bad tenant cookie sitting around", () => {
    cy.setCookie("session", "abc123");
    cy.setCookie("multitenant-session", "abc123");
    cy.setCookie("tenant", "bad-tenant");
    cy.visit({
      url: "/",
      failOnStatusCode: true,
    });
    cy.findByText("Log in to the DB Console");

    cy.loginOld();

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
  });
  it("logs in with old multi-tenant session sitting around", () => {
    cy.setCookie("multitenant-session", "old=abc123");
    cy.setCookie("tenant", "old");
    cy.visit({
      url: "/",
      failOnStatusCode: true,
    });
    cy.findByText("Log in to the DB Console");

    cy.loginOld();

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
  });
});
