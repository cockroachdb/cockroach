// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

describe("Scale Testing - DB Console", () => {
  beforeEach(() => {
    cy.login("roachprod", "cockroachdb");
  });

  it("loads the overview page within acceptable time", () => {
    cy.measureRender(
      "Overview Page",
      () => cy.visit("#/"),
      () =>
        cy
          .get(".node-liveness.cluster-summary__metric.live-nodes", {
            timeout: 30000,
          })
          .invoke("text")
          .then((text) => {
            const value = parseInt(text.trim(), 10);
            expect(value).to.be.greaterThan(0);
          }),
    );

    cy.get(".cluster-overview").should("be.visible");
  });
  it("loads the Metrics page and displays the correct heading and key elements", () => {
    cy.measureRender(
      "Metrics Page Graph",
      () => cy.visit("#/metrics/hardware/cluster"),
      () =>
        cy.get("canvas", { timeout: 30000 }).then(($canvas) => {
          const canvas = $canvas[0];
          const ctx = canvas.getContext("2d");
          const { width, height } = canvas;
          const imageData = ctx.getImageData(0, 0, width, height).data;

          // Check if any pixel is not fully transparent (alpha > 0)
          const hasPixels = Array.from({ length: width * height }).some(
            (_, i) => {
              return imageData[i * 4 + 3] !== 0; // alpha channel
            },
          );

          expect(hasPixels).to.be.true;
        }),
    );

    cy.get("h3").contains("Metrics").should("be.visible");
    cy.contains("Dashboard:").should("be.visible");
    cy.get("button")
      .contains(/Past Hour|1h/)
      .should("be.visible");
  });

  it("loads the Databases page, refreshes, and displays the correct heading and key elements", () => {
    cy.measureRender(
      "Databases Page",
      () => cy.visit("#/databases"),
      () =>
        cy
          .get("[data-row-key='1']", { timeout: 30000 })
          .contains("system")
          .should("exist"),
    );

    cy.get("h3").contains("Databases").should("be.visible");
  });

  it("loads the SQL Activity page and displays the correct heading and key elements", () => {
    cy.measureRender(
      "SQL Activity Page Table",
      () => cy.visit("#/sql-activity"),
      () => cy.get("table tr td", { timeout: 30000 }).should("exist"),
    );

    cy.get("h3").contains("SQL Activity").should("be.visible");
    cy.get('[role="tablist"]').should("be.visible");
    cy.get('[role="tab"]').contains("Statements").should("be.visible");
    cy.get('[role="tab"]').contains("Transactions").should("be.visible");
    cy.get('[role="tab"]').contains("Sessions").should("be.visible");
    cy.get("button")
      .contains(/Past Hour|1h/)
      .should("be.visible");
  });

  it("loads the Jobs page and displays the correct heading and key elements", () => {
    cy.measureRender(
      "Jobs Page Table",
      () => cy.visit("#/jobs"),
      () => cy.get("table tr td", { timeout: 30000 }).should("exist"),
    );

    cy.get("h3").contains("Jobs").should("be.visible");

    cy.get("button")
      .contains(/Status:/)
      .should("be.visible");
    cy.get("button").contains(/Type:/).should("be.visible");
    cy.get("button").contains(/Show:/).should("be.visible");
  });

  it("loads the Network page and displays the correct heading and key elements", () => {
    cy.measureRender(
      "Network Page Table",
      () => cy.visit("#/reports/network/region"),
      () => cy.get("table", { timeout: 30000 }).should("exist"),
    );

    cy.get("h3").contains("Network").should("be.visible");
    cy.contains("Sort By:").should("be.visible");
  });

  it("loads the Insights page and displays the correct heading and key elements", () => {
    cy.measureRender(
      "Insights Page Table",
      () => cy.visit("#/insights"),
      () => cy.get("table tr td", { timeout: 30000 }).should("exist"),
    );

    cy.get("h3").contains("Insights").should("be.visible");
    cy.get('[role="tablist"]').should("be.visible");
    cy.get('[role="tab"]').contains("Workload Insights").should("be.visible");
    cy.get('[role="tab"]').contains("Schema Insights").should("be.visible");
    cy.get("button")
      .contains(/Past Hour|1h/)
      .should("be.visible");
  });

  it("loads the Hot Ranges page and displays the correct heading and key elements", () => {
    cy.measureRender(
      "Hot Ranges Table",
      () => cy.visit("#/hotranges"),
      () => cy.get("table tr td", { timeout: 30000 }).should("exist"),
    );
  });

  it("loads the Schedules page and displays the correct heading and key elements", () => {
    cy.measureRender(
      "Schedules Page",
      () => cy.visit("#/schedules"),
      () => cy.get("table tr td", { timeout: 30000 }).should("exist"),
    );

    cy.get("h3").contains("Schedules").should("be.visible");
    cy.get("button")
      .contains(/Status:/)
      .should("be.visible");
    cy.get("button").contains(/Show:/).should("be.visible");
  });
});
