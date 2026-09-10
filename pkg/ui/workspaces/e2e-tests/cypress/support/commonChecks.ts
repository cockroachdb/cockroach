// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { checkClusterId } from "./utils";

export function commonChecks(): void {
  it("displays cluster ID", () => {
    checkClusterId();
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
}

export default commonChecks;
