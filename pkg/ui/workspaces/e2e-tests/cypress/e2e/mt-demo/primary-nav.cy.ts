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

    cy.get('[href="#/metrics"]').click();

    cy.findByText("Metrics").should("exist");
  });
});
