// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

describe("health check: unauthenticated user", () => {
  it("serves a DB Console login page", () => {
    // Ensure that *something* renders at / as a canary for all other tests,
    // making just enough assertions to ensure the right page loaded.
    // If this test fails, the server probably isn't running.
    cy.visit({
      url: "/",
      failOnStatusCode: true,
    });
    cy.findByText("Log in to the DB Console");
    cy.findByPlaceholderText("Username");
    cy.findByPlaceholderText("Password");
  });
});
