// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/cypress/add-commands";

Cypress.Commands.add("login", () => {
  cy.request({
    method: "POST",
    url: "/api/v2/login/",
    form: true,
    body: {
      username: Cypress.env("username"),
      password: Cypress.env("password"),
    },
    failOnStatusCode: true,
  }).then(({ body }) => {
    if ("session" in body) {
      // Set the returned session token as a cookie, emulating the 'Set-Cookie'
      // response header currently returned from CRDB:
      // Set-Cookie: session=REDACTED; Path=/; HttpOnly
      cy.setCookie("session", body.session, { path: "/", httpOnly: true });
    } else {
      throw new Error(
        `Unexpected response from /api/v2/login: ${JSON.stringify(body)}`,
      );
    }
  });
});
