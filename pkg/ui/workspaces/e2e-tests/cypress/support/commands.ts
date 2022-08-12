// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
