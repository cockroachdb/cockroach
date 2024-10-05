// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/cypress/add-commands";
import { SQLPrivilege, User } from "./types";

Cypress.Commands.add("login", (username: string, password: string) => {
  cy.request({
    method: "POST",
    url: "/api/v2/login/",
    form: true,
    body: {
      username: username,
      password: password,
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

// Gets a user from the users.json fixture with the given privileges.
Cypress.Commands.add("getUserWithExactPrivileges", (privs: SQLPrivilege[]) => {
  return cy.fixture("users").then((users) => {
    return users.find(
      (user: User) =>
        privs.every((priv) => user.sqlPrivileges.includes(priv)) ||
        (privs.length === 0 && user.sqlPrivileges.length === 0),
    );
  });
});
