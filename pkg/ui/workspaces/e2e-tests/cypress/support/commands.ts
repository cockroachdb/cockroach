// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/cypress/add-commands";
import { SQLPrivilege, User } from "./types";

Cypress.Commands.add("login", (username: string, password: string) => {
  cy.visit("/");
  cy.get('input[name="username"]').type(username);
  cy.get('input[name="password"]').type(password);
  cy.findByText("Log in").click();
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
