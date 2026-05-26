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

// measureRender can capture render time for a given action. Currently this will
// only work in scale tests because the `logTiming` task is only available in
// scale tests.
Cypress.Commands.add(
  "measureRender",
  { prevSubject: "optional" },
  (subject, renderName, actionFn, assertionChain) => {
    // If subject is provided, it means the command is chained (e.g., cy.get('selector').measureRender(...))
    const chainable = subject ? cy.wrap(subject) : cy;

    let startTime;

    chainable
      .then(() => {
        startTime = performance.now();
      })
      .then(() => {
        // Execute the action function provided by the user
        // The actionFn should return a chainable Cypress command
        return actionFn();
      })
      .then((actionResult) => {
        // Assert on the element/state that indicates render completion
        // The assertionChain should be a function that takes the result
        // of the action and returns a chainable assertion
        const assertionChainable = assertionChain(actionResult);

        // Ensure the assertion chain is properly waited on
        return assertionChainable.then(() => {
          const endTime = performance.now();
          const duration = endTime - startTime;
          const testTitle = Cypress.currentTest.title;

          cy.task("logTiming", {
            testTitle,
            renderName,
            duration,
          });
          cy.screenshot(`timing-${renderName}-${Date.now()}`);

          return assertionChainable;
        });
      });
  },
);
