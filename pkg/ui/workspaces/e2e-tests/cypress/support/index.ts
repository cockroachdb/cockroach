// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// ***********************************************************
// This support/index.ts is processed and
// loaded automatically before your test files.
//
// This is a great place to put global configuration and
// behavior that modifies Cypress.
//
// You can change the location of this file or turn off
// automatically serving support files with the
// 'supportFile' configuration option.
//
// You can read more here:
// https://on.cypress.io/configuration
// ***********************************************************

import "./commands";
import { SQLPrivilege, User } from "./types";

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace -- required for declaration merging
  namespace Cypress {
    interface Chainable {
      /**
       * Sets a session cookie for the demo user on the current
       * database.
       * @example
       * cy.login("cypress", "tests");
       * cy.visit("#/some/authenticated/route");
       */
      login(username: string, password: string): Chainable<void>;
      getUserWithExactPrivileges(privs: SQLPrivilege[]): Chainable<User>;
    }
  }
}
