// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace -- required for declaration merging
  namespace Cypress {
    interface Chainable {
      /**
       * Sets a session cookie for the demo user on the current
       * database.
       * @example
       * cy.login();
       * cy.visit("#/some/authenticated/route");
       */
      login(): void;
    }
  }
}
