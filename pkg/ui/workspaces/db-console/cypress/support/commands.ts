// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import "@testing-library/cypress/add-commands";
import { TIME_UNTIL_NODE_DEAD } from "./constants";
import Loggable = Cypress.Loggable;
import Timeoutable = Cypress.Timeoutable;
import Withinable = Cypress.Withinable;
import { addMatchImageSnapshotCommand } from "cypress-image-snapshot/command";

Cypress.Commands.add(
  "decommissionNode",
  (nodeId: number, wait: boolean = false) => {
    Cypress.log({
      displayName: "DECOMMISSION NODE",
      message: [`NodeID: ${nodeId}`],
    });
    const $exec = cy.exec(
      `make decommission-node NODE_ID=${nodeId} -C ./cypress`,
      { log: false, timeout: TIME_UNTIL_NODE_DEAD }
    );
    return wait ? cy.wait(TIME_UNTIL_NODE_DEAD) : $exec;
  }
);

Cypress.Commands.add("stopNode", (nodeId: number) => {
  Cypress.log({
    displayName: "STOP NODE",
    message: [`NodeID: ${nodeId}`],
  });
  const $exec = cy.exec(`make stop-node NODE_ID=${nodeId} -C ./cypress`, {
    log: false,
    timeout: TIME_UNTIL_NODE_DEAD,
  });
  return $exec;
});

Cypress.Commands.add("startCluster", () => {
  Cypress.log({
    displayName: "Start cluster",
  });
  return cy.exec(`make -C ./cypress`);
});

Cypress.Commands.add("teardown", () => {
  Cypress.log({
    displayName: "Teardown",
    message: [`Stop all nodes`],
  });
  return cy.exec(`make teardown -C ./cypress`, {
    log: false,
    failOnNonZeroExit: false,
  });
});

type CommandOptions = Partial<Loggable & Timeoutable & Withinable>;

// Override default GET function to provide custom message for logging
Cypress.Commands.overwrite(
  "get",
  (originalFn: typeof cy.get, selector: string, options?: CommandOptions) => {
    const log = options?.log || true;
    const logMessage = options?.logMessage || selector;

    if (log) {
      Cypress.log({
        displayName: "GET",
        message: [logMessage],
      });
    }
    return originalFn(selector, { log: false });
  }
);

addMatchImageSnapshotCommand("matchImageSnapshot", {
  capture: "fullPage",
  failureThreshold: 0.03,
  failureThresholdType: "percent",
});

declare global {
  namespace Cypress {
    interface Chainable<Subject> {
      stopNode: (nodeId: number) => Chainable<Subject>;
      decommissionNode: (nodeId: number, wait?: boolean) => Chainable<Subject>;
      startCluster: () => Chainable<Subject>;
      teardown: () => Chainable<Subject>;
      matchImageSnapshot: () => Chainable<Subject>;
    }

    interface Loggable {
      logMessage?: string;
    }
  }
}

export {};
