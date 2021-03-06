// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
describe("Transactions - Visual regression tests", () => {
  it("renders default view", () => {
    cy.visit("#/transactions");
    cy.contains("Transactions");
    cy.get("table").contains("transactions").contains("latency");
  });
});

describe.only("Transactions - Check whether the transactions match on the page", () => {
  it("renders default view", () => {
    // cy.exec("cockroach workload init movr --drop");
    cy.exec(
      'cockroach sql --insecure --execute="create table test(string name);show tables";'
    );
    cy.visit("#/transactions");
  });
});
