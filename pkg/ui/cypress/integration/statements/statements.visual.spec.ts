// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

describe.only("Statements - Check whether the table content has been rendered properly", () => {
  it("renders default view", () => {
    cy.visit("#/statements");
    // cy.findAllByAltText("cockroach");
    // cy.contains("Statements");
    // cy.get("table").contains("statements").contains("latency");
  });
});

describe("Statements - Check whether the written queries match on the statement page", () => {
  it("Writes some queries and checks them", () => {
    // cy.exec("cockroach start-single-node --insecure --background");
    // cy.exec("cockroach workload init movr --drop");
    // cy.exec(
    //   'cockroach sql --insecure --execute="create table nam (str name)";'
    // );
    cy.visit("#/statements");

    cy.get(":nth-child(1) > .cl-table-link__tooltip-hover-area").contains(
      "INSERT INTO rides"
    );
    cy.get(":nth-child(1) > .cl-table-link__tooltip-hover-area").contains(
      "ALTER TABLE"
    );
    cy.get(":nth-child(1) > .cl-table-link__tooltip-hover-area").contains(
      "SELECT FROM"
    );
  });
});
