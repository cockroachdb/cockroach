// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

describe("Statements - Check whether the table content has been rendered properly", () => {
  it("renders default view", () => {
    cy.visit("#/statements");
    cy.log(
      "Check whether 'Statement' text exists on the page before any statements"
    );
    cy.get("section").contains("Statements");
  });
});

describe("Statements - Check whether the written queries match on the statement page", () => {
  it("Writes some queries and checks them", () => {
    cy.exec("cockroach workload init movr --drop");
    cy.exec('cockroach sql --insecure --execute="create table test (id int)";');
    cy.exec(
      'cockroach sql --insecure --execute="insert into test values (1)";'
    );
    cy.visit("#/statements");

    cy.get(".ant-input").type("insert into test").get(".ant-btn").click();
    cy.log("Check whether the queries are there");
    cy.get("a > div").eq(-1).should("contain", "INSERT");
    cy.log(
      "Check whether the queries contain 'Activate' button and then click"
    );
    cy.get("td > div > button").eq(-1).should("contain", "Activate").click();
    cy.get(".ant-modal-content").contains("Activate statement diagnostics");
    cy.log("Click the button for the diagonistics");
    cy.get("div> div > div > div > div > button").eq(2).click();
    cy.exec(
      'cockroach sql --insecure --execute="insert into test values (1)";'
    );
    cy.reload();
  });
});

describe("Statements - Check whether diagnostics have been downloaded", () => {
  it("renders default view", () => {
    cy.visit("#/statements");
    cy.get(".ant-input").type("insert into test").get(".ant-btn").click();
    cy.get("tbody >tr > td > div > div > div").eq(-1).click();
    cy.get("td>  div > div > div > div > div > div > div").eq(-1).click();
    // cy.readFile("/Users/nandupokhrel/Downloads/stmt-bundle/**.zip").should(
    //   "exist"
    // );
  });
});
