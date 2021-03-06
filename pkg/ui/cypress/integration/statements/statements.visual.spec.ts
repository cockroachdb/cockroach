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
  });
});

describe("Statements - Check whether diagnostics have been downloaded", () => {
  it("renders default view", () => {
    cy.log(
      "Check whether the queries contain 'Activate' button and then click"
    );
    cy.get("td > div > button").eq(-1).should("contain", "Activate").click();
    cy.get(".ant-modal-content").contains("Activate statement diagnostics");
    cy.log("Click the button for the diagonistics");
    cy.get("button").eq(-1).should("have.text", "Activate").click();
    cy.reload();
    cy.get("td > div > div > div").should("have.text", "WAITING");
    cy.exec(
      'cockroach sql --insecure --execute="insert into test values (1)";'
    );
    cy.reload();
    cy.get("td > div").find("button").should("exist");
    cy.get("section").contains("Statements");
    cy.get(".ant-input").type("insert into test").get(".ant-btn").click();
    cy.get("tr").find("td > div > div").eq(-1).click();
    let statementBundleId = "";
    cy.get("td > div")
      .find("div > a")
      .eq(-1)
      .then((button) => {
        statementBundleId = button.attr("href")?.split("/")[3] || "";
        // console.log(statementBundleId);
        // console.log(button.attr("href"));
        cy.get("td > div").find("div").eq(-1).click();
        // cy.readFile(
        //   `/Users/nandupokhrel/Downloads/stmt-bundle-${statementBundleId}.zip`
        // ).should("exist");
      });
  });
});
