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
    cy.get("section").contains("Statements");
  });
});

describe("Statements - can successfully activate diagnostics", () => {
  it("Writes some queries and activates diagnostics", () => {
    cy.visit("#/statements");
    cy.exec('cockroach sql --insecure --execute="create table test (id int)";');
    cy.exec(
      'cockroach sql --insecure --execute="insert into test values (1)";'
    );
    cy.log("Searching for the specific and activating its diagnostics");
    cy.findAllByPlaceholderText("Search Statement")
      .should("exist")
      .click()
      .type("insert into test");
    cy.findAllByRole("button", "Enter").eq(0).click();
    cy.findAllByText("Statements");
    cy.log(
      "Check whether the queries contain 'Activate' button and then click"
    );
    cy.findByText("Activate").eq(0).click().should("exist");
    cy.log("Click the button for the diagonistics");
    cy.findAllByText("Activate").eq(1).click();
    cy.findAllByText("WAITING").should("exist");
    cy.exec(
      'cockroach sql --insecure --execute="insert into test values (1)";'
    );
    cy.reload();
    cy.findAllByText("Activate").should("exist");
  });
});

// TODO: Download and check the downloaded diagonistics file
