// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

describe("Transactions - Check whether the table content has been rendered properly", () => {
  it("renders default view", () => {
    cy.visit("#/transactions");
    cy.findAllByText("Transactions").should("exist");
  });
});

describe("Transactions - Check the transactions statistics show up properly", () => {
  it("Writes some queries and check transactions", () => {
    cy.visit("#/transactions");
    cy.exec(
      'cockroach sql --insecure --execute="BEGIN; CREATE TABLE if not exists cypress_test (id int); INSERT INTO cypress_test VALUES (234); INSERT INTO cypress_test VALUES (234); SELECT * FROM cypress_test; COMMIT;"'
    );
    cy.reload();
    cy.findByPlaceholderText("Search transactions")
      .should("exist")
      .click()
      .type("CREATE TABLE cypress_test");
    cy.findAllByRole("button", "Enter").click();
    cy.findAllByText("cypress_test").should("exist").click();
    cy.log(
      "Click into the particular transaction and check if it has been executed properly"
    );
    cy.findByText("Transaction Details").should("exist");
    cy.findAllByText("CREATE TABLE IF").should("exist");
    cy.findAllByText("INSERT INTO cypress_test").should("exist");
    cy.findAllByText("SELECT FROM cypress_test").should("exist");
  });
});
