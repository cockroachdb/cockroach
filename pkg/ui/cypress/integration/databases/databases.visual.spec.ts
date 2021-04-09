// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

describe("Databases - show databases without any tables", () => {
  it("renders default view", () => {
    cy.visit("#/databases/tables");
    cy.findAllByText("Databases").should("exist");
    cy.findAllByText("defaultdb").should("exist");
    cy.findAllByText("postgres").should("exist");
  });
});

describe("Databases - show databases with tables", () => {
  it("renders the view for databases/tables view", () => {
    cy.visit("#/databases/tables");
    cy.exec('cockroach sql --insecure --execute="create table test (id int)";');
    cy.exec(
      'cockroach sql --insecure --execute="create table test1 (id int)";'
    );
    cy.exec(
      'cockroach sql --insecure --execute="create table test2 (id int)";'
    );
    cy.reload();
    cy.log("check whether the tables exit in the database");
    cy.findAllByText("public.test").should("exist");
    cy.findAllByText("public.test1").should("exist");
    cy.findAllByText("public.test2").should("exist");
  });
});
