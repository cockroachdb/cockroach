// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

describe("Cluster Overview - check the number of live nodes and total ranges", () => {
  it("renders default view", () => {
    cy.visit("#/overview");
    cy.findAllByText("Capacity Usage");
    cy.findByText("Node Status");
    cy.findByText("Replication Status");
    cy.log("find the number of live nodes");
    cy.findAllByText("1");
    cy.log("find the number of ranges in replication status");
    cy.findAllByText("39");
  });
});

describe("Cluster Overview - Check Node map", () => {
  it("checks node maps information", () => {
    cy.visit("#/overview");
    cy.findAllByText("Node List").eq(1).click({ force: true });
    cy.findAllByText("Node Map").click();
    cy.log("Show the Node Map");
    cy.findByText("View the Node Map");
  });
});
