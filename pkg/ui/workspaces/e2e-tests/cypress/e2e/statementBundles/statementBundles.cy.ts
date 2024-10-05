// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SQLPrivilege } from "../../support/types";

// TODO (xinhaoz): This test currently only works when running pnpm run cy:run
// directly against a local cluster set up with sql activity in the last hour
// and the expected cypress users in fixtures. We need to automate this server
// setup for e2e testing.
describe("statement bundles", () => {
  const runTestsForPrivilegedUser = (privilege: SQLPrivilege) => {
    describe(`${privilege} user`, () => {
      beforeEach(() => {
        cy.getUserWithExactPrivileges([privilege]).then((user) => {
          cy.login(user.username, user.password);
        });
      });

      it("can request statement bundles", () => {
        cy.visit("#/sql-activity");
        cy.contains("button", "Apply").click();
        // Open modal.
        cy.contains("button", "Activate").click();
        // Wait for modal.
        cy.findByText(/activate statement diagnostics/i).should("be.visible");
        // Click the Activate button within the modal
        cy.get(`[role="dialog"]`) // Adjust this selector to match your modal's structure
          .contains("button", "Activate")
          .click();
        cy.findByText(/statement diagnostics were successfully activated/i);
      });

      it("can view statement bundles", () => {
        cy.visit("#/reports/statements/diagnosticshistory");
        cy.get("table tbody tr").should("have.length.at.least", 1);
      });

      it("can cancel statement bundles", () => {
        cy.visit("#/reports/statements/diagnosticshistory");
        cy.get("table tbody tr").should("have.length.at.least", 1);
        cy.contains("button", "Cancel").click();
        cy.findByText(/statement diagnostics were successfully cancelled/i);
      });
    });
  };

  const runTestsForNonPrivilegedUser = (privilege?: SQLPrivilege) => {
    beforeEach(() => {
      cy.getUserWithExactPrivileges(privilege ? [privilege] : []).then(
        (user) => {
          cy.login(user.username, user.password);
        },
      );
    });

    it("cannot request statement bundles", () => {
      cy.visit("#/sql-activity");
      cy.contains("button", "Apply").click();
      // Should not see an Activate button.
      cy.contains("button", "Activate").should("not.exist");
    });

    it("cannot view statement bundles", () => {
      cy.visit("#/reports/statements/diagnosticshistory");
      cy.findByText(/no statement diagnostics to show/i);
    });
  };

  describe("view activity user", () => {
    runTestsForPrivilegedUser(SQLPrivilege.VIEWACTIVITY);
  });

  describe("admin user", () => {
    runTestsForPrivilegedUser(SQLPrivilege.ADMIN);
  });

  describe("non-privileged VIEWACTIVITYREDACTED user", () => {
    runTestsForNonPrivilegedUser(SQLPrivilege.VIEWACTIVITYREDACTED);
  });

  describe("non-privileged user", () => {
    runTestsForNonPrivilegedUser();
  });
});
