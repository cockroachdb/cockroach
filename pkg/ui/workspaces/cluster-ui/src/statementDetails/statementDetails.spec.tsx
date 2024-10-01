// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen, fireEvent } from "@testing-library/react";
import { assert } from "chai";
import React from "react";
import { MemoryRouter as Router } from "react-router-dom";
import { createSandbox } from "sinon";

import { StatementDetails, StatementDetailsProps } from "./statementDetails";
import { getStatementDetailsPropsFixture } from "./statementDetails.fixture";

const sandbox = createSandbox();

describe("StatementDetails page", () => {
  let statementDetailsProps: StatementDetailsProps;

  beforeEach(() => {
    sandbox.reset();
    statementDetailsProps = getStatementDetailsPropsFixture();
  });

  it("shows loading indicator when data is not ready yet", () => {
    statementDetailsProps.isLoading = true;
    statementDetailsProps.statementDetails = null;
    statementDetailsProps.statementsError = null;

    render(
      <Router>
        <StatementDetails {...statementDetailsProps} />
      </Router>,
    );

    screen.getByLabelText("Loading...");
  });

  it("shows error alert when `lastError` is not null", () => {
    statementDetailsProps.statementsError = new Error("Something went wrong");

    render(
      <Router>
        <StatementDetails {...statementDetailsProps} />
      </Router>,
    );

    screen.getByText("Error message: Something went wrong;", { exact: false });
  });

  // Repeat this test for dedicated vs. tenant clusters.
  describe.each([false, true])("Diagnostics tab, isTenant = %p", isTenant => {
    beforeEach(() => {
      statementDetailsProps.isTenant = isTenant;
    });

    it("switches to the Diagnostics tab and triggers the activation modal", () => {
      const onTabChangeSpy = jest.fn();
      const refreshNodesClickSpy = sandbox.spy();
      const refreshNodesLivenessClickSpy = sandbox.spy();
      const refreshStatementDiagnosticsRequestsClickSpy = sandbox.spy();
      const onDiagnosticsActivateClickSpy = sandbox.spy();

      render(
        <Router>
          <StatementDetails
            {...statementDetailsProps}
            onTabChanged={onTabChangeSpy}
            createStatementDiagnosticsReport={onDiagnosticsActivateClickSpy}
            refreshNodes={refreshNodesClickSpy}
            refreshNodesLiveness={refreshNodesLivenessClickSpy}
            refreshStatementDiagnosticsRequests={
              refreshStatementDiagnosticsRequestsClickSpy
            }
          />
        </Router>,
      );

      // Click on the Diagnostics tab.
      fireEvent.click(screen.getByText("Diagnostics (1)"));
      expect(onTabChangeSpy).toHaveBeenCalledWith("diagnostics");

      fireEvent.click(
        screen.getByRole("button", { name: "Activate diagnostics" }),
      );

      onDiagnosticsActivateClickSpy.calledOnceWith(
        statementDetailsProps.statementDetails.statement.metadata.query,
      );

      // Expect calls to refresh page information on load and update events. Node
      // and liveness refreshes should not happen for tenants.
      assert.equal(
        refreshNodesClickSpy.callCount,
        isTenant ? 0 : 3,
        "refresh nodes",
      );
      assert.equal(
        refreshNodesLivenessClickSpy.callCount,
        isTenant ? 0 : 3,
        "refresh liveness",
      );
      assert.isTrue(
        refreshStatementDiagnosticsRequestsClickSpy.calledThrice,
        "refresh diagnostics requests",
      );
    });

    it("shows completed diagnostics report", () => {
      // Navigate to correct tab.
      statementDetailsProps.history.location.search = new URLSearchParams([
        ["tab", "diagnostics"],
      ]).toString();

      render(
        <Router>
          <StatementDetails {...statementDetailsProps} />
        </Router>,
      );

      screen.getByText("Dec 08, 2021 at 9:51 UTC", { exact: false });
    });
  });
});
