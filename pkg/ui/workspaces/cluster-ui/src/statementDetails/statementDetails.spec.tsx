// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { mount } from "enzyme";
import { assert } from "chai";
import { createSandbox } from "sinon";
import { MemoryRouter as Router } from "react-router-dom";
import { StatementDetails, StatementDetailsProps } from "./statementDetails";
import { DiagnosticsView } from "./diagnostics/diagnosticsView";
import { getStatementDetailsPropsFixture } from "./statementDetails.fixture";
import { Loading } from "../loading";

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

    const wrapper = mount(
      <Router>
        <StatementDetails {...statementDetailsProps} />
      </Router>,
    );
    assert.isTrue(wrapper.find(Loading).prop("loading"));
    assert.isFalse(
      wrapper.find(StatementDetails).find("div.ant-tabs-tab").exists(),
    );
  });

  it("shows error alert when `lastError` is not null", () => {
    statementDetailsProps.statementsError = new Error("Something went wrong");

    const wrapper = mount(
      <Router>
        <StatementDetails {...statementDetailsProps} />
      </Router>,
    );
    assert.isNotNull(wrapper.find(Loading).prop("error"));
    assert.isFalse(
      wrapper.find(StatementDetails).find("div.ant-tabs-tab").exists(),
    );
  });

  // Repeat this test for dedicated vs. tenant clusters.
  describe.each([true, true])("Diagnostics tab, isTenant = %p", isTenant => {
    beforeEach(() => {
      statementDetailsProps.isTenant = isTenant;
    });

    it("switches to the Diagnostics tab and triggers the activation modal", () => {
      const onTabChangeSpy = jest.fn();
      const refreshNodesClickSpy = sandbox.spy();
      const refreshNodesLivenessClickSpy = sandbox.spy();
      const refreshStatementDiagnosticsRequestsClickSpy = sandbox.spy();
      const onDiagnosticsActivateClickSpy = sandbox.spy();

      const wrapper = mount(
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
      wrapper
        .find(StatementDetails)
        .find("div.ant-tabs-tab")
        .last()
        .simulate("click");
      expect(onTabChangeSpy).toHaveBeenCalledWith("diagnostics");

      // Click on the "Activate diagnostics" button in the tab.
      wrapper
        .find(DiagnosticsView)
        .findWhere(n => n.prop("children") === "Activate diagnostics")
        .first()
        .simulate("click");
      onDiagnosticsActivateClickSpy.calledOnceWith(
        statementDetailsProps.statementDetails.statement.metadata.query,
      );

      // Expect calls to refresh page information on load and update events. Node
      // and liveness refreshes should not happen for tenants.
      assert.equal(
        refreshNodesClickSpy.callCount,
        isTenant ? 0 : 2,
        "refresh nodes",
      );
      assert.equal(
        refreshNodesLivenessClickSpy.callCount,
        isTenant ? 0 : 2,
        "refresh liveness",
      );
      assert.isTrue(
        refreshStatementDiagnosticsRequestsClickSpy.calledTwice,
        "refresh diagnostics requests",
      );
    });

    it("shows completed diagnostics report", () => {
      // Navigate to correct tab.
      statementDetailsProps.history.location.search = new URLSearchParams([
        ["tab", "diagnostics"],
      ]).toString();

      const wrapper = mount(
        <Router>
          <StatementDetails {...statementDetailsProps} />
        </Router>,
      );

      assert.isTrue(
        wrapper
          .find(DiagnosticsView)
          .findWhere(n => n.text() === "Dec 08, 2021 at 9:51 UTC")
          .exists(),
      );
    });
  });
});
